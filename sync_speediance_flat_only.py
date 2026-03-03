# sync_speediance_flat_only.py
#
# Purpose:
#   Fetch ALL performed workouts (within TRAINING_DAYS) from Speediance,
#   normalize training details, and write ONE flat file:
#     data/training_flat.csv
#
# Output columns:
#   WORKOUT NAME, DATE,
#   EXERCISE NAME, SETS, TOTAL REPS,
#   AVG WEIGHT/REP, MAX WEIGHT/REP, TOTAL WEIGHT,
#   1RM (estimated + best-effort found)
#
# Required env (GitHub Actions secrets):
#   SPEEDIANCE_REGION
#   SPEEDIANCE_DEVICE_TYPE
#   SPEEDIANCE_ALLOW_MONSTER_MOVES
#   SPEEDIANCE_UNIT
#   SPEEDIANCE_TOKEN
#   SPEEDIANCE_USER_ID
#
# Optional env:
#   TRAINING_DAYS (default 365)
#   DETAIL_THROTTLE_SECONDS (default 1.2)
#   DETAIL_RETRIES (default 3)
#   MAX_TRAINING_DETAILS (default 999999)  # safety cap if you want
#
# Notes:
# - This script depends on your existing repo modules:
#     api_client.py (SpeedianceClient)
#     sync_speediance.py (normalization helpers)
# - It does NOT write training_records.json, training_compact/, etc.

import csv
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from api_client import SpeedianceClient
import sync_speediance as ss


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _parse_date_isoish(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    txt = str(s).strip()
    if not txt:
        return None
    # most are ISO strings already; normalize to YYYY-MM-DD when possible
    if len(txt) >= 10:
        head = txt[:10]
        if ss.re.match(r"^\d{4}-\d{2}-\d{2}$", head):
            return head
    return txt


def epley_1rm(weight: float, reps: int) -> float:
    if reps <= 0 or weight <= 0:
        return 0.0
    return float(weight) * (1.0 + (float(reps) / 30.0))


def find_possible_1rm_value(obj: Any) -> Optional[float]:
    """
    Best-effort: look for numeric fields in the raw payload that look like 1RM/one-rep-max.
    Keep conservative; return the first plausible value.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            k2 = str(k).lower()
            if any(tok in k2 for tok in ("1rm", "one_rm", "onerepmax", "one_rep_max", "oneRepMax".lower(), "rm1")):
                fv = _safe_float(v, 0.0)
                if fv > 0:
                    return fv
        for v in obj.values():
            got = find_possible_1rm_value(v)
            if got is not None:
                return got
    elif isinstance(obj, list):
        for it in obj:
            got = find_possible_1rm_value(it)
            if got is not None:
                return got
    return None


def aggregate_exercise(ex: Dict[str, Any]) -> Dict[str, Any]:
    sets_list = ex.get("sets") if isinstance(ex.get("sets"), list) else []

    sets_count = 0
    total_reps = 0
    total_weight = 0.0
    max_weight_per_rep = 0.0

    best_1rm_est = 0.0
    best_1rm_set_weight = 0.0
    best_1rm_set_reps = 0

    for s in sets_list:
        if not isinstance(s, dict):
            continue
        sets_count += 1
        reps = _safe_int(s.get("reps"), 0)
        total_reps += reps

        reps_detail = s.get("reps_detail")
        if isinstance(reps_detail, list) and reps_detail:
            for rd in reps_detail:
                w = _safe_float((rd or {}).get("weight"), 0.0)
                total_weight += w
                if w > max_weight_per_rep:
                    max_weight_per_rep = w
        else:
            w_set = _safe_float(s.get("weight"), 0.0)
            total_weight += (w_set * reps)
            if w_set > max_weight_per_rep:
                max_weight_per_rep = w_set

        w_for_1rm = _safe_float(s.get("weight"), 0.0)
        est = epley_1rm(w_for_1rm, reps)
        if est > best_1rm_est:
            best_1rm_est = est
            best_1rm_set_weight = w_for_1rm
            best_1rm_set_reps = reps

    avg_weight_per_rep = (total_weight / total_reps) if total_reps > 0 else 0.0

    return {
        "sets": sets_count,
        "total_reps": total_reps,
        "avg_weight_per_rep": round(avg_weight_per_rep, 3),
        "max_weight_per_rep": round(max_weight_per_rep, 3),
        "total_weight": round(total_weight, 3),
        "one_rm_estimated": round(best_1rm_est, 3),
        "one_rm_estimated_from_set_weight": round(best_1rm_set_weight, 3),
        "one_rm_estimated_from_set_reps": int(best_1rm_set_reps),
    }


def run() -> None:
    # env
    days = ss._env_int("TRAINING_DAYS", 365)
    max_details = ss._env_int("MAX_TRAINING_DETAILS", 999999)
    throttle_s = ss._env_float("DETAIL_THROTTLE_SECONDS", 1.2)
    retries = ss._env_int("DETAIL_RETRIES", 3)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    # client
    c = SpeedianceClient()
    ss.configure_client(c)
    ss.ensure_auth_token_only(c)

    # records
    records_obj = c.get_training_records(start_date, end_date)
    records_list = ss.extract_records_list(records_obj)

    normalized_records: List[dict] = []
    for rec in records_list:
        rid, tid = ss.pick_ids(rec)
        if not rid or not tid:
            continue
        rtype = rec.get("type")
        try:
            rtype_i = int(rtype) if rtype is not None else None
        except Exception:
            rtype_i = None

        normalized_records.append(
            {
                "record_id": rid,
                "training_id": tid,
                "date": ss.get_record_date(rec),
                "title": rec.get("title"),
                "type": rtype_i,
                "startTime": rec.get("startTime"),
                "endTime": rec.get("endTime"),
            }
        )

    normalized_sorted = sorted(normalized_records, key=lambda x: (x.get("date") or ""), reverse=True)
    normalized_sorted = normalized_sorted[:max_details]

    # normalization helpers (library maps used for fallback naming)
    id_to_name, name_to_id = ss.load_or_refresh_library_maps(c)

    # aggregate rows
    rows: List[Dict[str, Any]] = []

    for item in normalized_sorted:
        tid = item["training_id"]
        rid = item["record_id"]
        rtype = item.get("type")

        payload: Optional[Any] = None
        last_err: Optional[str] = None

        for attempt in range(1, retries + 1):
            try:
                payload, _source_hint = ss.fetch_detail_with_type_rule(c, tid, rtype)
                if payload is not None:
                    break
                last_err = f"Empty detail for training_id={tid} (attempt {attempt})"
            except Exception as e:
                last_err = repr(e)
            time.sleep(throttle_s * attempt)

        if payload is None:
            # skip, but keep a sentinel row if you want later debugging
            rows.append(
                {
                    "workout_name": item.get("title"),
                    "date": _parse_date_isoish(item.get("date")),
                    "training_id": tid,
                    "record_id": rid,
                    "exercise_name": None,
                    "sets": None,
                    "total_reps": None,
                    "avg_weight_per_rep": None,
                    "max_weight_per_rep": None,
                    "total_weight": None,
                    "one_rm_found": None,
                    "one_rm_estimated": None,
                    "error": last_err,
                }
            )
            continue

        # normalize exercises from payload
        exercises, _normalized_as = ss.normalize_best(payload, id_to_name, name_to_id, c)

        # best-effort 1RM from raw payload (rare; optional)
        one_rm_found = find_possible_1rm_value(payload)

        for ex in exercises:
            if not isinstance(ex, dict):
                continue
            agg = aggregate_exercise(ex)

            rows.append(
                {
                    "workout_name": item.get("title"),
                    "date": _parse_date_isoish(item.get("date")),
                    "training_id": tid,
                    "record_id": rid,

                    "exercise_name": ex.get("name"),
                    "sets": agg["sets"],
                    "total_reps": agg["total_reps"],
                    "avg_weight_per_rep": agg["avg_weight_per_rep"],
                    "max_weight_per_rep": agg["max_weight_per_rep"],
                    "total_weight": agg["total_weight"],

                    "one_rm_found": round(float(one_rm_found), 3) if one_rm_found else None,
                    "one_rm_estimated": agg["one_rm_estimated"],
                    "one_rm_estimated_from_set_weight": agg["one_rm_estimated_from_set_weight"],
                    "one_rm_estimated_from_set_reps": agg["one_rm_estimated_from_set_reps"],

                    "error": None,
                }
            )

        time.sleep(throttle_s)

    # write ONE file
    os.makedirs("data", exist_ok=True)
    out_csv = os.path.join("data", "training_flat.csv")

    fieldnames = [
        "workout_name",
        "date",
        "exercise_name",
        "sets",
        "total_reps",
        "avg_weight_per_rep",
        "max_weight_per_rep",
        "total_weight",
        "one_rm_found",
        "one_rm_estimated",
        "one_rm_estimated_from_set_weight",
        "one_rm_estimated_from_set_reps",
        "training_id",
        "record_id",
        "error",
    ]

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {len(rows)} rows -> {out_csv}")

    # write JSON (Action-friendly)
    out_json = os.path.join("data", "training_flat.json")
    payload = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "training_days": days,
            "start_date": start_date,
            "end_date": end_date,
            "row_count": len(rows),
            "schema": "one row per (workout, exercise) aggregated across sets",
        },
        "rows": rows,
    }
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote JSON -> {out_json}")



if __name__ == "__main__":
    run()

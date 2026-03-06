# sync_speediance_flat_only_v3.py
#
# Purpose:
#   Fetch ALL performed workouts (within TRAINING_DAYS) from Speediance,
#   normalize training details, and write ONE flat file:
#     data/training_flat.csv
#
# Output columns:
#   workout_name, date,
#   exercise_name,
#   per_side (Yes/No),
#   sets, total_reps, seconds,
#   avg_weight_per_rep, max_weight_per_rep, total_weight,
#   error
#
# Notes on correctness tweaks (per user report):
# - seconds are sourced from raw trainingdetails: finishedReps[].time (fallback to normalized set.time)
# - unilateral (per_side) is sourced from finishedReps[].leftRight (0=both sides together; 1/2=single side)
# - for per_side=Yes, sets are halved (weight/reps aggregation is NOT altered)
# - one_rm_found, training_id, record_id are intentionally omitted

import csv
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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
    if len(txt) >= 10:
        head = txt[:10]
        if ss.re.match(r"^\d{4}-\d{2}-\d{2}$", head):
            return head
    return txt


_PER_SIDE_NAME_PATTERNS = (
    "per side",
    "each side",
    "each leg",
    "each arm",
    "per arm",
    "/side",
    "single arm",
    "single-arm",
    "single leg",
    "single-leg",
    "one arm",
    "one-arm",
    "one leg",
    "one-leg",
    "unilateral",
)


def _finished_reps_list(ex: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Speediance raw often uses finishedReps
    fr = ex.get("finishedReps")
    if isinstance(fr, list):
        return [x for x in fr if isinstance(x, dict)]
    # fallback: occasionally snake_case
    fr2 = ex.get("finished_reps")
    if isinstance(fr2, list):
        return [x for x in fr2 if isinstance(x, dict)]
    return []


def is_per_side_exercise(ex: Dict[str, Any]) -> bool:
    """Primary: finishedReps[].leftRight; fallback: name heuristics."""
    fr_list = _finished_reps_list(ex)
    # leftRight: 0 means both sides together; 1/2 indicates unilateral sides
    for fr in fr_list:
        lr = _safe_int(fr.get("leftRight"), 0)
        if lr in (1, 2):
            return True

    name = str(ex.get("name") or "").lower()
    if any(pat in name for pat in _PER_SIDE_NAME_PATTERNS):
        return True

    note = str(ex.get("note") or ex.get("comment") or "").lower()
    if note and any(pat in note for pat in ("per side", "per arm", "each side", "each arm", "each leg")):
        return True

    sets_list = ex.get("sets") if isinstance(ex.get("sets"), list) else []
    for s in sets_list:
        if not isinstance(s, dict):
            continue
        s_note = str(s.get("note") or s.get("comment") or s.get("remark") or "").lower()
        if s_note and any(pat in s_note for pat in ("per side", "per arm", "each side", "each arm", "each leg")):
            return True

    return False


def _count_sets_raw(ex: Dict[str, Any]) -> int:
    """Prefer finishedReps count; fallback to normalized sets count."""
    fr_list = _finished_reps_list(ex)
    if fr_list:
        return len(fr_list)
    sets_list = ex.get("sets") if isinstance(ex.get("sets"), list) else []
    return sum(1 for s in sets_list if isinstance(s, dict))


def aggregate_exercise(ex: Dict[str, Any], per_side: bool) -> Dict[str, Any]:
    sets_count_raw = _count_sets_raw(ex)

    # reps + seconds: source from finishedReps when present
    total_reps = 0
    total_seconds = 0

    fr_list = _finished_reps_list(ex)
    if fr_list:
        for fr in fr_list:
            # finishedCount is the performed rep count when reps-based
            fc = _safe_int(fr.get("finishedCount"), 0)
            total_reps += fc
            total_seconds += _safe_int(fr.get("time"), 0)
    else:
        # fallback to normalized sets
        sets_list = ex.get("sets") if isinstance(ex.get("sets"), list) else []
        for s in sets_list:
            if not isinstance(s, dict):
                continue
            total_reps += _safe_int(s.get("reps"), 0)
            total_seconds += _safe_int(s.get("time"), 0)

    # weights: keep your previous normalized logic (best-effort)
    total_weight = 0.0
    max_weight_per_rep = 0.0

    sets_list = ex.get("sets") if isinstance(ex.get("sets"), list) else []
    for s in sets_list:
        if not isinstance(s, dict):
            continue

        reps = _safe_int(s.get("reps"), 0)

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

    # output sets: halve if per_side=Yes, but do NOT affect reps/weight totals
    sets_out: Any = sets_count_raw
    if per_side and sets_count_raw > 0:
        sets_out = sets_count_raw / 2.0
        if float(sets_out).is_integer():
            sets_out = int(sets_out)

    avg_weight_per_rep = (total_weight / total_reps) if total_reps > 0 else 0.0

    return {
        "sets": sets_out,
        "total_reps": int(total_reps),
        "seconds": int(total_seconds),
        "avg_weight_per_rep": round(avg_weight_per_rep, 3),
        "max_weight_per_rep": round(max_weight_per_rep, 3),
        "total_weight": round(total_weight, 3),
    }


def run() -> None:
    days = ss._env_int("TRAINING_DAYS", 365)
    max_details = ss._env_int("MAX_TRAINING_DETAILS", 999999)
    throttle_s = ss._env_float("DETAIL_THROTTLE_SECONDS", 1.2)
    retries = ss._env_int("DETAIL_RETRIES", 3)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    c = SpeedianceClient()
    ss.configure_client(c)
    ss.ensure_auth_token_only(c)

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
            }
        )

    normalized_sorted = sorted(normalized_records, key=lambda x: (x.get("date") or ""), reverse=True)
    normalized_sorted = normalized_sorted[:max_details]

    id_to_name, name_to_id = ss.load_or_refresh_library_maps(c)

    rows: List[Dict[str, Any]] = []

    for item in normalized_sorted:
        tid = item["training_id"]
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
            rows.append(
                {
                    "workout_name": item.get("title"),
                    "date": _parse_date_isoish(item.get("date")),
                    "exercise_name": None,
                    "per_side": None,
                    "sets": None,
                    "total_reps": None,
                    "seconds": None,
                    "avg_weight_per_rep": None,
                    "max_weight_per_rep": None,
                    "total_weight": None,
                    "error": last_err,
                }
            )
            continue

        exercises, _normalized_as = ss.normalize_best(payload, id_to_name, name_to_id, c)

        for ex in exercises:
            if not isinstance(ex, dict):
                continue

            per_side = is_per_side_exercise(ex)
            agg = aggregate_exercise(ex, per_side=per_side)

            rows.append(
                {
                    "workout_name": item.get("title"),
                    "date": _parse_date_isoish(item.get("date")),
                    "exercise_name": ex.get("name"),
                    "per_side": "Yes" if per_side else "No",
                    "sets": agg["sets"],
                    "total_reps": agg["total_reps"],
                    "seconds": agg["seconds"],
                    "avg_weight_per_rep": agg["avg_weight_per_rep"],
                    "max_weight_per_rep": agg["max_weight_per_rep"],
                    "total_weight": agg["total_weight"],
                    "error": None,
                }
            )

        time.sleep(throttle_s)

    os.makedirs("data", exist_ok=True)

    out_csv = os.path.join("data", "training_flat.csv")
    fieldnames = [
        "workout_name",
        "date",
        "exercise_name",
        "per_side",
        "sets",
        "total_reps",
        "seconds",
        "avg_weight_per_rep",
        "max_weight_per_rep",
        "total_weight",
        "error",
    ]

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {len(rows)} rows -> {out_csv}")

    out_json = os.path.join("data", "training_flat.json")
    payload_out = {
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
        json.dump(payload_out, f, ensure_ascii=False, separators=(";", ":"))
    print(f"Wrote JSON -> {out_json}")


if __name__ == "__main__":
    run()

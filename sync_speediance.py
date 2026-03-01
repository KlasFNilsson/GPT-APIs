import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple, List, Dict

from api_client import SpeedianceClient

print("SYNC_SPEEDIANCE_VERSION=2026-03-01A_ONLY_CLEAN")

DATA_DIR = "data"

REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
    re.compile(r".*apple.*userid.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
    re.compile(r".*serial.*", re.IGNORECASE),
]

# Reduce file size: drop huge telemetry arrays you said you don't need
DROP_TELEMETRY_KEYS = {
    "leftWatts", "rightWatts",
    "leftAmplitudes", "rightAmplitudes",
    "leftRopeSpeeds", "rightRopeSpeeds",
    "leftMinRopeLengths", "rightMinRopeLengths",
    "leftMaxRopeLengths", "rightMaxRopeLengths",
    "leftFinishedTimes", "rightFinishedTimes",
    "leftBreakTimes", "rightBreakTimes",
    "leftTimestamps", "rightTimestamps",
}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_json(path: str, payload: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if any(p.match(str(k)) for p in REDACT_KEY_PATTERNS):
                continue
            out[k] = redact(v)
        return out
    if isinstance(obj, list):
        return [redact(x) for x in obj]
    return obj

def prune_telemetry(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in DROP_TELEMETRY_KEYS:
                continue
            out[k] = prune_telemetry(v)
        return out
    if isinstance(obj, list):
        return [prune_telemetry(x) for x in obj]
    return obj

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v != "" else default

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    if v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "yes", "y", "on")

def configure_client(c: SpeedianceClient) -> None:
    region = _env_str("SPEEDIANCE_REGION", "EU")
    device_type = _env_int("SPEEDIANCE_DEVICE_TYPE", 1)
    allow_monster_moves = _env_bool("SPEEDIANCE_ALLOW_MONSTER_MOVES", False)
    unit = _env_int("SPEEDIANCE_UNIT", 0)

    token = _env_str("SPEEDIANCE_TOKEN", "")
    user_id = _env_str("SPEEDIANCE_USER_ID", "")

    c.save_config(
        user_id=user_id,
        token=token,
        region=region,
        unit=unit,
        custom_instruction="",
        device_type=device_type,
        allow_monster_moves=allow_monster_moves,
    )

def ensure_auth_token_only(c: SpeedianceClient) -> None:
    creds = getattr(c, "credentials", None)
    if not isinstance(creds, dict):
        raise RuntimeError("Client has no credentials dict; cannot use token-only auth.")
    tok = str(creds.get("token") or "").strip()
    uid = str(creds.get("user_id") or "").strip()
    if not (tok and uid):
        raise RuntimeError("Token-only auth missing. Ensure SPEEDIANCE_TOKEN and SPEEDIANCE_USER_ID are set.")

def extract_records_list(records_obj: Any) -> list[dict]:
    if isinstance(records_obj, list):
        return [r for r in records_obj if isinstance(r, dict)]
    if isinstance(records_obj, dict):
        for k in ("list", "records", "items", "data", "rows"):
            v = records_obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
    return []

def get_record_date(rec: dict) -> Optional[str]:
    for k in ("endTime", "finishTime", "trainingTime", "createTime", "startTime", "date"):
        v = rec.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None

def pick_ids(rec: dict) -> Tuple[Optional[str], Optional[str]]:
    record_id = rec.get("id")
    training_id = rec.get("trainingId")
    if training_id is None or str(training_id).strip() == "":
        training_id = rec.get("trainingInfoId")

    rid = str(record_id).strip() if record_id is not None and str(record_id).strip() != "" else None
    tid = str(training_id).strip() if training_id is not None and str(training_id).strip() != "" else None
    return rid, tid

def unwrap_data(payload: Any) -> Any:
    if isinstance(payload, dict) and "data" in payload:
        return payload.get("data")
    return payload

def _nums(xs: Any) -> List[float]:
    if not isinstance(xs, list):
        return []
    out: List[float] = []
    for x in xs:
        try:
            out.append(float(x))
        except Exception:
            pass
    return out

def extract_set_weight(info: dict) -> Dict[str, Any]:
    """
    Returns:
      - weight (representative, usually max)
      - weight_max
      - left_weight_max/right_weight_max if present
    """
    weights = _nums(info.get("weights"))
    left = _nums(info.get("leftWeights"))
    right = _nums(info.get("rightWeights"))

    left_max = max(left) if left else None
    right_max = max(right) if right else None

    if weights:
        wmax = max(weights)
        rep = wmax
    elif left or right:
        # If per-side exists, keep per-side maxima and also provide a combined estimate
        rep = 0.0
        if left_max is not None:
            rep += left_max
        if right_max is not None:
            rep += right_max
        wmax = rep
    else:
        rep = 0.0
        wmax = 0.0

    return {
        "weight": rep,
        "weight_max": wmax,
        "left_weight_max": left_max,
        "right_weight_max": right_max,
    }

def normalize_course_detail(detail_list: Any) -> List[dict]:
    """
    Input is typically the unwrapped 'data' from courseTrainingInfoDetail, which is a list of exercises.
    Output: list of exercises with sets.
    """
    if not isinstance(detail_list, list):
        return []

    exercises: List[dict] = []

    for ex in detail_list:
        if not isinstance(ex, dict):
            continue

        name = ex.get("actionLibraryName") or ex.get("actionName") or ex.get("name")
        finished = ex.get("finishedReps") if isinstance(ex.get("finishedReps"), list) else []
        if not name or not finished:
            continue

        sets: List[dict] = []
        total_volume = 0.0
        max_weight_seen = 0.0

        for s in finished:
            if not isinstance(s, dict):
                continue
            reps = int(s.get("finishedCount") or 0)
            info = s.get("trainingInfoDetail") if isinstance(s.get("trainingInfoDetail"), dict) else {}
            w = extract_set_weight(info)

            weight = float(w["weight"] or 0.0)
            volume = float(reps) * weight

            sets.append({
                "reps": reps,
                "weight": weight,
                "left_weight_max": w["left_weight_max"],
                "right_weight_max": w["right_weight_max"],
                "volume": volume,
                "capacity": s.get("capacity"),
                "maxWeight": s.get("maxWeight"),
                "oneRepMax": s.get("oneRepMax"),
            })

            total_volume += volume
            if weight > max_weight_seen:
                max_weight_seen = weight

        exercises.append({
            "name": name,
            "sets": sets,
            "set_count": len(sets),
            "total_volume": round(total_volume, 2),
            "max_weight": round(max_weight_seen, 2),
            "pr_flags": {
                "maxWeightPr": ex.get("maxWeightPr"),
                "totalCapacityPr": ex.get("totalCapacityPr"),
                "levelPr": ex.get("levelPr"),
            },
        })

    return exercises

def get_training_detail_best(c: SpeedianceClient, training_id: str) -> Tuple[Optional[Any], str]:
    # course first (your strength workouts look like course)
    try:
        p = c.get_training_detail(training_id, "course")
        d = unwrap_data(p)
        if isinstance(d, list) and len(d) > 0:
            return p, "course"
    except Exception:
        pass

    # then ctt
    try:
        p = c.get_training_detail(training_id, "ctt")
        d = unwrap_data(p)
        if isinstance(d, list) and len(d) > 0:
            return p, "ctt"
    except Exception:
        pass

    return None, "none"

def run_training_sync(c: SpeedianceClient) -> None:
    days = _env_int("TRAINING_DAYS", 365)
    max_details = _env_int("MAX_TRAINING_DETAILS", 10)
    throttle_s = float(_env_str("DETAIL_THROTTLE_SECONDS", "2.0"))
    retries = _env_int("DETAIL_RETRIES", 2)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    records_obj = c.get_training_records(start_date, end_date)
    records_list = extract_records_list(records_obj)

    normalized_records = []
    for rec in records_list:
        record_id, training_id = pick_ids(rec)
        if not record_id or not training_id:
            continue
        normalized_records.append({
            "record_id": record_id,
            "training_id": training_id,
            "date": get_record_date(rec),
            "title": rec.get("title"),
            "type": rec.get("type"),
            "raw": rec,  # only kept inside training_records.json, not in per-pass files
        })

    normalized_sorted = sorted(normalized_records, key=lambda x: (x.get("date") or ""), reverse=True)

    ensure_dir(DATA_DIR)
    ensure_dir(os.path.join(DATA_DIR, "training_compact"))

    # Keep training_records.json as a lookup list (includes raw record fields)
    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {
            "meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)},
            "records": redact(normalized_sorted),
        },
    )

    index = {
        "meta": {
            "generated_at": now_iso(),
            "start_date": start_date,
            "end_date": end_date,
            "max_details": max_details,
            "detail_throttle_seconds": throttle_s,
            "detail_retries": retries,
            "count_written": 0,
            "count_failed": 0,
        },
        "items": [],
        "errors": {},
    }

    for item in normalized_sorted[:max_details]:
        rid = item["record_id"]
        tid = item["training_id"]
        rec = item["raw"]

        payload = None
        source = "none"
        last_err = None

        for attempt in range(1, retries + 1):
            try:
                payload, source = get_training_detail_best(c, tid)
                if payload is not None:
                    break
                last_err = f"Empty detail for training_id={tid} (attempt {attempt})"
            except Exception as e:
                last_err = repr(e)
            time.sleep(throttle_s * attempt)

        if payload is None:
            index["count_failed"] = index.get("count_failed", 0) + 1
            index["errors"][tid] = {"record_id": rid, "error": last_err}
            continue

        data_unwrapped = unwrap_data(payload)
        data_unwrapped = prune_telemetry(redact(data_unwrapped))

        exercises = normalize_course_detail(data_unwrapped)

        compact = {
            "meta": {
                "generated_at": now_iso(),
                "record_id": rid,
                "training_id": tid,
                "source": source,
                "title": rec.get("title"),
                "type": rec.get("type"),
                "startTime": rec.get("startTime"),
                "endTime": rec.get("endTime"),
                "trainingTime_sec": rec.get("trainingTime"),
                "calorie": rec.get("calorie"),
                "totalCapacity": rec.get("totalCapacity"),
                "totalEnergy": rec.get("totalEnergy"),
            },
            "exercises": exercises,
            "exercise_count": len(exercises),
        }

        write_json(os.path.join(DATA_DIR, "training_compact", f"{tid}.json"), compact)
        index["items"].append({
            "training_id": tid,
            "record_id": rid,
            "title": rec.get("title"),
            "date": item.get("date"),
            "path": f"/data/training_compact/{tid}.json",
        })
        index["meta"]["count_written"] += 1

        time.sleep(throttle_s)

    write_json(os.path.join(DATA_DIR, "training_compact", "index.json"), index)

    # Sanity (no secrets)
    creds = getattr(c, "credentials", None)
    write_json(
        os.path.join(DATA_DIR, "sync_env_sanity.json"),
        {
            "meta": {"generated_at": now_iso()},
            "region": getattr(c, "region", None),
            "base_url": getattr(c, "base_url", None),
            "host": getattr(c, "host", None),
            "has_credentials_dict": isinstance(creds, dict),
            "credentials_keys": sorted(list(creds.keys())) if isinstance(creds, dict) else None,
            "has_user_id": bool(str(creds.get("user_id") or "").strip()) if isinstance(creds, dict) else False,
            "has_token": bool(str(creds.get("token") or "").strip()) if isinstance(creds, dict) else False,
        },
    )

def main() -> None:
    mode = _env_str("SYNC_MODE", "training").lower()
    if mode != "training":
        raise RuntimeError("This script only supports SYNC_MODE=training.")

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)
    run_training_sync(c)

if __name__ == "__main__":
    main()

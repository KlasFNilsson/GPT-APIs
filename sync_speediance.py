# sync_speediance.py
#
# Normalizes Speediance training details into a single compact format:
#   data/training_compact/index.json
#   data/training_compact/<training_id>.json
#
# Rule:
#   record.type == 5  -> custom (CTT) => try ctt first, then course fallback
#   record.type != 5  -> official (course) => try course first, then ctt fallback
#
# Output contains NO raw blobs; only compact exercise + set data.
#
# Env (GitHub Actions secrets):
#   SPEEDIANCE_REGION
#   SPEEDIANCE_DEVICE_TYPE
#   SPEEDIANCE_ALLOW_MONSTER_MOVES
#   SPEEDIANCE_UNIT
#   SPEEDIANCE_TOKEN
#   SPEEDIANCE_USER_ID
#
# Optional env:
#   TRAINING_DAYS (default 365)
#   MAX_TRAINING_DETAILS (default 10)
#   DETAIL_THROTTLE_SECONDS (default 2.0)
#   DETAIL_RETRIES (default 2)

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from api_client import SpeedianceClient

print("SYNC_SPEEDIANCE_VERSION=2026-03-01_NORMALIZE_TYPE5")

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

# Drop very large telemetry arrays to keep compact files small
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

def unwrap_data(payload: Any) -> Any:
    if isinstance(payload, dict) and "data" in payload:
        return payload.get("data")
    return payload

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

def is_nonempty_payload(payload: Any) -> bool:
    if payload is None:
        return False
    d = unwrap_data(payload)
    if isinstance(d, list):
        return len(d) > 0
    if isinstance(d, dict):
        return len(d.keys()) > 0
    return True

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _parse_csv_numbers(s: Any) -> List[float]:
    if s is None:
        return []
    if isinstance(s, list):
        out = []
        for v in s:
            fv = _to_float(v)
            if fv is not None:
                out.append(fv)
        return out
    txt = str(s).strip()
    if not txt:
        return []
    parts = re.split(r"[,\s]+", txt)
    out: List[float] = []
    for p in parts:
        if not p:
            continue
        fv = _to_float(p)
        if fv is not None:
            out.append(fv)
    return out

def _parse_csv_ints(s: Any) -> List[int]:
    if s is None:
        return []
    if isinstance(s, list):
        out = []
        for v in s:
            try:
                out.append(int(v))
            except Exception:
                pass
        return out
    txt = str(s).strip()
    if not txt:
        return []
    parts = re.split(r"[,\s]+", txt)
    out: List[int] = []
    for p in parts:
        if not p:
            continue
        try:
            out.append(int(float(p)))
        except Exception:
            pass
    return out

def _extract_set_weight_from_trainingInfoDetail(info: dict) -> Dict[str, Any]:
    weights = _parse_csv_numbers(info.get("weights"))
    left = _parse_csv_numbers(info.get("leftWeights"))
    right = _parse_csv_numbers(info.get("rightWeights"))

    left_max = max(left) if left else None
    right_max = max(right) if right else None

    if weights:
        w = max(weights)
    elif left or right:
        # best-effort combined load (many cable moves store per side)
        w = float((left_max or 0.0) + (right_max or 0.0))
    else:
        w = 0.0

    return {
        "weight": float(w),
        "left_weight_max": left_max,
        "right_weight_max": right_max,
    }

def normalize_course_detail(data_list: Any) -> List[dict]:
    """
    courseTrainingInfoDetail returns a list of exercises.
    Each exercise often has finishedReps[] with trainingInfoDetail containing weights[] etc.
    """
    if not isinstance(data_list, list):
        return []

    exercises: List[dict] = []

    for ex in data_list:
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
            w = _extract_set_weight_from_trainingInfoDetail(info)

            weight = float(w["weight"] or 0.0)
            volume = float(reps) * weight

            sets.append({
                "reps": reps,
                "weight": weight,
                "volume": round(volume, 3),
                "left_weight_max": w["left_weight_max"],
                "right_weight_max": w["right_weight_max"],
            })

            total_volume += volume
            max_weight_seen = max(max_weight_seen, weight)

        exercises.append({
            "name": str(name),
            "sets": sets,
            "set_count": len(sets),
            "total_volume": round(total_volume, 3),
            "max_weight": round(max_weight_seen, 3),
            "pr_flags": {
                "maxWeightPr": ex.get("maxWeightPr"),
                "totalCapacityPr": ex.get("totalCapacityPr"),
                "levelPr": ex.get("levelPr"),
            }
        })

    return exercises

def normalize_ctt_detail(data_obj: Any) -> List[dict]:
    """
    cttTrainingInfoDetail is typically a dict. Common pattern includes an action list:
      actionLibraryList (or actionList) with per-exercise CSV fields:
        setsAndReps / reps
        weights / leftWeights / rightWeights
    Because structures can vary, we:
      - find a list among known keys
      - for each entry, build sets by zipping reps + weights
    """
    if not isinstance(data_obj, dict):
        return []

    # try common keys for the exercise list
    ex_list = None
    for k in ("actionLibraryList", "actionList", "actions", "exerciseList", "exercises"):
        v = data_obj.get(k)
        if isinstance(v, list) and v:
            ex_list = v
            break
    if ex_list is None:
        return []

    exercises: List[dict] = []

    for ex in ex_list:
        if not isinstance(ex, dict):
            continue

        name = ex.get("actionLibraryName") or ex.get("actionName") or ex.get("name") or ex.get("libraryName")
        if not name:
            continue

        # reps sources (CTT often uses setsAndReps or reps list)
        reps_list: List[int] = []
        if "setsAndReps" in ex:
            reps_list = _parse_csv_ints(ex.get("setsAndReps"))
        elif "reps" in ex:
            reps_list = _parse_csv_ints(ex.get("reps"))
        elif "setAndRep" in ex:
            reps_list = _parse_csv_ints(ex.get("setAndRep"))

        # weights sources
        weights = _parse_csv_numbers(ex.get("weights"))
        left = _parse_csv_numbers(ex.get("leftWeights"))
        right = _parse_csv_numbers(ex.get("rightWeights"))

        # Build per-set weights:
        # - if weights exists: use it
        # - else if left/right exist: combine per index (or max) into total
        set_weights: List[float] = []
        if weights:
            set_weights = weights
        elif left or right:
            n = max(len(left), len(right))
            for i in range(n):
                lw = left[i] if i < len(left) else (left[-1] if left else 0.0)
                rw = right[i] if i < len(right) else (right[-1] if right else 0.0)
                set_weights.append(float(lw + rw))

        # Determine number of sets:
        n_sets = max(len(reps_list), len(set_weights))
        if n_sets == 0:
            # no parsable set info; skip
            continue

        # pad reps/weights
        if len(reps_list) < n_sets:
            reps_list = reps_list + ([reps_list[-1]] * (n_sets - len(reps_list)) if reps_list else [0] * (n_sets - len(reps_list)))
        if len(set_weights) < n_sets:
            set_weights = set_weights + ([set_weights[-1]] * (n_sets - len(set_weights)) if set_weights else [0.0] * (n_sets - len(set_weights)))

        sets: List[dict] = []
        total_volume = 0.0
        max_weight_seen = 0.0

        for i in range(n_sets):
            reps = int(reps_list[i] or 0)
            w = float(set_weights[i] or 0.0)
            vol = float(reps) * w
            sets.append({
                "reps": reps,
                "weight": w,
                "volume": round(vol, 3),
                "left_weight_max": None,
                "right_weight_max": None,
            })
            total_volume += vol
            max_weight_seen = max(max_weight_seen, w)

        exercises.append({
            "name": str(name),
            "sets": sets,
            "set_count": len(sets),
            "total_volume": round(total_volume, 3),
            "max_weight": round(max_weight_seen, 3),
            "pr_flags": {
                "maxWeightPr": ex.get("maxWeightPr"),
                "totalCapacityPr": ex.get("totalCapacityPr"),
                "levelPr": ex.get("levelPr"),
            }
        })

    return exercises

def fetch_detail_with_type_rule(c: SpeedianceClient, training_id: str, record_type: Optional[int]) -> Tuple[Optional[Any], str]:
    """
    Returns (payload, source) where source is 'course' or 'ctt'.
    Implements:
      type==5 => try ctt then course
      else    => try course then ctt
    """
    def try_course() -> Optional[Any]:
        try:
            p = c.get_training_detail(training_id, "course")
            return p if is_nonempty_payload(p) else None
        except Exception:
            return None

    def try_ctt() -> Optional[Any]:
        try:
            p = c.get_training_detail(training_id, "ctt")
            return p if is_nonempty_payload(p) else None
        except Exception:
            return None

    if record_type == 5:
        p = try_ctt()
        if p is not None:
            return p, "ctt"
        p = try_course()
        if p is not None:
            return p, "course"
        return None, "none"
    else:
        p = try_course()
        if p is not None:
            return p, "course"
        p = try_ctt()
        if p is not None:
            return p, "ctt"
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

    normalized_records: List[dict] = []
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
            # keep minimal record meta; no raw dumping into compact files
            "startTime": rec.get("startTime"),
            "endTime": rec.get("endTime"),
            "trainingTime_sec": rec.get("trainingTime"),
            "calorie": rec.get("calorie"),
            "totalCapacity": rec.get("totalCapacity"),
            "totalEnergy": rec.get("totalEnergy"),
        })

    normalized_sorted = sorted(normalized_records, key=lambda x: (x.get("date") or ""), reverse=True)

    ensure_dir(DATA_DIR)
    ensure_dir(os.path.join(DATA_DIR, "training_compact"))

    # Write records list for reference (still redacted)
    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {
            "meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)},
            "records": redact(normalized_sorted),
        },
    )

    index: Dict[str, Any] = {
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
        tid = item["training_id"]
        rid = item["record_id"]
        rtype = item.get("type")
        try:
            rtype_int = int(rtype) if rtype is not None else None
        except Exception:
            rtype_int = None

        payload: Optional[Any] = None
        source = "none"
        last_err: Optional[str] = None

        for attempt in range(1, retries + 1):
            try:
                payload, source = fetch_detail_with_type_rule(c, tid, rtype_int)
                if payload is not None:
                    break
                last_err = f"Empty detail for training_id={tid} (attempt {attempt})"
            except Exception as e:
                last_err = repr(e)
            time.sleep(throttle_s * attempt)

        if payload is None:
            index["meta"]["count_failed"] += 1
            index["errors"][tid] = {"record_id": rid, "type": rtype_int, "error": last_err}
            continue

        data_unwrapped = prune_telemetry(redact(unwrap_data(payload)))

        # Normalize depending on which endpoint produced data (or by shape)
        exercises: List[dict] = []
        if source == "course":
            exercises = normalize_course_detail(data_unwrapped)
        elif source == "ctt":
            exercises = normalize_ctt_detail(data_unwrapped)
        else:
            # shape-based fallback
            if isinstance(data_unwrapped, list):
                exercises = normalize_course_detail(data_unwrapped)
            elif isinstance(data_unwrapped, dict):
                exercises = normalize_ctt_detail(data_unwrapped)

        compact = {
            "meta": {
                "generated_at": now_iso(),
                "record_id": rid,
                "training_id": tid,
                "source": source,
                "title": item.get("title"),
                "type": rtype_int,
                "date": item.get("date"),
                "startTime": item.get("startTime"),
                "endTime": item.get("endTime"),
                "trainingTime_sec": item.get("trainingTime_sec"),
                "calorie": item.get("calorie"),
                "totalCapacity": item.get("totalCapacity"),
                "totalEnergy": item.get("totalEnergy"),
            },
            "exercises": exercises,
            "exercise_count": len(exercises),
        }

        write_json(os.path.join(DATA_DIR, "training_compact", f"{tid}.json"), compact)

        index["items"].append({
            "training_id": tid,
            "record_id": rid,
            "title": item.get("title"),
            "type": rtype_int,
            "date": item.get("date"),
            "source": source,
            "path": f"/data/training_compact/{tid}.json",
        })
        index["meta"]["count_written"] += 1

        time.sleep(throttle_s)

    write_json(os.path.join(DATA_DIR, "training_compact", "index.json"), index)

    # Sanity file (no secrets)
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
        raise RuntimeError("This script supports SYNC_MODE=training only.")

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)
    run_training_sync(c)

if __name__ == "__main__":
    main()

# sync_speediance.py
#
# Produces compact, normalized workout files (NO raw blobs):
#   data/training_compact/index.json
#   data/training_compact/<training_id>.json
#
# Endpoint selection rule:
#   record.type == 5  -> try ctt first, then course
#   record.type != 5  -> try course first, then ctt
# PLUS: shape-based normalization (list => course, dict => ctt) regardless of source
# PLUS: name resolution by actionLibraryId via cached library lookup to avoid "shifted names"
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
#   MAX_TRAINING_DETAILS (default 30)
#   DETAIL_THROTTLE_SECONDS (default 1.2)
#   DETAIL_RETRIES (default 3)
#   LIBRARY_REFRESH_HOURS (default 24)  # rebuild action library name cache at most once/day

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from api_client import SpeedianceClient

print("SYNC_SPEEDIANCE_VERSION=2026-03-01_TYPE5_SHAPE_IDLOOKUP")

DATA_DIR = "data"
COMPACT_DIR = os.path.join(DATA_DIR, "training_compact")
LIBRARY_CACHE_PATH = os.path.join(DATA_DIR, "library_lookup.json")

REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
    re.compile(r".*apple.*userid.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
    re.compile(r".*serial.*", re.IGNORECASE),
]

# Keep compact files small by dropping huge telemetry arrays if present
DROP_TELEMETRY_KEYS = {
    "leftWatts", "rightWatts",
    "leftAmplitudes", "rightAmplitudes",
    "leftRopeSpeeds", "rightRopeSpeeds",
    "leftMinRopeLengths", "rightMinRopeLengths",
    "leftMaxRopeLengths", "rightMaxRopeLengths",
    "leftFinishedTimes", "rightFinishedTimes",
    "leftBreakTimes", "rightBreakTimes",
    "leftTimestamps", "rightTimestamps",
    "watts", "amplitudes", "ropeSpeeds", "timestamps",
}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_json(path: str, payload: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

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

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    if v == "":
        return default
    try:
        return float(v)
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

def is_nonempty_payload(payload: Any) -> bool:
    if payload is None:
        return False
    d = unwrap_data(payload)
    if isinstance(d, list):
        return len(d) > 0
    if isinstance(d, dict):
        return len(d.keys()) > 0
    return True

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

# -------------------------
# Library lookup (ID -> name)
# -------------------------

def _file_age_hours(path: str) -> Optional[float]:
    try:
        st = os.stat(path)
        age_sec = time.time() - st.st_mtime
        return age_sec / 3600.0
    except Exception:
        return None

def build_library_lookup(c: SpeedianceClient) -> Dict[str, str]:
    """
    Uses SpeedianceClient.get_library() cache logic (device_type + allow_monster_moves)
    and builds a flat map: actionLibraryId(str) -> name(str)
    """
    lookup: Dict[str, str] = {}

    if not hasattr(c, "get_library"):
        return lookup

    lib = c.get_library()
    lib = unwrap_data(lib)
    lib = prune_telemetry(redact(lib))

    # Library shapes vary. Try multiple likely shapes.
    # We only need (id, name) pairs.
    def ingest_item(it: Any) -> None:
        if not isinstance(it, dict):
            return
        _id = it.get("id") or it.get("actionLibraryId") or it.get("actionId") or it.get("libraryId")
        name = it.get("actionLibraryName") or it.get("name") or it.get("actionName") or it.get("libraryName")
        if _id is None or name is None:
            return
        sid = str(_id).strip()
        sname = str(name).strip()
        if sid and sname and sid not in lookup:
            lookup[sid] = sname

    def walk(obj: Any) -> None:
        if isinstance(obj, list):
            for x in obj:
                walk(x)
        elif isinstance(obj, dict):
            ingest_item(obj)
            for v in obj.values():
                walk(v)

    walk(lib)
    return lookup

def load_or_refresh_library_lookup(c: SpeedianceClient) -> Dict[str, str]:
    refresh_hours = _env_int("LIBRARY_REFRESH_HOURS", 24)
    ensure_dir(DATA_DIR)

    age = _file_age_hours(LIBRARY_CACHE_PATH)
    if age is not None and age < refresh_hours:
        try:
            cached = read_json(LIBRARY_CACHE_PATH)
            if isinstance(cached, dict) and "lookup" in cached and isinstance(cached["lookup"], dict):
                return {str(k): str(v) for k, v in cached["lookup"].items()}
        except Exception:
            pass

    # rebuild
    lookup = build_library_lookup(c)
    write_json(
        LIBRARY_CACHE_PATH,
        {"meta": {"generated_at": now_iso(), "count": len(lookup)}, "lookup": lookup},
    )
    return lookup

# -------------------------
# Parsing helpers
# -------------------------

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _split_csv_like(s: Any) -> List[str]:
    if s is None:
        return []
    if isinstance(s, list):
        return [str(x) for x in s]
    txt = str(s).strip()
    if not txt:
        return []
    # allow comma or whitespace separated
    parts = re.split(r"[,\s]+", txt)
    return [p for p in parts if p != ""]

def _parse_csv_numbers(s: Any) -> List[float]:
    out: List[float] = []
    for p in _split_csv_like(s):
        fv = _to_float(p)
        if fv is not None:
            out.append(fv)
    return out

def _parse_csv_ints(s: Any) -> List[int]:
    out: List[int] = []
    for p in _split_csv_like(s):
        try:
            out.append(int(float(p)))
        except Exception:
            pass
    return out

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default

def _extract_weights_from_trainingInfoDetail(info: dict) -> Dict[str, Any]:
    weights = _parse_csv_numbers(info.get("weights"))
    left = _parse_csv_numbers(info.get("leftWeights"))
    right = _parse_csv_numbers(info.get("rightWeights"))

    left_max = max(left) if left else None
    right_max = max(right) if right else None

    if weights:
        w = max(weights)
    elif left or right:
        w = float((left_max or 0.0) + (right_max or 0.0))
    else:
        w = 0.0

    return {
        "weight": float(w),
        "left_weight_max": left_max,
        "right_weight_max": right_max,
    }

# -------------------------
# Normalizers
# -------------------------

def normalize_as_course(data_list: Any, name_lookup: Dict[str, str]) -> List[dict]:
    """
    courseTrainingInfoDetail usually unwraps to a list of exercises.
    Each exercise has actionLibraryId and finishedReps[] with trainingInfoDetail.weights etc.
    """
    if not isinstance(data_list, list):
        return []

    exercises: List[dict] = []
    for ex in data_list:
        if not isinstance(ex, dict):
            continue

        action_id = ex.get("actionLibraryId") or ex.get("id") or ex.get("actionId")
        action_id_s = str(action_id).strip() if action_id is not None else ""
        name = ex.get("actionLibraryName") or ex.get("actionName") or ex.get("name")
        if (not name) and action_id_s and action_id_s in name_lookup:
            name = name_lookup[action_id_s]

        finished = ex.get("finishedReps") if isinstance(ex.get("finishedReps"), list) else []
        if not name or not finished:
            continue

        sets: List[dict] = []
        total_volume = 0.0
        max_weight_seen = 0.0

        for s in finished:
            if not isinstance(s, dict):
                continue
            reps = _safe_int(s.get("finishedCount"), 0)
            info = s.get("trainingInfoDetail") if isinstance(s.get("trainingInfoDetail"), dict) else {}
            w = _extract_weights_from_trainingInfoDetail(info)
            weight = float(w["weight"] or 0.0)
            volume = float(reps) * weight
            sets.append(
                {
                    "reps": reps,
                    "weight": round(weight, 3),
                    "volume": round(volume, 3),
                    "left_weight_max": w["left_weight_max"],
                    "right_weight_max": w["right_weight_max"],
                }
            )
            total_volume += volume
            max_weight_seen = max(max_weight_seen, weight)

        exercises.append(
            {
                "name": str(name),
                "actionLibraryId": action_id_s or None,
                "sets": sets,
                "set_count": len(sets),
                "total_volume": round(total_volume, 3),
                "max_weight": round(max_weight_seen, 3),
            }
        )

    return exercises

def _find_exercise_list_in_ctt_dict(d: dict) -> Optional[list]:
    """
    CTT payload shapes vary. Try multiple plausible keys.
    """
    for k in (
        "actionLibraryList",
        "actionList",
        "actions",
        "exerciseList",
        "exercises",
        "actionInfoList",
        "trainingActionList",
        "details",
        "detail",
    ):
        v = d.get(k)
        if isinstance(v, list) and v:
            return v
    return None

def normalize_as_ctt(data_obj: Any, name_lookup: Dict[str, str]) -> List[dict]:
    """
    cttTrainingInfoDetail often unwraps to a dict with a list of exercises under some key.
    For each exercise entry, we try to build sets from:
      - finishedReps[] (if present)  -> same as course style
      - OR CSV fields: setsAndReps/reps and weights/leftWeights/rightWeights
    """
    if not isinstance(data_obj, dict):
        return []

    ex_list = _find_exercise_list_in_ctt_dict(data_obj)
    if ex_list is None:
        return []

    exercises: List[dict] = []

    for ex in ex_list:
        if not isinstance(ex, dict):
            continue

        action_id = (
            ex.get("actionLibraryId")
            or ex.get("actionId")
            or ex.get("libraryId")
            or ex.get("id")
        )
        action_id_s = str(action_id).strip() if action_id is not None else ""

        name = ex.get("actionLibraryName") or ex.get("actionName") or ex.get("name") or ex.get("libraryName")
        if (not name) and action_id_s and action_id_s in name_lookup:
            name = name_lookup[action_id_s]

        # 1) If it actually contains finishedReps, treat like course
        finished = ex.get("finishedReps") if isinstance(ex.get("finishedReps"), list) else None
        if finished:
            # reuse course-style extraction on this single entry
            tmp = normalize_as_course([ex], name_lookup)
            exercises.extend(tmp)
            continue

        # 2) CSV-based
        reps_list: List[int] = []
        for rk in ("setsAndReps", "setAndRep", "reps", "repList", "finishedCounts"):
            if rk in ex:
                reps_list = _parse_csv_ints(ex.get(rk))
                if reps_list:
                    break

        weights = _parse_csv_numbers(ex.get("weights"))
        left = _parse_csv_numbers(ex.get("leftWeights"))
        right = _parse_csv_numbers(ex.get("rightWeights"))

        set_weights: List[float] = []
        left_maxes: List[Optional[float]] = []
        right_maxes: List[Optional[float]] = []

        if weights:
            set_weights = weights
            left_maxes = [None] * len(set_weights)
            right_maxes = [None] * len(set_weights)
        elif left or right:
            n = max(len(left), len(right))
            for i in range(n):
                lw = left[i] if i < len(left) else (left[-1] if left else 0.0)
                rw = right[i] if i < len(right) else (right[-1] if right else 0.0)
                set_weights.append(float(lw + rw))
                left_maxes.append(float(lw))
                right_maxes.append(float(rw))

        n_sets = max(len(reps_list), len(set_weights))
        if n_sets == 0 or not name:
            continue

        # pad
        if len(reps_list) < n_sets:
            reps_list = reps_list + ([reps_list[-1]] * (n_sets - len(reps_list)) if reps_list else [0] * (n_sets - len(reps_list)))
        if len(set_weights) < n_sets:
            set_weights = set_weights + ([set_weights[-1]] * (n_sets - len(set_weights)) if set_weights else [0.0] * (n_sets - len(set_weights)))
        if len(left_maxes) < n_sets:
            left_maxes = left_maxes + ([left_maxes[-1]] * (n_sets - len(left_maxes)) if left_maxes else [None] * (n_sets - len(left_maxes)))
        if len(right_maxes) < n_sets:
            right_maxes = right_maxes + ([right_maxes[-1]] * (n_sets - len(right_maxes)) if right_maxes else [None] * (n_sets - len(right_maxes)))

        sets: List[dict] = []
        total_volume = 0.0
        max_weight_seen = 0.0

        for i in range(n_sets):
            reps = int(reps_list[i] or 0)
            w = float(set_weights[i] or 0.0)
            vol = float(reps) * w
            sets.append(
                {
                    "reps": reps,
                    "weight": round(w, 3),
                    "volume": round(vol, 3),
                    "left_weight_max": left_maxes[i],
                    "right_weight_max": right_maxes[i],
                }
            )
            total_volume += vol
            max_weight_seen = max(max_weight_seen, w)

        exercises.append(
            {
                "name": str(name),
                "actionLibraryId": action_id_s or None,
                "sets": sets,
                "set_count": len(sets),
                "total_volume": round(total_volume, 3),
                "max_weight": round(max_weight_seen, 3),
            }
        )

    return exercises

# -------------------------
# Fetch logic with type rule + robust fallback
# -------------------------

def fetch_detail_course(c: SpeedianceClient, training_id: str) -> Optional[Any]:
    try:
        p = c.get_training_detail(training_id, "course")
        return p if is_nonempty_payload(p) else None
    except Exception:
        return None

def fetch_detail_ctt(c: SpeedianceClient, training_id: str) -> Optional[Any]:
    try:
        p = c.get_training_detail(training_id, "ctt")
        return p if is_nonempty_payload(p) else None
    except Exception:
        return None

def fetch_detail_with_type_rule(c: SpeedianceClient, training_id: str, record_type: Optional[int]) -> Tuple[Optional[Any], str]:
    """
    Returns (payload, source_hint)
    """
    if record_type == 5:
        p = fetch_detail_ctt(c, training_id)
        if p is not None:
            return p, "ctt"
        p = fetch_detail_course(c, training_id)
        if p is not None:
            return p, "course"
        return None, "none"
    else:
        p = fetch_detail_course(c, training_id)
        if p is not None:
            return p, "course"
        p = fetch_detail_ctt(c, training_id)
        if p is not None:
            return p, "ctt"
        return None, "none"

def normalize_best(payload: Any, source_hint: str, name_lookup: Dict[str, str]) -> Tuple[List[dict], str]:
    """
    Shape-based normalization:
      - list => course normalizer
      - dict => ctt normalizer
    If that yields empty, attempt the other normalizer as fallback.
    Returns (exercises, normalized_as)
    """
    d = prune_telemetry(redact(unwrap_data(payload)))

    if isinstance(d, list):
        exs = normalize_as_course(d, name_lookup)
        if exs:
            return exs, "course(list)"
        # fallback: sometimes list contains dicts not matching; try ctt in a wrapped dict
        exs2 = normalize_as_ctt({"detail": d}, name_lookup)
        return exs2, "ctt(fallback-from-list)" if exs2 else "none"
    elif isinstance(d, dict):
        exs = normalize_as_ctt(d, name_lookup)
        if exs:
            return exs, "ctt(dict)"
        # fallback: sometimes dict contains list under a key that looks like course payload
        for k in ("data", "detail", "details", "list", "records", "items"):
            v = d.get(k)
            if isinstance(v, list) and v:
                exs2 = normalize_as_course(v, name_lookup)
                if exs2:
                    return exs2, f"course(fallback-from-dict:{k})"
        return [], "none"
    else:
        return [], "none"

# -------------------------
# Main sync
# -------------------------

def run_training_sync(c: SpeedianceClient) -> None:
    days = _env_int("TRAINING_DAYS", 365)
    max_details = _env_int("MAX_TRAINING_DETAILS", 30)
    throttle_s = _env_float("DETAIL_THROTTLE_SECONDS", 1.2)
    retries = _env_int("DETAIL_RETRIES", 3)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    ensure_dir(DATA_DIR)
    ensure_dir(COMPACT_DIR)

    # Build or load name lookup (once/day)
    name_lookup = load_or_refresh_library_lookup(c)

    records_obj = c.get_training_records(start_date, end_date)
    records_list = extract_records_list(records_obj)

    normalized_records: List[dict] = []
    for rec in records_list:
        rid, tid = pick_ids(rec)
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
                "date": get_record_date(rec),
                "title": rec.get("title"),
                "type": rtype_i,
                "startTime": rec.get("startTime"),
                "endTime": rec.get("endTime"),
                "trainingTime_sec": rec.get("trainingTime"),
                "calorie": rec.get("calorie"),
                "totalCapacity": rec.get("totalCapacity"),
                "totalEnergy": rec.get("totalEnergy"),
            }
        )

    normalized_sorted = sorted(normalized_records, key=lambda x: (x.get("date") or ""), reverse=True)

    # Save records list (redacted, but already minimal)
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
            "library_lookup_count": len(name_lookup),
            "count_written": 0,
            "count_failed": 0,
            "count_empty_exercises": 0,
        },
        "items": [],
        "errors": {},
    }

    for item in normalized_sorted[:max_details]:
        tid = item["training_id"]
        rid = item["record_id"]
        rtype = item.get("type")

        payload: Optional[Any] = None
        source_hint = "none"
        last_err: Optional[str] = None

        for attempt in range(1, retries + 1):
            try:
                payload, source_hint = fetch_detail_with_type_rule(c, tid, rtype)
                if payload is not None:
                    break
                last_err = f"Empty detail for training_id={tid} (attempt {attempt})"
            except Exception as e:
                last_err = repr(e)
            time.sleep(throttle_s * attempt)

        if payload is None:
            index["meta"]["count_failed"] += 1
            index["errors"][tid] = {"record_id": rid, "type": rtype, "error": last_err}
            continue

        exercises, normalized_as = normalize_best(payload, source_hint, name_lookup)

        # If still empty, do an aggressive second fetch from the other endpoint (regardless of type)
        if not exercises:
            other_payload = fetch_detail_course(c, tid) if source_hint == "ctt" else fetch_detail_ctt(c, tid)
            if other_payload is not None:
                ex2, norm2 = normalize_best(other_payload, "fallback", name_lookup)
                if ex2:
                    payload = other_payload
                    exercises = ex2
                    normalized_as = f"{norm2} (endpoint-fallback)"

        if not exercises:
            index["meta"]["count_empty_exercises"] += 1

        compact = {
            "meta": {
                "generated_at": now_iso(),
                "record_id": rid,
                "training_id": tid,
                "title": item.get("title"),
                "type": rtype,
                "date": item.get("date"),
                "startTime": item.get("startTime"),
                "endTime": item.get("endTime"),
                "trainingTime_sec": item.get("trainingTime_sec"),
                "calorie": item.get("calorie"),
                "totalCapacity": item.get("totalCapacity"),
                "totalEnergy": item.get("totalEnergy"),
                "source_hint": source_hint,
                "normalized_as": normalized_as,
            },
            "exercises": exercises,
            "exercise_count": len(exercises),
        }

        write_json(os.path.join(COMPACT_DIR, f"{tid}.json"), compact)

        index["items"].append(
            {
                "training_id": tid,
                "record_id": rid,
                "title": item.get("title"),
                "type": rtype,
                "date": item.get("date"),
                "path": f"/data/training_compact/{tid}.json",
                "source_hint": source_hint,
                "normalized_as": normalized_as,
                "exercise_count": len(exercises),
            }
        )
        index["meta"]["count_written"] += 1

        time.sleep(throttle_s)

    write_json(os.path.join(COMPACT_DIR, "index.json"), index)

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
        raise RuntimeError("This script supports SYNC_MODE=training only.")

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)
    run_training_sync(c)

if __name__ == "__main__":
    main()

# sync_speediance.py
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from api_client import SpeedianceClient

print("SYNC_SPEEDIANCE_VERSION=2026-03-02_SIDE_FIX_V1")

DATA_DIR = "data"
COMPACT_DIR = os.path.join(DATA_DIR, "training_compact")
LIBRARY_CACHE_PATH = os.path.join(DATA_DIR, "library_lookup.json")
LIBRARY_DUMP_KEYS_PATH = os.path.join(DATA_DIR, "library_dump_keys.json")
LIBRARY_DUMP_SAMPLE_PATH = os.path.join(DATA_DIR, "library_dump_sample.json")

REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
    re.compile(r".*apple.*userid.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
    re.compile(r".*serial.*", re.IGNORECASE),
    re.compile(r".*ip.*", re.IGNORECASE),
]

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

def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default

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
    for k in ("endTime", "finishTime", "createTime", "startTime", "date"):
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

def _file_age_hours(path: str) -> Optional[float]:
    try:
        st = os.stat(path)
        return (time.time() - st.st_mtime) / 3600.0
    except Exception:
        return None

def _norm_name(s: str) -> str:
    s = s.replace("\u200b", "").replace("\ufeff", "")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

CANDIDATE_ID_KEYS = {"actionLibraryId", "actionId", "libraryId", "id", "groupId"}
CANDIDATE_NAME_KEYS = {"actionLibraryName", "actionName", "libraryName", "name", "title"}

def build_library_debug_dump(redacted_lib: Any) -> None:
    def key_tree(obj: Any, depth: int) -> Any:
        if depth <= 0:
            if isinstance(obj, dict):
                return {"__type__": "dict", "__keys__": len(obj)}
            if isinstance(obj, list):
                return {"__type__": "list", "__len__": len(obj)}
            return {"__type__": type(obj).__name__}
        if isinstance(obj, dict):
            out = {"__type__": "dict", "__keys__": list(obj.keys())[:50]}
            child = {}
            for k in list(obj.keys())[:12]:
                child[k] = key_tree(obj.get(k), depth - 1)
            out["children"] = child
            return out
        if isinstance(obj, list):
            out = {"__type__": "list", "__len__": len(obj)}
            if obj:
                out["sample0"] = key_tree(obj[0], depth - 1)
            return out
        return {"__type__": type(obj).__name__, "__value_sample__": str(obj)[:80]}

    write_json(
        LIBRARY_DUMP_KEYS_PATH,
        {"meta": {"generated_at": now_iso()}, "key_tree_depth3": key_tree(redacted_lib, 3)},
    )

def extract_exercise_candidates(redacted_lib: Any) -> List[dict]:
    results: List[dict] = []

    def looks_like_exercise(d: dict) -> bool:
        has_id = any(k in d and d.get(k) not in (None, "", []) for k in CANDIDATE_ID_KEYS)
        has_name = any(k in d and str(d.get(k) or "").strip() != "" for k in CANDIDATE_NAME_KEYS)
        return has_id and has_name

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if looks_like_exercise(obj):
                results.append(obj)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)

    walk(redacted_lib)

    seen = set()
    deduped: List[dict] = []
    for d in results:
        _id = None
        _name = None
        for k in ("actionLibraryId", "id", "groupId", "libraryId", "actionId"):
            if d.get(k) not in (None, "", []):
                _id = str(d.get(k)).strip()
                break
        for k in ("actionLibraryName", "name", "actionName", "libraryName", "title"):
            if str(d.get(k) or "").strip():
                _name = str(d.get(k)).strip()
                break
        if not _id or not _name:
            continue
        key = (_id, _name.lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(d)

    return deduped

def build_library_maps(c: SpeedianceClient) -> Tuple[Dict[str, str], Dict[str, str]]:
    id_to_name: Dict[str, str] = {}
    name_to_id: Dict[str, str] = {}

    raw = c.get_library()
    lib = unwrap_data(raw)
    lib = prune_telemetry(redact(lib))

    build_library_debug_dump(lib)

    candidates = extract_exercise_candidates(lib)
    write_json(
        LIBRARY_DUMP_SAMPLE_PATH,
        {
            "meta": {"generated_at": now_iso(), "candidate_count": len(candidates)},
            "top_level_type": type(lib).__name__,
            "payload_sample": str(lib)[:2000],
            "candidate_sample_first20": candidates[:20],
        },
    )

    for item in candidates:
        gid = None
        for k in ("actionLibraryId", "id", "groupId", "libraryId", "actionId"):
            if item.get(k) not in (None, "", []):
                gid = item.get(k)
                break
        name = None
        for k in ("actionLibraryName", "name", "actionName", "libraryName", "title"):
            if str(item.get(k) or "").strip():
                name = item.get(k)
                break
        if gid is None or name is None:
            continue

        gid_s = str(gid).strip()
        name_s = str(name).strip()
        if not gid_s or not name_s:
            continue

        id_to_name.setdefault(gid_s, name_s)
        nn = _norm_name(name_s)
        if nn and nn not in name_to_id:
            name_to_id[nn] = gid_s

    return id_to_name, name_to_id

def load_or_refresh_library_maps(c: SpeedianceClient) -> Tuple[Dict[str, str], Dict[str, str]]:
    refresh_hours = _env_int("LIBRARY_REFRESH_HOURS", 24)
    ensure_dir(DATA_DIR)

    age = _file_age_hours(LIBRARY_CACHE_PATH)
    if age is not None and age < refresh_hours:
        try:
            cached = read_json(LIBRARY_CACHE_PATH)
            if isinstance(cached, dict) and isinstance(cached.get("id_to_name"), dict) and isinstance(cached.get("name_to_id"), dict):
                return (
                    {str(k): str(v) for k, v in cached["id_to_name"].items()},
                    {str(k): str(v) for k, v in cached["name_to_id"].items()},
                )
        except Exception:
            pass

    id_to_name, name_to_id = build_library_maps(c)
    write_json(
        LIBRARY_CACHE_PATH,
        {
            "meta": {
                "generated_at": now_iso(),
                "id_to_name_count": len(id_to_name),
                "name_to_id_count": len(name_to_id),
            },
            "id_to_name": id_to_name,
            "name_to_id": name_to_id,
        },
    )
    return id_to_name, name_to_id

def resolve_group_id(action_library_id: Optional[str], exercise_name: str, name_to_id: Dict[str, str]) -> Optional[str]:
    if action_library_id and str(action_library_id).strip():
        return str(action_library_id).strip()
    key = _norm_name(exercise_name or "")
    return name_to_id.get(key)

# ---------- SIDE FIX ----------
def _side_from_leftRight_value(obj: Any) -> Optional[str]:
    """
    Very conservative:
    - only maps values we are sure about
    - returns None if value is missing/unknown
    """
    if not isinstance(obj, dict):
        return None
    if "leftRight" not in obj:
        return None
    try:
        v = int(obj["leftRight"])
    except Exception:
        return None
    # empirical mapping (can vary between apps):
    # keep only the common-case mapping, but we won't use it unless unilateral and mixed.
    if v == 0:
        return "L"
    if v == 1:
        return "R"
    return None

def extract_set_weight_summary(info: dict) -> Dict[str, Any]:
    w_list = _parse_csv_numbers(info.get("weights"))
    l_list = _parse_csv_numbers(info.get("leftWeights"))
    r_list = _parse_csv_numbers(info.get("rightWeights"))

    left_max = max(l_list) if l_list else None
    right_max = max(r_list) if r_list else None

    if w_list:
        w = max(w_list)
    elif l_list or r_list:
        w = float((left_max or 0.0) + (right_max or 0.0))
    else:
        w = 0.0

    return {"weight": float(w), "left_weight_max": left_max, "right_weight_max": right_max}

def extract_rep_weights(info: dict, reps: int) -> List[dict]:
    weights = _parse_csv_numbers(info.get("weights"))
    left = _parse_csv_numbers(info.get("leftWeights"))
    right = _parse_csv_numbers(info.get("rightWeights"))

    out: List[dict] = []

    if weights:
        if len(weights) < reps and len(weights) > 0:
            weights = weights + [weights[-1]] * (reps - len(weights))
        for i in range(reps):
            w = weights[i] if i < len(weights) else (weights[-1] if weights else 0.0)
            out.append({"rep_index": i + 1, "weight": float(w), "left_weight": None, "right_weight": None})
        return out

    if left or right:
        n = max(len(left), len(right), reps)
        if len(left) < n and len(left) > 0:
            left = left + [left[-1]] * (n - len(left))
        if len(right) < n and len(right) > 0:
            right = right + [right[-1]] * (n - len(right))
        for i in range(reps):
            lw = left[i] if i < len(left) else (left[-1] if left else 0.0)
            rw = right[i] if i < len(right) else (right[-1] if right else 0.0)
            out.append({"rep_index": i + 1, "weight": float(lw + rw), "left_weight": float(lw), "right_weight": float(rw)})
        return out

    return []

def _is_unilateral(c: SpeedianceClient, group_id: Optional[str]) -> bool:
    if not group_id:
        return False
    if not hasattr(c, "is_exercise_unilateral"):
        return False
    try:
        return bool(c.is_exercise_unilateral(int(group_id)))
    except Exception:
        return False

def _normalize_unilateral_sides(sets: List[dict]) -> List[dict]:
    """
    If sets have mixed explicit L/R -> keep.
    If sets have only one side (or none) and count is even -> split half L / half R.
    """
    if not sets:
        return sets
    sides = [s.get("side") for s in sets if isinstance(s, dict) and s.get("side") in ("L", "R")]
    uniq = sorted(set(sides))

    if len(uniq) >= 2:
        return sets  # already good

    n = len(sets)
    if n % 2 != 0:
        # can't safely split
        for s in sets:
            if isinstance(s, dict):
                s["side"] = None
        return sets

    half = n // 2
    out: List[dict] = []
    for i, s in enumerate(sets):
        if not isinstance(s, dict):
            continue
        ss = dict(s)
        ss["side"] = "L" if i < half else "R"
        out.append(ss)
    return out

def normalize_as_course(data_list: Any, id_to_name: Dict[str, str], name_to_id: Dict[str, str], c: SpeedianceClient) -> List[dict]:
    if not isinstance(data_list, list):
        return []

    exercises: List[dict] = []

    for ex in data_list:
        if not isinstance(ex, dict):
            continue

        action_id = ex.get("actionLibraryId") or ex.get("id") or ex.get("actionId")
        action_id_s = str(action_id).strip() if action_id is not None and str(action_id).strip() else None

        name = ex.get("actionLibraryName") or ex.get("actionName") or ex.get("name")
        if (not name) and action_id_s and action_id_s in id_to_name:
            name = id_to_name[action_id_s]
        if not name:
            continue
        name = str(name).strip()

        group_id = resolve_group_id(action_id_s, name, name_to_id)
        unilateral = _is_unilateral(c, group_id)

        finished = ex.get("finishedReps") if isinstance(ex.get("finishedReps"), list) else []
        if not finished:
            continue

        sets: List[dict] = []
        total_volume = 0.0
        max_weight_seen = 0.0

        for s in finished:
            if not isinstance(s, dict):
                continue

            reps = _safe_int(s.get("finishedCount"), 0)
            info = s.get("trainingInfoDetail") if isinstance(s.get("trainingInfoDetail"), dict) else {}

            wsum = extract_set_weight_summary(info)
            weight = float(wsum["weight"] or 0.0)
            volume = float(reps) * weight
            reps_detail = extract_rep_weights(info, reps)

            # SIDE RULE:
            # - bilateral: never set side
            # - unilateral: set side only from explicit leftRight if present (still might be noisy; we fix later)
            side = _side_from_leftRight_value(s) if unilateral else None

            sets.append(
                {
                    "reps": reps,
                    "weight": round(weight, 3),
                    "volume": round(volume, 3),
                    "side": side,
                    "left_weight_max": wsum["left_weight_max"],
                    "right_weight_max": wsum["right_weight_max"],
                    "reps_detail": reps_detail,
                }
            )
            total_volume += volume
            max_weight_seen = max(max_weight_seen, weight)

        if unilateral:
            sets = _normalize_unilateral_sides(sets)

        ex_obj: Dict[str, Any] = {
            "name": name,
            "actionLibraryId": group_id,
            "unilateral": unilateral,
            "sets": sets,
            "set_count": len(sets),
            "total_volume": round(total_volume, 3),
            "max_weight": round(max_weight_seen, 3),
        }

        if unilateral and len(sets) % 2 == 0:
            ex_obj["set_count_per_side"] = len(sets) // 2

        exercises.append(ex_obj)

    return exercises

def _find_exercise_list_in_ctt_dict(d: dict) -> Optional[list]:
    for k in ("actionLibraryList", "actionList", "actions", "exerciseList", "exercises", "actionInfoList", "trainingActionList", "details", "detail"):
        v = d.get(k)
        if isinstance(v, list) and v:
            return v
    return None

def normalize_as_ctt(data_obj: Any, id_to_name: Dict[str, str], name_to_id: Dict[str, str], c: SpeedianceClient) -> List[dict]:
    if not isinstance(data_obj, dict):
        return []
    ex_list = _find_exercise_list_in_ctt_dict(data_obj)
    if ex_list is None:
        return []
    # Many CTT payloads in your case end up usable as the "course(list)" shape anyway.
    return normalize_as_course(ex_list, id_to_name, name_to_id, c)

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

def normalize_best(payload: Any, id_to_name: Dict[str, str], name_to_id: Dict[str, str], c: SpeedianceClient) -> Tuple[List[dict], str]:
    d = prune_telemetry(redact(unwrap_data(payload)))
    if isinstance(d, list):
        exs = normalize_as_course(d, id_to_name, name_to_id, c)
        return exs, "course(list)" if exs else "none"
    if isinstance(d, dict):
        exs = normalize_as_ctt(d, id_to_name, name_to_id, c)
        if exs:
            return exs, "ctt(dict)"
        for k in ("data", "detail", "details", "list", "records", "items"):
            v = d.get(k)
            if isinstance(v, list) and v:
                exs2 = normalize_as_course(v, id_to_name, name_to_id, c)
                if exs2:
                    return exs2, f"course(fallback-from-dict:{k})"
        return [], "none"
    return [], "none"

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

    id_to_name, name_to_id = load_or_refresh_library_maps(c)

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

        exercises, normalized_as = normalize_best(payload, id_to_name, name_to_id, c)

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

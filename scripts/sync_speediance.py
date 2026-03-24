import argparse
import csv
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from api_client import SpeedianceClient

print("SYNC_SPEEDIANCE_VERSION=2026-03-20_RESTRUCTURED_V1")

DATA_DIR = "data"
COMPACT_DIR = os.path.join(DATA_DIR, "training_compact")
LIBRARY_CACHE_PATH = os.path.join(DATA_DIR, "library_lookup.json")
TRAINING_RECORDS_PATH = os.path.join(DATA_DIR, "training_records.json")
TRAINING_FLAT_CSV_PATH = os.path.join(DATA_DIR, "training_flat.csv")

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


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def write_json(path: str, payload: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_text_if_changed(path: str, content: str) -> bool:
    ensure_dir(os.path.dirname(path))
    old = None
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            old = f.read()
    if old == content:
        return False
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return True


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


def json_dumps_stable(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def strip_generated_at(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: strip_generated_at(v)
            for k, v in obj.items()
            if k != "generated_at"
        }
    if isinstance(obj, list):
        return [strip_generated_at(x) for x in obj]
    return obj


def write_json_if_changed(path: str, payload: Any, *, ignore_generated_at: bool = False) -> bool:
    ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        try:
            current = read_json(path)
            left = strip_generated_at(current) if ignore_generated_at else current
            right = strip_generated_at(payload) if ignore_generated_at else payload
            if json_dumps_stable(left) == json_dumps_stable(right):
                return False
        except Exception:
            pass
    write_json(path, payload)
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


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
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
    return [p for p in re.split(r"[,\s]+", txt) if p]


def _parse_csv_numbers(s: Any) -> List[float]:
    out: List[float] = []
    for p in _split_csv_like(s):
        fv = _to_float(p)
        if fv is not None:
            out.append(fv)
    return out


def configure_client(c: SpeedianceClient) -> None:
    c.save_config(
        user_id=_env_str("SPEEDIANCE_USER_ID", ""),
        token=_env_str("SPEEDIANCE_TOKEN", ""),
        region=_env_str("SPEEDIANCE_REGION", "EU"),
        unit=_env_int("SPEEDIANCE_UNIT", 0),
        custom_instruction="",
        device_type=_env_int("SPEEDIANCE_DEVICE_TYPE", 1),
        allow_monster_moves=_env_bool("SPEEDIANCE_ALLOW_MONSTER_MOVES", False),
    )


def ensure_auth_token_only(c: SpeedianceClient) -> None:
    creds = getattr(c, "credentials", None)
    if not isinstance(creds, dict):
        raise RuntimeError("Client has no credentials dict; cannot use token-only auth.")
    tok = str(creds.get("token") or "").strip()
    uid = str(creds.get("user_id") or "").strip()
    if not (tok and uid):
        raise RuntimeError("Token-only auth missing. Ensure SPEEDIANCE_TOKEN and SPEEDIANCE_USER_ID are set.")


def extract_records_list(records_obj: Any) -> List[dict]:
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
    rid = str(record_id).strip() if record_id is not None and str(record_id).strip() else None
    tid = str(training_id).strip() if training_id is not None and str(training_id).strip() else None
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


def extract_exercise_candidates(redacted_lib: Any) -> List[dict]:
    results: List[dict] = []

    def looks_like_exercise(d: dict) -> bool:
        has_id = any(k in d and d.get(k) not in (None, "", []) for k in CANDIDATE_ID_KEYS)
        has_name = any(k in d and str(d.get(k) or "").strip() for k in CANDIDATE_NAME_KEYS)
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
    lib = prune_telemetry(redact(unwrap_data(raw)))
    for item in extract_exercise_candidates(lib):
        gid = None
        name = None
        for k in ("actionLibraryId", "id", "groupId", "libraryId", "actionId"):
            if item.get(k) not in (None, "", []):
                gid = item.get(k)
                break
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


def load_or_refresh_library_maps(c: SpeedianceClient, *, force_refresh: bool = False) -> Tuple[Dict[str, str], Dict[str, str]]:
    refresh_hours = _env_int("LIBRARY_REFRESH_HOURS", 24)
    ensure_dir(DATA_DIR)
    age = _file_age_hours(LIBRARY_CACHE_PATH)
    if not force_refresh and age is not None and age < refresh_hours:
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
    payload = {
        "meta": {"generated_at": now_iso(), "id_to_name_count": len(id_to_name), "name_to_id_count": len(name_to_id)},
        "id_to_name": id_to_name,
        "name_to_id": name_to_id,
    }
    write_json_if_changed(LIBRARY_CACHE_PATH, payload, ignore_generated_at=True)
    return id_to_name, name_to_id


def resolve_group_id(action_library_id: Optional[str], exercise_name: str, name_to_id: Dict[str, str]) -> Optional[str]:
    if action_library_id and str(action_library_id).strip():
        return str(action_library_id).strip()
    return name_to_id.get(_norm_name(exercise_name or ""))


def _is_unilateral(c: SpeedianceClient, group_id: Optional[str]) -> bool:
    if not group_id or not hasattr(c, "is_exercise_unilateral"):
        return False
    try:
        return bool(c.is_exercise_unilateral(int(group_id)))
    except Exception:
        return False


def _extract_set_weight(info: dict) -> float:
    w_list = _parse_csv_numbers(info.get("weights"))
    if w_list:
        return float(max(w_list))
    l_list = _parse_csv_numbers(info.get("leftWeights"))
    r_list = _parse_csv_numbers(info.get("rightWeights"))
    if l_list or r_list:
        return float((max(l_list) if l_list else 0.0) + (max(r_list) if r_list else 0.0))
    return 0.0


def _extract_rep_weights(info: dict, reps: int, set_weight: float) -> List[dict]:
    weights = _parse_csv_numbers(info.get("weights"))
    if weights:
        if len(weights) < reps:
            weights = weights + [weights[-1]] * (reps - len(weights))
        return [{"rep_index": i + 1, "weight": float(weights[i])} for i in range(reps)]
    return [{"rep_index": i + 1, "weight": float(set_weight)} for i in range(reps)] if reps > 0 else []


def pair_consecutive_sets(sets: List[dict]) -> Optional[List[dict]]:
    if not sets:
        return []
    if len(sets) % 2 != 0:
        return None
    return [{"pair_index": (i // 2) + 1, "A": sets[i], "B": sets[i + 1]} for i in range(0, len(sets), 2)]


def normalize_course_like(ex_list: Any, id_to_name: Dict[str, str], name_to_id: Dict[str, str], c: SpeedianceClient) -> List[dict]:
    if not isinstance(ex_list, list):
        return []
    exercises: List[dict] = []
    for ex in ex_list:
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
            set_weight = _extract_set_weight(info)
            volume = float(reps) * float(set_weight)
            reps_detail = _extract_rep_weights(info, reps, set_weight)
            sets.append({
                "reps": reps,
                "weight": round(float(set_weight), 3),
                "volume": round(float(volume), 3),
                "time": _safe_int(s.get("time"), 0),
                "reps_detail": reps_detail,
            })
            total_volume += volume
            max_weight_seen = max(max_weight_seen, float(set_weight))
        ex_obj: Dict[str, Any] = {
            "name": name,
            "actionLibraryId": group_id,
            "unilateral": unilateral,
            "sets": sets,
            "set_count": len(sets),
            "total_volume": round(float(total_volume), 3),
            "max_weight": round(float(max_weight_seen), 3),
        }
        if unilateral:
            paired = pair_consecutive_sets(sets)
            if paired is not None:
                ex_obj["pairing"] = {
                    "method": "consecutive_pairs",
                    "note": "A/B pairs; no reliable left/right markers in source data",
                    "set_count_per_side": len(paired),
                }
                ex_obj["paired_sets"] = paired
        exercises.append(ex_obj)
    return exercises


def _find_exercise_list(d: dict) -> Optional[list]:
    for k in ("actionLibraryList", "actionList", "actions", "exerciseList", "exercises", "actionInfoList", "trainingActionList", "details", "detail"):
        v = d.get(k)
        if isinstance(v, list) and v:
            return v
    return None


def normalize_best(payload: Any, id_to_name: Dict[str, str], name_to_id: Dict[str, str], c: SpeedianceClient) -> Tuple[List[dict], str]:
    d = prune_telemetry(redact(unwrap_data(payload)))
    if isinstance(d, list):
        exs = normalize_course_like(d, id_to_name, name_to_id, c)
        return exs, "course(list)" if exs else "none"
    if isinstance(d, dict):
        ex_list = _find_exercise_list(d)
        if ex_list is not None:
            exs = normalize_course_like(ex_list, id_to_name, name_to_id, c)
            return exs, "dict(list)" if exs else "none"
        for k in ("data", "detail", "details", "list", "records", "items"):
            v = d.get(k)
            if isinstance(v, list) and v:
                exs2 = normalize_course_like(v, id_to_name, name_to_id, c)
                if exs2:
                    return exs2, f"dict(fallback:{k})"
    return [], "none"


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
    p = fetch_detail_course(c, training_id)
    if p is not None:
        return p, "course"
    p = fetch_detail_ctt(c, training_id)
    if p is not None:
        return p, "ctt"
    return None, "none"


def _finished_reps_list(ex: Dict[str, Any]) -> List[Dict[str, Any]]:
    fr = ex.get("finishedReps")
    if isinstance(fr, list):
        return [x for x in fr if isinstance(x, dict)]
    fr2 = ex.get("finished_reps")
    if isinstance(fr2, list):
        return [x for x in fr2 if isinstance(x, dict)]
    return []


def is_per_side_exercise_flat(ex: Dict[str, Any]) -> bool:
    for fr in _finished_reps_list(ex):
        lr = _safe_int(fr.get("leftRight"), 0)
        if lr in (1, 2):
            return True
    name = str(ex.get("name") or "").lower()
    if any(pat in name for pat in _PER_SIDE_NAME_PATTERNS):
        return True
    note = str(ex.get("note") or ex.get("comment") or "").lower()
    if note and any(pat in note for pat in ("per side", "per arm", "each side", "each arm", "each leg")):
        return True
    return False


def aggregate_compact_exercise(ex: Dict[str, Any]) -> Dict[str, Any]:
    per_side = bool(ex.get("unilateral")) or is_per_side_exercise_flat(ex)
    sets_list = ex.get("sets") if isinstance(ex.get("sets"), list) else []
    sets_count_raw = len([s for s in sets_list if isinstance(s, dict)])
    total_reps = 0
    total_seconds = 0
    total_weight = 0.0
    max_weight_per_rep = 0.0
    for s in sets_list:
        if not isinstance(s, dict):
            continue
        reps = _safe_int(s.get("reps"), 0)
        total_reps += reps
        total_seconds += _safe_int(s.get("time"), 0)
        reps_detail = s.get("reps_detail") if isinstance(s.get("reps_detail"), list) else []
        if reps_detail:
            for rd in reps_detail:
                w = _safe_float((rd or {}).get("weight"), 0.0)
                total_weight += w
                if w > max_weight_per_rep:
                    max_weight_per_rep = w
        else:
            w_set = _safe_float(s.get("weight"), 0.0)
            total_weight += w_set * reps
            if w_set > max_weight_per_rep:
                max_weight_per_rep = w_set
    sets_out: Any = sets_count_raw
    if per_side and sets_count_raw > 0:
        sets_out = sets_count_raw / 2.0
        if float(sets_out).is_integer():
            sets_out = int(sets_out)
    avg_weight_per_rep = (total_weight / total_reps) if total_reps > 0 else 0.0
    return {
        "per_side": "Yes" if per_side else "No",
        "sets": sets_out,
        "total_reps": int(total_reps),
        "seconds": int(total_seconds),
        "avg_weight_per_rep": round(avg_weight_per_rep, 3),
        "max_weight_per_rep": round(max_weight_per_rep, 3),
        "total_weight": round(total_weight, 3),
    }


def _parse_date_isoish(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    txt = str(s).strip()
    if not txt:
        return None
    if len(txt) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}$", txt[:10]):
        return txt[:10]
    return txt


def build_training_flat_csv_from_local(records_path: Path, compact_dir: Path, out_csv: Path):
    import json
    import csv

    if not records_path.exists():
        print("No training_records.json found, skipping flat CSV build")
        return

    with records_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("records", [])
    rows = []

    for rec in records:
        training_id = rec.get("training_id")
        compact_path = compact_dir / f"{training_id}.json"

        if not compact_path.exists():
            continue

        with compact_path.open("r", encoding="utf-8") as f:
            detail = json.load(f)

        exercises = detail.get("exercises", [])

        for exercise_index, ex in enumerate(exercises):
            sets = ex.get("sets", [])
            total_reps = 0
            total_seconds = 0
            total_weight = 0.0
            max_weight = 0.0

            for s in sets:
                reps = s.get("reps") or 0
                weight = s.get("weight") or 0
                seconds = s.get("time") or 0

                total_reps += reps
                total_seconds += seconds
                total_weight += reps * weight
                max_weight = max(max_weight, weight)

            avg_weight = (total_weight / total_reps) if total_reps > 0 else 0.0

            rows.append({
                "date": rec.get("startTime") or rec.get("date") or "",
                "workout_name": rec.get("title") or "",
                "exercise_name": ex.get("name") or "",
                "per_side": ex.get("per_side") or "No",
                "sets": len(sets),
                "total_reps": total_reps,
                "seconds": total_seconds,
                "avg_weight_per_rep": round(avg_weight, 3),
                "max_weight_per_rep": max_weight,
                "total_weight": total_weight,
                "exercise_index": exercise_index,  # <-- viktig
            })

    # Sortering:
    # 1. nyaste pass först
    # 2. rätt ordning inom passet
    rows.sort(
        key=lambda r: (
            r.get("date") or "",
            r.get("workout_name") or "",
            -int(r.get("exercise_index") or 0),
        ),
        reverse=True,
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "date",
        "workout_name",
        "exercise_name",
        "per_side",
        "sets",
        "total_reps",
        "seconds",
        "avg_weight_per_rep",
        "max_weight_per_rep",
        "total_weight",
    ]

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for r in rows:
            r.pop("exercise_index", None)
            writer.writerow(r)

    print(f"Wrote training_flat.csv with {len(rows)} rows")


def run_library_sync(c: SpeedianceClient, *, force_refresh: bool = True) -> None:
    load_or_refresh_library_maps(c, force_refresh=force_refresh)


def run_training_sync(c: SpeedianceClient, *, skip_library: bool = True) -> None:
    days = _env_int("TRAINING_DAYS", 365)
    max_details = _env_int("MAX_TRAINING_DETAILS", 200)
    throttle_s = _env_float("DETAIL_THROTTLE_SECONDS", 1.2)
    retries = _env_int("DETAIL_RETRIES", 3)
    only_missing = _env_bool("ONLY_FETCH_MISSING", False)
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")
    ensure_dir(DATA_DIR)
    ensure_dir(COMPACT_DIR)
    id_to_name: Dict[str, str] = {}
    name_to_id: Dict[str, str] = {}
    if not skip_library:
        id_to_name, name_to_id = load_or_refresh_library_maps(c, force_refresh=False)
    elif os.path.exists(LIBRARY_CACHE_PATH):
        try:
            cached = read_json(LIBRARY_CACHE_PATH)
            id_to_name = {str(k): str(v) for k, v in cached.get("id_to_name", {}).items()}
            name_to_id = {str(k): str(v) for k, v in cached.get("name_to_id", {}).items()}
        except Exception:
            pass
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
        normalized_records.append({
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
        })
    normalized_sorted = sorted(normalized_records, key=lambda x: (x.get("date") or ""), reverse=True)
    write_json(TRAINING_RECORDS_PATH, {
        "meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)},
        "records": redact(normalized_sorted),
    })
    index: Dict[str, Any] = {
        "meta": {
            "generated_at": now_iso(),
            "start_date": start_date,
            "end_date": end_date,
            "max_details": max_details,
            "detail_throttle_seconds": throttle_s,
            "detail_retries": retries,
            "only_fetch_missing": only_missing,
            "count_written": 0,
            "count_skipped_existing": 0,
            "count_unchanged": 0,
            "count_failed": 0,
            "count_empty_exercises": 0,
            "training_flat_rows": 0,
        },
        "items": [],
        "errors": {},
    }
    for item in normalized_sorted[:max_details]:
        tid = item["training_id"]
        rid = item["record_id"]
        rtype = item.get("type")
        out_path = os.path.join(COMPACT_DIR, f"{tid}.json")
        if only_missing and os.path.exists(out_path):
            index["meta"]["count_skipped_existing"] += 1
            index["items"].append({
                "training_id": tid,
                "record_id": rid,
                "title": item.get("title"),
                "type": rtype,
                "date": item.get("date"),
                "path": f"/data/training_compact/{tid}.json",
                "source_hint": "skipped_existing",
                "normalized_as": "skipped_existing",
                "exercise_count": None,
            })
            continue
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
        changed = write_json_if_changed(out_path, compact, ignore_generated_at=True)
        if changed:
            index["meta"]["count_written"] += 1
        else:
            index["meta"]["count_unchanged"] += 1
        index["items"].append({
            "training_id": tid,
            "record_id": rid,
            "title": item.get("title"),
            "type": rtype,
            "date": item.get("date"),
            "path": f"/data/training_compact/{tid}.json",
            "source_hint": source_hint,
            "normalized_as": normalized_as,
            "exercise_count": len(exercises),
            "status": "written" if changed else "unchanged",
        })
        time.sleep(throttle_s)
    training_flat_rows = build_training_flat_csv_from_local()
    index["meta"]["training_flat_rows"] = training_flat_rows
    write_json(os.path.join(COMPACT_DIR, "index.json"), index)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--library-only", action="store_true")
    parser.add_argument("--training-only", action="store_true")
    parser.add_argument("--skip-library", action="store_true")
    parser.add_argument("--force-library-refresh", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.library_only and args.training_only:
        raise RuntimeError("Use either --library-only or --training-only, not both.")
    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)
    if args.library_only:
        run_library_sync(c, force_refresh=True or args.force_library_refresh)
        return
    run_training_sync(c, skip_library=args.skip_library or args.training_only)


if __name__ == "__main__":
    main()

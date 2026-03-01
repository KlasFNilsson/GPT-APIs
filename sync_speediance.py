import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from api_client import SpeedianceClient

print("SYNC_SPEEDIANCE_VERSION=2026-03-01C")

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

CONTENT_KEYS = (
    "actionInfoList", "actions", "actionList", "trainingActionList",
    "sets", "setList", "exerciseList", "exercises", "items"
)

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

def has_content(payload: Any) -> bool:
    if payload is None:
        return False
    if isinstance(payload, list):
        return len(payload) > 0
    if isinstance(payload, dict):
        if "data" in payload:
            d = payload.get("data")
            if d is None:
                return False
            if isinstance(d, list):
                return len(d) > 0
            if isinstance(d, dict):
                for k in CONTENT_KEYS:
                    v = d.get(k)
                    if isinstance(v, list) and len(v) > 0:
                        return True
                return len(d) > 0
            return True
        for k in CONTENT_KEYS:
            v = payload.get(k)
            if isinstance(v, list) and len(v) > 0:
                return True
        return len(payload) > 0
    return True

def debug_dir() -> str:
    d = os.path.join(DATA_DIR, "debug")
    ensure_dir(d)
    return d

def save_last_debug(c: SpeedianceClient, tag: str) -> None:
    dbg = getattr(c, "last_debug_info", None)
    if dbg:
        write_json(os.path.join(debug_dir(), f"{tag}_last_debug.json"), dbg)

def save_sig(tag: str, requested_id: str, method: str, payload: Any) -> None:
    sig = {
        "generated_at": now_iso(),
        "requested_id": requested_id,
        "method": method,
        "payload_type": type(payload).__name__,
    }
    if isinstance(payload, dict):
        sig["code"] = payload.get("code")
        sig["message"] = payload.get("message")
        if "data" in payload:
            d = payload.get("data")
            sig["data_type"] = type(d).__name__
            if isinstance(d, list):
                sig["len_data"] = len(d)
            elif isinstance(d, dict):
                sig["data_keys"] = sorted(list(d.keys()))[:80]
                for k in CONTENT_KEYS:
                    v = d.get(k)
                    if isinstance(v, list):
                        sig[f"len_{k}"] = len(v)
    elif isinstance(payload, list):
        sig["len_list"] = len(payload)
    write_json(os.path.join(debug_dir(), f"{tag}_sig.json"), sig)

def unwrap_data(payload: Any) -> Any:
    # If wrapper dict with data, return data; else return payload
    if isinstance(payload, dict) and "data" in payload:
        return payload.get("data")
    return payload

def deep_find_first(obj: Any, keys: tuple[str, ...]) -> Optional[Any]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and v is not None:
                return v
            found = deep_find_first(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = deep_find_first(it, keys)
            if found is not None:
                return found
    return None

def normalize_full(record_raw: dict, detail_payload: Any, session_payload: Any, workout_payload: Any) -> dict:
    """
    Produce a single ChatGPT-friendly object.
    We keep raw blobs too, but put normalized fields on top.
    """
    session_data = unwrap_data(session_payload)
    workout_data = unwrap_data(workout_payload)
    detail_data = unwrap_data(detail_payload)

    # Try to locate an exercise/action list in session first, then workout, then detail
    candidates = [session_data, workout_data, detail_data]

    actions = None
    for c in candidates:
        if isinstance(c, dict):
            for k in ("actionInfoList", "actions", "actionList", "trainingActionList", "exerciseList", "exercises", "setList"):
                v = c.get(k)
                if isinstance(v, list) and v:
                    actions = v
                    break
        if actions:
            break

    # Find workoutId-ish for traceability
    workout_id = deep_find_first(session_data, ("workoutId", "planId", "templateId", "courseId"))
    # Session id-ish
    session_id = deep_find_first(session_data, ("sessionId", "trainingSessionId", "id"))

    return {
        "meta": {
            "generated_at": now_iso(),
            "title": record_raw.get("title"),
            "type": record_raw.get("type"),
            "startTime": record_raw.get("startTime"),
            "endTime": record_raw.get("endTime"),
            "calorie": record_raw.get("calorie"),
            "trainingTime": record_raw.get("trainingTime"),
            "record_id": record_raw.get("id"),
            "training_id": record_raw.get("trainingId"),
            "session_id_guess": session_id,
            "workout_id_guess": workout_id,
        },
        "normalized": {
            "actions_or_exercises": actions,  # may still be None; then look at raw blobs
        },
        "raw": {
            "record": redact(record_raw),
            "detail": redact(detail_payload),
            "session": redact(session_payload),
            "workout": redact(workout_payload),
        },
    }

def fetch_detail_course_ctt(c: SpeedianceClient, training_id: str, tag: str, debug_signature: bool) -> Tuple[Optional[Any], str]:
    # course
    try:
        p = c.get_training_detail(training_id, "course")
        if debug_signature:
            save_sig(f"{tag}_course", training_id, "get_training_detail(course)", p)
        if has_content(p):
            return p, "course"
        save_sig(f"{tag}_empty_course", training_id, "get_training_detail(course)", p)
        save_last_debug(c, f"{tag}_empty_course")
    except Exception:
        save_last_debug(c, f"{tag}_exception_course")

    # ctt
    try:
        p = c.get_training_detail(training_id, "ctt")
        if debug_signature:
            save_sig(f"{tag}_ctt", training_id, "get_training_detail(ctt)", p)
        if has_content(p):
            return p, "ctt"
        save_sig(f"{tag}_empty_ctt", training_id, "get_training_detail(ctt)", p)
        save_last_debug(c, f"{tag}_empty_ctt")
    except Exception:
        save_last_debug(c, f"{tag}_exception_ctt")

    return None, "none"

def fetch_session_info(c: SpeedianceClient, training_id: str, tag: str, debug_signature: bool) -> Optional[Any]:
    if not hasattr(c, "get_training_session_info"):
        return None
    try:
        p = c.get_training_session_info(training_id)
        if debug_signature:
            save_sig(f"{tag}_session_info", training_id, "get_training_session_info", p)
        return p
    except Exception:
        save_last_debug(c, f"{tag}_exception_session_info")
        return None

def fetch_workout_detail_if_possible(c: SpeedianceClient, session_payload: Any, tag: str, debug_signature: bool) -> Optional[Any]:
    if not hasattr(c, "get_workout_detail"):
        return None
    session_data = unwrap_data(session_payload)
    workout_id = deep_find_first(session_data, ("workoutId", "planId", "templateId"))
    if workout_id is None:
        return None
    try:
        wid = str(workout_id)
        p = c.get_workout_detail(wid)
        if debug_signature:
            save_sig(f"{tag}_workout_detail", wid, "get_workout_detail", p)
        return p
    except Exception:
        save_last_debug(c, f"{tag}_exception_workout_detail")
        return None

def run_training_sync(c: SpeedianceClient) -> None:
    days = _env_int("TRAINING_DAYS", 365)
    max_details = _env_int("MAX_TRAINING_DETAILS", 10)
    throttle_s = float(_env_str("DETAIL_THROTTLE_SECONDS", "2.0"))
    retries = _env_int("DETAIL_RETRIES", 2)
    debug_signature = _env_bool("DEBUG_SIGNATURE", True)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    records_obj = c.get_training_records(start_date, end_date)
    records_list = extract_records_list(records_obj)

    normalized = []
    for rec in records_list:
        record_id, training_id = pick_ids(rec)
        if not record_id or not training_id:
            continue
        normalized.append({"record_id": record_id, "training_id": training_id, "date": get_record_date(rec), "raw": rec})

    normalized_sorted = sorted(normalized, key=lambda x: (x.get("date") or ""), reverse=True)

    ensure_dir(DATA_DIR)
    ensure_dir(os.path.join(DATA_DIR, "training_details"))
    ensure_dir(os.path.join(DATA_DIR, "training_sessions"))
    ensure_dir(os.path.join(DATA_DIR, "training_full"))
    ensure_dir(os.path.join(DATA_DIR, "debug"))

    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)}, "records": redact(normalized_sorted)},
    )

    index = {
        "meta": {
            "generated_at": now_iso(),
            "start_date": start_date,
            "end_date": end_date,
            "max_details": max_details,
            "detail_throttle_seconds": throttle_s,
            "detail_retries": retries,
            "count_full_written": 0,
            "count_skipped": 0,
        },
        "items": [],
        "errors": {},
    }

    latest = normalized_sorted[:max_details]

    for item in latest:
        rid = item["record_id"]
        tid = item["training_id"]
        raw = item["raw"]

        detail_payload = None
        detail_source = "none"

        # Try detail course/ctt with retries
        for attempt in range(1, retries + 1):
            tag = f"{tid}_attempt{attempt}"
            detail_payload, detail_source = fetch_detail_course_ctt(c, tid, tag, debug_signature=(debug_signature and attempt == 1))
            if has_content(detail_payload):
                break
            time.sleep(throttle_s * attempt)

        # Session info (often has the exercise list)
        session_payload = fetch_session_info(c, tid, f"{tid}", debug_signature=debug_signature)

        # Workout detail if discoverable from session info
        workout_payload = fetch_workout_detail_if_possible(c, session_payload, f"{tid}", debug_signature=debug_signature)

        # Save raw-ish outputs (even if partial)
        if detail_payload is not None:
            write_json(
                os.path.join(DATA_DIR, "training_details", f"{tid}.json"),
                {"meta": {"generated_at": now_iso(), "training_id": tid, "record_id": rid, "source": detail_source}, "detail": redact(detail_payload)},
            )

        if session_payload is not None:
            write_json(
                os.path.join(DATA_DIR, "training_sessions", f"{tid}.json"),
                {"meta": {"generated_at": now_iso(), "training_id": tid, "record_id": rid}, "session": redact(session_payload)},
            )

        if workout_payload is not None:
            write_json(
                os.path.join(DATA_DIR, "training_sessions", f"{tid}_workout.json"),
                {"meta": {"generated_at": now_iso(), "training_id": tid, "record_id": rid}, "workout": redact(workout_payload)},
            )

        full = normalize_full(raw, detail_payload, session_payload, workout_payload)
        write_json(os.path.join(DATA_DIR, "training_full", f"{tid}.json"), full)

        index["items"].append(
            {
                "training_id": tid,
                "record_id": rid,
                "title": raw.get("title"),
                "type": raw.get("type"),
                "paths": {
                    "full": f"/data/training_full/{tid}.json",
                    "detail": f"/data/training_details/{tid}.json",
                    "session": f"/data/training_sessions/{tid}.json",
                    "workout": f"/data/training_sessions/{tid}_workout.json",
                },
            }
        )
        index["meta"]["count_full_written"] += 1
        time.sleep(throttle_s)

    write_json(os.path.join(DATA_DIR, "training_full", "index.json"), index)

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

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)

    if mode != "training":
        raise RuntimeError("This script is currently focused on training sync. Use SYNC_MODE=training.")
    run_training_sync(c)

if __name__ == "__main__":
    main()

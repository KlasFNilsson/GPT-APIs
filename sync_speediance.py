import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from api_client import SpeedianceClient

print("SYNC_SPEEDIANCE_VERSION=2026-03-01B")

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
    "sets", "setList", "exerciseList", "exercises",
)

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
    """
    record_id = raw.id
    training_id = raw.trainingId (preferred) else raw.trainingInfoId
    """
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
                sig["data_keys"] = sorted(list(d.keys()))[:50]
    elif isinstance(payload, list):
        sig["len_list"] = len(payload)
    write_json(os.path.join(debug_dir(), f"{tag}_sig.json"), sig)

def fetch_detail(c: SpeedianceClient, training_id: str, tag: str, debug_signature: bool) -> Tuple[Optional[Any], str]:
    # session info (if exists)
    if hasattr(c, "get_training_session_info"):
        try:
            p = c.get_training_session_info(training_id)
            if debug_signature:
                save_sig(f"{tag}_session_info", training_id, "get_training_session_info", p)
            if has_content(p):
                return p, "session_info"
            save_sig(f"{tag}_empty_session_info", training_id, "get_training_session_info", p)
            save_last_debug(c, f"{tag}_empty_session_info")
        except Exception:
            save_last_debug(c, f"{tag}_exception_session_info")

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

    # Write a small ID diagnostic for the top records
    write_json(
        os.path.join(DATA_DIR, "debug", "top_record_ids.json"),
        {
            "generated_at": now_iso(),
            "top3": [
                {
                    "raw_id": x["raw"].get("id"),
                    "raw_trainingId": x["raw"].get("trainingId"),
                    "raw_trainingInfoId": x["raw"].get("trainingInfoId"),
                    "chosen_record_id": x["record_id"],
                    "chosen_training_id": x["training_id"],
                    "title": x["raw"].get("title"),
                    "type": x["raw"].get("type"),
                }
                for x in normalized_sorted[:3]
            ],
        },
    )

    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)}, "records": redact(normalized_sorted)},
    )

    details_dir = os.path.join(DATA_DIR, "training_details")
    ensure_dir(details_dir)

    index = {
        "meta": {
            "generated_at": now_iso(),
            "start_date": start_date,
            "end_date": end_date,
            "max_details": max_details,
            "detail_throttle_seconds": throttle_s,
            "detail_retries": retries,
            "count_written": 0,
            "count_skipped_invalid": 0,
        },
        "items": [],
        "errors": {},
    }

    latest = normalized_sorted[:max_details]

    for item in latest:
        rid = item["record_id"]
        tid = item["training_id"]

        payload = None
        source = "none"
        last_err = None

        for attempt in range(1, retries + 1):
            tag = f"{tid}_attempt{attempt}"
            payload, source = fetch_detail(c, tid, tag, debug_signature=(debug_signature and attempt == 1))
            if has_content(payload):
                break
            last_err = f"No detail content for training_id={tid} (attempt {attempt})"
            time.sleep(throttle_s * attempt)

        if not has_content(payload):
            index["meta"]["count_skipped_invalid"] += 1
            index["errors"][f"detail:{tid}"] = {"record_id": rid, "training_id": tid, "error": last_err}
            continue

        write_json(
            os.path.join(details_dir, f"{tid}.json"),
            {"meta": {"generated_at": now_iso(), "training_id": tid, "record_id": rid, "source": source, "date": item.get("date")}, "detail": redact(payload)},
        )
        index["items"].append({"training_id": tid, "record_id": rid, "source": source, "path": f"/data/training_details/{tid}.json"})
        index["meta"]["count_written"] += 1
        time.sleep(throttle_s)

    write_json(os.path.join(details_dir, "index.json"), index)

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

def run_reference_sync(c: SpeedianceClient) -> None:
    methods = sorted([name for name in dir(c) if name.startswith("get_") and callable(getattr(c, name))])
    results = {}
    errors = {}
    for name in methods:
        fn = getattr(c, name)
        try:
            results[name] = fn()
        except TypeError:
            continue
        except Exception as e:
            errors[name] = repr(e)
    write_json(os.path.join(DATA_DIR, "reference.json"), {"meta": {"generated_at": now_iso(), "errors": errors}, "data": redact(results)})

def main() -> None:
    ensure_dir(DATA_DIR)
    mode = _env_str("SYNC_MODE", "training").lower()

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)

    if mode == "training":
        run_training_sync(c)
    elif mode == "reference":
        run_reference_sync(c)
    else:
        raise RuntimeError("Invalid SYNC_MODE. Use 'training' or 'reference'.")

if __name__ == "__main__":
    main()

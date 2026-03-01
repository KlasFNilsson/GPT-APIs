import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from api_client import SpeedianceClient

DATA_DIR = "data"

# --- Redaction ---
REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
    re.compile(r".*apple.*userid.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
]

TRAINING_ID_KEYS = {
    "trainingId", "training_id", "trainingInfoId", "trainingInfoID",
    "recordId", "recordID", "appTrainingId", "appTrainingID",
    "id"
}

TYPE_KEYS = ["trainingType", "type", "sourceType", "courseType", "templateType"]

# --- Telemetry bloat to drop ---
TELEMETRY_KEYS_TO_DROP = {
    "leftWatts", "rightWatts",
    "leftAmplitudes", "rightAmplitudes",
    "leftRopeSpeeds", "rightRopeSpeeds",
    "leftMinRopeLengths", "rightMinRopeLengths",
    "leftMaxRopeLengths", "rightMaxRopeLengths",
    "leftFinishedTimes", "rightFinishedTimes",
    "leftBreakTimes", "rightBreakTimes",
    "leftTimestamps", "rightTimestamps",
}
TELEMETRY_NAME_PATTERNS = [
    re.compile(r".*watts.*", re.IGNORECASE),
    re.compile(r".*amplitude.*", re.IGNORECASE),
    re.compile(r".*ropespeed.*", re.IGNORECASE),
    re.compile(r".*ropelength.*", re.IGNORECASE),
    re.compile(r".*timestamp.*", re.IGNORECASE),
]
MAX_TELEMETRY_LIST_LEN = 12
ALLOW_LIST_KEYS = {"weights"}  # keep weights arrays


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def redact(obj):
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


def prune_telemetry(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in TELEMETRY_KEYS_TO_DROP:
                continue
            vv = prune_telemetry(v)
            if (
                k not in ALLOW_LIST_KEYS
                and isinstance(vv, list)
                and len(vv) > MAX_TELEMETRY_LIST_LEN
                and any(p.match(str(k)) for p in TELEMETRY_NAME_PATTERNS)
            ):
                continue
            out[k] = vv
        return out
    if isinstance(obj, list):
        return [prune_telemetry(x) for x in obj]
    return obj


# --- env helpers (treat empty as missing) ---
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


# --- Auth/config ---
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


# --- Debug helpers ---
def debug_dir() -> str:
    d = os.path.join(DATA_DIR, "debug")
    ensure_dir(d)
    return d


def save_last_debug(c: SpeedianceClient, tag: str) -> None:
    dbg = getattr(c, "last_debug_info", None)
    if dbg:
        write_json(os.path.join(debug_dir(), f"{tag}_last_debug.json"), dbg)


def save_signature(tag: str, requested_id: str, ttype: Optional[str], detail: Any) -> None:
    """
    Stores a safe signature of the response (no secrets), to diagnose "empty but looks valid".
    """
    sig = {
        "generated_at": now_iso(),
        "requested_id": requested_id,
        "type_hint": ttype,
        "detail_type": type(detail).__name__,
    }

    if isinstance(detail, dict):
        sig["keys"] = sorted(list(detail.keys()))[:80]
        sig["code"] = detail.get("code")
        sig["message"] = detail.get("message")
        data = detail.get("data")
        sig["data_type"] = type(data).__name__
        if isinstance(data, dict):
            sig["data_keys"] = sorted(list(data.keys()))[:120]
            # common arrays that indicate real workout content
            for k in ("actionInfoList", "actions", "actionList", "trainingActionList", "finishedReps", "finishedRepList"):
                v = data.get(k)
                if isinstance(v, list):
                    sig[f"len_{k}"] = len(v)
    write_json(os.path.join(debug_dir(), f"{tag}_sig.json"), sig)


# --- Records parsing ---
def extract_records_list(records_obj):
    if isinstance(records_obj, list):
        return [r for r in records_obj if isinstance(r, dict)]
    if isinstance(records_obj, dict):
        for k in ("list", "records", "items", "data", "rows"):
            v = records_obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
    return []


def get_record_id(rec: dict) -> Optional[str]:
    for k in ("id", "trainingId", "trainingInfoId", "recordId"):
        v = rec.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def get_record_date(rec: dict) -> Optional[str]:
    for k in ("endTime", "finishTime", "trainingTime", "createTime", "startTime", "date"):
        v = rec.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def guess_record_type(rec: dict) -> Optional[str]:
    # If an explicit string type exists
    for k in TYPE_KEYS:
        v = rec.get(k)
        if v is None:
            continue
        s = str(v).lower()
        if "course" in s:
            return "course"
        if "ctt" in s or "custom" in s or "template" in s:
            return "ctt"

    # If numeric type exists, keep as None and try both later
    return None


# --- API wrappers ---
def fetch_training_records(c: SpeedianceClient, start_date: str, end_date: str):
    return c.get_training_records(start_date, end_date)


def fetch_training_stats(c: SpeedianceClient, start_date: str, end_date: str):
    if hasattr(c, "get_training_stats"):
        return c.get_training_stats(start_date, end_date)
    return None


def fetch_training_detail(c: SpeedianceClient, training_id: str, training_type: Optional[str]):
    # If we know the type, use it.
    if training_type in ("course", "ctt"):
        return c.get_training_detail(training_id, training_type)

    # Otherwise try both, but we will validate the payload strictly
    try:
        return c.get_training_detail(training_id, "course")
    except Exception:
        return c.get_training_detail(training_id, "ctt")


# --- Strict validation: real content + id match ---
def deep_find_first_id(obj: Any) -> Optional[str]:
    """
    Find a plausible training/session id inside payload.
    Returns first encountered value for keys like trainingId/trainingInfoId/id if it looks numeric.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in TRAINING_ID_KEYS and v is not None:
                s = str(v).strip()
                if s.isdigit():
                    return s
            found = deep_find_first_id(v)
            if found:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = deep_find_first_id(it)
            if found:
                return found
    return None


def has_workout_content(detail: Any) -> bool:
    """
    Require a wrapper dict with non-empty 'data' dict and at least one content list.
    """
    if not isinstance(detail, dict):
        return False

    # If the API uses {code, message, data}, require data dict
    if "data" in detail:
        data = detail.get("data")
        if not isinstance(data, dict) or len(data) == 0:
            return False

        content_keys = ("actionInfoList", "actions", "actionList", "trainingActionList", "finishedReps", "finishedRepList")
        for k in content_keys:
            v = data.get(k)
            if isinstance(v, list) and len(v) > 0:
                return True

        # Some payloads nest further
        for k in content_keys:
            # scan one level down for lists
            for vv in data.values():
                if isinstance(vv, dict):
                    w = vv.get(k)
                    if isinstance(w, list) and len(w) > 0:
                        return True

        return False

    # If there is no 'data', reject (we only accept expected structure)
    return False


def is_detail_valid_for_id(detail: Any, requested_id: str) -> bool:
    if not has_workout_content(detail):
        return False

    found_id = deep_find_first_id(detail.get("data"))
    if found_id and found_id != str(requested_id):
        # Payload contains an id that doesn't match the one requested => wrong pass
        return False

    return True


def run_training_sync(c: SpeedianceClient) -> None:
    days = _env_int("TRAINING_DAYS", 365)
    max_details = _env_int("MAX_TRAINING_DETAILS", 60)

    throttle_s = float(_env_str("DETAIL_THROTTLE_SECONDS", "1.5"))
    retries = _env_int("DETAIL_RETRIES", 4)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    # Records
    records_obj = fetch_training_records(c, start_date, end_date)
    records_list = extract_records_list(records_obj)

    normalized = []
    for rec in records_list:
        tid = get_record_id(rec)
        if not tid:
            continue
        normalized.append(
            {"id": tid, "type": guess_record_type(rec), "date": get_record_date(rec), "raw": rec}
        )

    normalized_sorted = sorted(normalized, key=lambda x: (x.get("date") or ""), reverse=True)

    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {
            "meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)},
            "records": redact(normalized_sorted),
        },
    )

    # Stats
    stats_obj = fetch_training_stats(c, start_date, end_date)
    write_json(
        os.path.join(DATA_DIR, "training_stats.json"),
        {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date}, "stats": redact(stats_obj)},
    )

    # Details + index
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
        tid = item["id"]
        ttype = item.get("type")
        out_path = os.path.join(details_dir, f"{tid}.json")

        # Determine if an existing file looks good
        existing_ok = False
        if os.path.exists(out_path):
            try:
                existing = read_json(out_path)
                existing_detail = existing.get("detail")
                if is_detail_valid_for_id(existing_detail, tid):
                    existing_ok = True
            except Exception:
                existing_ok = False

        detail = None
        last_err = None

        for attempt in range(1, retries + 1):
            try:
                detail = fetch_training_detail(c, tid, ttype)

                # Always store a signature for first attempt if DEBUG_SIGNATURE=1
                if _env_bool("DEBUG_SIGNATURE", False) and attempt == 1:
                    save_signature(f"{tid}_attempt{attempt}", tid, ttype, detail)

                if is_detail_valid_for_id(detail, tid):
                    break

                # Save signature + last_debug_info for invalid payloads
                save_signature(f"{tid}_invalid_attempt{attempt}", tid, ttype, detail)
                save_last_debug(c, f"{tid}_invalid_attempt{attempt}")
                last_err = f"Invalid/empty or id-mismatch (attempt {attempt})"

            except Exception as e:
                save_last_debug(c, f"{tid}_exception_attempt{attempt}")
                last_err = repr(e)

            # backoff
            time.sleep(throttle_s * attempt)

        if not is_detail_valid_for_id(detail, tid):
            index["meta"]["count_skipped_invalid"] += 1
            index["errors"][f"detail:{tid}"] = last_err or "Invalid after retries"
            # Do not overwrite a good file with junk
            if existing_ok:
                index["items"].append(
                    {"id": tid, "type": ttype, "date": item.get("date"), "path": f"/data/training_details/{tid}.json", "note": "kept_existing"}
                )
            continue

        detail_clean = prune_telemetry(redact(detail))
        write_json(
            out_path,
            {
                "meta": {"generated_at": now_iso(), "id": tid, "type": ttype, "date": item.get("date")},
                "detail": detail_clean,
            },
        )

        index["items"].append(
            {"id": tid, "type": ttype, "date": item.get("date"), "path": f"/data/training_details/{tid}.json"}
        )
        index["meta"]["count_written"] += 1

        time.sleep(throttle_s)

    write_json(os.path.join(details_dir, "index.json"), index)


def run_reference_sync(c: SpeedianceClient) -> None:
    methods = sorted([name for name in dir(c) if name.startswith("get_") and callable(getattr(c, name))])
    errors = {}
    results = {}
    for name in methods:
        fn = getattr(c, name)
        try:
            results[name] = fn()
        except TypeError:
            continue
        except Exception as e:
            errors[name] = repr(e)

    write_json(
        os.path.join(DATA_DIR, "reference.json"),
        {"meta": {"generated_at": now_iso(), "mode": "reference", "errors": errors}, "data": redact(results)},
    )


def main():
    ensure_dir(DATA_DIR)

    mode = _env_str("SYNC_MODE", "training").lower()

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)

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

    if mode == "training":
        run_training_sync(c)
    elif mode == "reference":
        run_reference_sync(c)
    else:
        raise RuntimeError("Invalid SYNC_MODE. Use 'training' or 'reference'.")


if __name__ == "__main__":
    main()

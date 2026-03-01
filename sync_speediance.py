import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

from api_client import SpeedianceClient

DATA_DIR = "data"

# --- Redaction (avoid publishing secrets/private identity) ---
REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
    re.compile(r".*apple.*userid.*", re.IGNORECASE),
    re.compile(r".*user.*id.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
    re.compile(r".*serial.*", re.IGNORECASE),
]

TRAINING_ID_KEYS = [
    "trainingId", "training_id", "trainingInfoId", "trainingInfoID",
    "id", "recordId", "recordID",
]

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
    re.compile(r".*breaktime.*", re.IGNORECASE),
    re.compile(r".*finishedtime.*", re.IGNORECASE),
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


# --- env helpers (treat empty string as missing) ---
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

    # This client stores auth in credentials dict via save_config
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
    tok = ""
    uid = ""
    if isinstance(creds, dict):
        tok = str(creds.get("token") or "").strip()
        uid = str(creds.get("user_id") or "").strip()

    if tok and uid:
        return

    raise RuntimeError(
        "Token-only auth missing. Ensure SPEEDIANCE_TOKEN and SPEEDIANCE_USER_ID are set and passed via workflow env."
    )


# --- Debug dump ---
def save_debug(c: SpeedianceClient, tag: str) -> None:
    dbg = getattr(c, "last_debug_info", None)
    if not dbg:
        return
    ddir = os.path.join(DATA_DIR, "debug")
    ensure_dir(ddir)
    write_json(os.path.join(ddir, f"{tag}.json"), dbg)


# --- Record parsing helpers ---
def extract_records_list(records_obj):
    if isinstance(records_obj, list):
        return [r for r in records_obj if isinstance(r, dict)]
    if isinstance(records_obj, dict):
        for k in ("list", "records", "items", "data", "rows"):
            v = records_obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
    return []


def get_record_id(rec: dict) -> str | None:
    for k in TRAINING_ID_KEYS:
        v = rec.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def get_record_date(rec: dict) -> str | None:
    for k in ("endTime", "finishTime", "trainingTime", "createTime", "startTime", "date"):
        v = rec.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def guess_record_type(rec: dict) -> str | None:
    for k in TYPE_KEYS:
        v = rec.get(k)
        if v is None:
            continue
        s = str(v).lower()
        if "course" in s:
            return "course"
        if "ctt" in s or "custom" in s or "template" in s:
            return "ctt"

    for k in ("templateCode", "customTrainingTemplateCode", "customTrainingTemplateId"):
        if rec.get(k) is not None:
            return "ctt"
    return None


# --- API wrappers ---
def fetch_training_records(c: SpeedianceClient, start_date: str, end_date: str):
    if hasattr(c, "get_training_records"):
        return c.get_training_records(start_date, end_date)
    raise RuntimeError("Client missing get_training_records")


def fetch_training_stats(c: SpeedianceClient, start_date: str, end_date: str):
    if hasattr(c, "get_training_stats"):
        return c.get_training_stats(start_date, end_date)
    return None


def fetch_training_detail(c: SpeedianceClient, training_id: str, training_type: str | None):
    if not hasattr(c, "get_training_detail"):
        raise RuntimeError("Client missing get_training_detail")

    # If we know the type, use it.
    if training_type in ("course", "ctt"):
        return c.get_training_detail(training_id, training_type)

    # Otherwise try both.
    try:
        return c.get_training_detail(training_id, "course")
    except Exception:
        return c.get_training_detail(training_id, "ctt")


def is_detail_valid(detail) -> bool:
    """
    Reject empty dict / None.
    Additionally try to confirm there is at least some payload under common keys.
    """
    if detail is None:
        return False
    if isinstance(detail, dict) and len(detail) == 0:
        return False

    # Often: { code, message, data: {...} } or { meta, detail: {...} }
    if isinstance(detail, dict):
        for k in ("data", "detail", "actions", "actionList", "actionInfoList", "trainingActionList"):
            if k in detail and detail[k]:
                return True
        # If it has non-empty keys but not recognized, accept (avoid false negatives)
        return len(detail.keys()) > 0

    return True


def run_training_sync(c: SpeedianceClient) -> None:
    # window + caps
    days = _env_int("TRAINING_DAYS", 120)
    max_details = _env_int("MAX_TRAINING_DETAILS", 30)

    # throttling/retries
    throttle_s = float(_env_str("DETAIL_THROTTLE_SECONDS", "1.2"))
    retries = _env_int("DETAIL_RETRIES", 3)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    # 1) Records
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

    # Keep API order if date missing; otherwise sort by date string desc (best-effort)
    normalized_sorted = sorted(normalized, key=lambda x: (x.get("date") or ""), reverse=True)

    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {
            "meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)},
            "records": redact(normalized_sorted),
        },
    )

    # 2) Stats
    stats_obj = fetch_training_stats(c, start_date, end_date)
    write_json(
        os.path.join(DATA_DIR, "training_stats.json"),
        {
            "meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date},
            "stats": redact(stats_obj),
        },
    )

    # 3) Details + index (do not overwrite with junk)
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

        # If file already exists and looks non-empty, we keep it unless we get a valid new one.
        existing_ok = False
        if os.path.exists(out_path):
            try:
                existing = read_json(out_path)
                existing_detail = existing.get("detail")
                if is_detail_valid(existing_detail):
                    existing_ok = True
            except Exception:
                existing_ok = False

        detail = None
        last_err = None

        for attempt in range(1, retries + 1):
            try:
                detail = fetch_training_detail(c, tid, ttype)
                if is_detail_valid(detail):
                    break
                save_debug(c, f"detail_{tid}_invalid_attempt{attempt}")
                last_err = f"Invalid/empty detail (attempt {attempt})"
            except Exception as e:
                save_debug(c, f"detail_{tid}_exception_attempt{attempt}")
                last_err = repr(e)
            time.sleep(throttle_s * attempt)

        if not is_detail_valid(detail):
            index["meta"]["count_skipped_invalid"] += 1
            index["errors"][f"detail:{tid}"] = last_err or "Invalid/empty after retries"
            # Do not overwrite a good file with invalid content
            if existing_ok:
                index["items"].append(
                    {"id": tid, "type": ttype, "date": item.get("date"), "path": f"/data/training_details/{tid}.json", "note": "kept_existing"}
                )
            continue

        # Clean + write
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

    # Conservative: only call no-arg get_* methods; skip those requiring params
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
        {
            "meta": {"generated_at": now_iso(), "mode": "reference", "errors": errors},
            "data": redact(results),
        },
    )


def main():
    ensure_dir(DATA_DIR)

    mode = _env_str("SYNC_MODE", "training").lower()

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)

    # quick sanity check: dump config credentials presence (no values)
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

    try:
        if mode == "training":
            run_training_sync(c)
        elif mode == "reference":
            run_reference_sync(c)
        else:
            raise RuntimeError("Invalid SYNC_MODE. Use 'training' or 'reference'.")
    except Exception:
        save_debug(c, f"run_failed_{mode}")
        raise


if __name__ == "__main__":
    main()

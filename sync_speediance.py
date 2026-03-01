import json
import os
import re
from datetime import datetime, timedelta, timezone

from api_client import SpeedianceClient

DATA_DIR = "data"

# Remove keys matching these patterns from public JSON
REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
    re.compile(r".*user.*id.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
    re.compile(r".*serial.*", re.IGNORECASE),
]

# Candidate keys for IDs in training record objects
TRAINING_ID_KEYS = [
    "trainingId",
    "training_id",
    "trainingInfoId",
    "trainingInfoID",
    "id",
    "recordId",
    "recordID",
]

# Candidate keys for "type" in training record objects
TYPE_KEYS = [
    "trainingType",
    "type",
    "sourceType",
    "courseType",
    "templateType",
]

# Keys that explode file size (large telemetry arrays)
# Keep weights, counts, HR, etc. Remove these arrays.
TELEMETRY_KEYS_TO_DROP = {
    "leftWatts",
    "rightWatts",
    "leftAmplitudes",
    "rightAmplitudes",
    "leftRopeSpeeds",
    "rightRopeSpeeds",
    "leftMinRopeLengths",
    "rightMinRopeLengths",
    "leftMaxRopeLengths",
    "rightMaxRopeLengths",
    "leftFinishedTimes",
    "rightFinishedTimes",
    "leftBreakTimes",
    "rightBreakTimes",
    "leftTimestamps",
    "rightTimestamps",
}

# If you later find other huge arrays, add their keys here.
# We also drop "telemetry-like" lists by name heuristics (below) when very long.
TELEMETRY_NAME_PATTERNS = [
    re.compile(r".*watts.*", re.IGNORECASE),
    re.compile(r".*amplitude.*", re.IGNORECASE),
    re.compile(r".*ropespeed.*", re.IGNORECASE),
    re.compile(r".*ropelength.*", re.IGNORECASE),
    re.compile(r".*timestamp.*", re.IGNORECASE),
    re.compile(r".*breaktime.*", re.IGNORECASE),
    re.compile(r".*finishedtime.*", re.IGNORECASE),
]

# Only apply heuristic list-dropping when list is "really long"
MAX_TELEMETRY_LIST_LEN = 12

# NEVER drop these keys even if they are lists (important for strength analysis)
ALLOW_LIST_KEYS = {"weights"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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
    """
    Remove the worst telemetry arrays to keep files small and GPT-friendly,
    while keeping key training info (reps/weights/counts/HR/pace/etc.).
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            # Hard drop: known huge telemetry arrays
            if k in TELEMETRY_KEYS_TO_DROP:
                continue

            # Recurse
            vv = prune_telemetry(v)

            # Heuristic: drop very long lists that look like telemetry by key name,
            # BUT never drop important lists like 'weights'
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
    region = _env_str("SPEEDIANCE_REGION", "Global")
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


def ensure_auth(c: SpeedianceClient) -> None:
    token = (getattr(c, "token", None) or "").strip()
    user_id = (getattr(c, "user_id", None) or "").strip()
    if token and user_id:
        return

    email = _env_str("SPEEDIANCE_EMAIL", "")
    password = _env_str("SPEEDIANCE_PASSWORD", "")
    if not email or not password:
        raise RuntimeError(
            "Missing auth. Provide SPEEDIANCE_TOKEN+SPEEDIANCE_USER_ID or SPEEDIANCE_EMAIL+SPEEDIANCE_PASSWORD as repo secrets."
        )

    ok, msg, err = c.login(email, password)
    if not ok:
        raise RuntimeError(f"Login failed: {msg}\n{err or ''}")


def list_get_methods(c: SpeedianceClient):
    return sorted(
        [name for name in dir(c) if name.startswith("get_") and callable(getattr(c, name))]
    )


def is_reference_method(name: str) -> bool:
    n = name.lower()
    markers = (
        "library",
        "exercise",
        "movement",
        "category",
        "muscle",
        "equipment",
        "tag",
        "body",
        "template",
        "plan",
    )
    return any(m in n for m in markers)


def safe_call(fn, name: str, errors: dict):
    try:
        return fn()
    except Exception as e:
        errors[name] = repr(e)
        return None


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


def fetch_training_records(c: SpeedianceClient, start_date: str, end_date: str, errors: dict):
    if hasattr(c, "get_training_records"):
        return c.get_training_records(start_date, end_date)
    errors["get_training_records"] = "Method not found on client."
    return None


def fetch_training_stats(c: SpeedianceClient, start_date: str, end_date: str, errors: dict):
    if hasattr(c, "get_training_stats"):
        return c.get_training_stats(start_date, end_date)
    return None


def fetch_training_detail(c: SpeedianceClient, training_id: str, training_type: str | None, errors: dict):
    if hasattr(c, "get_training_detail"):
        if training_type in ("course", "ctt"):
            return c.get_training_detail(training_id, training_type)

        try:
            return c.get_training_detail(training_id, "course")
        except Exception:
            try:
                return c.get_training_detail(training_id, "ctt")
            except Exception as e:
                errors[f"get_training_detail:{training_id}"] = repr(e)
                return None

    errors["get_training_detail"] = "Method not found on client."
    return None


def run_reference_sync(c: SpeedianceClient) -> None:
    errors = {}
    results = {}
    methods = list_get_methods(c)

    for name in methods:
        if not is_reference_method(name):
            continue
        fn = getattr(c, name)
        try:
            results[name] = fn()
        except TypeError:
            continue
        except Exception as e:
            errors[name] = repr(e)

    payload = {
        "meta": {
            "generated_at": now_iso(),
            "mode": "reference",
            "methods_detected": methods,
            "errors": errors,
        },
        "data": redact(results),
    }
    write_json(os.path.join(DATA_DIR, "reference.json"), payload)


def run_training_sync(c: SpeedianceClient) -> None:
    errors = {}

    days = _env_int("TRAINING_DAYS", 120)
    max_details = _env_int("MAX_TRAINING_DETAILS", 30)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    # 1) records
    try:
        records_obj = fetch_training_records(c, start_date, end_date, errors)
    except Exception as e:
        errors["get_training_records"] = repr(e)
        records_obj = None

    records_list = extract_records_list(records_obj)

    normalized = []
    for rec in records_list:
        tid = get_record_id(rec)
        if not tid:
            continue
        normalized.append(
            {
                "id": tid,
                "type": guess_record_type(rec),
                "date": get_record_date(rec),
                "raw": rec,
            }
        )

    def sort_key(x):
        return x.get("date") or ""

    normalized_sorted = sorted(normalized, key=sort_key, reverse=True)

    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {
            "meta": {
                "generated_at": now_iso(),
                "start_date": start_date,
                "end_date": end_date,
                "count": len(normalized_sorted),
            },
            "records": redact(normalized_sorted),
            "errors": errors,
        },
    )

    # 2) stats
    try:
        stats_obj = fetch_training_stats(c, start_date, end_date, errors)
    except Exception as e:
        errors["get_training_stats"] = repr(e)
        stats_obj = None

    write_json(
        os.path.join(DATA_DIR, "training_stats.json"),
        {
            "meta": {
                "generated_at": now_iso(),
                "start_date": start_date,
                "end_date": end_date,
            },
            "stats": redact(stats_obj),
            "errors": errors,
        },
    )

    # 3) details + index
    details_dir = os.path.join(DATA_DIR, "training_details")
    ensure_dir(details_dir)

    latest = normalized_sorted[:max_details]

    index = {
        "meta": {
            "generated_at": now_iso(),
            "start_date": start_date,
            "end_date": end_date,
            "count": 0,
            "max_details": max_details,
        },
        "items": [],
        "errors": {},
    }

    for item in latest:
        tid = item["id"]
        ttype = item.get("type")
        path = os.path.join(details_dir, f"{tid}.json")

        try:
            detail = fetch_training_detail(c, tid, ttype, index["errors"])
            # redact sensitive keys first, then prune telemetry arrays
            detail_clean = prune_telemetry(redact(detail))

            write_json(
                path,
                {
                    "meta": {
                        "generated_at": now_iso(),
                        "id": tid,
                        "type": ttype,
                        "date": item.get("date"),
                    },
                    "detail": detail_clean,
                },
            )

            index["items"].append(
                {
                    "id": tid,
                    "type": ttype,
                    "date": item.get("date"),
                    "path": f"/data/training_details/{tid}.json",
                }
            )
            index["meta"]["count"] += 1
        except Exception as e:
            index["errors"][f"detail:{tid}"] = repr(e)

    write_json(os.path.join(details_dir, "index.json"), index)


def main():
    ensure_dir(DATA_DIR)

    mode = _env_str("SYNC_MODE", "training").lower()

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth(c)

    if mode == "reference":
        run_reference_sync(c)
    elif mode == "training":
        run_training_sync(c)
    else:
        raise RuntimeError("Invalid SYNC_MODE. Use 'reference' or 'training'.")


if __name__ == "__main__":
    main()

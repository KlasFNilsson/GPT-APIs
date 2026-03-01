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
           

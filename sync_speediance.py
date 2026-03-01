# sync_speediance.py
#
# Purpose:
# - SYNC_MODE=reference  -> update slowly-changing reference data 1x/day (data/reference.json)
# - SYNC_MODE=training   -> update workouts + details + stats frequently (data/workouts_completed.json, data/workout_details/*, data/stats.json)
#
# Auth:
# - Preferred: SPEEDIANCE_TOKEN + SPEEDIANCE_USER_ID (as GitHub secrets)
# - Fallback:  SPEEDIANCE_EMAIL + SPEEDIANCE_PASSWORD (login each run)
#
# Notes:
# - This script writes ONLY under ./data/
# - Designed to be robust: missing secrets -> defaults; API hiccups -> errors collected, still writes output

import json
import os
import re
from datetime import datetime, timezone

from api_client import SpeedianceClient

DATA_DIR = "data"

# Keys/patterns to remove from public JSON
REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*user.*id.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
]

# Some APIs may return a "code" or "templateCode" etc.
WORKOUT_CODE_KEYS = ["code", "templateCode", "workoutCode", "id"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def redact(obj):
    """Recursively remove sensitive keys before committing to a public repo."""
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


def _env_str(name: str, default: str) -> str:
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
    """
    SpeedianceClient in api_client.py uses a config.json pattern.
    We write it in the Actions runner's workspace only (not committed).
    """
    region = _env_str("SPEEDIANCE_REGION", "Global")
    device_type = _env_int("SPEEDIANCE_DEVICE_TYPE", 1)
    allow_monster_moves = _env_bool("SPEEDIANCE_ALLOW_MONSTER_MOVES", False)
    unit = _env_int("SPEEDIANCE_UNIT", 0)  # 0=metric, 1=imperial (based on your config usage)

    token = _env_str("SPEEDIANCE_TOKEN", "")
    user_id = _env_str("SPEEDIANCE_USER_ID", "")

    # Save config (writes local config file in runner; not committed unless you add it)
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
    """
    If token/user_id exists, client can use them.
    Otherwise login using email/password (secrets).
    """
    token = getattr(c, "token", None) or ""
    user_id = getattr(c, "user_id", None) or ""

    if token.strip() and user_id.strip():
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


def safe_call(fn, name: str, errors: dict):
    try:
        return fn()
    except Exception as e:
        errors[name] = repr(e)
        return None


def list_get_methods(c: SpeedianceClient):
    return sorted(
        [name for name in dir(c) if name.startswith("get_") and callable(getattr(c, name))]
    )


def is_training_method(name: str) -> bool:
    """
    Heuristic mapping. You can refine as you see what returns useful data.
    """
    training_markers = (
        "workout",
        "calendar",
        "history",
        "record",
        "stats",
        "training",
        "action",
        "report",
        "log",
    )
    return any(m in name.lower() for m in training_markers)


def is_reference_method(name: str) -> bool:
    """
    Everything that's likely to be library/static:
    categories, exercises, movements, equipment, etc.
    """
    reference_markers = (
        "library",
        "exercise",
        "movement",
        "category",
        "muscle",
        "equipment",
        "tag",
        "body",
        "plan",
        "template",
    )
    return any(m in name.lower() for m in reference_markers)


def extract_codes(workouts_obj):
    """
    Tries to find workout codes from common response shapes.
    Returns list[str]
    """
    codes = []

    def try_add_code(d: dict):
        for k in WORKOUT_CODE_KEYS:
            v = d.get(k)
            if v is not None and str(v).strip() != "":
                codes.append(str(v))
                return

    # Common shapes: list of dicts, or dict with list under "data"/"list"/"records"
    if isinstance(workouts_obj, list):
        for w in workouts_obj:
            if isinstance(w, dict):
                try_add_code(w)
    elif isinstance(workouts_obj, dict):
        for key in ("list", "lists", "records", "items", "data", "workouts"):
            v = workouts_obj.get(key)
            if isinstance(v, list):
                for w in v:
                    if isinstance(w, dict):
                        try_add_code(w)

    # Deduplicate while preserving order
    seen = set()
    out = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def run_reference_sync(c: SpeedianceClient) -> None:
    """
    Writes: data/reference.json
    Strategy:
    - Run all get_* methods that look like reference/static AND have no args.
    - Also record detected get_* method names.
    """
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
            # requires args
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
    """
    Writes:
    - data/workouts_completed.json
    - data/workout_details/index.json
    - data/workout_details/<code>.json
    - data/stats.json
    """
    errors = {}

    # 1) Workouts list (completed/history)
    workouts = None
    if hasattr(c, "get_user_workouts"):
        workouts = safe_call(c.get_user_workouts, "get_user_workouts", errors)
    else:
        # fallback: try any method that contains workouts but no args
        for name in list_get_methods(c):
            if "workout" in name.lower():
                fn = getattr(c, name)
                try:
                    workouts = fn()
                    break
                except TypeError:
                    continue
                except Exception as e:
                    errors[name] = repr(e)

    write_json(
        os.path.join(DATA_DIR, "workouts_completed.json"),
        {"meta": {"generated_at": now_iso()}, "workouts": redact(workouts), "errors": errors},
    )

    # 2) Calendar month (optional)
    # If you have get_calendar_month(YYYY-MM), write it too (useful for "completed" filtering).
    cal = None
    if hasattr(c, "get_calendar_month"):
        try:
            ym = datetime.now(timezone.utc).strftime("%Y-%m")
            cal = c.get_calendar_month(ym)
            write_json(
                os.path.join(DATA_DIR, "calendar_this_month.json"),
                {"meta": {"generated_at": now_iso(), "year_month": ym}, "calendar": redact(cal)},
            )
        except Exception as e:
            errors["get_calendar_month"] = repr(e)

    # 3) Workout details (per code) - only for a limited number to keep repo size under control
    details_dir = os.path.join(DATA_DIR, "workout_details")
    ensure_dir(details_dir)

    codes = extract_codes(workouts)
    max_details = _env_int("MAX_WORKOUT_DETAILS", 50)  # set as secret/var if you want
    codes = codes[:max_details]

    index = {"meta": {"generated_at": now_iso(), "count": 0}, "items": [], "errors": {}}

    if hasattr(c, "get_workout_detail"):
        for code in codes:
            try:
                detail = c.get_workout_detail(str(code))
                detail_path = os.path.join(details_dir, f"{code}.json")
                write_json(
                    detail_path,
                    {"meta": {"generated_at": now_iso(), "code": code}, "detail": redact(detail)},
                )
                index["items"].append({"code": code, "path": f"/data/workout_details/{code}.json"})
                index["meta"]["count"] += 1
            except Exception as e:
                index["errors"][f"get_workout_detail:{code}"] = repr(e)
    else:
        index["errors"]["get_workout_detail"] = "Method not found on client."

    write_json(os.path.join(details_dir, "index.json"), index)

    # 4) Stats (API-provided where available)
    stats = {}

    # Try common stat methods if present
    if hasattr(c, "get_user_action_stats"):
        stats["user_action_stats"] = safe_call(c.get_user_action_stats, "get_user_action_stats", errors)

    # Include any get_* that looks like stats/training (no args) but avoid duplicating huge blobs.
    for name in list_get_methods(c):
        if not is_training_method(name):
            continue
        if name in ("get_user_workouts", "get_workout_detail", "get_calendar_month"):
            continue
        fn = getattr(c, name)
        try:
            stats[name] = fn()
        except TypeError:
            continue
        except Exception as e:
            errors[name] = repr(e)

    write_json(
        os.path.join(DATA_DIR, "stats.json"),
        {"meta": {"generated_at": now_iso(), "mode": "training"}, "stats": redact(stats), "errors": errors},
    )


def main():
    mode = _env_str("SYNC_MODE", "training").lower()

    c = SpeedianceClient()
    configure_client(c)
    ensure_auth(c)

    # Ensure data dir exists
    ensure_dir(DATA_DIR)

    if mode == "reference":
        run_reference_sync(c)
    elif mode == "training":
        run_training_sync(c)
    else:
        raise RuntimeError("Invalid SYNC_MODE. Use 'reference' or 'training'.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
sync_speediance.py

Purpose:
- Mirror Speediance training data into ./data/ for use via GitHub Pages/raw.
- Token-only auth (SPEEDIANCE_USER_ID + SPEEDIANCE_TOKEN).
- Robust detail fetching:
  - Try course, and if response is "empty-but-success" (e.g. data: []), fall back to ctt.
  - Retry/backoff + throttling.
  - Do not overwrite a previously good detail file with junk.
- Writes debug signatures into data/debug/ when detail payload is invalid.

Environment variables (GitHub Actions env):
- SPEEDIANCE_REGION (default: EU)
- SPEEDIANCE_DEVICE_TYPE (default: 1)
- SPEEDIANCE_ALLOW_MONSTER_MOVES (default: false)
- SPEEDIANCE_UNIT (default: 0)
- SPEEDIANCE_USER_ID (required)
- SPEEDIANCE_TOKEN (required)
- SYNC_MODE: training | reference (default: training)

Training tuning:
- TRAINING_DAYS (default: 365)
- MAX_TRAINING_DETAILS (default: 60)
- DETAIL_THROTTLE_SECONDS (default: 1.5)
- DETAIL_RETRIES (default: 4)
- DEBUG_SIGNATURE (default: 0) -> set to 1 to always write a signature for attempt 1
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from api_client import SpeedianceClient

DATA_DIR = "data"

# ---- Redaction (avoid publishing secrets/private identity) ----
REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*phone.*", re.IGNORECASE),
    re.compile(r".*apple.*userid.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
]

# ---- Telemetry bloat to drop ----
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
ALLOW_LIST_KEYS = {"weights"}  # keep weights arrays (useful)


# -----------------------
# Generic helpers
# -----------------------
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


# -----------------------
# Env helpers (treat empty as missing)
# -----------------------
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


# -----------------------
# Auth/config (token-only)
# -----------------------
def configure_client(c: SpeedianceClient) -> None:
    region = _env_str("SPEEDIANCE_REGION", "EU")
    device_type = _env_int("SPEEDIANCE_DEVICE_TYPE", 1)
    allow_monster_moves = _env_bool("SPEEDIANCE_ALLOW_MONSTER_MOVES", False)
    unit = _env_int("SPEEDIANCE_UNIT", 0)

    token = _env_str("SPEEDIANCE_TOKEN", "")
    user_id = _env_str("SPEEDIANCE_USER_ID", "")

    # api_client uses credentials dict internally
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


# -----------------------
# Debug helpers
# -----------------------
def debug_dir() -> str:
    d = os.path.join(DATA_DIR, "debug")
    ensure_dir(d)
    return d


def save_last_debug(c: SpeedianceClient, tag: str) -> None:
    dbg = getattr(c, "last_debug_info", None)
    if dbg:
        write_json(os.path.join(debug_dir(), f"{tag}_last_debug.json"), dbg)


def save_signature(tag: str, requested_id: str, type_used: Optional[str], detail: Any) -> None:
    sig: dict[str, Any] = {
        "generated_at": now_iso(),
        "requested_id": requested_id,
        "type_used": type_used,
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
            for k in ("actionInfoList", "actions", "actionList", "trainingActionList", "finishedReps", "finishedRepList"):
                v = data.get(k)
                if isinstance(v, list):
                    sig[f"len_{k}"] = len(v)
        elif isinstance(data, list):
            sig["len_data"] = len(data)
    elif isinstance(detail, list):
        sig["len_list"] = len(detail)
    write_json(os.path.join(debug_dir(), f"{tag}_sig.json"), sig)


# -----------------------
# Records parsing (defensive)
# -----------------------
def extract_records_list(records_obj: Any) -> list[dict]:
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


# -----------------------
# Detail: content detection + robust fallback (course <-> ctt)
# -----------------------
CONTENT_LIST_KEYS = ("actionInfoList", "actions", "actionList", "trainingActionList", "finishedReps", "finishedRepList")


def _has_content(detail: Any) -> bool:
    """
    Returns True if detail looks like it contains real workout content.
    Handles:
      - wrapper dict: {code,message,data:{...}} or {code,message,data:[...]}
      - bare list payloads (some endpoints return list)
      - bare dict with content (rare)
    """
    if detail is None:
        return False

    if isinstance(detail, list):
        return len(detail) > 0

    if not isinstance(detail, dict):
        return True  # unknown type, assume content to avoid false negatives

    # wrapper dict with data
    if "data" in detail:
        data = detail.get("data")
        if data is None:
            return False
        if isinstance(data, list):
            return len(data) > 0
        if isinstance(data, dict):
            # direct content lists
            for k in CONTENT_LIST_KEYS:
                v = data.get(k)
                if isinstance(v, list) and len(v) > 0:
                    return True
            # sometimes nested
            for vv in data.values():
                if isinstance(vv, dict):
                    for k in CONTENT_LIST_KEYS:
                        w = vv.get(k)
                        if isinstance(w, list) and len(w) > 0:
                            return True
            # non-empty dict might still be useful
            return len(data) > 0
        # any other data type
        return True

    # no data wrapper: look for content keys at top-level
    for k in CONTENT_LIST_KEYS:
        v = detail.get(k)
        if isinstance(v, list) and len(v) > 0:
            return True

    # non-empty dict: ambiguous, but accept
    return len(detail) > 0


def fetch_training_detail_best_effort(
    c: SpeedianceClient,
    training_id: str,
    type_hint: Optional[str],
    debug_tag_prefix: str,
    debug_signature: bool,
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Returns (detail, type_used).

    Strategy:
    - If type_hint is course/ctt:
        try hinted type; if empty => try the other.
    - If no hint:
        try course; if empty => try ctt.
    - If both empty, return the last response (so we can signature it) but type_used indicates last attempt.
    """
    attempts = []
    if type_hint in ("course", "ctt"):
        attempts = [type_hint, "ctt" if type_hint == "course" else "course"]
    else:
        attempts = ["course", "ctt"]

    last_detail = None
    last_type = None

    for idx, t in enumerate(attempts, start=1):
        last_type = t
        try:
            d = c.get_training_detail(training_id, t)
            last_detail = d
            if debug_signature and idx == 1:
                save_signature(f"{debug_tag_prefix}_attempt1_{t}", training_id, t, d)

            if _has_content(d):
                return d, t

            # "Success but empty" -> signature + last_debug
            save_signature(f"{debug_tag_prefix}_empty_{t}", training_id, t, d)
            save_last_debug(c, f"{debug_tag_prefix}_empty_{t}")

        except Exception:
            save_last_debug(c, f"{debug_tag_prefix}_exception_{t}")
            # continue to next attempt

    return last_detail, last_type


# -----------------------
# Sync modes
# -----------------------
def run_training_sync(c: SpeedianceClient) -> None:
    days = _env_int("TRAINING_DAYS", 365)
    max_details = _env_int("MAX_TRAINING_DETAILS", 60)

    throttle_s = float(_env_str("DETAIL_THROTTLE_SECONDS", "1.5"))
    retries = _env_int("DETAIL_RETRIES", 4)
    debug_signature = _env_bool("DEBUG_SIGNATURE", False)

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    # 1) Records
    records_obj = c.get_training_records(start_date, end_date)
    records_list = extract_records_list(records_obj)

    normalized = []
    for rec in records_list:
        tid = get_record_id(rec)
        if not tid:
            continue
        normalized.append(
            {"id": tid, "type": None, "date": get_record_date(rec), "raw": rec}
        )

    # Sort best-effort by date string
    normalized_sorted = sorted(normalized, key=lambda x: (x.get("date") or ""), reverse=True)

    write_json(
        os.path.join(DATA_DIR, "training_records.json"),
        {
            "meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date, "count": len(normalized_sorted)},
            "records": redact(normalized_sorted),
        },
    )

    # 2) Stats (optional)
    stats_obj = None
    if hasattr(c, "get_training_stats"):
        try:
            stats_obj = c.get_training_stats(start_date, end_date)
        except Exception:
            save_last_debug(c, "training_stats_failed")

    write_json(
        os.path.join(DATA_DIR, "training_stats.json"),
        {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date}, "stats": redact(stats_obj)},
    )

    # 3) Details
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
        date = item.get("date")
        type_hint = item.get("type")  # often None; we handle fallback

        out_path = os.path.join(details_dir, f"{tid}.json")

        # Detect if existing file looks contentful (so we avoid overwriting with empty)
        existing_ok = False
        if os.path.exists(out_path):
            try:
                existing = read_json(out_path)
                existing_detail = existing.get("detail")
                if _has_content(existing_detail):
                    existing_ok = True
            except Exception:
                existing_ok = False

        best_detail = None
        best_type = None
        last_err = None

        for attempt in range(1, retries + 1):
            tag = f"{tid}_attempt{attempt}"
            try:
                detail, used_type = fetch_training_detail_best_effort(
                    c=c,
                    training_id=tid,
                    type_hint=type_hint,
                    debug_tag_prefix=tag,
                    debug_signature=debug_signature,
                )
                best_detail, best_type = detail, used_type

                if _has_content(best_detail):
                    break

                last_err = f"Empty/invalid content after trying course+ctt (attempt {attempt})"
            except Exception as e:
                save_last_debug(c, f"{tag}_unexpected_exception")
                last_err = repr(e)

            # backoff
            time.sleep(throttle_s * attempt)

        if not _has_content(best_detail):
            index["meta"]["count_skipped_invalid"] += 1
            index["errors"][f"detail:{tid}"] = last_err or "Empty/invalid detail after retries"
            if existing_ok:
                index["items"].append(
                    {"id": tid, "type": best_type, "date": date, "path": f"/data/training_details/{tid}.json", "note": "kept_existing"}
                )
            else:
                # leave no detail file (avoids creating junk)
                pass
            time.sleep(throttle_s)
            continue

        # Save cleaned detail
        detail_clean = prune_telemetry(redact(best_detail))
        write_json(
            out_path,
            {
                "meta": {"generated_at": now_iso(), "id": tid, "type": best_type, "date": date},
                "detail": detail_clean,
            },
        )

        index["items"].append(
            {"id": tid, "type": best_type, "date": date, "path": f"/data/training_details/{tid}.json"}
        )
        index["meta"]["count_written"] += 1

        time.sleep(throttle_s)

    write_json(os.path.join(details_dir, "index.json"), index)

    # Also write a quick sanity file every run
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
    errors = {}
    results = {}

    for name in methods:
        fn = getattr(c, name)
        try:
            # only no-arg methods
            results[name] = fn()
        except TypeError:
            continue
        except Exception as e:
            errors[name] = repr(e)
            save_last_debug(c, f"reference_{name}_failed")

    write_json(
        os.path.join(DATA_DIR, "reference.json"),
        {"meta": {"generated_at": now_iso(), "mode": "reference", "errors": errors}, "data": redact(results)},
    )


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

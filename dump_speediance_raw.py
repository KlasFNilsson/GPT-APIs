#!/usr/bin/env python3
"""
dump_speediance_raw.py

Manual raw dumper for Speediance GET endpoints as implemented in
hbui3/UnofficialSpeedianceWorkoutManager/api_client.py.

Writes raw JSON files to disk for:
- training_records (GET userTrainingDataRecord)
- training_stats   (GET userTrainingDataStat)
- training_detail  (GET courseTrainingInfoDetail/{id} and cttTrainingInfoDetail/{id})
- training_session_info (GET courseTrainingInfo/{id})

Auth: token-only via SpeedianceClient.save_config(...) + existing api_client.py.

Usage examples:
  python dump_speediance_raw.py --start 2025-03-01 --end 2026-03-01 --max-details 80
  python dump_speediance_raw.py --days 30 --max-details 40 --skip-existing
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Requires api_client.py in same repo/folder (from hbui3 project)
from api_client import SpeedianceClient


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def is_nonempty_payload(payload: Any) -> bool:
    if payload is None:
        return False
    if isinstance(payload, dict):
        # In this client, getters typically return dict/list under 'data'
        d = payload.get("data") if "data" in payload else payload
        if isinstance(d, dict):
            return len(d) > 0
        if isinstance(d, list):
            return len(d) > 0
        return True
    if isinstance(payload, list):
        return len(payload) > 0
    return True


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def parse_ymd(s: str) -> str:
    # minimal validation
    datetime.strptime(s, "%Y-%m-%d")
    return s


def pick_record_ids(rec: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    rid = rec.get("id")
    tid = rec.get("trainingId") or rec.get("trainingInfoId")
    rid_s = str(rid).strip() if rid is not None and str(rid).strip() else None
    tid_s = str(tid).strip() if tid is not None and str(tid).strip() else None
    return rid_s, tid_s


def get_record_type(rec: Dict[str, Any]) -> Optional[int]:
    t = rec.get("type")
    if t is None:
        return None
    try:
        return int(t)
    except Exception:
        return None


def safe_filename(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in s)


def configure_client_from_env(c: SpeedianceClient) -> None:
    """
    Token-only config. Uses same fields as your sync:
      SPEEDIANCE_REGION
      SPEEDIANCE_DEVICE_TYPE
      SPEEDIANCE_ALLOW_MONSTER_MOVES
      SPEEDIANCE_UNIT
      SPEEDIANCE_TOKEN
      SPEEDIANCE_USER_ID
    """
    region = env_str("SPEEDIANCE_REGION", "EU")
    device_type = env_int("SPEEDIANCE_DEVICE_TYPE", 1)
    allow_monster_moves = env_bool("SPEEDIANCE_ALLOW_MONSTER_MOVES", False)
    unit = env_int("SPEEDIANCE_UNIT", 0)

    token = env_str("SPEEDIANCE_TOKEN", "")
    user_id = env_str("SPEEDIANCE_USER_ID", "")

    if not token or not user_id:
        raise RuntimeError(
            "Missing SPEEDIANCE_TOKEN or SPEEDIANCE_USER_ID. "
            "Set them as env vars (or GitHub Actions secrets)."
        )

    c.save_config(
        user_id=str(user_id),
        token=str(token),
        region=region,
        unit=unit,
        custom_instruction="",
        device_type=int(device_type),
        allow_monster_moves=bool(allow_monster_moves),
    )


def dump_training_records_and_stats(
    c: SpeedianceClient,
    out_dir: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """
    Writes:
      out_dir/training_records.<start>_<end>.json
      out_dir/training_stats.<start>_<end>.json

    Returns records list (raw list).
    """
    records = c.get_training_records(start_date, end_date)
    stats = c.get_training_stats(start_date, end_date)

    write_json(
        os.path.join(out_dir, f"training_records.{start_date}_{end_date}.json"),
        {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date}, "data": records},
    )
    write_json(
        os.path.join(out_dir, f"training_stats.{start_date}_{end_date}.json"),
        {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date}, "data": stats},
    )
    if not isinstance(records, list):
        return []
    return [r for r in records if isinstance(r, dict)]


def dump_training_details(
    c: SpeedianceClient,
    out_dir: str,
    records: List[Dict[str, Any]],
    max_details: int,
    throttle_s: float,
    skip_existing: bool,
) -> Dict[str, Any]:
    """
    For each record:
      - tries c.get_training_detail(training_id, 'course')
      - tries c.get_training_detail(training_id, 'ctt')
      - saves whichever returns non-empty (and saves both if you want—here: saves both attempts)
      - also saves c.get_training_session_info(training_id)

    Output:
      out_dir/details/<training_id>.course.json
      out_dir/details/<training_id>.ctt.json
      out_dir/session_info/<training_id>.json
      out_dir/index.json
    """
    details_dir = os.path.join(out_dir, "details")
    session_dir = os.path.join(out_dir, "session_info")
    ensure_dir(details_dir)
    ensure_dir(session_dir)

    index: Dict[str, Any] = {
        "meta": {
            "generated_at": now_iso(),
            "max_details": max_details,
            "throttle_seconds": throttle_s,
            "count_records_seen": len(records),
            "count_details_attempted": 0,
            "count_details_written": 0,
            "count_session_written": 0,
            "count_skipped_existing": 0,
            "count_empty_both": 0,
        },
        "items": [],
        "errors": {},
    }

    for rec in records[:max_details]:
        rid, tid = pick_record_ids(rec)
        rtype = get_record_type(rec)

        if not tid:
            continue

        base_name = safe_filename(tid)
        course_path = os.path.join(details_dir, f"{base_name}.course.json")
        ctt_path = os.path.join(details_dir, f"{base_name}.ctt.json")
        sess_path = os.path.join(session_dir, f"{base_name}.json")

        if skip_existing and os.path.exists(course_path) and os.path.exists(ctt_path) and os.path.exists(sess_path):
            index["meta"]["count_skipped_existing"] += 1
            index["items"].append(
                {
                    "training_id": tid,
                    "record_id": rid,
                    "type": rtype,
                    "skipped": True,
                    "paths": {
                        "course": f"./details/{base_name}.course.json",
                        "ctt": f"./details/{base_name}.ctt.json",
                        "session_info": f"./session_info/{base_name}.json",
                    },
                }
            )
            continue

        index["meta"]["count_details_attempted"] += 1

        # Always attempt both for debugging
        try:
            course = c.get_training_detail(tid, "course")
        except Exception as e:
            course = None
            index["errors"][f"{tid}:course"] = repr(e)

        time.sleep(throttle_s)

        try:
            ctt = c.get_training_detail(tid, "ctt")
        except Exception as e:
            ctt = None
            index["errors"][f"{tid}:ctt"] = repr(e)

        time.sleep(throttle_s)

        try:
            sess = c.get_training_session_info(tid)
        except Exception as e:
            sess = None
            index["errors"][f"{tid}:session"] = repr(e)

        # Write files (even if empty -> still useful for debugging)
        write_json(
            course_path,
            {
                "meta": {"generated_at": now_iso(), "training_id": tid, "record_id": rid, "record_type": rtype, "kind": "course"},
                "data": course,
            },
        )
        write_json(
            ctt_path,
            {
                "meta": {"generated_at": now_iso(), "training_id": tid, "record_id": rid, "record_type": rtype, "kind": "ctt"},
                "data": ctt,
            },
        )
        index["meta"]["count_details_written"] += 2

        write_json(
            sess_path,
            {
                "meta": {"generated_at": now_iso(), "training_id": tid, "record_id": rid, "record_type": rtype, "kind": "session_info"},
                "data": sess,
            },
        )
        index["meta"]["count_session_written"] += 1

        if not is_nonempty_payload(course) and not is_nonempty_payload(ctt):
            index["meta"]["count_empty_both"] += 1

        index["items"].append(
            {
                "training_id": tid,
                "record_id": rid,
                "type": rtype,
                "paths": {
                    "course": f"./details/{base_name}.course.json",
                    "ctt": f"./details/{base_name}.ctt.json",
                    "session_info": f"./session_info/{base_name}.json",
                },
                "nonempty": {"course": is_nonempty_payload(course), "ctt": is_nonempty_payload(ctt)},
            }
        )

        time.sleep(throttle_s)

    write_json(os.path.join(out_dir, "index.json"), index)
    return index


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw_dump", help="Output directory (default: data/raw_dump)")
    ap.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=None, help="Alternative to --start/--end: last N days ending today (UTC)")
    ap.add_argument("--max-details", type=int, default=60, help="How many trainings to fetch details for")
    ap.add_argument("--throttle", type=float, default=1.2, help="Seconds to sleep between API calls")
    ap.add_argument("--skip-existing", action="store_true", help="Skip if all three files exist for a training_id")
    args = ap.parse_args()

    ensure_dir(args.out)

    if args.days is not None:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=int(args.days))
        start_date = start.strftime("%Y-%m-%d")
        end_date = end.strftime("%Y-%m-%d")
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide either --days N or both --start YYYY-MM-DD --end YYYY-MM-DD")
        start_date = parse_ymd(args.start)
        end_date = parse_ymd(args.end)

    c = SpeedianceClient()
    configure_client_from_env(c)

    records = dump_training_records_and_stats(c, args.out, start_date, end_date)
    dump_training_details(
        c=c,
        out_dir=args.out,
        records=records,
        max_details=int(args.max_details),
        throttle_s=float(args.throttle),
        skip_existing=bool(args.skip_existing),
    )

    # Also dump last_debug_info (useful when things go wrong)
    try:
        write_json(os.path.join(args.out, "last_debug_info.json"), {"meta": {"generated_at": now_iso()}, "data": getattr(c, "last_debug_info", {})})
    except Exception:
        pass

    print(f"Done. Wrote raw dump to: {args.out}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise

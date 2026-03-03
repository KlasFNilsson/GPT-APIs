#!/usr/bin/env python3
"""
dump_speediance_raw_all_gets.py

Manual raw dumper for GET endpoints available in hbui3/UnofficialSpeedianceWorkoutManager/api_client.py.

Dumps:
- library
- categories
- accessories
- user_workouts + workout_detail (for each code)
- exercise_detail (for a chosen sample of ids)
- batch_details (for same ids, chunked)
- training_records + training_stats
- training_detail (course + ctt for each training_id)
- training_session_info
- user_action_stats (for chosen sample ids)
- courses_page + course_detail (for chosen sample course ids)
- programs_page + program_detail (for chosen sample plan ids)

Token-only auth: SPEEDIANCE_TOKEN + SPEEDIANCE_USER_ID (+ region/device/unit/allow).
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from api_client import SpeedianceClient


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json(path: str, payload: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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
    datetime.strptime(s, "%Y-%m-%d")
    return s


def safe_name(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in str(s))


def pick_record_ids(rec: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    rid = rec.get("id")
    tid = rec.get("trainingId") or rec.get("trainingInfoId")
    rid_s = str(rid).strip() if rid is not None and str(rid).strip() else None
    tid_s = str(tid).strip() if tid is not None and str(tid).strip() else None
    return rid_s, tid_s


def chunk(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def configure_client_from_env(c: SpeedianceClient) -> None:
    region = env_str("SPEEDIANCE_REGION", "EU")
    device_type = env_int("SPEEDIANCE_DEVICE_TYPE", 1)
    allow_monster_moves = env_bool("SPEEDIANCE_ALLOW_MONSTER_MOVES", False)
    unit = env_int("SPEEDIANCE_UNIT", 0)

    token = env_str("SPEEDIANCE_TOKEN", "")
    user_id = env_str("SPEEDIANCE_USER_ID", "")

    if not token or not user_id:
        raise RuntimeError("Missing SPEEDIANCE_TOKEN or SPEEDIANCE_USER_ID env vars/secrets.")

    c.save_config(
        user_id=str(user_id),
        token=str(token),
        region=region,
        unit=unit,
        custom_instruction="",
        device_type=int(device_type),
        allow_monster_moves=bool(allow_monster_moves),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw_dump_all", help="Output dir")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=60, help="Alternative to --start/--end")
    ap.add_argument("--max-details", type=int, default=80, help="How many trainings to dump details for")
    ap.add_argument("--throttle", type=float, default=1.2, help="Sleep seconds between calls")
    ap.add_argument("--sample-exercises", type=int, default=60, help="How many exercise IDs to sample for exercise_detail/user_action_stats")
    ap.add_argument("--sample-courses", type=int, default=40, help="How many course IDs to sample for course_detail")
    ap.add_argument("--sample-programs", type=int, default=20, help="How many plan IDs to sample for program_detail")
    args = ap.parse_args()

    ensure_dir(args.out)
    throttle = float(args.throttle)

    if args.start and args.end:
        start_date = parse_ymd(args.start)
        end_date = parse_ymd(args.end)
    else:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=int(args.days))
        start_date = start.strftime("%Y-%m-%d")
        end_date = end.strftime("%Y-%m-%d")

    c = SpeedianceClient()
    configure_client_from_env(c)

    # 1) categories
    categories = c.get_categories()
    write_json(os.path.join(args.out, "categories.json"), {"meta": {"generated_at": now_iso()}, "data": categories})
    time.sleep(throttle)

    # 2) library
    library = c.get_library()
    write_json(os.path.join(args.out, "library.json"), {"meta": {"generated_at": now_iso()}, "data": library})
    time.sleep(throttle)

    # Extract exercise ids from library if possible
    exercise_ids: List[int] = []
    if isinstance(library, list):
        for it in library:
            if isinstance(it, dict) and it.get("id") is not None:
                try:
                    exercise_ids.append(int(it["id"]))
                except Exception:
                    pass
    exercise_ids = list(dict.fromkeys(exercise_ids))[: int(args.sample_exercises)]
    write_json(os.path.join(args.out, "exercise_ids_sample.json"), {"meta": {"generated_at": now_iso()}, "ids": exercise_ids})

    # 3) accessories
    accessories = c.get_accessories()
    write_json(os.path.join(args.out, "accessories.json"), {"meta": {"generated_at": now_iso()}, "data": accessories})
    time.sleep(throttle)

    # 4) user_workouts + workout_detail
    user_workouts = c.get_user_workouts()
    write_json(os.path.join(args.out, "user_workouts.json"), {"meta": {"generated_at": now_iso()}, "data": user_workouts})
    time.sleep(throttle)

    workout_codes: List[str] = []
    if isinstance(user_workouts, list):
        for w in user_workouts:
            if isinstance(w, dict):
                code = w.get("code") or w.get("templateCode")
                if code:
                    workout_codes.append(str(code))
    workout_codes = list(dict.fromkeys(workout_codes))
    write_json(os.path.join(args.out, "workout_codes.json"), {"meta": {"generated_at": now_iso()}, "codes": workout_codes})

    wd_dir = os.path.join(args.out, "workout_detail")
    ensure_dir(wd_dir)
    for code in workout_codes:
        detail = c.get_workout_detail(code)
        write_json(os.path.join(wd_dir, f"{safe_name(code)}.json"), {"meta": {"generated_at": now_iso(), "code": code}, "data": detail})
        time.sleep(throttle)

    # 5) exercise_detail (sample)
    ed_dir = os.path.join(args.out, "exercise_detail")
    ensure_dir(ed_dir)
    for ex_id in exercise_ids:
        detail = c.get_exercise_detail(ex_id)
        write_json(os.path.join(ed_dir, f"{ex_id}.json"), {"meta": {"generated_at": now_iso(), "exercise_id": ex_id}, "data": detail})
        time.sleep(throttle)

    # 6) batch_details (same sample, chunked)
    bd_dir = os.path.join(args.out, "batch_details")
    ensure_dir(bd_dir)
    for i, ch in enumerate(chunk(exercise_ids, 50), start=1):
        details = c.get_batch_details(ch)
        write_json(os.path.join(bd_dir, f"batch_{i:03d}.json"), {"meta": {"generated_at": now_iso(), "ids": ch}, "data": details})
        time.sleep(throttle)

    # 7) training_records + training_stats
    records = c.get_training_records(start_date, end_date)
    stats = c.get_training_stats(start_date, end_date)
    write_json(os.path.join(args.out, f"training_records.{start_date}_{end_date}.json"),
              {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date}, "data": records})
    write_json(os.path.join(args.out, f"training_stats.{start_date}_{end_date}.json"),
              {"meta": {"generated_at": now_iso(), "start_date": start_date, "end_date": end_date}, "data": stats})
    time.sleep(throttle)

    # training ids from records
    training_ids: List[str] = []
    if isinstance(records, list):
        for r in records:
            if isinstance(r, dict):
                _, tid = pick_record_ids(r)
                if tid:
                    training_ids.append(tid)
    training_ids = list(dict.fromkeys(training_ids))[: int(args.max_details)]
    write_json(os.path.join(args.out, "training_ids_sample.json"), {"meta": {"generated_at": now_iso()}, "ids": training_ids})

    # 8) training_detail (course + ctt) + training_session_info
    td_dir = os.path.join(args.out, "training_detail")
    tsi_dir = os.path.join(args.out, "training_session_info")
    ensure_dir(td_dir)
    ensure_dir(tsi_dir)

    for tid in training_ids:
        # detail course
        course = c.get_training_detail(tid, "course")
        write_json(os.path.join(td_dir, f"{safe_name(tid)}.course.json"),
                  {"meta": {"generated_at": now_iso(), "training_id": tid, "kind": "course"}, "data": course})
        time.sleep(throttle)

        # detail ctt
        ctt = c.get_training_detail(tid, "ctt")
        write_json(os.path.join(td_dir, f"{safe_name(tid)}.ctt.json"),
                  {"meta": {"generated_at": now_iso(), "training_id": tid, "kind": "ctt"}, "data": ctt})
        time.sleep(throttle)

        # session info
        si = c.get_training_session_info(tid)
        write_json(os.path.join(tsi_dir, f"{safe_name(tid)}.json"),
                  {"meta": {"generated_at": now_iso(), "training_id": tid}, "data": si})
        time.sleep(throttle)

    # 9) user_action_stats (sample)
    uas_dir = os.path.join(args.out, "user_action_stats")
    ensure_dir(uas_dir)
    for ex_id in exercise_ids:
        resp = c.get_user_action_stats(ex_id, page=1, size=12)
        write_json(os.path.join(uas_dir, f"{ex_id}.json"), {"meta": {"generated_at": now_iso(), "group_id": ex_id}, "data": resp})
        time.sleep(throttle)

    # 10) courses_page + course_detail (sample)
    courses = c.get_courses_page(page=1, page_size=200)
    write_json(os.path.join(args.out, "courses_page_1.json"), {"meta": {"generated_at": now_iso()}, "data": courses})
    time.sleep(throttle)

    course_ids: List[int] = []
    if isinstance(courses, list):
        for it in courses:
            if isinstance(it, dict) and it.get("id") is not None:
                try:
                    course_ids.append(int(it["id"]))
                except Exception:
                    pass
    course_ids = list(dict.fromkeys(course_ids))[: int(args.sample_courses)]
    write_json(os.path.join(args.out, "course_ids_sample.json"), {"meta": {"generated_at": now_iso()}, "ids": course_ids})

    cd_dir = os.path.join(args.out, "course_detail")
    ensure_dir(cd_dir)
    for cid in course_ids:
        d = c.get_course_detail(cid)
        write_json(os.path.join(cd_dir, f"{cid}.json"), {"meta": {"generated_at": now_iso(), "course_id": cid}, "data": d})
        time.sleep(throttle)

    # 11) programs_page + program_detail (sample)
    programs = c.get_programs_page(page=1, page_size=200)
    write_json(os.path.join(args.out, "programs_page_1.json"), {"meta": {"generated_at": now_iso()}, "data": programs})
    time.sleep(throttle)

    plan_ids: List[int] = []
    if isinstance(programs, list):
        for it in programs:
            if isinstance(it, dict) and it.get("id") is not None:
                try:
                    plan_ids.append(int(it["id"]))
                except Exception:
                    pass
    plan_ids = list(dict.fromkeys(plan_ids))[: int(args.sample_programs)]
    write_json(os.path.join(args.out, "program_ids_sample.json"), {"meta": {"generated_at": now_iso()}, "ids": plan_ids})

    pd_dir = os.path.join(args.out, "program_detail")
    ensure_dir(pd_dir)
    for pid in plan_ids:
        d = c.get_program_detail(pid)
        write_json(os.path.join(pd_dir, f"{pid}.json"), {"meta": {"generated_at": now_iso(), "plan_id": pid}, "data": d})
        time.sleep(throttle)

    # last_debug_info
    write_json(os.path.join(args.out, "last_debug_info.json"),
              {"meta": {"generated_at": now_iso()}, "data": getattr(c, "last_debug_info", {})})

    print(f"Done. Raw GET dump written to: {args.out}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        raise

import json
import os
import re
import time
from datetime import datetime, timezone

from api_client import SpeedianceClient

DATA_DIR = "data"

# Nycklar att försöka ta bort ur publika JSON (du kan lägga till fler)
REDACT_KEY_PATTERNS = [
    re.compile(r".*token.*", re.IGNORECASE),
    re.compile(r".*password.*", re.IGNORECASE),
    re.compile(r".*email.*", re.IGNORECASE),
    re.compile(r".*user.*id.*", re.IGNORECASE),
    re.compile(r".*device.*id.*", re.IGNORECASE),
]

def redact(obj):
    """Recursivt ta bort fält som matchar mönster ovan."""
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

def write_json(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def main():
    # Init klient
    c = SpeedianceClient()

    # Konfig från Secrets/Env
    region = os.getenv("SPEEDIANCE_REGION", "Global")
    device_type = int(os.getenv("SPEEDIANCE_DEVICE_TYPE", "1"))
    allow_monster_moves = os.getenv("SPEEDIANCE_ALLOW_MONSTER_MOVES", "false").lower() in ("1","true","yes")
    unit = int(os.getenv("SPEEDIANCE_UNIT", "0"))

    # Auth: antingen TOKEN/USER_ID eller EMAIL/PASSWORD
    token = os.getenv("SPEEDIANCE_TOKEN", "").strip()
    user_id = os.getenv("SPEEDIANCE_USER_ID", "").strip()
    email = os.getenv("SPEEDIANCE_EMAIL", "").strip()
    password = os.getenv("SPEEDIANCE_PASSWORD", "").strip()

    # Sätt grundconfig i klienten (utan att skriva till disk i repo)
    # Vi skriver config.json lokalt i Actions-runnern, inte i git (workflowen committar bara /data)
    c.save_config(
        user_id=user_id,
        token=token,
        region=region,
        unit=unit,
        custom_instruction="",
        device_type=device_type,
        allow_monster_moves=allow_monster_moves,
    )

    # Om vi saknar token/user_id: logga in (behövs för att kunna göra GET-anrop)
    if not token or not user_id:
        if not email or not password:
            raise RuntimeError("Missing auth. Provide SPEEDIANCE_TOKEN+SPEEDIANCE_USER_ID or SPEEDIANCE_EMAIL+SPEEDIANCE_PASSWORD as Secrets.")
        ok, msg, err = c.login(email, password)
        if not ok:
            raise RuntimeError(f"Login failed: {msg}\n{err or ''}")

    # Vilka GET-metoder finns?
    get_method_names = sorted([
        name for name in dir(c)
        if name.startswith("get_") and callable(getattr(c, name))
    ])

    # Kör metoderna som kan köras utan argument
    results = {}
    errors = {}

    for name in get_method_names:
        fn = getattr(c, name)
        try:
            # Metoder som kräver argument hoppar vi över här
            # (vi kör dem separat längre ner)
            res = fn()
            results[name] = res
        except TypeError:
            # kräver args
            pass
        except Exception as e:
            errors[name] = str(e)

    # Metoder med obligatoriska argument (vi kör rimliga default-filer)
    # 1) get_calendar_month('YYYY-MM')
    try:
        this_month = datetime.now(timezone.utc).strftime("%Y-%m")
        results["get_calendar_month"] = c.get_calendar_month(this_month)
    except Exception as e:
        errors["get_calendar_month"] = str(e)

    # 2) get_workout_detail(code) – kräver code, så vi kan:
    #    - hämta user workouts och skriva detaljer per code om de finns
    workout_details = {}
    try:
        workouts = results.get("get_user_workouts") or c.get_user_workouts()
        # Försök hitta "code" eller "templateCode" i listan
        codes = []
        if isinstance(workouts, dict):
            # ibland ligger listan i t.ex. records/list
            pass
        if isinstance(workouts, list):
            for w in workouts:
                if isinstance(w, dict):
                    code = w.get("code") or w.get("templateCode")
                    if code:
                        codes.append(code)
        for code in codes[:50]:  # begränsa
            try:
                workout_details[str(code)] = c.get_workout_detail(str(code))
            except Exception as ee:
                errors[f"get_workout_detail:{code}"] = str(ee)
    except Exception as e:
        errors["get_workout_detail:scan"] = str(e)

    # Skriv output
    now_iso = datetime.now(timezone.utc).isoformat()
    meta = {
        "generated_at": now_iso,
        "region": region,
        "device_type": device_type,
        "allow_monster_moves": allow_monster_moves,
        "methods_detected": get_method_names,
        "errors": errors,
    }

    # Redigera innan publicering
    payload_latest = {
        "meta": meta,
        "data": redact(results),
        "workout_details": redact(workout_details),
    }

    write_json(f"{DATA_DIR}/latest.json", payload_latest)

if __name__ == "__main__":
    main()

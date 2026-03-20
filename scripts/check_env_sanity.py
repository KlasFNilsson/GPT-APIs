import json
import os
from datetime import datetime, timezone

from api_client import SpeedianceClient
from sync_speediance import configure_client, ensure_auth_token_only

OUT_PATH = os.path.join("data", "sync_env_sanity.json")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    c = SpeedianceClient()
    configure_client(c)
    ensure_auth_token_only(c)
    creds = getattr(c, "credentials", None)
    payload = {
        "meta": {"generated_at": now_iso()},
        "region": getattr(c, "region", None),
        "base_url": getattr(c, "base_url", None),
        "host": getattr(c, "host", None),
        "has_credentials_dict": isinstance(creds, dict),
        "credentials_keys": sorted(list(creds.keys())) if isinstance(creds, dict) else None,
        "has_user_id": bool(str(creds.get("user_id") or "").strip()) if isinstance(creds, dict) else False,
        "has_token": bool(str(creds.get("token") or "").strip()) if isinstance(creds, dict) else False,
    }
    os.makedirs("data", exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()

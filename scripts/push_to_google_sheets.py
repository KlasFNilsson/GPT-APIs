#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import random
import socket
import time
from pathlib import Path
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

# Increase default timeout for Google API requests
socket.setdefaulttimeout(120)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

WORKOUTS_SHEET = "Workouts"
SUMMARY_SHEET = "Summary"

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TRAINING_FLAT_CSV = DATA_DIR / "training_flat.csv"
TRAINING_RECORDS_JSON = DATA_DIR / "training_records.json"


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_service_account_info() -> dict[str, Any]:
    raw = env_required("GOOGLE_SERVICE_ACCOUNT_JSON")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from exc


def build_service():
    creds_info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def execute_with_retry(request, retries: int = 5, base_sleep: float = 2.0):
    last_exc = None
    for attempt in range(retries):
        try:
            return request.execute(num_retries=2)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries - 1:
                sleep_s = base_sleep * (2 ** attempt) + random.random()
                print(
                    f"Google Sheets request failed on attempt {attempt + 1}/{retries}: "
                    f"{type(exc).__name__}: {exc}. Retrying in {sleep_s:.1f}s..."
                )
                time.sleep(sleep_s)
    raise last_exc


def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str) -> None:
    meta = execute_with_retry(
        service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties.title",
        )
    )
    existing_titles = {
        sheet["properties"]["title"]
        for sheet in meta.get("sheets", [])
        if "properties" in sheet and "title" in sheet["properties"]
    }

    if sheet_name in existing_titles:
        return

    execute_with_retry(
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": sheet_name,
                            }
                        }
                    }
                ]
            },
        )
    )
    print(f"Created sheet: {sheet_name}")


def clear_and_write_values(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    rows: list[list[Any]],
) -> None:
    ensure_sheet_exists(service, spreadsheet_id, sheet_name)

    execute_with_retry(
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:ZZ",
            body={},
        )
    )

    execute_with_retry(
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": rows},
        )
    )

    print(f"Wrote {max(len(rows) - 1, 0)} data rows to sheet '{sheet_name}'")


def read_training_flat_rows(path: Path) -> list[list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = [row for row in reader]

    if not rows:
        return [["No data"]]

    return rows


def split_datetime(value: str) -> tuple[str, str]:
    value = (value or "").strip()
    if not value:
        return "", ""

    if "T" in value:
        date_part, time_part = value.split("T", 1)
    elif " " in value:
        date_part, time_part = value.split(" ", 1)
    else:
        return value[:10], ""

    return date_part[:10], time_part[:5]


def read_training_records_summary_rows(path: Path) -> list[list[Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("records", [])
    rows: list[list[Any]] = [
        ["Date", "Title", "Start time", "Training minutes", "Calories", "Volume", "Output"]
    ]

    for record in records:
        start_date, start_clock = split_datetime(str(record.get("startTime", "") or ""))

        if not start_date:
            fallback_date, _ = split_datetime(str(record.get("date", "") or ""))
            start_date = fallback_date

        seconds = record.get("trainingTime_sec", 0) or 0
        minutes = round(seconds / 60, 1)

        rows.append(
            [
                start_date,
                record.get("title", "") or "",
                start_clock,
                minutes,
                record.get("calorie", "") or "",
                record.get("totalCapacity", "") or "",
                record.get("totalEnergy", "") or "",
            ]
        )

    return rows


def main() -> None:
    spreadsheet_id = env_required("GOOGLE_SHEETS_SPREADSHEET_ID")
    service = build_service()

    workouts_rows = read_training_flat_rows(TRAINING_FLAT_CSV)
    summary_rows = read_training_records_summary_rows(TRAINING_RECORDS_JSON)

    clear_and_write_values(service, spreadsheet_id, WORKOUTS_SHEET, workouts_rows)
    clear_and_write_values(service, spreadsheet_id, SUMMARY_SHEET, summary_rows)

    print("Google Sheets sync complete.")


if __name__ == "__main__":
    main()

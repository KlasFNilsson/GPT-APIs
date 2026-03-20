import csv
import json
import os
from typing import List

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TRAINING_FLAT_CSV_PATH = "data/training_flat.csv"
TRAINING_RECORDS_PATH = "data/training_records.json"
WORKOUTS_SHEET = "Workouts"
SUMMARY_SHEET = "Summary"


def load_service_account_info() -> dict:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from exc


def build_sheets_service():
    info = load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def ensure_sheet_exists(service, spreadsheet_id: str, title: str) -> None:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", [])
    existing = {s.get("properties", {}).get("title") for s in sheets}
    if title in existing:
        return
    body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def clear_and_write_values(service, spreadsheet_id: str, sheet_name: str, rows: List[List[str]]) -> None:
    ensure_sheet_exists(service, spreadsheet_id, sheet_name)
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        body={},
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


def read_training_flat_rows() -> List[List[str]]:
    if not os.path.exists(TRAINING_FLAT_CSV_PATH):
        raise FileNotFoundError(f"Missing {TRAINING_FLAT_CSV_PATH}")
    with open(TRAINING_FLAT_CSV_PATH, "r", encoding="utf-8", newline="") as f:
        return [row for row in csv.reader(f)]


def split_date_time(value: str) -> tuple[str, str]:
    txt = str(value or "").strip()
    if not txt:
        return "", ""
    if " " in txt:
        date_part, time_part = txt.split(" ", 1)
        return date_part, time_part[:5]
    if "T" in txt:
        date_part, time_part = txt.split("T", 1)
        return date_part, time_part[:5]
    return txt[:10], ""


def read_summary_rows() -> List[List[str]]:
    if not os.path.exists(TRAINING_RECORDS_PATH):
        raise FileNotFoundError(f"Missing {TRAINING_RECORDS_PATH}")
    with open(TRAINING_RECORDS_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    records = payload.get("records", []) if isinstance(payload, dict) else []
    rows: List[List[str]] = [["Date", "Title", "Start time", "Training time", "Calories", "Volume", "Output"]]

    for r in records:
        if not isinstance(r, dict):
            continue
        start_date, start_clock = split_date_time(r.get("startTime", ""))
        if not start_date:
            fallback_date, _ = split_date_time(r.get("date", ""))
            start_date = fallback_date

        rows.append([
            str(start_date or ""),
            str(r.get("title", "") or ""),
            str(start_clock or ""),
            str(r.get("trainingTime_sec", "") or ""),
            str(r.get("calorie", "") or ""),
            str(r.get("totalCapacity", "") or ""),
            str(r.get("totalEnergy", "") or ""),
        ])
    return rows


def main() -> None:
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing GOOGLE_SHEETS_SPREADSHEET_ID")

    service = build_sheets_service()
    workouts_rows = read_training_flat_rows()
    summary_rows = read_summary_rows()

    clear_and_write_values(service, spreadsheet_id, WORKOUTS_SHEET, workouts_rows)
    clear_and_write_values(service, spreadsheet_id, SUMMARY_SHEET, summary_rows)

    print(f"Updated Google Sheets tabs: {WORKOUTS_SHEET} ({max(len(workouts_rows)-1, 0)} rows), {SUMMARY_SHEET} ({max(len(summary_rows)-1, 0)} rows)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
export_training_flat.py

- Reads training_flat.json (single-line or pretty; doesn't matter)
- Writes:
  1) pretty JSON (indented) for readability
  2) TSV (tab-separated) for robust spreadsheet import (commas in names are safe)

Usage examples:
  python export_training_flat.py --in training_flat.json
  python export_training_flat.py --in training_flat.json --out-tsv training_flat.tsv --out-json training_flat_pretty.json
  python export_training_flat.py --in training_flat.json --sort
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


DEFAULT_FIELDS = [
    "workout_name",
    "date",
    "exercise_name",
    "per_side",
    "sets",
    "total_reps",
    "seconds",
    "avg_weight_per_rep",
    "max_weight_per_rep",
    "total_weight",
    "error",
]


def _safe_str(v: Any) -> str:
    """Convert values to a TSV-safe string (tabs/newlines removed)."""
    if v is None:
        return ""
    s = str(v)
    # Tabs/newlines can break TSV rows; replace with spaces.
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return s


def load_training_flat_json(path: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    meta = data.get("meta", {})
    rows = data.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError("JSON does not contain a list at key 'rows'.")

    # Ensure each row is dict-like
    cleaned: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        if isinstance(r, dict):
            cleaned.append(r)
        else:
            # Keep place, but mark it as error row
            cleaned.append({"error": f"Non-dict row at index {i}: {type(r)}"})
    return meta, cleaned


def infer_fields(rows: List[Dict[str, Any]], default_fields: List[str]) -> List[str]:
    """Use DEFAULT_FIELDS first, then append any other keys seen in rows."""
    keys = set()
    for r in rows:
        keys.update(r.keys())
    # Preserve order: defaults first, then extras sorted
    fields = [f for f in default_fields if f in keys]
    extras = sorted([k for k in keys if k not in fields])
    return fields + extras


def write_pretty_json(out_path: Path, meta: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    payload = {"meta": meta, "rows": rows}
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_tsv(out_path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(
            f,
            delimiter="\t",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
            escapechar="\\",
        )
        writer.writerow(fields)
        for r in rows:
            writer.writerow([_safe_str(r.get(col)) for col in fields])


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_path", required=True, help="Input training_flat.json")
    p.add_argument(
        "--out-json",
        dest="out_json",
        default=None,
        help="Output pretty JSON path (default: <in>_pretty.json)",
    )
    p.add_argument(
        "--out-tsv",
        dest="out_tsv",
        default=None,
        help="Output TSV path (default: <in>.tsv)",
    )
    p.add_argument(
        "--sort",
        action="store_true",
        help="Sort rows by date then workout_name then exercise_name (stable).",
    )
    p.add_argument(
        "--fields",
        default="",
        help=(
            "Comma-separated list of fields to export. "
            "If omitted, uses defaults + any extra keys found."
        ),
    )
    args = p.parse_args()

    in_path = Path(args.in_path)
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    meta, rows = load_training_flat_json(in_path)

    # Optional sort
    if args.sort:
        def sort_key(r: Dict[str, Any]) -> Tuple[str, str, str]:
            # Treat missing as empty strings
            return (
                _safe_str(r.get("date")),
                _safe_str(r.get("workout_name")),
                _safe_str(r.get("exercise_name")),
            )
        rows = sorted(rows, key=sort_key)

    # Determine fields
    if args.fields.strip():
        fields = [f.strip() for f in args.fields.split(",") if f.strip()]
    else:
        fields = infer_fields(rows, DEFAULT_FIELDS)

    out_json = Path(args.out_json) if args.out_json else in_path.with_name(in_path.stem + "_pretty.json")
    out_tsv = Path(args.out_tsv) if args.out_tsv else in_path.with_suffix(".tsv")

    write_pretty_json(out_json, meta, rows)
    write_tsv(out_tsv, rows, fields)

    print(f"Wrote pretty JSON: {out_json}")
    print(f"Wrote TSV:         {out_tsv}")
    print(f"Rows: {len(rows)}  Fields: {len(fields)}")


if __name__ == "__main__":
    main()

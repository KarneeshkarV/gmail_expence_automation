#!/usr/bin/env python3
"""Sync HDFC credit card and UPI expenses from Gmail to Google Sheets using gog."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_STATE_FILE = ".hdfc_sync_state.json"
DEFAULT_TAB = "Transactions"
DEFAULT_LOOKBACK_DAYS = 90


@dataclass
class Config:
    gog_account: Optional[str]
    spreadsheet_id: Optional[str]
    spreadsheet_title: str
    tab_name: str
    lookback_days: int
    state_file: Path
    dry_run: bool


def run_gog(args: List[str], gog_account: Optional[str]) -> Dict[str, Any]:
    cmd = ["gog"]
    if gog_account:
        cmd.extend(["--account", gog_account])
    cmd.extend(args)
    cmd.append("--json")

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"gog command failed: {' '.join(cmd)}\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
        )

    if not proc.stdout.strip():
        return {}

    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON from gog: {exc}\nOutput: {proc.stdout}"
        ) from exc


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"processed_message_ids": []}

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"processed_message_ids": []}


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def find_spreadsheet_id(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        if "spreadsheetId" in payload and isinstance(payload["spreadsheetId"], str):
            return payload["spreadsheetId"]
        for value in payload.values():
            found = find_spreadsheet_id(value)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_spreadsheet_id(item)
            if found:
                return found
    return None


def get_header(message: Dict[str, Any], name: str) -> str:
    headers = message.get("payload", {}).get("headers", [])
    for item in headers:
        if item.get("name", "").lower() == name.lower():
            return str(item.get("value", "")).strip()
    return ""


def as_float(amount_text: str) -> float:
    return float(amount_text.replace(",", "").strip())


def is_expense_candidate(subject: str, snippet: str) -> bool:
    text = f"{subject} {snippet}".lower()

    expense_keywords = ["debited", "spent", "purchase", "upi txn"]
    income_keywords = ["credited", "refund", "reversal", "cashback", "reversed"]

    has_expense = any(word in text for word in expense_keywords)
    has_income = any(word in text for word in income_keywords)
    return has_expense and not has_income


def parse_credit_card(subject: str, snippet: str) -> Optional[Dict[str, Any]]:
    text = f"{subject} {snippet}"

    amount_match = re.search(r"Rs\.?\s*([0-9,]+(?:\.[0-9]{1,2})?)", text, re.IGNORECASE)
    card_match = re.search(r"(?:ending|\*\*)(\d{2,4})", text, re.IGNORECASE)
    merchant_match = re.search(
        r"(?:towards|at)\s+(.+?)\s+on\s+", snippet, re.IGNORECASE
    )
    date_match = re.search(
        r"on\s+([0-9]{1,2}\s+[A-Za-z]{3},\s*[0-9]{4})(?:\s+at\s+([0-9:]{5,8}))?",
        snippet,
        re.IGNORECASE,
    )

    if not amount_match:
        return None

    if "credit card" not in text.lower():
        return None

    txn_date = ""
    if date_match:
        txn_date = date_match.group(1)
        if date_match.group(2):
            txn_date = f"{txn_date} {date_match.group(2)}"

    return {
        "amount": as_float(amount_match.group(1)),
        "mode": "credit_card",
        "merchant_or_payee": merchant_match.group(1).strip() if merchant_match else "",
        "account_or_card": f"**{card_match.group(1)}" if card_match else "",
        "reference_no": "",
        "txn_date": txn_date,
    }


def parse_upi(snippet: str, subject: str) -> Optional[Dict[str, Any]]:
    text = f"{subject} {snippet}".lower()
    if "upi" not in text and "vpa" not in text:
        return None

    main = re.search(
        r"Rs\.?\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+has\s+been\s+debited\s+from\s+account\s+\**(\d{2,4})\s+to\s+VPA\s+(.+?)\s+on\s+([0-9]{2}-[0-9]{2}-[0-9]{2})",
        snippet,
        re.IGNORECASE,
    )
    if not main:
        return None

    ref_match = re.search(
        r"(?:reference\s+number\s+is|UTR\s*(?:number)?\s*is)\s*([A-Za-z0-9]+)",
        snippet,
        re.IGNORECASE,
    )

    return {
        "amount": as_float(main.group(1)),
        "mode": "upi",
        "merchant_or_payee": main.group(3).strip(),
        "account_or_card": f"**{main.group(2)}",
        "reference_no": ref_match.group(1) if ref_match else "",
        "txn_date": main.group(4),
    }


def parse_transaction(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    message_id = message.get("id", "")
    snippet = message.get("snippet", "")
    subject = get_header(message, "Subject")
    from_header = get_header(message, "From")

    if "hdfcbank" not in from_header.lower():
        return None

    if not is_expense_candidate(subject, snippet):
        return None

    parsed = parse_credit_card(subject, snippet)
    if not parsed:
        parsed = parse_upi(snippet, subject)

    if not parsed:
        return None

    parsed["message_id"] = message_id
    parsed["subject"] = subject
    parsed["snippet"] = snippet
    parsed["synced_at"] = datetime.now(timezone.utc).isoformat()

    if not parsed.get("txn_date"):
        parsed["txn_date"] = get_header(message, "Date")

    return parsed


def search_threads(config: Config) -> List[Dict[str, Any]]:
    query = (
        "(from:alerts@hdfcbank.net OR from:alerts@hdfcbank.com) "
        '("Credit Card" OR "UPI txn" OR debited OR transaction) '
        f"newer_than:{config.lookback_days}d"
    )
    payload = run_gog(["gmail", "search", query, "--max", "200"], config.gog_account)
    return payload.get("threads") or []


def get_thread_messages(config: Config, thread_id: str) -> List[Dict[str, Any]]:
    payload = run_gog(["gmail", "thread", "get", thread_id], config.gog_account)
    return payload.get("thread", {}).get("messages", [])


def ensure_sheet(config: Config, state: Dict[str, Any]) -> str:
    spreadsheet_id = config.spreadsheet_id or state.get("spreadsheet_id")
    if spreadsheet_id:
        return spreadsheet_id

    if config.dry_run:
        return "DRY_RUN_SPREADSHEET_ID"

    created = run_gog(
        ["sheets", "create", config.spreadsheet_title], config.gog_account
    )
    spreadsheet_id = find_spreadsheet_id(created)
    if not spreadsheet_id:
        raise RuntimeError(
            f"Could not determine spreadsheet ID from create response: {created}"
        )

    headers = [
        [
            "txn_date",
            "amount",
            "mode",
            "merchant_or_payee",
            "account_or_card",
            "reference_no",
            "subject",
            "message_id",
            "snippet",
            "synced_at",
        ]
    ]

    run_gog(
        [
            "sheets",
            "update",
            spreadsheet_id,
            f"{config.tab_name}!A1:J1",
            "--values-json",
            json.dumps(headers),
            "--input",
            "USER_ENTERED",
        ],
        config.gog_account,
    )

    state["spreadsheet_id"] = spreadsheet_id
    return spreadsheet_id


def append_rows(
    config: Config, spreadsheet_id: str, transactions: List[Dict[str, Any]]
) -> None:
    if not transactions:
        return

    values = []
    for t in transactions:
        values.append(
            [
                t.get("txn_date", ""),
                t.get("amount", ""),
                t.get("mode", ""),
                t.get("merchant_or_payee", ""),
                t.get("account_or_card", ""),
                t.get("reference_no", ""),
                t.get("subject", ""),
                t.get("message_id", ""),
                t.get("snippet", ""),
                t.get("synced_at", ""),
            ]
        )

    if config.dry_run:
        print(json.dumps(values, indent=2))
        return

    run_gog(
        [
            "sheets",
            "append",
            spreadsheet_id,
            f"{config.tab_name}!A:J",
            "--values-json",
            json.dumps(values),
            "--input",
            "USER_ENTERED",
            "--insert",
            "INSERT_ROWS",
        ],
        config.gog_account,
    )


def build_config(args: argparse.Namespace) -> Config:
    state_path = (
        Path(os.getenv("STATE_FILE", DEFAULT_STATE_FILE)).expanduser().resolve()
    )
    return Config(
        gog_account=os.getenv("GOG_ACCOUNT") or None,
        spreadsheet_id=os.getenv("SPREADSHEET_ID") or None,
        spreadsheet_title=os.getenv("SPREADSHEET_TITLE", "HDFC Expenses"),
        tab_name=os.getenv("TRANSACTIONS_TAB", DEFAULT_TAB),
        lookback_days=int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS))),
        state_file=state_path,
        dry_run=args.dry_run,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync HDFC expenses from Gmail to Google Sheets"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print parsed rows without writing"
    )
    args = parser.parse_args()

    config = build_config(args)
    state = load_state(config.state_file)
    processed_ids = set(state.get("processed_message_ids", []))
    newly_seen_ids: set[str] = set()

    threads = search_threads(config)
    transactions: List[Dict[str, Any]] = []

    for thread in threads:
        thread_id = thread.get("id")
        if not thread_id:
            continue

        messages = get_thread_messages(config, thread_id)
        for message in messages:
            message_id = message.get("id")
            if not message_id or message_id in processed_ids:
                continue

            parsed = parse_transaction(message)
            newly_seen_ids.add(message_id)
            if parsed:
                transactions.append(parsed)

    spreadsheet_id = ensure_sheet(config, state)
    append_rows(config, spreadsheet_id, transactions)

    if not config.dry_run:
        processed_ids.update(newly_seen_ids)
        state["processed_message_ids"] = sorted(processed_ids)[-20000:]
        if spreadsheet_id and spreadsheet_id != "DRY_RUN_SPREADSHEET_ID":
            state["spreadsheet_id"] = spreadsheet_id
        save_state(config.state_file, state)

    print(
        f"Scanned threads: {len(threads)} | Parsed new expenses: {len(transactions)} | "
        f"Spreadsheet: {spreadsheet_id}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)

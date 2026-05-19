#!/usr/bin/env python3
"""Sync HDFC credit card and UPI expenses from Gmail to Google Sheets using gog."""

from __future__ import annotations

import argparse
import email.utils
import json
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_STATE_FILE = ".hdfc_sync_state.json"
DEFAULT_UNPARSED_FILE = ".hdfc_unparsed.json"
DEFAULT_DEBUG_UNPARSED_FILE = ".hdfc_unparsed_debug.json"
DEFAULT_TAB = "Transactions"
DEFAULT_LOOKBACK_DAYS = 90

# Subjects/snippets that look transactional but never represent a real spend.
# These keep promotional and account-service mail out of the "failed to parse"
# alert stream so notifications stay signal, not ad noise.
NON_TXN_MARKERS: List[str] = [
    "otp", "one time password", "e-mandate", "emandate", "mandate",
    "pin change", "card usage settings", "secure usage tips",
    "relationship manager", "missed call", "statement", "e-statement",
    "minimum amount due", "total amount due", "bill generated", "due date",
    "pre-approved", "pre approved", "reward point", "rewards point",
    "offer", "voucher", "% off", "flat ", "cashback offer", "discount",
    "lucky", "congratulations", "personal loan", "loan offer",
    "limit increase", "upgrade your", "exclusive", "introducing",
    "registering", "registration", "successfully read",
]

# ── Auto-categorisation rules ────────────────────────────────────────────────
# Each rule is (category, [keywords]).  First match wins (order matters).
CATEGORY_RULES: List[tuple] = [
    ("Food", [
        "zomato", "swiggy", "eatclub", "blinkit", "dominos", "pizza", "burger",
        "hotel", "restaurant", "navarasa", "udupi", "bhavan", "dharthi",
        "corner house", "sampoorna", "food", "bakery", "cafe", "coffee", "nabeel m",
        "indicafe", "grounded", "chai", "taco bell", "nagercoil catering",
        "nutberry", "paati veedu", "ristara", "samosaparty", "curefoods",
        "grub group", "my bake", "bistro", "lemon tree", "munchmart",
        "naidu garu", "discover food", "royal feast", "annalakshmi",
        "bae and chill", "boho", "lillys", "eatalios", "organic creamery",
        "t3 chat",
        "jas delicacies", "eternal limited", "ovenly", "tera bites",
        "tvk casual dine",
    ]),
    ("Transport", [
        "redbus", "rapido", "uber", "ola", "metro", "bmtc", "bus",
        "fuel", "petro", "petroleum", "hpcl", "iocl", "bpcl",
        "filling station", "makemytrip", "balasubramanian auto",
        "roppen transport", "toll",
    ]),
    ("Medical", [
        "medic", "venus medic", "krishna medic", "pharma", "hospital",
        "clinic", "apollo", "health",
    ]),
    ("Subscriptions", [
        "x corp", "claude.ai", "openai", "chatgpt", "google play",
        "google india digital", "google india service", "google cloud",
        "stripe-z.ai", "eversub", "soic", "raz*soic", "linkedin",
        "airtel", "jio", "netflix", "spotify", "amazon prime",
    ]),
    ("Rent", [
        "rentok", "eazyapp", "eazypg", "sandhya p g", "sandhyapg",
        "eqaro",
    ]),
    ("Investment", [
        "wint wealth", "wintwealth", "zerodha", "groww", "mutual fund",
        "association of mutual", "national institute of", "nism",
    ]),
    ("Entertainment", [
        "bookmyshow", "amoeba", "pvr", "inox",
        "deva__darts", "deva_darts",
    ]),
    ("Fitness / Self Improvement", [
        "cult.fit", "cultfit", "cure.fit", "curefit",
        "gym", "fitness", "yoga", "decathlon",
        "udemy", "coursera", "skillshare",
        "techmash", "badminton",
    ]),
    ("Shopping", [
        "amazon", "flipkart", "myntra", "meesho", "reliance",
        "sri kumaran", "silks", "van heusen", "vishal mega",
        "maharaja mens", "merin shopping", "duty free", "lulu",
        "sobana", "alamelu", "kavithaa", "chandra agency",
        "sai enterprise", "skb agency", "smt agencies",
        "ranjith", "geetham", "veera siva", "lakshmi computer",
        "elayaraja", "reliance digital", "datosmind", "minetech",
        "vendolite", "dhanush store", "neels super",
        "brida home", "durga leather",
    ]),
    ("Groceries", [
        "kpn farm", "kpn ff", "thefarmer", "farm fresh",
        "sowbhagya", "sri jayadurga",
        "patel trading", "provission store", "provision store",
    ]),
    ("Fuel", [
        "aboorva glo", "srinivasa service", "platinum petro",
    ]),
    ("Personal Transfer", [
        "veera020204", "vel murugan", "somasundaram",
        "avneesh kumar", "deepak prakash", "jayanth srinivasan",
        "jeeva svithra", "avish vijay", "anirudh raman",
        "achyut narayan", "md sajjad", "nitheesh bharadwaj",
        "nadimpalli nitya", "shivani balasubra", "shivani narayan",
        "sairam b", "m prasanna venkat", "nithya r", "tarun b",
        "prince kumar", "ashish ram",
        "sandhya pg", "sandhyapg", "mr m deepak", "bharath k",
        "fidusachatesvit", "venkatesh r", "suriyamoorthi",
        "r aadithya", "abdul rahim", "nasurutheen",
    ]),
    ("Food", [
        "sri matha vaibhava", "sri guru raghavendra", "mc donalds",
        "saurav ventures", "dotpe",
    ]),
    ("Shopping", [
        "selvin store", "sgp shriiwisdom", "vm group", "asspl",
        "cred club",
    ]),
    ("Transport", [
        "cmrl", "chennai metro",
    ]),
    ("Subscriptions", [
        "tamizkumaran", "corequest",
    ]),
    ("Grooming", [
        "saloon", "salon", "just trim", "barber",
    ]),
    ("Travel", [
        "hosteller", "hostel", "oyo", "airbnb", "treebo", "fabhotels",
    ]),
    ("Misc", [
        "airplaza retail", "brokentusk", "lavanya enterprises",
        "mas forum", "victorious",
    ]),
]

MONTHLY_BUDGET: Dict[str, float] = {
    "Food": 3500,
    "Shopping": 3000,
    "Transport": 1500,
    "Subscriptions": 800,
    "Medical": 500,
    "Entertainment": 500,
    "Groceries": 1000,
    "Fuel": 1500,
    "Rent": 13000,
    "Investment": 20000,
    "Personal Transfer": 5000,
    "Grooming": 500,
    "Travel": 2000,
    "Misc": 500,
}


def categorize_merchant(merchant: str, snippet: str = "") -> str:
    """Return a category tag for *merchant* using keyword rules."""
    text = f"{merchant} {snippet}".lower()
    for category, keywords in CATEGORY_RULES:
        if any(kw in text for kw in keywords):
            return category
    return ""


@dataclass
class Config:
    gog_account: Optional[str]
    spreadsheet_id: Optional[str]
    spreadsheet_title: str
    tab_name: str
    lookback_days: int
    state_file: Path
    unparsed_file: Path
    debug_unparsed_file: Path
    dry_run: bool


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if key not in os.environ:
            os.environ[key] = value


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


def normalize_txn_date(value: str) -> str:
    raw = value.strip()
    # Gmail truncates snippets, so a cut-off time can leave a dangling
    # separator like "14 May, 2026 10:09:" — trim trailing colons/spaces.
    raw = raw.rstrip(": ")
    if not raw:
        return ""

    patterns = [
        ("%d-%m-%y", "%Y-%m-%d"),
        ("%d-%m-%Y", "%Y-%m-%d"),
        ("%d %b, %Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"),
        ("%d %b %Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"),
        ("%d %b, %Y %H:%M", "%Y-%m-%d %H:%M:%S"),
        ("%d %b %Y %H:%M", "%Y-%m-%d %H:%M:%S"),
        ("%d %b, %Y", "%Y-%m-%d"),
        ("%d %b %Y", "%Y-%m-%d"),
    ]

    for pattern, output_pattern in patterns:
        try:
            return datetime.strptime(raw, pattern).strftime(output_pattern)
        except ValueError:
            continue

    parsed_email_date = email.utils.parsedate_to_datetime(raw)
    if parsed_email_date:
        if parsed_email_date.tzinfo:
            parsed_email_date = parsed_email_date.astimezone(timezone.utc)
        return parsed_email_date.strftime("%Y-%m-%d %H:%M:%S")

    return raw


def to_sheet_txn_date(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""
    return f"'{cleaned}"


def is_expense_candidate(subject: str, snippet: str) -> bool:
    text = f"{subject} {snippet}".lower()

    if any(marker in text for marker in NON_TXN_MARKERS):
        return False

    expense_keywords = ["debited", "spent", "purchase", "upi txn"]
    income_keywords = ["credited", "refund", "reversal", "cashback", "reversed"]

    has_expense = any(word in text for word in expense_keywords)
    has_income = any(word in text for word in income_keywords)
    return has_expense and not has_income


def unparsed_reason(subject: str, snippet: str) -> str:
    """Classify why an HDFC mail did not yield a transaction (for debugging)."""
    if is_expense_candidate(subject, snippet):
        return "expense_unparsed"
    text = f"{subject} {snippet}".lower()
    if any(marker in text for marker in NON_TXN_MARKERS):
        return "non_txn_marker"
    if any(word in text for word in ("credited", "refund", "reversal", "cashback")):
        return "income_or_reversal"
    return "no_expense_keyword"


def parse_credit_card(subject: str, snippet: str) -> Optional[Dict[str, Any]]:
    text = f"{subject} {snippet}"

    amount_match = re.search(r"Rs\.?\s*([0-9,]+(?:\.[0-9]{1,2})?)", text, re.IGNORECASE)
    card_match = re.search(r"(?:ending\s+|\*\*)(\d{2,4})", text, re.IGNORECASE)
    merchant_match = re.search(
        r"\b(?:towards|at)\b\s+(.+?)\s+on\s+", snippet, re.IGNORECASE
    )
    date_match = re.search(
        r"on\s+([0-9]{1,2}\s+[A-Za-z]{3},\s*[0-9]{4})"
        r"(?:\s+at\s+([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?))?",
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
        txn_date = normalize_txn_date(txn_date)

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

    # Matches both the legacy wording
    #   "Rs.X has been debited from account **8727 to VPA vpa NAME on DD-MM-YY"
    # and the current wording
    #   "Rs.X is debited from your account ending 8727 towards VPA vpa (NAME) on DD-MM-YY"
    main = re.search(
        r"Rs\.?\s*([0-9,]+(?:\.[0-9]{1,2})?)\s+"
        r"(?:has\s+been|is)\s+debited\s+from\s+(?:your\s+)?account\s+"
        r"(?:ending\s+)?\**\s*(\d{2,4})\s+"
        r"(?:to|towards)\s+VPA\s+(\S+)"
        r"(?:\s+\(([^)]+)\)|\s+([^(]+?))?"
        r"\s+on\s+([0-9]{2}-[0-9]{2}-[0-9]{2})",
        snippet,
        re.IGNORECASE,
    )
    if not main:
        return None

    ref_match = re.search(
        r"(?:reference\s+number\s+is|UTR\s*(?:number)?\s*is"
        r"|reference\s+no\.?\s*:?)\s*([A-Za-z0-9]+)",
        snippet,
        re.IGNORECASE,
    )

    entity = (main.group(4) or main.group(5) or "").strip()

    return {
        "amount": as_float(main.group(1)),
        "mode": "upi",
        "merchant_or_payee": entity if entity else main.group(3).strip(),
        "account_or_card": f"**{main.group(2)}",
        "reference_no": ref_match.group(1) if ref_match else "",
        "txn_date": normalize_txn_date(main.group(6)),
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
        parsed["txn_date"] = normalize_txn_date(get_header(message, "Date"))

    return parsed


def search_threads(config: Config) -> List[Dict[str, Any]]:
    query = (
        "(from:alerts@hdfcbank.net OR from:alerts@hdfcbank.com OR from:alerts@hdfcbank.bank.in) "
        '("Credit Card" OR "UPI txn" OR debited OR transaction) '
        f"newer_than:{config.lookback_days}d"
    )
    payload = run_gog(["gmail", "search", query, "--max", "1000"], config.gog_account)
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
        [
            "sheets",
            "create",
            config.spreadsheet_title,
            "--sheets",
            config.tab_name,
        ],
        config.gog_account,
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
            "tag",
        ]
    ]

    run_gog(
        [
            "sheets",
            "update",
            spreadsheet_id,
            f"{config.tab_name}!A1:K1",
            "--values-json",
            json.dumps(headers),
            "--input",
            "USER_ENTERED",
        ],
        config.gog_account,
    )

    state["spreadsheet_id"] = spreadsheet_id
    return spreadsheet_id


def resolve_tab_name(config: Config, spreadsheet_id: str) -> str:
    metadata = run_gog(["sheets", "metadata", spreadsheet_id], config.gog_account)
    tabs = []
    for sheet in metadata.get("sheets", []):
        title = sheet.get("properties", {}).get("title")
        if isinstance(title, str) and title:
            tabs.append(title)

    if config.tab_name in tabs:
        return config.tab_name
    if tabs:
        return tabs[0]
    return config.tab_name


def ensure_header_row(config: Config, spreadsheet_id: str, tab_name: str) -> None:
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
            "tag",
        ]
    ]

    run_gog(
        [
            "sheets",
            "update",
            spreadsheet_id,
            f"{tab_name}!A1:K1",
            "--values-json",
            json.dumps(headers),
            "--input",
            "USER_ENTERED",
        ],
        config.gog_account,
    )


def read_sheet_values(config: Config, spreadsheet_id: str, tab_name: str) -> List[List[str]]:
    """Read all values from the sheet (header + data rows)."""
    try:
        payload = run_gog(
            ["sheets", "get", spreadsheet_id, f"{tab_name}!A:K"],
            config.gog_account,
        )
        return payload.get("values", [])
    except RuntimeError:
        return []


TAGGING_TAB = "Tagging"


def read_tagging_tab(config: Config, spreadsheet_id: str) -> List[List[str]]:
    try:
        payload = run_gog(
            ["sheets", "get", spreadsheet_id, f"{TAGGING_TAB}!A:E"],
            config.gog_account,
        )
        return payload.get("values", [])
    except RuntimeError:
        return []


def pull_tags_from_tagging(
    config: Config, spreadsheet_id: str, tab_name: str
) -> int:
    """Apply user-edited tags from Tagging back to Sheet1 (matched by message_id).

    Incremental: only column K is rewritten; other columns untouched.
    """
    tagging_rows = read_tagging_tab(config, spreadsheet_id)
    if len(tagging_rows) < 2:
        return 0

    user_tags: Dict[str, str] = {}
    for row in tagging_rows[1:]:
        if len(row) < 5:
            continue
        mid = row[4].strip()
        tag = row[3].strip()
        if mid and tag:
            user_tags[mid] = tag
    if not user_tags:
        return 0

    sheet_rows = read_sheet_values(config, spreadsheet_id, tab_name)
    if len(sheet_rows) < 2:
        return 0

    updated = 0
    new_col_k: List[List[str]] = []
    for row in sheet_rows[1:]:
        while len(row) < 11:
            row.append("")
        mid = row[7].strip()
        existing_tag = row[10].strip()
        if mid in user_tags and user_tags[mid] != existing_tag:
            new_col_k.append([user_tags[mid]])
            updated += 1
        else:
            new_col_k.append([existing_tag])

    if updated and not config.dry_run:
        last = len(sheet_rows)
        run_gog(
            [
                "sheets", "update", spreadsheet_id,
                f"{tab_name}!K2:K{last}",
                "--values-json", json.dumps(new_col_k),
                "--input", "USER_ENTERED",
            ],
            config.gog_account,
        )
    return updated


def push_to_tagging(config: Config, spreadsheet_id: str, tab_name: str) -> int:
    """Append rows in Sheet1 that aren't yet in Tagging (matched by message_id).

    Incremental: never clears Tagging — preserves any manual tags or notes.
    """
    tagging_rows = read_tagging_tab(config, spreadsheet_id)
    existing_mids = set()
    for row in tagging_rows[1:] if len(tagging_rows) > 1 else []:
        if len(row) >= 5 and row[4].strip():
            existing_mids.add(row[4].strip())

    sheet_rows = read_sheet_values(config, spreadsheet_id, tab_name)
    if len(sheet_rows) < 2:
        return 0

    new_rows: List[List[Any]] = []
    for row in sheet_rows[1:]:
        while len(row) < 11:
            row.append("")
        mid = row[7].strip()
        if not mid or mid in existing_mids:
            continue
        date = str(row[0]).lstrip("'").strip()[:10]
        merchant = row[3]
        try:
            amount: Any = float(str(row[1]).replace(",", ""))
        except ValueError:
            amount = row[1]
        tag = row[10]
        new_rows.append([date, merchant, amount, tag, mid])

    if not new_rows:
        return 0
    if config.dry_run:
        return len(new_rows)

    try:
        run_gog(
            [
                "sheets", "append", spreadsheet_id, f"{TAGGING_TAB}!A1",
                "--values-json", json.dumps(new_rows),
                "--input", "USER_ENTERED",
            ],
            config.gog_account,
        )
    except RuntimeError as exc:
        print(f"  (skipped Tagging append: {exc})", file=sys.stderr)
        return 0
    return len(new_rows)


def build_merchant_tag_map(rows: List[List[str]]) -> Dict[str, str]:
    """Return merchant -> most-used tag from existing sheet rows (skips header)."""
    merchant_tags: Dict[str, Counter] = {}
    for row in rows[1:]:  # skip header
        merchant = row[3].strip() if len(row) > 3 else ""
        tag = row[10].strip() if len(row) > 10 else ""
        if merchant and tag:
            merchant_tags.setdefault(merchant, Counter())[tag] += 1
    return {m: c.most_common(1)[0][0] for m, c in merchant_tags.items()}


def write_transactions(
    config: Config,
    spreadsheet_id: str,
    tab_name: str,
    new_transactions: List[Dict[str, Any]],
    existing_rows: List[List[str]],
) -> None:
    """Append new transactions to Sheet1. Existing rows are never touched."""
    new_rows = []
    for t in new_transactions:
        new_rows.append(
            [
                to_sheet_txn_date(t.get("txn_date", "")),
                t.get("amount", ""),
                t.get("mode", ""),
                t.get("merchant_or_payee", ""),
                t.get("account_or_card", ""),
                t.get("reference_no", ""),
                t.get("subject", ""),
                t.get("message_id", ""),
                t.get("snippet", ""),
                t.get("synced_at", ""),
                t.get("tag", ""),
            ]
        )

    if config.dry_run:
        print(json.dumps(new_rows, indent=2))
        return
    if not new_rows:
        return

    run_gog(
        [
            "sheets", "append", spreadsheet_id,
            f"{tab_name}!A1",
            "--values-json", json.dumps(new_rows),
            "--input", "USER_ENTERED",
        ],
        config.gog_account,
    )


def retag_sheet(config: Config, force: bool = False) -> int:
    """Re-categorise rows in the sheet using the rules engine.

    When force=True, overwrites existing tags too (not just blank ones).
    """
    state = load_state(config.state_file)
    spreadsheet_id = config.spreadsheet_id or state.get("spreadsheet_id")
    if not spreadsheet_id:
        print("No spreadsheet ID configured.", file=sys.stderr)
        return 1

    tab_name = resolve_tab_name(config, spreadsheet_id)
    rows = read_sheet_values(config, spreadsheet_id, tab_name)
    if len(rows) < 2:
        print("Sheet has no data rows.")
        return 0

    updated = 0
    new_col_k: List[List[str]] = []
    for row in rows[1:]:
        merchant = row[3].strip() if len(row) > 3 else ""
        snippet = row[8].strip() if len(row) > 8 else ""
        existing_tag = row[10].strip() if len(row) > 10 else ""
        if existing_tag and not force:
            new_col_k.append([existing_tag])
            continue
        new_tag = categorize_merchant(merchant, snippet)
        if new_tag and new_tag != existing_tag:
            new_col_k.append([new_tag])
            updated += 1
        else:
            new_col_k.append([existing_tag])

    if updated and not config.dry_run:
        run_gog(
            [
                "sheets", "update", spreadsheet_id,
                f"{tab_name}!K2:K{len(rows)}",
                "--values-json", json.dumps(new_col_k),
                "--input", "USER_ENTERED",
            ],
            config.gog_account,
        )

    print(f"Re-tagged {updated} rows (of {len(rows) - 1} total).")
    return 0


def scan_unparsed(config: Config) -> int:
    """Scan the lookback window and dump every unparsed HDFC mail.

    Ignores processed state and never touches the sheet/state — purely a
    debugging aid to inspect parser gaps.
    """
    debug: Dict[str, Any] = {}
    if config.debug_unparsed_file.exists():
        try:
            loaded = json.loads(
                config.debug_unparsed_file.read_text(encoding="utf-8")
            )
            if isinstance(loaded, dict):
                debug = loaded
        except json.JSONDecodeError:
            debug = {}

    threads = search_threads(config)
    scanned = 0
    reasons: Counter = Counter()
    for thread in threads:
        thread_id = thread.get("id")
        if not thread_id:
            continue
        for message in get_thread_messages(config, thread_id):
            message_id = message.get("id")
            if not message_id:
                continue
            from_h = get_header(message, "From")
            if "hdfcbank" not in from_h.lower():
                continue
            scanned += 1
            if parse_transaction(message):
                continue
            subject = get_header(message, "Subject")
            snippet = message.get("snippet", "")
            reason = unparsed_reason(subject, snippet)
            reasons[reason] += 1
            debug[message_id] = {
                "message_id": message_id,
                "from": from_h,
                "subject": subject,
                "snippet": snippet,
                "date": normalize_txn_date(get_header(message, "Date")),
                "reason": reason,
                "last_seen": datetime.now(timezone.utc).isoformat(),
            }

    if len(debug) > 5000:
        debug = {k: debug[k] for k in list(debug)[-5000:]}
    config.debug_unparsed_file.write_text(
        json.dumps(debug, indent=2, sort_keys=True), encoding="utf-8"
    )

    print(
        f"Scanned {scanned} HDFC mails | unparsed logged: {len(debug)} "
        f"({dict(reasons)}) | file: {config.debug_unparsed_file}"
    )
    return 0


def generate_report(config: Config) -> int:
    """Read the sheet and print a monthly spending report with budget comparison."""
    state = load_state(config.state_file)
    spreadsheet_id = config.spreadsheet_id or state.get("spreadsheet_id")
    if not spreadsheet_id:
        print("No spreadsheet ID configured.", file=sys.stderr)
        return 1

    tab_name = resolve_tab_name(config, spreadsheet_id)
    rows = read_sheet_values(config, spreadsheet_id, tab_name)
    if len(rows) < 2:
        print("Sheet has no data rows.")
        return 0

    # Parse rows into structured data
    monthly: Dict[str, Dict[str, float]] = {}  # "2026-01" -> {"Food": 1234, ...}
    category_totals: Dict[str, float] = {}
    untagged_count = 0
    total_spend = 0.0

    for row in rows[1:]:
        raw_date = str(row[0]).lstrip("'").strip() if row else ""
        try:
            amount = float(str(row[1]).replace(",", "")) if len(row) > 1 else 0.0
        except ValueError:
            continue
        tag = row[10].strip() if len(row) > 10 else ""
        merchant = row[3].strip() if len(row) > 3 else ""

        if not tag:
            snippet = row[8].strip() if len(row) > 8 else ""
            tag = categorize_merchant(merchant, snippet)
        if not tag:
            tag = "Uncategorized"
            untagged_count += 1

        # Extract month key from date (handles "2026-01-15", "2026-01-15 10:30:00")
        month_key = raw_date[:7] if len(raw_date) >= 7 else "unknown"

        monthly.setdefault(month_key, {})
        monthly[month_key][tag] = monthly[month_key].get(tag, 0) + amount
        category_totals[tag] = category_totals.get(tag, 0) + amount
        total_spend += amount

    months_sorted = sorted(monthly.keys())
    num_months = max(len(months_sorted), 1)
    all_cats = sorted(category_totals.keys(), key=lambda c: -category_totals[c])

    # ── Overall summary ──────────────────────────────────────────────────────
    print("=" * 80)
    print(f"  EXPENSE REPORT  |  {months_sorted[0] if months_sorted else '?'} to {months_sorted[-1] if months_sorted else '?'}  |  {len(rows)-1} transactions")
    print("=" * 80)

    print(f"\n{'Category':<22} {'Total':>10} {'Monthly Avg':>12} {'Budget':>8} {'Status':>10}")
    print(f"{'─'*22} {'─'*10} {'─'*12} {'─'*8} {'─'*10}")

    for cat in all_cats:
        total = category_totals[cat]
        avg = total / num_months
        budget = MONTHLY_BUDGET.get(cat, 0)
        if budget:
            pct = avg / budget * 100
            status = f"{pct:.0f}%" if pct <= 100 else f"OVER {pct:.0f}%"
        else:
            status = "no budget"
        print(f"{cat:<22} {total:>10,.0f} {avg:>12,.0f} {budget:>8,.0f} {status:>10}")

    print(f"{'─'*22} {'─'*10} {'─'*12}")
    print(f"{'TOTAL':<22} {total_spend:>10,.0f} {total_spend/num_months:>12,.0f}")
    if untagged_count:
        print(f"\n  ({untagged_count} transactions uncategorized — run --retag to fix)")

    # ── Month-by-month ───────────────────────────────────────────────────────
    print(f"\n{'─'*80}")
    print("MONTH-BY-MONTH BREAKDOWN")
    print(f"{'─'*80}")

    # Header row
    top_cats = all_cats[:8]  # show top 8 categories in the grid
    header = f"{'Month':<10}"
    for cat in top_cats:
        header += f" {cat[:9]:>9}"
    header += f" {'TOTAL':>9}"
    print(header)
    print(f"{'─'*10}" + f" {'─'*9}" * (len(top_cats) + 1))

    for month in months_sorted:
        line = f"{month:<10}"
        month_total = 0.0
        for cat in top_cats:
            val = monthly[month].get(cat, 0)
            month_total += val
            line += f" {val:>9,.0f}"
        # Add remaining categories to total
        for cat in all_cats:
            if cat not in top_cats:
                month_total += monthly[month].get(cat, 0)
        line += f" {month_total:>9,.0f}"
        print(line)

    # ── Budget alerts ────────────────────────────────────────────────────────
    latest_month = months_sorted[-1] if months_sorted else None
    if latest_month and latest_month in monthly:
        print(f"\n{'─'*80}")
        print(f"BUDGET ALERTS — {latest_month}")
        print(f"{'─'*80}")
        alerts = []
        for cat, spent in sorted(monthly[latest_month].items(), key=lambda x: -x[1]):
            budget = MONTHLY_BUDGET.get(cat, 0)
            if budget and spent > budget:
                over = spent - budget
                alerts.append((cat, spent, budget, over))

        if alerts:
            for cat, spent, budget, over in alerts:
                bar_len = min(int(spent / budget * 20), 40)
                bar = "█" * min(20, bar_len) + "▓" * max(0, bar_len - 20)
                print(f"  {cat:<20} {bar} {spent:>8,.0f} / {budget:>6,.0f}  (+{over:,.0f} over)")
        else:
            print("  All categories within budget!")

    return 0


def build_config(args: argparse.Namespace) -> Config:
    load_dotenv(Path(".env").resolve())
    state_path = (
        Path(os.getenv("STATE_FILE", DEFAULT_STATE_FILE)).expanduser().resolve()
    )
    unparsed_path = (
        Path(os.getenv("UNPARSED_FILE", DEFAULT_UNPARSED_FILE))
        .expanduser()
        .resolve()
    )
    debug_unparsed_path = (
        Path(os.getenv("DEBUG_UNPARSED_FILE", DEFAULT_DEBUG_UNPARSED_FILE))
        .expanduser()
        .resolve()
    )
    return Config(
        gog_account=os.getenv("GOG_ACCOUNT") or None,
        spreadsheet_id=os.getenv("SPREADSHEET_ID") or None,
        spreadsheet_title=os.getenv("SPREADSHEET_TITLE", "HDFC Expenses"),
        tab_name=os.getenv("TRANSACTIONS_TAB", DEFAULT_TAB),
        lookback_days=int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS))),
        state_file=state_path,
        unparsed_file=unparsed_path,
        debug_unparsed_file=debug_unparsed_path,
        dry_run=args.dry_run,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync HDFC expenses from Gmail to Google Sheets"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print parsed rows without writing"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Print per-message parse details"
    )
    parser.add_argument(
        "--report", action="store_true",
        help="Print monthly spending report from the sheet (no sync)",
    )
    parser.add_argument(
        "--retag", action="store_true",
        help="Re-categorise untagged rows in the sheet using rules engine",
    )
    parser.add_argument(
        "--force-retag", action="store_true",
        help="Re-categorise ALL rows (including already-tagged) using rules engine",
    )
    parser.add_argument(
        "--scan-unparsed", action="store_true",
        help="Scan the lookback window (ignoring processed state) and write "
        "every unparsed HDFC mail to the debug file. No sheet/state writes.",
    )
    args = parser.parse_args()

    config = build_config(args)

    if args.scan_unparsed:
        return scan_unparsed(config)
    if args.report:
        return generate_report(config)
    if args.retag:
        return retag_sheet(config)
    if args.force_retag:
        return retag_sheet(config, force=True)
    state = load_state(config.state_file)
    processed_ids = set(state.get("processed_message_ids", []))
    notified_unparsed = set(state.get("notified_unparsed_ids", []))
    newly_seen_ids: set[str] = set()
    new_unparsed: List[Dict[str, Any]] = []

    unparsed_debug: Dict[str, Any] = {}
    if config.debug_unparsed_file.exists():
        try:
            loaded = json.loads(
                config.debug_unparsed_file.read_text(encoding="utf-8")
            )
            if isinstance(loaded, dict):
                unparsed_debug = loaded
        except json.JSONDecodeError:
            unparsed_debug = {}

    # Resolve sheet first so we can dedup against existing rows (state file
    # is a cache, sheet is source-of-truth — survives state-file deletion).
    spreadsheet_id = ensure_sheet(config, state)
    tab_name = config.tab_name
    existing_rows: List[List[str]] = []
    pulled_count = 0
    if spreadsheet_id != "DRY_RUN_SPREADSHEET_ID":
        tab_name = resolve_tab_name(config, spreadsheet_id)
        if not config.dry_run:
            ensure_header_row(config, spreadsheet_id, tab_name)
            pulled_count = pull_tags_from_tagging(config, spreadsheet_id, tab_name)
            existing_rows = read_sheet_values(config, spreadsheet_id, tab_name)
            for row in existing_rows[1:]:
                if len(row) > 7 and row[7].strip():
                    processed_ids.add(row[7].strip())

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
                if args.debug and message_id:
                    print(f"[SKIP already-processed] {message_id}")
                continue

            subject = get_header(message, "Subject")
            from_h = get_header(message, "From")
            snippet = message.get("snippet", "")
            parsed = parse_transaction(message)
            if parsed:
                newly_seen_ids.add(message_id)
                transactions.append(parsed)
                if args.debug:
                    print(f"[PARSED] {message_id} | {parsed.get('mode')} | Rs.{parsed.get('amount')} | {parsed.get('merchant_or_payee')}")
                continue

            if "hdfcbank" not in from_h.lower():
                newly_seen_ids.add(message_id)
                if args.debug:
                    print(f"[SKIPPED non-HDFC] {message_id} from={from_h[:60]}")
                continue

            # Every HDFC mail we could not turn into a transaction is logged
            # to the debug file (deduped by id) so parser gaps can be
            # inspected later.
            reason = unparsed_reason(subject, snippet)
            unparsed_debug[message_id] = {
                "message_id": message_id,
                "from": from_h,
                "subject": subject,
                "snippet": snippet,
                "date": normalize_txn_date(get_header(message, "Date")),
                "reason": reason,
                "last_seen": datetime.now(timezone.utc).isoformat(),
            }

            if reason == "expense_unparsed":
                # Looks like a spend but no rule matched — likely an HDFC
                # wording change. Leave it unprocessed so a future parser
                # fix can still pick it up, and alert once.
                if message_id not in notified_unparsed:
                    notified_unparsed.add(message_id)
                    new_unparsed.append(
                        {
                            "message_id": message_id,
                            "subject": subject,
                            "snippet": snippet,
                            "date": normalize_txn_date(
                                get_header(message, "Date")
                            ),
                        }
                    )
                if args.debug:
                    print(f"[UNPARSED-EXPENSE] {message_id} | {subject[:70]}")
            else:
                newly_seen_ids.add(message_id)
                if args.debug:
                    print(f"[SKIPPED {reason}] {message_id}")
                    print(f"  subj={subject[:80]}")
                    print(f"  snippet={snippet[:120]}")

    merchant_tag_map = build_merchant_tag_map(existing_rows)
    for txn in transactions:
        merchant = txn.get("merchant_or_payee", "")
        tag = merchant_tag_map.get(merchant, "")
        if not tag:
            tag = categorize_merchant(merchant, txn.get("snippet", ""))
        txn["tag"] = tag

    write_transactions(config, spreadsheet_id, tab_name, transactions, existing_rows)

    pushed_count = 0
    if not config.dry_run and spreadsheet_id and spreadsheet_id != "DRY_RUN_SPREADSHEET_ID":
        pushed_count = push_to_tagging(config, spreadsheet_id, tab_name)

    if not config.dry_run:
        processed_ids.update(newly_seen_ids)
        state["processed_message_ids"] = sorted(processed_ids)[-20000:]
        state["notified_unparsed_ids"] = sorted(notified_unparsed)[-5000:]
        if spreadsheet_id and spreadsheet_id != "DRY_RUN_SPREADSHEET_ID":
            state["spreadsheet_id"] = spreadsheet_id
        save_state(config.state_file, state)
        config.unparsed_file.write_text(
            json.dumps(new_unparsed, indent=2), encoding="utf-8"
        )
        if len(unparsed_debug) > 5000:
            keep = list(unparsed_debug)[-5000:]
            unparsed_debug = {k: unparsed_debug[k] for k in keep}
        config.debug_unparsed_file.write_text(
            json.dumps(unparsed_debug, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    print(
        f"Scanned threads: {len(threads)} | Parsed new expenses: {len(transactions)} | "
        f"Unparsed expense candidates: {len(new_unparsed)} | "
        f"Unparsed HDFC mails logged: {len(unparsed_debug)} | "
        f"Tagging: pulled {pulled_count} edits, pushed {pushed_count} rows | "
        f"Spreadsheet: {spreadsheet_id} | Tab: {tab_name}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)

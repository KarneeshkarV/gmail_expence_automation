#!/usr/bin/env python3
"""Export transaction data from Google Sheets to JSON for the dashboard."""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

# Reuse categorization logic from the sync script
from sync_hdfc_expenses import (
    CATEGORY_RULES,
    MONTHLY_BUDGET,
    categorize_merchant,
    load_dotenv,
    run_gog,
)

OUTPUT_FILE = Path(__file__).parent / "dashboard_data.json"


def main() -> int:
    load_dotenv(Path(".env").resolve())
    account = os.getenv("GOG_ACCOUNT") or None
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("SPREADSHEET_ID not set", file=sys.stderr)
        return 1

    # Resolve tab name
    metadata = run_gog(["sheets", "metadata", spreadsheet_id], account)
    tabs = [
        s["properties"]["title"]
        for s in metadata.get("sheets", [])
        if "properties" in s
    ]
    tab = "Transactions" if "Transactions" in tabs else (tabs[0] if tabs else "Sheet1")

    payload = run_gog(["sheets", "get", spreadsheet_id, f"{tab}!A:K"], account)
    rows = payload.get("values", [])
    if len(rows) < 2:
        print("No data rows found", file=sys.stderr)
        return 1

    transactions = []
    category_totals: dict[str, float] = defaultdict(float)
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    monthly_totals: dict[str, float] = defaultdict(float)
    merchant_totals: dict[str, float] = defaultdict(float)

    for row in rows[1:]:
        raw_date = str(row[0]).lstrip("'").strip() if row else ""
        try:
            amount = float(str(row[1]).replace(",", "")) if len(row) > 1 else 0.0
        except ValueError:
            continue

        mode = row[2].strip() if len(row) > 2 else ""
        merchant = row[3].strip() if len(row) > 3 else ""
        tag = row[10].strip() if len(row) > 10 else ""
        snippet = row[8].strip() if len(row) > 8 else ""

        if not tag:
            tag = categorize_merchant(merchant, snippet)
        if not tag:
            tag = "Uncategorized"

        month_key = raw_date[:7] if len(raw_date) >= 7 else "unknown"

        transactions.append({
            "date": raw_date,
            "amount": amount,
            "mode": mode,
            "merchant": merchant,
            "category": tag,
        })

        category_totals[tag] += amount
        monthly[month_key][tag] += amount
        monthly_totals[month_key] += amount
        if merchant:
            merchant_totals[merchant] += amount

    # Sort merchants by total spend
    top_merchants = sorted(merchant_totals.items(), key=lambda x: -x[1])[:20]

    # Build monthly breakdown sorted by month
    months_sorted = sorted(monthly.keys())
    all_categories = sorted(category_totals.keys(), key=lambda c: -category_totals[c])

    monthly_breakdown = []
    for m in months_sorted:
        entry = {"month": m, "total": monthly_totals[m]}
        for cat in all_categories:
            entry[cat] = monthly[m].get(cat, 0)
        monthly_breakdown.append(entry)

    output = {
        "generated_at": __import__("datetime").datetime.now().isoformat(),
        "transaction_count": len(transactions),
        "transactions": transactions,
        "categories": all_categories,
        "category_totals": dict(category_totals),
        "monthly_breakdown": monthly_breakdown,
        "months": months_sorted,
        "budget": MONTHLY_BUDGET,
        "top_merchants": [{"name": n, "total": t} for n, t in top_merchants],
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Exported {len(transactions)} transactions to {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# HDFC Expense Sync (gog + Google Sheets)

Sync HDFC credit card and UPI debit expenses from Gmail into a Google Sheet.

## What is included

- `sync_hdfc_expenses.py`: pulls emails using `gog`, parses expenses, writes rows to Google Sheets.
- `run_hdfc_sync.sh`: cron-friendly wrapper that runs sync only when CPU and RAM are below thresholds.
- `.env.example`: environment configuration template.

Debit card parsing is intentionally disabled for now.

## 1) Prerequisites

- `gog` installed and authenticated for Gmail + Sheets.
- Python 3.9+.

Useful checks:

```bash
gog auth status
gog gmail --help
gog sheets --help
```

## 2) Configure

```bash
cp .env.example .env
```

Edit `.env`:

- `GOG_ACCOUNT`: your Google account email used by `gog`.
- `SPREADSHEET_ID`: optional. Leave empty to auto-create on first run.
- `LOOKBACK_DAYS`: first scan window (default 90).
- `CPU_THRESHOLD` / `RAM_THRESHOLD`: wrapper run limits (default 50).

## 3) First run

```bash
chmod +x run_hdfc_sync.sh
./run_hdfc_sync.sh
```

Dry run (parse only, no writes):

```bash
python3 sync_hdfc_expenses.py --dry-run
```

## 4) Daily cron

Open crontab:

```bash
crontab -e
```

Run once per day at 9:00 AM:

```cron
0 9 * * * /home/karneeshkar/Desktop/personal/gmail_expence_automation/run_hdfc_sync.sh
```

## Sheet columns

- `txn_date`
- `amount`
- `mode` (`credit_card` or `upi`)
- `merchant_or_payee`
- `account_or_card`
- `reference_no`
- `subject`
- `message_id`
- `snippet`
- `synced_at`

## Notes

- Only expense-like entries are inserted (`debited`, `spent`, etc.).
- Credit/refund/reversal emails are skipped.
- De-duplication is done via Gmail `message_id` in local state file (`.hdfc_sync_state.json`).

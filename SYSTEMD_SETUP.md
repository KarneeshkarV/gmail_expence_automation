# HDFC Expense Sync - Systemd Setup

## What's Running

Two systemd units handle automated syncing:

### 1. Daily Timer (9 PM every day)
Runs the sync at 9 PM daily. If the PC was off or sleeping at 9 PM, it runs immediately on next boot/wake (`Persistent=true`).

**Files:**
- `~/.config/systemd/user/hdfc-sync.service`
- `~/.config/systemd/user/hdfc-sync.timer`

### 2. Resume from Sleep Service
Runs the sync every time the PC wakes from sleep (suspend, hibernate, hybrid-sleep).

**File:**
- `/etc/systemd/system/hdfc-sync-resume.service`

---

## Useful Commands

```bash
# Check timer — next trigger time
systemctl --user list-timers hdfc-sync.timer

# Check resume service status
systemctl status hdfc-sync-resume.service

# Manually trigger a sync
systemctl --user start hdfc-sync.service

# View logs
tail -f ~/Desktop/personal/gmail_expence_automation/logs/hdfc_sync_$(date +%F).log

# View systemd journal for resume service
journalctl -u hdfc-sync-resume.service -n 30
```

---

## Fix Applied

`.env` had an unquoted value with a space:
```
# Before (broken)
SPREADSHEET_TITLE=HDFC Expenses

# After (fixed)
SPREADSHEET_TITLE="HDFC Expenses"
```
Bash was interpreting `Expenses` as a command, causing exit code 127.

---

## Re-enable After System Changes

If units ever stop working:
```bash
# Timer
systemctl --user daemon-reload
systemctl --user enable --now hdfc-sync.timer

# Resume service
sudo systemctl daemon-reload
sudo systemctl reset-failed hdfc-sync-resume.service
sudo systemctl enable --now hdfc-sync-resume.service
```

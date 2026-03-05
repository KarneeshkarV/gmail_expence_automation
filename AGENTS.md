# AGENTS.md

Guide for coding agents working in this repository.

## Project Overview

Python project that syncs HDFC credit card and UPI expenses from Gmail to Google Sheets using the `gog` CLI tool.

## Build/Lint/Test Commands

### Running the Application

```bash
python3 sync_hdfc_expenses.py              # Normal execution
python3 sync_hdfc_expenses.py --dry-run    # Parse only, no writes
./run_hdfc_sync.sh                          # Wrapper with CPU/RAM checks
uv run sync_hdfc_expenses.py               # Preferred if uv available
```

### Linting and Formatting

```bash
ruff check .           # Lint all Python files
ruff format .          # Format all Python files
```

### Testing

No test framework configured. When adding tests, use pytest:

```bash
pytest                                    # Run all tests
pytest tests/test_example.py              # Run single test file
pytest tests/test_example.py::test_name -v  # Run single test
pytest --cov=.                            # Run with coverage
```

## Code Style Guidelines

### Python Version

Target Python 3.9+ for compatibility (development uses 3.14).

### Imports

```python
from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
```

- `from __future__` first, then standard library, third-party, local
- Sort alphabetically within groups
- Use `from typing import ...` (not `list[]`, `dict[]` syntax)

### Formatting

- Use ruff (Black-compatible)
- Max line length: 88 characters
- Double quotes for strings
- 4 spaces for indentation (no tabs)
- Blank lines between top-level functions/classes

### Type Hints

```python
from typing import Any, Dict, List, Optional

def function_name(items: List[str], config: Dict[str, Any]) -> Optional[str]:
    ...

@dataclass
class Config:
    gog_account: Optional[str]
    spreadsheet_id: Optional[str]
    dry_run: bool
```

### Naming Conventions

- **Functions/Methods**: `snake_case` — `load_state`, `parse_transaction`
- **Classes**: `PascalCase` — `Config`
- **Constants**: `UPPER_SNAKE_CASE` — `DEFAULT_STATE_FILE`, `DEFAULT_TAB`
- **Variables**: `snake_case` — `spreadsheet_id`, `processed_ids`

### Error Handling

```python
# Raise RuntimeError with descriptive messages
if proc.returncode != 0:
    raise RuntimeError(f"Command failed: {proc.stderr}")

# Chain exceptions with 'from'
except json.JSONDecodeError as exc:
    raise RuntimeError(f"Invalid JSON: {exc}") from exc

# Main entry point
if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
```

### Documentation

- Module-level docstring at top of file
- No inline comments (code should be self-documenting)
- Function docstrings only for non-obvious behavior

### Common Patterns

```python
# File operations
content = path.read_text(encoding="utf-8")
path.write_text(json.dumps(data, indent=2), encoding="utf-8")

# Environment variables
value = os.getenv("KEY", "default")
value = os.getenv("KEY") or None  # Empty string to None

# Subprocess
proc = subprocess.run(cmd, capture_output=True, text=True)
if proc.returncode != 0:
    raise RuntimeError(f"Failed: {proc.stderr}")

# Argparse
parser = argparse.ArgumentParser(description="...")
parser.add_argument("--dry-run", action="store_true", help="...")
args = parser.parse_args()
```

## Project Structure

```
├── sync_hdfc_expenses.py   # Main script
├── run_hdfc_sync.sh        # Cron wrapper
├── .env                     # Local config (gitignored)
├── .env.example             # Config template
├── .hdfc_sync_state.json    # Dedup state (gitignored)
└── AGENTS.md
```

## External Dependencies

- **gog**: CLI for Gmail/Sheets API — `gog gmail search`, `gog sheets append`
- **uv**: Optional Python runner (fallback: `python3`)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GOG_ACCOUNT` | None | Google account email |
| `SPREADSHEET_ID` | Auto-created | Google Sheets ID |
| `LOOKBACK_DAYS` | 90 | Days to search in Gmail |
| `STATE_FILE` | `.hdfc_sync_state.json` | Dedup state path |

## Notes

- Process expense keywords: debited, spent, purchase
- Skip: credited, refund, reversal, cashback
- Dedup via Gmail message IDs (max 20,000 in state)
- Debit card parsing disabled

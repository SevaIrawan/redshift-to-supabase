# Shared logic untuk sync Redshift → rs_blue_whale_* (recent days) + log CSV.
# Dipanggil oleh sync_blue_whale_usc/sgd/myr_recent_days.py

import csv
import io
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
LOGS_DIR = SCRIPT_DIR / "logs"


def build_log_csv(
    market: str,
    source: str,
    target: str,
    date_range: str,
    rows: int,
    started: str,
    finished: str,
    status: str,
    message: str,
) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["# Sync Redshift → rs_blue_whale_* (recent days)"])
    w.writerow([
        "market", "source", "target", "date_range", "rows_inserted",
        "started", "finished", "status", "message",
    ])
    w.writerow([
        market, source, target, date_range, rows,
        started, finished, status, message,
    ])
    return buf.getvalue()

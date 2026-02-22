# Shared logic untuk sync Redshift → rs_blue_whale_* (recent days) + log CSV + Slack bot.
# Dipanggil oleh sync_blue_whale_usc/sgd/myr_recent_days.py

import csv
import io
import os
from datetime import datetime
from pathlib import Path

from slack_sdk import WebClient
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


def _upload_file_to_slack(file_path: Path, channel_id: str, token: str, initial_comment: str) -> bool:
    try:
        client = WebClient(token=token)
        client.files_upload_v2(
            channel=channel_id,
            title=file_path.stem,
            filename=file_path.name,
            file=str(file_path),
            initial_comment=initial_comment,
        )
        return True
    except Exception as e:
        err = str(e)
        print(f"[Slack] File upload GAGAL: {err}")
        if "not_in_channel" in err:
            print("         → Bot belum di-invite ke channel. Ketik: /invite @ETL_Airflow")
        elif "invalid_auth" in err or "token_revoked" in err:
            print("         → Cek SLACK_BOT_TOKEN di .env")
        elif "channel_not_found" in err:
            print("         → Cek SLACK_CHANNEL_ID (format: C1234567890)")
        return False


def send_log_to_slack(log_path_csv: Path, market: str, rows: int, status: str, date_range: str) -> None:
    title = f"Sync Redshift → rs_blue_whale_* ({market.upper()}) — {status} — {rows:,} rows — {date_range}"
    bot_token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    channel_id = os.getenv("SLACK_CHANNEL_ID", "").strip()
    if bot_token and channel_id:
        _upload_file_to_slack(
            log_path_csv,
            channel_id,
            bot_token,
            f"*{title}*\n\n📎 Rekap CSV — klik untuk buka:",
        )

"""
Send Slack notification when a table sync completes successfully.
Uses SLACK_WEBHOOK_URL from .env. If not set, no message is sent.
"""
import os
import json
import urllib.request
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def send_sync_success(
    table_name: str,
    rows_inserted: int,
    start_time: float | None = None,
    end_time: float | None = None,
    data_range: str = "",
) -> None:
    """Send a professional success message to Slack with optional timestamps and data range."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return
    rows_str = f"{rows_inserted:,}"
    start_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S") if start_time else "—"
    end_str = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S") if end_time else "—"
    # Script sync selalu set 3 hari ke belakang dari today, jadi data_range selalu ada.
    blocks = [
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*Data Sync Completed*"},
                {"type": "mrkdwn", "text": f"*Data :* {data_range}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Table:*\n`{table_name}`"},
                {"type": "mrkdwn", "text": f"*Rows synced:*\n{rows_str}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Sync started:*\n{start_str}"},
                {"type": "mrkdwn", "text": f"*Sync ended:*\n{end_str}"},
            ],
        },
    ]
    payload = {
        "text": f"Data sync completed: {table_name} — {rows_str} rows. Data : {data_range}. Started: {start_str} | Ended: {end_str}",
        "blocks": blocks,
    }
    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass

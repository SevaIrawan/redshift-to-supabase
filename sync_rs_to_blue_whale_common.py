# Shared logic untuk sync rs_blue_whale_* → blue_whale_* (H-1) + log + Slack.
# Dipanggil oleh sync_rs_to_blue_whale_usc/sgd/myr.py

import csv
import io
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
from slack_sdk import WebClient
import requests
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
LOGS_DIR = SCRIPT_DIR / "logs"
DATE_COLUMN = "date"
SCHEMA = "public"

COLUMN_MAPPING = [
    ("username", "user_name"),
    ("vip_level", "vip_level"),
    ("operator_username", "operator"),
    ("unique_code", "unique_code"),
    ("absent", "absent"),
    ("deposit_cases", "deposit_cases"),
    ("deposit_amount", "deposit_amount"),
    ("withdraw_cases", "withdraw_cases"),
    ("withdraw_amount", "withdraw_amount"),
    ("bonus", "bonus"),
    ("adjustment_cases", "cases_adjustment"),
    ("add_bonus", "add_bonus"),
    ("deduct_bonus", "deduct_bonus"),
    ("add_transaction", "add_transaction"),
    ("deduct_transaction", "deduct_transaction"),
    ("total_bet", "cases_bets"),
    ("total_bet_amount", "bets_amount"),
    ("total_valid_bet_amount", "valid_amount"),
    ("ggr", "ggr"),
    ("net_profit", "net_profit"),
    ("date", "date"),
    ("year", "year"),
    ("month_text", "month"),
    ("line", "line"),
    ("currency", "currency"),
    ("user_key", "userkey"),
    ("unique_key", "uniquekey"),
]


def quote_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def connect_supabase():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT")),
        database=os.getenv("SUPABASE_DATABASE"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        sslmode="require",
    )
    conn.autocommit = False
    return conn


# Kolom source yang boleh NULL — ganti dengan '' agar target NOT NULL constraint ok
COALESCE_COLS = {"operator_username"}


def run_sync(conn, source_table: str, target_table: str) -> int:
    today = date.today()
    h1 = today - timedelta(days=1)
    src_cols, tgt_cols = zip(*COLUMN_MAPPING)

    def _sel(c):
        if c in COALESCE_COLS:
            return f"COALESCE({quote_name(c)}, '')"
        return quote_name(c)

    src_list = ", ".join(_sel(c) for c in src_cols)
    tgt_list = ", ".join(quote_name(c) for c in tgt_cols)
    placeholders = ", ".join(["%s"] * len(src_cols))
    q_schema = quote_name(SCHEMA)
    q_source = quote_name(source_table)
    q_target = quote_name(target_table)
    q_date = quote_name(DATE_COLUMN)

    cur = conn.cursor()
    cur.execute(f'DELETE FROM {q_schema}.{q_target} WHERE {q_date} = %s', (h1,))
    cur.execute(f'SELECT {src_list} FROM {q_schema}.{q_source} WHERE {q_date} = %s', (h1,))
    rows = cur.fetchall()
    if not rows:
        conn.commit()
        cur.close()
        return 0
    insert_sql = f'INSERT INTO {q_schema}.{q_target} ({tgt_list}) VALUES ({placeholders})'
    execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()
    cur.close()
    return len(rows)


def build_log_csv(market: str, source: str, target: str, h1: date, rows: int, started: str, finished: str, status: str, message: str) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["# Sync rs_* → blue_whale_* (H-1)"])
    w.writerow(["market", "source", "target", "date_h1", "rows_inserted", "started", "finished", "status", "message"])
    w.writerow([market, source, target, str(h1), rows, started, finished, status, message])
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


def send_log_to_slack(log_path_csv: Path, market: str, rows: int, status: str) -> None:
    title = f"Sync rs_* → blue_whale_* ({market.upper()}) — {status} — {rows:,} rows"
    bot_token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    channel_id = os.getenv("SLACK_CHANNEL_ID", "").strip()
    if bot_token and channel_id:
        _upload_file_to_slack(log_path_csv, channel_id, bot_token, f"*{title}*\n\n📎 Rekap CSV — klik untuk buka:")

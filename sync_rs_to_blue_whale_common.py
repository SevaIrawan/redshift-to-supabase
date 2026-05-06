# Shared logic sync rs_blue_whale_* → blue_whale_* (H-2 = 2 hari ke belakang) + log CSV.
# Dipanggil oleh sync_rs_to_blue_whale_usc/sgd/myr.py
from __future__ import annotations

import csv
import io
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
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

# Kolom tambahan (nama sama di rs_blue_whale_* dan blue_whale_*); dipakai rs_to_*.py
EXTRA_COLUMNS_TRAFFIC_REGISTER_FTD: list[tuple[str, str]] = [
    ("traffic", "traffic"),
    ("register_date", "register_date"),
    ("first_deposit_date", "first_deposit_date"),
    ("first_deposit_amount", "first_deposit_amount"),
]

COLUMN_MAPPING_WITH_TRAFFIC_FTD = COLUMN_MAPPING + EXTRA_COLUMNS_TRAFFIC_REGISTER_FTD


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


def run_sync(
    conn,
    source_table: str,
    target_table: str,
    *,
    column_mapping: list[tuple[str, str]] | None = None,
) -> tuple[int, list[date]]:
    """Sync 2 hari ke belakang (kemarin + 2 hari lalu). Return (row_count, [date_2, date_1])."""
    today = date.today()
    date_1 = today - timedelta(days=1)   # kemarin (01/03 bila hari ini 02/03)
    date_2 = today - timedelta(days=2)   # 2 hari lalu (28/02 bila hari ini 02/03)
    dates = [date_2, date_1]   # kronologi: 28/02 dulu, 01/03 kemudian

    mapping = column_mapping if column_mapping is not None else COLUMN_MAPPING
    src_cols, tgt_cols = zip(*mapping)

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
    # DELETE 2 hari: H-1 dan H-2
    cur.execute(f'DELETE FROM {q_schema}.{q_target} WHERE {q_date} IN (%s, %s)', (date_1, date_2))
    # SELECT 2 hari: H-1 dan H-2
    cur.execute(f'SELECT {src_list} FROM {q_schema}.{q_source} WHERE {q_date} IN (%s, %s) ORDER BY {q_date}', (date_1, date_2))
    rows = cur.fetchall()
    if not rows:
        conn.commit()
        cur.close()
        return 0, dates
    insert_sql = f'INSERT INTO {q_schema}.{q_target} ({tgt_list}) VALUES ({placeholders})'
    execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()
    cur.close()
    return len(rows), dates


def build_log_csv(market: str, source: str, target: str, dates: list[date], rows: int, started: str, finished: str, status: str, message: str) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["# Sync rs_* → blue_whale_* (H-2 = 2 hari ke belakang)"])
    w.writerow(["market", "source", "target", "dates_deleted_and_replaced", "rows_inserted", "started", "finished", "status", "message"])
    dates_str = ", ".join(str(d) for d in dates)
    w.writerow([market, source, target, dates_str, rows, started, finished, status, message])
    return buf.getvalue()

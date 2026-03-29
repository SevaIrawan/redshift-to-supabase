"""
TY: rs_blue_whale_* → blue_whale_*.
File baru; tidak mengimpor sync_rs_to_blue_whale_common.

Paritas dengan sync_rs_to_blue_whale_common.run_sync (run_rs_to_blue_whale_sync.bat):
  - COLUMN_MAPPING, COALESCE_COLS, SCHEMA, DATE_COLUMN, quote_name, execute_batch page_size 1000
    — sama persis dengan file common lama.
  - Pola SQL: DELETE ... WHERE date IN (...); SELECT kolom ter-map + COALESCE operator_username;
    INSERT ke target — sama.

Perbedaan yang disengaja: pasangan tanggal. Legacy run_sync memakai H-1 & H-2 (kemarin + 2 hari lalu).
  TY memakai kemarin & hari ini agar selaras dengan isi rs_* setelah ty_redshift_to_rs (window TY).
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()

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

COALESCE_COLS = {"operator_username"}


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


def run_sync_yesterday_today(conn, source_table: str, target_table: str) -> tuple[int, list[date]]:
    today = date.today()
    yesterday = today - timedelta(days=1)
    dates = [yesterday, today]

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
    cur.execute(
        f"DELETE FROM {q_schema}.{q_target} WHERE {q_date} IN (%s, %s)",
        (yesterday, today),
    )
    cur.execute(
        f"SELECT {src_list} FROM {q_schema}.{q_source} WHERE {q_date} IN (%s, %s) ORDER BY {q_date}",
        (yesterday, today),
    )
    rows = cur.fetchall()
    if not rows:
        conn.commit()
        cur.close()
        return 0, dates
    insert_sql = f"INSERT INTO {q_schema}.{q_target} ({tgt_list}) VALUES ({placeholders})"
    execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()
    cur.close()
    return len(rows), dates

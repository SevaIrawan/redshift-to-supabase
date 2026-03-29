"""
TY = kemarin + hari ini. Redshift → rs_blue_whale_*.
File baru; tidak mengimpor sync_blue_whale_* / sync_blue_whale_recent_common.

Paritas dengan sync_blue_whale_*_recent_days (run_all_sync.bat):
  - Kunci config JSON sama: source_table, target_table, filter_column, line_filter_mode,
    line_values, schema, date_column, order_column, batch_size; env SYNC_CONFIG*_PATH.
  - Filter tanggal: txn_date >= start AND txn_date < end + opsional IN(filter_column)
    + activity OR (kolom & COALESCE sama persis).
  - DELETE di Supabase untuk rentang yang sama (tanpa filter line).
  - INSERT semua kolom dari Redshift + last_synced_at jika kolom ada; execute_batch page_size 500.

Perbedaan yang disengaja: rentang tanggal TY = kemarin .. < besok (get_yesterday_today_bounds),
  bukan get_recent_days_window(days_back) dari config (biasanya 3 hari).
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, timedelta

import psycopg2
import redshift_connector
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

LAST_SYNC_COLUMN = "last_synced_at"


def quote_name(name: str) -> str:
    return '"{}"'.format(name.replace('"', '""'))


def connect_redshift():
    return redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        database=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
    )


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


def ensure_last_synced_column(conn, qualified_table: str):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        ALTER TABLE {qualified_table}
        ADD COLUMN IF NOT EXISTS {quote_name(LAST_SYNC_COLUMN)} timestamptz DEFAULT NOW()
        """
    )
    cursor.execute(
        f"""
        UPDATE {qualified_table}
        SET {quote_name(LAST_SYNC_COLUMN)} = NOW()
        WHERE {quote_name(LAST_SYNC_COLUMN)} IS NULL
        """
    )
    conn.commit()
    cursor.close()


def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file '{config_path}' tidak ditemukan. "
            f"Silakan buat file config terlebih dahulu."
        )
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_yesterday_today_bounds() -> tuple[date, date]:
    """(start_inclusive, end_exclusive): kemarin .. < besok."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    end_exclusive = today + timedelta(days=1)
    return yesterday, end_exclusive


def sync_ty_window(
    redshift,
    supabase,
    *,
    source_table: str,
    target_table: str,
    start_date: date,
    end_exclusive: date,
    line_values: list[str],
    filter_column: str = "agent_code",
    schema: str = "public",
    date_column: str = "txn_date",
    order_column: str = "unique_key",
    batch_size: int = 2000,
    market: str = "usc",
):
    today = date.today()
    qualified_source = f"{quote_name(schema)}.{quote_name(source_table)}"
    qualified_target = f"{quote_name(schema)}.{quote_name(target_table)}"
    order_column_q = quote_name(order_column)
    date_column_q = quote_name(date_column)
    filter_column_q = quote_name(filter_column)

    print("\n" + "=" * 70)
    print(f"TY SYNC {source_table} -> {target_table} ({market})")
    print("=" * 70)
    print(f"Today: {today}")
    print(f"Window: {start_date} <= {date_column} < {end_exclusive}")
    print(f"Filter column: {filter_column}")
    if line_values:
        print(f"Filter values: {', '.join(line_values)}")
    else:
        print(f"Filter values: ALL")
    print(
        "Activity filter: only rows with at least one of "
        "(deposit_cases, deposit_amount, withdraw_*, bonus, add/deduct_*, total_bet*) > 0"
    )

    info_cursor = redshift.cursor()
    info_cursor.execute(f"SELECT * FROM {qualified_source} LIMIT 0")
    columns = [desc[0] for desc in info_cursor.description]
    info_cursor.close()

    if date_column not in columns:
        raise SystemExit(f"[ERROR] Kolom {date_column} tidak ada di source table.")
    if filter_column not in columns:
        raise SystemExit(f"[ERROR] Kolom {filter_column} tidak ada di source table.")
    activity_columns = [
        "deposit_cases", "deposit_amount", "withdraw_cases", "withdraw_amount",
        "bonus", "add_transaction", "deduct_transaction", "add_bonus", "deduct_bonus",
        "total_bet", "total_bet_amount", "total_valid_bet_amount",
    ]
    missing_activity = [c for c in activity_columns if c not in columns]
    if missing_activity:
        raise SystemExit(f"[ERROR] Kolom activity tidak ada: {', '.join(missing_activity)}")

    ensure_last_synced_column(supabase, qualified_target)

    if line_values:
        filter_placeholders = ",".join(["%s"] * len(line_values))
        where_clause = (
            f"{date_column_q} >= %s AND {date_column_q} < %s "
            f"AND {filter_column_q} IN ({filter_placeholders})"
        )
        where_params = [start_date, end_exclusive] + line_values
    else:
        where_clause = f"{date_column_q} >= %s AND {date_column_q} < %s"
        where_params = [start_date, end_exclusive]

    activity_conditions = [f"COALESCE({quote_name(c)}, 0) > 0" for c in activity_columns]
    activity_clause = "(" + " OR ".join(activity_conditions) + ")"
    where_clause = where_clause + " AND " + activity_clause

    print("\n[STEP 1] Delete window dari Supabase...")
    delete_cursor = supabase.cursor()
    delete_sql = (
        f"DELETE FROM {qualified_target} "
        f"WHERE {date_column_q} >= %s AND {date_column_q} < %s"
    )
    delete_cursor.execute(delete_sql, (start_date, end_exclusive))
    supabase.commit()
    deleted = delete_cursor.rowcount
    delete_cursor.close()
    print(f"[OK] Deleted {deleted:,} rows")

    print("\n[STEP 2] Fetch dari Redshift...")
    count_cursor = redshift.cursor()
    count_cursor.execute(
        f"SELECT COUNT(*) FROM {qualified_source} WHERE {where_clause}",
        where_params,
    )
    rows_to_sync = count_cursor.fetchone()[0]
    count_cursor.close()
    print(f"Rows to import: {rows_to_sync:,}")

    if rows_to_sync == 0:
        print("[INFO] Tidak ada data.")
        return

    breakdown_cursor = redshift.cursor()
    breakdown_cursor.execute(
        f"""
        SELECT
            {date_column_q}::date as tanggal,
            {filter_column_q} as filter_value,
            COUNT(*) as jumlah
        FROM {qualified_source}
        WHERE {where_clause}
        GROUP BY {date_column_q}::date, {filter_column_q}
        ORDER BY {date_column_q}::date DESC, {filter_column_q}
        """,
        where_params,
    )
    print(f"\nBreakdown per tanggal dan {filter_column}:")
    for row in breakdown_cursor.fetchall():
        print(f"  - {row[0]} | {row[1]}: {row[2]:,} rows")
    breakdown_cursor.close()

    col_names = ", ".join(quote_name(col) for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    target_info_cursor = supabase.cursor()
    target_info_cursor.execute(f"SELECT * FROM {qualified_target} LIMIT 0")
    target_columns = [desc[0] for desc in target_info_cursor.description]
    target_info_cursor.close()

    if LAST_SYNC_COLUMN in target_columns:
        insert_sql = (
            f"INSERT INTO {qualified_target} ({col_names}, {quote_name(LAST_SYNC_COLUMN)}) "
            f"VALUES ({placeholders}, NOW())"
        )
    else:
        insert_sql = (
            f"INSERT INTO {qualified_target} ({col_names}) "
            f"VALUES ({placeholders})"
        )

    fetch_cursor = redshift.cursor()
    fetch_cursor.execute(
        f"""
        SELECT *
        FROM {qualified_source}
        WHERE {where_clause}
        ORDER BY {order_column_q}
        """,
        where_params,
    )

    insert_cursor = supabase.cursor()
    inserted = 0
    batch_num = 0
    start_time = time.time()

    while True:
        rows = fetch_cursor.fetchmany(batch_size)
        if not rows:
            break
        batch_num += 1
        execute_batch(insert_cursor, insert_sql, rows, page_size=500)
        supabase.commit()
        inserted += len(rows)
        elapsed = time.time() - start_time
        speed = inserted / elapsed if elapsed else 0
        progress = (inserted / rows_to_sync) * 100
        eta = (rows_to_sync - inserted) / speed / 60 if speed else 0
        print(
            f"[Batch {batch_num}] Inserted {inserted:,}/{rows_to_sync:,} "
            f"({progress:.1f}%) | {speed:.0f} r/s | ETA {eta:.1f} m"
        )

    insert_cursor.close()
    fetch_cursor.close()
    print(f"\n[STEP 3] [DONE] {inserted:,} rows -> {target_table}.")


def run_ty_main(*, env_var: str, default_config: str, market: str):
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    load_dotenv()
    config_path = os.getenv(env_var, default_config)
    try:
        config = load_config(config_path)
        print(f"[OK] Config: {config_path}")
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON invalid: {e}")
        sys.exit(1)

    SOURCE_TABLE = config.get("source_table", "blue_whale_usc")
    TARGET_TABLE = config.get("target_table", "rs_blue_whale_usc")
    FILTER_COLUMN = config.get("filter_column", "agent_code")
    LINE_FILTER_MODE = config.get("line_filter_mode", "specific").lower()
    LINE_VALUES = list(config.get("line_values", []))
    SCHEMA = config.get("schema", "public")
    DATE_COLUMN = config.get("date_column", "txn_date")
    ORDER_COLUMN = config.get("order_column", "unique_key")
    BATCH_SIZE = config.get("batch_size", 2000)

    if LINE_FILTER_MODE not in ("all", "specific"):
        print(f"[ERROR] line_filter_mode harus 'all' atau 'specific'")
        sys.exit(1)
    if LINE_FILTER_MODE == "all":
        LINE_VALUES = []
    elif not LINE_VALUES:
        print("[WARNING] line_values kosong — akan DISTINCT dari Redshift")

    start_date, end_exclusive = get_yesterday_today_bounds()

    print("=" * 70)
    print(f"TY REDSHIFT → RS — {market.upper()}")
    print("=" * 70)

    redshift = connect_redshift()
    supabase = connect_supabase()
    try:
        if not LINE_VALUES:
            qualified_source = f"{quote_name(SCHEMA)}.{quote_name(SOURCE_TABLE)}"
            date_column_q = quote_name(DATE_COLUMN)
            filter_column_q = quote_name(FILTER_COLUMN)
            fc = redshift.cursor()
            fc.execute(
                f"""
                SELECT DISTINCT {filter_column_q}
                FROM {qualified_source}
                WHERE {date_column_q} >= %s AND {date_column_q} < %s
                AND {filter_column_q} IS NOT NULL
                ORDER BY {filter_column_q}
                """,
                (start_date, end_exclusive),
            )
            LINE_VALUES = [row[0] for row in fc.fetchall()]
            fc.close()
            if not LINE_VALUES:
                print(f"[INFO] Tidak ada {FILTER_COLUMN} untuk window TY.")
                return
            print(f"[INFO] {len(LINE_VALUES)} nilai {FILTER_COLUMN}.")

        sync_ty_window(
            redshift,
            supabase,
            source_table=SOURCE_TABLE,
            target_table=TARGET_TABLE,
            start_date=start_date,
            end_exclusive=end_exclusive,
            line_values=LINE_VALUES,
            filter_column=FILTER_COLUMN,
            schema=SCHEMA,
            date_column=DATE_COLUMN,
            order_column=ORDER_COLUMN,
            batch_size=BATCH_SIZE,
            market=market,
        )
    finally:
        try:
            supabase.close()
        except Exception:
            pass
        try:
            redshift.close()
        except Exception:
            pass

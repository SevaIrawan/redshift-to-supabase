import os
import sys
import time
import json
from datetime import date, timedelta

import redshift_connector
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

from sync_blue_whale_recent_common import (
    LOGS_DIR,
    build_log_csv,
    send_log_to_slack,
)


def quote_name(name: str) -> str:
    return '"{}"'.format(name.replace('"', '""'))


LAST_SYNC_COLUMN = "last_synced_at"


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


def load_config(config_path: str = "sync_config_sgd.json") -> dict:
    """
    Load konfigurasi dari file JSON.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file '{config_path}' tidak ditemukan. "
            f"Silakan buat file config terlebih dahulu."
        )
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    return config


def get_recent_days_window(days_back: int) -> tuple[date, date]:
    """
    Menghitung tanggal window untuk beberapa hari terakhir.
    Auto detect tanggal hari ini dari sistem.
    Contoh: jika hari ini tanggal 19 dan days_back=3, 
    maka akan mengambil data tanggal 18, 17, 16 (H-1, H-2, H-3)
    """
    today = date.today()  # Auto detect tanggal hari ini
    # H-1 dari hari ini, jadi mulai dari kemarin
    end_date = today  # Exclusive end (tidak termasuk hari ini)
    start_date = today - timedelta(days=days_back)  # H-3, H-2, H-1
    return start_date, end_date


def sync_recent_days(
    redshift,
    supabase,
    source_table: str,
    target_table: str,
    days_back: int,
    line_values: list[str],
    filter_column: str = "agent_code",
    schema: str = "public",
    date_column: str = "txn_date",
    order_column: str = "unique_key",
    batch_size: int = 2000,
    market: str = "sgd",
):
    """
    Sync data beberapa hari terakhir dengan filter berdasarkan kolom tertentu.
    Logic: Hapus 3 hari back dari Supabase, lalu import 3 hari back dari Redshift.
    
    Args:
        days_back: Berapa hari ke belakang dari hari ini (misalnya 3 untuk H-1, H-2, H-3)
        line_values: List nilai filter yang akan diambil (contoh: ['SBKH', 'UWKH'])
        filter_column: Nama kolom untuk filter (default: 'agent_code')
    """
    sync_start_ts = time.time()
    today = date.today()  # Auto detect tanggal hari ini
    start_date, end_date = get_recent_days_window(days_back)
    d = start_date
    day_list = []
    while d < end_date:
        day_list.append(d)
        d += timedelta(days=1)
    sync_data_range = "/".join(d.strftime("%d") for d in day_list) + " " + start_date.strftime("%B %Y") + f" ({len(day_list)} Days)"
    qualified_source = f"{quote_name(schema)}.{quote_name(source_table)}"
    qualified_target = f"{quote_name(schema)}.{quote_name(target_table)}"
    order_column_q = quote_name(order_column)
    date_column_q = quote_name(date_column)
    filter_column_q = quote_name(filter_column)

    print("\n" + "=" * 70)
    print(f"SYNC {source_table} -> {target_table}")
    print("=" * 70)
    print(f"Today date: {today} (auto detected)")
    print(f"Date window: {start_date} <= {date_column} < {end_date}")
    print(f"Days back: {days_back} (H-1, H-2, ..., H-{days_back})")
    print(f"Filter column: {filter_column}")
    if line_values:
        print(f"Filter values: {', '.join(line_values)}")
    else:
        print(f"Filter values: ALL (semua {filter_column})")
    print("Activity filter: only rows with at least one of (deposit_cases, deposit_amount, withdraw_*, bonus, add/deduct_*, total_bet*) > 0")

    # Cek kolom di source table
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
        raise SystemExit(f"[ERROR] Kolom activity tidak ada di source table: {', '.join(missing_activity)}")

    ensure_last_synced_column(supabase, qualified_target)

    # Build WHERE clause - dengan atau tanpa filter
    if line_values:
        # Filter berdasarkan nilai tertentu
        filter_placeholders = ",".join(["%s"] * len(line_values))
        where_clause = (
            f"{date_column_q} >= %s AND {date_column_q} < %s "
            f"AND {filter_column_q} IN ({filter_placeholders})"
        )
        where_params = [start_date, end_date] + line_values
    else:
        # Semua nilai (tanpa filter)
        where_clause = (
            f"{date_column_q} >= %s AND {date_column_q} < %s"
        )
        where_params = [start_date, end_date]

    # Filter activity: hanya baris yang punya minimal satu aktivitas > 0
    activity_conditions = [f"COALESCE({quote_name(c)}, 0) > 0" for c in activity_columns]
    activity_clause = "(" + " OR ".join(activity_conditions) + ")"
    where_clause = where_clause + " AND " + activity_clause

    # STEP 1: Hapus 3 hari back dari Supabase (sesuai date window, tanpa filter line)
    print("\n[STEP 1] Deleting 3 days back from Supabase...")
    delete_cursor = supabase.cursor()
    delete_sql = (
        f"DELETE FROM {qualified_target} "
        f"WHERE {date_column_q} >= %s AND {date_column_q} < %s"
    )
    delete_cursor.execute(delete_sql, (start_date, end_date))
    supabase.commit()
    deleted = delete_cursor.rowcount
    delete_cursor.close()
    print(f"[OK] Deleted {deleted:,} rows from Supabase (date range: {start_date} to {end_date})")

    # STEP 2: Ambil 3 hari back dari Redshift dengan filter line
    print("\n[STEP 2] Fetching 3 days back from Redshift...")
    count_cursor = redshift.cursor()
    count_cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {qualified_source}
        WHERE {where_clause}
        """,
        where_params,
    )
    rows_to_sync = count_cursor.fetchone()[0]
    count_cursor.close()
    print(f"Rows to import: {rows_to_sync:,}")
    
    if rows_to_sync == 0:
        print("[INFO] Tidak ada data untuk periode dan line yang dipilih.")
        sync_end_ts = time.time()
        _notify_sync_done(market, source_table, target_table, sync_data_range, 0, sync_start_ts, sync_end_ts, "success", "No data to sync")
        return

    # Tampilkan breakdown per tanggal dan filter column
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

    # Prepare insert
    # Karena data untuk date range sudah dihapus di STEP 1, tidak akan ada duplikat
    # Jadi gunakan INSERT biasa saja tanpa ON CONFLICT
    col_names = ", ".join(quote_name(col) for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    # Cek apakah kolom last_synced_at ada di target table
    target_info_cursor = supabase.cursor()
    target_info_cursor.execute(f"SELECT * FROM {qualified_target} LIMIT 0")
    target_columns = [desc[0] for desc in target_info_cursor.description]
    target_info_cursor.close()
    
    if LAST_SYNC_COLUMN in target_columns:
        # Jika kolom last_synced_at ada, tambahkan ke insert
        insert_sql = (
            f"INSERT INTO {qualified_target} ({col_names}, {quote_name(LAST_SYNC_COLUMN)}) "
            f"VALUES ({placeholders}, NOW())"
        )
    else:
        # Jika kolom last_synced_at tidak ada, insert tanpa kolom tersebut
        insert_sql = (
            f"INSERT INTO {qualified_target} ({col_names}) "
            f"VALUES ({placeholders})"
        )

    # Fetch dan insert
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
    print(f"\n[STEP 3] [DONE] Synced {inserted:,} rows into {target_table}.")
    sync_end_ts = time.time()
    _notify_sync_done(market, source_table, target_table, sync_data_range, inserted, sync_start_ts, sync_end_ts, "success", "OK")


def _notify_sync_done(market: str, source: str, target: str, date_range: str, rows: int, start_ts: float, end_ts: float, status: str, message: str):
    from datetime import datetime
    started = datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
    finished = datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path_csv = LOGS_DIR / f"sync_recent_{market}_{ts}.csv"
    log_csv = build_log_csv(market, source, target, date_range, rows, started, finished, status, message)
    log_path_csv.write_text(log_csv, encoding="utf-8")
    print(f"Log: {log_path_csv}")
    send_log_to_slack(log_path_csv, market, rows, status, date_range)


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    load_dotenv()

    # -------------------------------------------------------------------
    # Load config dari file
    # -------------------------------------------------------------------
    config_path = os.getenv("SYNC_CONFIG_SGD_PATH", "sync_config_sgd.json")
    try:
        config = load_config(config_path)
        print(f"[OK] Config loaded from: {config_path}")
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Format JSON tidak valid: {e}")
        sys.exit(1)

    # Extract config values
    SOURCE_TABLE = config.get("source_table", "blue_whale_sgd")
    TARGET_TABLE = config.get("target_table", "rs_blue_whale_sgd")
    DAYS_BACK = config.get("days_back", 3)
    FILTER_COLUMN = config.get("filter_column", "agent_code")
    LINE_FILTER_MODE = config.get("line_filter_mode", "specific").lower()
    LINE_VALUES = config.get("line_values", [])
    SCHEMA = config.get("schema", "public")
    DATE_COLUMN = config.get("date_column", "txn_date")
    ORDER_COLUMN = config.get("order_column", "unique_key")
    BATCH_SIZE = config.get("batch_size", 2000)

    # Validasi line_filter_mode
    if LINE_FILTER_MODE not in ["all", "specific"]:
        print(f"[ERROR] line_filter_mode harus 'all' atau 'specific', ditemukan: {LINE_FILTER_MODE}")
        sys.exit(1)

    # Jika mode = "all", ignore line_values
    if LINE_FILTER_MODE == "all":
        LINE_VALUES = []
        print("[INFO] Line filter mode: ALL - semua line akan diambil")
    else:
        if not LINE_VALUES:
            print("[WARNING] line_filter_mode = 'specific' tapi line_values kosong!")
            print("[INFO] Akan otomatis mengambil semua line yang ada")
            LINE_VALUES = []

    today = date.today()  # Auto detect tanggal hari ini
    start_date, end_date = get_recent_days_window(DAYS_BACK)

    print("=" * 70)
    print("SYNC BLUE WHALE SGD - RECENT DAYS (AUTO DETECT)")
    print("=" * 70)
    print(f"Config file: {config_path}")
    print(f"Today date: {today} (auto detected from system)")
    print(f"Source table: {SOURCE_TABLE}")
    print(f"Target table: {TARGET_TABLE}")
    print(f"Days back: {DAYS_BACK} (H-1, H-2, H-3)")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Filter column: {FILTER_COLUMN}")
    print(f"Line filter mode: {LINE_FILTER_MODE.upper()}")
    if LINE_FILTER_MODE == "specific" and LINE_VALUES:
        print(f"Filter values: {', '.join(LINE_VALUES)}")
    else:
        print(f"Filter values: ALL (semua {FILTER_COLUMN})")
    print("=" * 70)

    redshift = connect_redshift()
    supabase = connect_supabase()

    try:
        # Jika LINE_VALUES kosong, ambil semua nilai filter column
        if not LINE_VALUES:
            # Ambil semua nilai filter column yang ada untuk periode tersebut
            qualified_source = f"{quote_name(SCHEMA)}.{quote_name(SOURCE_TABLE)}"
            date_column_q = quote_name(DATE_COLUMN)
            filter_column_q = quote_name(FILTER_COLUMN)
            filter_cursor = redshift.cursor()
            filter_cursor.execute(
                f"""
                SELECT DISTINCT {filter_column_q}
                FROM {qualified_source}
                WHERE {date_column_q} >= %s AND {date_column_q} < %s
                AND {filter_column_q} IS NOT NULL
                ORDER BY {filter_column_q}
                """,
                (start_date, end_date),
            )
            LINE_VALUES = [row[0] for row in filter_cursor.fetchall()]
            filter_cursor.close()
            
            if not LINE_VALUES:
                print(f"\n[INFO] Tidak ada data {FILTER_COLUMN} untuk periode tersebut.")
                return
            
            print(f"\n[INFO] Ditemukan {len(LINE_VALUES)} nilai {FILTER_COLUMN}: {', '.join(LINE_VALUES)}")

        sync_recent_days(
            redshift,
            supabase,
            source_table=SOURCE_TABLE,
            target_table=TARGET_TABLE,
            days_back=DAYS_BACK,
            line_values=LINE_VALUES,
            filter_column=FILTER_COLUMN,
            schema=SCHEMA,
            date_column=DATE_COLUMN,
            order_column=ORDER_COLUMN,
            batch_size=BATCH_SIZE,
            market="sgd",
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


if __name__ == "__main__":
    main()


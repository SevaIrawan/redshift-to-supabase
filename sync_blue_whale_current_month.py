import os
import sys
import time
from datetime import date

import redshift_connector
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv


def first_day_of_next_month(reference: date) -> date:
    if reference.month == 12:
        return date(reference.year + 1, 1, 1)
    return date(reference.year, reference.month + 1, 1)


def get_current_month_window(reference: date | None = None) -> tuple[date, date]:
    today = reference or date.today()
    start = today.replace(day=1)
    end = first_day_of_next_month(start)
    return start, end


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


def sync_table(
    redshift,
    supabase,
    source_table: str,
    target_table: str,
    schema: str = "public",
    order_column: str = "unique_key",
    batch_size: int = 2000,
):
    start_date, end_date = get_current_month_window()
    qualified_source = f"{quote_name(schema)}.{quote_name(source_table)}"
    qualified_target = f"{quote_name(schema)}.{quote_name(target_table)}"
    order_column_q = quote_name(order_column)
    txn_column = quote_name("txn_date")

    print("\n" + "=" * 70)
    print(f"SYNC {source_table} -> {target_table}")
    print("=" * 70)
    print(f"Window : {start_date} <= txn_date < {end_date}")

    info_cursor = redshift.cursor()
    info_cursor.execute(f"SELECT * FROM {qualified_source} LIMIT 0")
    columns = [desc[0] for desc in info_cursor.description]
    info_cursor.close()

    if "txn_date" not in columns:
        raise SystemExit("[ERROR] Kolom txn_date tidak ada di source table.")

    ensure_last_synced_column(supabase, qualified_target)

    # Delete current month slice in target
    delete_cursor = supabase.cursor()
    delete_sql = (
        f"DELETE FROM {qualified_target}"
        f" WHERE {txn_column} >= %s AND {txn_column} < %s"
    )
    delete_cursor.execute(delete_sql, (start_date, end_date))
    supabase.commit()
    deleted = delete_cursor.rowcount
    delete_cursor.close()
    print(f"Deleted {deleted} rows from target (current month).")

    # Count rows to sync
    count_cursor = redshift.cursor()
    count_cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {qualified_source}
        WHERE {txn_column} >= %s AND {txn_column} < %s
        """,
        (start_date, end_date),
    )
    rows_to_sync = count_cursor.fetchone()[0]
    count_cursor.close()
    print(f"Rows to import: {rows_to_sync:,}")
    if rows_to_sync == 0:
        print("[INFO] Tidak ada data bulan ini.")
        return

    # Prepare insert/upsert
    col_names = ", ".join(quote_name(col) for col in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_assignments = [
        f'{quote_name(col)} = EXCLUDED.{quote_name(col)}'
        for col in columns
        if col != "unique_key"
    ]
    update_assignments.append(f"{quote_name(LAST_SYNC_COLUMN)} = NOW()")
    update_clause = ", ".join(update_assignments)
    insert_sql = (
        f"INSERT INTO {qualified_target} ({col_names}, {quote_name(LAST_SYNC_COLUMN)}) "
        f"VALUES ({placeholders}, NOW()) "
        f"ON CONFLICT ({quote_name('unique_key')}) DO UPDATE SET {update_clause}"
    )

    fetch_cursor = redshift.cursor()
    fetch_cursor.execute(
        f"""
        SELECT *
        FROM {qualified_source}
        WHERE {txn_column} >= %s AND {txn_column} < %s
        ORDER BY {order_column_q}
        """,
        (start_date, end_date),
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
    print(f"[DONE] Synced {inserted:,} rows into {target_table}.")


TABLES = [
    {"source": "blue_whale_myr", "target": "rs_blue_whale_myr"},
    {"source": "blue_whale_sgd", "target": "rs_blue_whale_sgd"},
    {"source": "blue_whale_usc", "target": "rs_blue_whale_usc"},
]


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    load_dotenv()

    redshift = connect_redshift()
    supabase = connect_supabase()

    try:
        for table in TABLES:
            sync_table(
                redshift,
                supabase,
                source_table=table["source"],
                target_table=table["target"],
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
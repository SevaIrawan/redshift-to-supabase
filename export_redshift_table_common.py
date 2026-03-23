"""
Common helper for Redshift table export with period selection.
"""
import os
import sys
from math import ceil

import pandas as pd
import redshift_connector
from dotenv import load_dotenv

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

SCHEMA = "public"
OUTPUT_DIR = "exports_redshift"
BATCH_SIZE = 10000
PERIOD_COLUMN = "year"
EXCEL_MAX_ROWS = 1_048_576


def quote_ident(name: str) -> str:
    return '"{}"'.format(name.replace('"', '""'))


def _pick_period_filter(conn, qualified_table: str, columns: list[str]):
    if PERIOD_COLUMN not in columns:
        print(f"[WARN] Kolom period '{PERIOD_COLUMN}' tidak ditemukan. Export ALL data.")
        return "", [], "all"

    cur = conn.cursor()
    cur.execute(
        f"SELECT DISTINCT {quote_ident(PERIOD_COLUMN)} FROM {qualified_table} "
        f"WHERE {quote_ident(PERIOD_COLUMN)} IS NOT NULL ORDER BY 1 DESC"
    )
    values = [row[0] for row in cur.fetchall()]
    cur.close()

    if not values:
        print(f"[WARN] Nilai period '{PERIOD_COLUMN}' kosong. Export ALL data.")
        return "", [], "all"

    print("\nPilih period untuk export:")
    print("  0) ALL")
    for idx, val in enumerate(values, start=1):
        print(f"  {idx}) {val}")

    try:
        raw = input("Masukkan pilihan [0 untuk ALL]: ").strip()
    except EOFError:
        raw = "0"

    if raw == "" or raw == "0":
        return "", [], "all"

    try:
        picked = values[int(raw) - 1]
    except Exception:
        print("[WARN] Pilihan tidak valid. Pakai ALL.")
        return "", [], "all"

    return f" WHERE {quote_ident(PERIOD_COLUMN)} = %s", [picked], f"{PERIOD_COLUMN}={picked}"


def _fetch_dataframe(conn, qualified_table: str, where_sql: str, where_params: list):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {qualified_table} LIMIT 0")
    columns = [desc[0] for desc in cur.description]
    cur.close()

    cur = conn.cursor()
    sql_count = f"SELECT COUNT(*) FROM {qualified_table}{where_sql}"
    if where_params:
        cur.execute(sql_count, tuple(where_params))
    else:
        cur.execute(sql_count)
    total_rows = cur.fetchone()[0]
    cur.close()

    if total_rows == 0:
        return pd.DataFrame(columns=columns), 0

    all_rows = []
    offset = 0
    while offset < total_rows:
        cur = conn.cursor()
        sql = (
            f"SELECT * FROM {qualified_table}{where_sql} "
            f"ORDER BY 1 LIMIT {BATCH_SIZE} OFFSET {offset}"
        )
        if where_params:
            cur.execute(sql, tuple(where_params))
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        print(f"  Fetched: {len(all_rows):,} / {total_rows:,}")

    return pd.DataFrame(all_rows, columns=columns), total_rows


def export_table(table_name: str, output_format: str):
    output_format = output_format.lower().strip()
    if output_format not in {"excel", "csv"}:
        raise ValueError("output_format must be 'excel' or 'csv'")

    print("=" * 70)
    print(f"REDSHIFT EXPORT {SCHEMA}.{table_name} -> {output_format.upper()}")
    print("=" * 70)
    print(f"Output dir    : {OUTPUT_DIR}")
    print(f"Period column : {PERIOD_COLUMN}")
    print("=" * 70)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("\nConnecting to Redshift...")
    try:
        conn = redshift_connector.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT", "5439")),
            database=os.getenv("REDSHIFT_DATABASE"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
        )
        print("[OK] Connected to Redshift")
    except Exception as e:
        print(f"[ERROR] Failed to connect: {e}")
        return 1

    qualified = f"{quote_ident(SCHEMA)}.{quote_ident(table_name)}"

    # ambil nama kolom dulu untuk menentukan period option
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {qualified} LIMIT 0")
    columns = [d[0] for d in cur.description]
    cur.close()

    where_sql, where_params, period_label = _pick_period_filter(conn, qualified, columns)
    print(f"\nExport mode: {period_label}")

    df, total_rows = _fetch_dataframe(conn, qualified, where_sql, where_params)
    conn.close()
    print(f"\nTotal rows selected: {total_rows:,}")

    if output_format == "csv":
        filename = f"{table_name}.csv" if period_label == "all" else f"{table_name}_{period_label}.csv"
        path = os.path.join(OUTPUT_DIR, filename.replace("=", "_"))
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  -> {path} ({len(df):,} rows)")
        print("\n[DONE]")
        return 0

    # Excel
    if len(df) <= EXCEL_MAX_ROWS:
        filename = f"{table_name}.xlsx" if period_label == "all" else f"{table_name}_{period_label}.xlsx"
        path = os.path.join(OUTPUT_DIR, filename.replace("=", "_"))
        df.to_excel(path, index=False, engine="openpyxl")
        print(f"  -> {path} ({len(df):,} rows)")
        print("\n[DONE]")
        return 0

    # Auto split jika melebihi batas Excel
    parts = ceil(len(df) / EXCEL_MAX_ROWS)
    print(
        f"[INFO] Rows {len(df):,} > limit Excel {EXCEL_MAX_ROWS:,}. "
        f"Split ke {parts} file."
    )
    for idx in range(parts):
        start = idx * EXCEL_MAX_ROWS
        end = min((idx + 1) * EXCEL_MAX_ROWS, len(df))
        chunk = df.iloc[start:end]
        base = table_name if period_label == "all" else f"{table_name}_{period_label}"
        path = os.path.join(OUTPUT_DIR, f"{base}_part{idx + 1}.xlsx".replace("=", "_"))
        chunk.to_excel(path, index=False, engine="openpyxl")
        print(f"  -> {path} ({len(chunk):,} rows)")

    print("\n[DONE]")
    return 0

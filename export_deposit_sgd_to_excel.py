"""
Export table deposit_sgd (Supabase) ke Excel. Semua kolom. Tanpa Slack.
"""
import os
import sys

import pandas as pd
import psycopg2
from dotenv import load_dotenv

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

SCHEMA = "public"
TABLE_NAME = "deposit_sgd"
OUTPUT_DIR = "exports"
BATCH_SIZE = 10000

print("=" * 70)
print(f"EXPORT {SCHEMA}.{TABLE_NAME} -> Excel")
print("=" * 70)
print(f"Output dir: {OUTPUT_DIR}")
print("=" * 70)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"\n[OK] Created directory: {OUTPUT_DIR}")

print("\nConnecting to Supabase...")
try:
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT")),
        database=os.getenv("SUPABASE_DATABASE"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        sslmode="require",
    )
    print("[OK] Connected to Supabase")
except Exception as e:
    print(f"[ERROR] Failed to connect: {e}")
    sys.exit(1)

cursor = conn.cursor()
cursor.execute(f'SELECT COUNT(*) FROM {SCHEMA}."{TABLE_NAME}"')
total_rows = cursor.fetchone()[0]
cursor.execute(f'SELECT * FROM {SCHEMA}."{TABLE_NAME}" LIMIT 0')
columns = [desc[0] for desc in cursor.description]
cursor.close()

if total_rows == 0:
    df = pd.DataFrame(columns=columns)
else:
    all_rows = []
    offset = 0
    while offset < total_rows:
        cur = conn.cursor()
        cur.execute(
            f'SELECT * FROM {SCHEMA}."{TABLE_NAME}" ORDER BY 1 LIMIT {BATCH_SIZE} OFFSET {offset}'
        )
        rows = cur.fetchall()
        cur.close()
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        print(f"  Fetched: {len(all_rows):,} / {total_rows:,}")
    df = pd.DataFrame(all_rows, columns=columns)

conn.close()

path = os.path.join(OUTPUT_DIR, f"{TABLE_NAME}.xlsx")
df.to_excel(path, index=False, engine="openpyxl")
print(f"\n  -> {path} ({len(df):,} rows)")
print("\n[DONE]")

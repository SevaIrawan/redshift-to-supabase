"""
Export table deposit dari Redshift ke Excel. Semua kolom.
Env: REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
"""
import os
import sys

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
TABLE_NAME = "deposit"
OUTPUT_DIR = "exports_redshift"
BATCH_SIZE = 10000


def quote_ident(name: str) -> str:
    return '"{}"'.format(name.replace('"', '""'))


print("=" * 70)
print(f"REDSHIFT EXPORT {SCHEMA}.{TABLE_NAME} -> Excel")
print("=" * 70)
print(f"Output dir: {OUTPUT_DIR}")
print("=" * 70)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"\n[OK] Created directory: {OUTPUT_DIR}")

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
    sys.exit(1)

qualified = f"{quote_ident(SCHEMA)}.{quote_ident(TABLE_NAME)}"
cursor = conn.cursor()
cursor.execute(f"SELECT COUNT(*) FROM {qualified}")
total_rows = cursor.fetchone()[0]
cursor.execute(f"SELECT * FROM {qualified} LIMIT 0")
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
            f"SELECT * FROM {qualified} ORDER BY 1 LIMIT {BATCH_SIZE} OFFSET {offset}"
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

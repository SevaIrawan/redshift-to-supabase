"""
Export 2 MV ke 2 file Excel (Supabase), lalu upload ke Slack channel #data_marketing_usc.
- nd_usc_marketing_mv  -> exports/nd_usc_marketing_mv.xlsx
- nd_trans_usc_marketing_mv -> exports/nd_trans_usc_marketing_mv.xlsx
Upload ke Slack butuh: SLACK_BOT_TOKEN + SLACK_CHANNEL_ID_DATA_MARKETING_USC di .env.
"""
import os
import sys
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from slack_sdk import WebClient

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

# Configuration (ikuti pola export_supabase_*.py)
SCHEMA = "public"
MV_MEMBER = "nd_usc_marketing_mv"
MV_TRANSACTION = "nd_trans_usc_marketing_mv"
OUTPUT_DIR = "exports"
BATCH_SIZE = 10000

print("=" * 70)
print("EXPORT nd_usc_marketing_mv + nd_trans_usc_marketing_mv -> Excel")
print("=" * 70)
print(f"Schema: {SCHEMA}")
print(f"Output dir: {OUTPUT_DIR}")
print("=" * 70)

# Create output directory
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"\n[OK] Created directory: {OUTPUT_DIR}")

# Connect to Supabase (pola sama export_supabase_usc.py)
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


def export_table_to_excel(conn, table_name):
    """Fetch table in batches via cursor, build DataFrame, write Excel (tanpa pd.read_sql)."""
    cursor = conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM {SCHEMA}."{table_name}"')
    total_rows = cursor.fetchone()[0]
    cursor.execute(f'SELECT * FROM {SCHEMA}."{table_name}" LIMIT 0')
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    if total_rows == 0:
        return pd.DataFrame(columns=columns), 0

    all_rows = []
    offset = 0
    while offset < total_rows:
        cur = conn.cursor()
        cur.execute(
            f'SELECT * FROM {SCHEMA}."{table_name}" ORDER BY 1 LIMIT {BATCH_SIZE} OFFSET {offset}'
        )
        rows = cur.fetchall()
        cur.close()
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        print(f"  Fetched: {len(all_rows):,} / {total_rows:,}")

    df = pd.DataFrame(all_rows, columns=columns)
    return df, total_rows


# Export Member
print(f"\nReading {SCHEMA}.{MV_MEMBER} ...")
df_member, count_member = export_table_to_excel(conn, MV_MEMBER)
path_member = os.path.join(OUTPUT_DIR, f"{MV_MEMBER}.xlsx")
df_member.to_excel(path_member, index=False, engine="openpyxl")
print(f"  -> {path_member} ({count_member:,} rows)")

# Export Transaction
print(f"\nReading {SCHEMA}.{MV_TRANSACTION} ...")
df_trans, count_trans = export_table_to_excel(conn, MV_TRANSACTION)
path_trans = os.path.join(OUTPUT_DIR, f"{MV_TRANSACTION}.xlsx")
df_trans.to_excel(path_trans, index=False, engine="openpyxl")
print(f"  -> {path_trans} ({count_trans:,} rows)")

conn.close()

# Upload 2 file ke Slack #data_marketing_usc
bot_token = os.getenv("SLACK_BOT_TOKEN", "").strip()
channel_id = (
    os.getenv("SLACK_CHANNEL_ID_DATA_MARKETING_USC", "").strip()
    or os.getenv("SLACK_CHANNEL_ID", "").strip()
)
if bot_token and channel_id:
    client = WebClient(token=bot_token)
    for path, rows in [(path_member, count_member), (path_trans, count_trans)]:
        try:
            client.files_upload_v2(
                channel=channel_id,
                title=os.path.splitext(os.path.basename(path))[0],
                filename=os.path.basename(path),
                file=path,
                initial_comment=f"Export: {os.path.basename(path)} — {rows:,} rows",
            )
            print(f"  Slack upload OK: {os.path.basename(path)}")
        except Exception as e:
            print(f"  [Slack] Upload GAGAL {os.path.basename(path)}: {e}")
else:
    print("\n[INFO] Slack upload skip: set SLACK_BOT_TOKEN + SLACK_CHANNEL_ID_DATA_MARKETING_USC di .env")

print("\n[DONE]")

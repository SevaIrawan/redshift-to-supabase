import redshift_connector
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
import time
import sys

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

load_dotenv()

# Configuration
SOURCE_TABLE = "blue_whale_usc"
TARGET_TABLE = "rs_blue_whale_usc"
BATCH_SIZE = 2000
SOURCE_SCHEMA = "public"
TARGET_SCHEMA = "public"

print("=" * 70)
print("REDSHIFT TO SUPABASE MIGRATION - USC")
print("=" * 70)
print(f"Source: {SOURCE_SCHEMA}.{SOURCE_TABLE}")
print(f"Target: {TARGET_SCHEMA}.{TARGET_TABLE}")
print("=" * 70)

# Connect
print("\nConnecting...")
redshift = redshift_connector.connect(
    host=os.getenv('REDSHIFT_HOST'),
    port=int(os.getenv('REDSHIFT_PORT', 5439)),
    database=os.getenv('REDSHIFT_DATABASE'),
    user=os.getenv('REDSHIFT_USER'),
    password=os.getenv('REDSHIFT_PASSWORD')
)
print("[OK] Redshift connected")

supabase = psycopg2.connect(
    host=os.getenv('SUPABASE_HOST'),
    port=int(os.getenv('SUPABASE_PORT')),
    database=os.getenv('SUPABASE_DATABASE'),
    user=os.getenv('SUPABASE_USER'),
    password=os.getenv('SUPABASE_PASSWORD'),
    sslmode='require'
)
supabase.set_session(autocommit=False)
print("[OK] Supabase connected")

# Get source info
print("\nGetting source data...")
cursor = redshift.cursor()
cursor.execute(f'SELECT COUNT(*) FROM {SOURCE_SCHEMA}."{SOURCE_TABLE}"')
total_rows = cursor.fetchone()[0]
cursor.execute(f'SELECT * FROM {SOURCE_SCHEMA}."{SOURCE_TABLE}" LIMIT 0')
columns = [desc[0] for desc in cursor.description]
cursor.close()
print(f"Total rows: {total_rows:,}")
print(f"Columns: {len(columns)}")

# Check target
cursor = supabase.cursor()
cursor.execute(f'SELECT COUNT(*) FROM {TARGET_SCHEMA}."{TARGET_TABLE}"')
existing = cursor.fetchone()[0]
cursor.close()
print(f"Existing rows in target: {existing:,}")

# Drop constraint
print("\nDropping UNIQUE constraint...")
try:
    cursor = supabase.cursor()
    cursor.execute(f'ALTER TABLE {TARGET_SCHEMA}."{TARGET_TABLE}" DROP CONSTRAINT IF EXISTS rs_blue_whale_usc_unique_key_unique')
    supabase.commit()
    cursor.close()
    print("[OK] Constraint dropped")
except Exception as e:
    print(f"[WARNING] {e}")

# Clear table if needed
if existing > 0:
    clear = input(f"\nClear {existing:,} existing rows? (yes/no): ").strip().lower()
    if clear == 'yes':
        cursor = supabase.cursor()
        cursor.execute(f'TRUNCATE TABLE {TARGET_SCHEMA}."{TARGET_TABLE}"')
        supabase.commit()
        cursor.close()
        print("[OK] Table cleared")

# Confirm
confirm = input(f"\nMigrate {total_rows:,} rows? (yes/no): ").strip().lower()
if confirm != 'yes':
    print("Cancelled")
    redshift.close()
    supabase.close()
    exit(0)

# Prepare insert
col_names = ', '.join([f'"{c}"' for c in columns])
placeholders = ', '.join(['%s'] * len(columns))
insert_sql = f'INSERT INTO {TARGET_SCHEMA}."{TARGET_TABLE}" ({col_names}) VALUES ({placeholders})'

print("\n" + "=" * 70)
print("MIGRATION STARTED")
print("=" * 70)

start = time.time()
offset = 0
migrated = 0
batch_num = 0
total_batches = (total_rows // BATCH_SIZE) + 1

while offset < total_rows:
    batch_num += 1
    
    # Fetch
    print(f"[{batch_num}/{total_batches}] Fetching {offset:,}...", end=' ')
    cursor = redshift.cursor()
    cursor.execute(f'SELECT * FROM {SOURCE_SCHEMA}."{SOURCE_TABLE}" ORDER BY unique_key LIMIT {BATCH_SIZE} OFFSET {offset}')
    rows = cursor.fetchall()
    cursor.close()
    
    if not rows:
        break
    
    print(f"Got {len(rows)}", end=' ')
    
    # Insert
    try:
        cursor = supabase.cursor()
        execute_batch(cursor, insert_sql, rows, page_size=500)
        supabase.commit()
        cursor.close()
        
        migrated += len(rows)
        offset += len(rows)
        
        elapsed = time.time() - start
        speed = migrated / elapsed
        progress = (offset / total_rows) * 100
        eta = (total_rows - offset) / speed / 60 if speed > 0 else 0
        
        print(f"| Inserted | {progress:.1f}% | {speed:.0f} r/s | ETA: {eta:.1f}m")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        supabase.rollback()
        break

duration = time.time() - start

print("\n" + "=" * 70)
print("MIGRATION COMPLETED")
print("=" * 70)
print(f"Migrated: {migrated:,} / {total_rows:,}")
print(f"Duration: {duration/60:.1f} minutes")

# Verify
cursor = supabase.cursor()
cursor.execute(f'SELECT COUNT(*) FROM {TARGET_SCHEMA}."{TARGET_TABLE}"')
final = cursor.fetchone()[0]
cursor.close()
print(f"Final count: {final:,}")

# Add constraint back
print("\nAdding UNIQUE constraint back...")
try:
    cursor = supabase.cursor()
    cursor.execute(f'ALTER TABLE {TARGET_SCHEMA}."{TARGET_TABLE}" ADD CONSTRAINT rs_blue_whale_usc_unique_key_unique UNIQUE (unique_key)')
    supabase.commit()
    cursor.close()
    print("[OK] Constraint added")
except Exception as e:
    print(f"[WARNING] Could not add constraint: {e}")
    print("Check for duplicates first")

redshift.close()
supabase.close()
print("\n[DONE]")


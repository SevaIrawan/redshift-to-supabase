import psycopg2
import csv
import os
from dotenv import load_dotenv
import sys
from datetime import datetime

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

load_dotenv()

# Configuration
TABLE_NAME = "blue_whale_usc"
SCHEMA = "public"
OUTPUT_DIR = "exports"
BATCH_SIZE = 10000  # Fetch dalam batch untuk menghindari memory issue

print("=" * 70)
print("EXPORT DATA FROM SUPABASE - USC (Support Khmer Language)")
print("=" * 70)
print(f"Table: {SCHEMA}.{TABLE_NAME}")
print("=" * 70)

# Create output directory
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"\n[OK] Created directory: {OUTPUT_DIR}")

# Connect to Supabase
print("\nConnecting to Supabase...")
try:
    supabase = psycopg2.connect(
        host=os.getenv('SUPABASE_HOST'),
        port=int(os.getenv('SUPABASE_PORT')),
        database=os.getenv('SUPABASE_DATABASE'),
        user=os.getenv('SUPABASE_USER'),
        password=os.getenv('SUPABASE_PASSWORD'),
        sslmode='require'
    )
    print("[OK] Connected to Supabase")
except Exception as e:
    print(f"[ERROR] Failed to connect: {e}")
    sys.exit(1)

# Get total rows
print("\nGetting table info...")
cursor = supabase.cursor()
cursor.execute(f'SELECT COUNT(*) FROM {SCHEMA}."{TABLE_NAME}"')
total_rows = cursor.fetchone()[0]

# Get column names
cursor.execute(f'SELECT * FROM {SCHEMA}."{TABLE_NAME}" LIMIT 0')
columns = [desc[0] for desc in cursor.description]
cursor.close()

print(f"Total rows: {total_rows:,}")
print(f"Columns: {len(columns)}")

if total_rows == 0:
    print("\n[WARNING] Table is empty. Nothing to export.")
    supabase.close()
    sys.exit(0)

# Generate output filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = os.path.join(OUTPUT_DIR, f"{TABLE_NAME}_{timestamp}.csv")

print(f"\nOutput file: {output_file}")
print("[INFO] File will use UTF-8 with BOM for Khmer language support")

# Export data
print("\n" + "=" * 70)
print("EXPORT STARTED")
print("=" * 70)

start_time = datetime.now()
exported = 0
offset = 0

# Open CSV file for writing dengan utf-8-sig untuk BOM (Excel compatibility)
# Ini memastikan text Khmer dan karakter unicode lainnya tetap benar
with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
    writer = csv.writer(csvfile)
    
    # Write header
    writer.writerow(columns)
    
    # Fetch and write data in batches
    while offset < total_rows:
        cursor = supabase.cursor()
        
        # Fetch batch - gunakan uniquekey (tanpa underscore) atau tanpa ORDER BY
        query = f'SELECT * FROM {SCHEMA}."{TABLE_NAME}" ORDER BY uniquekey LIMIT {BATCH_SIZE} OFFSET {offset}'
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        
        if not rows:
            break
        
        # Process rows untuk memastikan encoding benar (khusus untuk text Khmer)
        processed_rows = []
        for row in rows:
            processed_row = []
            for cell in row:
                if cell is None:
                    processed_row.append(None)
                elif isinstance(cell, str):
                    # Pastikan string di-handle dengan benar untuk Khmer
                    processed_row.append(cell)
                elif isinstance(cell, bytes):
                    # Decode bytes jika ada
                    try:
                        processed_row.append(cell.decode('utf-8'))
                    except:
                        processed_row.append(str(cell))
                else:
                    processed_row.append(cell)
            processed_rows.append(processed_row)
        
        # Write rows to CSV
        writer.writerows(processed_rows)
        exported += len(rows)
        offset += len(rows)
        
        # Progress
        progress = (offset / total_rows) * 100
        print(f"Exported: {exported:,} / {total_rows:,} ({progress:.1f}%)")

duration = datetime.now() - start_time

print("\n" + "=" * 70)
print("EXPORT COMPLETED")
print("=" * 70)
print(f"Exported: {exported:,} rows")
print(f"Duration: {duration.total_seconds():.1f} seconds")
print(f"Output file: {output_file}")
print(f"File size: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
print("[OK] File supports Khmer language (UTF-8 with BOM)")

supabase.close()
print("\n[DONE]")

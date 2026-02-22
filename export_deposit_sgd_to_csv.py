import os
import sys
import time
import csv
import redshift_connector
from dotenv import load_dotenv

DATE_TYPES = {
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
    "timestamp",
    "timestamptz",
}


def quote_ident(name: str) -> str:
    return '"{}"'.format(name.replace('"', '""'))


if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
SOURCE_SCHEMA = "public"
SOURCE_TABLE = "deposit_sgd"
ORDER_COLUMN = "unique_key"
YEAR_COLUMN = "year"
BATCH_SIZE = 20000
OUTPUT_DIR = "exports"
OUTPUT_PREFIX = "deposit_sgd_export"

print("=" * 70)
print("REDSHIFT TO CSV EXPORT - DEPOSIT SGD (per tahun)")
print("=" * 70)
print(f"Source table : {SOURCE_SCHEMA}.{SOURCE_TABLE}")
print(f"Order column : {ORDER_COLUMN}")
print(f"Year column  : {YEAR_COLUMN}")
print(f"Batch size   : {BATCH_SIZE}")
print(f"Output dir   : {OUTPUT_DIR}")
print("=" * 70)

# -------------------------------------------------------------------
# Connect ke Redshift
# -------------------------------------------------------------------
print("\nConnecting to Redshift...")
redshift = redshift_connector.connect(
    host=os.getenv("REDSHIFT_HOST"),
    port=int(os.getenv("REDSHIFT_PORT", 5439)),
    database=os.getenv("REDSHIFT_DATABASE"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD"),
)
print("[OK] Redshift connected")

# -------------------------------------------------------------------
# Hitung total rows & ambil nama kolom
# -------------------------------------------------------------------
print("\nGetting source info...")
cur = redshift.cursor()
qualified_table = f"{quote_ident(SOURCE_SCHEMA)}.{quote_ident(SOURCE_TABLE)}"

cur.execute(f"SELECT COUNT(*) FROM {qualified_table}")
total_rows = cur.fetchone()[0]
print(f"Total rows   : {total_rows:,}")

cur.execute(f"SELECT * FROM {qualified_table} LIMIT 0")
columns = [desc[0] for desc in cur.description]
cur.close()
print(f"Columns      : {len(columns)}")
print(f"Col names    : {', '.join(columns)}")

if YEAR_COLUMN not in columns:
    print(f"\n[ERROR] Kolom '{YEAR_COLUMN}' tidak ditemukan di tabel.")
    print("Silakan ubah YEAR_COLUMN sesuai kolom tanggal/tahun yang tersedia.")
    redshift.close()
    sys.exit(1)

# Ambil tipe data kolom tahun
type_cursor = redshift.cursor()
type_cursor.execute(
    """
    SELECT data_type
    FROM information_schema.columns
    WHERE table_schema = %s
    AND table_name = %s
    AND column_name = %s
    """,
    (SOURCE_SCHEMA, SOURCE_TABLE, YEAR_COLUMN),
)
type_info = type_cursor.fetchone()
type_cursor.close()

if not type_info:
    print(f"\n[ERROR] Tidak bisa mengambil metadata untuk kolom '{YEAR_COLUMN}'.")
    redshift.close()
    sys.exit(1)

year_data_type = type_info[0]
quoted_year_column = quote_ident(YEAR_COLUMN)

def build_year_expr() -> str:
    if year_data_type in DATE_TYPES:
        return f"CAST(EXTRACT(YEAR FROM {quoted_year_column}) AS INTEGER)"
    return f"CAST({quoted_year_column} AS INTEGER)"

year_expr = build_year_expr()

if total_rows == 0:
    print("\n[INFO] Tidak ada data di source table. Program dihentikan.")
    redshift.close()
    sys.exit(0)

# Pastikan output directory ada
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Ambil daftar tahun yang akan diexport
year_cursor = redshift.cursor()
year_cursor.execute(
    f"""
    SELECT DISTINCT {year_expr} AS year
    FROM {qualified_table}
    WHERE {year_expr} IS NOT NULL
    ORDER BY year
    """
)
years = [row[0] for row in year_cursor.fetchall()]
year_cursor.close()

if not years:
    print("\n[WARNING] Tidak ada nilai tahun yang terbaca. Export dibatalkan.")
    redshift.close()
    sys.exit(0)

print("\nTahun yang akan diexport:", ", ".join(str(y) for y in years))
confirm = input(
    "\nExport data per tahun sesuai daftar di atas? (yes/no): "
).strip().lower()
if confirm != "yes":
    print("Cancelled.")
    redshift.close()
    sys.exit(0)

quoted_order = quote_ident(ORDER_COLUMN)
total_exported = 0
files_created = []
start_time = time.time()

for year_value in years:
    year_start = time.time()

    count_cursor = redshift.cursor()
    count_cursor.execute(
        f"SELECT COUNT(*) FROM {qualified_table} WHERE {year_expr} = %s",
        (year_value,),
    )
    year_total = count_cursor.fetchone()[0]
    count_cursor.close()

    if year_total == 0:
        print(f"\n[Tahun {year_value}] Tidak ada baris, dilewati.")
        continue

    output_file = os.path.join(
        OUTPUT_DIR, f"{OUTPUT_PREFIX}_{year_value}.csv"
    )

    print("\n" + "=" * 70)
    print(f"Export tahun {year_value}")
    print(f"Baris       : {year_total:,}")
    print(f"Output file : {output_file}")
    print("=" * 70)

    data_cursor = redshift.cursor()
    data_cursor.execute(
        f"""
        SELECT *
        FROM {qualified_table}
        WHERE {year_expr} = %s
        ORDER BY {quoted_order}
        """,
        (year_value,),
    )

    # Gunakan utf-8-sig untuk BOM (Excel compatibility) dan pastikan text Khmer tetap benar
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        batch_num = 0
        exported_year = 0

        while True:
            rows = data_cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break

            batch_num += 1
            # Pastikan semua data di-handle dengan benar untuk encoding
            processed_rows = []
            for row in rows:
                processed_row = []
                for cell in row:
                    if cell is None:
                        processed_row.append(None)
                    elif isinstance(cell, str):
                        # Pastikan string di-handle dengan benar
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
            
            writer.writerows(processed_rows)
            exported_year += len(rows)

            elapsed = time.time() - year_start
            speed = exported_year / elapsed if elapsed > 0 else 0
            progress = (exported_year / year_total) * 100
            eta_min = (
                (year_total - exported_year) / speed / 60 if speed > 0 else 0
            )

            print(
                f"[Batch {batch_num}] {year_value} | "
                f"{exported_year:,}/{year_total:,} "
                f"({progress:.1f}%) | {speed:.0f} r/s | ETA: {eta_min:.1f} m"
            )

    data_cursor.close()
    total_exported += exported_year
    files_created.append((year_value, output_file, exported_year))
    print(
        f"[DONE] Tahun {year_value} | "
        f"{exported_year:,} baris -> {os.path.abspath(output_file)}"
    )

redshift.close()
duration = time.time() - start_time

print("\n" + "=" * 70)
print("EXPORT COMPLETED")
print("=" * 70)
print(f"Total exported : {total_exported:,} rows")
print(f"Files created  : {len(files_created)}")
for year_value, file_path, count in files_created:
    print(f"  - {year_value}: {count:,} rows -> {file_path}")
print(f"Duration       : {duration/60:.1f} minutes")
print("\n[DONE]")

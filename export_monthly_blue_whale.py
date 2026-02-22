import os
import sys
import csv
from datetime import datetime, date, timedelta
import argparse
import redshift_connector
from dotenv import load_dotenv


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export satu bulan data blue_whale dari Redshift."
    )
    parser.add_argument(
        "--table",
        default="blue_whale_myr",
        help="Nama tabel di Redshift (default: blue_whale_myr)",
    )
    parser.add_argument(
        "--month",
        default=datetime.today().strftime("%Y-%m"),
        help="Bulan yang ingin diexport (format YYYY-MM).",
    )
    parser.add_argument(
        "--schema", default="public", help="Schema tabel (default: public)"
    )
    parser.add_argument(
        "--output-dir",
        default="monthly_exports",
        help="Output directory tempat file CSV disimpan.",
    )
    return parser.parse_args()


def month_window(reference: str) -> tuple[date, date]:
    base = datetime.strptime(reference, "%Y-%m").date()
    start = base.replace(day=1)
    next_month = start.replace(day=28) + timedelta(days=4)
    end = next_month.replace(day=1)
    return start, end


def connect_redshift():
    return redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        database=os.getenv("REDSHIFT_DATABASE"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
    )


def quote_ident(name: str) -> str:
    return '"{}"'.format(name.replace('"', '""'))


def main():
    load_dotenv()
    args = parse_args()

    start_date, end_date = month_window(args.month)
    os.makedirs(args.output_dir, exist_ok=True)

    table_ref = f'{quote_ident(args.schema)}.{quote_ident(args.table)}'
    output_path = os.path.join(
        args.output_dir, f"{args.table}_{start_date.strftime('%Y%m')}.csv"
    )

    print("=" * 70)
    print(f"Export bulan {args.month} dari {table_ref}")
    print(f"Rentang txn_date : {start_date} <= txn_date < {end_date}")
    print(f"Output file      : {output_path}")
    print("=" * 70)

    conn = connect_redshift()
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_ref} LIMIT 0')
    columns = [desc[0] for desc in cursor.description]
    required = ["txn_date"]
    if not all(col in columns for col in required):
        raise SystemExit("[ERROR] Kolom 'txn_date' tidak ditemukan di tabel.")

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_ref}
        WHERE txn_date >= %s AND txn_date < %s
        """,
        (start_date, end_date),
    )
    total_rows = cursor.fetchone()[0]
    print(f"Total baris bulan ini: {total_rows:,}")

    fetch_cursor = conn.cursor()
    fetch_cursor.execute(
        f"""
        SELECT *
        FROM {table_ref}
        WHERE txn_date >= %s AND txn_date < %s
        ORDER BY txn_date, unique_key
        """,
        (start_date, end_date),
    )

    # Gunakan utf-8-sig untuk BOM (Excel compatibility) dan pastikan text Khmer tetap benar
    with open(output_path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        exported = 0
        batch = 0

        while True:
            rows = fetch_cursor.fetchmany(2000)
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
            exported += len(rows)
            batch += 1
            print(f"[Batch {batch}] Exported {exported:,}/{total_rows:,}")

    fetch_cursor.close()
    cursor.close()
    conn.close()

    print("=" * 70)
    print(f"Export selesai: {exported:,} rows ke {output_path}")


if __name__ == "__main__":
    main()


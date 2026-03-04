"""
Script BARU: Sync 2 MV dari Supabase ke Google Sheet (2 tab).
- nd_usc_marketing_mv  -> sheet/tab 1
- nd_trans_usc_marketing_mv -> sheet/tab 2
Tidak mengubah script lain di project.

Env (.env):
  Supabase: SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD
  Google:   GOOGLE_SERVICE_ACCOUNT_JSON  = path ke file JSON (atau kosong, pakai default)
            GOOGLE_SHEET_ID               = ID spreadsheet (dari URL: /d/SHEET_ID/edit)
            GOOGLE_SHEET_TAB_MEMBER      = nama tab untuk nd_usc_marketing_mv (default: nd_usc_marketing_mv)
            GOOGLE_SHEET_TAB_TRANSACTION = nama tab untuk nd_trans_usc_marketing_mv (default: nd_trans_usc_marketing_mv)

Share Google Sheet ke client_email dari JSON (Editor). Dependency: gspread + google-auth (ada di requirements.txt)
"""
import os
import sys
import time
from pathlib import Path

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
MV_MEMBER = "nd_usc_marketing_mv"
MV_TRANSACTION = "nd_trans_usc_marketing_mv"
BATCH_SIZE = 10000
SHEET_BATCH_ROWS = 3000  # baris per batch (kecil = kurang timeout, delay 0.8s antar batch)

# Kolom ini tidak ditulis ke Google Sheet (untuk kedua tabel)
EXCLUDE_COLUMNS = ["user_key", "userkey", "user_unique", "user_identity"]


def export_table_to_dataframe(conn, table_name: str):
    """Baca tabel dari Supabase per batch, return (DataFrame, total_rows)."""
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {SCHEMA}."{table_name}"')
    total_rows = cur.fetchone()[0]
    cur.execute(f'SELECT * FROM {SCHEMA}."{table_name}" LIMIT 0')
    columns = [d[0] for d in cur.description]
    cur.close()

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


def get_gspread_client():
    """Auth ke Google pakai Service Account JSON. Butuh: gspread, google-auth."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("[ERROR] Install: pip install gspread google-auth")
        return None

    json_path = (
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        or "google-service-account.json"
    )
    if not os.path.isfile(json_path):
        # cari di folder project
        base = Path(__file__).resolve().parent
        for name in (
            "google-service-account.json",
            "data-marketing-usc-0c5b5d75895b.json",
        ):
            p = base / name
            if p.is_file():
                json_path = str(p)
                break
        else:
            print(f"[ERROR] File JSON tidak ditemukan: {json_path}")
            print("        Letakkan file JSON Service Account di folder project atau set GOOGLE_SERVICE_ACCOUNT_JSON di .env")
            return None

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(json_path, scopes=scopes)
    client = gspread.authorize(creds)
    # Timeout 5 min (300s) supaya request besar tidak hang
    try:
        client.set_timeout(300)
    except Exception:
        pass
    return client


def write_dataframe_to_sheet(ws, df: pd.DataFrame, sheet_title: str = "") -> None:
    """
    Hapus data lama di sheet (kecuali nanti kita overwrite dengan header baru),
    lalu tulis data baru dari Supabase. Alur: clear -> resize -> tulis.
    """
    header = [df.columns.tolist()]
    rows = df.fillna("").astype(str).values.tolist()
    need_rows = len(rows) + 1
    need_cols = max(len(df.columns), 26)

    # 1. Hapus data lama dulu (clear semua cell)
    if sheet_title:
        print(f"    [{sheet_title}] Menghapus data lama...")
    ws.clear()

    if df.empty:
        return

    # 2. Resize worksheet supaya muat data baru
    try:
        if need_rows > ws.row_count or need_cols > ws.col_count:
            ws.resize(rows=need_rows, cols=need_cols)
    except Exception:
        try:
            ws.resize(rows=need_rows)
        except Exception:
            pass

    # 3. Tulis data baru: kumpulkan semua batch lalu kirim sekali pakai batch_update (lebih stabil)
    if sheet_title:
        print(f"    [{sheet_title}] Menyiapkan {len(rows):,} baris...", flush=True)
    n_cols = len(df.columns)
    batch_updates = []
    start = 0
    while start < len(rows):
        end = min(start + SHEET_BATCH_ROWS, len(rows))
        if start == 0:
            data = header + rows[start:end]
            range_start = "A1"
        else:
            data = rows[start:end]
            range_start = f"A{start + 2}"
        if data:
            # Kolom terakhir dalam A1 (1=A, 26=Z, 27=AA, ...)
            c, col_letters = n_cols, ""
            while c > 0:
                c, r = divmod(c - 1, 26)
                col_letters = chr(65 + r) + col_letters
            first_row = 1 if start == 0 else start + 2
            last_row = first_row + len(data) - 1
            range_a1 = f"{range_start}:{col_letters}{last_row}"
            batch_updates.append({"range": range_a1, "values": data})
        start = end
        if sheet_title and start < len(rows):
            print(f"    Prepared {start:,} / {len(rows):,} rows", flush=True)

    # Kirim maksimal 5 range per request (satu API call = lebih stabil)
    CHUNK = 5
    for i in range(0, len(batch_updates), CHUNK):
        chunk = batch_updates[i : i + CHUNK]
        try:
            ws.batch_update(chunk, value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"    [ERROR] batch_update gagal: {e}", flush=True)
            raise
        done = sum(len(b["values"]) for b in batch_updates[: i + len(chunk)]) - 1
        if done < len(rows) and sheet_title:
            print(f"    Written {done:,} / {len(rows):,} rows", flush=True)
        time.sleep(0.3)
    if sheet_title and rows:
        print(f"    [{sheet_title}] Selesai: {len(rows):,} baris.", flush=True)


def main():
    print("=" * 70)
    print("SYNC 2 MV (Supabase) -> Google Sheet")
    print("=" * 70)

    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not sheet_id:
        print("[ERROR] Set GOOGLE_SHEET_ID di .env (dari URL: .../d/SHEET_ID/edit)")
        sys.exit(1)

    tab_member = os.getenv("GOOGLE_SHEET_TAB_MEMBER", MV_MEMBER).strip() or MV_MEMBER
    tab_trans = (
        os.getenv("GOOGLE_SHEET_TAB_TRANSACTION", MV_TRANSACTION).strip()
        or MV_TRANSACTION
    )

    # Supabase
    print("\nConnecting to Supabase...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("SUPABASE_HOST"),
            port=int(os.getenv("SUPABASE_PORT", "5432")),
            database=os.getenv("SUPABASE_DATABASE"),
            user=os.getenv("SUPABASE_USER"),
            password=os.getenv("SUPABASE_PASSWORD"),
            sslmode="require",
        )
        print("[OK] Connected to Supabase")
    except Exception as e:
        print(f"[ERROR] Supabase: {e}")
        sys.exit(1)

    # Baca 2 tabel
    print(f"\nReading {SCHEMA}.{MV_MEMBER} ...")
    df_member, count_member = export_table_to_dataframe(conn, MV_MEMBER)
    df_member = df_member.drop(columns=EXCLUDE_COLUMNS, errors="ignore")
    print(f"  -> {count_member:,} rows (kolom dikecualikan: {EXCLUDE_COLUMNS})")

    print(f"\nReading {SCHEMA}.{MV_TRANSACTION} ...")
    df_trans, count_trans = export_table_to_dataframe(conn, MV_TRANSACTION)
    df_trans = df_trans.drop(columns=EXCLUDE_COLUMNS, errors="ignore")
    print(f"  -> {count_trans:,} rows (kolom dikecualikan: {EXCLUDE_COLUMNS})")
    conn.close()

    # Google Sheet
    print("\nConnecting to Google Sheet...")
    client = get_gspread_client()
    if not client:
        sys.exit(1)
    try:
        sheet = client.open_by_key(sheet_id)
    except Exception as e:
        print(f"[ERROR] Buka spreadsheet: {e}")
        print("        Pastikan Sheet di-share ke client_email dari file JSON (Editor)")
        sys.exit(1)

    def get_or_create_worksheet(sheet_obj, title):
        try:
            return sheet_obj.worksheet(title)
        except Exception:
            return sheet_obj.add_worksheet(title=title, rows=1000, cols=26)

    # Tab 1: Member — hapus data lama, tulis data baru
    ws_member = get_or_create_worksheet(sheet, tab_member)
    print(f"\nSheet '{tab_member}': hapus data lama -> tulis {count_member:,} rows")
    write_dataframe_to_sheet(ws_member, df_member, sheet_title=tab_member)
    print(f"  OK: {tab_member}")

    # Tab 2: Transaction — hapus data lama, tulis data baru
    ws_trans = get_or_create_worksheet(sheet, tab_trans)
    print(f"\nSheet '{tab_trans}': hapus data lama -> tulis {count_trans:,} rows")
    write_dataframe_to_sheet(ws_trans, df_trans, sheet_title=tab_trans)
    print(f"  OK: {tab_trans}")

    print("\n[DONE] Google Sheet updated.")


if __name__ == "__main__":
    main()

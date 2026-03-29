"""
Sync rs_blue_whale_myr → blue_whale_myr (Supabase, H-2). Log CSV.
"""
import sys
from datetime import date, datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

from sync_rs_to_blue_whale_common import (
    LOGS_DIR,
    build_log_csv,
    connect_supabase,
    run_sync,
)

MARKET = "myr"
SOURCE_TABLE = "rs_blue_whale_myr"
TARGET_TABLE = "blue_whale_myr"


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_1 = date.today() - timedelta(days=1)
    date_2 = date.today() - timedelta(days=2)
    print("=" * 60)
    print(f"Sync rs_* → blue_whale_* (MYR) — H-2 = 2 hari: {date_2}, {date_1}")
    print("=" * 60)

    conn = connect_supabase()
    try:
        rows, dates = run_sync(conn, SOURCE_TABLE, TARGET_TABLE)
        status = "success"
        message = "OK"
        print(f"[OK] Inserted {rows:,} rows.")
    except Exception as e:
        rows = 0
        dates = [date_2, date_1]
        status = "error"
        message = str(e)
        print(f"[ERROR] {e}")
        sys.exit(1)
    finally:
        conn.close()

    finished = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path_csv = LOGS_DIR / f"rs_to_blue_myr_{ts}.csv"
    log_csv = build_log_csv(MARKET, SOURCE_TABLE, TARGET_TABLE, dates, rows, started, finished, status, message)
    log_path_csv.write_text(log_csv, encoding="utf-8")
    print(f"Log: {log_path_csv}")


if __name__ == "__main__":
    main()

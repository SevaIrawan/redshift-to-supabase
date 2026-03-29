"""TY: rs_blue_whale_sgd → blue_whale_sgd (kemarin + hari ini)."""
import sys
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()

from ty_rs_to_blue_whale_common import connect_supabase, run_sync_yesterday_today

SOURCE = "rs_blue_whale_sgd"
TARGET = "blue_whale_sgd"


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    today = date.today()
    yesterday = today - timedelta(days=1)
    print("=" * 60)
    print(f"TY rs→blue (SGD): {yesterday}, {today}")
    print("=" * 60)
    conn = connect_supabase()
    try:
        rows, _ = run_sync_yesterday_today(conn, SOURCE, TARGET)
        print(f"[OK] {rows:,} rows.")
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

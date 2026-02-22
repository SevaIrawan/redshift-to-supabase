"""
Validasi KPI per brand (rs_blue_whale_usc, rs_blue_whale_myr, rs_blue_whale_sgd).
Date range: H-1 sahaja (1 hari).
Laporan: satu baris per txn_date per brand — txn_date, deposit_cases (SUM), deposit_amount (SUM),
net_profit (SUM), active_members (COUNT DISTINCT user_unique WHERE deposit_cases > 0).
Log disimpan ke .csv dan dikirim ke Slack.
"""
import csv
import io
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from slack_sdk import WebClient

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
LOGS_DIR = SCRIPT_DIR / "logs"
DAYS_BACK = 1
TABLES = ["rs_blue_whale_usc", "rs_blue_whale_myr", "rs_blue_whale_sgd"]
CURRENCY_ORDER = {t: i for i, t in enumerate(TABLES)}


def get_recent_days_window(days_back: int) -> tuple[date, date]:
    today = date.today()
    end_date = today
    start_date = today - timedelta(days=days_back)
    return start_date, end_date


def connect_supabase():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT")),
        database=os.getenv("SUPABASE_DATABASE"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        sslmode="require",
    )
    conn.autocommit = True
    return conn


def validate_kpi_table(conn, table: str, start_date: date, end_date: date) -> list[dict]:
    """Return list of rows: txn_date, line, deposit_cases, deposit_amount, net_profit, active_members (per line per txn_date)."""
    sql = """
    SELECT
      txn_date,
      line,
      COALESCE(SUM(deposit_cases), 0) AS deposit_cases,
      COALESCE(SUM(deposit_amount), 0) AS deposit_amount,
      COALESCE(SUM(net_profit), 0) AS net_profit,
      COUNT(DISTINCT CASE WHEN COALESCE(deposit_cases, 0) > 0 THEN user_unique END)::INT AS active_members
    FROM public.""" + table + """
    WHERE txn_date >= %s AND txn_date < %s
    GROUP BY txn_date, line
    ORDER BY txn_date, line
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (start_date, end_date))
        rows = cur.fetchall()
        return [
            {
                "txn_date": str(r[0]),
                "line": str(r[1]) if r[1] is not None else "",
                "deposit_cases": float(r[2]) if r[2] is not None else 0,
                "deposit_amount": float(r[3]) if r[3] is not None else 0,
                "net_profit": float(r[4]) if r[4] is not None else 0,
                "active_members": int(r[5]) if r[5] is not None else 0,
            }
            for r in rows
        ]
    finally:
        cur.close()


def _format_title_dates(start_date: date, end_date: date) -> str:
    """Contoh: 18/19/20 Feb 2026"""
    days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days)]
    day_strs = [str(d.day) for d in days]
    month_year = start_date.strftime("%b %Y")
    return "/".join(day_strs) + " " + month_year


def _parse_date(d) -> date:
    """Parse txn_date dari string atau date object."""
    if isinstance(d, date):
        return d
    s = str(d)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.split()[0] if " " in s else s, fmt).date()
        except ValueError:
            continue
    return date.today()


def build_report_csv(report: dict, started: str, finished: str, start_date: date, end_date: date) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    title_dates = _format_title_dates(start_date, end_date)
    w.writerow([f"# KPI Validation Report - {title_dates}"])
    try:
        started_ts = datetime.strptime(started, "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
        finished_ts = datetime.strptime(finished, "%Y-%m-%d %H:%M:%S").strftime("%Y/%m/%d %H:%M:%S")
    except ValueError:
        started_ts, finished_ts = started, finished
    w.writerow(["# Started", started_ts])
    w.writerow(["# Finished", finished_ts])
    w.writerow(["table", "txn_date", "line", "deposit_cases", "deposit_amount", "net_profit", "active_members"])
    all_rows = []
    for table, rows in report.items():
        for r in rows:
            all_rows.append((table, r))
    all_rows.sort(key=lambda x: (CURRENCY_ORDER.get(x[0], 999), _parse_date(x[1]["txn_date"]), x[1].get("line") or ""))
    for table, r in all_rows:
        d = _parse_date(r["txn_date"])
        txn_date_fmt = d.strftime("%Y/%m/%d")
        dc = int(r["deposit_cases"])
        da = float(r["deposit_amount"])
        np = float(r["net_profit"])
        am = int(r["active_members"])
        w.writerow([
            table,
            txn_date_fmt,
            r.get("line") or "",
            f"{dc:,}",
            f"{da:,.2f}",
            f"{np:,.2f}",
            f"{am:,}",
        ])
    return buf.getvalue()


def send_report_to_slack(csv_content: str, log_path_csv: Path, start_date: date, end_date: date) -> None:
    title = f"KPI Validation completed — date range {start_date} to {end_date}"
    bot_token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    channel_id = os.getenv("SLACK_CHANNEL_ID", "").strip()

    if bot_token and channel_id:
        try:
            client = WebClient(token=bot_token)
            client.files_upload_v2(
                channel=channel_id,
                title=title,
                filename=log_path_csv.name,
                file=str(log_path_csv),
                initial_comment=f"*{title}*\n\n📎 Rekap CSV — klik untuk buka:",
            )
        except Exception as e:
            err = str(e)
            print(f"[Slack] File upload GAGAL: {err}")
            if "not_in_channel" in err:
                print("         → Bot belum di-invite ke channel. Ketik: /invite @ETL_Airflow")
            elif "invalid_auth" in err or "token_revoked" in err:
                print("         → Cek SLACK_BOT_TOKEN di .env")
            elif "channel_not_found" in err:
                print("         → Cek SLACK_CHANNEL_ID (format: C1234567890)")


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_date, end_date = get_recent_days_window(DAYS_BACK)

    print("=" * 60)
    print("KPI Validation — 1 day (H-1) per brand")
    print("=" * 60)
    print(f"Started:   {started}")
    print(f"Date range: {start_date} to {end_date} (exclusive)")
    print(f"Tables:    {', '.join(TABLES)}")
    print()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path_csv = LOGS_DIR / f"kpi_validation_{ts}.csv"

    conn = connect_supabase()
    report = {}
    try:
        for table in TABLES:
            print(f"Validating {table} ... ", end="", flush=True)
            try:
                rows = validate_kpi_table(conn, table, start_date, end_date)
                report[table] = rows
                print(f"OK ({len(rows)} date(s))")
            except Exception as e:
                print(f"ERROR — {e}")
                report[table] = []
    finally:
        conn.close()

    finished = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_csv = build_report_csv(report, started, finished, start_date, end_date)
    log_path_csv.write_text(log_csv, encoding="utf-8")

    print()
    print("=" * 60)
    print(f"Finished: {finished}")
    print(f"Log CSV:  {log_path_csv}")
    print("=" * 60)

    send_report_to_slack(log_csv, log_path_csv, start_date, end_date)


if __name__ == "__main__":
    main()

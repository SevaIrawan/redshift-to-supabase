"""
Jalankan semua perintah SQL (dari sql_runner_config.json) step by step.
Setiap step di-log (success/error). Log disimpan ke .csv.
"""
import csv
import io
import os
import sys
import json
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "sql_runner_config.json"
LOGS_DIR = SCRIPT_DIR / "logs"
EXPECTED_STEPS = 40


def connect_supabase():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT")),
        database=os.getenv("SUPABASE_DATABASE"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        sslmode="require",
    )
    conn.autocommit = True  # each statement commits
    return conn


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _has_real_sql(stmt: str) -> bool:
    """True jika statement ada SQL sebenar (bukan cuma comment/kosong)."""
    for line in stmt.splitlines():
        s = line.strip()
        if s and not s.startswith("--"):
            return True
    return False


def _split_sql_statements(content: str) -> list[str]:
    """Pisah SQL menjadi per statement; abaikan ; di dalam $$ ... $$."""
    out = []
    cur = []
    i = 0
    in_dollar = False
    n = len(content)
    while i < n:
        if not in_dollar and i <= n - 2 and content[i : i + 2] == "$$":
            in_dollar = True
            cur.append(content[i])
            i += 1
            cur.append(content[i])
            i += 1
            continue
        if in_dollar and i <= n - 2 and content[i : i + 2] == "$$":
            in_dollar = False
            cur.append(content[i])
            i += 1
            cur.append(content[i])
            i += 1
            continue
        if not in_dollar and content[i] == ";":
            stmt = "".join(cur).strip()
            if stmt and _has_real_sql(stmt):
                out.append(stmt)
            cur = []
            i += 1
            continue
        cur.append(content[i])
        i += 1
    stmt = "".join(cur).strip()
    if stmt and _has_real_sql(stmt):
        out.append(stmt)
    return out


def run_step(conn, step_name: str, sql_path: Path) -> dict:
    """Execute one SQL file (boleh berisi banyak statement). Return dict dengan status, message, rowcount."""
    sql_path = SCRIPT_DIR / sql_path if not sql_path.is_absolute() else sql_path
    if not sql_path.exists():
        return {
            "step": step_name,
            "status": "error",
            "message": f"File not found: {sql_path}",
            "rowcount": None,
        }
    content = sql_path.read_text(encoding="utf-8").strip()
    if not content:
        return {"step": step_name, "status": "error", "message": "SQL file is empty", "rowcount": None}
    statements = _split_sql_statements(content)
    if not statements:
        return {"step": step_name, "status": "error", "message": "No SQL statement found", "rowcount": None}
    try:
        cur = conn.cursor()
        rowcount = None
        for stmt in statements:
            cur.execute(stmt)
            if cur.rowcount is not None and cur.rowcount >= 0:
                rowcount = cur.rowcount if rowcount is None else rowcount + cur.rowcount
        cur.close()
        return {
            "step": step_name,
            "status": "success",
            "message": "OK",
            "rowcount": rowcount,
        }
    except Exception as e:
        return {
            "step": step_name,
            "status": "error",
            "message": str(e),
            "rowcount": None,
        }


def build_log_csv(entries: list, started: str, finished: str, summary: dict) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["# SQL Runner Log - Semua step"])
    w.writerow(["# Started", started])
    w.writerow(["# Finished", finished])
    w.writerow(["# Summary", f"Total: {summary['total']} | Success: {summary['success']} | Error: {summary['error']}"])
    w.writerow([])
    w.writerow(["step_num", "step_name", "status", "message", "rowcount"])
    for i, e in enumerate(entries, 1):
        rc = e.get("rowcount") if e.get("rowcount") is not None else ""
        w.writerow([i, e["step"], e["status"], e["message"], rc])
    return buf.getvalue()


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 60)
    print("SQL Runner — run all SQL steps")
    print("=" * 60)
    print(f"Started: {started}")
    print(f"Config:  {CONFIG_PATH}")
    print()

    try:
        config = load_config()
    except FileNotFoundError:
        print(f"[ERROR] Config not found: {CONFIG_PATH}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON: {e}")
        sys.exit(1)

    steps = config.get("steps", [])
    if not steps:
        print("[ERROR] No steps in config.")
        sys.exit(1)

    # Validasi: pastikan semua file SQL wujud
    missing = []
    for step_cfg in steps:
        sql_file = step_cfg.get("sql_file", "")
        if sql_file:
            p = SCRIPT_DIR / sql_file if not Path(sql_file).is_absolute() else Path(sql_file)
            if not p.exists():
                missing.append((step_cfg.get("name", "?"), sql_file))
    if missing:
        print("[ERROR] File SQL tidak dijumpai:")
        for name, path in missing:
            print(f"  - {name}: {path}")
        sys.exit(1)

    if len(steps) != EXPECTED_STEPS:
        print(f"[WARNING] Config ada {len(steps)} step (dijangka {EXPECTED_STEPS}). Teruskan anyway.")

    print(f"Total step: {len(steps)}")
    print()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path_csv = LOGS_DIR / f"sql_runner_{ts}.csv"

    conn = connect_supabase()
    entries = []
    try:
        for i, step_cfg in enumerate(steps, 1):
            name = step_cfg.get("name", f"Step {i}")
            sql_file = step_cfg.get("sql_file", "")
            if not sql_file:
                entries.append({"step": name, "status": "error", "message": "Missing sql_file", "rowcount": None})
                print(f"[{i}/{len(steps)}] {name} — ERROR (no sql_file)")
                continue
            print(f"[{i}/{len(steps)}] {name} ... ", end="", flush=True)
            result = run_step(conn, name, Path(sql_file))
            entries.append(result)
            if result["status"] == "success":
                print(f"OK (rows: {result.get('rowcount', '—')})")
            else:
                print(f"ERROR — {result['message'][:80]}")
    finally:
        conn.close()

    finished = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = len(entries)
    success = sum(1 for e in entries if e["status"] == "success")
    error = total - success
    summary = {"total": total, "success": success, "error": error}

    log_csv = build_log_csv(entries, started, finished, summary)
    log_path_csv.write_text(log_csv, encoding="utf-8")

    print()
    print("=" * 60)
    print(f"Finished: {finished}")
    print(f"Success: {success}/{total}  Errors: {error}")
    print(f"Log CSV:  {log_path_csv}")
    print("=" * 60)

    sys.exit(0 if error == 0 else 1)


if __name__ == "__main__":
    main()

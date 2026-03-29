"""
Jalankan 3 batch berurutan, lalu tunggu sampai batas jam berikutnya (UTC).
Ulang tanpa henti. Mulai siklus pertama segera setelah script dijalankan.

Urutan (tanpa Google Sheet — elak limit / gangguan pipeline):
  1. ty_run_all_sync.bat
  2. ty_run_rs_to_blue_whale_sync.bat
  3. run_sql_steps.bat

Waktu pakai UTC (satu referensi dengan timestamptz Postgres/Supabase).
Hentikan dengan Ctrl+C.
"""
from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

# Berurutan; satu harus selesai baru lanjut ke berikutnya
BATCH_FILES = [
    "ty_run_all_sync.bat",
    "ty_run_rs_to_blue_whale_sync.bat",
    "run_sql_steps.bat",
]


def _seconds_until_next_utc_hour() -> float:
    now = datetime.now(timezone.utc)
    this_hour = now.replace(minute=0, second=0, microsecond=0)
    next_boundary = this_hour + timedelta(hours=1)
    return max(0.0, (next_boundary - now).total_seconds())


def _run_one_bat(name: str) -> int:
    path = SCRIPT_DIR / name
    if not path.exists():
        print(f"[ERROR] File tidak ada: {path}", flush=True)
        return 127
    print(f"\n{'=' * 60}\n>>> {name}\n{'=' * 60}", flush=True)
    # stdin DEVNULL: hindari blok di `pause` di akhir .bat (automation)
    completed = subprocess.run(
        ["cmd.exe", "/c", str(path)],
        cwd=str(SCRIPT_DIR),
        stdin=subprocess.DEVNULL,
    )
    print(f"<<< {name} selesai — exit code {completed.returncode}\n", flush=True)
    return completed.returncode


def run_cycle() -> None:
    for bat in BATCH_FILES:
        _run_one_bat(bat)


def validate_batch_files() -> bool:
    """Pastikan semua .bat dalam BATCH_FILES ada di folder script."""
    ok = True
    for name in BATCH_FILES:
        p = SCRIPT_DIR / name
        if p.is_file():
            print(f"[OK] {name}", flush=True)
        else:
            print(f"[MISSING] {p}", flush=True)
            ok = False
    return ok


def main() -> None:
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    print(
        "TY hourly pipeline — UTC boundary\n"
        f"Folder: {SCRIPT_DIR}\n"
        "Ctrl+C untuk berhenti.\n",
        flush=True,
    )

    print("Cek file batch:", flush=True)
    if not validate_batch_files():
        print("[ERROR] Ada batch yang hilang. Perbaiki lalu jalankan lagi.", flush=True)
        sys.exit(1)

    cycle = 0
    try:
        while True:
            cycle += 1
            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"\n########## Siklus #{cycle} — mulai {now_utc} ##########", flush=True)
            run_cycle()
            wait_s = _seconds_until_next_utc_hour()
            next_utc = datetime.now(timezone.utc) + timedelta(seconds=wait_s)
            print(
                f"\nTunggu {wait_s:.0f} d (~{wait_s / 60:.1f} menit) "
                f"sampai jam UTC berikutnya (~{next_utc.strftime('%H:%M:%S')} UTC).",
                flush=True,
            )
            time.sleep(wait_s)
    except KeyboardInterrupt:
        print("\n[STOP] Dihentikan pengguna.", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()

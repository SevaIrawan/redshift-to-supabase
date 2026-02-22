# Laporan Scanning Project - Redshift to Supabase

**Tanggal:** 2026-02  
**Tujuan:** Cek file tidak berkaitan, duplicate, dan saran sebelum push ke GitHub.

---

## 1. FILE TIDAK BERKAITAN / KEMUNGKINAN ORPHAN

### 1.1 `slack_notify.py`
- **Status:** Tidak di-import oleh script manapun
- **Sebab:** `sync_blue_whale_*_recent_days.py` sekarang pakai `sync_blue_whale_recent_common` (bot), bukan `slack_notify` (webhook)
- **Rekomendasi:** Boleh disimpan jika ingin fallback webhook, atau dihapus kalau yakin tidak dipakai

### 1.2 SQL file TIDAK direferensi oleh `sql_runner_config.json`

| File | Keterangan |
|------|------------|
| `sql/01_backfill_blue_whale_usc.sql` | Backfill usc – config pakai `06_backfill_blue_whale_usc` |
| `sql/02_backfill_blue_whale_sgd.sql` | Backfill sgd – config pakai `05_backfill_blue_whale_sgd` |
| `sql/03_backfill_blue_whale_myr.sql` | Backfill myr – config pakai `04_backfill_blue_whale_myr` |
| `sql/04_update_unique_code_blue_whale_sgd.sql` | Update unique_code – config pakai `07_update_unique_code_blue_whale_sgd` |

- **Rekomendasi:** Kemungkinan versi lama / numbering lama. Boleh dihapus jika sudah yakin tidak dipakai, atau dipindah ke folder `sql/archive/` untuk backup.

### 1.3 `check.py`
- **Isi:** Hitung baris di `blue_whale_sgd_export.csv`
- **Rekomendasi:** Sepertinya utility one-off. Boleh disimpan jika masih dipakai, atau pindah ke folder `scripts/` / `utils/`.

### 1.4 `cara_ambil_data_monthly.txt`
- **Isi:** Contoh perintah export monthly
- **Rekomendasi:** Dokumentasi ringan. Aman disimpan.

---

## 2. DUPLICATE FUNCTIONS

### 2.1 `sync_blue_whale_usc_recent_days.py`, `sync_blue_whale_sgd_recent_days.py`, `sync_blue_whale_myr_recent_days.py`

Kode hampir sama di 3 file:

| Function | Lokasi |
|----------|--------|
| `quote_name` | Ketiganya |
| `connect_redshift` | Ketiganya |
| `connect_supabase` | Ketiganya |
| `ensure_last_synced_column` | Ketiganya |
| `get_recent_days_window` | Ketiganya |
| `sync_recent_days` | Ketiganya |
| `_notify_sync_done` | Ketiganya |

**Perbedaan utama:** `load_config` pakai config berbeda (`sync_config.json`, `sync_config_sgd.json`, `sync_config_myr.json`).

**Rekomendasi:** Bisa direfaktor ke satu modul shared + parameter config, tapi tidak wajib. Refactor besar bisa ditunda.

### 2.2 Export scripts (deposit/withdraw/blue_whale)

`quote_ident` dan `build_year_expr` diulang di banyak file:

- `export_deposit_usc_to_csv.py`, `export_deposit_sgd_to_csv.py`, `export_deposit_myr_to_csv.py`
- `export_withdraw_usc_to_csv.py`, `export_withdraw_sgd_to_csv.py`, `export_withdraw_myr_to_csv.py`
- `export_blue_whale_usc_to_csv.py`, `export_blue_whale_sgd_to_csv.py`, `export_blue_whale_myr_to_csv.py`
- `export_new_depositors_to_csv.py`

**Rekomendasi:** Boleh diextract ke modul shared jika mau, tapi tidak menghalangi push ke GitHub.

---

## 3. KELOMPOK FILE BERDASARKAN FUNGSI

### 3.1 4 automation utama (dipakai run_all_sync, run_rs_to_blue_whale_sync, run_sql_steps, run_validate_kpi)

| Automation | Files |
|------------|-------|
| run_all_sync | `sync_blue_whale_usc/sgd/myr_recent_days.py`, `sync_blue_whale_recent_common.py`, `sync_config*.json` |
| run_rs_to_blue_whale_sync | `sync_rs_to_blue_whale_usc/sgd/myr.py`, `sync_rs_to_blue_whale_common.py` |
| run_sql_steps | `run_sql_steps.py`, `sql_runner_config.json`, `sql/01_*` … `sql/36_*` (yang direferensi config) |
| run_validate_kpi | `validate_kpi.py` |

### 3.2 Migration (tidak dipakai 4 automation utama)

- `migrate_usc.py`, `migrate_sgd.py`, `migrate_myr.py`
- `migrate_usc_2022.py`, `migrate_usc_2023.py`, `migrate_usc_2024.py`, `migrate_usc_2025.py`
- `run_migrate_all_years.bat`

### 3.3 Export utilities

- `export_deposit_*_to_csv.py`, `export_withdraw_*_to_csv.py`
- `export_blue_whale_*_to_csv.py`
- `export_new_depositors_to_csv.py`
- `export_monthly_blue_whale.py`, `export_monthly_supabase.py`
- `export_supabase_usc/sgd/myr.py`
- `export_usc.py`, `export_sgd.py`, `export_myr.py`

### 3.4 Sync current month (workflow terpisah)

- `sync_blue_whale_current_month.py`
- `setup_sync_task.ps1` (memanggil script di atas)

### 3.5 Runner scripts (batch / powershell)

- `run_all_sync.bat`, `run_all_sync.ps1`
- `run_rs_to_blue_whale_sync.bat`
- `run_sql_steps.bat`
- `run_validate_kpi.bat`
- `run_migrate_all_years.bat`

---

## 4. UPDATE `.gitignore` (SUDAH DITERAPKAN)

```
.env, .env.local, .env.*.local
__pycache__/, *.pyc, venv/, .venv/
logs/
sync_log.txt
exports/
monthly_exports/
*.csv
*.db
.vscode/, .idea/
.DS_Store, Thumbs.db
```

---

## 5. RINGKASAN

| Kategori | Jumlah | Tindakan disarankan |
|----------|--------|----------------------|
| File orphan / tidak dipakai | 5+ | Review, archive atau hapus kalau tidak perlu |
| SQL tidak direferensi | 4 | Simpan di `sql/archive/` atau hapus |
| Duplicate functions | Banyak | Refactor optional, tidak wajib untuk push |
| File sensitif | - | Pastikan `.env`, `logs/`, `monthly_exports/` di-ignore |

**Rekomendasi:** Project siap push ke GitHub. Perubahan di atas bersifat opsional; tidak perlu dihapus sebelum push kalau ingin aman.

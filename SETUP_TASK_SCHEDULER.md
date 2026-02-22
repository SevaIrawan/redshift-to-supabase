# Panduan Setup Windows Task Scheduler

## Daftar Script yang Bisa Dijadwalkan

| No | File .bat | Fungsi | Contoh waktu jadwal |
|----|-----------|--------|---------------------|
| 1 | **run_all_sync.bat** | Import Redshift → Supabase (USC, SGD, MYR), 3 hari | Setiap hari 08:00 |
| 2 | **run_sql_steps.bat** | 35 step SQL (trigger, backfill, refresh MV) | Setiap hari 09:00 |
| 3 | **run_rs_to_blue_whale_sync.bat** | Copy H-1 rs_* → blue_whale_* (USC, SGD, MYR) | Setiap hari 08:30 |
| 4 | **run_validate_kpi.bat** | Validasi KPI + log ke Slack | Setiap hari 10:00 |

**Urutan disarankan:** 1 → 3 → 2 → 4 (sync dulu, lalu rs→blue_whale, lalu SQL steps, akhirnya validasi KPI).

---

## Step 1: Persiapan File

Pastikan folder project ada dan berisi:

**Untuk run_all_sync.bat:**
- ✅ `run_all_sync.bat`
- ✅ `sync_blue_whale_usc_recent_days.py`, `sync_blue_whale_sgd_recent_days.py`, `sync_blue_whale_myr_recent_days.py`
- ✅ `sync_config.json`, `sync_config_sgd.json`, `sync_config_myr.json`
- ✅ `.env` (credentials)

**Untuk run_sql_steps.bat:**
- ✅ `run_sql_steps.bat`, `run_sql_steps.py`
- ✅ `sql_runner_config.json`, folder `sql/` (35 file .sql)
- ✅ `.env`

**Untuk run_rs_to_blue_whale_sync.bat:**
- ✅ `run_rs_to_blue_whale_sync.bat`
- ✅ `sync_rs_to_blue_whale_usc.py`, `sync_rs_to_blue_whale_sgd.py`, `sync_rs_to_blue_whale_myr.py`, `sync_rs_to_blue_whale_common.py`
- ✅ `.env`

**Untuk run_validate_kpi.bat:**
- ✅ `run_validate_kpi.bat`, `validate_kpi.py`
- ✅ `.env`

**Path folder project contoh:** `C:\Users\BDC Computer\Redshift to Supabase`  
(Ganti dengan path sebenarnya di PC Anda.)

---

## Step 2: Setup Task Scheduler

### 2.1 Buka Task Scheduler
1. Tekan `Windows + R`
2. Ketik: `taskschd.msc`
3. Tekan Enter

### 2.2 Create Basic Task
1. Klik **"Create Basic Task"** di panel kanan
2. **Name:** pilih salah satu dari tabel di bawah (atau nama sendiri)
3. **Description:** isi keterangan task
4. Klik **Next**

**Nama & program untuk setiap task:**

| Task | Name (contoh) | Program/script | Start in |
|------|----------------|----------------|----------|
| Sync Redshift | `Auto Sync Redshift to Supabase` | `C:\Users\BDC Computer\Redshift to Supabase\run_all_sync.bat` | `C:\Users\BDC Computer\Redshift to Supabase` |
| SQL Steps | `Auto SQL Runner (35 steps)` | `C:\Users\BDC Computer\Redshift to Supabase\run_sql_steps.bat` | `C:\Users\BDC Computer\Redshift to Supabase` |
| RS to Blue Whale | `Auto RS to Blue Whale (H-1)` | `C:\Users\BDC Computer\Redshift to Supabase\run_rs_to_blue_whale_sync.bat` | `C:\Users\BDC Computer\Redshift to Supabase` |
| Validate KPI | `Auto Validate KPI` | `C:\Users\BDC Computer\Redshift to Supabase\run_validate_kpi.bat` | `C:\Users\BDC Computer\Redshift to Supabase` |

**Ganti** `C:\Users\BDC Computer\Redshift to Supabase` dengan path folder project Anda jika berbeda.

### 2.3 Trigger
1. Pilih **"Daily"**
2. Klik **Next**
3. **Start:** Pilih tanggal hari ini
4. **Time:** `08:00:00` (8:00 AM)
5. **Recur every:** `1 days`
6. Klik **Next**

### 2.4 Action
1. Pilih **"Start a program"**
2. Klik **Next**
3. **Program/script:** masukkan **path lengkap** ke file .bat (lihat tabel di 2.2).  
   Contoh: `C:\Users\BDC Computer\Redshift to Supabase\run_all_sync.bat`
4. **Add arguments:** kosongkan (tidak perlu diisi untuk .bat)
5. **Start in:** isi **path folder** yang sama dengan folder .bat.  
   Contoh: `C:\Users\BDC Computer\Redshift to Supabase`  
   (Agar working directory benar saat script berjalan.)
6. Klik **Next**

### 2.5 Finish
1. Centang **"Open the Properties dialog for this task when I click Finish"**
2. Klik **Finish**
3. ⚠️ **Jika muncul dialog password:**
   - Masukkan **password Windows user** yang sedang login
   - Bukan password Microsoft account, tapi password **local Windows user**
   - Jika user tidak punya password, buat password dulu di Settings → Accounts → Sign-in options
   - Klik **OK**

---

## Step 3: Configure Advanced Settings

### 3.1 General Tab
**PILIH SALAH SATU:**

**Opsi A: Run whether user is logged on or not** (Recommended - lebih aman)
- ✅ Centang **"Run whether user is logged on or not"**
- ⚠️ **Akan diminta password** saat setup (masukkan password Windows user)
- ✅ Task akan dijalankan meskipun user tidak login
- ✅ **Run with highest privileges** (opsional - centang jika diperlukan)
- **Configure for:** Windows 10 / Windows 11
- ⚠️ **Jika password tidak bisa, gunakan Opsi B**

**Opsi B: Run only when user is logged on** (Tidak perlu password) ⭐ RECOMMENDED jika password bermasalah
- ✅ Centang **"Run only when user is logged on"**
- ✅ **TIDAK perlu password**
- ⚠️ Task hanya akan dijalankan jika user sudah login ke Windows
- ✅ **Run with highest privileges** (opsional - centang jika diperlukan)
- **Configure for:** Windows 10 / Windows 11
- ✅ **Missed schedule tetap akan dijalankan saat user login** (jika pengaturan "missed start" sudah dicentang)

**Catatan:** 
- Jika password tidak bisa, **gunakan Opsi B** (tidak perlu password)
- Dengan Opsi B, pastikan user login ke Windows setiap hari
- Missed schedule tetap akan bekerja: jika komputer mati jam 8, task akan dijalankan saat user login (jika pengaturan "missed start" dicentang)

### 3.2 Triggers Tab
1. Klik **Edit** pada trigger yang sudah dibuat
2. Di bagian **Advanced settings:**
   - ✅ **Enabled** (centang)
   - ✅ **Repeat task every:** (kosongkan)
   - ✅ **Stop task if it runs longer than:** (kosongkan atau set 2 hours)
   - ✅ **Expire:** (kosongkan atau set jauh ke depan)

### 3.3 Conditions Tab
- ✅ **Start the task only if the computer is on AC power:** (HAPUS centang — agar task bisa dijalankan saat pakai baterai)
- ✅ **Wake the computer to run this task:** (opsional - centang jika ingin wake device)
- ✅ **Start the task only if the following network connection is available:** (opsional)

### 3.4 Settings Tab (PENTING!)
- ✅ **Allow task to be run on demand**
- ✅ **Run task as soon as possible after a scheduled start is missed** ⭐ (PENTING!)
- ✅ **If the running task does not end when requested, force it to stop**
- **If the task is already running:** Pilih **"Do not start a new instance"** atau **"Stop the existing instance"**
- **If the task fails, restart every:** (opsional — bisa diset 10 menit, maksimal 3 kali)

### 3.5 Actions Tab
- Pastikan action sudah benar (lihat Step 2.4)

### 3.6 History Tab
- ✅ **Enable All Tasks History** (untuk debugging)

Klik **OK** untuk menyimpan

---

## Step 4: Test Manual

### Uji 1: Menjalankan Task Secara Manual
1. Di Task Scheduler, cari task yang baru dibuat
2. Klik kanan → **Run** (Jalankan)
3. Cek apakah script berjalan dengan benar
4. Cek output/log

### Uji 2: Uji Missed Schedule
1. Set trigger ke waktu 1-2 menit dari sekarang
2. Matikan device
3. Nyalakan device setelah waktu trigger terlewat
4. Cek apakah task otomatis dijalankan

---

## Step 5: Monitoring & Logs

### Cek Log Task Scheduler
1. Di Task Scheduler, klik **Task Scheduler Library**
2. Klik task yang dibuat
3. Di bawah, klik tab **History**
4. Lihat log execution

### Cek Log Script
- File `sync_log.txt` akan dibuat di folder project
- Berisi timestamp setiap sync

---

## Troubleshooting

### Password tidak bisa / Authentication failed

**CARA YANG BENAR UNTUK MASUKKAN PASSWORD:**

1. **Pastikan password yang benar:**
   - Gunakan password **Windows local user** (bukan Microsoft account password)
   - Password harus sama dengan password yang dipakai untuk login Windows
   - Cek di **Settings → Accounts → Sign-in options** untuk pastikan password

2. **Jika user tidak punya password:**
   - Buka **Settings → Accounts → Sign-in options**
   - Klik **Password** → **Add** atau **Change**
   - Buat password untuk Windows user
   - Gunakan password ini di Task Scheduler

3. **Format password:**
   - Masukkan password **tanpa spasi** di awal/akhir
   - Case sensitive (huruf besar/kecil harus benar)
   - Jika ada karakter khusus, pastikan diketik dengan benar

4. **Cek user account:**
   - Buka **Control Panel → User Accounts**
   - Pastikan user account adalah **Administrator**
   - Jika bukan Administrator, ubah ke Administrator dulu

5. **Jalankan Task Scheduler sebagai Administrator:**
   - Tutup Task Scheduler
   - Klik kanan **Task Scheduler** → **Run as administrator**
   - Setup task lagi dengan password

6. **Jika masih tidak bisa, coba cara ini:**
   - Buka **Properties** task yang sudah dibuat
   - Di tab **General**, klik **Change User or Group**
   - Ketik: `.\YourUsername` (contoh: `.\BDC Computer`)
   - Klik **Check Names** → **OK**
   - Masukkan password lagi

**ALTERNATIF (jika password tetap tidak bisa):**
- Gunakan **"Run only when user is logged on"** (tidak perlu password)
- Task akan dijalankan saat user login (missed schedule tetap dijalankan jika pengaturan dicentang)

### Task tidak dijalankan otomatis
- ✅ Cek apakah task **Enabled**
- ✅ Cek **History** untuk error messages
- ✅ Cek apakah Python path benar
- ✅ Cek apakah file `.env` ada dan credentials benar
- ✅ Jika memakai "Run only when user is logged on", pastikan user sudah login

### Task dijalankan tapi script error
- ✅ Cek output di Task Scheduler History
- ✅ Jalankan script secara manual untuk cek error
- ✅ Pastikan semua dependencies terinstall
- ✅ Cek apakah path file benar (gunakan full path)

### Missed schedule tidak dijalankan otomatis
- ✅ Pastikan setting **"Run task as soon as possible after a scheduled start is missed"** sudah dicentang
- ✅ Pastikan task **Enabled**
- ✅ Cek **History** untuk melihat apakah task ter-trigger
- ⚠️ Jika memakai "Run only when user is logged on", task hanya dijalankan saat user login (tidak otomatis saat komputer hidup)

### Permission denied
- ✅ Jalankan Task Scheduler sebagai Administrator
- ✅ Atau set task untuk **"Run only when user is logged on"** (tidak perlu password)
- ✅ Pastikan akun user punya izin untuk menjalankan script
- ✅ Pastikan folder script tidak di-lock atau read-only

---

## Tips

1. **Uji dulu secara manual** sebelum mengatur jadwal
2. **Pantau History** beberapa hari pertama untuk memastikan berjalan dengan baik
3. **Backup config files** sebelum deploy
4. **Set notification** (opsional) jika task gagal

---

## Alternative: PowerShell Script untuk Setup Otomatis

Bisa juga buat PowerShell script untuk setup Task Scheduler secara otomatis (akan dibuat jika diperlukan).

---

## Ringkasan: Cara Schedule Semua Script

Ulangi **Step 2–3** untuk setiap script. Setiap script = **satu task terpisah**.

| Task | Name task | File .bat | Saran waktu trigger |
|------|-----------|-----------|------------------------|
| 1 | Auto Sync Redshift to Supabase | run_all_sync.bat | Daily 08:00 |
| 2 | Auto RS to Blue Whale H-1 | run_rs_to_blue_whale_sync.bat | Daily 08:30 |
| 3 | Auto SQL Runner 35 steps | run_sql_steps.bat | Daily 09:00 |
| 4 | Auto Validate KPI | run_validate_kpi.bat | Daily 10:00 |

**Program/script:** path lengkap ke .bat (contoh: `C:\Users\BDC Computer\Redshift to Supabase\run_all_sync.bat`)  
**Start in:** path folder project (contoh: `C:\Users\BDC Computer\Redshift to Supabase`)

---

## Catatan Penting

⚠️ **Komputer harus dalam keadaan hidup** agar task bisa dijalankan. Jika komputer mati:
- Task akan **otomatis dijalankan saat komputer hidup** (berkat pengaturan "missed start")
- Namun sync akan berjalan saat komputer hidup, bukan tepat pada jam yang disetel

✅ **Praktik terbaik:**
- Pastikan komputer hidup pada waktu jadwal agar sync tepat waktu
- Atau set device untuk auto wake (jika support)
- Centang **"Run task as soon as possible after a scheduled start is missed"** di pengaturan task


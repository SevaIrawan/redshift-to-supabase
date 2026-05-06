@echo off
setlocal EnableExtensions
REM Urutan ketat: tiap python harus exit 0 dulu; gagal = berhenti, tidak lanjut step berikutnya.
REM Jeda antar blok: 15 dtk. (python ... menunggu proses selesai secara default.)
cd /d "%~dp0"

echo ============================================
echo UPDATE RS TO SUPABASE (strict sequential)
echo ============================================
echo Start: %date% %time%
echo.

REM --- Blok 1: sync recent days (USC, SGD, MYR) ---
echo [1a/3] sync_blue_whale_usc_recent_days.py
python sync_blue_whale_usc_recent_days.py
if %errorlevel% neq 0 goto :FAILED

echo [1b/3] sync_blue_whale_sgd_recent_days.py
python sync_blue_whale_sgd_recent_days.py
if %errorlevel% neq 0 goto :FAILED

echo [1c/3] sync_blue_whale_myr_recent_days.py
python sync_blue_whale_myr_recent_days.py
if %errorlevel% neq 0 goto :FAILED

echo.
echo Jeda 15 detik...
timeout /t 15 /nobreak >nul
if %errorlevel% neq 0 goto :FAILED
echo.

REM --- Blok 2: rs_to_* (SGD, MYR, USC) ---
echo [2a/3] rs_to_sgd.py
python rs_to_sgd.py
if %errorlevel% neq 0 goto :FAILED

echo [2b/3] rs_to_myr.py
python rs_to_myr.py
if %errorlevel% neq 0 goto :FAILED

echo [2c/3] rs_to_usc.py
python rs_to_usc.py
if %errorlevel% neq 0 goto :FAILED

echo.
echo Jeda 15 detik...
timeout /t 15 /nobreak >nul
if %errorlevel% neq 0 goto :FAILED
echo.

REM --- Blok 3: SQL steps ---
echo [3] run_sql_steps.py
python run_sql_steps.py
if %errorlevel% neq 0 goto :FAILED

echo.
echo Jeda 15 detik...
timeout /t 15 /nobreak >nul
if %errorlevel% neq 0 goto :FAILED
echo.

REM --- Blok 4: Google Sheet ---
echo [4] sync_nd_usc_marketing_to_google_sheet.py
python sync_nd_usc_marketing_to_google_sheet.py
if %errorlevel% neq 0 goto :FAILED

echo.
echo ============================================
echo SEMUA STEP OK — Selesai: %date% %time%
echo ============================================
REM Tanpa pause — aman untuk Task Scheduler (tidak menunggu input).
endlocal & exit /b 0

:FAILED
echo.
echo ============================================
echo [ERROR] Step di atas gagal — pipeline DIHENTIKAN.
echo Waktu: %date% %time%
echo ============================================
endlocal & exit /b 1

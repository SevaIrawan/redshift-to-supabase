@echo off
REM ============================================
REM Script untuk menjalankan 3 sync script
REM USC, SGD, dan MYR
REM ============================================

echo ============================================
echo AUTO SYNC - BLUE WHALE (USC, SGD, MYR)
echo ============================================
echo Start time: %date% %time%
echo.

REM Set working directory ke folder script
cd /d "%~dp0"

REM ============================================
REM 1. SYNC USC
REM ============================================
echo.
echo [1/3] Starting USC sync...
echo ============================================
python sync_blue_whale_usc_recent_days.py
if %errorlevel% neq 0 (
    echo [ERROR] USC sync failed with error code %errorlevel%
    echo Continuing with next script...
) else (
    echo [OK] USC sync completed successfully
)
echo.

REM ============================================
REM 2. SYNC SGD
REM ============================================
echo.
echo [2/3] Starting SGD sync...
echo ============================================
python sync_blue_whale_sgd_recent_days.py
if %errorlevel% neq 0 (
    echo [ERROR] SGD sync failed with error code %errorlevel%
    echo Continuing with next script...
) else (
    echo [OK] SGD sync completed successfully
)
echo.

REM ============================================
REM 3. SYNC MYR
REM ============================================
echo.
echo [3/3] Starting MYR sync...
echo ============================================
python sync_blue_whale_myr_recent_days.py
if %errorlevel% neq 0 (
    echo [ERROR] MYR sync failed with error code %errorlevel%
) else (
    echo [OK] MYR sync completed successfully
)
echo.

REM ============================================
REM SUMMARY
REM ============================================
echo.
echo ============================================
echo ALL SYNC COMPLETED
echo ============================================
echo End time: %date% %time%
echo.

REM Optional: Log ke file
echo %date% %time% - Sync completed >> sync_log.txt

pause


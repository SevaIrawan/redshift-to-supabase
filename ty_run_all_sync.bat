@echo off
REM TY = kemarin + hari ini. Redshift → rs_blue_whale_* (USC, SGD, MYR)
REM File baru; tidak menggantikan run_all_sync.bat

echo ============================================
echo TY AUTO SYNC - BLUE WHALE (USC, SGD, MYR)
echo ============================================
echo Start time: %date% %time%
echo.

cd /d "%~dp0"

echo.
echo [1/3] Starting USC sync...
echo ============================================
python ty_redshift_to_rs_usc.py
if %errorlevel% neq 0 (
    echo [ERROR] USC sync failed with error code %errorlevel%
    echo Continuing with next script...
) else (
    echo [OK] USC sync completed successfully
)
echo.

echo.
echo [2/3] Starting SGD sync...
echo ============================================
python ty_redshift_to_rs_sgd.py
if %errorlevel% neq 0 (
    echo [ERROR] SGD sync failed with error code %errorlevel%
    echo Continuing with next script...
) else (
    echo [OK] SGD sync completed successfully
)
echo.

echo.
echo [3/3] Starting MYR sync...
echo ============================================
python ty_redshift_to_rs_myr.py
if %errorlevel% neq 0 (
    echo [ERROR] MYR sync failed with error code %errorlevel%
) else (
    echo [OK] MYR sync completed successfully
)
echo.

echo.
echo ============================================
echo ALL TY SYNC COMPLETED
echo ============================================
echo End time: %date% %time%
echo.

echo %date% %time% - TY Redshift sync completed >> sync_log.txt

pause

@echo off
REM Sync rs_blue_whale_* -> blue_whale_* (H-1) - 3 script USC, SGD, MYR
REM Boleh di-schedule di Windows Task Scheduler
cd /d "%~dp0"

echo ============================================
echo RS TO BLUE WHALE SYNC (H-1)
echo ============================================
echo Start: %date% %time%
echo.

echo [1/3] USC...
python sync_rs_to_blue_whale_usc.py
if %errorlevel% neq 0 echo [WARNING] USC sync failed.
echo.

echo [2/3] SGD...
python sync_rs_to_blue_whale_sgd.py
if %errorlevel% neq 0 echo [WARNING] SGD sync failed.
echo.

echo [3/3] MYR...
python sync_rs_to_blue_whale_myr.py
if %errorlevel% neq 0 echo [WARNING] MYR sync failed.
echo.

echo ============================================
echo End: %date% %time%
echo ============================================
pause

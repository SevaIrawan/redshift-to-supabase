@echo off
REM TY = kemarin + hari ini. rs_blue_whale_* → blue_whale_*
REM File baru; tidak menggantikan run_rs_to_blue_whale_sync.bat

cd /d "%~dp0"

echo ============================================
echo TY RS TO BLUE WHALE SYNC
echo ============================================
echo Start: %date% %time%
echo.

echo [1/3] USC...
python ty_rs_to_blue_whale_usc.py
if %errorlevel% neq 0 echo [WARNING] USC sync failed.
echo.

echo [2/3] SGD...
python ty_rs_to_blue_whale_sgd.py
if %errorlevel% neq 0 echo [WARNING] SGD sync failed.
echo.

echo [3/3] MYR...
python ty_rs_to_blue_whale_myr.py
if %errorlevel% neq 0 echo [WARNING] MYR sync failed.
echo.

echo ============================================
echo End: %date% %time%
echo ============================================
pause

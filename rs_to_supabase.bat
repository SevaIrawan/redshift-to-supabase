@echo off
REM rs_to_* — sync rs_blue_whale_* -> blue_whale_* (H-2) termasuk traffic, register_date, first_deposit_*
cd /d "%~dp0"

echo ============================================
echo RS TO SUPABASE (USC, SGD, MYR + extra columns)
echo ============================================
echo Start: %date% %time%
echo.

echo [1/3] USC...
python rs_to_usc.py
if %errorlevel% neq 0 echo [WARNING] USC failed.
echo.

echo [2/3] SGD...
python rs_to_sgd.py
if %errorlevel% neq 0 echo [WARNING] SGD failed.
echo.

echo [3/3] MYR...
python rs_to_myr.py
if %errorlevel% neq 0 echo [WARNING] MYR failed.
echo.

echo ============================================
echo End: %date% %time%
echo ============================================
pause

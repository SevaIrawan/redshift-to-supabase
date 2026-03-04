@echo off
REM Sync 2 MV (nd_usc_marketing_mv, nd_trans_usc_marketing_mv) dari Supabase ke Google Sheet (2 tab)
cd /d "%~dp0"

echo ============================================
echo SYNC 2 MV -^> Google Sheet
echo ============================================
echo Start: %date% %time%
echo.

python sync_nd_usc_marketing_to_google_sheet.py
if %errorlevel% neq 0 echo [WARNING] Sync failed.
echo.

echo ============================================
echo End: %date% %time%
echo ============================================
pause

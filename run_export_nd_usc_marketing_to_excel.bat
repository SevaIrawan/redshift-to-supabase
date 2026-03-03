@echo off
REM Export nd_usc_marketing_mv + nd_trans_usc_marketing_mv -> 2 Excel files, upload ke Slack #data_marketing_usc
REM Bisa dijadwalkan di Windows Task Scheduler
cd /d "%~dp0"

echo ============================================
echo EXPORT ND USC MARKETING -> EXCEL + SLACK
echo ============================================
echo Start: %date% %time%
echo.

python export_nd_usc_marketing_to_excel.py
if %errorlevel% neq 0 echo [WARNING] Export failed.
echo.

echo ============================================
echo End: %date% %time%
echo ============================================
pause

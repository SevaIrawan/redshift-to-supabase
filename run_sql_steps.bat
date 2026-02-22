@echo off
REM ============================================
REM SQL Runner — jalankan semua step SQL
REM Boleh di-schedule di Windows Task Scheduler
REM ============================================

echo ============================================
echo SQL RUNNER - RUN ALL SQL STEPS
echo ============================================
echo Start time: %date% %time%
echo.

cd /d "%~dp0"

python run_sql_steps.py
if %errorlevel% neq 0 (
    echo [WARNING] SQL Runner finished with errors. Check log and Slack.
) else (
    echo [OK] SQL Runner completed successfully.
)

echo.
echo End time: %date% %time%
echo.

pause

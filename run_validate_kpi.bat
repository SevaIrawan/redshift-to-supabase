@echo off
REM KPI Validation — H-1 (1 day) per brand, log ke Slack
cd /d "%~dp0"
echo KPI Validation - Started %date% %time%
python validate_kpi.py
echo Finished %date% %time%
pause

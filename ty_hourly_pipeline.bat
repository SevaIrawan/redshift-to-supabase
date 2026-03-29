@echo off
REM Jalankan ty_hourly_pipeline.py (loop tiap jam UTC, 3 batch — tanpa Google Sheet)
cd /d "%~dp0"
python ty_hourly_pipeline.py
pause

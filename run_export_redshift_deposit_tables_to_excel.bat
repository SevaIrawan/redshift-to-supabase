@echo off
REM Export deposit, deposit_usc, deposit_sgd dari Redshift -> Excel (folder exports_redshift)
cd /d "%~dp0"

echo ============================================
echo REDSHIFT DEPOSIT TABLES -^> EXCEL
echo ============================================
echo Start: %date% %time%
echo.

echo [1/3] deposit...
python export_redshift_deposit_to_excel.py
if %errorlevel% neq 0 echo [WARNING] deposit export failed.
echo.

echo [2/3] deposit_usc...
python export_redshift_deposit_usc_to_excel.py
if %errorlevel% neq 0 echo [WARNING] deposit_usc export failed.
echo.

echo [3/3] deposit_sgd...
python export_redshift_deposit_sgd_to_excel.py
if %errorlevel% neq 0 echo [WARNING] deposit_sgd export failed.
echo.

echo ============================================
echo End: %date% %time%
echo ============================================
pause

@echo off
echo ============================================
echo MIGRATE USC - ALL YEARS (2025, 2024, 2023, 2022)
echo ============================================
echo.

echo [1/4] Migrating year 2025...
python migrate_usc_2025.py
echo.

echo [2/4] Migrating year 2024...
python migrate_usc_2024.py
echo.

echo [3/4] Migrating year 2023...
python migrate_usc_2023.py
echo.

echo [4/4] Migrating year 2022...
python migrate_usc_2022.py
echo.

echo ============================================
echo ALL MIGRATIONS COMPLETED
echo ============================================
pause

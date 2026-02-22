# ============================================
# PowerShell Script untuk menjalankan 3 sync script
# USC, SGD, dan MYR
# ============================================

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "AUTO SYNC - BLUE WHALE (USC, SGD, MYR)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Start time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Green
Write-Host ""

# Set working directory ke folder script
Set-Location $PSScriptRoot

$ErrorActionPreference = "Continue"
$scriptResults = @()

# ============================================
# 1. SYNC USC
# ============================================
Write-Host ""
Write-Host "[1/3] Starting USC sync..." -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
try {
    $uscResult = python sync_blue_whale_usc_recent_days.py
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] USC sync completed successfully" -ForegroundColor Green
        $scriptResults += @{Script="USC"; Status="Success"}
    } else {
        Write-Host "[ERROR] USC sync failed with error code $LASTEXITCODE" -ForegroundColor Red
        $scriptResults += @{Script="USC"; Status="Failed"; ErrorCode=$LASTEXITCODE}
    }
} catch {
    Write-Host "[ERROR] USC sync exception: $_" -ForegroundColor Red
    $scriptResults += @{Script="USC"; Status="Exception"; Error=$_.Exception.Message}
}
Write-Host ""

# ============================================
# 2. SYNC SGD
# ============================================
Write-Host ""
Write-Host "[2/3] Starting SGD sync..." -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
try {
    $sgdResult = python sync_blue_whale_sgd_recent_days.py
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] SGD sync completed successfully" -ForegroundColor Green
        $scriptResults += @{Script="SGD"; Status="Success"}
    } else {
        Write-Host "[ERROR] SGD sync failed with error code $LASTEXITCODE" -ForegroundColor Red
        $scriptResults += @{Script="SGD"; Status="Failed"; ErrorCode=$LASTEXITCODE}
    }
} catch {
    Write-Host "[ERROR] SGD sync exception: $_" -ForegroundColor Red
    $scriptResults += @{Script="SGD"; Status="Exception"; Error=$_.Exception.Message}
}
Write-Host ""

# ============================================
# 3. SYNC MYR
# ============================================
Write-Host ""
Write-Host "[3/3] Starting MYR sync..." -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
try {
    $myrResult = python sync_blue_whale_myr_recent_days.py
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] MYR sync completed successfully" -ForegroundColor Green
        $scriptResults += @{Script="MYR"; Status="Success"}
    } else {
        Write-Host "[ERROR] MYR sync failed with error code $LASTEXITCODE" -ForegroundColor Red
        $scriptResults += @{Script="MYR"; Status="Failed"; ErrorCode=$LASTEXITCODE}
    }
} catch {
    Write-Host "[ERROR] MYR sync exception: $_" -ForegroundColor Red
    $scriptResults += @{Script="MYR"; Status="Exception"; Error=$_.Exception.Message}
}
Write-Host ""

# ============================================
# SUMMARY
# ============================================
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "ALL SYNC COMPLETED" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "End time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Green
Write-Host ""

# Tampilkan summary
Write-Host "Summary:" -ForegroundColor Cyan
foreach ($result in $scriptResults) {
    if ($result.Status -eq "Success") {
        Write-Host "  $($result.Script): SUCCESS" -ForegroundColor Green
    } else {
        Write-Host "  $($result.Script): FAILED" -ForegroundColor Red
        if ($result.ErrorCode) {
            Write-Host "    Error Code: $($result.ErrorCode)" -ForegroundColor Red
        }
        if ($result.Error) {
            Write-Host "    Error: $($result.Error)" -ForegroundColor Red
        }
    }
}
Write-Host ""

# Log ke file
$logEntry = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - Sync completed"
Add-Content -Path "sync_log.txt" -Value $logEntry

# Optional: Pause jika di-run manual
# Read-Host "Press Enter to exit"


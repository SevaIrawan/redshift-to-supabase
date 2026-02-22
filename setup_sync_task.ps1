param (
    [string]$TaskBaseName = "SyncBlueWhaleCurrentMonth",
    [int]$HourlyInterval = 1
)

function Quote($text) {
    return '"' + $text.Replace('"', '""') + '"'
}

$scriptPath = Join-Path $PSScriptRoot "sync_blue_whale_current_month.py"
if (-not (Test-Path $scriptPath)) {
    Write-Error "File sync_blue_whale_current_month.py tidak ditemukan di $PSScriptRoot"
    exit 1
}

try {
    $pythonPath = (Get-Command python -ErrorAction Stop).Source
} catch {
    Write-Error "Python belum tersedia di PATH. Jalankan installer Python terlebih dahulu."
    exit 1
}

function CreateScheduledTask($name, $schedule, $additionalArgs = @()) {
    $fullTaskName = "${TaskBaseName}_$name"
    schtasks /Delete /TN $fullTaskName /F | Out-Null
    $tr = Quote("$pythonPath $scriptPath")
    $arguments = @(
        "/Create",
        "/F",
        "/SC", $schedule,
        "/TN", $fullTaskName,
        "/TR", $tr,
        "/RL", "HIGHEST",
        "/RU", "$env:USERNAME"
    )
    if ($schedule -eq "HOURLY") {
        $arguments += @( "/MO", $HourlyInterval.ToString() )
    }
    $arguments += $additionalArgs
    Write-Output "Running: schtasks $($arguments -join ' ')"
    Start-Process -FilePath "schtasks.exe" -ArgumentList $arguments -Wait -NoNewWindow
}

Write-Output "Mendaftarkan task untuk sync Blue Whale (per jam + at startup)..."
CreateScheduledTask -name "Hourly" -schedule "HOURLY"
CreateScheduledTask -name "AtStartup" -schedule "ONSTART"

Write-Output "Task sudah dibuat. Jalankan Task Scheduler untuk melihat status."


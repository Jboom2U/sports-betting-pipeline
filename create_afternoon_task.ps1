# create_afternoon_task.ps1
# Double-click this file to create the MLB Afternoon Refresh scheduled task.
# It will automatically request Administrator rights via UAC prompt.

# ── Self-elevate if not already running as Administrator ─────────────────────
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]"Administrator")) {
    Start-Process PowerShell.exe "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    Exit
}

# ── Create the task ───────────────────────────────────────────────────────────
$taskName   = "MLB Afternoon Refresh"
$batFile    = "C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline\run_afternoon.bat"
$workingDir = "C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline"

$action  = New-ScheduledTaskAction -Execute $batFile -WorkingDirectory $workingDir
$trigger = New-ScheduledTaskTrigger -Daily -At "11:30AM"
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -RestartCount 1 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName   $taskName `
    -Action     $action `
    -Trigger    $trigger `
    -Settings   $settings `
    -RunLevel   Highest `
    -Force

Write-Host ""
Write-Host "Task '$taskName' created successfully." -ForegroundColor Green
Write-Host "Runs daily at 11:30 AM." -ForegroundColor Green
Write-Host ""
Write-Host "To test it now, run:" -ForegroundColor Yellow
Write-Host "  schtasks /run /tn `"$taskName`"" -ForegroundColor Yellow
Write-Host ""
Pause

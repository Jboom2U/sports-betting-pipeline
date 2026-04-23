# create_task.ps1
# Run this once by right-clicking and selecting "Run with PowerShell"
# Creates the Sports Betting Parlay Genius daily scheduled task at 7:00 AM

$taskName   = "Sports Betting Parlay Genius"
$batFile    = "C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline\run_daily.bat"
$workingDir = "C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline"

$action  = New-ScheduledTaskAction -Execute $batFile -WorkingDirectory $workingDir
$trigger = New-ScheduledTaskTrigger -Daily -At "07:00AM"
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
Write-Host "Runs daily at 7:00 AM." -ForegroundColor Green
Write-Host ""
Write-Host "To test it now, run:" -ForegroundColor Yellow
Write-Host "  schtasks /run /tn `"$taskName`"" -ForegroundColor Yellow
Write-Host ""
Pause

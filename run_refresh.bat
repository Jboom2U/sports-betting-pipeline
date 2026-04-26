@echo off
:: ============================================================
:: Sports Betting - Intraday Refresh
:: Runs every 30 min via Task Scheduler to pull fresh odds,
:: regenerate the dashboard, and push to GitHub Pages.
:: ============================================================

set REPO=C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline
set LOG=%REPO%\logs\refresh_%date:~10,4%-%date:~4,2%-%date:~7,2%.log

cd /d %REPO%

echo [%date% %time%] Intraday refresh starting... >> "%LOG%"

:: Regenerate dashboard (odds scraper runs automatically inside this script)
python run_picks_html.py --no-open >> "%LOG%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ERROR: Dashboard refresh failed. >> "%LOG%"
    exit /b 1
)

:: Commit and push updated HTML to GitHub Pages
git add picks/ >> "%LOG%" 2>&1
git commit -m "chore: intraday odds refresh %date:~10,4%-%date:~4,2%-%date:~7,2% %time:~0,5%" >> "%LOG%" 2>&1
git push origin main >> "%LOG%" 2>&1

echo [%date% %time%] Refresh complete. >> "%LOG%"

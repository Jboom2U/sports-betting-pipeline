@echo off
:: ============================================================
:: Sports Betting Parlay Genius - Daily Automation
:: Runs every morning to pull fresh data, generate picks,
:: commit the HTML dashboard, and push to GitHub Pages.
:: ============================================================

set REPO=C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline
set LOG=%REPO%\logs\daily_auto_%date:~10,4%-%date:~4,2%-%date:~7,2%.log

cd /d %REPO%

echo [%date% %time%] Starting daily pipeline... >> "%LOG%"

:: Step 1 - Pull fresh data
echo [%date% %time%] Step 1: Running pipeline... >> "%LOG%"
python run_pipeline.py >> "%LOG%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ERROR: Pipeline failed. >> "%LOG%"
    exit /b 1
)

:: Step 2 - Grade yesterday's picks (generates mlb_analysis_YYYY-MM-DD.json for Yesterday panel)
echo [%date% %time%] Step 2: Grading yesterday's picks... >> "%LOG%"
python run_analysis.py >> "%LOG%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] WARNING: Analysis failed - continuing anyway. >> "%LOG%"
)

:: Step 3 - Generate HTML picks dashboard
echo [%date% %time%] Step 3: Generating picks dashboard... >> "%LOG%"
python run_picks_html.py --no-open >> "%LOG%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ERROR: Picks generation failed. >> "%LOG%"
    exit /b 1
)

:: Step 3 - Commit and push to GitHub Pages
echo [%date% %time%] Step 3: Pushing to GitHub... >> "%LOG%"
git add picks/ data/clean/ >> "%LOG%" 2>&1
git commit -m "chore: daily picks update %date:~10,4%-%date:~4,2%-%date:~7,2%" >> "%LOG%" 2>&1
git push origin main >> "%LOG%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] WARNING: Git push failed - check credentials. >> "%LOG%"
)

echo [%date% %time%] Daily run complete. >> "%LOG%"
echo Done! Picks are live on GitHub Pages.

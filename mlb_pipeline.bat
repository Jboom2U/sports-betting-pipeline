@echo off
:: ============================================================
:: mlb_pipeline.bat
:: Called by Windows Task Scheduler at 4:00 AM daily.
:: Edit PYTHON_PATH and PROJECT_PATH before first run.
:: ============================================================

:: ── CONFIGURATION ────────────────────────────────────────────
set PROJECT_PATH=C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline
:: ─────────────────────────────────────────────────────────────

set LOG_FILE=%PROJECT_PATH%\logs\task_scheduler_%DATE:~-4,4%-%DATE:~-7,2%-%DATE:~-10,2%.log

echo [%DATE% %TIME%] Task Scheduler trigger fired >> "%LOG_FILE%"
cd /d "%PROJECT_PATH%"

python run_pipeline.py >> "%LOG_FILE%" 2>&1
echo [%DATE% %TIME%] Pipeline complete with code %ERRORLEVEL% >> "%LOG_FILE%"

python run_picks_html.py --no-open >> "%LOG_FILE%" 2>&1
echo [%DATE% %TIME%] Picks HTML generated >> "%LOG_FILE%"

git add picks\ data\clean\ >> "%LOG_FILE%" 2>&1
git commit -m "chore: auto picks update %DATE%" >> "%LOG_FILE%" 2>&1
git push origin main >> "%LOG_FILE%" 2>&1
echo [%DATE% %TIME%] Pushed to GitHub Pages >> "%LOG_FILE%"

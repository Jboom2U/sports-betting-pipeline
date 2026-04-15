@echo off
:: ============================================================
:: mlb_pipeline.bat
:: Called by Windows Task Scheduler at 4:00 AM daily.
:: Edit PYTHON_PATH and PROJECT_PATH before first run.
:: ============================================================

:: ── EDIT THESE TWO LINES ─────────────────────────────────────
set PYTHON_PATH=C:\Users\YourUsername\AppData\Local\Programs\Python\Python311\python.exe
set PROJECT_PATH=C:\Users\YourUsername\Documents\mlb-betting-pipeline
:: ─────────────────────────────────────────────────────────────

set LOG_FILE=%PROJECT_PATH%\logs\task_scheduler_%DATE:~-4,4%-%DATE:~-7,2%-%DATE:~-10,2%.log

echo [%DATE% %TIME%] Task Scheduler trigger fired >> "%LOG_FILE%"
cd /d "%PROJECT_PATH%"
"%PYTHON_PATH%" run_pipeline.py >> "%LOG_FILE%" 2>&1
echo [%DATE% %TIME%] Pipeline exited with code %ERRORLEVEL% >> "%LOG_FILE%"

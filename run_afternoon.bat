@echo off
REM ─────────────────────────────────────────────────────────────
REM  run_afternoon.bat
REM  Afternoon lineup + props refresh — scheduled at 11:30 AM ET
REM
REM  Add to Windows Task Scheduler:
REM    Action:    run_afternoon.bat
REM    Trigger:   Daily at 11:30 AM
REM    Start in:  C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline
REM ─────────────────────────────────────────────────────────────

cd /d "%~dp0"
python run_afternoon.py --no-open >> logs\afternoon_%date:~-4,4%-%date:~-10,2%-%date:~-7,2%.log 2>&1

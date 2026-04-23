@echo off
:: Sports Betting Parlay Genius - Local Server
:: Double-click this to start the dashboard locally with the Refresh button active.
:: Opens automatically in your browser at http://localhost:8765

cd /d C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline
echo Starting Sports Betting Parlay Genius local server...
echo Dashboard will open at http://localhost:8765
echo Press Ctrl+C to stop.
echo.
python serve_picks.py
pause

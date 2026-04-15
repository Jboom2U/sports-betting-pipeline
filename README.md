# MLB Betting Data Pipeline

Daily automated data collection for MLB scores, standings, injuries, and schedules.
Feeds into the parlay optimization tool. Runs at 4:00 AM via Windows Task Scheduler.

---

## Repo Structure

```
mlb-betting-pipeline/
├── data/
│   ├── raw/              # Daily raw CSVs (not pushed to GitHub)
│   └── clean/            # Master files — appended daily, pushed to GitHub
├── scrapers/
│   └── mlb_scraper.py    # Pulls from MLB Stats API (free, no key needed)
├── normalize/
│   └── mlb_normalize.py  # Cleans + appends to master files
├── logs/                 # Pipeline logs (local only)
├── run_pipeline.py       # Single entry point
├── mlb_pipeline.bat      # Task Scheduler trigger
└── requirements.txt
```

---

## First-Time Setup

### 1. Clone the Repo (GitHub Desktop)
1. Open GitHub Desktop
2. File > Clone Repository
3. Choose this repo, pick your local path (e.g. `C:\Users\YourName\Documents\mlb-betting-pipeline`)
4. Click Clone

### 2. Install Dependencies
Open a Command Prompt in the project folder and run:
```
pip install -r requirements.txt
```

### 3. Run a Manual Test First
```
python run_pipeline.py
```
Check the `logs/` folder and `data/clean/` to confirm files were created.

### 4. Backfill Historical Data (Optional)
To pull a specific past date:
```
python run_pipeline.py --date 2025-04-10
```
Run this repeatedly with different dates to build history.

---

## Windows Task Scheduler Setup

1. Open **Task Scheduler** (search in Start menu)
2. Click **Create Basic Task**
3. Name it: `MLB Betting Pipeline`
4. Trigger: **Daily** at **4:00 AM**
5. Action: **Start a Program**
6. Program/script: Browse to `mlb_pipeline.bat` in your project folder
7. Click Finish

**Before first run:** Open `mlb_pipeline.bat` and update:
- `PYTHON_PATH` — path to your python.exe (run `where python` in Command Prompt to find it)
- `PROJECT_PATH` — path to your project folder

---

## Data Sources

| Source | What | Cost |
|--------|------|------|
| statsapi.mlb.com | Scores, schedule, standings, transactions | Free, no key |

---

## Output Files (data/clean/)

| File | Contents | Dedup Key |
|------|----------|-----------|
| `mlb_scores_master.csv` | Final scores, pitcher decisions | game_id + date |
| `mlb_standings_master.csv` | Daily team standings snapshot | team + date |
| `mlb_injuries_master.csv` | IL placements and activations | player_id + date |
| `mlb_schedule_master.csv` | Upcoming games + probable pitchers | game_id + date |

---

## GitHub Workflow (Daily)

After the pipeline runs each morning:
1. Open GitHub Desktop
2. You'll see new/changed files in `data/clean/`
3. Add a commit message like `data: 2025-04-14 daily update`
4. Click **Commit to main**
5. Click **Push origin**

Optionally automate the git push by adding these lines to `mlb_pipeline.bat`:
```bat
cd /d "%PROJECT_PATH%"
git add data/clean/
git commit -m "data: %DATE% daily update"
git push origin main
```

---

## Adding More Sports Later

1. Copy `scrapers/mlb_scraper.py` to `scrapers/nba_scraper.py`
2. Update the API endpoints and field names for that sport
3. Copy `normalize/mlb_normalize.py` to `normalize/nba_normalize.py`
4. Add the new sport's `run()` calls to `run_pipeline.py`
5. New master files will be created automatically in `data/clean/`

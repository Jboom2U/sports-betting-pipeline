"""
mlb_historical_scraper.py
One-time backfill: pulls all game scores for the 2023, 2024, and 2025 MLB seasons.
Rate-limited to be respectful of the MLB Stats API (~150 req/min).
Resume-safe: skips dates already recorded in the progress file.
"""

import requests
import csv
import os
import time
import logging
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

BASE_DIR      = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR       = os.path.join(BASE_DIR, "data", "raw")
PROGRESS_FILE = os.path.join(RAW_DIR, ".historical_progress.txt")
os.makedirs(RAW_DIR, exist_ok=True)

MLB_API = "https://statsapi.mlb.com/api/v1"
HEADERS = {"User-Agent": "mlb-betting-pipeline/1.0"}

# Season date ranges (inclusive). 2025 ends at yesterday.
SEASONS = [
    ("2023-03-30", "2023-10-01", "2023"),
    ("2024-03-20", "2024-09-29", "2024"),
    ("2025-03-27", "2025-04-13", "2025"),
]

OUTFILE = os.path.join(RAW_DIR, "mlb_historical_scores.csv")

FIELDNAMES = [
    "record_type", "game_id", "game_date", "season", "status",
    "away_team", "away_team_id", "away_score", "away_hits", "away_errors",
    "home_team", "home_team_id", "home_score", "home_hits", "home_errors",
    "winner", "run_line_result", "total_runs",
    "winning_pitcher", "losing_pitcher", "save_pitcher",
    "innings", "venue", "timestamp",
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def date_range(start: str, end: str):
    cur = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur <= end_dt:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def load_completed_dates() -> set:
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())


def mark_completed(date_str: str):
    with open(PROGRESS_FILE, "a") as f:
        f.write(date_str + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────────────────
def fetch_scores_for_date(date_str: str, season: str) -> list:
    url = f"{MLB_API}/schedule"
    params = {
        "sportId": 1,
        "date": date_str,
        "hydrate": "linescore,decisions,probablePitcher",
        "gameType": "R",
    }
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Failed to fetch {date_str}: {e}")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            status = game.get("status", {}).get("abstractGameState", "")
            if status != "Final":
                continue

            away = game["teams"]["away"]
            home = game["teams"]["home"]
            decisions  = game.get("decisions", {})
            linescore  = game.get("linescore", {})

            away_score = int(away.get("score") or 0)
            home_score = int(home.get("score") or 0)
            total_runs = away_score + home_score
            margin     = abs(home_score - away_score)

            if home_score > away_score:
                winner          = home["team"]["name"]
                run_line_result = "home_covered" if margin >= 2 else "away_covered"
            else:
                winner          = away["team"]["name"]
                run_line_result = "away_covered" if margin >= 2 else "home_covered"

            rows.append({
                "record_type":     "score",
                "game_id":         game.get("gamePk"),
                "game_date":       date_str,
                "season":          season,
                "status":          status,
                "away_team":       away["team"]["name"],
                "away_team_id":    away["team"]["id"],
                "away_score":      away_score,
                "away_hits":       linescore.get("teams", {}).get("away", {}).get("hits", ""),
                "away_errors":     linescore.get("teams", {}).get("away", {}).get("errors", ""),
                "home_team":       home["team"]["name"],
                "home_team_id":    home["team"]["id"],
                "home_score":      home_score,
                "home_hits":       linescore.get("teams", {}).get("home", {}).get("hits", ""),
                "home_errors":     linescore.get("teams", {}).get("home", {}).get("errors", ""),
                "winner":          winner,
                "run_line_result": run_line_result,
                "total_runs":      total_runs,
                "winning_pitcher": decisions.get("winner", {}).get("fullName", ""),
                "losing_pitcher":  decisions.get("loser",  {}).get("fullName", ""),
                "save_pitcher":    decisions.get("save",   {}).get("fullName", ""),
                "innings":         linescore.get("currentInning", 9),
                "venue":           game.get("venue", {}).get("name", ""),
                "timestamp":       timestamp,
            })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run() -> dict:
    log.info("=" * 60)
    log.info("Historical Scores Backfill started")
    log.info("=" * 60)

    completed = load_completed_dates()

    all_dates = []
    for start, end, season_year in SEASONS:
        for d in date_range(start, end):
            if d not in completed:
                all_dates.append((d, season_year))

    log.info(f"Dates to pull: {len(all_dates)} | Already done: {len(completed)}")

    if not all_dates:
        log.info("All dates already pulled. Nothing to do.")
        return {"dates_pulled": 0, "games_found": 0}

    write_header = not os.path.exists(OUTFILE) or os.path.getsize(OUTFILE) == 0
    total_games  = 0
    dates_pulled = 0

    with open(OUTFILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()

        for i, (date_str, season) in enumerate(all_dates):
            rows = fetch_scores_for_date(date_str, season)
            if rows:
                writer.writerows(rows)
                f.flush()
                total_games += len(rows)

            mark_completed(date_str)
            dates_pulled += 1

            if (i + 1) % 25 == 0 or (i + 1) == len(all_dates):
                pct = ((i + 1) / len(all_dates)) * 100
                log.info(f"Progress: {i+1}/{len(all_dates)} dates ({pct:.1f}%) | Games: {total_games}")

            time.sleep(0.4)

    log.info(f"Backfill complete | {dates_pulled} dates | {total_games} games")
    return {"dates_pulled": dates_pulled, "games_found": total_games}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])
    run()

"""
mlb_scraper.py
Pulls scores, standings, player stats, and injuries from the free MLB Stats API.
No API key required. Runs daily at 4am via Windows Task Scheduler.
"""

import requests
import csv
import json
import os
import logging
from datetime import datetime, timedelta

# ── Logging setup ────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR    = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# ── MLB Stats API base ────────────────────────────────────────────────────────
MLB_API = "https://statsapi.mlb.com/api/v1"

HEADERS = {"User-Agent": "mlb-betting-pipeline/1.0"}

TODAY      = datetime.now().strftime("%Y-%m-%d")
YESTERDAY  = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
TIMESTAMP  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
# SCORES  (yesterday's completed games)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_scores(date: str = YESTERDAY) -> list[dict]:
    """Pull completed game scores for a given date."""
    url = f"{MLB_API}/schedule"
    params = {
        "sportId": 1,
        "date": date,
        "hydrate": "linescore,decisions,probablePitcher",
        "gameType": "R"          # Regular season only; add "P" for playoffs later
    }
    log.info(f"Fetching scores for {date}")
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            status = game.get("status", {}).get("abstractGameState", "")
            if status != "Final":
                log.warning(f"Game {game.get('gamePk')} not Final yet, skipping.")
                continue

            away = game["teams"]["away"]
            home = game["teams"]["home"]
            decisions = game.get("decisions", {})
            linescore = game.get("linescore", {})

            rows.append({
                "record_type":       "score",
                "game_id":           game.get("gamePk"),
                "game_date":         date,
                "status":            status,
                "away_team":         away["team"]["name"],
                "away_team_id":      away["team"]["id"],
                "away_score":        away.get("score", ""),
                "away_hits":         linescore.get("teams", {}).get("away", {}).get("hits", ""),
                "away_errors":       linescore.get("teams", {}).get("away", {}).get("errors", ""),
                "home_team":         home["team"]["name"],
                "home_team_id":      home["team"]["id"],
                "home_score":        home.get("score", ""),
                "home_hits":         linescore.get("teams", {}).get("home", {}).get("hits", ""),
                "home_errors":       linescore.get("teams", {}).get("home", {}).get("errors", ""),
                "winning_pitcher":   decisions.get("winner", {}).get("fullName", ""),
                "losing_pitcher":    decisions.get("loser",  {}).get("fullName", ""),
                "save_pitcher":      decisions.get("save",   {}).get("fullName", ""),
                "innings":           linescore.get("currentInning", 9),
                "venue":             game.get("venue", {}).get("name", ""),
                "timestamp":         TIMESTAMP,
            })

    log.info(f"Scores fetched: {len(rows)} games")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# STANDINGS
# ─────────────────────────────────────────────────────────────────────────────
def fetch_standings() -> list[dict]:
    """Pull current AL and NL standings."""
    url = f"{MLB_API}/standings"
    params = {"leagueId": "103,104", "season": datetime.now().year, "standingsType": "regularSeason"}
    log.info("Fetching standings")
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for record in data.get("records", []):
        division = record.get("division", {}).get("name", "")
        for team_record in record.get("teamRecords", []):
            team = team_record.get("team", {})
            rows.append({
                "record_type":        "standing",
                "game_date":          TODAY,
                "team":               team.get("name", ""),
                "team_id":            team.get("id", ""),
                "division":           division,
                "wins":               team_record.get("wins", ""),
                "losses":             team_record.get("losses", ""),
                "pct":                team_record.get("winningPercentage", ""),
                "games_back":         team_record.get("gamesBack", ""),
                "home_record":        team_record.get("records", {}).get("splitRecords", [{}])[0].get("wins", ""),
                "away_record":        team_record.get("records", {}).get("splitRecords", [{}])[1].get("wins", "") if len(team_record.get("records", {}).get("splitRecords", [])) > 1 else "",
                "streak":             team_record.get("streak", {}).get("streakCode", ""),
                "runs_scored":        team_record.get("runsScored", ""),
                "runs_allowed":       team_record.get("runsAllowed", ""),
                "run_differential":   team_record.get("runDifferential", ""),
                "last_10":            next((r.get("wins","") for r in team_record.get("records",{}).get("splitRecords",[]) if r.get("type") == "lastTen"), ""),
                "timestamp":          TIMESTAMP,
            })

    log.info(f"Standings fetched: {len(rows)} teams")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# INJURIES
# ─────────────────────────────────────────────────────────────────────────────
def fetch_injuries() -> list[dict]:
    """
    Pull injury / transaction data from the MLB transactions endpoint.
    Covers IL placements, activations, and DL moves for yesterday.
    """
    url = f"{MLB_API}/transactions"
    params = {"startDate": YESTERDAY, "endDate": TODAY, "sportId": 1}
    log.info("Fetching injury / transaction data")

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Injury fetch failed: {e}")
        return []

    IL_KEYWORDS = {"injured list", "il", "disabled list", "dl", "60-day", "15-day", "7-day"}

    rows = []
    for txn in data.get("transactions", []):
        desc = (txn.get("description") or "").lower()
        txn_type = (txn.get("typeDesc") or "").lower()

        # Only keep IL-related transactions
        if not any(kw in desc or kw in txn_type for kw in IL_KEYWORDS):
            continue

        person = txn.get("person", {})
        team   = txn.get("toTeam") or txn.get("fromTeam") or {}

        rows.append({
            "record_type":    "injury",
            "game_date":      TODAY,
            "player_name":    person.get("fullName", ""),
            "player_id":      person.get("id", ""),
            "team":           team.get("name", ""),
            "team_id":        team.get("id", ""),
            "transaction":    txn.get("typeDesc", ""),
            "description":    txn.get("description", ""),
            "effective_date": txn.get("date", ""),
            "timestamp":      TIMESTAMP,
        })

    log.info(f"Injury transactions fetched: {len(rows)}")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# UPCOMING SCHEDULE  (today + next 2 days for odds prep)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_schedule(days_ahead: int = 2) -> list[dict]:
    """Pull upcoming game schedule so the odds layer has game_ids ready."""
    rows = []
    for offset in range(0, days_ahead + 1):
        target_date = (datetime.now() + timedelta(days=offset)).strftime("%Y-%m-%d")
        url = f"{MLB_API}/schedule"
        params = {
            "sportId": 1,
            "date": target_date,
            "hydrate": "probablePitcher,venue",
            "gameType": "R"
        }
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for date_entry in data.get("dates", []):
            for game in date_entry.get("games", []):
                away = game["teams"]["away"]
                home = game["teams"]["home"]
                rows.append({
                    "record_type":          "schedule",
                    "game_id":              game.get("gamePk"),
                    "game_date":            target_date,
                    "game_time_utc":        game.get("gameDate", ""),
                    "status":               game.get("status", {}).get("abstractGameState", ""),
                    "away_team":            away["team"]["name"],
                    "away_team_id":         away["team"]["id"],
                    "away_probable_pitcher":away.get("probablePitcher", {}).get("fullName", ""),
                    "home_team":            home["team"]["name"],
                    "home_team_id":         home["team"]["id"],
                    "home_probable_pitcher":home.get("probablePitcher", {}).get("fullName", ""),
                    "venue":                game.get("venue", {}).get("name", ""),
                    "timestamp":            TIMESTAMP,
                })

    log.info(f"Schedule rows fetched: {len(rows)}")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# WRITE RAW CSV
# ─────────────────────────────────────────────────────────────────────────────
def write_raw(rows: list[dict], record_type: str):
    """Write a dated raw CSV for each record type. One file per day per type."""
    if not rows:
        log.warning(f"No rows to write for {record_type}")
        return

    filename = os.path.join(RAW_DIR, f"mlb_{record_type}_{TODAY}.csv")
    fieldnames = list(rows[0].keys())

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"Raw file written: {filename} ({len(rows)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info(f"MLB Scraper started | {TIMESTAMP}")
    log.info("=" * 60)

    scores    = fetch_scores()
    standings = fetch_standings()
    injuries  = fetch_injuries()
    schedule  = fetch_schedule()

    write_raw(scores,    "scores")
    write_raw(standings, "standings")
    write_raw(injuries,  "injuries")
    write_raw(schedule,  "schedule")

    log.info("MLB Scraper complete.")
    return {
        "scores":    len(scores),
        "standings": len(standings),
        "injuries":  len(injuries),
        "schedule":  len(schedule),
    }


if __name__ == "__main__":
    run()

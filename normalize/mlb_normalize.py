"""
mlb_normalize.py
Reads today's raw CSVs, cleans and standardizes them, then appends to master files.
Handles deduplication so re-runs are safe.
"""

import csv
import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)

TODAY     = datetime.now().strftime("%Y-%m-%d")
BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)

# ── Team name standardization map ────────────────────────────────────────────
# MLB API is generally consistent but this protects against any edge cases
# or future cross-source joins (e.g., Odds API uses different names)
TEAM_NAME_MAP = {
    "Arizona Diamondbacks":       "Arizona Diamondbacks",
    "D-backs":                    "Arizona Diamondbacks",
    "Atlanta Braves":             "Atlanta Braves",
    "Baltimore Orioles":          "Baltimore Orioles",
    "Boston Red Sox":             "Boston Red Sox",
    "Chicago Cubs":               "Chicago Cubs",
    "Chicago White Sox":          "Chicago White Sox",
    "Cincinnati Reds":            "Cincinnati Reds",
    "Cleveland Guardians":        "Cleveland Guardians",
    "Colorado Rockies":           "Colorado Rockies",
    "Detroit Tigers":             "Detroit Tigers",
    "Houston Astros":             "Houston Astros",
    "Kansas City Royals":         "Kansas City Royals",
    "Los Angeles Angels":         "Los Angeles Angels",
    "Los Angeles Dodgers":        "Los Angeles Dodgers",
    "Miami Marlins":              "Miami Marlins",
    "Milwaukee Brewers":          "Milwaukee Brewers",
    "Minnesota Twins":            "Minnesota Twins",
    "New York Mets":              "New York Mets",
    "New York Yankees":           "New York Yankees",
    "Oakland Athletics":          "Oakland Athletics",
    "Philadelphia Phillies":      "Philadelphia Phillies",
    "Pittsburgh Pirates":         "Pittsburgh Pirates",
    "San Diego Padres":           "San Diego Padres",
    "San Francisco Giants":       "San Francisco Giants",
    "Seattle Mariners":           "Seattle Mariners",
    "St. Louis Cardinals":        "St. Louis Cardinals",
    "Tampa Bay Rays":             "Tampa Bay Rays",
    "Texas Rangers":              "Texas Rangers",
    "Toronto Blue Jays":          "Toronto Blue Jays",
    "Washington Nationals":       "Washington Nationals",
    # Athletics moved to Sacramento — map both
    "Sacramento Athletics":       "Oakland Athletics",
}

def normalize_team(name: str) -> str:
    return TEAM_NAME_MAP.get(name, name)

def normalize_player(name: str) -> str:
    """Standardize player names: strip extra spaces, title case."""
    if not name:
        return ""
    return " ".join(name.strip().split()).title()

def safe_float(val) -> str:
    """Convert to float string or empty string."""
    try:
        return str(float(val))
    except (ValueError, TypeError):
        return ""

def safe_int(val) -> str:
    try:
        return str(int(val))
    except (ValueError, TypeError):
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZE SCORES
# ─────────────────────────────────────────────────────────────────────────────
def normalize_scores(rows: list[dict]) -> list[dict]:
    cleaned = []
    for r in rows:
        cleaned.append({
            "record_type":     "score",
            "game_id":         safe_int(r.get("game_id")),
            "game_date":       r.get("game_date", ""),
            "status":          r.get("status", "").strip(),
            "away_team":       normalize_team(r.get("away_team", "")),
            "away_team_id":    safe_int(r.get("away_team_id")),
            "away_score":      safe_int(r.get("away_score")),
            "away_hits":       safe_int(r.get("away_hits")),
            "away_errors":     safe_int(r.get("away_errors")),
            "home_team":       normalize_team(r.get("home_team", "")),
            "home_team_id":    safe_int(r.get("home_team_id")),
            "home_score":      safe_int(r.get("home_score")),
            "home_hits":       safe_int(r.get("home_hits")),
            "home_errors":     safe_int(r.get("home_errors")),
            "winner":          normalize_team(r.get("home_team", "") if safe_int(r.get("home_score","0")) > safe_int(r.get("away_score","0")) else r.get("away_team", "")),
            "winning_pitcher": normalize_player(r.get("winning_pitcher", "")),
            "losing_pitcher":  normalize_player(r.get("losing_pitcher", "")),
            "save_pitcher":    normalize_player(r.get("save_pitcher", "")),
            "innings":         safe_int(r.get("innings")),
            "venue":           r.get("venue", "").strip(),
            "timestamp":       r.get("timestamp", ""),
        })
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZE STANDINGS
# ─────────────────────────────────────────────────────────────────────────────
def normalize_standings(rows: list[dict]) -> list[dict]:
    cleaned = []
    for r in rows:
        cleaned.append({
            "record_type":      "standing",
            "game_date":        r.get("game_date", ""),
            "team":             normalize_team(r.get("team", "")),
            "team_id":          safe_int(r.get("team_id")),
            "division":         r.get("division", "").strip(),
            "wins":             safe_int(r.get("wins")),
            "losses":           safe_int(r.get("losses")),
            "pct":              safe_float(r.get("pct")),
            "games_back":       r.get("games_back", "").strip(),
            "streak":           r.get("streak", "").strip(),
            "runs_scored":      safe_int(r.get("runs_scored")),
            "runs_allowed":     safe_int(r.get("runs_allowed")),
            "run_differential": safe_int(r.get("run_differential")),
            "last_10":          r.get("last_10", ""),
            "timestamp":        r.get("timestamp", ""),
        })
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZE INJURIES
# ─────────────────────────────────────────────────────────────────────────────
def normalize_injuries(rows: list[dict]) -> list[dict]:
    cleaned = []
    for r in rows:
        cleaned.append({
            "record_type":    "injury",
            "game_date":      r.get("game_date", ""),
            "player_name":    normalize_player(r.get("player_name", "")),
            "player_id":      safe_int(r.get("player_id")),
            "team":           normalize_team(r.get("team", "")),
            "team_id":        safe_int(r.get("team_id")),
            "transaction":    r.get("transaction", "").strip(),
            "description":    r.get("description", "").strip(),
            "effective_date": r.get("effective_date", "").strip(),
            "timestamp":      r.get("timestamp", ""),
        })
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZE SCHEDULE
# ─────────────────────────────────────────────────────────────────────────────
def normalize_schedule(rows: list[dict]) -> list[dict]:
    cleaned = []
    for r in rows:
        cleaned.append({
            "record_type":           "schedule",
            "game_id":               safe_int(r.get("game_id")),
            "game_date":             r.get("game_date", ""),
            "game_time_utc":         r.get("game_time_utc", "").strip(),
            "status":                r.get("status", "").strip(),
            "away_team":             normalize_team(r.get("away_team", "")),
            "away_team_id":          safe_int(r.get("away_team_id")),
            "away_probable_pitcher": normalize_player(r.get("away_probable_pitcher", "")),
            "home_team":             normalize_team(r.get("home_team", "")),
            "home_team_id":          safe_int(r.get("home_team_id")),
            "home_probable_pitcher": normalize_player(r.get("home_probable_pitcher", "")),
            "venue":                 r.get("venue", "").strip(),
            "timestamp":             r.get("timestamp", ""),
        })
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# APPEND TO MASTER (with deduplication)
# ─────────────────────────────────────────────────────────────────────────────
def append_to_master(rows: list[dict], record_type: str, dedup_key: str):
    """
    Appends cleaned rows to the master CSV.
    Deduplicates on dedup_key + game_date so re-runs never create duplicates.
    """
    master_file = os.path.join(CLEAN_DIR, f"mlb_{record_type}_master.csv")

    if not rows:
        log.warning(f"No cleaned rows to append for {record_type}")
        return

    fieldnames = list(rows[0].keys())
    existing_keys = set()

    # Load existing keys to dedup
    if os.path.exists(master_file):
        with open(master_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for existing_row in reader:
                key = (existing_row.get(dedup_key, ""), existing_row.get("game_date", ""))
                existing_keys.add(key)

    new_rows = []
    for row in rows:
        key = (row.get(dedup_key, ""), row.get("game_date", ""))
        if key not in existing_keys:
            new_rows.append(row)
            existing_keys.add(key)

    if not new_rows:
        log.info(f"No new rows for {record_type} — all already exist in master")
        return

    write_header = not os.path.exists(master_file)
    with open(master_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    log.info(f"Appended {len(new_rows)} new rows to {master_file}")


# ─────────────────────────────────────────────────────────────────────────────
# READ RAW
# ─────────────────────────────────────────────────────────────────────────────
def read_raw(record_type: str) -> list[dict]:
    filename = os.path.join(RAW_DIR, f"mlb_{record_type}_{TODAY}.csv")
    if not os.path.exists(filename):
        log.warning(f"Raw file not found: {filename}")
        return []
    with open(filename, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run():
    log.info("Normalizer started")

    # Scores
    raw_scores = read_raw("scores")
    clean_scores = normalize_scores(raw_scores)
    append_to_master(clean_scores, "scores", dedup_key="game_id")

    # Standings (dedup on team + date)
    raw_standings = read_raw("standings")
    clean_standings = normalize_standings(raw_standings)
    append_to_master(clean_standings, "standings", dedup_key="team")

    # Injuries (dedup on player_id + date)
    raw_injuries = read_raw("injuries")
    clean_injuries = normalize_injuries(raw_injuries)
    append_to_master(clean_injuries, "injuries", dedup_key="player_id")

    # Schedule (dedup on game_id + date)
    raw_schedule = read_raw("schedule")
    clean_schedule = normalize_schedule(raw_schedule)
    append_to_master(clean_schedule, "schedule", dedup_key="game_id")

    log.info("Normalizer complete")


if __name__ == "__main__":
    run()

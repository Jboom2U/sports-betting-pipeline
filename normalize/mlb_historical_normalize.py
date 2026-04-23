"""
mlb_historical_normalize.py
Normalizes and appends historical game scores, pitcher stats, and team stats
from data/raw/ into their respective master CSVs in data/clean/.
Deduplication is handled on all appends so re-runs are always safe.
"""

import csv
import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)

BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)

SEASONS = [2023, 2024, 2025]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def safe_int(val) -> str:
    try:
        return str(int(float(val)))
    except (ValueError, TypeError):
        return ""


def safe_float(val) -> str:
    try:
        return str(round(float(val), 4))
    except (ValueError, TypeError):
        return ""


def normalize_team(name: str) -> str:
    from normalize.mlb_normalize import TEAM_NAME_MAP
    return TEAM_NAME_MAP.get(name, name)


def normalize_player(name: str) -> str:
    if not name:
        return ""
    return " ".join(name.strip().split()).title()


def read_csv(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_master(new_rows: list, master_path: str, fieldnames: list):
    """Append new_rows to master CSV, writing header only if file is new."""
    if not new_rows:
        return
    write_header = not os.path.exists(master_path)
    with open(master_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)


# ─────────────────────────────────────────────────────────────────────────────
# HISTORICAL SCORES
# ─────────────────────────────────────────────────────────────────────────────
def normalize_historical_scores() -> int:
    infile = os.path.join(RAW_DIR, "mlb_historical_scores.csv")
    master = os.path.join(CLEAN_DIR, "mlb_scores_master.csv")

    raw_rows = read_csv(infile)
    if not raw_rows:
        log.warning("Historical scores file not found or empty, skipping")
        return 0

    # Load existing game_ids to dedup
    existing_ids = set()
    if os.path.exists(master):
        with open(master, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_ids.add(row.get("game_id", ""))

    fieldnames = [
        "record_type", "game_id", "game_date", "season", "status",
        "away_team", "away_team_id", "away_score", "away_hits", "away_errors",
        "home_team", "home_team_id", "home_score", "home_hits", "home_errors",
        "winner", "run_line_result", "total_runs",
        "winning_pitcher", "losing_pitcher", "save_pitcher",
        "innings", "venue", "timestamp",
    ]

    new_rows = []
    for r in raw_rows:
        gid = safe_int(r.get("game_id"))
        if gid in existing_ids:
            continue

        new_rows.append({
            "record_type":     "score",
            "game_id":         gid,
            "game_date":       r.get("game_date", ""),
            "season":          r.get("season", ""),
            "status":          "Final",
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
            "winner":          normalize_team(r.get("winner", "")),
            "run_line_result": r.get("run_line_result", ""),
            "total_runs":      safe_int(r.get("total_runs")),
            "winning_pitcher": normalize_player(r.get("winning_pitcher", "")),
            "losing_pitcher":  normalize_player(r.get("losing_pitcher", "")),
            "save_pitcher":    normalize_player(r.get("save_pitcher", "")),
            "innings":         safe_int(r.get("innings")),
            "venue":           r.get("venue", "").strip(),
            "timestamp":       r.get("timestamp", ""),
        })
        existing_ids.add(gid)

    append_master(new_rows, master, fieldnames)
    log.info(f"Historical scores: appended {len(new_rows)} rows to {master}")
    return len(new_rows)


# ─────────────────────────────────────────────────────────────────────────────
# PITCHER SEASON STATS
# ─────────────────────────────────────────────────────────────────────────────
def normalize_pitcher_stats() -> int:
    master = os.path.join(CLEAN_DIR, "mlb_pitcher_stats_master.csv")

    existing_keys = set()
    if os.path.exists(master):
        with open(master, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_keys.add((row.get("player_id", ""), str(row.get("season", ""))))

    all_new = []
    fieldnames = None

    for season in SEASONS:
        rows = read_csv(os.path.join(RAW_DIR, f"mlb_pitcher_stats_{season}.csv"))
        if not rows:
            log.warning(f"No pitcher stats file for {season}")
            continue
        if fieldnames is None:
            fieldnames = list(rows[0].keys())

        for row in rows:
            key = (row.get("player_id", ""), str(row.get("season", "")))
            if key in existing_keys:
                continue
            row["player_name"] = normalize_player(row.get("player_name", ""))
            row["team_name"]   = normalize_team(row.get("team_name", ""))
            all_new.append(row)
            existing_keys.add(key)

    append_master(all_new, master, fieldnames or [])
    log.info(f"Pitcher stats: appended {len(all_new)} rows to {master}")
    return len(all_new)


# ─────────────────────────────────────────────────────────────────────────────
# PITCHER HOME / AWAY SPLITS
# ─────────────────────────────────────────────────────────────────────────────
def normalize_pitcher_splits() -> int:
    master = os.path.join(CLEAN_DIR, "mlb_pitcher_splits_master.csv")

    existing_keys = set()
    if os.path.exists(master):
        with open(master, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_keys.add((row.get("player_id", ""), str(row.get("season", "")), row.get("split", "")))

    all_new = []
    fieldnames = None

    for season in SEASONS:
        rows = read_csv(os.path.join(RAW_DIR, f"mlb_pitcher_splits_{season}.csv"))
        if not rows:
            continue
        if fieldnames is None:
            fieldnames = list(rows[0].keys())

        for row in rows:
            key = (row.get("player_id", ""), str(row.get("season", "")), row.get("split", ""))
            if key in existing_keys:
                continue
            row["player_name"] = normalize_player(row.get("player_name", ""))
            all_new.append(row)
            existing_keys.add(key)

    append_master(all_new, master, fieldnames or [])
    log.info(f"Pitcher splits: appended {len(all_new)} rows to {master}")
    return len(all_new)


# ─────────────────────────────────────────────────────────────────────────────
# TEAM HITTING STATS
# ─────────────────────────────────────────────────────────────────────────────
def normalize_team_stats(stat_type: str) -> int:
    master = os.path.join(CLEAN_DIR, f"mlb_team_{stat_type}_master.csv")

    existing_keys = set()
    if os.path.exists(master):
        with open(master, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_keys.add((row.get("team_id", ""), str(row.get("season", ""))))

    all_new = []
    fieldnames = None

    for season in SEASONS:
        rows = read_csv(os.path.join(RAW_DIR, f"mlb_team_{stat_type}_{season}.csv"))
        if not rows:
            log.warning(f"No team {stat_type} file for {season}")
            continue
        if fieldnames is None:
            fieldnames = list(rows[0].keys())

        for row in rows:
            key = (row.get("team_id", ""), str(row.get("season", "")))
            if key in existing_keys:
                continue
            row["team_name"] = normalize_team(row.get("team_name", ""))
            all_new.append(row)
            existing_keys.add(key)

    append_master(all_new, master, fieldnames or [])
    log.info(f"Team {stat_type}: appended {len(all_new)} rows to {master}")
    return len(all_new)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run() -> dict:
    log.info("Historical Normalizer started")

    counts = {
        "historical_scores": normalize_historical_scores(),
        "pitcher_stats":     normalize_pitcher_stats(),
        "pitcher_splits":    normalize_pitcher_splits(),
        "team_hitting":      normalize_team_stats("hitting"),
        "team_pitching":     normalize_team_stats("pitching"),
    }

    log.info(f"Historical normalize complete: {counts}")
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])
    run()

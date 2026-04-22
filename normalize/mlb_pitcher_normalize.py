"""
mlb_pitcher_normalize.py
Normalizes and appends pitcher platoon splits and recent starts
to their master CSV files in data/clean/.
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

PLATOON_FIELDNAMES = [
    "season", "player_id", "player_name", "split",
    "games_started", "era", "whip", "innings_pitched",
    "strikeouts", "walks", "home_runs_allowed",
    "k_per_9", "bb_per_9", "fip", "batting_avg_against",
    "ops_against", "timestamp",
]

RECENT_FIELDNAMES = [
    "player_id", "player_name", "season",
    "game_date", "opponent", "is_home",
    "innings_pitched", "hits", "runs", "earned_runs",
    "strikeouts", "walks", "home_runs", "era_game",
    "game_score", "timestamp",
]


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_master(rows, master_path, fieldnames):
    if not rows:
        return
    write_header = not os.path.exists(master_path)
    with open(master_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        w.writerows(rows)


def normalize_player(name):
    if not name:
        return ""
    return " ".join(str(name).strip().split()).title()


# ─────────────────────────────────────────────────────────────────────────────
# PLATOON SPLITS
# ─────────────────────────────────────────────────────────────────────────────
def normalize_platoon_splits() -> int:
    master = os.path.join(CLEAN_DIR, "mlb_pitcher_platoon_master.csv")

    existing = set()
    if os.path.exists(master):
        with open(master, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing.add((row.get("player_id",""), str(row.get("season","")), row.get("split","")))

    all_new = []
    fieldnames = None

    for season in SEASONS:
        rows = read_csv(os.path.join(RAW_DIR, f"mlb_pitcher_platoon_{season}.csv"))
        if not rows:
            continue
        if fieldnames is None:
            fieldnames = list(rows[0].keys())

        for row in rows:
            key = (row.get("player_id",""), str(row.get("season","")), row.get("split",""))
            if key in existing:
                continue
            row["player_name"] = normalize_player(row.get("player_name",""))
            all_new.append(row)
            existing.add(key)

    append_master(all_new, master, fieldnames or PLATOON_FIELDNAMES)
    log.info(f"Platoon splits: appended {len(all_new)} rows")
    return len(all_new)


# ─────────────────────────────────────────────────────────────────────────────
# RECENT STARTS (daily — replaces existing rows for same pitcher/date)
# ─────────────────────────────────────────────────────────────────────────────
def normalize_recent_starts(rows: list = None) -> int:
    """
    Upsert recent start rows into mlb_pitcher_recent_master.csv.
    If rows is provided, use those directly. Otherwise reads from raw file.
    Deduplicates on player_id + game_date.
    """
    master = os.path.join(CLEAN_DIR, "mlb_pitcher_recent_master.csv")

    # Load existing
    existing_rows = read_csv(master)
    existing_keys = set()
    existing_map  = {}
    for r in existing_rows:
        k = (r.get("player_id",""), r.get("game_date",""))
        existing_keys.add(k)
        existing_map[k] = r

    if rows is None:
        today = datetime.now().strftime("%Y-%m-%d")
        raw   = os.path.join(RAW_DIR, f"mlb_recent_starts_{today}.csv")
        rows  = read_csv(raw)

    new_rows = []
    for row in rows:
        k = (row.get("player_id",""), row.get("game_date",""))
        if k not in existing_keys:
            row["player_name"] = normalize_player(row.get("player_name",""))
            new_rows.append(row)
            existing_keys.add(k)

    if new_rows:
        fieldnames = list(new_rows[0].keys())
        append_master(new_rows, master, fieldnames)

    log.info(f"Recent starts: appended {len(new_rows)} new rows")
    return len(new_rows)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run() -> dict:
    log.info("Pitcher Normalizer started")
    counts = {
        "platoon_splits": normalize_platoon_splits(),
        "recent_starts":  normalize_recent_starts(),
    }
    log.info(f"Pitcher normalize complete: {counts}")
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()

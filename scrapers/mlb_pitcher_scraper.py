"""
mlb_pitcher_scraper.py
Pulls starting pitcher season stats and home/away splits for 2023, 2024, 2025.
Calculates FIP, K/9, BB/9, HR/9 from raw API data.
Only includes pitchers with 3+ starts (filters out pure relievers).
"""

import requests
import csv
import os
import time
import logging
from datetime import datetime

log = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

MLB_API      = "https://statsapi.mlb.com/api/v1"
HEADERS      = {"User-Agent": "mlb-betting-pipeline/1.0"}
FIP_CONSTANT = 3.10   # League-average FIP constant (approximation)
SEASONS      = [2023, 2024, 2025]

STAT_FIELDNAMES = [
    "season", "player_id", "player_name", "team_id", "team_name",
    "games_played", "games_started", "wins", "losses",
    "era", "whip", "innings_pitched", "hits_allowed", "runs_allowed",
    "earned_runs", "home_runs_allowed", "strikeouts", "walks",
    "k_per_9", "bb_per_9", "hr_per_9", "fip", "win_pct", "timestamp",
]

SPLIT_FIELDNAMES = [
    "season", "player_id", "player_name", "split",
    "games_started", "era", "whip", "innings_pitched",
    "strikeouts", "walks", "home_runs_allowed",
    "k_per_9", "bb_per_9", "fip", "timestamp",
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def per_9(stat: float, ip: float) -> str:
    if ip <= 0:
        return ""
    return f"{(stat * 9) / ip:.2f}"


def fip(hr: float, bb: float, k: float, ip: float) -> str:
    if ip <= 0:
        return ""
    return f"{((13 * hr) + (3 * bb) - (2 * k)) / ip + FIP_CONSTANT:.3f}"


# ─────────────────────────────────────────────────────────────────────────────
# FETCH SEASON STATS
# ─────────────────────────────────────────────────────────────────────────────
def fetch_season_stats(season: int) -> list:
    url = f"{MLB_API}/stats"
    params = {
        "stats":      "season",
        "group":      "pitching",
        "season":     season,
        "playerPool": "All",
        "gameType":   "R",
        "sportId":    1,
        "limit":      1000,
    }
    log.info(f"Fetching pitcher season stats for {season}")
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Pitcher stats fetch failed ({season}): {e}")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            stat   = split.get("stat", {})
            player = split.get("player", {})
            team   = split.get("team", {})

            gs = int(stat.get("gamesStarted") or 0)
            if gs < 3:
                continue   # skip relievers

            ip = safe_float(stat.get("inningsPitched"))
            hr = safe_float(stat.get("homeRuns"))
            bb = safe_float(stat.get("baseOnBalls"))
            k  = safe_float(stat.get("strikeOuts"))
            w  = safe_float(stat.get("wins"))
            l  = safe_float(stat.get("losses"))

            rows.append({
                "season":            season,
                "player_id":         player.get("id", ""),
                "player_name":       player.get("fullName", ""),
                "team_id":           team.get("id", ""),
                "team_name":         team.get("name", ""),
                "games_played":      stat.get("gamesPlayed", ""),
                "games_started":     gs,
                "wins":              w,
                "losses":            l,
                "era":               stat.get("era", ""),
                "whip":              stat.get("whip", ""),
                "innings_pitched":   ip,
                "hits_allowed":      stat.get("hits", ""),
                "runs_allowed":      stat.get("runs", ""),
                "earned_runs":       stat.get("earnedRuns", ""),
                "home_runs_allowed": hr,
                "strikeouts":        k,
                "walks":             bb,
                "k_per_9":           per_9(k, ip),
                "bb_per_9":          per_9(bb, ip),
                "hr_per_9":          per_9(hr, ip),
                "fip":               fip(hr, bb, k, ip),
                "win_pct":           f"{w / max(w + l, 1):.3f}",
                "timestamp":         timestamp,
            })

    log.info(f"Pitcher season stats {season}: {len(rows)} qualified starters")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# FETCH HOME / AWAY SPLITS
# ─────────────────────────────────────────────────────────────────────────────
def fetch_home_away_splits(season: int) -> list:
    url = f"{MLB_API}/stats"
    params = {
        "stats":      "statSplits",
        "group":      "pitching",
        "season":     season,
        "sitCodes":   "h,a",
        "playerPool": "All",
        "gameType":   "R",
        "sportId":    1,
        "limit":      2000,
    }
    log.info(f"Fetching pitcher home/away splits for {season}")
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Pitcher splits fetch failed ({season}): {e}")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            stat        = split.get("stat", {})
            player      = split.get("player", {})
            split_label = split.get("split", {}).get("description", "")

            gs = int(stat.get("gamesStarted") or 0)
            if gs < 2:
                continue

            ip = safe_float(stat.get("inningsPitched"))
            hr = safe_float(stat.get("homeRuns"))
            bb = safe_float(stat.get("baseOnBalls"))
            k  = safe_float(stat.get("strikeOuts"))

            rows.append({
                "season":            season,
                "player_id":         player.get("id", ""),
                "player_name":       player.get("fullName", ""),
                "split":             split_label,
                "games_started":     gs,
                "era":               stat.get("era", ""),
                "whip":              stat.get("whip", ""),
                "innings_pitched":   ip,
                "strikeouts":        k,
                "walks":             bb,
                "home_runs_allowed": hr,
                "k_per_9":           per_9(k, ip),
                "bb_per_9":          per_9(bb, ip),
                "fip":               fip(hr, bb, k, ip),
                "timestamp":         timestamp,
            })

    log.info(f"Pitcher splits {season}: {len(rows)} records")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# LHB / RHB PLATOON SPLITS
# ─────────────────────────────────────────────────────────────────────────────
PLATOON_FIELDNAMES = [
    "season", "player_id", "player_name", "split",
    "games_started", "era", "whip", "innings_pitched",
    "strikeouts", "walks", "home_runs_allowed",
    "k_per_9", "bb_per_9", "fip", "batting_avg_against",
    "ops_against", "timestamp",
]

def fetch_platoon_splits(season: int) -> list:
    """Pull pitcher stats vs left-handed and right-handed batters."""
    url = f"{MLB_API}/stats"
    params = {
        "stats":      "statSplits",
        "group":      "pitching",
        "season":     season,
        "sitCodes":   "vl,vr",        # vl = vs LHB, vr = vs RHB
        "playerPool": "All",
        "gameType":   "R",
        "sportId":    1,
        "limit":      2000,
    }
    log.info(f"Fetching pitcher platoon splits (LHB/RHB) for {season}")
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Platoon splits fetch failed ({season}): {e}")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            stat        = split.get("stat", {})
            player      = split.get("player", {})
            split_label = split.get("split", {}).get("description", "")

            # Filter to meaningful sample (10+ PA proxy via AB)
            ab = safe_float(stat.get("atBats") or 0)
            if ab < 30:
                continue

            ip = safe_float(stat.get("inningsPitched"))
            hr = safe_float(stat.get("homeRuns"))
            bb = safe_float(stat.get("baseOnBalls"))
            k  = safe_float(stat.get("strikeOuts"))

            rows.append({
                "season":              season,
                "player_id":           player.get("id", ""),
                "player_name":         player.get("fullName", ""),
                "split":               split_label,   # "vs. Left" or "vs. Right"
                "games_started":       stat.get("gamesStarted", ""),
                "era":                 stat.get("era", ""),
                "whip":                stat.get("whip", ""),
                "innings_pitched":     ip,
                "strikeouts":          k,
                "walks":               bb,
                "home_runs_allowed":   hr,
                "k_per_9":             per_9(k, ip),
                "bb_per_9":            per_9(bb, ip),
                "fip":                 fip(hr, bb, k, ip),
                "batting_avg_against": stat.get("avg", ""),
                "ops_against":         stat.get("ops", ""),
                "timestamp":           timestamp,
            })

    log.info(f"Platoon splits {season}: {len(rows)} records")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# LAST N STARTS (recent form)
# ─────────────────────────────────────────────────────────────────────────────
RECENT_FIELDNAMES = [
    "player_id", "player_name", "season",
    "game_date", "opponent", "is_home",
    "innings_pitched", "hits", "runs", "earned_runs",
    "strikeouts", "walks", "home_runs", "era_game",
    "game_score", "timestamp",
]

def fetch_recent_starts(player_id: str, player_name: str,
                         season: int, n: int = 5) -> list:
    """Pull last N starts from a pitcher's game log."""
    url = f"{MLB_API}/people/{player_id}/stats"
    params = {
        "stats":    "gameLog",
        "group":    "pitching",
        "season":   season,
        "gameType": "R",
    }
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Game log failed for {player_name}: {e}")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    starts = []

    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            stat = split.get("stat", {})
            gs   = int(stat.get("gamesStarted") or 0)
            if gs == 0:
                continue    # skip relief appearances

            ip    = safe_float(stat.get("inningsPitched"))
            er    = safe_float(stat.get("earnedRuns"))
            era_g = round((er * 9 / ip), 2) if ip > 0 else 0.0

            # Bill James Game Score (simplified)
            k  = safe_float(stat.get("strikeOuts"))
            bb = safe_float(stat.get("baseOnBalls"))
            h  = safe_float(stat.get("hits"))
            gs_score = round(50 + (ip * 3) + k - (2 * bb) - (2 * h) - (3 * er), 1)

            game    = split.get("game", {})
            is_home = not split.get("isAway", False)
            opp     = (split.get("opponent") or {}).get("name", "")

            starts.append({
                "player_id":      player_id,
                "player_name":    player_name,
                "season":         season,
                "game_date":      split.get("date", ""),
                "opponent":       opp,
                "is_home":        is_home,
                "innings_pitched":ip,
                "hits":           h,
                "runs":           safe_float(stat.get("runs")),
                "earned_runs":    er,
                "strikeouts":     k,
                "walks":          bb,
                "home_runs":      safe_float(stat.get("homeRuns")),
                "era_game":       era_g,
                "game_score":     gs_score,
                "timestamp":      timestamp,
            })

    # Return most recent N starts sorted desc
    starts.sort(key=lambda x: x["game_date"], reverse=True)
    return starts[:n]


def fetch_all_recent_starts(pitcher_ids: list, season: int, n: int = 5) -> list:
    """Pull last N starts for a list of (player_id, player_name) tuples."""
    all_rows = []
    for pid, pname in pitcher_ids:
        rows = fetch_recent_starts(pid, pname, season, n)
        all_rows.extend(rows)
        time.sleep(0.25)
    log.info(f"Recent starts fetched: {len(all_rows)} rows for {len(pitcher_ids)} pitchers")
    return all_rows


# ─────────────────────────────────────────────────────────────────────────────
# WRITE
# ─────────────────────────────────────────────────────────────────────────────
def write_raw(rows: list, filename: str, fieldnames: list):
    if not rows:
        log.warning(f"No rows to write: {filename}")
        return
    path = os.path.join(RAW_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Written: {path} ({len(rows)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run() -> dict:
    log.info("=" * 60)
    log.info("Pitcher Scraper started")
    log.info("=" * 60)

    total_stats   = 0
    total_splits  = 0
    total_platoon = 0

    for season in SEASONS:
        stats = fetch_season_stats(season)
        write_raw(stats, f"mlb_pitcher_stats_{season}.csv", STAT_FIELDNAMES)
        total_stats += len(stats)
        time.sleep(0.5)

        splits = fetch_home_away_splits(season)
        write_raw(splits, f"mlb_pitcher_splits_{season}.csv", SPLIT_FIELDNAMES)
        total_splits += len(splits)
        time.sleep(0.5)

        platoon = fetch_platoon_splits(season)
        write_raw(platoon, f"mlb_pitcher_platoon_{season}.csv", PLATOON_FIELDNAMES)
        total_platoon += len(platoon)
        time.sleep(0.5)

    log.info(f"Pitcher scraper complete | {total_stats} stat rows | "
             f"{total_splits} split rows | {total_platoon} platoon rows")
    return {"pitcher_stats": total_stats, "pitcher_splits": total_splits,
            "pitcher_platoon": total_platoon}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])
    run()

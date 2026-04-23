"""
mlb_team_scraper.py
Pulls team-level hitting and pitching stats for 2023, 2024, 2025 seasons.
Calculates derived metrics: runs/game, K rate, BB rate, K/9, BB/9.
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

MLB_API = "https://statsapi.mlb.com/api/v1"
HEADERS = {"User-Agent": "mlb-betting-pipeline/1.0"}
SEASONS = [2023, 2024, 2025]

HITTING_FIELDNAMES = [
    "season", "team_id", "team_name", "games_played",
    "runs", "hits", "doubles", "triples", "home_runs", "rbi",
    "walks", "strikeouts", "batting_avg", "obp", "slg", "ops",
    "runs_per_game", "k_rate", "bb_rate", "timestamp",
]

PITCHING_FIELDNAMES = [
    "season", "team_id", "team_name", "games_played",
    "era", "whip", "strikeouts", "walks", "home_runs_allowed",
    "runs_allowed", "earned_runs", "innings_pitched",
    "k_per_9", "bb_per_9", "timestamp",
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def sf(val) -> str:
    return str(val) if val is not None else ""


# ─────────────────────────────────────────────────────────────────────────────
# FETCH TEAM HITTING
# ─────────────────────────────────────────────────────────────────────────────
def fetch_team_hitting(season: int) -> list:
    url = f"{MLB_API}/teams/stats"
    params = {
        "season":   season,
        "group":    "hitting",
        "stats":    "season",
        "gameType": "R",
        "sportId":  1,
    }
    log.info(f"Fetching team hitting stats for {season}")
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Team hitting fetch failed ({season}): {e}")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            stat = split.get("stat", {})
            team = split.get("team", {})

            gp   = safe_float(stat.get("gamesPlayed") or 1)
            runs = safe_float(stat.get("runs") or 0)
            k    = safe_float(stat.get("strikeOuts") or 0)
            bb   = safe_float(stat.get("baseOnBalls") or 0)
            ab   = safe_float(stat.get("atBats") or 1)
            hbp  = safe_float(stat.get("hitByPitch") or 0)
            sf_  = safe_float(stat.get("sacFlies") or 0)
            pa   = ab + bb + hbp + sf_

            rows.append({
                "season":        season,
                "team_id":       team.get("id", ""),
                "team_name":     team.get("name", ""),
                "games_played":  sf(stat.get("gamesPlayed")),
                "runs":          sf(stat.get("runs")),
                "hits":          sf(stat.get("hits")),
                "doubles":       sf(stat.get("doubles")),
                "triples":       sf(stat.get("triples")),
                "home_runs":     sf(stat.get("homeRuns")),
                "rbi":           sf(stat.get("rbi")),
                "walks":         sf(stat.get("baseOnBalls")),
                "strikeouts":    sf(stat.get("strikeOuts")),
                "batting_avg":   sf(stat.get("avg")),
                "obp":           sf(stat.get("obp")),
                "slg":           sf(stat.get("slg")),
                "ops":           sf(stat.get("ops")),
                "runs_per_game": f"{runs / max(gp, 1):.3f}",
                "k_rate":        f"{k / max(pa, 1):.3f}",
                "bb_rate":       f"{bb / max(pa, 1):.3f}",
                "timestamp":     timestamp,
            })

    log.info(f"Team hitting {season}: {len(rows)} teams")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# FETCH TEAM PITCHING
# ─────────────────────────────────────────────────────────────────────────────
def fetch_team_pitching(season: int) -> list:
    url = f"{MLB_API}/teams/stats"
    params = {
        "season":   season,
        "group":    "pitching",
        "stats":    "season",
        "gameType": "R",
        "sportId":  1,
    }
    log.info(f"Fetching team pitching stats for {season}")
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Team pitching fetch failed ({season}): {e}")
        return []

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for stat_group in data.get("stats", []):
        for split in stat_group.get("splits", []):
            stat = split.get("stat", {})
            team = split.get("team", {})

            ip = safe_float(stat.get("inningsPitched") or 1)
            k  = safe_float(stat.get("strikeOuts") or 0)
            bb = safe_float(stat.get("baseOnBalls") or 0)

            rows.append({
                "season":            season,
                "team_id":           team.get("id", ""),
                "team_name":         team.get("name", ""),
                "games_played":      sf(stat.get("gamesPlayed")),
                "era":               sf(stat.get("era")),
                "whip":              sf(stat.get("whip")),
                "strikeouts":        sf(stat.get("strikeOuts")),
                "walks":             sf(stat.get("baseOnBalls")),
                "home_runs_allowed": sf(stat.get("homeRuns")),
                "runs_allowed":      sf(stat.get("runs")),
                "earned_runs":       sf(stat.get("earnedRuns")),
                "innings_pitched":   sf(ip),
                "k_per_9":           f"{(k * 9) / max(ip, 1):.2f}",
                "bb_per_9":          f"{(bb * 9) / max(ip, 1):.2f}",
                "timestamp":         timestamp,
            })

    log.info(f"Team pitching {season}: {len(rows)} teams")
    return rows


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
    log.info("Team Scraper started")
    log.info("=" * 60)

    total_hitting  = 0
    total_pitching = 0

    for season in SEASONS:
        hitting = fetch_team_hitting(season)
        write_raw(hitting, f"mlb_team_hitting_{season}.csv", HITTING_FIELDNAMES)
        total_hitting += len(hitting)
        time.sleep(0.5)

        pitching = fetch_team_pitching(season)
        write_raw(pitching, f"mlb_team_pitching_{season}.csv", PITCHING_FIELDNAMES)
        total_pitching += len(pitching)
        time.sleep(0.5)

    log.info(f"Team scraper complete | {total_hitting} hitting rows | {total_pitching} pitching rows")
    return {"team_hitting": total_hitting, "team_pitching": total_pitching}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])
    run()

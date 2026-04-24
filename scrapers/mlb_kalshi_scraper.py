"""
scrapers/mlb_kalshi_scraper.py
Fetches Kalshi prediction market implied probabilities for MLB games.

Kalshi is a CFTC-regulated prediction market. Real money trades on binary
outcomes, so market prices reflect the best available crowd-sourced probability
for each game. Comparing your model's win probability to Kalshi's implied
probability flags picks where the model agrees with the market (higher conviction)
vs. disagrees (potential model flaw or genuine edge).

Multi-snapshot mode: Each run APPENDS a new snapshot row rather than replacing
today's data. This enables Kalshi movement detection — tracking when the
prediction market shifts (often ahead of sportsbooks).

Movement thresholds (percentage points of implied probability):
    STEAM  — 5+ pp shift  (strong signal — market strongly repriced)
    DRIFT  — 2-4 pp shift (moderate signal — notable repricing)
    STABLE — <2 pp        (normal variance)

Setup:
    1. Create an account at kalshi.com and generate an API key
    2. Add KALSHI_API_KEY=your_key_id to your .env file

Output:
    data/clean/mlb_kalshi_master.csv   (all snapshots, appended)
    data/clean/mlb_kalshi_movement_YYYY-MM-DD.csv
    data/raw/mlb_kalshi_YYYY-MM-DD.json
"""

import os
import csv
import json
import logging
import re
from datetime import datetime

import requests

log = logging.getLogger(__name__)

BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(RAW_DIR,   exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Known Kalshi series tickers for MLB game winner markets.
MLB_SERIES = ["KXMLBW", "MLBWINNER", "KXMLB"]

# Model vs Kalshi signal thresholds
AGREE_THRESHOLD    = 0.04   # within 4 pp -> NEUTRAL
DISAGREE_THRESHOLD = 0.12   # 12+ pp apart, opposite sides of 50% -> DISAGREE

# Kalshi movement thresholds (percentage points of implied probability)
KALSHI_STEAM_THRESH = 0.05   # 5 pp move = strong market repricing
KALSHI_DRIFT_THRESH = 0.02   # 2 pp move = moderate repricing

MASTER_FIELDS = [
    "snapshot_date", "snapshot_time", "game_date",
    "away_team", "home_team",
    "kalshi_ticker",
    "kalshi_away_prob",   # implied probability (0-1)
    "kalshi_home_prob",
    "kalshi_yes_ask",     # raw ask price in cents (0-100)
    "kalshi_yes_bid",
    "kalshi_volume",
    "market_title",
]

KALSHI_MOVEMENT_FIELDS = [
    "away_team", "home_team", "game_date",
    "snap1_time", "snap2_time",
    "kalshi_away_open", "kalshi_away_now", "kalshi_away_move",
    "kalshi_home_open", "kalshi_home_now", "kalshi_home_move",
    "kalshi_signal", "kalshi_sharp_side",
    "timestamp",
]


# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────

def _get_api_key() -> str:
    """Load Kalshi API key from .env or environment."""
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("KALSHI_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    key = os.environ.get("KALSHI_API_KEY", "")
    if key:
        return key
    raise ValueError(
        "KALSHI_API_KEY not found. Add KALSHI_API_KEY=your_key_id to your .env file."
    )


def _headers(api_key: str) -> dict:
    return {
        "Authorization": api_key,
        "Content-Type":  "application/json",
        "User-Agent":    "mlb-betting-pipeline/1.0",
    }


# ─────────────────────────────────────────────────────────────────────────────
# FETCH MARKETS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_markets(api_key: str, series_ticker: str) -> list:
    """Fetch all open markets for a series ticker, paginating as needed."""
    url    = f"{KALSHI_BASE}/markets"
    cursor = None
    all_markets = []

    while True:
        params = {
            "limit":         200,
            "series_ticker": series_ticker,
            "status":        "open",
        }
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(url, params=params, headers=_headers(api_key), timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 404:
                log.debug(f"Series {series_ticker} not found on Kalshi")
                return []
            log.warning(f"Kalshi API error for {series_ticker}: {e}")
            return []
        except Exception as e:
            log.warning(f"Kalshi fetch failed: {e}")
            return []

        markets = data.get("markets", [])
        all_markets.extend(markets)

        cursor = data.get("cursor")
        if not cursor or not markets:
            break

    log.info(f"Fetched {len(all_markets)} markets for series {series_ticker}")
    return all_markets


def fetch_all_mlb_markets(api_key: str) -> list:
    """Try known MLB series tickers and combine deduplicated results."""
    all_markets  = []
    seen_tickers = set()

    for series in MLB_SERIES:
        markets = fetch_markets(api_key, series)
        for m in markets:
            t = m.get("ticker", "")
            if t not in seen_tickers:
                seen_tickers.add(t)
                all_markets.append(m)

    if not all_markets:
        log.info("Known series yielded no markets — trying broad search")
        all_markets = _broad_search(api_key)

    return all_markets


def _broad_search(api_key: str) -> list:
    """Search all open markets for anything matching 'mlb' or 'baseball'."""
    url = f"{KALSHI_BASE}/markets"
    try:
        resp = requests.get(
            url,
            params={"limit": 200, "status": "open"},
            headers=_headers(api_key),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Broad Kalshi search failed: {e}")
        return []

    mlb_markets = [
        m for m in data.get("markets", [])
        if "mlb" in m.get("title", "").lower()
        or "baseball" in m.get("title", "").lower()
    ]
    log.info(f"Broad search found {len(mlb_markets)} MLB-related markets")
    return mlb_markets


# ─────────────────────────────────────────────────────────────────────────────
# PARSE MARKET -> TEAM NAMES
# ─────────────────────────────────────────────────────────────────────────────

TEAM_ALIASES = {
    "diamondbacks": "Arizona Diamondbacks",
    "braves":       "Atlanta Braves",
    "orioles":      "Baltimore Orioles",
    "red sox":      "Boston Red Sox",
    "cubs":         "Chicago Cubs",
    "white sox":    "Chicago White Sox",
    "reds":         "Cincinnati Reds",
    "guardians":    "Cleveland Guardians",
    "rockies":      "Colorado Rockies",
    "tigers":       "Detroit Tigers",
    "astros":       "Houston Astros",
    "royals":       "Kansas City Royals",
    "angels":       "Los Angeles Angels",
    "dodgers":      "Los Angeles Dodgers",
    "marlins":      "Miami Marlins",
    "brewers":      "Milwaukee Brewers",
    "twins":        "Minnesota Twins",
    "mets":         "New York Mets",
    "yankees":      "New York Yankees",
    "athletics":    "Athletics",
    "phillies":     "Philadelphia Phillies",
    "pirates":      "Pittsburgh Pirates",
    "padres":       "San Diego Padres",
    "giants":       "San Francisco Giants",
    "mariners":     "Seattle Mariners",
    "cardinals":    "St. Louis Cardinals",
    "rays":         "Tampa Bay Rays",
    "rangers":      "Texas Rangers",
    "blue jays":    "Toronto Blue Jays",
    "nationals":    "Washington Nationals",
}


def _match_team(text: str) -> str:
    text_lower = text.lower()
    for alias, full_name in TEAM_ALIASES.items():
        if alias in text_lower:
            return full_name
    return text.strip()


def parse_market_teams(market: dict):
    """
    Parse (away_team, home_team, yes_prob) from a Kalshi market dict.
    Returns None if the title can't be parsed into two teams.
    """
    title = market.get("title", "")

    vs_patterns = [
        r'(.+?)\s+(?:vs\.?|v\.?|at|@)\s+(.+?)(?:\s*[-—]|$|\?)',
        r'(?:Will\s+)?(.+?)\s+(?:beat|defeat|win\s+(?:vs|against))\s+(.+?)(?:\s*\?|$)',
    ]

    team_a, team_b = None, None
    for pat in vs_patterns:
        m = re.search(pat, title, re.IGNORECASE)
        if m:
            team_a = _match_team(m.group(1))
            team_b = _match_team(m.group(2))
            break

    if not team_a or not team_b:
        return None

    yes_ask  = market.get("yes_ask", 50)
    yes_bid  = market.get("yes_bid", 50)
    yes_prob = ((yes_ask + yes_bid) / 2) / 100.0

    return team_a, team_b, yes_prob


def extract_game_probabilities(markets: list) -> list:
    """Convert raw Kalshi markets into game-level probability dicts."""
    games = []
    seen  = set()

    for m in markets:
        parsed = parse_market_teams(m)
        if not parsed:
            continue
        team_a, team_b, yes_prob = parsed

        key = tuple(sorted([team_a, team_b]))
        if key in seen:
            continue
        seen.add(key)

        games.append({
            "away_team":        team_a,
            "home_team":        team_b,
            "kalshi_ticker":    m.get("ticker", ""),
            "kalshi_away_prob": round(yes_prob, 3),
            "kalshi_home_prob": round(1 - yes_prob, 3),
            "kalshi_yes_ask":   m.get("yes_ask", ""),
            "kalshi_yes_bid":   m.get("yes_bid", ""),
            "kalshi_volume":    m.get("volume", 0),
            "market_title":     m.get("title", ""),
        })

    log.info(f"Parsed {len(games)} unique game markets from Kalshi")
    return games


# ─────────────────────────────────────────────────────────────────────────────
# SAVE / LOAD
# ─────────────────────────────────────────────────────────────────────────────

def save_raw(date: str, markets: list):
    path = os.path.join(RAW_DIR, f"mlb_kalshi_{date}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(markets, f, indent=2)
    log.info(f"Raw Kalshi data saved: {path}")


def save_master(date: str, snapshot_time_str: str, games: list):
    """
    APPEND this snapshot to the master CSV.
    Each call throughout the day adds a new set of rows so movement can be detected.
    """
    path = os.path.join(CLEAN_DIR, "mlb_kalshi_master.csv")
    write_header = not os.path.exists(path)

    new_rows = [
        {
            "snapshot_date":    date,
            "snapshot_time":    snapshot_time_str,
            "game_date":        date,
            "away_team":        g["away_team"],
            "home_team":        g["home_team"],
            "kalshi_ticker":    g["kalshi_ticker"],
            "kalshi_away_prob": g["kalshi_away_prob"],
            "kalshi_home_prob": g["kalshi_home_prob"],
            "kalshi_yes_ask":   g["kalshi_yes_ask"],
            "kalshi_yes_bid":   g["kalshi_yes_bid"],
            "kalshi_volume":    g["kalshi_volume"],
            "market_title":     g["market_title"],
        }
        for g in games
    ]

    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MASTER_FIELDS)
        if write_header:
            w.writeheader()
        w.writerows(new_rows)

    log.info(f"Kalshi snapshot appended: {len(new_rows)} games at {snapshot_time_str}")


def load_earliest_snapshot(today: str) -> list:
    """
    Load the EARLIEST Kalshi snapshot for today's games (opening market baseline).
    Used as the comparison baseline for movement detection.
    """
    path = os.path.join(CLEAN_DIR, "mlb_kalshi_master.csv")
    if not os.path.exists(path):
        return []

    with open(path, encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("game_date") == today]

    earliest = {}
    for r in rows:
        k = (r.get("away_team", ""), r.get("home_team", ""))
        if k not in earliest or r.get("snapshot_time", "") < earliest[k].get("snapshot_time", ""):
            earliest[k] = r
    return list(earliest.values())


def load_kalshi_for_date(date: str) -> dict:
    """
    Load the MOST RECENT Kalshi snapshot for each game on a given date.
    Returns dict keyed by sorted (team_a, team_b) tuple.
    Used for displaying current Kalshi probability on pick cards.
    """
    path = os.path.join(CLEAN_DIR, "mlb_kalshi_master.csv")
    if not os.path.exists(path):
        return {}

    data = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("game_date") != date:
                continue
            away = row.get("away_team", "").strip()
            home = row.get("home_team", "").strip()
            try:
                away_p = float(row.get("kalshi_away_prob", 0.5))
                home_p = float(row.get("kalshi_home_prob", 0.5))
            except ValueError:
                away_p, home_p = 0.5, 0.5
            key = tuple(sorted([away, home]))
            # Iterating chronologically means last row per game = most recent
            data[key] = {
                "away_team":        away,
                "home_team":        home,
                "kalshi_away_prob": away_p,
                "kalshi_home_prob": home_p,
                "kalshi_volume":    row.get("kalshi_volume", 0),
                "market_title":     row.get("market_title", ""),
            }
    return data


def load_kalshi_movement(date: str) -> list:
    """
    Load today's Kalshi movement records.
    Returns list of movement dicts with STEAM/DRIFT signals.
    """
    path = os.path.join(CLEAN_DIR, f"mlb_kalshi_movement_{date}.csv")
    if not os.path.exists(path):
        return []
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                sig = r.get("kalshi_signal", "STABLE")
                if sig in ("STEAM", "DRIFT"):
                    out.append(dict(r))
    except Exception as e:
        log.warning(f"Kalshi movement load failed: {e}")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# MOVEMENT DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _kalshi_signal(move: float) -> str:
    if move is None:
        return "NO_DATA"
    abs_m = abs(move)
    if abs_m >= KALSHI_STEAM_THRESH:
        return "STEAM"
    if abs_m >= KALSHI_DRIFT_THRESH:
        return "DRIFT"
    return "STABLE"


def detect_kalshi_movement(prev_snaps: list, curr_games: list, date: str, curr_time: str) -> list:
    """
    Compare current Kalshi snapshot to the earliest baseline for today.
    Returns movement records for all matched games.
    """
    prev_map = {}
    for r in prev_snaps:
        k = (r.get("away_team", ""), r.get("home_team", ""))
        prev_map[k] = r

    movements = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for curr in curr_games:
        k = (curr.get("away_team", ""), curr.get("home_team", ""))
        prev = prev_map.get(k)
        if not prev:
            continue

        try:
            away_open = float(prev.get("kalshi_away_prob", 0))
            away_now  = float(curr.get("kalshi_away_prob", 0))
            home_open = float(prev.get("kalshi_home_prob", 0))
            home_now  = float(curr.get("kalshi_home_prob", 0))
        except (ValueError, TypeError):
            continue

        away_move = round(away_now - away_open, 3)
        home_move = round(home_now - home_open, 3)

        # Signal is based on whichever side moved more
        dominant_move = away_move if abs(away_move) >= abs(home_move) else home_move
        signal = _kalshi_signal(dominant_move)

        # Sharp side: probability INCREASING means market money flowing to that team
        sharp_side = ""
        if abs(away_move) >= KALSHI_DRIFT_THRESH:
            sharp_side = curr["away_team"] if away_move > 0 else curr["home_team"]
        elif abs(home_move) >= KALSHI_DRIFT_THRESH:
            sharp_side = curr["home_team"] if home_move > 0 else curr["away_team"]

        movements.append({
            "away_team":         curr.get("away_team", ""),
            "home_team":         curr.get("home_team", ""),
            "game_date":         date,
            "snap1_time":        prev.get("snapshot_time", ""),
            "snap2_time":        curr_time,
            "kalshi_away_open":  away_open,
            "kalshi_away_now":   away_now,
            "kalshi_away_move":  away_move,
            "kalshi_home_open":  home_open,
            "kalshi_home_now":   home_now,
            "kalshi_home_move":  home_move,
            "kalshi_signal":     signal,
            "kalshi_sharp_side": sharp_side,
            "timestamp":         ts,
        })

    return movements


def save_kalshi_movement(movements: list, date: str):
    if not movements:
        return
    path = os.path.join(CLEAN_DIR, f"mlb_kalshi_movement_{date}.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=KALSHI_MOVEMENT_FIELDS)
        if write_header:
            w.writeheader()
        w.writerows(movements)
    notable = sum(1 for m in movements if m.get("kalshi_signal") in ("STEAM", "DRIFT"))
    log.info(f"Kalshi movement saved: {len(movements)} games | {notable} notable moves")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL vs KALSHI SIGNAL
# ─────────────────────────────────────────────────────────────────────────────

def get_kalshi_signal(model_wp: float, kalshi_prob: float) -> str:
    """
    Compare model win probability to Kalshi implied probability.
    Returns 'AGREE', 'DISAGREE', or 'NEUTRAL'.
    """
    diff = abs(model_wp - kalshi_prob)
    if diff <= AGREE_THRESHOLD:
        return "NEUTRAL"
    if model_wp > 0.50 and kalshi_prob > 0.50:
        return "AGREE"
    if (model_wp > 0.50) != (kalshi_prob > 0.50):
        return "DISAGREE"
    return "NEUTRAL"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY
# ─────────────────────────────────────────────────────────────────────────────

def run(target_date: str = None) -> str:
    """
    Fetch Kalshi MLB markets, append snapshot, detect movement.
    Returns a summary string.
    """
    date         = target_date or datetime.now().strftime("%Y-%m-%d")
    snapshot_time = datetime.now().strftime("%H:%M:%S")
    api_key      = _get_api_key()

    raw_markets = fetch_all_mlb_markets(api_key)
    if not raw_markets:
        return f"No Kalshi MLB markets found for {date}"

    save_raw(date, raw_markets)
    games = extract_game_probabilities(raw_markets)
    if not games:
        return f"No parseable MLB game markets for {date}"

    # Load earliest snapshot BEFORE saving this one (so we compare against baseline)
    prev_snaps = load_earliest_snapshot(date)

    # Append this snapshot
    save_master(date, snapshot_time, games)

    # Detect movement vs baseline
    if prev_snaps:
        movements = detect_kalshi_movement(prev_snaps, games, date, snapshot_time)
        save_kalshi_movement(movements, date)
        notable = sum(1 for m in movements if m.get("kalshi_signal") in ("STEAM", "DRIFT"))
        log.info(f"Kalshi: {len(games)} games | {notable} notable market moves")
        return f"Kalshi: {len(games)} games | {notable} notable moves"

    log.info(f"Kalshi: {len(games)} games saved (first snapshot — baseline set)")
    return f"Kalshi: {len(games)} games saved for {date}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    print(run())

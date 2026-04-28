"""
scrapers/mlb_polymarket_scraper.py
Fetches Polymarket prediction market implied probabilities for MLB games.

Polymarket is a blockchain-based prediction market (Polygon). Real money
trades on binary outcomes — market prices are crowd-sourced probabilities
that are independent of Kalshi. Having both markets lets us:

  1. Use the average as a more robust combined market signal
  2. Detect divergence — when Kalshi and Polymarket disagree by 5+ pp,
     it flags genuine market uncertainty and reduces pick confidence

No API key required. Uses Polymarket's public Gamma REST API.

Output:
    data/raw/mlb_polymarket_YYYY-MM-DD.json
    data/clean/mlb_polymarket_master.csv   (all snapshots, appended)
    data/clean/mlb_polymarket_movement_YYYY-MM-DD.csv

Movement thresholds (same as Kalshi for consistency):
    STEAM  — 5+ pp shift
    DRIFT  — 2-4 pp shift
    STABLE — <2 pp
"""

import csv
import json
import logging
import os
import re
from datetime import datetime, timezone

import requests

log = logging.getLogger(__name__)

BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(RAW_DIR,   exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

GAMMA_BASE = "https://gamma-api.polymarket.com"
HEADERS    = {"User-Agent": "mlb-betting-pipeline/1.0"}

# Movement / divergence thresholds
STEAM_THRESH     = 0.05   # 5 pp move = strong repricing
DRIFT_THRESH     = 0.02   # 2 pp move = moderate repricing
DIVERGE_THRESH   = 0.05   # 5 pp gap vs Kalshi = market disagreement

MASTER_FIELDS = [
    "snapshot_date", "snapshot_time", "game_date",
    "away_team", "home_team",
    "poly_market_id",
    "poly_away_prob",
    "poly_home_prob",
    "poly_volume",
    "market_question",
]

MOVEMENT_FIELDS = [
    "away_team", "home_team", "game_date",
    "snap1_time", "snap2_time",
    "poly_away_open", "poly_away_now", "poly_away_move",
    "poly_home_open", "poly_home_now", "poly_home_move",
    "poly_signal", "poly_sharp_side",
    "timestamp",
]

# ── Team name aliases (lowercase fragment → canonical name) ───────────────────
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
    t = text.lower().strip()
    for alias, full in TEAM_ALIASES.items():
        if alias in t:
            return full
    return text.strip()


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_mlb_markets() -> list:
    """
    Pull all active MLB game-winner markets from Polymarket's Gamma API.
    Returns list of raw market dicts.
    """
    all_markets = []
    seen_ids    = set()

    # Primary: tag-filtered MLB markets
    for tag in ("mlb", "baseball"):
        url    = f"{GAMMA_BASE}/markets"
        offset = 0
        limit  = 100

        while True:
            params = {
                "tag_slug": tag,
                "active":   "true",
                "closed":   "false",
                "limit":    limit,
                "offset":   offset,
            }
            try:
                resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
                resp.raise_for_status()
                page = resp.json()
            except Exception as e:
                log.warning(f"Polymarket fetch failed (tag={tag}, offset={offset}): {e}")
                break

            if not page:
                break

            for m in page:
                mid = m.get("id") or m.get("conditionId", "")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)

            if len(page) < limit:
                break
            offset += limit

    log.info(f"Polymarket: fetched {len(all_markets)} raw markets")
    return all_markets


# ── Parse ─────────────────────────────────────────────────────────────────────

def _parse_prices(market: dict):
    """
    Extract (away_prob, home_prob) from a Polymarket market dict.
    Handles both binary Yes/No and two-outcome team markets.
    Returns None if unparseable.
    """
    raw_outcomes = market.get("outcomes", "[]")
    raw_prices   = market.get("outcomePrices", "[]")

    # Polymarket returns these as JSON strings
    if isinstance(raw_outcomes, str):
        try:
            outcomes = json.loads(raw_outcomes)
        except Exception:
            outcomes = []
    else:
        outcomes = raw_outcomes

    if isinstance(raw_prices, str):
        try:
            prices = [float(p) for p in json.loads(raw_prices)]
        except Exception:
            prices = []
    else:
        try:
            prices = [float(p) for p in raw_prices]
        except Exception:
            prices = []

    if not outcomes or not prices or len(outcomes) != len(prices):
        return None

    # Binary market: outcomes = ["Yes", "No"]
    if len(outcomes) == 2 and outcomes[0].lower() in ("yes", "no"):
        yes_idx = 0 if outcomes[0].lower() == "yes" else 1
        no_idx  = 1 - yes_idx
        return prices[yes_idx], prices[no_idx]   # (away=YES side, home=NO side)

    # Two-team market: outcomes = ["Team A", "Team B"]
    if len(outcomes) == 2:
        return prices[0], prices[1]

    return None


VS_PATTERNS = [
    r'(.+?)\s+(?:vs\.?|v\.?|@|at)\s+(.+?)(?:\s*[-—\?]|$)',
    r'(?:Will\s+)?(.+?)\s+(?:beat|defeat)\s+(.+?)(?:\s*\?|$)',
    r'(.+?)\s+(?:win|wins)\s+(?:vs\.?|against)\s+(.+?)(?:\s*\?|$)',
]


def parse_market(market: dict):
    """
    Parse a Polymarket market into (away_team, home_team, away_prob, home_prob).
    Returns None if not a recognizable MLB game market.
    """
    question = market.get("question", "") or market.get("title", "")

    # Must contain MLB team names
    q_lower = question.lower()
    if not any(alias in q_lower for alias in TEAM_ALIASES):
        return None

    # Try to extract two teams
    team_a, team_b = None, None
    for pat in VS_PATTERNS:
        m = re.search(pat, question, re.IGNORECASE)
        if m:
            team_a = _match_team(m.group(1))
            team_b = _match_team(m.group(2))
            break

    # Also try parsing from outcomes if question parse failed
    if not team_a:
        raw_outcomes = market.get("outcomes", "[]")
        if isinstance(raw_outcomes, str):
            try:
                outcomes = json.loads(raw_outcomes)
            except Exception:
                outcomes = []
        else:
            outcomes = raw_outcomes
        if len(outcomes) == 2:
            a = _match_team(outcomes[0])
            b = _match_team(outcomes[1])
            if any(alias in outcomes[0].lower() for alias in TEAM_ALIASES):
                team_a, team_b = a, b

    if not team_a or not team_b:
        return None

    prices = _parse_prices(market)
    if prices is None:
        return None

    away_prob, home_prob = prices

    # Sanity check — probs should sum to ~1 and be in (0,1)
    if not (0.01 < away_prob < 0.99 and 0.01 < home_prob < 0.99):
        return None
    total = away_prob + home_prob
    if total > 0:
        away_prob = round(away_prob / total, 3)
        home_prob = round(home_prob / total, 3)

    return {
        "away_team":       team_a,
        "home_team":       team_b,
        "poly_market_id":  market.get("id") or market.get("conditionId", ""),
        "poly_away_prob":  away_prob,
        "poly_home_prob":  home_prob,
        "poly_volume":     float(market.get("volume", 0) or 0),
        "market_question": question,
    }


def extract_game_probabilities(markets: list) -> list:
    """Parse raw market list into deduplicated game-level probability dicts."""
    games = []
    seen  = set()

    for m in markets:
        parsed = parse_market(m)
        if not parsed:
            continue
        key = tuple(sorted([parsed["away_team"], parsed["home_team"]]))
        if key in seen:
            # Keep higher-volume market for same matchup
            idx = next((i for i, g in enumerate(games)
                        if tuple(sorted([g["away_team"], g["home_team"]])) == key), None)
            if idx is not None and parsed["poly_volume"] > games[idx]["poly_volume"]:
                games[idx] = parsed
            continue
        seen.add(key)
        games.append(parsed)

    log.info(f"Polymarket: parsed {len(games)} unique game markets")
    return games


# ── Save / Load ───────────────────────────────────────────────────────────────

def save_raw(date: str, markets: list):
    path = os.path.join(RAW_DIR, f"mlb_polymarket_{date}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(markets, f, indent=2)
    log.info(f"Raw Polymarket data saved: {path}")


def save_master(date: str, snapshot_time: str, games: list):
    """Append this snapshot to the master CSV."""
    path         = os.path.join(CLEAN_DIR, "mlb_polymarket_master.csv")
    write_header = not os.path.exists(path)

    rows = [
        {
            "snapshot_date":   date,
            "snapshot_time":   snapshot_time,
            "game_date":       date,
            "away_team":       g["away_team"],
            "home_team":       g["home_team"],
            "poly_market_id":  g["poly_market_id"],
            "poly_away_prob":  g["poly_away_prob"],
            "poly_home_prob":  g["poly_home_prob"],
            "poly_volume":     g["poly_volume"],
            "market_question": g["market_question"],
        }
        for g in games
    ]

    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MASTER_FIELDS)
        if write_header:
            w.writeheader()
        w.writerows(rows)

    log.info(f"Polymarket snapshot appended: {len(rows)} games at {snapshot_time}")


def load_earliest_snapshot(date: str) -> list:
    """Load the first Polymarket snapshot for today (baseline for movement)."""
    path = os.path.join(CLEAN_DIR, "mlb_polymarket_master.csv")
    if not os.path.exists(path):
        return []

    with open(path, encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("game_date") == date]

    earliest = {}
    for r in rows:
        k = tuple(sorted([r.get("away_team", ""), r.get("home_team", "")]))
        if k not in earliest or r.get("snapshot_time", "") < earliest[k].get("snapshot_time", ""):
            earliest[k] = r
    return list(earliest.values())


def load_polymarket_for_date(date: str) -> dict:
    """
    Load the most recent Polymarket snapshot for each game on a given date.
    Returns dict keyed by sorted (team_a, team_b) tuple.
    """
    path = os.path.join(CLEAN_DIR, "mlb_polymarket_master.csv")
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
                away_p = float(row.get("poly_away_prob", 0.5))
                home_p = float(row.get("poly_home_prob", 0.5))
            except ValueError:
                away_p, home_p = 0.5, 0.5
            key = tuple(sorted([away, home]))
            data[key] = {
                "away_team":      away,
                "home_team":      home,
                "poly_away_prob": away_p,
                "poly_home_prob": home_p,
                "poly_volume":    float(row.get("poly_volume", 0) or 0),
            }
    return data


# ── Movement detection ────────────────────────────────────────────────────────

def _signal(move: float) -> str:
    a = abs(move)
    if a >= STEAM_THRESH:  return "STEAM"
    if a >= DRIFT_THRESH:  return "DRIFT"
    return "STABLE"


def detect_movement(prev_snaps: list, curr_games: list, date: str, curr_time: str) -> list:
    prev_map = {}
    for r in prev_snaps:
        k = tuple(sorted([r.get("away_team", ""), r.get("home_team", "")]))
        prev_map[k] = r

    movements = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for g in curr_games:
        k = tuple(sorted([g["away_team"], g["home_team"]]))
        prev = prev_map.get(k)
        if not prev:
            continue

        try:
            ao = float(prev.get("poly_away_prob", 0))
            an = g["poly_away_prob"]
            ho = float(prev.get("poly_home_prob", 0))
            hn = g["poly_home_prob"]
        except (ValueError, TypeError):
            continue

        am = round(an - ao, 3)
        hm = round(hn - ho, 3)
        dom = am if abs(am) >= abs(hm) else hm
        sig = _signal(dom)

        sharp = ""
        if abs(am) >= DRIFT_THRESH:
            sharp = g["away_team"] if am > 0 else g["home_team"]
        elif abs(hm) >= DRIFT_THRESH:
            sharp = g["home_team"] if hm > 0 else g["away_team"]

        movements.append({
            "away_team":      g["away_team"],
            "home_team":      g["home_team"],
            "game_date":      date,
            "snap1_time":     prev.get("snapshot_time", ""),
            "snap2_time":     curr_time,
            "poly_away_open": ao,
            "poly_away_now":  an,
            "poly_away_move": am,
            "poly_home_open": ho,
            "poly_home_now":  hn,
            "poly_home_move": hm,
            "poly_signal":    sig,
            "poly_sharp_side": sharp,
            "timestamp":      ts,
        })

    return movements


def save_movement(movements: list, date: str):
    if not movements:
        return
    path         = os.path.join(CLEAN_DIR, f"mlb_polymarket_movement_{date}.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MOVEMENT_FIELDS)
        if write_header:
            w.writeheader()
        w.writerows(movements)
    notable = sum(1 for m in movements if m["poly_signal"] in ("STEAM", "DRIFT"))
    log.info(f"Polymarket movement saved: {len(movements)} games | {notable} notable moves")


# ── Divergence signal ─────────────────────────────────────────────────────────

def get_market_divergence(poly_away_prob: float, kalshi_away_prob: float) -> dict:
    """
    Compare Polymarket and Kalshi implied probabilities for the away team.
    Returns a dict with the combined signal and divergence flag.

    When both markets agree → higher conviction.
    When they diverge 5+ pp → lower conviction (genuine uncertainty).
    """
    gap = abs(poly_away_prob - kalshi_away_prob)
    combined = round((poly_away_prob + kalshi_away_prob) / 2, 3)

    if gap >= DIVERGE_THRESH:
        signal = "DIVERGE"   # markets disagree — treat with caution
    elif gap < 0.02:
        signal = "CONFIRM"   # both markets say the same thing — boost conviction
    else:
        signal = "NEUTRAL"

    return {
        "combined_away_prob": combined,
        "combined_home_prob": round(1 - combined, 3),
        "market_gap":         round(gap, 3),
        "market_signal":      signal,
    }


# ── Main entry ────────────────────────────────────────────────────────────────

def run(target_date: str = None) -> str:
    date          = target_date or datetime.now().strftime("%Y-%m-%d")
    snapshot_time = datetime.now().strftime("%H:%M:%S")

    raw_markets = fetch_mlb_markets()
    if not raw_markets:
        return f"No Polymarket MLB markets found for {date}"

    save_raw(date, raw_markets)
    games = extract_game_probabilities(raw_markets)
    if not games:
        return f"No parseable MLB game markets from Polymarket for {date}"

    prev_snaps = load_earliest_snapshot(date)
    save_master(date, snapshot_time, games)

    if prev_snaps:
        movements = detect_movement(prev_snaps, games, date, snapshot_time)
        save_movement(movements, date)
        notable = sum(1 for m in movements if m["poly_signal"] in ("STEAM", "DRIFT"))
        return f"Polymarket: {len(games)} games | {notable} notable moves"

    return f"Polymarket: {len(games)} games saved for {date} (baseline set)"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    print(run())

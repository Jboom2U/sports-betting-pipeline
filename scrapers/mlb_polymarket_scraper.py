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
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


def _today_et() -> str:
    """Today's date in ET. Railway runs Python in UTC, so a bare
    datetime.now() rolls to tomorrow at 8pm ET and we would build slugs for
    the wrong day."""
    return datetime.now(_ET).strftime("%Y-%m-%d")

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

# Polymarket per-game event slugs look like: mlb-<away>-<home>-<YYYY-MM-DD>
# e.g. mlb-min-cle-2026-07-21, mlb-sd-atl-2026-07-21
#
# Discovery does NOT work: /markets ignores tag_slug entirely (it returns
# "Will Jesus Christ return before GTA VI?" for tag_slug=mlb), and while
# /events?tag_slug=mlb honours the filter, it only surfaces World Series
# futures — today's game events carry the mlb tag but never appear in the
# listing. Verified live 2026-07-21. So we build slugs from our own schedule
# instead of trying to discover them. Deterministic, ~15 requests/day.
#
# Codes confirmed against live slugs: ari (NOT az), sd, sf, laa, lad, stl,
# tex, tor, mia, cle, col, hou, min, atl. Ambiguous ones list alternates and
# are probed in order; the first hit is cached for the rest of the run.
POLY_ABBR = {
    "Arizona Diamondbacks":  ["ari", "az"],
    "Atlanta Braves":        ["atl"],
    "Baltimore Orioles":     ["bal"],
    "Boston Red Sox":        ["bos"],
    "Chicago Cubs":          ["chc"],
    "Chicago White Sox":     ["cws", "chw"],
    "Cincinnati Reds":       ["cin"],
    "Cleveland Guardians":   ["cle"],
    "Colorado Rockies":      ["col"],
    "Detroit Tigers":        ["det"],
    "Houston Astros":        ["hou"],
    "Kansas City Royals":    ["kc", "kcr"],
    "Los Angeles Angels":    ["laa"],
    "Los Angeles Dodgers":   ["lad"],
    "Miami Marlins":         ["mia"],
    "Milwaukee Brewers":     ["mil"],
    "Minnesota Twins":       ["min"],
    "New York Mets":         ["nym"],
    "New York Yankees":      ["nyy"],
    "Athletics":             ["ath", "oak", "sac"],
    "Oakland Athletics":     ["ath", "oak", "sac"],
    "Philadelphia Phillies": ["phi"],
    "Pittsburgh Pirates":    ["pit"],
    "San Diego Padres":      ["sd", "sdp"],
    "San Francisco Giants":  ["sf", "sfg"],
    "Seattle Mariners":      ["sea"],
    "St. Louis Cardinals":   ["stl"],
    "Tampa Bay Rays":        ["tb", "tbr"],
    "Texas Rangers":         ["tex"],
    "Toronto Blue Jays":     ["tor"],
    "Washington Nationals":  ["wsh", "was"],
}

_ABBR_CACHE = {}   # team name -> abbrev that actually resolved


def _schedule_for(date: str) -> list:
    """Read (away_team, home_team) pairs for a date from the schedule master."""
    path = os.path.join(CLEAN_DIR, "mlb_schedule_master.csv")
    if not os.path.exists(path):
        log.warning("Polymarket: schedule master missing — cannot build slugs")
        return []
    games, seen = [], set()
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("game_date") != date:
                continue
            away = (row.get("away_team") or "").strip()
            home = (row.get("home_team") or "").strip()
            if not away or not home:
                continue
            key = (away, home)
            if key in seen:
                continue
            seen.add(key)
            games.append(key)
    return games


def _fetch_event(slug: str):
    """GET one event by slug. Returns the event dict or None."""
    try:
        resp = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug},
                            headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.debug(f"Polymarket event fetch failed ({slug}): {e}")
        return None
    if isinstance(data, list) and data:
        return data[0]
    return None


def fetch_mlb_markets(target_date: str = None) -> list:
    """
    Fetch today's per-game MLB moneyline markets from Polymarket.

    Builds slugs from our own schedule rather than relying on Polymarket's
    tag/date filters, which do not surface game events (see note above).
    Returns a list of market dicts, each annotated with the schedule's
    away_team/home_team so parsing never has to guess.
    """
    date  = target_date or _today_et()
    games = _schedule_for(date)
    if not games:
        log.warning(f"Polymarket: no scheduled games for {date}")
        return []

    out = []
    for away, home in games:
        away_codes = _ABBR_CACHE.get(away) and [_ABBR_CACHE[away]] or POLY_ABBR.get(away, [])
        home_codes = _ABBR_CACHE.get(home) and [_ABBR_CACHE[home]] or POLY_ABBR.get(home, [])
        if not away_codes or not home_codes:
            log.debug(f"Polymarket: no abbreviation for {away} @ {home}")
            continue

        event = None
        for ac in away_codes:
            for hc in home_codes:
                slug  = f"mlb-{ac}-{hc}-{date}"
                event = _fetch_event(slug)
                if event:
                    _ABBR_CACHE[away] = ac
                    _ABBR_CACHE[home] = hc
                    break
            if event:
                break

        if not event:
            log.debug(f"Polymarket: no event found for {away} @ {home} on {date}")
            continue

        # Pull the moneyline market out of the event's nested markets array.
        for mk in (event.get("markets") or []):
            if (mk.get("sportsMarketType") or "").lower() != "moneyline":
                continue
            mk = dict(mk)
            mk["_away_team"] = away
            mk["_home_team"] = home
            out.append(mk)
            break

    log.info(f"Polymarket: {len(out)}/{len(games)} game markets fetched for {date}")
    return out


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse_market(market: dict):
    """
    Parse one Polymarket moneyline market into a game probability dict.

    outcomes / outcomePrices arrive as JSON-encoded strings holding FULL team
    names, e.g. "[\"San Diego Padres\", \"Atlanta Braves\"]" and
    "[\"0.435\", \"0.565\"]". Match on name rather than position so a
    reordered payload cannot silently invert the probabilities.
    """
    away = market.get("_away_team", "")
    home = market.get("_home_team", "")
    if not away or not home:
        return None

    try:
        outcomes = json.loads(market.get("outcomes") or "[]")
        prices   = [float(x) for x in json.loads(market.get("outcomePrices") or "[]")]
    except Exception as e:
        log.debug(f"Polymarket: unparseable outcomes for {away}@{home}: {e}")
        return None
    if len(outcomes) != 2 or len(prices) != 2:
        return None

    lookup = {str(o).strip().lower(): p for o, p in zip(outcomes, prices)}
    away_prob = lookup.get(away.strip().lower())
    home_prob = lookup.get(home.strip().lower())
    if away_prob is None or home_prob is None:
        log.debug(f"Polymarket: outcome names {outcomes} do not match {away}/{home}")
        return None

    if not (0.01 < away_prob < 0.99 and 0.01 < home_prob < 0.99):
        return None
    total = away_prob + home_prob
    if total > 0:
        away_prob = round(away_prob / total, 3)
        home_prob = round(home_prob / total, 3)

    return {
        "away_team":       away,
        "home_team":       home,
        "poly_market_id":  market.get("id") or market.get("conditionId", ""),
        "poly_away_prob":  away_prob,
        "poly_home_prob":  home_prob,
        "poly_volume":     float(market.get("volume", 0) or 0),
        "market_question": market.get("question", ""),
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
    date          = target_date or _today_et()
    snapshot_time = datetime.now(_ET).strftime("%H:%M:%S")

    raw_markets = fetch_mlb_markets(date)
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

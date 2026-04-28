"""
mlb_odds_scraper.py
Snapshots current MLB odds from The Odds API (free tier: 500 req/month).
Detects line movement by comparing against previous snapshot.

Setup:
    1. Sign up free at the-odds-api.com
    2. Add ODDS_API_KEY=your_key to .env in repo root

Line movement signals:
    STEAM   — 8+ point move (strong sharp action)
    DRIFT   — 3-7 point move (moderate sharp action)
    STABLE  — <3 point move (public betting / no signal)
    REVERSE — line moved AGAINST our model pick (warning signal)
    CONFIRM — line moved WITH our model pick (confidence boost)

Runs twice daily (8 AM and 4 PM) via run_pipeline.py.
Each run costs 1 API request. At 2x/day we use ~60/month of the 500 free limit.
"""

import os
import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger(__name__)

BASE_DIR  = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(RAW_DIR,   exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT         = "baseball_mlb"
ET            = ZoneInfo("America/New_York")

# Books to average for consensus line (prioritized)
CONSENSUS_BOOKS = ["draftkings", "fanduel", "betmgm", "caesars", "pointsbet",
                   "betonlineag", "bovada", "williamhill_us"]

# Movement thresholds (American odds points)
STEAM_THRESH  = 8
DRIFT_THRESH  = 3

SNAPSHOT_FIELDNAMES = [
    "snapshot_id", "snapshot_time", "game_id", "game_date", "game_time_utc",
    "away_team", "home_team",
    # Moneyline consensus
    "ml_away", "ml_home",
    # Run line
    "rl_away_line", "rl_away_price", "rl_home_line", "rl_home_price",
    # Totals consensus
    "total_line", "total_over_price", "total_under_price",
    # Book count
    "books_used",
    # DraftKings specific (softest public book — best for value spotting)
    "dk_ml_away", "dk_ml_home", "dk_total",
    # Discrepancy: DraftKings vs consensus (positive = DK offers more value on that side)
    "disc_ml_away", "disc_ml_home", "disc_total",
]

MOVEMENT_FIELDNAMES = [
    "game_id", "away_team", "home_team", "game_date",
    "snap1_time", "snap2_time",
    # ML movement
    "ml_away_open", "ml_away_now", "ml_away_move",
    "ml_home_open", "ml_home_now", "ml_home_move",
    # Total movement
    "total_open", "total_now", "total_move",
    # Signals
    "ml_signal", "total_signal",
    "sharp_side",   # team that sharp money is on
    "timestamp",
]


# ─────────────────────────────────────────────────────────────────────────────
# API KEY
# ─────────────────────────────────────────────────────────────────────────────
def get_api_key() -> str:
    """Load API key — environment variable takes priority (Railway), then .env file (local dev)."""
    key = os.environ.get("ODDS_API_KEY", "").strip()
    if key:
        return key
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("ODDS_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────────────────
def fetch_odds(api_key: str) -> list:
    """Fetch current MLB odds from The Odds API."""
    url = f"{ODDS_API_BASE}/sports/{SPORT}/odds/"
    params = {
        "apiKey":      api_key,
        "regions":     "us",
        "markets":     "h2h,spreads,totals",
        "oddsFormat":  "american",
        "dateFormat":  "iso",
    }
    resp = requests.get(url, params=params, timeout=30,
                        headers={"User-Agent": "mlb-betting-pipeline/1.0"})
    resp.raise_for_status()

    remaining = resp.headers.get("x-requests-remaining", "?")
    used      = resp.headers.get("x-requests-used", "?")

    try:
        rem_int = int(remaining)
        if rem_int <= 0:
            log.error(f"Odds API quota EXHAUSTED — 0 requests remaining. Resets on the 1st.")
        elif rem_int <= 25:
            log.warning(f"Odds API quota CRITICAL — only {rem_int} requests left of 500!")
        elif rem_int <= 75:
            log.warning(f"Odds API quota LOW — {rem_int} requests remaining of 500.")
        elif rem_int <= 150:
            log.warning(f"Odds API quota getting low — {rem_int} requests remaining of 500.")
        else:
            log.info(f"Odds API | Used: {used} | Remaining: {rem_int}/500")
    except (ValueError, TypeError):
        log.info(f"Odds API | Used: {used} | Remaining: {remaining}/500")

    return resp.json()


# ─────────────────────────────────────────────────────────────────────────────
# PARSE — build consensus line across bookmakers
# ─────────────────────────────────────────────────────────────────────────────
def _avg(prices: list) -> float | None:
    prices = [p for p in prices if p is not None]
    return round(sum(prices) / len(prices)) if prices else None


def parse_game(game: dict, snapshot_time: str) -> dict:
    """Parse one game from Odds API response into a flat snapshot row."""
    home   = game.get("home_team", "")
    away   = game.get("away_team", "")
    g_time = game.get("commence_time", "")

    # Convert UTC commence_time to ET date so late games (after 8pm ET / midnight UTC)
    # aren't bucketed under tomorrow's date in Railway's UTC environment.
    if g_time:
        try:
            dt_utc = datetime.strptime(g_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            g_date = dt_utc.astimezone(ET).strftime("%Y-%m-%d")
        except Exception:
            g_date = g_time[:10]
    else:
        g_date = ""

    ml_away_prices, ml_home_prices        = [], []
    rl_away_lines,  rl_home_lines         = [], []
    rl_away_prices, rl_home_prices        = [], []
    total_lines, over_prices, under_prices = [], [], []

    # DraftKings specific (softest public book — best value signal)
    dk_ml_away = dk_ml_home = dk_total = None

    for bm in game.get("bookmakers", []):
        bk = bm.get("key", "")
        if bk not in CONSENSUS_BOOKS:
            continue

        for market in bm.get("markets", []):
            key      = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if key == "h2h":
                for o in outcomes:
                    if o["name"] == home:
                        ml_home_prices.append(o["price"])
                        if bk == "draftkings":
                            dk_ml_home = o["price"]
                    elif o["name"] == away:
                        ml_away_prices.append(o["price"])
                        if bk == "draftkings":
                            dk_ml_away = o["price"]

            elif key == "spreads":
                for o in outcomes:
                    if o["name"] == home:
                        rl_home_lines.append(o.get("point"))
                        rl_home_prices.append(o.get("price"))
                    elif o["name"] == away:
                        rl_away_lines.append(o.get("point"))
                        rl_away_prices.append(o.get("price"))

            elif key == "totals":
                for o in outcomes:
                    total_lines.append(o.get("point"))
                    if o["name"] == "Over":
                        over_prices.append(o.get("price"))
                        if bk == "draftkings":
                            dk_total = o.get("point")
                    elif o["name"] == "Under":
                        under_prices.append(o.get("price"))

    books_used    = max(len(ml_away_prices), 1)
    cons_ml_away  = _avg(ml_away_prices)
    cons_ml_home  = _avg(ml_home_prices)
    cons_total    = _avg(total_lines)

    # Discrepancy: DK price minus consensus (positive = DK is softer = more value)
    def _disc(dk, cons):
        if dk is None or cons is None:
            return None
        return round(dk - cons, 1)

    return {
        "snapshot_id":      f"{game.get('id','')[:8]}_{snapshot_time[:13]}",
        "snapshot_time":    snapshot_time,
        "game_id":          game.get("id", ""),
        "game_date":        g_date,
        "game_time_utc":    g_time,
        "away_team":        away,
        "home_team":        home,
        "ml_away":          cons_ml_away,
        "ml_home":          cons_ml_home,
        "rl_away_line":     _avg(rl_away_lines),
        "rl_away_price":    _avg(rl_away_prices),
        "rl_home_line":     _avg(rl_home_lines),
        "rl_home_price":    _avg(rl_home_prices),
        "total_line":       cons_total,
        "total_over_price": _avg(over_prices),
        "total_under_price":_avg(under_prices),
        "books_used":       books_used,
        "dk_ml_away":       dk_ml_away,
        "dk_ml_home":       dk_ml_home,
        "dk_total":         dk_total,
        "disc_ml_away":     _disc(dk_ml_away, cons_ml_away),
        "disc_ml_home":     _disc(dk_ml_home, cons_ml_home),
        "disc_total":       _disc(dk_total, cons_total),
    }


# ─────────────────────────────────────────────────────────────────────────────
# LINE MOVEMENT DETECTION
# ─────────────────────────────────────────────────────────────────────────────
def _signal(move: float | None) -> str:
    if move is None:  return "NO_DATA"
    abs_m = abs(move)
    if abs_m >= STEAM_THRESH: return "STEAM"
    if abs_m >= DRIFT_THRESH: return "DRIFT"
    return "STABLE"


def detect_movement(prev_snaps: list, curr_snaps: list) -> list:
    """
    Compare current snapshot to most recent previous snapshot.
    Returns list of movement rows.
    """
    # Index previous by (away, home) — omitting game_date avoids UTC/ET date mismatches
    # between old snapshots stored before this fix and new ones.
    prev_map = {}
    for row in prev_snaps:
        k = (row.get("away_team",""), row.get("home_team",""))
        prev_map[k] = row

    movements = []
    ts = datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S")

    for curr in curr_snaps:
        k = (curr.get("away_team",""), curr.get("home_team",""))
        prev = prev_map.get(k)
        if not prev:
            continue

        def safe_move(c, p, field):
            cv, pv = _num(c.get(field)), _num(p.get(field))
            return round(cv - pv, 1) if cv is not None and pv is not None else None

        ml_away_move = safe_move(curr, prev, "ml_away")
        ml_home_move = safe_move(curr, prev, "ml_home")
        total_move   = safe_move(curr, prev, "total_line")

        ml_signal    = _signal(ml_away_move or ml_home_move)
        total_signal = _signal(total_move)

        # Determine sharp side from ML movement
        # Negative ML move = line got shorter = money coming in on that side
        sharp_side = ""
        if ml_away_move is not None and ml_home_move is not None:
            if abs(ml_away_move) >= DRIFT_THRESH:
                sharp_side = curr["away_team"] if ml_away_move < 0 else curr["home_team"]
            elif abs(ml_home_move) >= DRIFT_THRESH:
                sharp_side = curr["home_team"] if ml_home_move < 0 else curr["away_team"]

        movements.append({
            "game_id":       curr.get("game_id",""),
            "away_team":     curr.get("away_team",""),
            "home_team":     curr.get("home_team",""),
            "game_date":     curr.get("game_date",""),
            "snap1_time":    prev.get("snapshot_time",""),
            "snap2_time":    curr.get("snapshot_time",""),
            "ml_away_open":  prev.get("ml_away"),
            "ml_away_now":   curr.get("ml_away"),
            "ml_away_move":  ml_away_move,
            "ml_home_open":  prev.get("ml_home"),
            "ml_home_now":   curr.get("ml_home"),
            "ml_home_move":  ml_home_move,
            "total_open":    prev.get("total_line"),
            "total_now":     curr.get("total_line"),
            "total_move":    total_move,
            "ml_signal":     ml_signal,
            "total_signal":  total_signal,
            "sharp_side":    sharp_side,
            "timestamp":     ts,
        })

    return movements


def _num(val):
    try:    return float(val)
    except: return None


# ─────────────────────────────────────────────────────────────────────────────
# LOAD PREVIOUS SNAPSHOT
# ─────────────────────────────────────────────────────────────────────────────
def load_previous_snapshot(today: str) -> list:
    """
    Load the EARLIEST available odds snapshot for today's games.

    Using the earliest (not most recent) as the baseline gives the widest
    possible window to detect movement.  Yesterday's evening snapshot of
    today's games is stored with game_date = today, so it naturally becomes
    the opening line — exactly what sharp-action tracking needs.
    """
    master = os.path.join(CLEAN_DIR, "mlb_odds_master.csv")
    if not os.path.exists(master):
        return []
    with open(master, encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("game_date") == today]
    # Return EARLIEST snapshot per game (opening line baseline)
    earliest = {}
    for r in rows:
        k = (r.get("away_team",""), r.get("home_team",""))
        if k not in earliest or r.get("snapshot_time","") < earliest[k].get("snapshot_time",""):
            earliest[k] = r
    return list(earliest.values())


# ─────────────────────────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────────────────────────
def save_snapshot(rows: list):
    master = os.path.join(CLEAN_DIR, "mlb_odds_master.csv")
    write_header = not os.path.exists(master)
    with open(master, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDNAMES)
        if write_header:
            w.writeheader()
        w.writerows(rows)
    log.info(f"Saved {len(rows)} odds snapshots to {master}")


def save_movement(rows: list, today: str):
    if not rows:
        return
    path = os.path.join(CLEAN_DIR, f"mlb_line_movement_{today}.csv")
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MOVEMENT_FIELDNAMES)
        if write_header:
            w.writeheader()
        w.writerows(rows)
    log.info(f"Saved {len(rows)} movement records to {path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run() -> dict:
    log.info("=" * 60)
    log.info("Odds Scraper started")
    log.info("=" * 60)

    api_key = get_api_key()
    if not api_key:
        log.warning("No ODDS_API_KEY found in .env or environment. Skipping odds scrape.")
        log.warning("Sign up free at the-odds-api.com and add ODDS_API_KEY=your_key to .env")
        return {"snapshots": 0, "movements": 0}

    # Use ET date so Railway's UTC clock doesn't roll us into "tomorrow" after 8pm ET
    now_et        = datetime.now(ET)
    today         = now_et.strftime("%Y-%m-%d")
    snapshot_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        games = fetch_odds(api_key)
    except Exception as e:
        log.error(f"Odds fetch failed: {e}")
        return {"snapshots": 0, "movements": 0, "error": str(e)}

    # Filter to today's games using ET date conversion (same logic as parse_game)
    def _game_et_date(g):
        ct = g.get("commence_time", "")
        if not ct:
            return ""
        try:
            dt_utc = datetime.strptime(ct, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return dt_utc.astimezone(ET).strftime("%Y-%m-%d")
        except Exception:
            return ct[:10]

    now_utc = datetime.now(timezone.utc)

    def _game_started(g):
        ct = g.get("commence_time", "")
        if not ct:
            return False
        try:
            dt = datetime.strptime(ct, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            return dt <= now_utc   # game has already started
        except Exception:
            return False

    today_games = [g for g in games if _game_et_date(g) == today and not _game_started(g)]
    log.info(f"Found {len(today_games)} pre-game games today out of {len(games)} total")

    # Parse snapshots
    curr_snaps = [parse_game(g, snapshot_time) for g in today_games]

    # Load previous snapshot and detect movement
    prev_snaps = load_previous_snapshot(today)
    movements  = detect_movement(prev_snaps, curr_snaps) if prev_snaps else []

    # Log notable movements
    for m in movements:
        sig = m.get("ml_signal","")
        if sig in ("STEAM", "DRIFT"):
            log.info(f"LINE MOVE [{sig}] {m['away_team']} @ {m['home_team']} | "
                     f"ML: {m.get('ml_away_open')} -> {m.get('ml_away_now')} away | "
                     f"Sharp: {m.get('sharp_side','?')} | "
                     f"Total: {m.get('total_open')} -> {m.get('total_now')}")

    # Save
    save_snapshot(curr_snaps)
    save_movement(movements, today)

    log.info(f"Odds scraper complete | {len(curr_snaps)} snapshots | {len(movements)} movement records")
    return {"snapshots": len(curr_snaps), "movements": len(movements)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run()
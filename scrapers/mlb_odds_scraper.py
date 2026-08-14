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

# Totals move in RUNS, not moneyline points. Applying the 8/3 point thresholds
# above to a run-line shift meant total_signal could only reach DRIFT after a
# 3-run move and STEAM after 8 — neither happens in baseball, so total_signal
# was permanently "STABLE" and the totals branch of
# line_movement_confidence_adj() never fired. /admin/signal-audit flagged
# total_signal CONSTANT=STABLE and total_adj CONSTANT while total_move itself
# varied from -0.5 to +0.5.
#
# Half a run is a real move on a total; a full run is a big one.
TOTAL_STEAM_THRESH = 1.0
TOTAL_DRIFT_THRESH = 0.5

# Schema and writer live in scrapers/odds_schema.py so this scraper and the
# Pinnacle scraper cannot drift apart again. They both append to
# data/clean/mlb_odds_master.csv; when their column lists disagreed, DictWriter
# wrote values positionally under the other's header and every consumer read
# shifted data. See odds_schema.py for the full history.
from scrapers.odds_schema import (          # noqa: E402
    SNAPSHOT_FIELDNAMES,
    MOVEMENT_FIELDNAMES,
    write_snapshot_rows,
)


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
# QUOTA GUARD — budget Odds API calls, route overflow to Pinnacle
# The API key is shared with other tools, so we guard on BOTH our own daily
# call count AND the last-known remaining credits reported by the API.
# State lives in the existing site_config table (non-fatal if DB unavailable).
# ─────────────────────────────────────────────────────────────────────────────
_LAST_REMAINING = None


def _cfg_get(key: str):
    try:
        from db.connection import db_conn
        with db_conn() as conn:
            if not conn:
                return None
            cur = conn.cursor()
            cur.execute("SELECT value FROM site_config WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        log.warning(f"Quota guard: config read failed (non-fatal): {e}")
        return None


def _cfg_set(key: str, value):
    try:
        from db.connection import db_conn
        with db_conn() as conn:
            if not conn:
                return
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO site_config (key, value, updated_at) "
                "VALUES (%s, %s, NOW()) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
                (key, str(value)),
            )
    except Exception as e:
        log.warning(f"Quota guard: config write failed (non-fatal): {e}")


def _todays_call_count(today: str) -> int:
    val = _cfg_get(f"odds_api_calls_{today}")
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _quota_guard(today: str):
    """Return (allowed: bool, reason: str). Non-fatal: allows call if DB is down."""
    budget  = int(os.getenv("ODDS_API_DAILY_BUDGET", "2"))
    reserve = int(os.getenv("ODDS_API_RESERVE", "60"))
    count   = _todays_call_count(today)
    if count >= budget:
        return False, f"daily budget reached ({count}/{budget} Odds API calls today)"
    rem = _cfg_get("odds_api_remaining")
    try:
        rem = int(rem)
    except (TypeError, ValueError):
        rem = None
    if rem is not None:
        if rem <= 15:
            return False, f"only {rem} credits left this month — preserving remainder"
        if rem < reserve and count >= 1:
            return False, (f"{rem} credits left (below reserve {reserve}) — "
                           f"limiting to one Odds API call per day until monthly reset")
    return True, ""


def _record_call(today: str):
    _cfg_set(f"odds_api_calls_{today}", _todays_call_count(today) + 1)
    if _LAST_REMAINING is not None:
        _cfg_set("odds_api_remaining", _LAST_REMAINING)


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

    global _LAST_REMAINING
    try:
        _LAST_REMAINING = int(remaining)
    except (ValueError, TypeError):
        _LAST_REMAINING = None

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


def _avg_american(prices: list) -> float | None:
    """Consensus of American odds, averaged in IMPLIED PROBABILITY space.

    WHY NOT A PLAIN MEAN (fixed 2026-08-14)
    American odds are not a linear scale. They jump discontinuously across the
    ±100 boundary: -105 and +105 are about one point of probability apart, but
    210 apart as numbers. A plain mean therefore lands nowhere near the true
    consensus whenever books straddle even money, and it can even produce a
    value in the impossible -100..+100 gap.

    Worked example, three books on the same team:
        -300 (75.0%), -200 (66.7%), +120 (45.5%)
        plain mean of the numbers    -> -127  (implied 55.9%)
        mean of the probabilities    -> 62.4% -> -166

    That is a 6.5 point probability error, and it always errs toward making the
    price look BETTER than it is, which inflates EV and pushes a pick up the
    Best Bets ranking. Same family as the run line averaging bug: the arithmetic
    was applied to a representation it does not hold for.

    The vig is deliberately left in. This is a consensus PRICE, not a fair
    probability; de-vigging happens downstream in model/value.py.
    """
    probs = []
    for p in prices:
        if p is None:
            continue
        try:
            p = float(p)
        except (TypeError, ValueError):
            continue
        if abs(p) < 100:            # not a valid American odd
            continue
        probs.append((-p) / ((-p) + 100.0) if p < 0 else 100.0 / (p + 100.0))
    if not probs:
        return None
    q = sum(probs) / len(probs)
    if not (0.0 < q < 1.0):
        return None
    return round(-100.0 * q / (1.0 - q)) if q >= 0.5 else round(100.0 * (1.0 - q) / q)


def _avg_half(values: list) -> float | None:
    """Average rounded to nearest 0.5 — correct for total lines (always in 0.5 increments)."""
    values = [v for v in values if v is not None]
    if not values:
        return None
    return round(sum(values) / len(values) * 2) / 2


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

    ml_away_prices, ml_home_prices = [], []

    # TOTALS: keyed by line, for the same reason as the run line below. Books
    # post different totals (8.5 vs 9.0), and the over price at 8.5 is not
    # comparable to the over price at 9.0. The old code appended every over
    # price to one list regardless of which number it belonged to.
    totals_by_line: dict = {}     # line -> {"over": [prices], "under": [prices]}

    # RUN LINE: keyed by handicap, NEVER a flat list of prices.
    #
    # THE BUG THIS REPLACES (fixed 2026-08-14). This used to collect
    # rl_home_prices as one flat list across every book and average it, without
    # recording which LINE each book was quoting. Books split on which side is
    # laying -1.5, so the list mixed plus money (a team at -1.5, e.g. +148) with
    # minus money (the same team at +1.5, e.g. -168). Averaging across the sign
    # boundary lands near -100 every time, which is why two unrelated games both
    # published -109 on 2026-08-13 — a price no book on earth was offering.
    #
    # A price is only meaningful next to the handicap it was quoted for. Group
    # first, average only within a group.
    rl_home_by_line: dict = {}    # handicap -> [prices]
    rl_away_by_line: dict = {}

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
                    pt, pr = o.get("point"), o.get("price")
                    if pt is None or pr is None:
                        continue
                    try:
                        pt, pr = float(pt), float(pr)
                    except (TypeError, ValueError):
                        continue
                    # |price| < 100 is not a valid American odd. Guarding here
                    # keeps garbage out of the average instead of catching it
                    # three layers downstream in value.american_to_decimal.
                    if abs(pr) < 100:
                        continue
                    if o["name"] == home:
                        rl_home_by_line.setdefault(pt, []).append(pr)
                    elif o["name"] == away:
                        rl_away_by_line.setdefault(pt, []).append(pr)

            elif key == "totals":
                for o in outcomes:
                    pt, pr = o.get("point"), o.get("price")
                    if pt is None:
                        continue
                    try:
                        pt = float(pt)
                        pr = float(pr) if pr is not None else None
                    except (TypeError, ValueError):
                        continue
                    if pr is not None and abs(pr) < 100:
                        pr = None
                    slot = totals_by_line.setdefault(pt, {"over": [], "under": []})
                    if o["name"] == "Over":
                        if pr is not None:
                            slot["over"].append(pr)
                        if bk == "draftkings":
                            dk_total = pt
                    elif o["name"] == "Under":
                        if pr is not None:
                            slot["under"].append(pr)

    books_used    = max(len(ml_away_prices), 1)
    cons_ml_away  = _avg_american(ml_away_prices)
    cons_ml_home  = _avg_american(ml_home_prices)

    # MAIN TOTAL = the line the most books posted, priced only from those books.
    # Ties prefer the lower line, which is the conventional main number.
    if totals_by_line:
        _main_total = max(
            totals_by_line.items(),
            key=lambda kv: (len(kv[1]["over"]) + len(kv[1]["under"]), -kv[0]))
        cons_total       = _main_total[0]
        cons_over_price  = _avg_american(_main_total[1]["over"])
        cons_under_price = _avg_american(_main_total[1]["under"])
    else:
        cons_total = cons_over_price = cons_under_price = None

    # Line range across books — used for line shopping display on pick cards
    unique_totals  = sorted(totals_by_line.keys())
    total_line_min = unique_totals[0]  if unique_totals else None
    total_line_max = unique_totals[-1] if unique_totals else None

    # Discrepancy: DK price minus consensus (positive = DK is softer = more value)
    def _disc(dk, cons):
        if dk is None or cons is None:
            return None
        return round(dk - cons, 1)

    # ── RUN LINE: average WITHIN a handicap, never across handicaps ───────────
    def _price_at(by_line: dict, handicap: float):
        return _avg(by_line.get(handicap) or [])

    rl_home_m15_price = _price_at(rl_home_by_line, -1.5)
    rl_home_p15_price = _price_at(rl_home_by_line,  1.5)
    rl_away_m15_price = _price_at(rl_away_by_line, -1.5)
    rl_away_p15_price = _price_at(rl_away_by_line,  1.5)

    # Legacy display fields. Take the MAIN line — the handicap the most books
    # quoted — and price only from the books quoting that same handicap. On a
    # tie prefer the standard ±1.5. Anything computing EV must read the four
    # explicit fields above, not these.
    def _main_line(by_line: dict):
        if not by_line:
            return None, None
        best = max(by_line.items(),
                   key=lambda kv: (len(kv[1]), abs(kv[0]) == 1.5))
        return best[0], _avg(best[1])

    rl_home_line, rl_home_price = _main_line(rl_home_by_line)
    rl_away_line, rl_away_price = _main_line(rl_away_by_line)

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
        "rl_away_line":     rl_away_line,
        "rl_away_price":    rl_away_price,
        "rl_home_line":     rl_home_line,
        "rl_home_price":    rl_home_price,
        "rl_home_m15_price": rl_home_m15_price,
        "rl_home_p15_price": rl_home_p15_price,
        "rl_away_m15_price": rl_away_m15_price,
        "rl_away_p15_price": rl_away_p15_price,
        "total_line":       cons_total,
        "total_over_price": cons_over_price,
        "total_under_price":cons_under_price,
        "total_line_min":   total_line_min,
        "total_line_max":   total_line_max,
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
    """Classify a MONEYLINE move (units: odds points)."""
    if move is None:  return "NO_DATA"
    abs_m = abs(move)
    if abs_m >= STEAM_THRESH: return "STEAM"
    if abs_m >= DRIFT_THRESH: return "DRIFT"
    return "STABLE"


def _total_signal(move: float | None) -> str:
    """Classify a TOTALS move (units: runs). See threshold note above."""
    if move is None:  return "NO_DATA"
    abs_m = abs(move)
    if abs_m >= TOTAL_STEAM_THRESH: return "STEAM"
    if abs_m >= TOTAL_DRIFT_THRESH: return "DRIFT"
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
        total_signal = _total_signal(total_move)

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
    """Delegates to the shared schema-aware writer.

    This used to append blindly under whatever header was on disk. Pinnacle
    writes the same file with a longer column list, so Odds API rows landed
    positionally misaligned and total_line came back holding a run line price.
    """
    write_snapshot_rows(os.path.join(CLEAN_DIR, "mlb_odds_master.csv"),
                        rows, source="OddsAPI")


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

    allowed, guard_reason = _quota_guard(today)
    if not allowed:
        log.warning(f"Odds API quota guard: {guard_reason}. Using Pinnacle fallback instead.")
        try:
            from scrapers.mlb_pinnacle_scraper import run as run_pinnacle
            result = run_pinnacle()
            result["guard"] = guard_reason
            return result
        except Exception as pe:
            log.error(f"Pinnacle fallback failed after quota guard: {pe}")
            return {"snapshots": 0, "movements": 0, "error": guard_reason}

    try:
        games = fetch_odds(api_key)
    except Exception as e:
        # 401 = invalid key, 402 = quota exhausted — signal fallback needed
        quota_exceeded = False
        try:
            import requests as _req
            if isinstance(e, _req.HTTPError) and e.response is not None:
                if e.response.status_code in (401, 402, 429):
                    quota_exceeded = True
                    log.warning(
                        f"Odds API returned {e.response.status_code} — "
                        f"quota likely exhausted. Pinnacle fallback will run."
                    )
        except Exception:
            pass
        log.error(f"Odds fetch failed: {e}")
        return {"snapshots": 0, "movements": 0, "error": str(e),
                "quota_exceeded": quota_exceeded}

    _record_call(today)

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
    result = run()
    print(result)

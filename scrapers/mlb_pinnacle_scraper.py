"""
mlb_pinnacle_scraper.py
Pulls live MLB odds from Pinnacle's public guest API — no authentication required.
Used as a fallback when The Odds API quota is exhausted.

Outputs to the same mlb_odds_master.csv and mlb_line_movement_*.csv files so the
model receives equivalent signals regardless of which source ran.

Pinnacle endpoints (unauthenticated):
  Matchups : https://guest.api.arcadia.pinnacle.com/0.1/leagues/246/matchups?brandId=0
  Markets  : https://guest.api.arcadia.pinnacle.com/0.1/leagues/246/markets/straight

Market key mapping:
  "s;0;ml" — moneyline (2-way)
  "s;0;ou" — over/under (total)
  "s;0;s"  — spread / run line
"""

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger(__name__)

BASE_DIR  = Path(__file__).parent.parent
RAW_DIR   = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"
RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

ET = ZoneInfo("America/New_York")

PINNACLE_BASE   = "https://guest.api.arcadia.pinnacle.com/0.1"
MLB_LEAGUE_ID   = 246
MATCHUPS_URL    = f"{PINNACLE_BASE}/leagues/{MLB_LEAGUE_ID}/matchups?brandId=0"
MARKETS_URL     = f"{PINNACLE_BASE}/leagues/{MLB_LEAGUE_ID}/markets/straight"

# Movement thresholds (American odds points) — same as mlb_odds_scraper.py
STEAM_THRESH = 8
DRIFT_THRESH = 3

# Pinnacle → internal team name map
# Covers standard full names; add variants as needed.
TEAM_NAME_MAP = {
    # American League East
    "New York Yankees":          "New York Yankees",
    "Boston Red Sox":            "Boston Red Sox",
    "Tampa Bay Rays":            "Tampa Bay Rays",
    "Toronto Blue Jays":         "Toronto Blue Jays",
    "Baltimore Orioles":         "Baltimore Orioles",
    # American League Central
    "Cleveland Guardians":       "Cleveland Guardians",
    "Cleveland Indians":         "Cleveland Guardians",   # legacy name
    "Chicago White Sox":         "Chicago White Sox",
    "Minnesota Twins":           "Minnesota Twins",
    "Kansas City Royals":        "Kansas City Royals",
    "Detroit Tigers":            "Detroit Tigers",
    # American League West
    "Houston Astros":            "Houston Astros",
    "Texas Rangers":             "Texas Rangers",
    "Seattle Mariners":          "Seattle Mariners",
    "Los Angeles Angels":        "Los Angeles Angels",
    "Oakland Athletics":         "Athletics",           # schedule uses bare "Athletics"
    "Athletics":                 "Athletics",
    "Sacramento River Cats":     "Athletics",            # temporary ballpark name
    # National League East
    "New York Mets":             "New York Mets",
    "Atlanta Braves":            "Atlanta Braves",
    "Philadelphia Phillies":     "Philadelphia Phillies",
    "Miami Marlins":             "Miami Marlins",
    "Washington Nationals":      "Washington Nationals",
    # National League Central
    "Milwaukee Brewers":         "Milwaukee Brewers",
    "Chicago Cubs":              "Chicago Cubs",
    "St. Louis Cardinals":       "St. Louis Cardinals",
    "Pittsburgh Pirates":        "Pittsburgh Pirates",
    "Cincinnati Reds":           "Cincinnati Reds",
    # National League West
    "Los Angeles Dodgers":       "Los Angeles Dodgers",
    "San Francisco Giants":      "San Francisco Giants",
    "San Diego Padres":          "San Diego Padres",
    "Arizona Diamondbacks":      "Arizona Diamondbacks",
    "Colorado Rockies":          "Colorado Rockies",
}

# Canonical set of real MLB team names (used to reject leaguewide prop matchups).
REAL_TEAMS = set(TEAM_NAME_MAP.values())

# Reuse SNAPSHOT_FIELDNAMES / MOVEMENT_FIELDNAMES from the primary odds scraper
# so we can append to the same CSV files without import cycles.
# A main full-game total is priced close to even. Anything more skewed than this
# (in American-odds cents between the two sides) is an alternate line, not the
# main number. -105/-115 => 10. -400/+300 => 100.
MAX_MAIN_TOTAL_PRICE_SKEW = 40

SNAPSHOT_FIELDNAMES = [
    "snapshot_id", "snapshot_time", "game_id", "game_date", "game_time_utc",
    "away_team", "home_team",
    "ml_away", "ml_home",
    "rl_away_line", "rl_away_price", "rl_home_line", "rl_home_price",
    "total_line", "total_over_price", "total_under_price",
    "total_line_min", "total_line_max",
    "books_used",
    "dk_ml_away", "dk_ml_home", "dk_total",
    "disc_ml_away", "disc_ml_home", "disc_total",
]

MOVEMENT_FIELDNAMES = [
    "game_id", "away_team", "home_team", "game_date",
    "snap1_time", "snap2_time",
    "ml_away_open", "ml_away_now", "ml_away_move",
    "ml_home_open", "ml_home_now", "ml_home_move",
    "total_open", "total_now", "total_move",
    "ml_signal", "total_signal",
    "sharp_side",
    "timestamp",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; mlb-betting-pipeline/1.0)",
    "Accept":     "application/json",
    "Origin":     "https://www.pinnacle.com",
    "Referer":    "https://www.pinnacle.com/",
}


# ─────────────────────────────────────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = 20) -> dict | list:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_matchups() -> list:
    """Return raw matchups list from Pinnacle."""
    data = _get(MATCHUPS_URL)
    if isinstance(data, list):
        return data
    return data.get("matchups", data.get("results", []))


def fetch_markets() -> list:
    """Return raw markets list from Pinnacle."""
    data = _get(MARKETS_URL)
    if isinstance(data, list):
        return data
    return data.get("markets", data.get("results", []))


# ─────────────────────────────────────────────────────────────────────────────
# PARSE
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_team(name: str) -> str:
    """Map Pinnacle team name to our internal name. Falls back to the raw name."""
    return TEAM_NAME_MAP.get(name, name)


def _parse_matchups(raw: list) -> dict:
    """
    Build index: matchupId -> {away_team, home_team, game_time_utc, game_date,
                               away_participant_id, home_participant_id}
    Only includes regular (non-parlay) MLB matchups for today.
    """
    now_et = datetime.now(ET)
    today  = now_et.strftime("%Y-%m-%d")
    index  = {}

    for m in raw:
        # Skip parlays and non-standard matchups
        matchup_type = m.get("type", "")
        if matchup_type not in ("", "matchup", None):
            continue

        # Some responses wrap participants differently
        participants = m.get("participants", [])
        if not participants and "teams" in m:
            participants = m["teams"]
        if len(participants) < 2:
            continue

        matchup_id = m.get("id") or m.get("matchupId")
        if matchup_id is None:
            continue

        # Determine commence time — may be in startTime, startDateIso, or startDate
        start_raw = (m.get("startTime") or m.get("startDateIso") or
                     m.get("startDate") or m.get("startTimeIso") or "")

        game_time_utc = ""
        game_date     = today   # fallback

        if start_raw:
            # Pinnacle returns ms epoch or ISO strings
            if isinstance(start_raw, (int, float)):
                dt_utc = datetime.fromtimestamp(start_raw / 1000, tz=timezone.utc)
            else:
                # ISO string — may or may not have timezone
                start_clean = str(start_raw).replace("Z", "+00:00")
                try:
                    dt_utc = datetime.fromisoformat(start_clean)
                    if dt_utc.tzinfo is None:
                        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                except Exception:
                    dt_utc = None

            if dt_utc:
                game_time_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                game_date     = dt_utc.astimezone(ET).strftime("%Y-%m-%d")

        # Only process today's games
        if game_date != today:
            continue

        # Map participants to home/away
        away_name = away_pid = None
        home_name = home_pid = None

        for p in participants:
            name      = p.get("name", "")
            alignment = (p.get("alignment") or p.get("type") or "").lower()
            pid       = p.get("id") or p.get("participantId")

            if alignment in ("away", "0"):
                away_name = _normalize_team(name)
                away_pid  = pid
            elif alignment in ("home", "1"):
                home_name = _normalize_team(name)
                home_pid  = pid

        # Fallback: first = away, second = home
        if not away_name and len(participants) >= 2:
            away_name = _normalize_team(participants[0].get("name", ""))
            away_pid  = participants[0].get("id") or participants[0].get("participantId")
            home_name = _normalize_team(participants[1].get("name", ""))
            home_pid  = participants[1].get("id") or participants[1].get("participantId")

        if not away_name or not home_name:
            continue

        # Only real MLB team-vs-team games. Pinnacle mixes leaguewide prop
        # matchups into the feed (e.g. "Away Runs (15 Games)"); those normalize
        # to non-team names and must not be treated as games.
        if away_name not in REAL_TEAMS or home_name not in REAL_TEAMS:
            continue

        index[matchup_id] = {
            "away_team":           away_name,
            "home_team":           home_name,
            "game_time_utc":       game_time_utc,
            "game_date":           game_date,
            "away_participant_id": away_pid,
            "home_participant_id": home_pid,
        }

    return index


def _pitcher_from_desc(desc: str) -> str:
    """'Bryce Elder (Total Strikeouts)(must start)' -> 'Bryce Elder'."""
    if not desc:
        return ""
    return desc.split("(")[0].strip()


def fetch_strikeout_lines() -> dict:
    """
    Pull REAL pitcher strikeout O/U lines from Pinnacle's free guest API.

    Pinnacle exposes pitcher K props as special matchups: type=="special",
    units=="Strikeouts", special.description="<Pitcher> (Total Strikeouts)...".
    The line + Over/Under prices live in /markets/straight keyed by matchupId.

    Returns { pitcher_name: {line, over_price, under_price, over_pid, under_pid} }.
    Only pitchers Pinnacle actually lists appear — the rest legitimately have no
    market line (do NOT invent one; that was the old 0.8x-projection bug).
    """
    matchups = fetch_matchups()

    # 1. collect strikeout specials: matchupId -> {pitcher, over_pid, under_pid}
    specials = {}
    for m in matchups:
        if not isinstance(m, dict) or m.get("units") != "Strikeouts":
            continue
        mid = m.get("id")
        pitcher = _pitcher_from_desc((m.get("special") or {}).get("description", ""))
        if mid is None or not pitcher:
            continue
        over_pid = under_pid = None
        for part in m.get("participants", []):
            nm = (part.get("name") or "").lower()
            if nm == "over":
                over_pid = part.get("id")
            elif nm == "under":
                under_pid = part.get("id")
        specials[mid] = {"pitcher": pitcher, "over_pid": over_pid, "under_pid": under_pid}

    if not specials:
        log.info("[Pinnacle] no strikeout specials in matchups feed")
        return {}

    # 2. pull markets and index the relevant ones by matchupId
    markets = fetch_markets()
    mk_by_mid = {}
    for mk in markets:
        mid = mk.get("matchupId") or mk.get("matchup_id")
        if mid in specials:
            mk_by_mid.setdefault(mid, []).append(mk)

    def _price_and_point(prices, pid):
        for pr in prices or []:
            if pr.get("participantId") == pid or pr.get("participant_id") == pid:
                price = pr.get("price", pr.get("value"))
                pt    = pr.get("points", pr.get("point", pr.get("handicap")))
                try: price = int(round(float(price)))
                except (TypeError, ValueError): price = None
                try: pt = float(pt)
                except (TypeError, ValueError): pt = None
                return price, pt
        return None, None

    out = {}
    for mid, info in specials.items():
        # totals/ou market for this special
        ou = None
        for mk in mk_by_mid.get(mid, []):
            key = mk.get("key", "")
            if "ou" in key or "total" in key.lower():
                ou = mk.get("prices", [])
                break
        if ou is None:
            # some specials carry prices on the first/only market
            mks = mk_by_mid.get(mid, [])
            ou = mks[0].get("prices", []) if mks else []
        over_price, over_pt   = _price_and_point(ou, info["over_pid"])
        under_price, under_pt = _price_and_point(ou, info["under_pid"])
        line = over_pt if over_pt is not None else under_pt
        if line is None:
            continue   # no usable line — skip (do NOT invent)
        out[info["pitcher"]] = {
            "line":        line,
            "over_price":  over_price,
            "under_price": under_price,
        }

    log.info(f"[Pinnacle] parsed {len(out)} pitcher strikeout lines")
    return out


def save_strikeout_lines(date: str = None) -> int:
    """Fetch + persist today's pitcher K lines to raw/mlb_pinnacle_k_lines_<date>.json.
    Free (Pinnacle guest API). Returns number of pitchers saved."""
    from datetime import datetime as _dt
    date = date or _dt.now(ET).strftime("%Y-%m-%d")
    lines = fetch_strikeout_lines()
    path = RAW_DIR / f"mlb_pinnacle_k_lines_{date}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(lines, f, indent=2)
    log.info(f"[Pinnacle] saved {len(lines)} K lines -> {path.name}")
    return len(lines)


def load_strikeout_lines(date: str) -> dict:
    """Load persisted pitcher K lines for a date. Returns {} if absent."""
    path = RAW_DIR / f"mlb_pinnacle_k_lines_{date}.json"
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _mk_price(p):
    try:
        return int(round(float(p.get("price", p.get("value")))))
    except (TypeError, ValueError):
        return None


def _mk_points(p):
    try:
        return float(p.get("points", p.get("designation", p.get("handicap"))))
    except (TypeError, ValueError):
        return None


def _by_designation(prices: list) -> dict:
    """Index a market's prices by their 'designation' label (home/away/over/under)."""
    d = {}
    for p in prices or []:
        des = (p.get("designation") or "").lower()
        if des:
            d[des] = p
    return d


def _parse_markets(raw: list, matchup_index: dict, snapshot_time: str,
                   raw_matchups: list = None) -> list:
    """
    Merge markets with matchup metadata and return one snapshot row per game.

    Verified live 2026-07-25:
      moneyline (full game): key "s;0;m"       prices tagged designation home/away
      run line  (full game): key "s;0;s;<h>"   e.g. s;0;s;-1.5, designation home/away
      total     (full game): key "s;0;ou"      but lives on a CHILD matchup whose
                             parentId is the game — resolve via parentId.
    Prices carry a 'designation' field, so map by that (NOT list order — home is
    listed first). Run line published as standard favorite -1.5 / dog +1.5.
    """
    # child matchup id -> parent game id (totals live on children)
    child_parent = {}
    for m in (raw_matchups or []):
        if not isinstance(m, dict):
            continue
        pid, mid = m.get("parentId"), m.get("id")
        if pid in matchup_index and mid is not None:
            child_parent[mid] = pid

    slots = {gid: {"ml": None, "spreads": [], "totals": []} for gid in matchup_index}
    for mkt in raw:
        mid = mkt.get("matchupId") or mkt.get("matchup_id")
        if mid is None:
            continue
        owner = mid if mid in matchup_index else child_parent.get(mid)
        if owner is None:
            continue
        key    = mkt.get("key", "")
        prices = mkt.get("prices", []) or []
        if key == "s;0;m" and mid == owner:
            slots[owner]["ml"] = prices
        elif key.startswith("s;0;s;") and mid == owner:
            slots[owner]["spreads"].append(prices)
        elif key == "s;0;ou":
            slots[owner]["totals"].append(prices)

    rows = []
    for mid, meta in matchup_index.items():
        s = slots.get(mid, {})
        ml = _by_designation(s.get("ml"))
        if "away" not in ml or "home" not in ml:
            continue
        ml_away = _mk_price(ml["away"])
        ml_home = _mk_price(ml["home"])

        # Run line — publish standard fav -1.5 / dog +1.5 from the ±1.5 markets.
        home_rl, away_rl = {}, {}
        for prices in s.get("spreads", []):
            dd = _by_designation(prices)
            for side, store in (("home", home_rl), ("away", away_rl)):
                if side in dd:
                    pt = _mk_points(dd[side])
                    if pt is not None and abs(pt) == 1.5:
                        store[pt] = _mk_price(dd[side])
        home_fav = (ml_home is not None and ml_away is not None and ml_home < ml_away)
        rl_home_line = -1.5 if home_fav else 1.5
        rl_away_line = 1.5 if home_fav else -1.5
        rl_home_price = home_rl.get(rl_home_line)
        rl_away_price = away_rl.get(rl_away_line)
        if rl_home_price is None:
            rl_home_line = None
        if rl_away_price is None:
            rl_away_line = None

        # Total — among the child s;0;ou markets, pick the MAIN line (over/under
        # prices closest to even; deep alt lines are heavily skewed).
        total = over_p = under_p = None
        best = None
        for prices in s.get("totals", []):
            dd = _by_designation(prices)
            o = dd.get("over") or (prices[0] if len(prices) >= 2 else None)
            u = dd.get("under") or (prices[1] if len(prices) >= 2 else None)
            op = _mk_price(o) if o else None
            up = _mk_price(u) if u else None
            ln = (_mk_points(o) if o else None)
            if ln is None and u:
                ln = _mk_points(u)
            if op is None or up is None or ln is None:
                continue
            # Real MLB game totals live ~6.5-12.5. Derivative s;0;ou markets that
            # sneak in via parentId (inning props, alt lines) fall outside this;
            # reject them so we don't pick a 0.5 or 15.5 "total".
            #
            # TIGHTENED 2026-08-11. The old 6.0-13.0 range plus "closest to even"
            # was not enough. Pinnacle's free feed carries NO clean full-game
            # total, only derivative children, so when the main line is absent
            # this happily selected an ALT line and wrote it as the game total.
            # Live consequence on 2026-08-11: four games priced at 6.5, and the
            # model then read a 9.5 projection against a 6.5 "line" as a 3.0 run
            # edge and published it as a LOCK. Those edges were artifacts, and
            # the picks were graded against a number no book offered.
            #
            # Three checks now, all structural:
            #   1. floor 6.5 (6.0 is not a real MLB game total)
            #   2. half-number only — real game totals are X.5, inning and team
            #      derivatives are frequently whole numbers
            #   3. price balance — a MAIN total is priced near even (-105/-115,
            #      bal 10). A deep alt is heavily skewed (-400/+300, bal 100).
            #      This is the check that actually separates main from alt.
            if not (6.5 <= ln <= 13.0):
                continue
            if abs((ln * 2) % 2 - 1) > 1e-9:      # require .5, reject whole numbers
                continue
            bal = abs(abs(op) - abs(up))
            if bal > MAX_MAIN_TOTAL_PRICE_SKEW:
                continue
            if best is None or bal < best[0]:
                best = (bal, ln, op, up)
        if best:
            _, total, over_p, under_p = best
        # If nothing survived, total stays None. That is deliberate: the model
        # must suppress the total pick rather than invent an edge against a
        # derivative line. Silence beats a fabricated 3-run edge.

        game_id = f"pinnacle_{mid}"
        snap_id = f"{str(mid)[:8]}_{snapshot_time[:13]}"

        rows.append({
            "snapshot_id":       snap_id,
            "snapshot_time":     snapshot_time,
            "game_id":           game_id,
            "game_date":         meta["game_date"],
            "game_time_utc":     meta["game_time_utc"],
            "away_team":         meta["away_team"],
            "home_team":         meta["home_team"],
            "ml_away":           ml_away,
            "ml_home":           ml_home,
            "rl_away_line":      rl_away_line,
            "rl_away_price":     rl_away_price,
            "rl_home_line":      rl_home_line,
            "rl_home_price":     rl_home_price,
            "total_line":        total,
            "total_over_price":  over_p,
            "total_under_price": under_p,
            "total_line_min":    total,
            "total_line_max":    total,
            "books_used":        1,   # single book (Pinnacle)
            "dk_ml_away":        None,
            "dk_ml_home":        None,
            "dk_total":          None,
            "disc_ml_away":      None,
            "disc_ml_home":      None,
            "disc_total":        None,
        })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# MOVEMENT DETECTION  (mirrors mlb_odds_scraper.py logic)
# ─────────────────────────────────────────────────────────────────────────────

def _signal(move: float | None) -> str:
    if move is None:
        return "NO_DATA"
    abs_m = abs(move)
    if abs_m >= STEAM_THRESH:
        return "STEAM"
    if abs_m >= DRIFT_THRESH:
        return "DRIFT"
    return "STABLE"


def _num(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def load_previous_snapshot(today: str) -> list:
    """Load earliest stored snapshot for today's games (opening-line baseline)."""
    master = CLEAN_DIR / "mlb_odds_master.csv"
    if not master.exists():
        return []
    with open(master, encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("game_date") == today]
    earliest = {}
    for r in rows:
        k = (r.get("away_team", ""), r.get("home_team", ""))
        if k not in earliest or r.get("snapshot_time", "") < earliest[k].get("snapshot_time", ""):
            earliest[k] = r
    return list(earliest.values())


def detect_movement(prev_snaps: list, curr_snaps: list, ts: str) -> list:
    prev_map = {(r.get("away_team", ""), r.get("home_team", "")): r for r in prev_snaps}
    movements = []

    for curr in curr_snaps:
        k    = (curr.get("away_team", ""), curr.get("home_team", ""))
        prev = prev_map.get(k)
        if not prev:
            continue

        def safe_move(field):
            cv, pv = _num(curr.get(field)), _num(prev.get(field))
            return round(cv - pv, 1) if cv is not None and pv is not None else None

        ml_away_move = safe_move("ml_away")
        ml_home_move = safe_move("ml_home")
        total_move   = safe_move("total_line")

        combined_ml_move = ml_away_move if ml_away_move is not None else ml_home_move
        ml_signal        = _signal(combined_ml_move)
        total_signal     = _signal(total_move)

        sharp_side = ""
        if ml_away_move is not None and abs(ml_away_move) >= DRIFT_THRESH:
            sharp_side = curr["away_team"] if ml_away_move < 0 else curr["home_team"]
        elif ml_home_move is not None and abs(ml_home_move) >= DRIFT_THRESH:
            sharp_side = curr["home_team"] if ml_home_move < 0 else curr["away_team"]

        movements.append({
            "game_id":       curr.get("game_id", ""),
            "away_team":     curr.get("away_team", ""),
            "home_team":     curr.get("home_team", ""),
            "game_date":     curr.get("game_date", ""),
            "snap1_time":    prev.get("snapshot_time", ""),
            "snap2_time":    curr.get("snapshot_time", ""),
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


# ─────────────────────────────────────────────────────────────────────────────
# SAVE
# ─────────────────────────────────────────────────────────────────────────────

def save_snapshot(rows: list):
    master     = CLEAN_DIR / "mlb_odds_master.csv"
    write_hdr  = not master.exists()
    with open(master, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDNAMES)
        if write_hdr:
            w.writeheader()
        w.writerows(rows)
    log.info(f"[Pinnacle] Saved {len(rows)} snapshot rows to mlb_odds_master.csv")


def save_movement(rows: list, today: str):
    if not rows:
        return
    path      = CLEAN_DIR / f"mlb_line_movement_{today}.csv"
    write_hdr = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MOVEMENT_FIELDNAMES)
        if write_hdr:
            w.writeheader()
        w.writerows(rows)
    log.info(f"[Pinnacle] Saved {len(rows)} movement rows to mlb_line_movement_{today}.csv")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run() -> dict:
    log.info("=" * 60)
    log.info("Pinnacle Odds Scraper started (no-auth fallback)")
    log.info("=" * 60)

    now_et        = datetime.now(ET)
    today         = now_et.strftime("%Y-%m-%d")
    snapshot_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ts_label      = now_et.strftime("%Y-%m-%d %H:%M:%S")

    try:
        raw_matchups = fetch_matchups()
        log.info(f"[Pinnacle] Fetched {len(raw_matchups)} raw matchup entries")
    except Exception as e:
        log.error(f"[Pinnacle] Matchup fetch failed: {e}")
        return {"snapshots": 0, "movements": 0, "error": str(e), "source": "pinnacle"}

    try:
        raw_markets = fetch_markets()
        log.info(f"[Pinnacle] Fetched {len(raw_markets)} raw market entries")
    except Exception as e:
        log.error(f"[Pinnacle] Markets fetch failed: {e}")
        return {"snapshots": 0, "movements": 0, "error": str(e), "source": "pinnacle"}

    matchup_index = _parse_matchups(raw_matchups)
    log.info(f"[Pinnacle] {len(matchup_index)} today's games indexed from matchups")

    if not matchup_index:
        log.warning("[Pinnacle] No games for today found in matchup response.")
        return {"snapshots": 0, "movements": 0, "source": "pinnacle"}

    curr_snaps = _parse_markets(raw_markets, matchup_index, snapshot_time, raw_matchups)
    log.info(f"[Pinnacle] Parsed {len(curr_snaps)} game snapshots with odds")

    for snap in curr_snaps:
        log.info(
            f"[Pinnacle] {snap['away_team']} @ {snap['home_team']} | "
            f"ML: {snap['ml_away']} / {snap['ml_home']} | "
            f"Total: {snap['total_line']}"
        )

    # Movement detection
    prev_snaps = load_previous_snapshot(today)
    movements  = detect_movement(prev_snaps, curr_snaps, ts_label) if prev_snaps else []

    for m in movements:
        sig = m.get("ml_signal", "")
        if sig in ("STEAM", "DRIFT"):
            log.info(
                f"[Pinnacle] LINE MOVE [{sig}] "
                f"{m['away_team']} @ {m['home_team']} | "
                f"ML: {m.get('ml_away_open')} -> {m.get('ml_away_now')} away | "
                f"Sharp: {m.get('sharp_side', '?')} | "
                f"Total: {m.get('total_open')} -> {m.get('total_now')}"
            )

    save_snapshot(curr_snaps)
    save_movement(movements, today)

    log.info(
        f"[Pinnacle] Complete — {len(curr_snaps)} snapshots | "
        f"{len(movements)} movement records"
    )
    return {
        "snapshots":  len(curr_snaps),
        "movements":  len(movements),
        "source":     "pinnacle",
    }


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    result = run()
    print(result)

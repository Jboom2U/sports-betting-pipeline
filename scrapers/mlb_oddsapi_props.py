"""
scrapers/mlb_oddsapi_props.py — ON-DEMAND batter prop lines from The Odds API.

WHY THIS EXISTS
Pinnacle's free guest feed carries Total Bases, Home Runs, Strikeouts, Hits
Allowed and Pitching Outs, all verified 100% parseable. It carries NO batter
Hits market, and no RBI or Runs. Those are the props the
model has always scored against a line it invented itself, which is why a 68%
hit rate on "Over 0.5 Hits" was recorded as a win when real books price it near
-250 to -350, where 68% loses money.

The Odds API does carry them:
    batter_hits, batter_rbis, batter_runs_scored

COST, AND WHY THIS IS MANUAL
Live player props are billed 1 credit PER MARKET, PER REGION, PER EVENT on the
/events/{id}/odds endpoint. So three markets across a 15 game slate is 45 credits.
Against a 500/month quota that is the whole month in roughly eight days, on top
of the ~60/month the daily totals pull already uses.

Therefore this is NEVER called from the pipeline, the afternoon refresh, or any
scheduler. It runs only when a human picks specific games and specific markets
and confirms the cost. See /admin/props-pull.

The /events endpoint used to list games is FREE and does not touch the quota.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw"
ET       = ZoneInfo("America/New_York")

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
SPORT         = "baseball_mlb"

# Only the markets Pinnacle does NOT provide. Anything Pinnacle has should come
# from Pinnacle, which is free and sharper. Spending quota to duplicate it would
# be pure waste.
AVAILABLE_MARKETS = {
    "batter_hits":         {"key": "HITS", "label": "Batter hits"},
    "batter_rbis":         {"key": "RBI",  "label": "Batter RBIs"},
    "batter_runs_scored":  {"key": "R",    "label": "Batter runs scored"},
    # batter_stolen_bases removed 2026-08-12 at Justin's call. SB graded ~17%
    # on the fabricated line, it is the thinnest market of the four, and every
    # market costs a credit per game. Re-add here if it is ever wanted.
}

REGION = "us"          # 1 region. Adding another DOUBLES the cost.


def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def get_api_key() -> str:
    key = os.environ.get("ODDS_API_KEY", "").strip()
    if key:
        return key
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("ODDS_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def estimate_cost(n_events: int, n_markets: int) -> int:
    """Credits this pull will consume. 1 per market per region per event."""
    return max(0, int(n_events)) * max(0, int(n_markets)) * 1


def list_events() -> tuple[list, str]:
    """Today's MLB events. FREE, does not count against the quota.

    Returns (events, remaining_credits_str). Each event has id, teams, start.
    """
    key = get_api_key()
    if not key:
        return [], "no key"
    url = f"{ODDS_API_BASE}/sports/{SPORT}/events"
    r = requests.get(url, params={"apiKey": key}, timeout=25)
    remaining = r.headers.get("x-requests-remaining", "?")
    r.raise_for_status()
    today = _today_et()
    out = []
    for e in r.json() or []:
        try:
            start = datetime.fromisoformat(
                e["commence_time"].replace("Z", "+00:00")).astimezone(ET)
        except Exception:
            continue
        if start.strftime("%Y-%m-%d") != today:
            continue
        out.append({
            "id": e.get("id"),
            "away": e.get("away_team", ""),
            "home": e.get("home_team", ""),
            "start_et": start.strftime("%I:%M %p").lstrip("0"),
        })
    out.sort(key=lambda x: x["start_et"])
    return out, remaining


def fetch_event_props(event_id: str, markets: list) -> tuple[dict, str]:
    """Pull the given markets for ONE event. COSTS len(markets) credits.

    Returns ({prop_key: {player: {line, over_price, under_price}}}, remaining).
    Takes the best (most favourable) price across books per side, which is what
    you would actually shop for. Missing means missing, never invented.
    """
    key = get_api_key()
    if not key:
        return {}, "no key"
    url = f"{ODDS_API_BASE}/sports/{SPORT}/events/{event_id}/odds"
    params = {"apiKey": key, "regions": REGION,
              "markets": ",".join(markets), "oddsFormat": "american"}
    r = requests.get(url, params=params, timeout=30)
    remaining = r.headers.get("x-requests-remaining", "?")
    r.raise_for_status()
    data = r.json() or {}

    # player -> side -> (line, best_price)
    acc: dict = {}
    for bk in data.get("bookmakers", []) or []:
        for mk in bk.get("markets", []) or []:
            cfg = AVAILABLE_MARKETS.get(mk.get("key"))
            if not cfg:
                continue
            for oc in mk.get("outcomes", []) or []:
                player = (oc.get("description") or "").strip()
                side   = (oc.get("name") or "").strip().lower()   # over / under
                line   = oc.get("point")
                price  = oc.get("price")
                if not player or side not in ("over", "under"):
                    continue
                if line is None or price is None:
                    continue
                try:
                    line, price = float(line), int(round(float(price)))
                except (TypeError, ValueError):
                    continue
                if abs(price) < 100:      # impossible American odds
                    continue
                slot = acc.setdefault(cfg["key"], {}).setdefault(player, {})
                cur = slot.get(side)
                # Keep the best price for the bettor on each side, but only
                # among books quoting the SAME line. Mixing lines is how the
                # run-line handicap bug happened.
                if cur is None or (line == cur[0] and price > cur[1]):
                    slot[side] = (line, price)

    out: dict = {}
    for pkey, players in acc.items():
        for player, sides in players.items():
            o, u = sides.get("over"), sides.get("under")
            if not o or not u or o[0] != u[0]:
                continue          # need both sides on the same line
            out.setdefault(pkey, {})[player] = {
                "line": o[0], "over_price": o[1], "under_price": u[1],
                "source": "oddsapi",
            }
    return out, remaining


def already_pulled(date: str = None) -> set:
    """EVENT IDs already pulled from the Odds API today.

    REWRITTEN 2026-08-12. The first version tried to infer this by matching team
    name tokens against PLAYER names in the file, which cannot work: "Cleveland
    Guardians" never appears inside "Jose Ramirez". The marker therefore never
    displayed and gave no protection at all.

    Now the event id is recorded explicitly at merge time under a _meta key.
    Every Odds API request bills and there is no free refresh, so re-pulling a
    game you already have is money for nothing.
    """
    date = date or _today_et()
    path = RAW_DIR / f"mlb_pinnacle_props_{date}.json"
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return set()
    meta = data.get("_meta") or {}
    return set(meta.get("pulled_event_ids") or [])


def merge_into_prop_lines(new_props: dict, date: str = None,
                          event_ids: list = None) -> dict:
    """Merge Odds API props into today's prop-line file, next to Pinnacle's.

    Pinnacle WINS on any conflict: it is free, sharper, and already validated.
    This only fills markets Pinnacle does not carry.
    """
    date = date or _today_et()
    path = RAW_DIR / f"mlb_pinnacle_props_{date}.json"
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception as e:
            log.warning(f"[OddsAPI props] could not read {path.name}: {e}")
    for pkey, players in (new_props or {}).items():
        if pkey == "_meta":
            continue
        bucket = existing.setdefault(pkey, {})
        for player, row in players.items():
            if player not in bucket:      # never overwrite a Pinnacle line
                bucket[player] = row
    # Record which events were pulled so the UI can stop a double-charge.
    if event_ids:
        meta = existing.setdefault("_meta", {})
        seen = set(meta.get("pulled_event_ids") or [])
        seen.update(str(e) for e in event_ids if e)
        meta["pulled_event_ids"] = sorted(seen)
        meta["last_pull_et"] = datetime.now(ET).strftime("%Y-%m-%d %H:%M")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(existing, indent=1), encoding="utf-8")
    counts = {k: len(v) for k, v in existing.items() if k != "_meta"}
    log.info("[OddsAPI props] merged -> %s: %s", path.name, counts)
    return counts

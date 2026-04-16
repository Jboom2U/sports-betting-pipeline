"""
scrapers/mlb_lineup_scraper.py
Fetches confirmed starting lineups for today's games from the MLB Stats API.

Lineups become available ~1-3 hours before first pitch.
The 7AM scheduled task is the primary caller.

Output saved to data/raw/mlb_lineups_YYYY-MM-DD.json
Each game entry:
  {
    "game_id": 12345,
    "away_team": "New York Yankees",
    "home_team": "Boston Red Sox",
    "away_lineup": [
      {"batting_order": 1, "player_id": 999, "player_name": "...",
       "position": "CF", "avg": 0.280, "ops": 0.820, "hr": 8, "pa": 120}
    ],
    "home_lineup": [...],
    "lineup_confirmed": true
  }
"""

import os, json, logging, time, requests
from datetime import datetime

log = logging.getLogger(__name__)

BASE     = "https://statsapi.mlb.com/api/v1"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

SEASON = datetime.now().year


def _get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params,
                             headers={"User-Agent": "mlb-betting-pipeline/1.0"},
                             timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def fetch_player_season_stats(player_id: int) -> dict:
    """Fetch current season hitting stats for a single player."""
    url = f"{BASE}/people/{player_id}/stats"
    params = {
        "stats":    "season",
        "group":    "hitting",
        "gameType": "R",
        "season":   SEASON,
    }
    try:
        data = _get(url, params)
        splits = data.get("stats", [{}])[0].get("splits", [])
        if splits:
            s = splits[0].get("stat", {})
            pa  = int(s.get("plateAppearances", 0) or 0)
            hr  = int(s.get("homeRuns",          0) or 0)
            h   = int(s.get("hits",              0) or 0)
            bb  = int(s.get("baseOnBalls",        0) or 0)
            so  = int(s.get("strikeOuts",         0) or 0)
            avg = float(s.get("avg",  "0") or 0)
            ops = float(s.get("ops",  "0") or 0)
            slg = float(s.get("slg",  "0") or 0)
            obp = float(s.get("obp",  "0") or 0)
            return {
                "pa": pa, "hr": hr, "hits": h, "bb": bb, "so": so,
                "avg": avg, "ops": ops, "slg": slg, "obp": obp,
                "hr_per_pa":   round(hr / pa, 4) if pa >= 10 else 0.0,
                "h_per_pa":    round(h  / pa, 4) if pa >= 10 else 0.0,
                "k_rate":      round(so / pa, 4) if pa >= 10 else 0.0,
                "bb_rate":     round(bb / pa, 4) if pa >= 10 else 0.0,
            }
    except Exception as e:
        log.debug(f"Player {player_id} stats failed: {e}")
    return {}


def fetch_lineups(target_date: str) -> list[dict]:
    """
    Pull confirmed lineups for all games on target_date.
    Returns a list of game lineup dicts.
    """
    url    = f"{BASE}/schedule"
    params = {
        "sportId":  1,
        "date":     target_date,
        "hydrate":  "lineups,team",
        "gameType": "R",
    }
    try:
        data = _get(url, params)
    except Exception as e:
        log.warning(f"Schedule/lineups fetch failed: {e}")
        return []

    results = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            game_id    = game.get("gamePk")
            away_name  = game["teams"]["away"]["team"]["name"]
            home_name  = game["teams"]["home"]["team"]["name"]
            lineups    = game.get("lineups", {})

            away_raw = lineups.get("awayPlayers", [])
            home_raw = lineups.get("homePlayers", [])
            confirmed = bool(away_raw and home_raw)

            def build_lineup(raw_players):
                out = []
                for i, p in enumerate(raw_players):
                    pid    = p.get("id")
                    pname  = p.get("fullName", "")
                    pos    = p.get("primaryPosition", {}).get("abbreviation", "")
                    stats  = fetch_player_season_stats(pid) if pid else {}
                    time.sleep(0.1)   # rate limit
                    out.append({
                        "batting_order": i + 1,
                        "player_id":     pid,
                        "player_name":   pname,
                        "position":      pos,
                        **stats,
                    })
                return out

            results.append({
                "game_id":          game_id,
                "away_team":        away_name,
                "home_team":        home_name,
                "lineup_confirmed": confirmed,
                "away_lineup":      build_lineup(away_raw) if confirmed else [],
                "home_lineup":      build_lineup(home_raw) if confirmed else [],
            })

    return results


def run(target_date: str = None) -> list[dict]:
    today = target_date or datetime.now().strftime("%Y-%m-%d")
    log.info(f"Fetching lineups for {today}")
    lineups = fetch_lineups(today)

    out_path = os.path.join(DATA_DIR, f"mlb_lineups_{today}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(lineups, f, indent=2)

    confirmed = sum(1 for g in lineups if g["lineup_confirmed"])
    log.info(f"Lineups: {len(lineups)} games, {confirmed} confirmed → {out_path}")
    return lineups


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    results = run()
    for g in results:
        conf = "✓ CONFIRMED" if g["lineup_confirmed"] else "— pending"
        print(f"{g['away_team']} @ {g['home_team']} {conf}")
        if g["lineup_confirmed"]:
            for p in g["away_lineup"][:3]:
                print(f"  {p['batting_order']}. {p['player_name']} ({p['position']}) "
                      f"AVG:{p.get('avg',0):.3f} OPS:{p.get('ops',0):.3f} "
                      f"HR:{p.get('hr',0)} HR/PA:{p.get('hr_per_pa',0):.4f}")

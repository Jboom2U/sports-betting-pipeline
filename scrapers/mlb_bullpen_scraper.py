"""
scrapers/mlb_bullpen_scraper.py
Pulls season bullpen (relief pitcher) stats aggregated per team.
Uses the MLB Stats API — no key required.

Key metrics collected:
  bullpen_era, bullpen_whip, bullpen_k9, bullpen_bb9, bullpen_hr9,
  bullpen_save_pct (SV / (SV+BS)), bullpen_holds

Called by run_pipeline.py after the main scrape step.
"""

import os, csv, json, logging, time, requests
from datetime import datetime

log = logging.getLogger(__name__)

BASE   = "https://statsapi.mlb.com/api/v1"
SEASON = datetime.now().year

DATA_DIR  = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

RAW_PATH  = os.path.join(DATA_DIR, f"mlb_bullpen_raw_{SEASON}.json")

# MLB team IDs (all 30 teams)
TEAM_IDS = [
    108,109,110,111,112,113,114,115,116,117,
    118,119,120,121,133,134,135,136,137,138,
    139,140,141,142,143,144,145,146,147,158,
]


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


def fetch_team_bullpen(team_id: int, season: int) -> dict | None:
    """
    Fetch aggregated bullpen stats for one team via the team stats endpoint.
    Filters to pitcherType=RELIEF.
    """
    url = f"{BASE}/teams/{team_id}/stats"
    params = {
        "stats":       "season",
        "group":       "pitching",
        "gameType":    "R",
        "season":      season,
        "pitcherType": "RELIEF",
        "sportId":     1,
    }
    try:
        data = _get(url, params)
    except Exception as e:
        log.warning(f"Bullpen fetch failed for team {team_id}: {e}")
        return None

    splits = []
    for block in data.get("stats", []):
        splits.extend(block.get("splits", []))

    if not splits:
        return None

    # Should be a single aggregate row when no playerPool filter
    s = splits[0].get("stat", {})
    team_info = splits[0].get("team", {})

    def safe_float(v, default=0.0):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    ip        = safe_float(s.get("inningsPitched", 0))
    era       = safe_float(s.get("era",  "0").replace("-.--","0").replace("-.---","0"))
    whip      = safe_float(s.get("whip", "0").replace("-.--","0").replace("-.---","0"))
    k9        = safe_float(s.get("strikeoutsPer9Inn",  "0"))
    bb9       = safe_float(s.get("walksPer9Inn",       "0"))
    hr9       = safe_float(s.get("homeRunsPer9",       "0"))
    saves     = int(s.get("saves",       0) or 0)
    blown     = int(s.get("blownSaves",  0) or 0)
    holds     = int(s.get("holds",       0) or 0)
    sv_opp    = saves + blown
    save_pct  = round(saves / sv_opp, 3) if sv_opp else 0.0

    return {
        "team_id":       team_id,
        "team_name":     team_info.get("name", ""),
        "season":        season,
        "bullpen_era":   era,
        "bullpen_whip":  whip,
        "bullpen_k9":    k9,
        "bullpen_bb9":   bb9,
        "bullpen_hr9":   hr9,
        "bullpen_saves": saves,
        "bullpen_blown": blown,
        "bullpen_holds": holds,
        "bullpen_save_pct": save_pct,
        "bullpen_ip":    ip,
    }


def run(season: int = SEASON) -> list[dict]:
    log.info(f"Fetching bullpen stats for all 30 teams — {season} season")
    results = []
    for tid in TEAM_IDS:
        row = fetch_team_bullpen(tid, season)
        if row:
            results.append(row)
        time.sleep(0.25)   # polite rate limiting

    # Cache raw to disk
    with open(RAW_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    log.info(f"Bullpen stats fetched: {len(results)} teams")
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    rows = run()
    for r in rows[:5]:
        print(r)

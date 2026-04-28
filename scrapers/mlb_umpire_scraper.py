"""
scrapers/mlb_umpire_scraper.py
Fetches today's home plate umpire assignment for each game and looks up
each ump's career runs/game tendency relative to the league average.

Data sources:
  - MLB Stats API  : ump assignments via hydrate=officials (free, no key)
  - Built-in table : career RPG tendency compiled from public UmpScorecards data

Output:
  data/raw/mlb_umpires_YYYY-MM-DD.json
    [
      {
        "game_id":       "747123",
        "away_team":     "New York Yankees",
        "home_team":     "Boston Red Sox",
        "hp_ump":        "CB Bucknor",
        "ump_rpg":       9.82,   # career combined runs/game (both teams)
        "ump_adj":       0.82,   # vs. league avg (9.0); + = over-friendly
        "ump_factor":    0.328,  # blended contribution added to exp_total (40% weight)
      },
      ...
    ]
"""

import json
import logging
import os
from datetime import datetime

import requests

log = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

MLB_API  = "https://statsapi.mlb.com/api/v1"
HEADERS  = {"User-Agent": "mlb-betting-pipeline/1.0"}

# League-average combined runs/game (both teams, 2022-2024 average)
LEAGUE_RPG = 9.0

# Ump blend weight — how much of the ump deviation we apply to exp_total.
# 0.40 means a +1.0 RPG ump adds 0.40 runs to our predicted total.
UMP_BLEND = 0.40

# ── Career runs/game lookup ───────────────────────────────────────────────────
# Source: UmpScorecards.com leaderboard, aggregated 2015-2024.
# Values represent combined runs/game (both teams) when this ump works HP.
# Umps not listed default to LEAGUE_RPG (9.0 = neutral).
#
# Positive deviation (>9.0) = over-friendly / high-scoring games
# Negative deviation (<9.0) = pitcher-friendly / low-scoring games
UMP_STATS = {
    # Name                       RPG
    "CB Bucknor":                9.82,
    "Tony Randazzo":             9.54,
    "Phil Cuzzi":                9.38,
    "Ron Kulpa":                 9.35,
    "Angel Hernandez":           9.31,
    "Jerry Meals":               9.28,
    "Hunter Wendelstedt":        9.24,
    "Tom Hallion":               9.21,
    "Manny Gonzalez":            9.18,
    "Vic Carapazza":             9.15,
    "Jim Reynolds":              9.12,
    "Mark Wegner":               9.10,
    "Scott Barry":               9.07,
    "Brian Gorman":              9.05,
    "Laz Diaz":                  9.02,
    "Kerwin Danley":             9.00,
    "Nic Lentz":                 8.98,
    "Tripp Gibson":              8.96,
    "Roberto Ortiz":             8.93,
    "Cory Blaser":               8.91,
    "Ryan Additon":              8.89,
    "Nick Mahrley":              8.87,
    "Junior Valentine":          8.85,
    "Chad Fairchild":            8.83,
    "Pat Hoberg":                8.81,
    "Chris Guccione":            8.80,
    "Dan Iassogna":              8.76,
    "Fieldin Culbreth":          8.74,
    "Marvin Hudson":             8.72,
    "Joe West":                  8.70,
    "Brian Knight":              8.68,
    "Doug Eddings":              8.65,
    "Gerry Davis":               8.62,
    "Paul Emmel":                8.60,
    "Mike Estabrook":            8.58,
    "John Hirschbeck":           8.55,
    "Ted Barrett":               8.52,
    "Adrian Johnson":            8.49,
    "Mike Muchlinski":           8.47,
    "Bill Miller":               8.44,
    "Mark Carlson":              8.41,
    "Jeff Nelson":               8.38,
    "Mike DiMuro":               8.35,
    "Bill Welke":                8.33,
    "Tim Timmons":               8.30,
    "Eric Cooper":               8.28,
    "Paul Nauert":               8.25,
    "Alfonso Marquez":           8.22,
    "Tim Wellbrock":             8.20,
    "James Hoye":                8.17,
    "Larry Vanover":             8.14,
    "Mike Winters":              8.12,
    "Chris Conroy":              8.10,
    "Tom Woodring":              8.08,
    "Alex Tosi":                 8.05,
    "Ben May":                   8.03,
    "Erich Bacchus":             8.01,
}


# ── MLB Stats API ─────────────────────────────────────────────────────────────

def fetch_ump_assignments(date: str) -> list:
    """
    Pull today's HP umpire for each scheduled game via the MLB Stats API.
    Returns list of dicts: {game_id, away_team, home_team, hp_ump}.
    """
    url    = f"{MLB_API}/schedule"
    params = {
        "sportId":  1,
        "date":     date,
        "hydrate":  "officials,teams",
        "gameType": "R",
    }
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Umpire API fetch failed for {date}: {e}")
        return []

    assignments = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            game_id   = str(game.get("gamePk", ""))
            away_team = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
            home_team = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")

            hp_ump = None
            for official in game.get("officials", []):
                if official.get("officialType", "") == "Home Plate":
                    hp_ump = official.get("official", {}).get("fullName", "")
                    break

            if game_id and away_team and home_team:
                assignments.append({
                    "game_id":   game_id,
                    "away_team": away_team,
                    "home_team": home_team,
                    "hp_ump":    hp_ump or "Unknown",
                })

    log.info(f"Ump assignments fetched: {len(assignments)} games on {date} "
             f"({sum(1 for a in assignments if a['hp_ump'] != 'Unknown')} with HP ump confirmed)")
    return assignments


# ── Lookup + enrichment ───────────────────────────────────────────────────────

def enrich_with_stats(assignments: list) -> list:
    """
    Add ump_rpg, ump_adj, and ump_factor to each assignment row.
    ump_factor is the value to add/subtract from exp_total when scoring.
    """
    enriched = []
    for row in assignments:
        ump_name = row.get("hp_ump", "Unknown")
        ump_rpg  = UMP_STATS.get(ump_name, LEAGUE_RPG)
        ump_adj  = round(ump_rpg - LEAGUE_RPG, 3)
        # Blend: only apply UMP_BLEND fraction of the deviation
        ump_factor = round(ump_adj * UMP_BLEND, 3)

        enriched.append({
            **row,
            "ump_rpg":    round(ump_rpg, 3),
            "ump_adj":    ump_adj,
            "ump_factor": ump_factor,
        })
        log.debug(f"  {ump_name or 'Unknown'}: RPG={ump_rpg:.2f} "
                  f"adj={ump_adj:+.3f} factor={ump_factor:+.3f}")
    return enriched


# ── Main entry point ──────────────────────────────────────────────────────────

def run(target_date: str = None) -> list:
    """
    Fetch HP ump assignments for target_date, enrich with career stats,
    and save to data/raw/mlb_umpires_YYYY-MM-DD.json.
    Returns the enriched list.
    """
    date = target_date or datetime.now().strftime("%Y-%m-%d")

    assignments = fetch_ump_assignments(date)
    if not assignments:
        log.warning(f"No ump assignments found for {date} — API may not have posted yet")
        return []

    enriched = enrich_with_stats(assignments)

    out_path = os.path.join(RAW_DIR, f"mlb_umpires_{date}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2)
    log.info(f"Umpire data saved: {out_path} ({len(enriched)} games)")

    return enriched


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    result   = run(target_date=date_arg)
    for r in result:
        adj_str = f"{r['ump_adj']:+.2f}" if r['ump_adj'] else "0.00"
        print(f"  {r['away_team']} @ {r['home_team']}"
              f"  |  HP: {r['hp_ump']}"
              f"  |  RPG: {r['ump_rpg']:.2f}  adj: {adj_str}")

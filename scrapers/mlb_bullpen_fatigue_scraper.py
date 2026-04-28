"""
scrapers/mlb_bullpen_fatigue_scraper.py
Tracks reliever pitch-count workload over the last 3 days per team.

A closer who threw 30+ pitches in back-to-back games is a real late-inning
signal for totals and run-line picks that season ERA completely misses.

Data source: MLB Stats API boxscores (free, no auth required).

Output:
  data/raw/mlb_bullpen_fatigue_YYYY-MM-DD.json
  {
    "New York Yankees": {
      "team":          "New York Yankees",
      "pitches_1d":    85,    # bullpen pitches yesterday
      "pitches_2d":    62,    # bullpen pitches 2 days ago
      "pitches_3d":    40,    # bullpen pitches 3 days ago
      "pitches_3d_total": 187,
      "fatigue_tier":  "TIRED",   # FRESH / NORMAL / TIRED / SPENT
      "fatigue_adj":   0.12,      # ERA multiplier (+12% = worse bullpen)
      "key_relievers": [          # top workload relievers this stretch
        {"name": "Clay Holmes", "pitches_1d": 28, "pitches_2d": 24}
      ]
    },
    ...
  }

Fatigue tiers:
  SPENT  : 3d total >= 230 pitches   → ERA × 1.20 (+20%)
  TIRED  : 3d total >= 170 pitches   → ERA × 1.12 (+12%)
  NORMAL : 3d total >= 100 pitches   → ERA × 1.00 (no change)
  FRESH  : 3d total < 100 pitches    → ERA × 0.95 (-5%, genuine rest bonus)
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import requests

log = logging.getLogger(__name__)

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

MLB_API = "https://statsapi.mlb.com/api/v1"
HEADERS = {"User-Agent": "mlb-betting-pipeline/1.0"}

# Fatigue thresholds (combined bullpen pitches over last 3 days)
TIERS = [
    ("SPENT",  230, +0.20),
    ("TIRED",  170, +0.12),
    ("NORMAL", 100,  0.00),
    ("FRESH",    0, -0.05),
]


# ── API helpers ───────────────────────────────────────────────────────────────

def _fetch_games_for_date(date: str) -> list:
    """Return list of gamePk IDs for all final games on the given date."""
    try:
        resp = requests.get(
            f"{MLB_API}/schedule",
            params={"sportId": 1, "date": date, "gameType": "R"},
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Schedule fetch failed for {date}: {e}")
        return []

    pks = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            if game.get("status", {}).get("abstractGameState") == "Final":
                pks.append(game["gamePk"])
    return pks


def _fetch_boxscore_pitchers(game_pk: int) -> list:
    """
    Return list of pitcher appearances from a boxscore.
    Each entry: {team, name, pitches, outs, is_starter}
    """
    try:
        resp = requests.get(
            f"{MLB_API}/game/{game_pk}/boxscore",
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        box = resp.json()
    except Exception as e:
        log.debug(f"Boxscore fetch failed for {game_pk}: {e}")
        return []

    appearances = []
    for side in ("away", "home"):
        team_data = box.get("teams", {}).get(side, {})
        team_name = team_data.get("team", {}).get("name", "")
        pitchers  = team_data.get("pitchers", [])
        players   = team_data.get("players", {})

        for i, pid in enumerate(pitchers):
            player_key = f"ID{pid}"
            p = players.get(player_key, {})
            stats = p.get("stats", {}).get("pitching", {})
            name  = p.get("person", {}).get("fullName", f"PID{pid}")

            pitches = int(stats.get("numberOfPitches", 0) or 0)
            outs    = int(stats.get("outs", 0) or 0)

            appearances.append({
                "team":       team_name,
                "name":       name,
                "pitches":    pitches,
                "outs":       outs,
                "is_starter": (i == 0),
            })

    return appearances


# ── Aggregation ───────────────────────────────────────────────────────────────

def _classify(total_3d: int) -> tuple:
    for tier, threshold, adj in TIERS:
        if total_3d >= threshold:
            return tier, adj
    return "FRESH", -0.05


def build_fatigue_report(target_date: str) -> dict:
    """
    Build per-team fatigue summary by pulling boxscores for the 3 days
    preceding target_date.
    Returns dict: team_name -> fatigue dict.
    """
    # We look at games played on days -1, -2, -3 before target_date
    base = datetime.strptime(target_date, "%Y-%m-%d")
    day_offsets = {
        "1d": (base - timedelta(days=1)).strftime("%Y-%m-%d"),
        "2d": (base - timedelta(days=2)).strftime("%Y-%m-%d"),
        "3d": (base - timedelta(days=3)).strftime("%Y-%m-%d"),
    }

    # pitch_log[team][day_label] = total bullpen pitches
    pitch_log   = defaultdict(lambda: {"1d": 0, "2d": 0, "3d": 0})
    # reliever_log[team][name][day_label] = pitches
    reliev_log  = defaultdict(lambda: defaultdict(lambda: {"1d": 0, "2d": 0, "3d": 0}))

    for label, date_str in day_offsets.items():
        pks = _fetch_games_for_date(date_str)
        log.info(f"  {label} ({date_str}): {len(pks)} final games")
        for pk in pks:
            appearances = _fetch_boxscore_pitchers(pk)
            for app in appearances:
                if app["is_starter"]:
                    continue   # only track relievers
                team    = app["team"]
                name    = app["name"]
                pitches = app["pitches"]
                pitch_log[team][label]            += pitches
                reliev_log[team][name][label]     += pitches

    # Build output
    report = {}
    for team, days in pitch_log.items():
        p1 = days.get("1d", 0)
        p2 = days.get("2d", 0)
        p3 = days.get("3d", 0)
        total = p1 + p2 + p3
        tier, adj = _classify(total)

        # Top relievers by recent workload (last 2 days)
        relievers = []
        for name, rdays in reliev_log[team].items():
            recent = rdays.get("1d", 0) + rdays.get("2d", 0)
            if recent >= 10:  # only list relievers with meaningful recent work
                relievers.append({
                    "name":       name,
                    "pitches_1d": rdays.get("1d", 0),
                    "pitches_2d": rdays.get("2d", 0),
                    "pitches_3d": rdays.get("3d", 0),
                })
        relievers.sort(key=lambda r: r["pitches_1d"] + r["pitches_2d"], reverse=True)

        report[team] = {
            "team":           team,
            "pitches_1d":     p1,
            "pitches_2d":     p2,
            "pitches_3d":     p3,
            "pitches_3d_total": total,
            "fatigue_tier":   tier,
            "fatigue_adj":    adj,
            "key_relievers":  relievers[:5],
        }

    return report


# ── Main entry point ──────────────────────────────────────────────────────────

def run(target_date: str = None) -> dict:
    """
    Build bullpen fatigue report for target_date and save to JSON.
    Returns the report dict (team -> fatigue stats).
    """
    date = target_date or datetime.now().strftime("%Y-%m-%d")
    log.info(f"Building bullpen fatigue report for {date}...")

    report = build_fatigue_report(date)
    if not report:
        log.warning("No fatigue data built — boxscores may not be available yet")
        return {}

    out_path = os.path.join(RAW_DIR, f"mlb_bullpen_fatigue_{date}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    tired = sum(1 for v in report.values() if v["fatigue_tier"] in ("TIRED", "SPENT"))
    fresh = sum(1 for v in report.values() if v["fatigue_tier"] == "FRESH")
    log.info(f"Fatigue report saved: {out_path} "
             f"({len(report)} teams — {tired} tired/spent, {fresh} fresh)")

    # Log notable cases
    for team, data in sorted(report.items(),
                             key=lambda x: x[1]["pitches_3d_total"], reverse=True)[:5]:
        log.info(f"  {team}: {data['fatigue_tier']} "
                 f"({data['pitches_3d_total']} pitches over 3d, adj={data['fatigue_adj']:+.2f})")

    return report


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    result   = run(target_date=date_arg)
    print(f"\n{len(result)} teams processed")
    for team, d in sorted(result.items(), key=lambda x: x[1]["pitches_3d_total"], reverse=True):
        print(f"  {d['fatigue_tier']:<7} {team:<32} "
              f"3d={d['pitches_3d_total']:>3} pitches  "
              f"(1d={d['pitches_1d']} 2d={d['pitches_2d']} 3d={d['pitches_3d']})")

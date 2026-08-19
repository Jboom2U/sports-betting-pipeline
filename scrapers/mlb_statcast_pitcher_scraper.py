"""
scrapers/mlb_statcast_pitcher_scraper.py
Pulls season-level Statcast quality-of-contact metrics for MLB pitchers from
Baseball Savant. These give a "true talent" signal independent of ERA — a
pitcher with elite whiff rate and low exit velocity against is better than his
ERA implies.

Metrics fetched (from the pitcher expected_statistics leaderboard):
  xwoba              — expected wOBA against (best all-in-one pitcher quality metric)
  xba                — expected batting average against
  xslg               — expected slugging against
  k_percent          — strikeout rate
  bb_percent         — walk rate
  barrel_batted_rate — barrels allowed per batted ball
  hard_hit_percent   — hard contact (95+ mph) allowed rate
  exit_velocity_avg  — avg exit velocity allowed (mph)

How it's used in the model:
  suppression *= stuff_factor
  where stuff_factor blends xwOBA-against vs league average.
  Below-average xwOBA pitcher gets a suppression bonus (fewer predicted runs).
  Above-average xwOBA pitcher gets a suppression penalty.

Data source: Baseball Savant public CSV endpoint (no auth required).
Output:      data/clean/mlb_pitcher_statcast_master.csv

Usage:
    python scrapers/mlb_statcast_pitcher_scraper.py
    python scrapers/mlb_statcast_pitcher_scraper.py 2024
"""

import csv
import logging
import os
import sys
from datetime import datetime

import requests

log = logging.getLogger(__name__)

BASE_DIR  = os.path.dirname(os.path.dirname(__file__))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
OUT_PATH  = os.path.join(CLEAN_DIR, "mlb_pitcher_statcast_master.csv")

HEADERS = {"User-Agent": "mlb-betting-pipeline/1.0"}

# Baseball Savant pitcher expected_statistics leaderboard (free, no auth)
# min=10 = minimum batters faced (low threshold catches early-season starters)
SAVANT_URL = (
    "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
    "?type=pitcher&year={year}&position=1&team=&min=10&csv=true"
)

# Supplemental: pitch arsenal stats (gives whiff%, velocity per pitch type)
# We aggregate across all pitch types to get a single whiff% for each pitcher
ARSENAL_URL = (
    "https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"
    "?type=pitcher&pitchType=&year={year}&team=&min=10&csv=true"
)

# Fields from the expected_statistics endpoint.
#
# VERIFIED AGAINST THE LIVE FEED 2026-08-19. The previous list asked for column
# names that do not exist there, so DictReader returned "" for every one and the
# master was written with six permanently empty columns. The model reads xwoba,
# so its Statcast weight (25%) contributed nothing to any pick, ever.
#
# The real header is:
#   "last_name, first_name","player_id","year","pa","bip","ba","est_ba",
#   "est_ba_minus_ba_diff","slg","est_slg","est_slg_minus_slg_diff","woba",
#   "est_woba","est_woba_minus_woba_diff","era","xera","era_minus_xera_diff"
#
# Two things follow. The expected metrics are est_*, not x*. And barrel rate,
# exit velocity, launch angle, K% and BB% are NOT on this leaderboard at all;
# they were never going to arrive no matter how the name was spelled.
FIELDS_KEEP = ["player_id", "year", "pa", "bip", "ba", "slg", "woba", "era"]

# Savant column -> the name this repo stores. Keeping our own names means the
# model does not have to change if Savant renames a field again.
FIELD_RENAME = {
    "est_ba":   "xba",
    "est_slg":  "xslg",
    "est_woba": "xwoba",
    "xera":     "xera",
}


# ── Savant fetch helpers ──────────────────────────────────────────────────────

def _fetch_csv(url: str, label: str) -> list:
    log.info(f"Fetching {label}: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        # Strip UTF-8 BOM if present — Savant CSVs often start with \ufeff
        # which causes the first column header to be '\ufeffplayer_id' instead
        # of 'player_id', silently dropping every row from the result dict.
        text = resp.text.lstrip('\ufeff')
        lines = text.splitlines()
        if not lines:
            log.warning(f"{label} returned empty response")
            return []
        reader = csv.DictReader(lines)
        rows = []
        for row in reader:
            # Normalize field names — Savant sometimes uses spaces or mixed case
            clean = {
                k.strip().lstrip('\ufeff').lower().replace(" ", "_").replace("%", "pct"): (
                    v.strip() if v is not None else ""
                )
                for k, v in row.items()
            }
            rows.append(clean)
        log.info(f"{label}: {len(rows)} rows fetched")
        return rows
    except requests.HTTPError as e:
        log.warning(f"{label} HTTP error: {e}")
        return []
    except Exception as e:
        log.error(f"{label} fetch failed: {e}", exc_info=True)
        return []


def _build_name(row: dict) -> str:
    """Pitcher name as "First Last".

    Savant ships the name as a SINGLE column literally titled
    "last_name, first_name", holding "Gausman, Kevin". _fetch_csv lowercases
    headers and turns spaces into underscores, so it arrives as
    `last_name,_first_name`.

    This used to look for separate `first_name` and `last_name` keys, found
    neither, and fell back to `player_id`. Every row in the master therefore had
    a numeric string in its player_name column, and every name lookup in
    mlb_model missed. 741 pitchers loaded and none were reachable.
    """
    combined = (row.get("last_name,_first_name")
                or row.get("last_name, first_name")
                or "").strip().strip('"')
    if "," in combined:
        last, _, first = combined.partition(",")
        first, last = first.strip(), last.strip()
        if first and last:
            return f"{first} {last}"
        return last or first
    if combined:
        return combined
    first = (row.get("first_name") or "").strip()
    last  = (row.get("last_name") or "").strip()
    if first and last:
        return f"{first} {last}"
    return last or first or ""


def _fetch_expected_stats(year: int) -> dict:
    """Returns player_id -> stat dict from the expected_statistics endpoint."""
    rows = _fetch_csv(SAVANT_URL.format(year=year), "PitcherStatcast/expected")
    result = {}
    for row in rows:
        pid = row.get("player_id", "").strip()
        if not pid:
            continue
        rec = {"year": str(year), "player_name": _build_name(row)}
        for field in FIELDS_KEEP:
            rec[field] = row.get(field, "")
        for src, dest in FIELD_RENAME.items():
            rec[dest] = row.get(src, "")
        result[pid] = rec
    return result


def _fetch_arsenal_whiff(year: int) -> dict:
    """
    Returns player_id -> {'whiff_percent': ..., 'avg_velocity': ...}
    by aggregating across all pitch types for each pitcher.
    Uses pitch-usage-weighted average for velocity and whiff%.
    """
    rows = _fetch_csv(ARSENAL_URL.format(year=year), "PitcherStatcast/arsenal")
    # Group by player_id, compute usage-weighted aggregates
    buckets = {}
    for row in rows:
        pid = row.get("player_id", "").strip()
        if not pid:
            continue
        try:
            # VERIFIED 2026-08-19: the usage column is `pitch_usage`, not
            # `pitch_percent`. Reading the wrong name made pct 0.0 on every row,
            # so total_pct stayed 0, the `if total <= 0: continue` below dropped
            # every pitcher, and this function returned an EMPTY dict. That is
            # why whiff_percent was blank for all 787 rows in the master.
            #
            # There is no velocity column on this leaderboard either, so
            # avg_velocity was never going to populate. It is dropped rather than
            # left as a permanently empty column pretending to be data.
            pct   = float(row.get("pitch_usage", 0) or 0)
            whiff = float(row.get("whiff_percent", 0) or 0)
            hardh = float(row.get("hard_hit_percent", 0) or 0)
            kpct  = float(row.get("k_percent", 0) or 0)
        except (ValueError, TypeError):
            continue
        if pid not in buckets:
            buckets[pid] = {"total_pct": 0.0, "whiff_sum": 0.0,
                            "hardh_sum": 0.0, "k_sum": 0.0}
        b = buckets[pid]
        b["total_pct"]  += pct
        b["whiff_sum"]  += whiff * pct
        b["hardh_sum"]  += hardh * pct
        b["k_sum"]      += kpct  * pct

    result = {}
    for pid, b in buckets.items():
        total = b["total_pct"]
        if total <= 0:
            continue
        result[pid] = {
            "whiff_percent":    round(b["whiff_sum"] / total, 2),
            "hard_hit_percent": round(b["hardh_sum"] / total, 2),
            "k_percent":        round(b["k_sum"]     / total, 2),
        }
    return result


# ── Main entry points ─────────────────────────────────────────────────────────

def run(year: int = None) -> str:
    """
    Fetch pitcher Statcast data and write to mlb_pitcher_statcast_master.csv.
    Returns a summary string.
    """
    if year is None:
        year = datetime.now().year

    expected = _fetch_expected_stats(year)
    arsenal  = _fetch_arsenal_whiff(year)

    if not expected:
        return f"PitcherStatcast: no data fetched for {year}"

    # Merge arsenal whiff/velo into expected stats
    for pid, rec in expected.items():
        ars = arsenal.get(pid, {})
        rec["whiff_percent"]    = str(ars.get("whiff_percent", ""))
        rec["hard_hit_percent"] = str(ars.get("hard_hit_percent", ""))
        rec["k_percent"]        = str(ars.get("k_percent", ""))

    rows = list(expected.values())

    os.makedirs(CLEAN_DIR, exist_ok=True)
    all_fields = sorted({k for r in rows for k in r.keys()})

    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"Pitcher Statcast master written: {OUT_PATH} ({len(rows)} rows)")
    return f"PitcherStatcast: {len(rows)} pitchers written to mlb_pitcher_statcast_master.csv"


def load_pitcher_statcast(min_pa: int = 10) -> dict:
    """
    Load the pitcher Statcast master and return a dict keyed by lowercase pitcher name.
    Values include float-coerced numeric fields.
    Used by mlb_model.py to adjust SP suppression factor.
    """
    if not os.path.exists(OUT_PATH):
        log.debug("Pitcher Statcast master not found — run scraper first")
        return {}

    NUMERIC = {
        "xwoba", "xba", "xslg", "woba", "ba", "slg",
        "exit_velocity_avg", "launch_angle_avg",
        "barrel_batted_rate", "hard_hit_percent",
        "k_percent", "bb_percent",
        "whiff_percent", "hard_hit_percent",
        "pa", "bip",
    }

    data = {}
    try:
        with open(OUT_PATH, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    pa = int(float(row.get("pa") or 0))
                except (ValueError, TypeError):
                    pa = 0
                if pa < min_pa:
                    continue
                # KEY BY BOTH ID AND NAME (fixed 2026-08-19).
                #
                # This dict was keyed on player_name alone. In the master CSV that
                # column holds player IDs, not names, so every lookup in
                # mlb_model.exp_runs (which searches by lowercase pitcher name)
                # missed. 741 pitchers loaded and none were reachable, so
                # stuff_mult stayed 1.0 and the xwOBA (25%) and whiff (10%)
                # weights contributed nothing to any pick.
                #
                # Storing the same record under both keys makes the lookup work
                # whatever that column happens to contain, and an ID match cannot
                # suffer the name matching problems that have cost this project
                # Kalshi, platoon and pitcher matching bugs already.
                name = row.get("player_name", "").strip().lower()
                pid  = str(row.get("player_id", "") or "").strip()
                if not name and not pid:
                    continue
                rec = dict(row)
                for field in NUMERIC:
                    v = rec.get(field, "")
                    try:
                        rec[field] = float(v) if v not in ("", None) else None
                    except (ValueError, TypeError):
                        rec[field] = None
                if name:
                    data[name] = rec
                if pid:
                    data[pid] = rec
    except Exception as e:
        log.warning(f"load_pitcher_statcast failed: {e}")

    log.debug(f"Pitcher Statcast loaded: {len(data)} qualified pitchers")
    return data


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    year_arg = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now().year
    print(run(year=year_arg))

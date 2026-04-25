"""
mlb_statcast_scraper.py
Pulls season-level Statcast quality-of-contact metrics from Baseball Savant.

Metrics fetched:
  barrel_batted_rate   — % of batted balls that are barrels (hardest, best angle)
  exit_velocity_avg    — avg exit velo on contact (mph)
  hard_hit_percent     — % of batted balls hit 95+ mph
  xba                  — expected batting average (contact quality)
  xslg                 — expected slugging percentage

These are used by the model as a "true talent" signal independent of park/luck.
A player with elite barrel rate but poor results is likely due for positive regression.

Data source: Baseball Savant public CSV endpoint (no auth required).
Refreshed once per day during the morning pipeline run.

Usage:
    python scrapers/mlb_statcast_scraper.py
    from scrapers.mlb_statcast_scraper import run, load_statcast
"""

import os
import csv
import time
import logging
import requests
from datetime import datetime

log = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(os.path.dirname(__file__))
CLEAN_DIR  = os.path.join(BASE_DIR, "data", "clean")
OUT_PATH   = os.path.join(CLEAN_DIR, "mlb_statcast_master.csv")

# Baseball Savant free public CSV — season-level expected stats leaderboard
# min=q means qualified batters (enough PAs to be meaningful)
SAVANT_URL = (
    "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
    "?type=batter&year={year}&position=&team=&min=20&csv=true"
)

FIELDS_KEEP = [
    "player_id", "last_name", "first_name", "year",
    "pa", "bip",
    "ba", "xba",
    "slg", "xslg",
    "woba", "xwoba",
    "exit_velocity_avg", "launch_angle_avg",
    "barrel_batted_rate", "hard_hit_percent",
    "k_percent", "bb_percent",
]


def _fetch_savant_csv(year: int) -> list[dict]:
    url = SAVANT_URL.format(year=year)
    log.info(f"Fetching Statcast leaderboard: {url}")
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": "mlb-betting-pipeline/1.0"},
            timeout=30,
        )
        resp.raise_for_status()
        lines = resp.text.splitlines()
        if not lines:
            log.warning("Savant returned empty CSV")
            return []

        reader = csv.DictReader(lines)
        rows   = []
        for row in reader:
            # Normalise field names (Savant sometimes uses spaces)
            clean = {k.strip().lower().replace(" ", "_"): (v.strip() if v is not None else "") for k, v in row.items()}

            # Build canonical record — keep only what the model needs
            rec = {"year": str(year)}
            for field in FIELDS_KEEP:
                rec[field] = clean.get(field, "")

            # Derived full name for matching against lineup data
            first = clean.get("first_name", "").strip()
            last  = clean.get("last_name", "").strip()
            if first and last:
                rec["player_name"] = f"{first} {last}"
            elif last:
                rec["player_name"] = last
            else:
                rec["player_name"] = clean.get("player_id", "")

            rows.append(rec)

        log.info(f"Statcast: {len(rows)} batters fetched for {year}")
        return rows

    except requests.HTTPError as e:
        log.warning(f"Savant HTTP error: {e}")
        return []
    except Exception as e:
        log.error(f"Statcast fetch failed: {e}", exc_info=True)
        return []


def run(year: int = None) -> str:
    """
    Fetch Statcast data and write/merge into mlb_statcast_master.csv.
    Returns a summary string.
    """
    if year is None:
        year = datetime.now().year

    rows = _fetch_savant_csv(year)
    if not rows:
        return f"Statcast: no data fetched for {year}"

    os.makedirs(CLEAN_DIR, exist_ok=True)

    # All fields across all rows (union)
    all_fields = list({k for r in rows for k in r.keys()})
    all_fields = sorted(all_fields)

    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"Statcast master written: {OUT_PATH} ({len(rows)} rows)")
    return f"Statcast: {len(rows)} batters written to mlb_statcast_master.csv"


def load_statcast(min_pa: int = 20) -> dict:
    """
    Load the statcast master and return a dict keyed by lowercase player name.
    Values are the row dict with float-coerced numeric fields.
    Used by mlb_props_model.py and mlb_model.py for quality-of-contact signals.

    Example:
        sc = load_statcast()
        sc["aaron judge"]["barrel_batted_rate"]  # -> 0.182 (18.2%)
    """
    if not os.path.exists(OUT_PATH):
        log.debug("Statcast master not found — run scraper first")
        return {}

    NUMERIC = {
        "barrel_batted_rate", "exit_velocity_avg", "hard_hit_percent",
        "xba", "xslg", "xwoba", "woba", "ba", "slg",
        "launch_angle_avg", "k_percent", "bb_percent", "pa", "bip",
    }

    data = {}
    try:
        with open(OUT_PATH, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    pa = int(float(row.get("pa") or 0))
                except ValueError:
                    pa = 0
                if pa < min_pa:
                    continue

                name = row.get("player_name", "").strip().lower()
                if not name:
                    continue

                rec = dict(row)
                for field in NUMERIC:
                    v = rec.get(field, "")
                    try:
                        rec[field] = float(v) if v not in ("", None) else None
                    except ValueError:
                        rec[field] = None

                data[name] = rec

    except Exception as e:
        log.warning(f"load_statcast failed: {e}")

    log.debug(f"Statcast loaded: {len(data)} qualified batters")
    return data


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    year = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now().year
    print(run(year=year))

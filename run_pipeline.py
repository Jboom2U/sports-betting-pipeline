"""
run_pipeline.py
Single entry point for the MLB data pipeline.
Called by Windows Task Scheduler at 4am daily.

Usage:
    python run_pipeline.py
    python run_pipeline.py --date 2025-04-13   # backfill a specific date
"""

import sys
import os
import logging
import argparse
from datetime import datetime

# ── Add project root to path ──────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="MLB Betting Data Pipeline")
    parser.add_argument("--date", type=str, default=None,
                        help="Override date for backfill (YYYY-MM-DD). Defaults to yesterday.")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"PIPELINE START | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # ── Step 1: Scrape ────────────────────────────────────────────────────────
    try:
        from scrapers.mlb_scraper import run as scrape
        if args.date:
            # Backfill: override YESTERDAY in scraper
            import scrapers.mlb_scraper as scraper_module
            scraper_module.YESTERDAY = args.date
            scraper_module.TODAY     = args.date
            scraper_module.TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        counts = scrape()
        log.info(f"Scrape complete: {counts}")
    except Exception as e:
        log.error(f"SCRAPE FAILED: {e}", exc_info=True)
        sys.exit(1)

    # ── Step 2: Normalize + Append ────────────────────────────────────────────
    try:
        from normalize.mlb_normalize import run as normalize
        if args.date:
            import normalize.mlb_normalize as norm_module
            norm_module.TODAY = args.date
        normalize()
        log.info("Normalize + append complete")
    except Exception as e:
        log.error(f"NORMALIZE FAILED: {e}", exc_info=True)
        sys.exit(1)

    # ── Step 3: Odds snapshot + line movement ─────────────────────────────────
    try:
        from scrapers.mlb_odds_scraper import run as run_odds
        odds_result = run_odds()
        log.info(f"Odds scrape: {odds_result}")
    except Exception as e:
        log.warning(f"Odds scrape failed (non-fatal): {e}")

    # ── Step 4: Weather for today's games ─────────────────────────────────────
    try:
        from scrapers.mlb_weather_scraper import run as run_weather
        today = datetime.now().strftime("%Y-%m-%d")
        weather_rows = run_weather(target_date=today)
        log.info(f"Weather fetched: {len(weather_rows)} games")
    except Exception as e:
        log.warning(f"Weather fetch failed (non-fatal): {e}")

    # ── Step 4: Today's probable pitcher recent starts ─────────────────────────
    try:
        from scrapers.mlb_pitcher_scraper import fetch_all_recent_starts
        from normalize.mlb_pitcher_normalize import normalize_recent_starts
        import csv as _csv

        # Get today's probable pitchers from schedule master
        sched_path = os.path.join(os.path.dirname(__file__), "data", "clean", "mlb_schedule_master.csv")
        today = datetime.now().strftime("%Y-%m-%d")
        pitcher_ids = []
        if os.path.exists(sched_path):
            with open(sched_path, encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    if row.get("game_date") == today:
                        for col in ("away_probable_pitcher", "home_probable_pitcher"):
                            pname = row.get(col, "").strip()
                            if pname and pname != "TBD":
                                pitcher_ids.append(pname)

        # Look up player IDs from pitcher stats master
        stats_path = os.path.join(os.path.dirname(__file__), "data", "clean", "mlb_pitcher_stats_master.csv")
        name_to_id = {}
        if os.path.exists(stats_path):
            with open(stats_path, encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    name_to_id[row.get("player_name","").strip()] = row.get("player_id","")

        id_name_pairs = [(name_to_id[n], n) for n in pitcher_ids if n in name_to_id]

        if id_name_pairs:
            season = datetime.now().year
            recent = fetch_all_recent_starts(id_name_pairs, season, n=5)
            normalize_recent_starts(recent)
            log.info(f"Recent starts fetched: {len(recent)} rows for {len(id_name_pairs)} pitchers")
        else:
            log.info("No pitcher IDs resolved for today — skipping recent starts")
    except Exception as e:
        log.warning(f"Recent starts fetch failed (non-fatal): {e}")

    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

"""
run_historical.py
One-time historical data backfill for the MLB betting pipeline.
Pulls 2023-2025 game scores, pitcher season stats, and team stats,
then normalizes everything into clean master CSVs.

Run this ONCE to seed the database. After that, run_pipeline.py handles daily updates.

Usage:
    python run_historical.py
    python run_historical.py --skip-scores     # skip game score backfill
    python run_historical.py --skip-pitchers   # skip pitcher stats
    python run_historical.py --skip-teams      # skip team stats
    python run_historical.py --normalize-only  # skip all scraping, just normalize

Estimated runtime: 5-12 minutes depending on network speed.
Safe to cancel and re-run -- score backfill resumes where it left off.
"""

import sys
import os
import logging
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"historical_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="MLB Historical Data Backfill")
    parser.add_argument("--skip-scores",    action="store_true", help="Skip game score backfill")
    parser.add_argument("--skip-pitchers",  action="store_true", help="Skip pitcher stats pull")
    parser.add_argument("--skip-teams",     action="store_true", help="Skip team stats pull")
    parser.add_argument("--normalize-only", action="store_true", help="Skip all scraping, just normalize existing raw files")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"HISTORICAL BACKFILL START | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)
    log.info("Pulling 2023-2025 game scores, pitcher stats, and team stats.")
    log.info("Estimated time: 5-12 minutes. Safe to cancel and re-run.")
    log.info("=" * 60)

    results = {}

    # ── Step 1: Historical Game Scores ────────────────────────────────────────
    if not args.skip_scores and not args.normalize_only:
        log.info("STEP 1/4: Historical game scores (2023-2025)...")
        try:
            from scrapers.mlb_historical_scraper import run as run_scores
            results["scores"] = run_scores()
        except Exception as e:
            log.error(f"Historical scores failed: {e}", exc_info=True)
    else:
        log.info("STEP 1/4: Skipped (game scores)")

    # ── Step 2: Pitcher Stats ─────────────────────────────────────────────────
    if not args.skip_pitchers and not args.normalize_only:
        log.info("STEP 2/4: Pitcher season stats + home/away splits (2023-2025)...")
        try:
            from scrapers.mlb_pitcher_scraper import run as run_pitchers
            results["pitchers"] = run_pitchers()
        except Exception as e:
            log.error(f"Pitcher stats failed: {e}", exc_info=True)
    else:
        log.info("STEP 2/4: Skipped (pitcher stats)")

    # ── Step 3: Team Stats ────────────────────────────────────────────────────
    if not args.skip_teams and not args.normalize_only:
        log.info("STEP 3/4: Team hitting + pitching stats (2023-2025)...")
        try:
            from scrapers.mlb_team_scraper import run as run_teams
            results["teams"] = run_teams()
        except Exception as e:
            log.error(f"Team stats failed: {e}", exc_info=True)
    else:
        log.info("STEP 3/4: Skipped (team stats)")

    # ── Step 4: Normalize All Raw Data ────────────────────────────────────────
    log.info("STEP 4/4: Normalizing all historical data into clean master CSVs...")
    try:
        from normalize.mlb_historical_normalize import run as normalize
        results["normalize"] = normalize()
    except Exception as e:
        log.error(f"Historical normalize failed: {e}", exc_info=True)

    log.info("=" * 60)
    log.info("HISTORICAL BACKFILL COMPLETE")
    log.info(f"Results: {results}")
    log.info(f"Clean data written to: data/clean/")
    log.info("Master files created:")
    log.info("  mlb_scores_master.csv        (all game results 2023-2025)")
    log.info("  mlb_pitcher_stats_master.csv (season stats per starter)")
    log.info("  mlb_pitcher_splits_master.csv (home/away splits per starter)")
    log.info("  mlb_team_hitting_master.csv  (team offense per season)")
    log.info("  mlb_team_pitching_master.csv (team pitching per season)")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

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

    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

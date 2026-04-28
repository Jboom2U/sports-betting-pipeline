"""
run_afternoon.py
Second daily run — scheduled at 11:30 AM ET via Windows Task Scheduler.

Purpose:
    The 4 AM pipeline runs before lineups are posted (1-3 hrs before first pitch),
    so DATA_PROPS is always empty in the morning HTML. This script re-runs the
    lineup + hitter scrapers and regenerates the HTML once lineups are live.

    Also re-snapshots odds (lines move throughout the morning) and runs the
    Kalshi scraper if configured.

Usage:
    python run_afternoon.py               # today
    python run_afternoon.py --no-open     # don't open browser

Schedule:
    Windows Task Scheduler → Action: python run_afternoon.py
    Trigger: Daily at 11:30 AM
    Same working directory as mlb_pipeline.bat
"""

import sys
import os
import logging
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"afternoon_{datetime.now().strftime('%Y-%m-%d')}.log")
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
    parser = argparse.ArgumentParser(description="MLB Afternoon Refresh")
    parser.add_argument("--no-open", action="store_true", help="Don't open browser after rebuild")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info(f"AFTERNOON REFRESH | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # ── Step 0: Auto-grade yesterday's picks ─────────────────────────────────
    # Runs before anything else so yesterday's analysis JSON is ready
    # for the HTML dashboard to embed in its Yesterday panel.
    try:
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        import run_analysis
        run_analysis.run(yesterday)
        log.info(f"Yesterday's picks graded: {yesterday}")
    except Exception as e:
        log.warning(f"Analysis grade failed (non-fatal): {e}")

    # ── Step 1: Refresh odds (lines move all morning) ─────────────────────────
    try:
        from scrapers.mlb_odds_scraper import run as run_odds
        result = run_odds()
        log.info(f"Odds refreshed: {result}")
    except Exception as e:
        log.warning(f"Odds refresh failed (non-fatal): {e}")

    # ── Step 1b: Refresh umpire assignments (can post/change up to game time) ──
    try:
        from scrapers.mlb_umpire_scraper import run as run_umps
        ump_rows = run_umps(target_date=today)
        log.info(f"Umpires refreshed: {len(ump_rows)} games")
    except Exception as e:
        log.warning(f"Umpire refresh failed (non-fatal): {e}")

    # ── Step 1c: Refresh bullpen fatigue (workload accumulates through day) ────
    try:
        from scrapers.mlb_bullpen_fatigue_scraper import run as run_fatigue
        fatigue_report = run_fatigue(target_date=today)
        log.info(f"Bullpen fatigue refreshed: {len(fatigue_report)} teams")
    except Exception as e:
        log.warning(f"Bullpen fatigue refresh failed (non-fatal): {e}")

    # ── Step 2: Refresh lineups + hitter stats ────────────────────────────────
    try:
        from scrapers.mlb_lineup_scraper import run as run_lineups
        lineups = run_lineups(target_date=today)
        confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
        log.info(f"Lineups: {len(lineups)} games, {confirmed} confirmed")

        if confirmed > 0:
            from scrapers.mlb_hitter_scraper import run as run_hitters
            run_hitters(target_date=today)
            log.info("Hitter stats refreshed — props will now populate")
        else:
            log.info("No confirmed lineups yet. Try again closer to first pitch.")
    except Exception as e:
        log.warning(f"Lineup/hitter refresh failed (non-fatal): {e}")

    # ── Step 3: Kalshi market snapshot (optional — needs .env config) ─────────
    try:
        from scrapers.mlb_kalshi_scraper import run as run_kalshi
        k_result = run_kalshi(target_date=today)
        log.info(f"Kalshi: {k_result}")
    except ImportError:
        log.info("Kalshi scraper not yet configured — skipping")
    except Exception as e:
        log.warning(f"Kalshi snapshot failed (non-fatal): {e}")

    # ── Step 4: Regenerate HTML dashboard ────────────────────────────────────
    # run_picks_html.main() already re-runs lineup/hitter refresh internally,
    # but we do it above first so the data is warm before the model scores.
    try:
        import run_picks_html
        # Patch sys.argv so argparse inside main() picks up --no-open correctly
        sys.argv = ["run_picks_html.py"]
        if args.no_open:
            sys.argv.append("--no-open")
        run_picks_html.main()
        log.info("HTML dashboard regenerated with updated props + odds")
    except Exception as e:
        log.error(f"HTML rebuild failed: {e}", exc_info=True)

    log.info("=" * 60)
    log.info("AFTERNOON REFRESH COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

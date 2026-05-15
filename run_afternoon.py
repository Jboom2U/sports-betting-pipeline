"""
run_afternoon.py
Second daily run — scheduled at 11:30 AM ET via Railway scheduler (app.py background thread).

Purpose:
    The 6am pipeline runs before lineups are posted (4-5 hrs before first pitch),
    so player props are sparse in the morning. This script re-runs the lineup +
    hitter scrapers and regenerates the dashboard once lineups are live.

    Also re-snapshots odds (lines move throughout the morning), umpires, bullpen
    fatigue, Kalshi, and Polymarket.

Usage:
    python run_afternoon.py               # manual trigger if needed
    python run_afternoon.py --no-open     # don't open browser
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

            # Upload hitter stats JSON to R2 so batter props survive Railway restarts
            try:
                from db.csv_sync import upload_hitter_stats
                upload_hitter_stats(today)
            except Exception as _ue:
                log.warning(f"Hitter stats R2 upload failed (non-fatal): {_ue}")

            # Save confirmed prop picks to DB (ON CONFLICT DO NOTHING so safe to re-run)
            try:
                from model.mlb_props_model import score_all_props
                from db.picks_store import save_prop_pick
                props = score_all_props(target_date=today)
                saved = 0
                for _p in props:
                    if _p.get("projected"):
                        continue  # skip non-confirmed lineup props
                    save_prop_pick(
                        game_date=today,
                        player_name=_p.get("player_name", ""),
                        team=_p.get("team", ""),
                        away_team=_p.get("away_team", ""),
                        home_team=_p.get("home_team", ""),
                        prop_type=_p.get("prop_type", ""),
                        line=_p.get("line", 0),
                        model_conf=_p.get("confidence", 0),
                    )
                    saved += 1
                log.info(f"Props saved to DB: {saved} confirmed picks")
            except Exception as _pe:
                log.warning(f"Prop DB save failed (non-fatal): {_pe}")
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

    # ── Step 3b: Polymarket snapshot ─────────────────────────────────────────
    try:
        from scrapers.mlb_polymarket_scraper import run as run_polymarket
        poly_result = run_polymarket(target_date=today)
        log.info(f"Polymarket: {poly_result}")
    except Exception as e:
        log.warning(f"Polymarket snapshot failed (non-fatal): {e}")

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

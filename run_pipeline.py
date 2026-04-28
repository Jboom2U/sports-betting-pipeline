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

# ── Persistence helpers (non-fatal — work without DATABASE_URL / STORAGE_*) ───
try:
    from db.pipeline_log import mark_pipeline_started, mark_pipeline_complete, mark_pipeline_failed
    from db.picks_store import save_picks, save_scored_games
    from db.csv_sync import upload_all as csv_upload_all, storage_available
    _DB_AVAILABLE = True
except ImportError as _e:
    _DB_AVAILABLE = False
    # Define no-ops so the rest of the file doesn't need if-guards everywhere
    def mark_pipeline_started():     pass
    def mark_pipeline_complete():    pass
    def mark_pipeline_failed(n=""): pass
    def save_picks(p, d):            return 0
    def save_scored_games(g, d):     return 0
    def csv_upload_all():            return 0
    def storage_available():         return False

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
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# ── Single-instance lock — prevents Windows Task Scheduler double-runs ────────
LOCK_FILE = os.path.join(LOG_DIR, "pipeline.lock")

def _acquire_lock() -> bool:
    """Return True if this process acquired the run lock, False if already running."""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE) as f:
                pid = int(f.read().strip())
            # Check if that PID is still alive
            import signal
            try:
                os.kill(pid, 0)   # signal 0 = check existence only
                log.warning(f"Pipeline already running (PID {pid}) — aborting duplicate run")
                return False
            except (OSError, ProcessLookupError):
                pass  # PID is dead — stale lock, safe to overwrite
        except Exception:
            pass
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True

def _release_lock():
    try:
        os.remove(LOCK_FILE)
    except Exception:
        pass


def main(date=None):
    # argparse is handled in __main__ block only — never called from here.
    log.info("=" * 60)
    log.info(f"PIPELINE START | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    mark_pipeline_started()

    # ── Step 1: Scrape ────────────────────────────────────────────────────────
    try:
        from scrapers.mlb_scraper import run as scrape
        if date:
            # Backfill: override YESTERDAY in scraper
            import scrapers.mlb_scraper as scraper_module
            scraper_module.YESTERDAY = date
            scraper_module.TODAY     = date
            scraper_module.TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        counts = scrape()
        log.info(f"Scrape complete: {counts}")
    except Exception as e:
        log.error(f"SCRAPE FAILED: {e}", exc_info=True)
        sys.exit(1)

    # ── Step 2: Normalize + Append ────────────────────────────────────────────
    try:
        from normalize.mlb_normalize import run as normalize
        if date:
            import normalize.mlb_normalize as norm_module
            norm_module.TODAY = date
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

    # ── Step 4b: Umpire assignments for today's games ──────────────────────────
    try:
        from scrapers.mlb_umpire_scraper import run as run_umps
        today = datetime.now().strftime("%Y-%m-%d")
        ump_rows = run_umps(target_date=today)
        log.info(f"Umpires fetched: {len(ump_rows)} games")
    except Exception as e:
        log.warning(f"Umpire fetch failed (non-fatal): {e}")

    # ── Step 4c: Bullpen fatigue (reliever workload last 3 days) ───────────────
    try:
        from scrapers.mlb_bullpen_fatigue_scraper import run as run_fatigue
        today = datetime.now().strftime("%Y-%m-%d")
        fatigue_report = run_fatigue(target_date=today)
        log.info(f"Bullpen fatigue: {len(fatigue_report)} teams processed")
    except Exception as e:
        log.warning(f"Bullpen fatigue fetch failed (non-fatal): {e}")

    # ── Step 4c: Today's probable pitcher recent starts ────────────────────────
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

    # ── Step 5: Statcast quality-of-contact metrics (batters) ─────────────────
    try:
        from scrapers.mlb_statcast_scraper import run as run_statcast
        sc_result = run_statcast()
        log.info(f"Statcast (batters): {sc_result}")
    except Exception as e:
        log.warning(f"Statcast (batters) fetch failed (non-fatal): {e}")

    # ── Step 5b: Statcast stuff metrics (pitchers) ─────────────────────────────
    try:
        from scrapers.mlb_statcast_pitcher_scraper import run as run_pitcher_statcast
        psc_result = run_pitcher_statcast()
        log.info(f"Statcast (pitchers): {psc_result}")
    except Exception as e:
        log.warning(f"Statcast (pitchers) fetch failed (non-fatal): {e}")

    # ── Step 6: Bullpen stats ──────────────────────────────────────────────────
    try:
        from scrapers.mlb_bullpen_scraper import run as run_bullpen
        from normalize.mlb_bullpen_normalize import run as normalize_bullpen
        bp_rows = run_bullpen()
        normalize_bullpen()
        log.info(f"Bullpen stats fetched and normalized: {len(bp_rows)} teams")
    except Exception as e:
        log.warning(f"Bullpen fetch failed (non-fatal): {e}")

    # ── Step 6: Confirmed lineups + hitter stats for props ─────────────────────
    try:
        from scrapers.mlb_lineup_scraper import run as run_lineups
        today = datetime.now().strftime("%Y-%m-%d")
        lineups = run_lineups(target_date=today)
        confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
        log.info(f"Lineups: {len(lineups)} games, {confirmed} confirmed")

        if confirmed > 0:
            from scrapers.mlb_hitter_scraper import run as run_hitters
            run_hitters(target_date=today)
            log.info("Hitter stats for props fetched")
        else:
            log.info("No confirmed lineups yet — skipping hitter stats for props")
    except Exception as e:
        log.warning(f"Lineup/hitter fetch failed (non-fatal): {e}")

    # ── Step 7: Grade yesterday's picks ──────────────────────────────────────
    try:
        from run_analysis import main as run_analysis
        run_analysis()
        log.info("Yesterday's picks graded — analysis JSON written")
    except Exception as e:
        log.warning(f"Analysis grader failed (non-fatal): {e}")

    # ── Step 8: Score today's games and save picks to DB ─────────────────────
    # We score here (after all scrapers have run) so picks saved to DB reflect
    # the freshest data. run_picks_html.py will score again when building HTML —
    # that's fine, scoring is fast and idempotent.
    try:
        today_str = datetime.now().strftime("%Y-%m-%d") if not date else date
        from model.mlb_model import MLBModel
        from model.mlb_picks import generate_picks

        _model = MLBModel()
        _model.load()
        scored_games, actual_date = _model.score_today(target_date=today_str)

        if scored_games:
            picks = generate_picks(scored_games)
            n_picks = save_picks(picks, actual_date)
            n_games = save_scored_games(scored_games, actual_date)
            log.info(
                f"DB: {n_picks} picks + {n_games} scored games saved for {actual_date}."
            )
    except Exception as e:
        log.warning(f"DB save failed (non-fatal): {e}")

    # ── Step 9: Mark pipeline complete in DB ──────────────────────────────────
    mark_pipeline_complete()

    # ── Step 10: Upload CSVs to object storage ────────────────────────────────
    try:
        if storage_available():
            log.info("Uploading CSV snapshots to object storage...")
            n_uploaded = csv_upload_all()
            log.info(f"CSV sync upload complete: {n_uploaded} file(s).")
        else:
            log.debug("Object storage not configured -- skipping CSV upload.")
    except Exception as e:
        log.warning(f"CSV upload failed (non-fatal): {e}")

    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MLB Betting Data Pipeline")
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()
    if not _acquire_lock():
        sys.exit(0)   # Another instance is running -- exit cleanly
    try:
        main(date=args.date)
    finally:
        _release_lock()

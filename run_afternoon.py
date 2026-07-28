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

    # ── Step 1: Refresh odds — Pinnacle for ML/RL, Odds API for the TOTAL ─────
    # Pinnacle (free, sharp, paired line/price) drives ML/RL. Pinnacle's guest
    # feed has no clean full-game total, so we take a SECOND Odds-API total pull
    # here (~2h before first pitch = near the closing number). Two accurate total
    # updates/day (6am + now), still well inside the 500/mo quota.
    try:
        from scrapers.mlb_odds_scraper import run as run_odds
        odds_api_result = run_odds()
        log.info(f"Odds API (afternoon total): {odds_api_result}")
    except Exception as e:
        log.warning(f"Odds API afternoon total failed (non-fatal): {e}")
    try:
        from scrapers.mlb_pinnacle_scraper import run as run_pinnacle
        result = run_pinnacle()
        log.info(f"Pinnacle odds refreshed (ML/RL): {result}")
    except Exception as e:
        log.warning(f"Pinnacle odds refresh failed (non-fatal): {e}")

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

    # ── Step 1d: Refresh probable pitchers (rotation can change after 6am) ──────
    try:
        from scrapers.mlb_scraper import fetch_schedule, write_raw
        from normalize.mlb_normalize import normalize_schedule, append_to_master
        sched_rows = fetch_schedule(days_ahead=1)
        write_raw(sched_rows, "schedule")
        clean_rows = normalize_schedule(sched_rows)
        append_to_master(clean_rows, "schedule", dedup_key="game_id")
        log.info(f"Probable pitchers refreshed: {len(sched_rows)} schedule rows")
    except Exception as e:
        log.warning(f"Schedule/probable pitcher refresh failed (non-fatal): {e}")

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

    # ── Step 3b: Polymarket snapshot ─────────────────────────────────────────
    try:
        from scrapers.mlb_polymarket_scraper import run as run_polymarket
        poly_result = run_polymarket(target_date=today)
        log.info(f"Polymarket: {poly_result}")
    except Exception as e:
        log.warning(f"Polymarket snapshot failed (non-fatal): {e}")

    # ── Step 3c: Save confirmed props to DB so they get graded tomorrow ───────
    # The 6am pipeline runs BEFORE lineups post, so every prop is "projected"
    # and skipped — player_prop_history ends up empty and grading finds nothing
    # (that was the Props 0-0 bug). Now that lineups are confirmed, persist the
    # real props here. Idempotent: save_prop_pick is ON CONFLICT DO NOTHING.
    try:
        # Pull real Pinnacle K lines first (free) — without them score_all_props
        # emits 0 K props, so nothing saves to grade. This was the props 0-0 bug.
        try:
            from scrapers.mlb_pinnacle_scraper import save_strikeout_lines
            _nk = save_strikeout_lines(today)
            log.info(f"Pinnacle K lines pulled for prop save: {_nk} pitcher(s)")
        except Exception as _pe:
            log.warning(f"Pinnacle K pull before prop save failed (non-fatal): {_pe}")
        from model.mlb_props_model import score_all_props as _score_props
        from db.picks_store import save_prop_pick as _save_prop
        _props = _score_props(target_date=today)
        _saved = 0
        for _pp in _props:
            if _pp.get("projected"):
                continue  # lineup still unconfirmed for this game — skip
            _save_prop(
                game_date  = today,
                player_name= _pp.get("player_name", ""),
                team       = _pp.get("side", ""),
                away_team  = _pp.get("away_team", ""),
                home_team  = _pp.get("home_team", ""),
                prop_type  = _pp.get("prop_type", ""),
                line       = _pp.get("line", 0),
                model_conf = _pp.get("confidence", 0),
                pick_side  = _pp.get("pick_side", "OVER"),
            )
            _saved += 1
        log.info(f"Props persisted for grading: {_saved} confirmed prop pick(s)")
    except Exception as e:
        log.warning(f"Afternoon prop save failed (non-fatal): {e}")

    # ── Step 3d: Player game logs (powers the Players section trend charts) ───
    # The 6am pipeline runs this too, but lineups aren't posted yet so
    # get_lineup_players() finds nobody and player_game_logs stays EMPTY — which
    # is why the Players search/pages had no data. Now that lineups are confirmed,
    # pull each player's season game log so their trend charts fill in.
    try:
        from model.mlb_props_model import score_all_props as _sap
        from scrapers.mlb_player_gamelog_scraper import run_for_players as _rfp
        _plist = {}
        for _p in _sap(target_date=today):
            _pid = _p.get("player_id")
            if not _pid:
                continue
            _plist[int(_pid)] = {"player_id": int(_pid),
                                 "player_name": _p.get("player_name", ""),
                                 "is_pitcher": _p.get("side") == "pitcher"}
        _gl = _rfp(list(_plist.values()))
        log.info(f"Player game logs (afternoon, from props): {_gl}")
    except Exception as e:
        log.warning(f"Afternoon game-log scrape failed (non-fatal): {e}")

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

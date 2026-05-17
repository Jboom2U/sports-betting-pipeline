"""
app.py — Flask web server for Statalizers
- Serves dashboard instantly from cache on every request
- Runs full data pipeline at 6am ET every morning automatically
- On startup, checks if today's data is missing and runs pipeline if so
- Background cache refresh every 10 minutes — never blocks a request

Deploy to Railway:
    railway up
"""

import os
import sys
import time
import logging
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask, Response, redirect, request
from flask_compress import Compress

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Persistence layer (non-fatal — works without DATABASE_URL / STORAGE_*) ────
try:
    from db.schema import create_all as _db_create_all
    from db.pipeline_log import pipeline_ran_today as _db_pipeline_ran_today
    from db.csv_sync import download_all as _csv_download, storage_available as _storage_ok
    _DB_AVAILABLE = True
except ImportError as _e:
    log.warning(f"db/ module not importable: {_e} — falling back to file-based checks.")
    _DB_AVAILABLE = False

app = Flask(__name__)
Compress(app)   # gzip all responses — shrinks 570KB HTML to ~80KB

BASE_DIR  = os.path.dirname(__file__)
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
ET        = ZoneInfo("America/New_York")

# ── Cache ─────────────────────────────────────────────────────────────────────
CACHE_TTL = 10 * 60          # seconds — regenerate dashboard every 10 minutes
_cache_lock = threading.Lock()
_cache = {
    "html":         None,
    "generated_at": 0,
    "generating":   False,
}

WARMING_HTML = """<!DOCTYPE html><html><head>
<meta http-equiv="refresh" content="30">
<style>body{background:#0d1117;color:#fff;font-family:sans-serif;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.box{text-align:center}.spinner{font-size:2em;margin-bottom:16px}
p{color:#aaa;margin-top:8px;font-size:14px}</style></head><body>
<div class="box"><div class="spinner">⚾</div>
<h2>Dashboard is warming up...</h2>
<p>Fetching lineups, stats, and odds. This page will auto-refresh in 30 seconds.</p>
</div></body></html>"""


# ── Pipeline ──────────────────────────────────────────────────────────────────
def _needs_pipeline_run() -> bool:
    """
    Check if today's pipeline needs to run.
    Uses DB check when available, falls back to pipeline_run_date.txt.
    """
    if _DB_AVAILABLE:
        try:
            return not _db_pipeline_ran_today()
        except Exception as e:
            log.warning(f"DB pipeline check failed, using file fallback: {e}")

    # File-based fallback
    today = datetime.now(ET).strftime("%Y-%m-%d")
    marker = os.path.join(BASE_DIR, "data", "pipeline_run_date.txt")
    if os.path.exists(marker):
        with open(marker) as f:
            last_run = f.read().strip()
        return last_run != today
    return True


def _mark_pipeline_ran():
    """
    Mark the pipeline as run today.
    DB write is handled inside run_pipeline.main() via pipeline_log.
    This keeps the legacy file marker as a fallback.
    """
    today = datetime.now(ET).strftime("%Y-%m-%d")
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
    with open(os.path.join(BASE_DIR, "data", "pipeline_run_date.txt"), "w") as f:
        f.write(today)


def _run_full_pipeline():
    """Run the full data pipeline then regenerate the dashboard."""
    log.info("Running full data pipeline...")
    try:
        from run_pipeline import main as pipeline
        pipeline()
        _mark_pipeline_ran()
        log.info("Pipeline complete.")
    except Exception as e:
        log.error(f"Pipeline failed: {e}", exc_info=True)

    # Schedule afternoon refresh 2 hours before first pitch (one-shot, non-fatal)
    try:
        _schedule_adaptive_refresh()
    except Exception as _sar_e:
        log.warning(f"Adaptive refresh scheduling failed (non-fatal): {_sar_e}")

    # Grade yesterday's picks and push results to DB (non-fatal)
    yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        from run_analysis import run as grade_picks
        result = grade_picks(yesterday)
        if result:
            graded_count = len([p for p in result.get("graded", [])
                                 if p.get("result") in ("WIN", "LOSS", "PUSH")])
            log.info(f"Nightly grading complete: {graded_count} picks graded for {yesterday}")
        else:
            log.info(f"Nightly grading: no picks or results found for {yesterday}")
    except Exception as e:
        log.warning(f"Nightly grading failed (non-fatal): {e}")


def _needs_odds_snapshot() -> bool:
    """
    Returns True if it's between 8am-10pm ET and the last odds snapshot
    is more than 2 hours old. Keeps API usage well under the 500/month free limit
    (~4 snapshots/day * 30 days = 120 requests/month).
    """
    now   = datetime.now(ET)
    if now.hour < 8 or now.hour >= 22:
        return False   # Outside game hours — don't waste API calls
    today     = now.strftime("%Y-%m-%d")
    odds_path = os.path.join(CLEAN_DIR, "mlb_odds_master.csv")
    if not os.path.exists(odds_path):
        return True
    # Check age of most recent row for today
    try:
        import csv as _csv
        latest_time = ""
        with open(odds_path, encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                if row.get("game_date") == today:
                    t = row.get("snapshot_time", "")
                    if t > latest_time:
                        latest_time = t
        if not latest_time:
            return True   # No snapshot for today yet
        from datetime import timezone as _tz
        snap_dt  = datetime.strptime(latest_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_tz.utc)
        age_secs = (datetime.now(_tz.utc) - snap_dt).total_seconds()
        return age_secs > 2 * 3600   # Snapshot older than 2 hours
    except Exception:
        return False


def _run_odds_snapshot():
    """Take a fresh odds + Kalshi snapshot. Non-fatal — powers the Sharp Action panel."""
    log.info("Taking mid-day odds + Kalshi snapshot...")
    try:
        from scrapers.mlb_odds_scraper import run as run_odds
        result = run_odds()
        log.info(f"Odds snapshot complete: {result}")
    except Exception as e:
        log.warning(f"Odds snapshot failed (non-fatal): {e}")
    try:
        from scrapers.mlb_kalshi_scraper import run as run_kalshi
        k_result = run_kalshi()
        log.info(f"Kalshi snapshot complete: {k_result}")
    except Exception as e:
        log.warning(f"Kalshi snapshot failed (non-fatal): {e}")


def _needs_lineup_refresh() -> bool:
    """
    Returns True if we should re-fetch lineups + hitter stats.
    Checks:
      1. Before 10am ET — skip (lineups not posted yet)
      2. Hitter stats file missing — fetch immediately
      3. Hitter stats file >4 hours old — re-fetch (catches Railway restarts)
      4. Lineup JSON exists but has unconfirmed games — re-fetch until all confirmed
         This is the key fix: file age alone misses the window between
         "hitters fetched with no confirmed lineups" and "lineups actually post."
    """
    now   = datetime.now(ET)
    today = now.strftime("%Y-%m-%d")
    if now.hour < 10:
        return False   # Too early — lineups not posted yet

    stats_path  = os.path.join(BASE_DIR, "data", "raw", f"mlb_hitter_stats_{today}.json")
    lineup_path = os.path.join(BASE_DIR, "data", "raw", f"mlb_lineups_{today}.json")

    if not os.path.exists(stats_path):
        return True    # Never fetched today

    mtime     = os.path.getmtime(stats_path)
    age_hours = (time.time() - mtime) / 3600
    if age_hours > 4:
        return True    # Stale — Railway restart or long gap

    # Check if any games still have unconfirmed lineups — keep retrying until confirmed
    if os.path.exists(lineup_path):
        try:
            import json as _json
            with open(lineup_path, encoding="utf-8") as f:
                lineups = _json.load(f)
            total     = len(lineups)
            confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
            if total > 0 and confirmed < total:
                # Only retry every 30 minutes once we have the file, not every cache cycle
                if age_hours > 0.5:
                    log.info(f"Lineup refresh needed: {confirmed}/{total} confirmed, file age {age_hours:.1f}h")
                    return True
        except Exception:
            pass

    return False


def _run_lineup_refresh():
    """Re-run lineup + hitter steps only. Non-fatal — called mid-day after lineups post."""
    today = datetime.now(ET).strftime("%Y-%m-%d")
    log.info("Mid-day lineup refresh — checking for confirmed lineups...")
    try:
        from scrapers.mlb_lineup_scraper import run as run_lineups
        lineups  = run_lineups(target_date=today)
        confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
        log.info(f"Lineup refresh: {len(lineups)} games, {confirmed} confirmed")
        if confirmed > 0:
            from scrapers.mlb_hitter_scraper import run as run_hitters
            run_hitters(target_date=today)
            log.info("Hitter stats fetched — props will populate on next dashboard render")
    except Exception as e:
        log.warning(f"Mid-day lineup refresh failed (non-fatal): {e}")


# ── Dashboard generation ──────────────────────────────────────────────────────

def _picks_html_from_db() -> "str | None":
    """
    Query the DB for the most recent picks date, load those picks, and return
    a minimal dark-themed HTML page with a date banner.  Returns None if the
    DB is unavailable or no picks exist yet.
    """
    try:
        from db.connection import db_conn
        from db.picks_store import get_picks

        with db_conn() as conn:
            if conn is None:
                return None
            cur = conn.cursor()
            cur.execute(
                "SELECT MAX(pick_date) FROM picks WHERE pick_date <= CURRENT_DATE"
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return None
            latest_date = str(row[0])

        picks = get_picks(latest_date)
        if not picks:
            return None

        # Prefer forward-looking (PENDING) picks; fall back to all picks for that date
        display = [p for p in picks if p.get("actual_result") == "PENDING"] or picks

        TIER_ORDER = ["LOCK", "STRONG", "LEAN", "TOSSUP"]
        TIER_COLOR = {"LOCK": "#ffc107", "STRONG": "#42a5f5", "LEAN": "#66bb6a", "TOSSUP": "#a09ae0"}

        cards_html = ""
        for tier in TIER_ORDER:
            for p in [x for x in display if x.get("tier") == tier]:
                color    = TIER_COLOR.get(tier, "#8b949e")
                conf_pct = f"{float(p.get('conf') or 0)*100:.1f}%"
                raw_rsn  = p.get("reasoning") or ""
                reasoning = raw_rsn[:220] + ("…" if len(raw_rsn) > 220 else "")
                res = p.get("actual_result", "PENDING")
                res_color = {"WIN": "#3fb950", "LOSS": "#f85149", "PUSH": "#8b949e"}.get(res, "#8b949e")
                res_badge = (
                    f'<span style="color:{res_color};font-weight:700;font-size:.75rem;margin-left:8px">{res}</span>'
                    if res not in ("PENDING", None) else ""
                )
                cards_html += (
                    f'<div style="background:#161b22;border:1px solid #30363d;'
                    f'border-left:3px solid {color};border-radius:8px;padding:14px 16px;margin-bottom:10px">'
                    f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">'
                    f'<span style="color:{color};font-weight:700;font-size:.8rem">{tier}</span>'
                    f'<span style="color:#8b949e;font-size:.75rem">{p.get("pick_type","").upper()}</span>'
                    f'<span style="color:#e6edf3;font-size:.8rem;margin-left:auto;font-weight:600">{conf_pct}</span>'
                    f'{res_badge}</div>'
                    f'<div style="color:#e6edf3;font-weight:600;margin-bottom:2px">{p.get("label","")}</div>'
                    f'<div style="color:#8b949e;font-size:.8rem;margin-bottom:6px">{p.get("game","")}</div>'
                    f'<div style="color:#8b949e;font-size:.75rem;line-height:1.5">{reasoning}</div>'
                    f'</div>'
                )

        today_str  = datetime.now(ET).strftime("%Y-%m-%d")
        is_today   = (latest_date == today_str)
        banner_msg = (
            f"Today's picks — {latest_date} &nbsp;·&nbsp; Picks lock once games start"
            if is_today else
            f"Showing picks from {latest_date} &nbsp;·&nbsp; Next pipeline run at 6 am ET loads today's picks"
        )

        return (
            "<!DOCTYPE html><html lang='en'><head>"
            "<meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>Statalizers — {latest_date}</title>"
            "<style>"
            "*{box-sizing:border-box;margin:0;padding:0}"
            "body{background:#0d1117;color:#e6edf3;"
            "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;"
            "min-height:100vh;padding:0 0 40px}"
            ".banner{background:#1c2128;border-bottom:1px solid #30363d;"
            "padding:10px 20px;font-size:.85rem;color:#8b949e;text-align:center}"
            ".banner strong{color:#ffc107}"
            ".wrap{max-width:820px;margin:24px auto;padding:0 16px}"
            "h1{font-size:1.3rem;font-weight:700;margin-bottom:4px}"
            ".sub{color:#8b949e;font-size:.8rem;margin-bottom:20px}"
            "</style></head><body>"
            f"<div class='banner'>⚾ Statalizers &nbsp;|&nbsp; <strong>{banner_msg}</strong>"
            f" &nbsp;|&nbsp; {len(display)} picks</div>"
            "<div class='wrap'>"
            f"<h1>MLB Picks</h1>"
            f"<p class='sub'>{latest_date}</p>"
            + (cards_html or "<p style='color:#8b949e'>No picks to display.</p>")
            + "</div></body></html>"
        )

    except Exception as e:
        log.warning(f"_picks_html_from_db failed: {e}")
        return None


def _generate() -> "str | None":
    """
    Run the dashboard HTML generator and return the full HTML string, or None
    when no upcoming games are found (e.g. after today's games have all started).
    Returning None lets the caller decide whether to keep the existing cache rather
    than replacing a rich dashboard with a stripped-down fallback page.
    """
    log.info("Generating dashboard...")
    from run_picks_html import main as build_html
    html = build_html(date=None, no_open=True)
    if html:
        log.info("Dashboard generation complete.")
        return html
    log.info("Dashboard generation returned None — today's games have started or no schedule found.")
    return None


GENERATION_TIMEOUT = 4 * 60   # 4 minutes — if generation hangs past this, force-unblock


def _regenerate_in_background():
    """Kick off a background thread to refresh the cache without blocking requests."""
    def _worker():
        started = time.time()
        try:
            # Mid-day odds snapshot — every 2 hours between 8am-10pm ET
            # Builds the line movement data that powers the Sharp Money panel
            if _needs_odds_snapshot():
                _run_odds_snapshot()
            # Mid-day lineup refresh — after 10am when lineups post
            if _needs_lineup_refresh():
                _run_lineup_refresh()
            html = _generate()
            with _cache_lock:
                if html is not None:
                    # Fresh full dashboard — update the cache.
                    _cache["html"] = html
                    _cache["r2_seeded"] = False
                    log.info(f"Background cache refresh complete in {int(time.time()-started)}s.")
                elif _cache["html"] is not None:
                    # main() returned None (games started / no upcoming slate).
                    # If cache came from R2 seed (old code HTML), force a DB-based
                    # rebuild so new code template is applied immediately after deploy.
                    if _cache.get("r2_seeded"):
                        log.info("R2-seeded cache + None generation — forcing DB fallback with new code template.")
                        fallback = _picks_html_from_db()
                        if fallback:
                            _cache["html"] = fallback
                        _cache["r2_seeded"] = False
                    else:
                        log.info(
                            "Dashboard generation returned None — preserving existing cached "
                            f"dashboard ({int(time.time()-started)}s). Site stays populated."
                        )
                else:
                    # Nothing in cache and nothing generated — last resort DB fallback.
                    fallback = _picks_html_from_db()
                    _cache["html"] = fallback or "<h1>No picks available yet — check back soon.</h1>"
                    log.info("No cache and no dashboard — serving DB fallback picks page.")
                _cache["generated_at"] = time.time()
                _cache["generating"] = False
        except Exception as e:
            log.error(f"Background generation failed: {e}", exc_info=True)
            with _cache_lock:
                _cache["generating"] = False
        except BaseException as e:
            # Catches SystemExit, KeyboardInterrupt, etc. — always unblock the cache
            log.error(f"Background generation killed by BaseException: {e}")
            with _cache_lock:
                _cache["generating"] = False

    def _watchdog(worker_thread):
        """Kill the generating flag if the worker hangs past GENERATION_TIMEOUT."""
        worker_thread.join(timeout=GENERATION_TIMEOUT)
        if worker_thread.is_alive():
            log.error(
                f"Generation worker exceeded {GENERATION_TIMEOUT}s — force-clearing generating flag. "
                "Stale cache will be served until next refresh."
            )
            with _cache_lock:
                _cache["generating"] = False

    with _cache_lock:
        if _cache["generating"]:
            return
        _cache["generating"] = True

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    w = threading.Thread(target=_watchdog, args=(t,), daemon=True)
    w.start()


def get_cached_html() -> str:
    """
    Always returns immediately — never blocks a request.
    - Fresh cache: serve it.
    - Stale cache: serve stale, kick off background refresh.
    - Empty cache: return warming-up page (auto-refreshes every 30s).
    """
    now = time.time()
    with _cache_lock:
        age        = now - _cache["generated_at"]
        html       = _cache["html"]
        stale      = age > CACHE_TTL
        generating = _cache["generating"]

    if html is None:
        if not generating:
            _regenerate_in_background()
        return WARMING_HTML

    if stale and not generating:
        log.info(f"Cache is {int(age)}s old — serving stale, refreshing in background.")
        _regenerate_in_background()

    return html


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    resp = Response(get_cached_html(), content_type="text/html; charset=utf-8")
    resp.headers["Cache-Control"] = "public, max-age=300, stale-while-revalidate=60"
    return resp


@app.route("/refresh")
def force_refresh():
    """Force a background cache refresh and redirect home."""
    with _cache_lock:
        _cache["generated_at"] = 0
    _regenerate_in_background()
    return redirect("/")


@app.route("/force-odds")
def force_odds():
    """Force an immediate odds snapshot regardless of the 2-hour gate."""
    def _worker():
        _run_odds_snapshot()
        with _cache_lock:
            _cache["generated_at"] = 0   # force dashboard rebuild with new movement data
        _regenerate_in_background()
    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return {"status": "ok", "message": "Odds snapshot started — dashboard will refresh automatically in ~60 seconds."}


@app.route("/debug-odds")
def debug_odds():
    """Run odds scraper synchronously and return full diagnostic output."""
    import os as _os
    diag = {}
    # Check API key visibility
    key = _os.environ.get("ODDS_API_KEY", "").strip()
    diag["key_found"]   = bool(key)
    diag["key_preview"] = (key[:4] + "..." + key[-4:]) if len(key) > 8 else ("SET" if key else "MISSING")
    # Dump ALL env var names visible to the process
    diag["all_env_var_names"] = sorted(_os.environ.keys())
    diag["total_env_vars"]    = len(_os.environ)
    # Run scraper
    try:
        from scrapers.mlb_odds_scraper import run as run_odds
        result = run_odds()
        diag["scraper_result"] = result
    except Exception as e:
        import traceback
        diag["scraper_error"] = str(e)
        diag["traceback"]     = traceback.format_exc()
    # Check what got saved
    import csv as _csv
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    today = _dt.now(_ZI("America/New_York")).strftime("%Y-%m-%d")
    odds_path = os.path.join(CLEAN_DIR, "mlb_odds_master.csv")
    if os.path.exists(odds_path):
        with open(odds_path) as f:
            rows = [r for r in _csv.DictReader(f) if r.get("game_date") == today]
        diag["rows_saved_today"] = len(rows)
        diag["snap_times"] = list(set(r.get("snapshot_time","") for r in rows))
    else:
        diag["rows_saved_today"] = 0
        diag["odds_file_exists"] = False
    return diag


@app.route("/unstick")
def unstick():
    """
    Emergency reset — force-clears the generating flag and resets the cache timer.
    Use this if the dashboard is stuck on 'warming up' and won't recover on its own.
    Visit /unstick then wait ~30 seconds and reload the home page.
    """
    with _cache_lock:
        was_generating = _cache["generating"]
        _cache["generating"] = False
        _cache["generated_at"] = 0   # forces a fresh regeneration
    log.warning(f"/unstick called — generating was {was_generating}, cache reset.")
    _regenerate_in_background()
    return {
        "status": "ok",
        "was_stuck": was_generating,
        "message": "Cache reset. Dashboard is regenerating — reload the home page in ~60 seconds.",
    }


@app.route("/force-pipeline")
def force_pipeline():
    """
    Emergency pipeline trigger — runs the full data pipeline on THIS Railway container.
    Use when the DB says pipeline ran today but the schedule data is missing/corrupt.
    Dashboard rebuilds automatically when the pipeline finishes (~10-15 min).
    Visit /force-pipeline, wait 15 minutes, then reload the home page.
    """
    def _worker():
        log.warning("/force-pipeline triggered — running full pipeline regardless of DB state.")
        _run_full_pipeline()
        # Force dashboard rebuild with fresh data
        with _cache_lock:
            _cache["generated_at"] = 0
        _regenerate_in_background()
        log.warning("/force-pipeline complete — dashboard rebuilding.")

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return {
        "status": "ok",
        "message": (
            "Full pipeline started on Railway container. "
            "Wait ~15 minutes then reload statalizers.com. "
            "Check /status to monitor progress."
        ),
    }



@app.route("/force-html")
def force_html():
    """
    Force an immediate HTML rebuild using existing data — no scraping.
    Use after a code deploy with template changes when you want to see the
    new layout without waiting for the next pipeline run.
    Visit /force-html, wait ~60 seconds, then hard-refresh the home page.
    """
    with _cache_lock:
        _cache["generated_at"] = 0
        _cache["generating"]   = False
    _regenerate_in_background()
    return {
        "status":  "ok",
        "message": (
            "Dashboard is rebuilding from existing data. "
            "Wait ~60 seconds then hard-refresh statalizers.com."
        ),
    }


@app.route("/health")
def health():
    with _cache_lock:
        age        = int(time.time() - _cache["generated_at"])
        generating = _cache["generating"]
    return {
        "status":            "ok",
        "cache_age_seconds": age,
        "regenerating":      generating,
        "date":              datetime.now(ET).strftime("%Y-%m-%d %H:%M ET"),
    }


@app.route("/status")
def status():
    """Friendly HTML status page — pipeline health at a glance."""
    import csv as _csv

    now   = datetime.now(ET)
    today = now.strftime("%Y-%m-%d")

    # ── Pipeline last run ────────────────────────────────────────────────────
    # Pipeline runs on Railway at 6am ET daily. Check DB first (authoritative),
    # then fall back to schedule CSV check.
    pipeline_ok    = None
    pipeline_label = "Scheduled for 6am ET — data pending"

    if _DB_AVAILABLE:
        try:
            if _db_pipeline_ran_today():
                pipeline_ok    = True
                pipeline_label = f"Ran today on Railway ✓"
            else:
                pipeline_ok    = False
                pipeline_label = "Scheduled for 6am ET — data pending"
        except Exception:
            pass   # fall through to CSV check

    if pipeline_ok is None:
        # DB unavailable — fall back to schedule CSV as proxy
        sched_path = os.path.join(CLEAN_DIR, "mlb_schedule_master.csv")
        if os.path.exists(sched_path):
            try:
                with open(sched_path, encoding="utf-8") as f:
                    has_today = any(r.get("game_date") == today for r in _csv.DictReader(f))
                pipeline_ok    = has_today
                pipeline_label = "Ran today on Railway ✓" if has_today else "Scheduled for 6am ET — data pending"
            except Exception:
                pipeline_label = "Scheduled for 6am ET — status unknown"

    # ── Odds snapshots today ─────────────────────────────────────────────────
    odds_path  = os.path.join(CLEAN_DIR, "mlb_odds_master.csv")
    snap_times = []
    if os.path.exists(odds_path):
        try:
            with open(odds_path, encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    if row.get("game_date") == today:
                        t = row.get("snapshot_time", "")
                        if t and t not in snap_times:
                            snap_times.append(t)
            snap_times = sorted(set(snap_times))
        except Exception:
            pass

    def fmt_snap(t):
        try:
            from datetime import timezone as _tz
            dt = datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_tz.utc)
            return dt.astimezone(ET).strftime("%-I:%M %p ET")
        except Exception:
            return t

    snap_count  = len(snap_times)
    snap_labels = [fmt_snap(t) for t in snap_times]
    snaps_ok    = snap_count >= 2
    snaps_str   = ", ".join(snap_labels) if snap_labels else "None yet today"

    # ── Line movement ────────────────────────────────────────────────────────
    mv_path = os.path.join(CLEAN_DIR, f"mlb_line_movement_{today}.csv")
    mv_ok   = os.path.exists(mv_path)
    mv_rows = 0
    if mv_ok:
        try:
            with open(mv_path, encoding="utf-8") as f:
                mv_rows = sum(
                    1 for r in _csv.DictReader(f)
                    if r.get("ml_signal") in ("STEAM", "DRIFT")
                    or r.get("total_signal") in ("STEAM", "DRIFT")
                )
        except Exception:
            pass
    mv_label = f"{mv_rows} game(s) with notable movement" if mv_ok else "Not yet — need 2+ snapshots"

    # ── Hitter stats ─────────────────────────────────────────────────────────
    stats_path = os.path.join(BASE_DIR, "data", "raw", f"mlb_hitter_stats_{today}.json")
    if os.path.exists(stats_path):
        age_secs    = time.time() - os.path.getmtime(stats_path)
        age_hrs     = age_secs / 3600
        stats_ok    = age_hrs < 6
        stats_label = f"Fetched {int(age_hrs)}h {int((age_hrs % 1)*60)}m ago"
    else:
        stats_ok    = False
        stats_label = "Not fetched yet — will pull once lineups confirm"

    # ── Dashboard cache ──────────────────────────────────────────────────────
    with _cache_lock:
        cache_age  = int(time.time() - _cache["generated_at"])
        generating = _cache["generating"]
        has_cache  = _cache["html"] is not None

    cache_mins  = cache_age // 60
    cache_secs  = cache_age % 60
    cache_ok    = has_cache and cache_age < 900
    if generating:
        cache_label = "Generating now…"
    elif has_cache:
        cache_label = f"Generated {cache_mins}m {cache_secs}s ago"
    else:
        cache_label = "Not yet generated"

    # ── Next snapshot ────────────────────────────────────────────────────────
    if snap_times:
        try:
            from datetime import timezone as _tz2
            last_t  = datetime.strptime(snap_times[-1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_tz2.utc)
            next_t  = last_t.astimezone(ET) + timedelta(hours=2)
            if next_t <= now:
                next_label = "Overdue — fires on next dashboard visit"
            else:
                next_label = next_t.strftime("%-I:%M %p ET")
        except Exception:
            next_label = "~2 hours after last snapshot"
    else:
        next_label = "Waiting for first snapshot"

    # ── Render ───────────────────────────────────────────────────────────────
    def status_row(label, value, ok=None, detail=""):
        if ok is True:    dot = '<span style="color:#00e676">●</span>'
        elif ok is False: dot = '<span style="color:#ef5350">●</span>'
        else:             dot = '<span style="color:#7a8899">●</span>'
        detail_html = (f'<div style="font-size:.72rem;color:#7a8899;margin-top:3px">{detail}</div>'
                       if detail else "")
        return f"""<div style="display:flex;justify-content:space-between;align-items:flex-start;
          padding:14px 20px;border-bottom:1px solid #1e2d44">
          <div style="color:#7a8899;font-size:.82rem;font-weight:600;min-width:180px">{label}</div>
          <div style="text-align:right">
            <div style="display:flex;align-items:center;gap:8px;justify-content:flex-end">
              {dot}<span style="color:#e2e8f0;font-size:.88rem;font-weight:600">{value}</span>
            </div>{detail_html}
          </div></div>"""

    html = f"""<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<meta http-equiv="refresh" content="30"/>
<title>Statalizers Status</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet"/>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#07090f;color:#e2e8f0;font-family:'Inter',sans-serif;min-height:100vh;padding:40px 20px}}
.wrap{{max-width:660px;margin:0 auto}}
.hdr{{text-align:center;margin-bottom:36px}}
.title{{font-size:1.8rem;font-weight:800;background:linear-gradient(90deg,#00e676,#42a5f5);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
.sub{{color:#7a8899;font-size:.85rem;margin-top:6px}}
.card{{background:#111827;border:1px solid #1e2d44;border-radius:12px;margin-bottom:20px;overflow:hidden}}
.card-hdr{{padding:12px 20px;font-size:.7rem;font-weight:700;text-transform:uppercase;
  letter-spacing:1px;color:#7a8899;border-bottom:1px solid #1e2d44;background:#0d1117}}
.footer{{text-align:center;margin-top:24px}}
.back{{color:#00e676;text-decoration:none;font-size:.85rem;font-weight:600}}
.force-btn{{display:inline-block;margin-left:16px;padding:9px 22px;
  background:linear-gradient(135deg,#00e676,#00b248);border-radius:24px;
  color:#000;font-weight:800;font-size:.83rem;text-decoration:none;
  cursor:pointer;border:none}}
.note{{color:#7a8899;font-size:.7rem;margin-top:12px}}
</style></head><body>
<div class="wrap">
  <div class="hdr">
    <div class="title">⚾ Statalizers Status</div>
    <div class="sub">As of {now.strftime("%-I:%M %p ET, %A %B %-d")}</div>
  </div>

  <div class="card">
    <div class="card-hdr">Pipeline</div>
    {status_row("Daily Pipeline", pipeline_label, pipeline_ok)}
    {status_row("Dashboard Cache", cache_label, cache_ok, "Auto-refreshes every 10 minutes")}
  </div>

  <div class="card">
    <div class="card-hdr">Odds &amp; Line Movement</div>
    {status_row("Snapshots Today", f"{snap_count} snapshot{'s' if snap_count != 1 else ''}",
                snaps_ok, snaps_str)}
    {status_row("Next Snapshot", next_label, None, "Every 2 hours, 8am–10pm ET")}
    {status_row("Line Movement File", mv_label, mv_ok if snap_count >= 2 else None)}
  </div>

  <div class="card">
    <div class="card-hdr">Lineups &amp; Props</div>
    {status_row("Hitter Stats", stats_label, stats_ok,
                "Re-fetched when lineups confirm and file is 4+ hours old")}
  </div>

  <div class="footer">
    <a href="/" class="back">← Dashboard</a>
    <button class="force-btn" onclick="forceOdds(this)">🔄 Force Odds Snapshot</button>
    <div class="note">Page auto-refreshes every 30 seconds</div>
  </div>
</div>
<script>
function forceOdds(btn) {{
  btn.textContent = '⏳ Snapshot running...';
  btn.disabled = true;
  fetch('/force-odds')
    .then(r => r.json())
    .then(() => {{
      btn.textContent = '✅ Snapshot started — refreshing in 60s';
      setTimeout(() => location.reload(), 60000);
    }})
    .catch(() => {{
      btn.textContent = '❌ Error — try again';
      btn.disabled = false;
    }});
}}
</script>
</body></html>"""

    return Response(html, content_type="text/html; charset=utf-8")


# ── Performance / backtesting routes ─────────────────────────────────────────

@app.route("/performance")
def performance():
    """Browser → visual dashboard. API clients (Accept: application/json) → JSON."""
    accept = request.headers.get("Accept", "")
    if "text/html" in accept:
        from flask import redirect
        return redirect("/performance-html", code=302)

    # JSON API path (backward compatible)
    try:
        days = int(request.args.get("days", 30))
    except (TypeError, ValueError):
        days = 30
    try:
        from db.picks_store import get_accuracy_summary
        rows = get_accuracy_summary(days=days)
    except Exception as e:
        return {"error": str(e)}, 500
    if not rows:
        return {"days": days, "rows": [], "message": "No graded picks in this window yet."}
    total_wins   = sum(r.get("wins",   0) or 0 for r in rows)
    total_losses = sum(r.get("losses", 0) or 0 for r in rows)
    total_pushes = sum(r.get("pushes", 0) or 0 for r in rows)
    denom        = total_wins + total_losses
    overall_wr   = round(total_wins / denom, 3) if denom > 0 else None
    return {
        "days":    days,
        "overall": {"wins": total_wins, "losses": total_losses,
                    "pushes": total_pushes, "win_rate": overall_wr},
        "rows": rows,
    }


@app.route("/performance-html")
def performance_html():
    """Human-readable backtesting dashboard."""
    try:
        days = int(request.args.get("days", 30))
    except (TypeError, ValueError):
        days = 30

    try:
        from db.picks_store import get_accuracy_summary
        rows = get_accuracy_summary(days=days) or []
    except Exception as e:
        rows = []

    try:
        from db.picks_store import get_accuracy_by_market_signal, get_monthly_accuracy
        mkt_signal_rows = get_accuracy_by_market_signal(days=days) or []
        monthly_rows    = get_monthly_accuracy() or []
    except Exception:
        mkt_signal_rows = []
        monthly_rows    = []

    # ── Yesterday picks ──────────────────────────────────
    yesterday_picks = []
    try:
        from datetime import timedelta
        from db.picks_store import get_picks
        _yday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_picks = get_picks(_yday) or []
    except Exception:
        yesterday_picks = []

    # ── Aggregate stats ───────────────────────────────────────────────────────
    total_w = sum(r.get("wins",   0) or 0 for r in rows)
    total_l = sum(r.get("losses", 0) or 0 for r in rows)
    total_p = sum(r.get("pushes", 0) or 0 for r in rows)
    denom   = total_w + total_l
    overall_wr_str = f"{total_w/denom*100:.1f}%" if denom > 0 else "—"

    # Break-even at -110 is 52.4%
    breakeven = 52.4
    wr_float  = (total_w / denom * 100) if denom > 0 else 0
    edge_str  = f"+{wr_float - breakeven:.1f}%" if wr_float >= breakeven else f"{wr_float - breakeven:.1f}%"
    edge_color = "#3fb950" if wr_float >= breakeven else "#f85149"

    # ── Day toggle links ──────────────────────────────────────────────────────
    days_links = "".join(
        '<a href="/performance-html?days={d}" {cls}>{d}d</a>'.format(
            d=d, cls='class="active"' if d == days else ""
        )
        for d in [7, 14, 30, 60, 90]
    )

    # ── Tier bar chart data ───────────────────────────────────────────────────
    TIER_ORDER = ["LOCK", "STRONG", "LEAN", "TOSSUP"]
    TIER_COLOR = {"LOCK": "#ffc107", "STRONG": "#42a5f5", "LEAN": "#66bb6a", "TOSSUP": "#a09ae0"}

    tier_agg = {}
    for r in rows:
        t = r.get("tier", "")
        if t not in tier_agg:
            tier_agg[t] = {"wins": 0, "losses": 0, "avg_conf": 0, "n": 0}
        tier_agg[t]["wins"]    += r.get("wins",   0) or 0
        tier_agg[t]["losses"]  += r.get("losses", 0) or 0
        tier_agg[t]["avg_conf"] += float(r.get("avg_conf") or 0)
        tier_agg[t]["n"]       += 1

    tier_bars_html = ""
    for t in TIER_ORDER:
        if t not in tier_agg: continue
        td = tier_agg[t]
        d2 = td["wins"] + td["losses"]
        wr2 = td["wins"] / d2 * 100 if d2 > 0 else 0
        bar_w = max(4, min(100, wr2))
        color = TIER_COLOR.get(t, "#888")
        record = f"{td['wins']}-{td['losses']}"
        wr_label = f"{wr2:.1f}%"
        tier_bars_html += f"""
        <div class="tier-bar-row">
          <div class="tier-bar-label" style="color:{color}">{t}</div>
          <div class="tier-bar-track">
            <div class="tier-bar-fill" style="width:{bar_w}%;background:{color}"></div>
            <div class="tier-bar-be"></div>
          </div>
          <div class="tier-bar-stat">{wr_label} &nbsp;<span class="tier-bar-record">({record})</span></div>
        </div>"""

    # ── Detail table rows ─────────────────────────────────────────────────────
    primary_rows = ""
    secondary_rows = ""
    for t in TIER_ORDER:
        tier_rows = [r for r in rows if r.get("tier") == t]
        for r in tier_rows:
            wins    = r.get("wins",   0) or 0
            losses  = r.get("losses", 0) or 0
            pushes  = r.get("pushes", 0) or 0
            pending = r.get("pending", 0) or 0
            d3      = wins + losses
            wr3     = f"{wins/d3*100:.1f}%" if d3 > 0 else "—"
            avg_c   = f"{float(r.get('avg_conf') or 0)*100:.1f}%"
            color   = TIER_COLOR.get(t, "#8b949e")
            row_html = (
                f"<tr><td style='color:{color};font-weight:600'>{t}</td>"
                f"<td>{r.get('pick_type','')}</td>"
                f"<td style='color:#3fb950'>{wins}</td>"
                f"<td style='color:#f85149'>{losses}</td>"
                f"<td style='color:#8b949e'>{pushes}</td>"
                f"<td><strong>{wr3}</strong></td>"
                f"<td style='color:#8b949e'>{avg_c}</td></tr>\n"
            )
            if t in ("LOCK", "STRONG"):
                primary_rows += row_html
            else:
                secondary_rows += row_html

    empty_msg = (
        "<p style='color:#8b949e;padding:20px 0'>No graded picks yet for this window.</p>"
        if not rows else ""
    )

    try:
        from db.picks_store import get_prop_accuracy, get_player_prop_accuracy
        prop_rows        = get_prop_accuracy(days=days) or []
        player_prop_rows = get_player_prop_accuracy(days=days) or []
    except Exception:
        prop_rows        = []
        player_prop_rows = []

    # -- Props section HTML ---------------------------------------------------
    _BREAKEVEN = 52.4
    _prop_total_picks = sum(int(r.get('total') or 0) for r in prop_rows)
    _prop_total_hits  = sum(int(r.get('hits')  or 0) for r in prop_rows)
    _prop_overall_hr  = (_prop_total_hits / _prop_total_picks * 100) if _prop_total_picks > 0 else 0
    _prop_overall_str = f'{_prop_overall_hr:.1f}%' if _prop_total_picks > 0 else '---'

    __best  = max(prop_rows, key=lambda r: float(r.get('hit_rate') or 0), default=None)
    __worst = min(prop_rows, key=lambda r: float(r.get('hit_rate') or 0), default=None)
    _best_prop_str  = (f"{__best['prop_type']} — {float(__best['hit_rate'])*100:.0f}%"
                      if __best else '---')
    _worst_prop_str = (f"{__worst['prop_type']} — {float(__worst['hit_rate'])*100:.0f}%"
                      if __worst else '---')

    _prop_type_rows = []
    for _r in prop_rows:
        _pt    = _r.get('prop_type', '')
        _tot   = int(_r.get('total') or 0)
        _hits  = int(_r.get('hits')  or 0)
        _hrv   = float(_r.get('hit_rate') or 0) * 100
        _hrs   = f'{_hrv:.1f}%' if _tot > 0 else '---'
        _vsbe  = _hrv - _BREAKEVEN
        _vsbes = f'+{_vsbe:.1f}%' if _vsbe >= 0 else f'{_vsbe:.1f}%'
        _cc    = '#3fb950' if _vsbe >= 0 else '#f85149'
        _ac    = float(_r.get('avg_conf') or 0) * 100
        _prop_type_rows.append(
            f'<tr><td style="font-weight:600">{_pt}</td>'
            f'<td>{_tot}</td><td>{_hits}</td>'
            f'<td><strong>{_hrs}</strong></td>'
            f'<td style="color:{_cc}">{_vsbes}</td>'
            f'<td style="color:#8b949e">{_ac:.1f}%</td></tr>'
        )
    _prop_type_body = ''.join(_prop_type_rows) or (
        '<tr><td colspan="6" style="color:#8b949e;padding:14px">No graded props yet.</td></tr>'
    )

    _player_rows = []
    for _r in player_prop_rows[:30]:
        _pn   = _r.get('player_name', '')
        _pt   = _r.get('prop_type', '')
        _tot  = int(_r.get('total') or 0)
        _hrv  = float(_r.get('hit_rate') or 0) * 100
        _hrs  = f'{_hrv:.1f}%'
        _ac   = float(_r.get('avg_conf') or 0) * 100
        _hc   = '#3fb950' if _hrv >= _BREAKEVEN else '#f85149'
        _player_rows.append(
            f'<tr><td>{_pn}</td><td style="color:#8b949e">{_pt}</td>'
            f'<td>{_tot}</td>'
            f'<td style="color:{_hc};font-weight:600">{_hrs}</td>'
            f'<td style="color:#8b949e">{_ac:.1f}%</td></tr>'
        )
    _player_body = ''.join(_player_rows) or (
        '<tr><td colspan="5" style="color:#8b949e;padding:14px">Need 5+ picks per player to show.</td></tr>'
    )

    _props_section_html = (
        '<div class="secondary-section" style="margin-top:32px">'
        '<div class="secondary-toggle" '
        'onclick="var b=this.nextElementSibling;b.classList.toggle(\'open\');'
        'this.querySelector(\'.arr\').textContent=b.classList.contains(\'open\')?\'&#9660;\':\'&#9654;\'">'
        '<span class="arr">&#9654;</span> Player Props Performance'
        '</div><div class="secondary-body">'
        f'<div class="stat-grid" style="margin-top:14px">'
        f'<div class="stat-card"><div class="stat-val">{_prop_overall_str}</div>'
        f'<div class="stat-lbl">Props Hit Rate</div></div>'
        f'<div class="stat-card"><div class="stat-val" style="font-size:1.05rem">{_best_prop_str}</div>'
        f'<div class="stat-lbl">Best Prop Type</div></div>'
        f'<div class="stat-card"><div class="stat-val" style="font-size:1.05rem">{_worst_prop_str}</div>'
        f'<div class="stat-lbl">Worst Prop Type</div></div>'
        f'<div class="stat-card"><div class="stat-val">{_prop_total_picks}</div>'
        f'<div class="stat-lbl">Total Props Graded</div></div>'
        f'</div>'
        f'<div class="table-card" style="margin-top:14px">'
        f'<div class="section-title" style="padding:12px 14px 0">Hit Rate by Prop Type</div>'
        f'<table><thead><tr><th>Prop</th><th>Picks</th><th>Hits</th>'
        f'<th>Hit Rate</th><th>vs Break-even</th><th>Avg Conf</th></tr></thead>'
        f'<tbody>{_prop_type_body}</tbody></table></div>'
        '<div class="secondary-section" style="margin-top:12px">'
        '<div class="secondary-toggle" '
        'onclick="var b=this.nextElementSibling;b.classList.toggle(\'open\');'
        'this.querySelector(\'.arr\').textContent=b.classList.contains(\'open\')?\'&#9660;\':\'&#9654;\'">'
        '<span class="arr">&#9654;</span> Top Players (5+ picks)</div>'
        '<div class="secondary-body"><div class="table-card"><table>'
        '<thead><tr><th>Player</th><th>Prop</th><th>Picks</th>'
        '<th>Hit Rate</th><th>Avg Conf</th></tr></thead>'
        f'<tbody>{_player_body}</tbody></table></div></div></div>'
        '</div></div>'
    )

    # ── Market Signal section HTML ─────────────────────────────────────────
    _MKT_SIGNAL_ORDER = ["CONFIRM", "DIVERGE", "NEUTRAL"]
    _MKT_SIGNAL_COLOR = {"CONFIRM": "#3fb950", "DIVERGE": "#f85149", "NEUTRAL": "#8b949e"}
    _MKT_SIGNAL_ICON  = {"CONFIRM": "&#10003;", "DIVERGE": "&#10007;", "NEUTRAL": "&mdash;"}
    _mkt_index = {r.get("market_signal"): r for r in mkt_signal_rows}
    _mkt_rows_html = ""
    for _sig in _MKT_SIGNAL_ORDER:
        _rd  = _mkt_index.get(_sig, {})
        _w4  = int(_rd.get("wins",   0) or 0)
        _l4  = int(_rd.get("losses", 0) or 0)
        _p4  = int(_rd.get("pushes", 0) or 0)
        _d4  = _w4 + _l4
        _wr4 = f"{_w4/_d4*100:.1f}%" if _d4 > 0 else "&mdash;"
        _ac4 = f"{float(_rd.get('avg_conf') or 0)*100:.1f}%" if _rd else "&mdash;"
        _clr = _MKT_SIGNAL_COLOR.get(_sig, "#8b949e")
        _ico = _MKT_SIGNAL_ICON.get(_sig, "")
        _mkt_rows_html += (
            f"<tr><td style='color:{_clr};font-weight:600'>{_ico} {_sig}</td>"
            f"<td style='color:#3fb950'>{_w4}</td>"
            f"<td style='color:#f85149'>{_l4}</td>"
            f"<td style='color:#8b949e'>{_p4}</td>"
            f"<td><strong>{_wr4}</strong></td>"
            f"<td style='color:#8b949e'>{_ac4}</td></tr>\n"
        )
    _no_mkt = "<tr><td colspan='6' style='color:#8b949e;padding:14px'>No graded data yet.</td></tr>"
    _mkt_section_html = (
        '<div class="secondary-section" style="margin-top:24px">'
        '<div class="secondary-toggle" '
        'onclick="var b=this.nextElementSibling;b.classList.toggle(\'open\');'
        'this.querySelector(\'.arr\').textContent=b.classList.contains(\'open\')?\'&#9660;\':\'&#9654;\'">'
        '<span class="arr">&#9654;</span> Market Signal Breakdown (Kalshi / Polymarket)'
        '</div><div class="secondary-body">'
        '<p style="color:#8b949e;font-size:.75rem;margin:10px 0">'
        'CONFIRM = markets agreed with model &nbsp;&middot;&nbsp; '
        'DIVERGE = markets disagreed &nbsp;&middot;&nbsp; '
        'NEUTRAL = no market data</p>'
        '<div class="table-card">'
        '<table><thead><tr>'
        '<th>Signal</th><th>W</th><th>L</th><th>Push</th><th>Win %</th><th>Avg Conf</th>'
        '</tr></thead>'
        f'<tbody>{_mkt_rows_html or _no_mkt}</tbody>'
        '</table></div></div></div>'
    )

    # ── Monthly Summary section HTML ───────────────────────────────────────
    _monthly_rows_html = ""
    for _mr in monthly_rows:
        _mn5 = _mr.get("month", "")
        _w5  = int(_mr.get("wins",   0) or 0)
        _l5  = int(_mr.get("losses", 0) or 0)
        _p5  = int(_mr.get("pushes", 0) or 0)
        _d5  = _w5 + _l5
        _wr5 = f"{_w5/_d5*100:.1f}%" if _d5 > 0 else "&mdash;"
        _ac5 = f"{float(_mr.get('avg_conf') or 0)*100:.1f}%" if _mr else "&mdash;"
        _e5  = (_w5/_d5*100 - 52.4) if _d5 > 0 else None
        _es5 = (f"+{_e5:.1f}%" if _e5 >= 0 else f"{_e5:.1f}%") if _e5 is not None else "&mdash;"
        _ec5 = "#3fb950" if (_e5 is not None and _e5 >= 0) else "#f85149"
        _monthly_rows_html += (
            f"<tr><td style='font-weight:600'>{_mn5}</td>"
            f"<td style='color:#3fb950'>{_w5}</td>"
            f"<td style='color:#f85149'>{_l5}</td>"
            f"<td style='color:#8b949e'>{_p5}</td>"
            f"<td><strong>{_wr5}</strong></td>"
            f"<td style='color:{_ec5}'>{_es5}</td>"
            f"<td style='color:#8b949e'>{_ac5}</td></tr>\n"
        )
    _no_monthly = "<tr><td colspan='7' style='color:#8b949e;padding:14px'>No graded data yet.</td></tr>"
    _monthly_section_html = (
        '<div class="secondary-section" style="margin-top:24px">'
        '<div class="secondary-toggle" '
        'onclick="var b=this.nextElementSibling;b.classList.toggle(\'open\');'
        'this.querySelector(\'.arr\').textContent=b.classList.contains(\'open\')?\'&#9660;\':\'&#9654;\'">'
        '<span class="arr">&#9654;</span> Monthly Performance Summary'
        '</div><div class="secondary-body">'
        '<div class="table-card" style="margin-top:10px">'
        '<table><thead><tr>'
        '<th>Month</th><th>W</th><th>L</th><th>Push</th>'
        '<th>Win %</th><th>Edge vs -110</th><th>Avg Conf</th>'
        '</tr></thead>'
        f'<tbody>{_monthly_rows_html or _no_monthly}</tbody>'
        '</table></div></div></div>'
    )

    # ── Yesterday section HTML ───────────────────────────────────
    _TIER_ORDER_Y = ["LOCK", "STRONG", "LEAN", "TOSSUP"]
    _TIER_COLOR_Y = {"LOCK": "#ffc107", "STRONG": "#42a5f5", "LEAN": "#66bb6a", "TOSSUP": "#a09ae0"}
    _graded_yday  = [p for p in yesterday_picks if p.get("actual_result") in ("WIN", "LOSS", "PUSH")]
    _yday_w       = sum(1 for p in _graded_yday if p.get("actual_result") == "WIN")
    _yday_l       = sum(1 for p in _graded_yday if p.get("actual_result") == "LOSS")
    _yday_p       = sum(1 for p in _graded_yday if p.get("actual_result") == "PUSH")

    # Group by tier + pick_type
    _yday_groups = {}
    for _py in _graded_yday:
        _key = (_py.get("tier", ""), _py.get("pick_type", ""))
        if _key not in _yday_groups:
            _yday_groups[_key] = {"w": 0, "l": 0, "p": 0}
        _res = _py.get("actual_result")
        if _res == "WIN":   _yday_groups[_key]["w"] += 1
        elif _res == "LOSS": _yday_groups[_key]["l"] += 1
        elif _res == "PUSH": _yday_groups[_key]["p"] += 1

    _yday_badges = []
    for _tier in _TIER_ORDER_Y:
        for _ptype in ["ML", "TOTAL", "RL"]:
            _gd = _yday_groups.get((_tier, _ptype))
            if not _gd:
                continue
            _clr = _TIER_COLOR_Y.get(_tier, "#8b949e")
            _rec = f"{_gd['w']}-{_gd['l']}"
            if _gd["p"]:
                _rec += f" P{_gd['p']}"
            _yday_badges.append(
                f'<span style="background:#161b22;border:1px solid {_clr}33;' +
                f'border-radius:8px;padding:6px 12px;font-size:.8rem;white-space:nowrap">' +
                f'<span style="color:{_clr};font-weight:700">{_tier}</span> ' +
                f'<span style="color:#8b949e">{_ptype}</span> ' +
                f'<span style="font-weight:600">{_rec}</span></span>'
            )

    if _graded_yday:
        _yday_wr  = _yday_w / (_yday_w + _yday_l) * 100 if (_yday_w + _yday_l) > 0 else 0
        _yday_hdr_color = "#3fb950" if _yday_wr >= 52.4 else "#f85149"
        _yday_record_str = f"{_yday_w}-{_yday_l}"
        if _yday_p:
            _yday_record_str += f" ({_yday_p} push)"
        _badges_html = " ".join(_yday_badges)
        _yday_content = (
            f'<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:10px">' +
            f'<span style="font-size:1.3rem;font-weight:700;color:{_yday_hdr_color}">{_yday_record_str}</span>' +
            f'<span style="color:#8b949e;font-size:.8rem">{_yday_wr:.1f}% win rate</span>' +
            f'</div>' +
            f'<div style="display:flex;gap:8px;flex-wrap:wrap">{_badges_html}</div>'
        )
    else:
        _yday_content = '<p style="color:#8b949e;font-size:.83rem">No graded picks for yesterday yet — grades post after games finish.</p>'

    _yday_label = (datetime.now(ET) - timedelta(days=1)).strftime("%A, %b %-d") if yesterday_picks else "Yesterday"
    _yesterday_section_html = (
        '<div style="background:#161b22;border:1px solid #30363d;border-radius:10px;' +
        'padding:16px 20px;margin-bottom:24px">' +
        f'<div style="font-size:.72rem;font-weight:600;color:#8b949e;text-transform:uppercase;' +
        f'letter-spacing:.06em;margin-bottom:10px">📅 Yesterday — {_yday_label}</div>' +
        _yday_content +
        '</div>'
    )


    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Statalizers — Performance</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         background:#0d1117;color:#e6edf3;padding:28px 24px;max-width:900px;margin:0 auto}}
    h1{{font-size:1.3rem;font-weight:600;margin-bottom:4px}}
    .sub{{color:#8b949e;font-size:.83rem;margin-bottom:20px}}
    .days-nav{{display:flex;gap:6px;margin-bottom:24px;flex-wrap:wrap}}
    .days-nav a{{background:#161b22;border:1px solid #30363d;border-radius:20px;
                padding:5px 14px;font-size:.78rem;color:#8b949e;text-decoration:none;transition:all .15s}}
    .days-nav a:hover{{border-color:#58a6ff;color:#58a6ff}}
    .days-nav a.active{{background:#1f6feb;border-color:#388bfd;color:#fff;font-weight:600}}
    .stat-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px;margin-bottom:28px}}
    .stat-card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 16px}}
    .stat-val{{font-size:1.5rem;font-weight:700;line-height:1}}
    .stat-lbl{{font-size:.72rem;color:#8b949e;margin-top:5px}}
    .section-title{{font-size:.72rem;font-weight:600;color:#8b949e;text-transform:uppercase;
                    letter-spacing:.06em;margin:0 0 12px}}
    .chart-card{{background:#161b22;border:1px solid #30363d;border-radius:10px;
                 padding:16px 20px;margin-bottom:20px}}
    .tier-bar-row{{display:flex;align-items:center;gap:10px;margin-bottom:10px}}
    .tier-bar-label{{width:58px;font-size:.78rem;font-weight:600;flex-shrink:0}}
    .tier-bar-track{{flex:1;height:12px;background:#21262d;border-radius:6px;position:relative;overflow:visible}}
    .tier-bar-fill{{height:100%;border-radius:6px;transition:width .4s}}
    .tier-bar-be{{position:absolute;top:-3px;bottom:-3px;left:52.4%;width:1px;
                  background:rgba(255,255,255,.2)}}
    .tier-bar-stat{{font-size:.78rem;font-weight:600;width:56px;text-align:right;flex-shrink:0}}
    .tier-bar-record{{color:#8b949e;font-weight:400}}
    .table-card{{background:#161b22;border:1px solid #30363d;border-radius:10px;
                 overflow:hidden;margin-bottom:20px}}
    .table-card table{{width:100%;border-collapse:collapse;font-size:.83rem}}
    .table-card th{{padding:10px 14px;text-align:left;border-bottom:1px solid #30363d;
                    color:#8b949e;font-weight:600;font-size:.72rem;text-transform:uppercase;letter-spacing:.04em}}
    .table-card td{{padding:9px 14px;border-bottom:1px solid #21262d}}
    .table-card tr:last-child td{{border-bottom:none}}
    .table-card tr:hover td{{background:#0d1117}}
    .secondary-section{{margin-top:24px}}
    .secondary-toggle{{cursor:pointer;display:flex;align-items:center;gap:6px;
                       color:#8b949e;font-size:.78rem;margin-bottom:10px;user-select:none}}
    .secondary-toggle:hover{{color:#e6edf3}}
    .secondary-body{{display:none}}
    .secondary-body.open{{display:block}}
    .back-link{{color:#58a6ff;font-size:.8rem;text-decoration:none;display:inline-flex;
                align-items:center;gap:4px;margin-top:20px}}
    .back-link:hover{{text-decoration:underline}}
    .be-note{{font-size:.72rem;color:#8b949e;margin-top:8px}}
  </style>
</head>
<body>
  <div style="display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:4px">
    <h1>📊 Model Performance</h1>
    <a href="/" class="back-link">← Back to Picks</a>
  </div>
  <p class="sub">Last {days} days — graded picks only</p>

  {_yesterday_section_html}

  <div class="days-nav">{days_links}</div>

  {empty_msg}

  <div class="stat-grid">
    <div class="stat-card">
      <div class="stat-val">{total_w}–{total_l}</div>
      <div class="stat-lbl">Overall Record</div>
    </div>
    <div class="stat-card">
      <div class="stat-val">{overall_wr_str}</div>
      <div class="stat-lbl">Win Rate</div>
    </div>
    <div class="stat-card">
      <div class="stat-val" style="color:{edge_color}">{edge_str}</div>
      <div class="stat-lbl">Edge vs Break-even</div>
    </div>
    <div class="stat-card">
      <div class="stat-val">{total_p}</div>
      <div class="stat-lbl">Pushes</div>
    </div>
  </div>

  <div class="chart-card">
    <div class="section-title">Win rate by tier</div>
    {tier_bars_html}
    <div class="be-note">Vertical line = break-even at 52.4% (standard -110 juice)</div>
  </div>

  <div class="table-card">
    <div class="section-title" style="padding:12px 14px 0">Lock &amp; Strong — Primary Picks</div>
    <table>
      <thead><tr><th>Tier</th><th>Type</th><th>W</th><th>L</th><th>Push</th><th>Win %</th><th>Avg Conf</th></tr></thead>
      <tbody>{primary_rows if primary_rows else "<tr><td colspan='7' style='color:#8b949e;padding:14px'>No data yet</td></tr>"}</tbody>
    </table>
  </div>

  <div class="secondary-section">
    <div class="secondary-toggle" onclick="this.nextElementSibling.classList.toggle('open');this.querySelector('.arr').textContent=this.nextElementSibling.classList.contains('open')?'▼':'▶'">
      <span class="arr">▶</span> Lean &amp; Toss Up — Tracking Data
    </div>
    <div class="secondary-body">
      <div class="table-card">
        <table>
          <thead><tr><th>Tier</th><th>Type</th><th>W</th><th>L</th><th>Push</th><th>Win %</th><th>Avg Conf</th></tr></thead>
          <tbody>{secondary_rows if secondary_rows else "<tr><td colspan='7' style='color:#8b949e;padding:14px'>No data yet</td></tr>"}</tbody>
        </table>
      </div>
    </div>
  </div>

  {_props_section_html}

  {_mkt_section_html}

  {_monthly_section_html}

</body>
</html>"""

    return Response(html, content_type="text/html; charset=utf-8")


# ── Scheduled 6am ET daily pipeline ────────────────────────────────────────────
def _seconds_until_6am_et() -> float:
    """Return seconds until next 6:00am Eastern Time."""
    now    = datetime.now(ET)
    target = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _start_daily_scheduler():
    """Background thread that runs the full pipeline at 6am ET every day."""
    def _loop():
        while True:
            wait = _seconds_until_6am_et()
            log.info(f"Daily pipeline scheduled in {wait/3600:.1f}h (6am ET).")
            time.sleep(wait)
            log.info("=== 6am ET scheduled pipeline starting ===")
            _run_full_pipeline()
            # Force dashboard to rebuild with fresh data
            with _cache_lock:
                _cache["generated_at"] = 0
            _regenerate_in_background()

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


def _run_afternoon_refresh():
    """Re-run lineup + hitter + odds + umpire + bullpen fatigue scrapers and rebuild dashboard."""
    today = datetime.now(ET).strftime("%Y-%m-%d")
    log.info("=== Adaptive afternoon refresh starting ===")

    # Grade yesterday's picks first so Yesterday panel is ready
    try:
        yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
        from run_analysis import run as grade_picks
        grade_picks(yesterday)
        log.info(f"Afternoon grading complete: {yesterday}")
    except Exception as e:
        log.warning(f"Afternoon grading failed (non-fatal): {e}")

    # Refresh probable pitchers (rotation changes, scratches post at any time)
    # Upserts schedule master CSV so stale 6am assignments get corrected before scoring
    try:
        from scrapers.mlb_scraper import fetch_schedule
        from normalize.mlb_normalize import upsert_schedule_pitchers
        fresh_sched = fetch_schedule(days_ahead=1)   # today + tomorrow
        n_sp = upsert_schedule_pitchers(fresh_sched)
        log.info(f"Afternoon probable pitchers refreshed: {n_sp} game(s) updated/inserted")
    except Exception as e:
        log.warning(f"Afternoon pitcher refresh failed (non-fatal): {e}")

    # Refresh odds
    try:
        from scrapers.mlb_odds_scraper import run as run_odds
        run_odds()
        log.info("Afternoon odds refresh complete")
    except Exception as e:
        log.warning(f"Afternoon odds refresh failed (non-fatal): {e}")

    # Refresh umpires
    try:
        from scrapers.mlb_umpire_scraper import run as run_umps
        run_umps(target_date=today)
        log.info("Afternoon umpire refresh complete")
    except Exception as e:
        log.warning(f"Afternoon umpire refresh failed (non-fatal): {e}")

    # Refresh bullpen fatigue
    try:
        from scrapers.mlb_bullpen_fatigue_scraper import run as run_fatigue
        run_fatigue(target_date=today)
        log.info("Afternoon bullpen fatigue refresh complete")
    except Exception as e:
        log.warning(f"Afternoon bullpen fatigue refresh failed (non-fatal): {e}")

    # Refresh Kalshi + Polymarket snapshots
    try:
        from scrapers.mlb_kalshi_scraper import run as run_kalshi
        run_kalshi(target_date=today)
        log.info("Afternoon Kalshi refresh complete")
    except Exception as e:
        log.warning(f"Afternoon Kalshi refresh failed (non-fatal): {e}")

    try:
        from scrapers.mlb_polymarket_scraper import run as run_polymarket
        run_polymarket(target_date=today)
        log.info("Afternoon Polymarket refresh complete")
    except Exception as e:
        log.warning(f"Afternoon Polymarket refresh failed (non-fatal): {e}")

    # Refresh lineups + hitter stats
    try:
        from scrapers.mlb_lineup_scraper import run as run_lineups
        lineups = run_lineups(target_date=today)
        confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
        log.info(f"Afternoon lineups: {len(lineups)} games, {confirmed} confirmed")
        if confirmed > 0:
            from scrapers.mlb_hitter_scraper import run as run_hitters
            run_hitters(target_date=today)
            log.info("Afternoon hitter stats refreshed")
        else:
            log.info("No confirmed lineups yet — dashboard will retry automatically")
    except Exception as e:
        log.warning(f"Afternoon lineup/hitter refresh failed (non-fatal): {e}")

    # Rebuild dashboard
    with _cache_lock:
        _cache["generated_at"] = 0
    _regenerate_in_background()
    log.info("=== Afternoon refresh complete — dashboard rebuilding ===")


def _schedule_adaptive_refresh():
    """
    Schedule today's afternoon refresh to fire 2 hours before the first pitch.
    Called once at the end of the 6am pipeline. One-shot fire -- no recurring loop.
    Replaces the hardcoded 11:30am scheduler and the every-2-hour odds loop.
    API usage: 1 odds pull/day (~30/month) vs old ~7/day (~210/month).
    """
    import csv as _csv
    from datetime import timezone as _tz

    today      = datetime.now(ET).strftime("%Y-%m-%d")
    sched_path = os.path.join(CLEAN_DIR, "mlb_schedule_master.csv")

    earliest_et = None
    try:
        with open(sched_path, encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                if row.get("game_date") != today:
                    continue
                utc_str = row.get("game_time_utc", "").strip()
                if not utc_str:
                    continue
                try:
                    game_utc = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_tz.utc)
                    game_et  = game_utc.astimezone(ET)
                    if earliest_et is None or game_et < earliest_et:
                        earliest_et = game_et
                except ValueError:
                    continue
    except Exception as e:
        log.warning(f"Adaptive refresh: could not read schedule CSV: {e}")

    if earliest_et is None:
        log.warning("Adaptive refresh: no game times found for today -- refresh not scheduled.")
        return

    target_et = earliest_et - timedelta(hours=2)
    now_et    = datetime.now(ET)
    wait_secs = (target_et - now_et).total_seconds()

    if wait_secs < 0:
        if now_et < earliest_et:
            # Window passed but first pitch hasn't started -- run immediately
            log.info(
                f"Adaptive refresh target already passed -- running immediately "
                f"(first pitch {earliest_et.strftime('%I:%M %p ET')})"
            )
            wait_secs = 0
        else:
            log.info("Adaptive refresh: first pitch already started -- skipping.")
            return

    log.info(
        f"Adaptive refresh scheduled for {target_et.strftime('%I:%M %p ET')} "
        f"(first pitch {earliest_et.strftime('%I:%M %p ET')}, "
        f"{wait_secs/3600:.1f}h from now)"
    )

    def _fire():
        if wait_secs > 0:
            time.sleep(wait_secs)
        log.info("=== Adaptive refresh firing -- 2 hours before first pitch ===")
        _run_afternoon_refresh()

    t = threading.Thread(target=_fire, daemon=True)
    t.start()



# ── Startup ─────────────────────────────────────────────────────────────────────────────────────
def warm_cache():
    """
    On startup:
    1. Create DB schema (idempotent -- safe every boot)
    2. Download CSVs from object storage (so model has data after a fresh deploy)
    3. Run pipeline if today's data is missing
    4. Build dashboard cache
    """
    def _warm():
        time.sleep(2)   # let Flask finish binding first

        # ── Step 1: DB schema ──────────────────────────────────────────────────────────────────────────────────
        if _DB_AVAILABLE:
            try:
                _db_create_all()
            except Exception as e:
                log.warning(f"DB schema init failed (non-fatal): {e}")

        # ── Step 2: CSV sync download ─────────────────────────────────────────────────────────────────────────────
        if _DB_AVAILABLE:
            try:
                if _storage_ok():
                    log.info("Object storage detected -- downloading CSV snapshots...")
                    n = _csv_download()
                    if n > 0:
                        log.info(f"Startup CSV sync: {n} file(s) downloaded from storage.")
                    else:
                        log.info("CSV sync: local files are current (nothing to download).")
                else:
                    log.debug("Object storage not configured -- skipping CSV sync.")
            except Exception as e:
                log.warning(f"Startup CSV sync failed (non-fatal): {e}")

        # ── Step 3: Pipeline ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
        if _needs_pipeline_run():
            log.info("No pipeline data for today -- running full pipeline on startup...")
            _run_full_pipeline()
        else:
            log.info("Today's pipeline data exists -- skipping full pipeline run.")

        # ── Step 3b: Seed cache from R2 HTML ───────────────────────
        # If Railway restarted mid-day after a deploy, the in-memory cache is gone but
        # R2 has the HTML generated this morning. Seed the cache with it NOW so the site
        # serves the full dashboard immediately. If fresh generation succeeds later, it
        # replaces this. If generation returns None (games started / schedule gap), the
        # existing logic in _regenerate_in_background already preserves the cache.
        try:
            from db.csv_sync import _get_client, _bucket
            _r2 = _get_client()
            if _r2:
                import tempfile as _tmpmod
                _today_str = datetime.now(ET).strftime("%Y-%m-%d")
                for _r2_key in [
                    f"picks/mlb_picks_{_today_str}.html",
                    "picks/mlb_picks_latest.html",
                ]:
                    try:
                        with _tmpmod.NamedTemporaryFile(suffix=".html", delete=False) as _tf:
                            _tf_path = _tf.name
                        _r2.download_file(_bucket(), _r2_key, _tf_path)
                        with open(_tf_path, encoding="utf-8") as _f:
                            _r2_html = _f.read()
                        os.remove(_tf_path)
                        if _r2_html:
                            with _cache_lock:
                                _cache["html"] = _r2_html
                                # Set generated_at=0 so background regen always replaces this
                                # with fresh HTML from the current code, even if _generate()
                                # returns None (games started). The R2 HTML is just a stopgap
                                # while regeneration runs (~60s after startup).
                                _cache["generated_at"] = 0
                                _cache["r2_seeded"] = True
                            log.info(f"Startup: cache seeded from R2 ({_r2_key}) -- site ready immediately.")
                            break
                    except Exception:
                        continue
        except Exception as _r2e:
            log.debug(f"R2 cache seed skipped: {_r2e}")

        # ── Step 4: Dashboard cache ──────────────────────────────────────────────────────────────────────────────────
        log.info("Warming dashboard cache...")
        _regenerate_in_background()

    t = threading.Thread(target=_warm, daemon=True)
    t.start()


# Start schedulers and warm cache whether run via gunicorn or directly
_start_daily_scheduler()
warm_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

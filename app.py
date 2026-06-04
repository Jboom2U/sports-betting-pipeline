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
                    log.info(f"Background cache refresh complete in {int(time.time()-started)}s.")
                elif _cache["html"] is not None:
                    # main() returned None (games started / no upcoming slate) but we
                    # already have the rich morning dashboard in cache — keep it so the
                    # site stays populated all day without reverting to a stripped-down page.
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
    return Response(get_cached_html(), content_type="text/html; charset=utf-8")


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

    # -- Yesterday's Sharp Action ------------------------------------------------
    import csv as _csv2
    _yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")

    _mv_path_y = os.path.join(CLEAN_DIR, f"mlb_line_movement_{_yesterday}.csv")
    _mv_games = {}
    if os.path.exists(_mv_path_y):
        try:
            with open(_mv_path_y, encoding="utf-8") as _f:
                for _r in _csv2.DictReader(_f):
                    _k = (_r.get("away_team",""), _r.get("home_team",""))
                    if _k not in _mv_games or _r.get("snap2_time","") > _mv_games[_k].get("snap2_time",""):
                        _mv_games[_k] = _r
        except Exception:
            pass

    _graded_picks_y = []
    _game_scores_y  = {}   # (away_nick, home_nick) -> "Winner Name"
    # Try DB first — fetch ALL ML picks + scores
    try:
        from db.connection import get_conn
        _conn = get_conn()
        if _conn:
            with _conn.cursor() as _cur:
                # All ML picks for yesterday (any tier) + final scores
                _cur.execute(
                    "SELECT game, team, pick_type, tier, conf, actual_result, away_final, home_final "
                    "FROM picks WHERE pick_date = %s AND pick_type = %s",
                    (_yesterday, "ML")
                )
                for _r in _cur.fetchall():
                    _graded_picks_y.append({
                        "game": _r[0], "team": _r[1], "pick_type": _r[2],
                        "tier": _r[3], "conf": _r[4], "result": _r[5],
                        "away_final": _r[6], "home_final": _r[7],
                    })
                    # Build game scores lookup from any row that has final scores
                    if _r[6] is not None and _r[7] is not None:
                        _parts = (_r[0] or "").split(" @ ")
                        if len(_parts) == 2:
                            _ak = _parts[0].split()[-1].lower()
                            _hk = _parts[1].split()[-1].lower()
                            _winner_name = _parts[0] if int(_r[6]) > int(_r[7]) else _parts[1]
                            _game_scores_y[(_ak, _hk)] = _winner_name
            _conn.close()
    except Exception as _e:
        log.warning(f"Sharp table DB query failed: {_e}")
    # Fallback: load from analysis JSON (populated by run_analysis.py)
    if not _graded_picks_y:
        import json as _json2
        _analysis_path = os.path.join(BASE_DIR, "picks", f"mlb_analysis_{_yesterday}.json")
        if os.path.exists(_analysis_path):
            try:
                with open(_analysis_path, encoding="utf-8") as _f:
                    _analysis_data = _json2.load(_f)
                for _p in _analysis_data.get("graded_picks", []):
                    if _p.get("type","") == "ML" or _p.get("pick_type","") == "ML":
                        _graded_picks_y.append({
                            "game":   _p.get("game",""),
                            "team":   _p.get("label","").replace(" ML","").replace(" ml",""),
                            "tier":   _p.get("tier",""),
                            "conf":   _p.get("conf", 0),
                            "result": _p.get("result",""),
                        })
            except Exception as _e2:
                log.warning(f"Sharp table analysis JSON fallback failed: {_e2}")

    def _nick(name):
        return (name or "").split()[-1].lower() if name else ""

    _pick_by_game = {}
    for _p in _graded_picks_y:
        _pick_by_game[_p.get("game","")] = _p

    _sharp_rows_html = ""
    _sharp_model_w = _sharp_model_l = _sharp_sharp_w = _sharp_sharp_l = _both_wrong = 0

    for (_away, _home), _mv in sorted(_mv_games.items()):
        _ml_sig   = _mv.get("ml_signal","")
        _tot_sig  = _mv.get("total_signal","")
        _ml_move  = abs(float(_mv.get("ml_away_move") or 0))
        _tot_move = abs(float(_mv.get("total_move") or 0))
        _sharp_sd = _mv.get("sharp_side","")

        if _ml_move < 1 and _tot_move < 0.2 and _ml_sig == "STABLE" and _tot_sig == "STABLE":
            continue

        _sig_label = (_ml_sig if _ml_sig in ("STEAM","DRIFT") else
                      _tot_sig if _tot_sig in ("STEAM","DRIFT") else "MOVE")
        _sig_color = ("#ef5350" if _sig_label=="STEAM" else
                      "#ffa726" if _sig_label=="DRIFT" else "#8b949e")

        _model_pick_str = "—"
        _result_str = "—"
        _winner_str = "—"
        _result_color = "#8b949e"
        _winner_color = "#8b949e"

        _matched_pick = None
        for _gk, _pp in _pick_by_game.items():
            if _away in _gk and _home in _gk:
                _matched_pick = _pp
                break

        if _matched_pick:
            _conf_raw = float(_matched_pick.get('conf') or 0)
            _conf_pct = f"{_conf_raw * 100:.0f}%" if _conf_raw <= 1 else f"{_conf_raw:.0f}%"
            _tier_colors = {"LOCK":"#ffc107","STRONG":"#42a5f5","LEAN":"#66bb6a"}
            _tc = _tier_colors.get(_matched_pick.get("tier",""), "#8b949e")
            _model_pick_str = (f"<span style='color:{_tc}'>{_matched_pick.get('tier','')}</span> "
                               f"{_matched_pick.get('team','')} ({_conf_pct})")
            _actual = (_matched_pick.get("result") or "").upper()
            if _actual in ("WIN","LOSS"):
                # Result col: show actual winning team name
                _model_team = _matched_pick.get("team","")
                _model_nick = _nick(_model_team)
                _away_nick  = _nick(_away)
                _home_nick  = _nick(_home)
                if _actual == "WIN":
                    _winning_team = _model_team
                else:
                    # Model lost — other team won
                    _winning_team = _home if _model_nick == _away_nick else _away
                _result_str   = _winning_team.split()[-1] if _winning_team else "—"
                _result_color = "#e6edf3"

                _sharp_nick = _nick(_sharp_sd)
                if _sharp_sd and _model_nick == _sharp_nick:
                    # Sharp and model agree on same team
                    if _actual=="WIN":
                        _winner_str   = "Agree ✓"
                        _winner_color = "#3fb950"
                        _sharp_model_w+=1; _sharp_sharp_w+=1
                    else:
                        _winner_str   = '<span style="background:rgba(239,83,80,.15);color:#ef5350;font-weight:700;padding:2px 8px;border-radius:4px;font-size:.75rem">Both Wrong</span>'
                        _winner_color = ""
                        _both_wrong+=1; _sharp_model_l+=1; _sharp_sharp_l+=1
                elif _sharp_sd:
                    if _actual=="WIN":
                        _winner_str = "✓ Model"; _winner_color="#3fb950"
                        _sharp_model_w+=1; _sharp_sharp_l+=1
                    else:
                        _winner_str = "⚡ Sharp"; _winner_color="#ffa726"
                        _sharp_model_l+=1; _sharp_sharp_w+=1
                else:
                    _winner_str   = "✓ Model" if _actual=="WIN" else "✗ Model"
                    _winner_color = "#3fb950" if _actual=="WIN" else "#f85149"
                    if _actual=="WIN": _sharp_model_w+=1
                    else: _sharp_model_l+=1
        elif _sharp_sd:
            _model_pick_str = "<span style='color:#8b949e'>Pass</span>"

        # For rows with no result yet, try game scores lookup (covers TOSSUP + ungraded)
        if _result_str == "—":
            _score_key = (_nick(_away), _nick(_home))
            _actual_winner = _game_scores_y.get(_score_key, "")
            if _actual_winner:
                _result_str   = _actual_winner.split()[-1]
                _result_color = "#e6edf3"
                _sharp_nick   = _nick(_sharp_sd)
                _winner_nick  = _nick(_actual_winner)
                if _sharp_sd:
                    if _sharp_nick == _winner_nick:
                        _winner_str = "⚡ Sharp"; _winner_color="#ffa726"; _sharp_sharp_w+=1
                    else:
                        _winner_str = "✗ Sharp"; _winner_color="#f85149"; _sharp_sharp_l+=1

        _sharp_cell = _sharp_sd or "<span style='color:#8b949e'>—</span>"
        _move_note = ""
        if _ml_move >= 1:
            _move_note = f" <span style='font-size:.68rem;color:#8b949e'>({_mv.get('ml_away_open','?')}→{_mv.get('ml_away_now','?')})</span>"

        _sharp_rows_html += (
            f"<tr>"
            f"<td>{_away} @ {_home}{_move_note}</td>"
            f"<td style='color:{_sig_color};font-weight:700'>{_sig_label}</td>"
            f"<td>{_sharp_cell}</td>"
            f"<td>{_model_pick_str}</td>"
            f"<td style='color:{_result_color};font-weight:700'>{_result_str}</td>"
            f"<td style='color:{_winner_color};font-weight:700'>{_winner_str}</td>"
            f"</tr>\n"
        )

    _sm_d = _sharp_model_w + _sharp_model_l
    _ss_d = _sharp_sharp_w + _sharp_sharp_l
    _sm_str = f"{_sharp_model_w}/{_sm_d} ({_sharp_model_w/_sm_d*100:.0f}%)" if _sm_d>0 else "—"
    _ss_str = f"{_sharp_sharp_w}/{_ss_d} ({_sharp_sharp_w/_ss_d*100:.0f}%)" if _ss_d>0 else "—"

    if not _sharp_rows_html:
        _sharp_rows_html = "<tr><td colspan='6' style='color:#8b949e;padding:14px'>No movement data for yesterday.</td></tr>"

    _sharp_section_html = (
        f'<div class="secondary-section" style="margin-top:32px">' +
        f'<div class="secondary-toggle" onclick="var b=this.nextElementSibling;' +
        f'b.classList.toggle(\'open\');this.querySelector(\'.' +
        f'arr\').textContent=b.classList.contains(\'open\')?\'&#9660;\':\'&#9654;\'">' +
        f'<span class="arr">&#9654;</span> Yesterday\'s Sharp Action &mdash; {_yesterday}' +
        f'</div><div class="secondary-body" style="margin-top:10px">' +
        f'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:12px">' +
        f'<div class="stat-card"><div class="stat-val">{_sm_str}</div>' +
        f'<div class="stat-lbl">Model W/L (games with movement)</div></div>' +
        f'<div class="stat-card"><div class="stat-val">{_ss_str}</div>' +
        f'<div class="stat-lbl">Sharp W/L (games with sharp side)</div></div>' +
        f'<div class="stat-card"><div class="stat-val" style="color:#ef5350">{_both_wrong}</div>' +
        f'<div class="stat-lbl">Both Wrong (review)</div></div></div>' +
        f'<div class="table-card"><table><thead><tr>' +
        f'<th>Game</th><th>Signal</th><th>Sharp Side</th>' +
        f'<th>Model Pick</th><th>Result</th><th>Winner</th>' +
        f'</tr></thead><tbody>{_sharp_rows_html}</tbody></table></div>' +
        f'</div></div>'
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

  {_sharp_section_html}

  {_props_section_html}

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


# ── Scheduled 11:30am ET afternoon refresh ──────────────────────────────────────────────
def _seconds_until_1130am_et() -> float:
    """Return seconds until next 11:30am Eastern Time."""
    now    = datetime.now(ET)
    target = now.replace(hour=11, minute=30, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _run_afternoon_refresh():
    """Re-run lineup + hitter + odds + umpire + bullpen fatigue scrapers and rebuild dashboard."""
    today = datetime.now(ET).strftime("%Y-%m-%d")
    log.info("=== 11:30am ET afternoon refresh starting ===")

    # Grade yesterday's picks first so Yesterday panel is ready
    try:
        yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
        from run_analysis import run as grade_picks
        grade_picks(yesterday)
        log.info(f"Afternoon grading complete: {yesterday}")
    except Exception as e:
        log.warning(f"Afternoon grading failed (non-fatal): {e}")

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
            log.info("No confirmed lineups yet at 11:30am — dashboard will retry automatically")
    except Exception as e:
        log.warning(f"Afternoon lineup/hitter refresh failed (non-fatal): {e}")

    # Rebuild dashboard
    with _cache_lock:
        _cache["generated_at"] = 0
    _regenerate_in_background()
    log.info("=== Afternoon refresh complete — dashboard rebuilding ===")


def _start_afternoon_scheduler():
    """Background thread that runs the afternoon refresh at 11:30am ET every day."""
    def _loop():
        while True:
            wait = _seconds_until_1130am_et()
            log.info(f"Afternoon refresh scheduled in {wait/3600:.1f}h (11:30am ET).")
            time.sleep(wait)
            _run_afternoon_refresh()

    t = threading.Thread(target=_loop, daemon=True)
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

        # ── Step 4: Dashboard cache ──────────────────────────────────────────────────────────────────────────────────
        log.info("Warming dashboard cache...")
        _regenerate_in_background()

    t = threading.Thread(target=_warm, daemon=True)
    t.start()


# Start schedulers and warm cache whether run via gunicorn or directly
_start_daily_scheduler()
_start_afternoon_scheduler()
warm_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

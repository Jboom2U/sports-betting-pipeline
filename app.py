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

from flask import Flask, Response, redirect
from flask_compress import Compress

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

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
    """Check if today's foundational data is missing — if so, pipeline must run first."""
    today = datetime.now(ET).strftime("%Y-%m-%d")
    marker = os.path.join(BASE_DIR, "data", "pipeline_run_date.txt")
    if os.path.exists(marker):
        with open(marker) as f:
            last_run = f.read().strip()
        return last_run != today
    return True


def _mark_pipeline_ran():
    """Write today's date as the pipeline run marker."""
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
    Returns True if it's past 10am ET and we haven't fetched hitter stats today.
    This catches the window after lineups post (~10-11am) but before the next full pipeline run.
    """
    now   = datetime.now(ET)
    today = now.strftime("%Y-%m-%d")
    if now.hour < 10:
        return False   # Too early — lineups aren't posted yet
    stats_path = os.path.join(BASE_DIR, "data", "raw", f"mlb_hitter_stats_{today}.json")
    if not os.path.exists(stats_path):
        return True    # File doesn't exist — lineups may have confirmed, try fetch
    mtime = os.path.getmtime(stats_path)
    age_hours = (time.time() - mtime) / 3600
    return age_hours > 4   # Re-fetch if older than 4 hours (catches Railway restarts)


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
def _generate() -> str:
    """Run the dashboard HTML generator and return the HTML string."""
    log.info("Generating dashboard...")
    from run_picks_html import main as build_html
    html = build_html(date=None, no_open=True)
    log.info("Dashboard generation complete.")
    return html or "<h1>No picks available yet — check back soon.</h1>"


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
                _cache["html"] = html
                _cache["generated_at"] = time.time()
                _cache["generating"] = False
            log.info(f"Background cache refresh complete in {int(time.time()-started)}s.")
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
    # Pipeline runs on Windows (Task Scheduler) not on Railway — check for today's
    # committed data files as a proxy for "did the pipeline run today"
    marker = os.path.join(BASE_DIR, "data", "pipeline_run_date.txt")
    sched_path = os.path.join(CLEAN_DIR, "mlb_schedule_master.csv")
    if os.path.exists(marker):
        with open(marker) as f:
            pipeline_date = f.read().strip()
        pipeline_ok    = pipeline_date == today
        pipeline_label = f"Ran today ({pipeline_date})" if pipeline_ok else f"Last ran {pipeline_date}"
    elif os.path.exists(sched_path):
        # Check if schedule data contains today's games (proxy for pipeline ran today)
        try:
            with open(sched_path, encoding="utf-8") as f:
                has_today = any(r.get("game_date") == today for r in _csv.DictReader(f))
            if has_today:
                pipeline_ok    = True
                pipeline_label = f"Ran today (via schedule data)"
            else:
                pipeline_ok    = False
                pipeline_label = "Runs on Windows at 4am — data pending"
        except Exception:
            pipeline_ok    = None
            pipeline_label = "Runs on Windows at 4am"
    else:
        pipeline_ok    = None
        pipeline_label = "Runs on Windows at 4am"

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


# ── Scheduled 6am ET daily pipeline ──────────────────────────────────────────
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


# ── Startup ───────────────────────────────────────────────────────────────────
def warm_cache():
    """On startup: run pipeline if today's data is missing, then build dashboard."""
    def _warm():
        time.sleep(2)   # let Flask finish binding first
        if _needs_pipeline_run():
            log.info("No pipeline data for today — running full pipeline on startup...")
            _run_full_pipeline()
        else:
            log.info("Today's pipeline data exists — skipping full pipeline run.")
        log.info("Warming dashboard cache...")
        _regenerate_in_background()

    t = threading.Thread(target=_warm, daemon=True)
    t.start()


# Start scheduler and warm cache whether run via gunicorn or directly
_start_daily_scheduler()
warm_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

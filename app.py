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

from flask import Flask, Response, redirect, request, session, jsonify
from flask_compress import Compress

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Alerting (non-fatal — silently disabled if ALERT_EMAIL_* vars not set) ────
try:
    from alerts import send_alert as _send_alert
except ImportError:
    def _send_alert(subject, body="", exc=None): pass

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
app.secret_key = os.environ.get("ADMIN_SECRET") or os.environ.get("SECRET_KEY", "statalizers-dev-fallback")
Compress(app)   # gzip all responses — shrinks 570KB HTML to ~80KB

BASE_DIR  = os.path.dirname(__file__)
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
ET        = ZoneInfo("America/New_York")

# ── Cache ─────────────────────────────────────────────────────────────────────
CACHE_TTL = 10 * 60          # seconds — regenerate dashboard every 10 minutes
_cache_lock = threading.Lock()

# Set once the startup R2 CSV sync has finished. Dashboard regeneration loads
# every master CSV from data/clean/, and Railway's healthcheck hits / the moment
# Flask binds -- which fired _regenerate_in_background() while download_all() was
# still writing files. The model then loaded a half-populated directory and
# scored games with pitcher stats, team hitting and even the schedule missing,
# logging only "File not found" warnings. Gate regeneration on this.
_csv_ready = threading.Event()
_cache = {
    "html":         None,
    "generated_at": 0,
    "generating":   False,
}

# Stores scheduled times so the dashboard can surface them
_schedule_state = {
    "next_pipeline_et":  None,   # datetime — next 6am ET pipeline
    "next_refresh_et":   None,   # datetime — next adaptive refresh (2h before first pitch)
    "first_pitch_et":    None,   # datetime — earliest first pitch today
    "lineup_check_et":   None,   # datetime — next lineup check (every 30 min until confirmed)
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
        _send_alert(
            "6am pipeline FAILED",
            f"The morning pipeline crashed — today's picks may not have generated.\n\nError: {e}",
            exc=e,
        )

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
    # Pinnacle FIRST (free, sharp, accurate lines). Odds API only if Pinnacle is empty.
    odds_ok = False
    try:
        from scrapers.mlb_pinnacle_scraper import run as run_pinnacle
        pin_result = run_pinnacle()
        log.info(f"Pinnacle mid-day snapshot: {pin_result}")
        if (pin_result or {}).get("snapshots", 0) > 0:
            odds_ok = True
    except Exception as pe:
        log.warning(f"Pinnacle mid-day snapshot failed (non-fatal): {pe}")

    if not odds_ok:
        try:
            from scrapers.mlb_odds_scraper import run as run_odds
            result = run_odds()
            log.info(f"Odds API fallback snapshot: {result}")
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
        # Alert if it's past 3pm ET and still zero confirmed lineups
        if confirmed == 0 and len(lineups) > 0 and datetime.now(ET).hour >= 15:
            _send_alert(
                "Lineup scraper: 0 confirmed lineups after 3pm ET",
                f"The lineup scraper returned {len(lineups)} games but 0 confirmed lineups.\n"
                "Player props and lineup-adjusted picks may be missing or stale.",
            )
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
            # Wait for the startup CSV sync before touching the model. Without
            # this the model can load an empty data/clean/ and score on defaults.
            if not _csv_ready.wait(timeout=180):
                log.warning("Cache regen: CSV sync not confirmed after 180s — "
                            "proceeding, model data may be incomplete.")
            # Odds snapshots run only via the adaptive refresh (2h before first pitch)
            # and the 6am pipeline — NOT on every cache cycle. Keeps Odds API usage
            # to ~2 pulls/day (~60/month) instead of the old every-2-hour loop.
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
                    # main() returned None (games started / no upcoming slate) but we
                    # already have the rich morning dashboard in cache — keep it so the
                    # site stays populated all day without reverting to a stripped-down page.
                    _cache.pop("r2_seeded", None)
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
            _send_alert(
                "Dashboard generation failed",
                f"The dashboard cache refresh crashed — site may be serving stale picks.\n\nError: {e}",
                exc=e,
            )
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


@app.route("/admin/pinnacle-odds-test")
def pinnacle_odds_test():
    """Dry-run: pull Pinnacle game odds (ML/total/RL) and show every game with
    its lines + team names, so we can confirm they match the schedule (the A's
    naming is the one to eyeball). Writes nothing, spends zero quota."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/pinnacle-odds-test")
    import html as _h
    from datetime import datetime as _dt, timezone as _tz
    try:
        import json as _json
        from scrapers.mlb_pinnacle_scraper import (
            fetch_matchups, fetch_markets, _parse_matchups, _parse_markets)
        snapt = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        raw_m  = fetch_matchups()
        raw_mk = fetch_markets()
        idx    = _parse_matchups(raw_m)
        rows   = _parse_markets(raw_mk, idx, snapt, raw_m)

        out = [f"Pinnacle games parsed: {len(rows)}  (matchups indexed: {len(idx)})",
               f"raw matchups: {len(raw_m)}  |  raw markets: {len(raw_mk)}", ""]

        # ── Diagnostics: why markets aren't matching matchups ────────────────
        idx_ids = set(idx.keys())
        mk_mids, key_samples = [], {}
        for mk in raw_mk:
            if not isinstance(mk, dict):
                continue
            mid = mk.get("matchupId", mk.get("matchup_id"))
            mk_mids.append(mid)
            key_samples.setdefault(mk.get("key", "?"), 0)
            key_samples[mk.get("key", "?")] += 1
        mk_mid_set = set(mk_mids)
        overlap = idx_ids & mk_mid_set
        out.append(f"matchup index ids (sample 5): {list(idx_ids)[:5]}")
        out.append(f"market matchupIds (sample 5): {list(mk_mid_set)[:5]}")
        out.append(f"ids present in BOTH index and markets: {len(overlap)}")
        out.append(f"market key counts (top): "
                   f"{dict(sorted(key_samples.items(), key=lambda x:-x[1])[:12])}")
        out.append("")
        # Deep-dump ONE indexed game: its matchup participants + ml/ou/spread
        # markets, so we can confirm prices[0]=away is the correct mapping.
        one_id = next(iter(idx_ids), None)
        if one_id is not None:
            meta = idx.get(one_id, {})
            out.append(f"DEEP DUMP for indexed game {one_id}: "
                       f"{meta.get('away_team')} (away) @ {meta.get('home_team')} (home)")
            gm = next((m for m in raw_m if isinstance(m, dict) and m.get("id") == one_id), {})
            out.append("  matchup participants: " +
                       _json.dumps(gm.get("participants", []))[:500])
            for mk in raw_mk:
                if not isinstance(mk, dict) or mk.get("matchupId") != one_id:
                    continue
                k = mk.get("key", "")
                if k == "s;0;m" or k == "s;0;ou" or k.startswith("s;0;s;"):
                    out.append(f"  market {k}: prices={_json.dumps(mk.get('prices', []))[:300]}")
            # total candidates: children of this game carrying s;0;ou
            kids = {m.get("id") for m in raw_m
                    if isinstance(m, dict) and m.get("parentId") == one_id}
            # Every child of this game: its units + type + its s;0;ou line.
            # The game total is the one whose units marks a full-game runs total.
            ou_line = {}
            for mk in raw_mk:
                if isinstance(mk, dict) and mk.get("key") == "s;0;ou":
                    pr = mk.get("prices", [])
                    ln = next((p.get("points") for p in pr if p.get("points") is not None), None)
                    if ln is not None:
                        ou_line[mk.get("matchupId")] = ln
            out.append("  ALL children of this game (units | type | period | s;0;ou line):")
            for m in raw_m:
                if not isinstance(m, dict) or m.get("parentId") != one_id:
                    continue
                cid = m.get("id")
                out.append(f"    units={m.get('units')} type={m.get('type')} "
                           f"period={m.get('period')} line={ou_line.get(cid)}")
            # Also: any Over/Under matchup ANYWHERE with a game-total-range line
            out.append("  Over/Under matchups with line 7-12 (likely GAME totals):")
            hunt = 0
            for m in raw_m:
                if not isinstance(m, dict):
                    continue
                pn = [(p.get("name") or "").lower() for p in m.get("participants", [])]
                ln = ou_line.get(m.get("id"))
                if set(pn) == {"over", "under"} and ln is not None and 7 <= ln <= 12 and hunt < 15:
                    out.append(f"    id {m.get('id')} parent {m.get('parentId')} "
                               f"units={m.get('units')} line={ln}")
                    hunt += 1
        out.append("")
        for r in sorted(rows, key=lambda x: x["away_team"]):
            out.append(f"{r['away_team']} @ {r['home_team']}  |  ML {r['ml_away']}/{r['ml_home']}"
                       f"  |  RL {r['rl_away_line']}({r['rl_away_price']})/{r['rl_home_line']}({r['rl_home_price']})"
                       f"  |  Tot {r['total_line']} o{r['total_over_price']}/u{r['total_under_price']}")
        body = _h.escape("\n".join(out))
        return Response(f"<body style='background:#0d1117;color:#c9d1d9;font-family:ui-monospace,monospace;"
                        f"padding:24px'><h2 style='color:#58a6ff'>Pinnacle odds diagnostic</h2>"
                        f"<pre>{body}</pre></body>", mimetype="text/html")
    except Exception as e:
        import traceback
        return Response(f"<pre>{traceback.format_exc()}</pre>", mimetype="text/html"), 500


@app.route("/ask/answer", methods=["POST"])
def ask_answer():
    """Answer a natural-language question about today's board. Site-auth gated."""
    q = (request.form.get("q") or request.args.get("q") or "").strip()
    try:
        from ask_model import answer_question
        res = answer_question(q)
        return {"answer": res.get("answer", ""), "gemini": res.get("gemini", "")}
    except Exception as e:
        import traceback
        log.warning(f"/ask/answer failed: {traceback.format_exc()}")
        return {"answer": f"Something went wrong: {e}"}, 500


@app.route("/ask")
def ask_page():
    """Ask-the-model page: type a question about any game/team/prop, get a read."""
    # Warm the board pack in the background so the first question hits a ready
    # cache instead of loading the model + scoring inside the request (the timeout).
    def _warm():
        try:
            from ask_model import build_board_pack
            build_board_pack()
        except Exception as e:
            log.warning(f"ask warm failed: {e}")
    threading.Thread(target=_warm, daemon=True).start()
    html = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Statalizer Bot — Statalizers</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#c9d1d9;--sub:#8b949e;--blue:#58a6ff;--green:#3fb950}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:system-ui,Segoe UI,Arial;margin:0;padding:24px;line-height:1.55}
.wrap{max-width:820px;margin:0 auto}
h1{color:var(--blue);font-size:1.4rem;margin:0 0 4px}
.sub{color:var(--sub);font-size:.9rem;margin-bottom:18px}
textarea{width:100%;min-height:80px;background:var(--card);border:1px solid var(--border);
  border-radius:10px;color:var(--text);padding:12px 14px;font-size:1rem;font-family:inherit;resize:vertical}
.row{display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap}
.btn{background:var(--green);color:#04260f;border:none;padding:10px 20px;border-radius:9px;
  font-weight:800;cursor:pointer;font-size:.95rem}
.btn:disabled{opacity:.5;cursor:default}
.chips{display:flex;gap:8px;flex-wrap:wrap;margin:14px 0}
.chip{background:var(--card);border:1px solid var(--border);color:var(--sub);border-radius:20px;
  padding:6px 12px;font-size:.8rem;cursor:pointer}
.chip:hover{color:var(--text);border-color:var(--blue)}
.answer{margin-top:8px;background:var(--card);border:1px solid var(--border);border-left:3px solid var(--green);
  border-radius:10px;padding:16px 18px;white-space:pre-wrap;display:none}
.gemini-answer{border-left-color:#f0883e}
.ans-label{font-weight:700;font-size:.85rem;margin-top:20px;display:none}
#al{color:var(--green)} #gl{color:#f0883e}
.loading{color:var(--sub);font-style:italic;margin-top:20px;display:none}
a.back{color:var(--blue);text-decoration:none;font-size:.85rem}
</style></head><body><div class="wrap">
<h1>🧠 Statalizer Bot</h1>
<div class="sub">Ask about any game, matchup, team, or prop on today's board. Statalizer Bot answers from the
model's own reads and is honest about where it's calibrated and where it isn't.</div>
<textarea id="q" placeholder="e.g. What do you think about the Phillies moneyline tonight? Is there value on Skubal's strikeout prop?"></textarea>
<div class="row"><button class="btn" id="go" onclick="ask()">Ask</button>
<a class="back" href="/">&larr; Back to dashboard</a></div>
<div class="chips">
  <span class="chip" onclick="fill('Which moneylines have the best value today?')">Best value MLs?</span>
  <span class="chip" onclick="fill('What is your read on tonight\\'s best game?')">Best game read</span>
  <span class="chip" onclick="fill('Any strikeout props worth betting today?')">K props?</span>
  <span class="chip" onclick="fill('Which favorites are overpriced chalk to avoid?')">Chalk to avoid</span>
</div>
<div class="loading" id="load">Reading the board…</div>
<div class="ans-label" id="al">🔵 Statalizer Bot (Claude)</div>
<div class="answer" id="a"></div>
<div class="ans-label" id="gl">🟠 Second Opinion — Gemini (its own read + debate)</div>
<div class="answer gemini-answer" id="g"></div>
</div>
<script>
function fill(t){document.getElementById('q').value=t;}
async function ask(){
  const q=document.getElementById('q').value.trim();
  if(!q)return;
  const go=document.getElementById('go'),load=document.getElementById('load'),a=document.getElementById('a');
  const al=document.getElementById('al'),gl=document.getElementById('gl'),g=document.getElementById('g');
  go.disabled=true;load.style.display='block';
  a.style.display='none';al.style.display='none';gl.style.display='none';g.style.display='none';
  try{
    const r=await fetch('/ask/answer',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:'q='+encodeURIComponent(q)});
    const txt=await r.text();
    let d;
    try{ d=JSON.parse(txt); }catch(_){ d=null; }
    if(d && d.answer){
      a.textContent=d.answer; al.style.display='block';
      if(d.gemini && d.gemini.trim() && d.gemini.indexOf('[GEMINI_API_KEY not set')!==0){
        g.textContent=d.gemini; g.style.display='block'; gl.style.display='block';
      }
    }
    else if(!r.ok || d===null){
      a.textContent="Statalizer Bot is still warming up the day's data (first question after a while can take a moment). Give it 15 seconds and ask again.";
    } else { a.textContent='No answer.'; }
  }catch(e){a.textContent='That took too long to load. Wait a few seconds and try again.';}
  finally{go.disabled=false;load.style.display='none';a.style.display='block';}
}
document.getElementById('q').addEventListener('keydown',e=>{if(e.key==='Enter'&&(e.metaKey||e.ctrlKey))ask();});
</script></body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/admin/rebuild-schedule")
def admin_rebuild_schedule():
    """Delete + freshly re-scrape the schedule master to repair the column-shift
    corruption (home_team showing a pitcher id / blank). Rewrites clean, no quota."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/rebuild-schedule")
    def _worker():
        try:
            sp = os.path.join(CLEAN_DIR, "mlb_schedule_master.csv")
            if os.path.exists(sp):
                os.remove(sp)
                log.info("Deleted corrupt schedule master for clean rebuild")
            from scrapers.mlb_scraper import run as scrape
            scrape()
            from normalize.mlb_normalize import run as normalize
            normalize()
            with _cache_lock:
                _cache["generated_at"] = 0
            _regenerate_in_background()
            log.info("Schedule master rebuilt clean + dashboard regenerated")
        except Exception as e:
            log.warning(f"rebuild-schedule failed: {e}")
    threading.Thread(target=_worker, daemon=True).start()
    return Response(
        "<body style='background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:40px'>"
        "<h2>Rebuilding schedule from a fresh scrape…</h2>"
        "<p>Deletes the corrupted master and re-scrapes clean (~30-60s), then rebuilds "
        "the dashboard. Team names will be correct after it finishes. No Odds API used.</p>"
        "<p><a href='/' style='color:#58a6ff'>&rarr; Dashboard</a> "
        "(hard-refresh in ~1 minute)</p></body>",
        mimetype="text/html")


@app.route("/admin/refresh-gamelogs")
def admin_refresh_gamelogs():
    """Populate player_game_logs on demand (background). Powers the Players section.
    Needs today's lineups posted — run once lineups are confirmed."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/refresh-gamelogs")
    def _worker():
        try:
            # Ensure the ON CONFLICT target exists (self-heal if the prod table
            # predates the UNIQUE constraint — that would fail every upsert).
            from db.connection import db_conn
            with db_conn() as conn:
                if conn is not None:
                    conn.cursor().execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_pgl_date_name_team "
                        "ON player_game_logs (game_date, player_name, team)")
                    conn.commit()
        except Exception as e:
            log.warning(f"gamelog index ensure failed (non-fatal): {e}")
        try:
            from scrapers.mlb_player_gamelog_scraper import roster_players, run_for_players
            players = {}
            # 1. today's PROP players first — guarantees every prop-card player is
            #    covered even if they're not on a standard roster (probable pitchers,
            #    IL rehab, call-ups). These are what the user clicks through to.
            try:
                from model.mlb_props_model import score_all_props
                today = datetime.now(ET).strftime("%Y-%m-%d")
                for p in score_all_props(today):
                    pid = p.get("player_id")
                    if pid:
                        players[int(pid)] = {"player_id": int(pid),
                                             "player_name": p.get("player_name", ""),
                                             "is_pitcher": p.get("side") == "pitcher"}
            except Exception as _pe:
                log.warning(f"props-player seed list failed: {_pe}")
            # 2. all 40-man rosters (broad searchable directory)
            for pl in roster_players("40Man"):
                players.setdefault(pl["player_id"], pl)
            res = run_for_players(list(players.values()))
            log.info(f"Manual game-log refresh (props + 40-man): {res}")
        except Exception as e:
            log.warning(f"Manual game-log refresh failed: {e}")
    threading.Thread(target=_worker, daemon=True).start()
    return Response(
        "<body style='background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:40px'>"
        "<h2>Player game-log refresh started</h2>"
        "<p>Pulling season logs for EVERY active MLB player (all 30 rosters, ~2-3 min). "
        "Works regardless of lineups. Refresh the Players page in a few minutes.</p>"
        "<p><a href='/admin/gamelog-diag' style='color:#58a6ff'>Run diagnostic</a> · "
        "<a href='/players' style='color:#58a6ff'>Players</a></p></body>",
        mimetype="text/html")


@app.route("/admin/props-diag")
def props_diag():
    """Diagnose props 0-0: shows how many props score today (save side) and how
    many are saved+graded per date in player_prop_history (grade side)."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/props-diag")
    import html as _h, traceback as _tb
    from datetime import timedelta as _td
    out = []
    today = datetime.now(ET).strftime("%Y-%m-%d")
    try:
        from scrapers.mlb_pinnacle_scraper import save_strikeout_lines, load_strikeout_lines
        try:
            save_strikeout_lines(today)
        except Exception:
            pass
        klines = load_strikeout_lines(today)
        out.append(f"Pinnacle K lines file for {today}: {len(klines)} pitchers")
        from model.mlb_props_model import score_all_props
        props = score_all_props(today)
        nonproj = [p for p in props if not p.get("projected")]
        kp = [p for p in props if p.get("prop_type") == "K"]
        out.append(f"score_all_props: {len(props)} total | {len(nonproj)} non-projected (saveable) | {len(kp)} K props")
    except Exception:
        out.append("score ERROR:\n" + _tb.format_exc())
    try:
        from db.connection import db_conn
        with db_conn() as conn:
            cur = conn.cursor()
            cutoff = (datetime.now(ET) - _td(days=6)).strftime("%Y-%m-%d")
            cur.execute("SELECT game_date, COUNT(*), COUNT(*) FILTER (WHERE result IS NOT NULL) "
                        "FROM player_prop_history WHERE game_date >= %s GROUP BY game_date ORDER BY game_date DESC",
                        (cutoff,))
            out.append("\nplayer_prop_history (game_date: saved / graded):")
            rows = cur.fetchall()
            if not rows:
                out.append("  (no rows in last 6 days — props are NOT being SAVED)")
            for r in rows:
                out.append(f"  {r[0]}: {r[1]} saved, {r[2]} graded")
    except Exception:
        out.append("count ERROR:\n" + _tb.format_exc())
    return Response("<pre style='color:#c9d1d9;background:#0d1117;padding:20px;white-space:pre-wrap'>"
                    + _h.escape("\n".join(str(x) for x in out)) + "</pre>", mimetype="text/html")


@app.route("/admin/gemini-test")
def gemini_test():
    """Confirm the RUNNING process actually sees GEMINI_API_KEY, and do a live ping.
    If this says 'not visible' but the var is in Railway, the deployment predates the
    variable — redeploy so the container boots with it."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/gemini-test")
    import html as _h
    key = os.environ.get("GEMINI_API_KEY", "")
    model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
    out = [
        f"GEMINI_API_KEY visible to this process: {'YES' if key.strip() else 'NO'}",
        f"  key length: {len(key.strip())} chars" if key.strip() else "  (empty — redeploy so the container picks up the Railway variable)",
        f"GEMINI_MODEL: {model}",
        "",
    ]
    try:
        from gemini_client import call_gemini
        ping = call_gemini("You are a test.", "Reply with exactly: PONG", max_tokens=20)
        out.append(f"Live call result: {ping[:200]}")
    except Exception as e:
        out.append(f"Live call error: {e}")
    return Response("<pre style='color:#c9d1d9;background:#0d1117;padding:20px;white-space:pre-wrap'>"
                    + _h.escape("\n".join(out)) + "</pre>", mimetype="text/html")


@app.route("/admin/loss-analysis")
def loss_analysis():
    """Reverse-engineer losses: where is the model actually leaking?
    Breaks graded losses down by bet type, tier, confidence band, market signal,
    and sharp divergence, plus the worst high-confidence beats. Post-fix window only
    (>= 2026-07-25) so contaminated pre-fix picks never skew the read.
    ?days=N to widen the window (still floored at the boundary)."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/loss-analysis")
    import html as _h, traceback as _tb
    from datetime import timedelta as _td
    POSTFIX = "2026-07-25"
    try:
        days = int(request.args.get("days", 21))
    except Exception:
        days = 21
    start = (datetime.now(ET) - _td(days=days)).strftime("%Y-%m-%d")
    if start < POSTFIX:
        start = POSTFIX

    def _c(v):
        try:
            c = float(v or 0)
        except Exception:
            c = 0.0
        return c * 100 if c <= 1.5 else c

    def _band(c):
        if c >= 80: return "80%+"
        if c >= 70: return "70-80%"
        if c >= 60: return "60-70%"
        return "<60%"

    rows = []
    try:
        from db.connection import db_conn
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT p.pick_date, p.game, p.pick_type, p.label, p.team, p.conf,
                       p.tier, p.actual_result, p.market_signal,
                       p.away_final, p.home_final, sg.ml_signal, sg.sharp_side
                FROM picks p
                LEFT JOIN scored_games sg
                  ON sg.score_date = p.pick_date AND sg.game_id = p.game_id
                WHERE p.pick_date >= %s
                  AND p.actual_result IN ('WIN','LOSS','PUSH')
                ORDER BY p.pick_date DESC
                """,
                (start,))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        return Response("<pre style='color:#f85149;background:#0d1117;padding:20px'>"
                        + _h.escape(_tb.format_exc()) + "</pre>", mimetype="text/html")

    graded = [r for r in rows if (r["actual_result"] or "").upper() in ("WIN", "LOSS")]
    W = sum(1 for r in graded if r["actual_result"].upper() == "WIN")
    L = sum(1 for r in graded if r["actual_result"].upper() == "LOSS")
    n = W + L
    wr = (W / n * 100) if n else 0

    def _grp(keyfn):
        agg = {}
        for r in graded:
            k = keyfn(r)
            if k is None:
                continue
            a = agg.setdefault(k, [0, 0])
            if r["actual_result"].upper() == "WIN":
                a[0] += 1
            else:
                a[1] += 1
        out = []
        for k, (w, l) in agg.items():
            tot = w + l
            out.append((k, w, l, tot, (l / tot * 100) if tot else 0))
        return sorted(out, key=lambda x: x[4], reverse=True)

    by_type = _grp(lambda r: r["pick_type"] or "?")
    by_tier = _grp(lambda r: r["tier"] or "?")
    by_band = _grp(lambda r: _band(_c(r["conf"])))
    by_sig = _grp(lambda r: (r["market_signal"] or "NONE").upper())

    # Sharp divergence among losses: did the model's ML/RL loss go against sharp money?
    sharp_div_loss = sharp_agree_loss = 0
    for r in graded:
        if r["actual_result"].upper() != "LOSS":
            continue
        if (r["pick_type"] not in ("ML", "RL")) or not r.get("sharp_side"):
            continue
        pn = (r["team"] or "").split(" ")[-1].lower()
        sn = (r["sharp_side"] or "").split(" ")[-1].lower()
        if pn and sn:
            if pn == sn:
                sharp_agree_loss += 1
            else:
                sharp_div_loss += 1

    # Worst beats: highest-confidence losses
    losses = [r for r in graded if r["actual_result"].upper() == "LOSS"]
    losses.sort(key=lambda r: _c(r["conf"]), reverse=True)
    worst = losses[:20]

    # Plain-English leak read
    leaks = []
    for label, data in (("bet type", by_type), ("tier", by_tier), ("confidence band", by_band)):
        worst_grp = [g for g in data if g[3] >= 5]
        if worst_grp and worst_grp[0][4] >= 55:
            k, w, l, tot, lr = worst_grp[0]
            leaks.append(f"Biggest {label} leak: <b>{_h.escape(str(k))}</b> is {w}-{l} ({lr:.0f}% losses, n={tot}).")
    if n and wr < 52.4:
        leaks.append(f"Overall {W}-{L} ({wr:.1f}%) is below the {'-110'} break-even of 52.4% — the book edge is still winning over this window.")
    elif n:
        leaks.append(f"Overall {W}-{L} ({wr:.1f}%) is above the 52.4% break-even for this window.")
    if sharp_div_loss + sharp_agree_loss >= 5:
        leaks.append(f"Of ML/RL losses with sharp data, {sharp_div_loss} came fading sharp money vs {sharp_agree_loss} agreeing — "
                     + ("fading sharps is a leak." if sharp_div_loss > sharp_agree_loss else "not a sharp-divergence problem."))

    # ── Render ──
    css = ("body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,Segoe UI,sans-serif;margin:0;padding:26px}"
           "h1{font-size:1.3rem;margin:0 0 4px}h2{font-size:.9rem;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;margin:26px 0 10px}"
           "table{border-collapse:collapse;width:100%;max-width:760px;margin-bottom:8px}"
           "th,td{padding:7px 12px;text-align:left;border-bottom:1px solid #21262d;font-size:.82rem}"
           "th{color:#8b949e;font-size:.68rem;text-transform:uppercase}"
           ".bad{color:#f85149;font-weight:700}.good{color:#3fb950;font-weight:700}"
           ".card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px 20px;max-width:760px;margin-bottom:10px}"
           ".leak{background:#161b22;border-left:3px solid #ffa726;border-radius:8px;padding:10px 14px;margin-bottom:8px;font-size:.85rem;max-width:760px}"
           "a{color:#58a6ff}")

    def _tbl(title, data, kname):
        h = [f"<h2>{title}</h2><table><tr><th>{kname}</th><th>W-L</th><th>Loss rate</th><th>n</th></tr>"]
        for k, w, l, tot, lr in data:
            cls = "bad" if lr >= 52 else "good"
            h.append(f"<tr><td>{_h.escape(str(k))}</td><td>{w}-{l}</td>"
                     f"<td class='{cls}'>{lr:.0f}%</td><td>{tot}</td></tr>")
        h.append("</table>")
        return "".join(h)

    worst_rows = "".join(
        f"<tr><td>{_h.escape(str(r['pick_date']))}</td><td>{_h.escape((r['game'] or '')[:34])}</td>"
        f"<td>{_h.escape(str(r['pick_type'] or ''))}</td><td>{_h.escape((r['label'] or '')[:34])}</td>"
        f"<td>{_c(r['conf']):.0f}%</td>"
        f"<td>{('' if r['away_final'] is None else str(r['away_final'])+'-'+str(r['home_final']))}</td></tr>"
        for r in worst)

    leaks_html = "".join(f"<div class='leak'>{x}</div>" for x in leaks) or "<div class='leak'>Not enough graded data in this window to call a leak yet.</div>"

    body = (f"<h1>🔍 Loss Analysis — where the model is leaking</h1>"
            f"<div style='color:#8b949e;font-size:.82rem;margin-bottom:6px'>Window: {start} → today (post-fix only) · "
            f"{n} graded picks · <a href='/admin/loss-analysis?days=45'>widen to 45d</a></div>"
            f"<div class='card'><b style='font-size:1.1rem;' class='{'good' if wr>=52.4 else 'bad'}'>{W}-{L} ({wr:.1f}%)</b> "
            f"<span style='color:#8b949e'>vs 52.4% break-even</span></div>"
            f"<h2>What the losses are telling us</h2>{leaks_html}"
            + _tbl("Loss rate by bet type", by_type, "Type")
            + _tbl("Loss rate by tier", by_tier, "Tier")
            + _tbl("Loss rate by confidence band", by_band, "Band")
            + _tbl("Loss rate by market signal", by_sig, "Signal")
            + f"<h2>Worst beats — highest-confidence losses</h2><table>"
              f"<tr><th>Date</th><th>Game</th><th>Type</th><th>Pick</th><th>Conf</th><th>Final</th></tr>{worst_rows}</table>")

    return Response(f"<html><head><meta charset='utf-8'><style>{css}</style></head><body>{body}</body></html>",
                    mimetype="text/html")


@app.route("/admin/gamelog-diag")
def gamelog_diag():
    """Test the game-log pipeline end-to-end on ONE player + report where it breaks."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/gamelog-diag")
    import html as _h, traceback as _tb
    out = []
    # ensure the unique index (self-heal)
    try:
        from db.connection import db_conn
        with db_conn() as conn:
            if conn is not None:
                conn.cursor().execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_pgl_date_name_team "
                    "ON player_game_logs (game_date, player_name, team)")
                conn.commit()
                out.append("unique index ensured ✓")
    except Exception:
        out.append("index ensure ERROR:\n" + _tb.format_exc())
    # test one player
    try:
        from scrapers.mlb_player_gamelog_scraper import fetch_game_log, upsert_game_logs, SEASON
        test_pid, test_name = 665742, "Juan Soto"   # known active batter
        out.append(f"SEASON={SEASON} | testing {test_name} ({test_pid})")
        logs = fetch_game_log(test_pid, SEASON, group="hitting")
        out.append(f"fetch_game_log -> {len(logs)} rows")
        if logs:
            out.append(f"sample row: {logs[0]}")
            n = upsert_game_logs(test_name, test_pid, logs)
            out.append(f"upsert_game_logs -> {n} rows written")
    except Exception:
        out.append("fetch/upsert ERROR:\n" + _tb.format_exc())
    # table count
    try:
        from db.connection import db_conn
        with db_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT player_id) FROM player_game_logs")
            r = cur.fetchone()
            out.append(f"player_game_logs: {r[0]} rows, {r[1]} distinct players")
    except Exception:
        out.append("count ERROR:\n" + _tb.format_exc())
    return Response("<pre style='color:#c9d1d9;background:#0d1117;padding:20px;white-space:pre-wrap'>"
                    + _h.escape("\n\n".join(str(x) for x in out)) + "</pre>", mimetype="text/html")


@app.route("/players")
def players_page():
    """Searchable directory of every player we have game logs for."""
    import html as _h
    q    = (request.args.get("q") or "").strip()
    team = (request.args.get("team") or "").strip()
    from player_data import search_players
    rows = search_players(q, team) if (q or team) else search_players()
    cards = ""
    for r in rows:
        pid = r.get("player_id")
        if pid is None:
            continue
        nm  = _h.escape(r.get("player_name") or "")
        tm  = _h.escape(r.get("team") or "")
        gm  = r.get("games") or 0
        face = (f"https://img.mlbstatic.com/mlb-photos/image/upload/w_96,q_100/v1/people/{pid}/headshot/67/current")
        cards += (f"<a class='pl-card' href='/player/{pid}'>"
                  f"<img class='pl-face' src='{face}' onerror=\"this.style.visibility='hidden'\" alt=''>"
                  f"<div><div class='pl-name'>{nm}</div>"
                  f"<div class='pl-team'>{tm} · {gm} games</div></div></a>")
    if not cards:
        cards = "<div class='pl-empty'>No players found. Try a different name or team.</div>"
    html = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>Players — Statalizers</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#c9d1d9;--sub:#8b949e;--blue:#58a6ff;--green:#3fb950}
*{box-sizing:border-box}body{background:var(--bg);color:var(--text);font-family:system-ui,Segoe UI,Arial;margin:0;padding:24px}
.wrap{max-width:900px;margin:0 auto}
h1{color:var(--blue);font-size:1.4rem;margin:0 0 14px}
.search{display:flex;gap:10px;margin-bottom:18px;flex-wrap:wrap}
.search input{flex:1;min-width:180px;background:var(--card);border:1px solid var(--border);border-radius:9px;color:var(--text);padding:10px 12px;font-size:1rem}
.search button{background:var(--green);color:#04260f;border:none;padding:10px 18px;border-radius:9px;font-weight:800;cursor:pointer}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:10px}
.pl-card{display:flex;align-items:center;gap:12px;background:var(--card);border:1px solid var(--border);border-radius:10px;padding:10px 12px;text-decoration:none;color:inherit}
.pl-card:hover{border-color:var(--blue)}
.pl-face{width:48px;height:48px;border-radius:50%;object-fit:cover;background:var(--bg);border:1px solid var(--border)}
.pl-name{font-weight:700}.pl-team{font-size:.78rem;color:var(--sub)}
.pl-empty{color:var(--sub);padding:20px}
a.back{color:var(--blue);text-decoration:none;font-size:.85rem}
</style></head><body><div class="wrap">
<h1>👤 Players</h1>
<form class="search" method="get" action="/players">
  <input name="q" placeholder="Search player name…" value="__Q__" autofocus>
  <input name="team" placeholder="Team…" value="__TEAM__" style="max-width:180px">
  <button type="submit">Search</button>
</form>
<div class="grid">__CARDS__</div>
<p style="margin-top:18px"><a class="back" href="/">&larr; Back to dashboard</a></p>
</div></body></html>"""
    html = (html.replace("__CARDS__", cards)
                .replace("__Q__", _h.escape(q)).replace("__TEAM__", _h.escape(team)))
    return Response(html, mimetype="text/html")


@app.route("/player/<int:pid>")
def player_page(pid):
    """Per-game trend charts for one player (hits/TB/HR/RBI/K/SB, L5/L10/L20)."""
    import html as _h, json as _json
    from player_data import get_player
    p = get_player(pid)
    name = _h.escape(p.get("player_name") or f"Player {pid}")
    team = _h.escape(p.get("team") or "")
    games_json = _json.dumps(p.get("games") or [], default=str)
    face = f"https://img.mlbstatic.com/mlb-photos/image/upload/w_180,q_100/v1/people/{pid}/headshot/67/current"
    html = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>__NAME__ — Statalizers</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#c9d1d9;--sub:#8b949e;--blue:#58a6ff;--green:#3fb950}
*{box-sizing:border-box}body{background:var(--bg);color:var(--text);font-family:system-ui,Segoe UI,Arial;margin:0;padding:24px}
.wrap{max-width:900px;margin:0 auto}
.phead{display:flex;align-items:center;gap:16px;margin-bottom:8px}
.phead img{width:66px;height:66px;border-radius:50%;object-fit:cover;background:var(--card);border:1px solid var(--border)}
.phead h1{color:var(--blue);font-size:1.5rem;margin:0}
.phead .team{color:var(--sub);font-size:.9rem}
.toggle{display:flex;gap:8px;margin:14px 0 18px}
.toggle button{background:var(--card);border:1px solid var(--border);color:var(--sub);border-radius:8px;padding:6px 14px;cursor:pointer;font-weight:700}
.toggle button.active{color:var(--green);border-color:var(--green)}
.pchart{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 16px;margin-bottom:14px}
.pchart-title{font-weight:800;margin-bottom:2px}
.pchart-avg{font-size:.75rem;color:var(--sub);font-weight:600;margin-left:8px}
.pbars{display:flex;align-items:flex-end;gap:6px;height:130px;margin-top:12px}
.pbar-col{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%}
.pbar-val{font-size:.72rem;font-weight:800;margin-bottom:3px}
.pbar{width:100%;max-width:34px;background:var(--green);border-radius:4px 4px 0 0;min-height:2px}
.pbar.zero{background:#30363d}
.pbar-lbl{font-size:.6rem;color:var(--sub);text-align:center;margin-top:5px;line-height:1.25}
.pl-empty{color:var(--sub);padding:24px;text-align:center}
a.back{color:var(--blue);text-decoration:none;font-size:.85rem}
</style></head><body><div class="wrap">
<div class="phead">
  <img src="__FACE__" onerror="this.style.visibility='hidden'" alt="">
  <div><h1>__NAME__</h1><div class="team">__TEAM__</div></div>
</div>
<div class="toggle">
  <button data-n="5">Last 5</button>
  <button data-n="10" class="active">Last 10</button>
  <button data-n="20">Last 20</button>
</div>
<div id="charts"></div>
<p style="margin-top:6px"><a class="back" href="/players">&larr; All players</a> &nbsp; <a class="back" href="/">Dashboard</a></p>
</div>
<script>
const GAMES = __GAMES__;   // oldest-first
// Pitchers have 0 at-bats; show pitching charts (strikeouts thrown, etc.).
const IS_PITCHER = GAMES.length && GAMES.every(x=>(+x.ab||0)===0) && GAMES.some(x=>(+x.k||0)>0);
const STATS = IS_PITCHER
  ? [["k","Strikeouts (thrown)"],["h","Hits allowed"],["bb","Walks"],["hr","HR allowed"]]
  : [["h","Hits"],["tb","Total Bases"],["hr","Home Runs"],["rbi","RBIs"],["k","Strikeouts"],["sb","Stolen Bases"]];
let N = 10;
function render(){
  const box=document.getElementById("charts");
  if(!GAMES.length){ box.innerHTML='<div class="pl-empty">No game logs yet for this player. They\\'ll fill in as games are played.</div>'; return; }
  const g = GAMES.slice(-N);
  box.innerHTML = STATS.map(([col,label])=>{
    const vals = g.map(x=>+(x[col]||0));
    const mx = Math.max(1, ...vals);
    const avg = vals.reduce((a,b)=>a+b,0)/(vals.length||1);
    const bars = g.map((x,i)=>{
      const v=vals[i]; const h=Math.round(v/mx*100);
      const d=(x.game_date||"").slice(5); const opp=(x.opponent||"").split(" ").slice(-1)[0];
      return `<div class="pbar-col"><div class="pbar-val">${v}</div>`+
             `<div class="pbar ${v===0?'zero':''}" style="height:${h}%"></div>`+
             `<div class="pbar-lbl">${d}<br>${opp}</div></div>`;
    }).join("");
    return `<div class="pchart"><div class="pchart-title">${label}`+
           `<span class="pchart-avg">avg ${avg.toFixed(1)} · L${g.length}</span></div>`+
           `<div class="pbars">${bars}</div></div>`;
  }).join("");
}
document.querySelectorAll(".toggle button").forEach(b=>b.onclick=()=>{
  document.querySelectorAll(".toggle button").forEach(x=>x.classList.remove("active"));
  b.classList.add("active"); N=+b.dataset.n; render();
});
render();
</script></body></html>"""
    html = (html.replace("__GAMES__", games_json).replace("__NAME__", name)
                .replace("__TEAM__", team).replace("__FACE__", face))
    return Response(html, mimetype="text/html")


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


@app.route("/force-statcast")
def force_statcast():
    """Force a fresh Statcast pitcher scrape, upload to R2, rebuild dashboard."""
    def _worker():
        try:
            from scrapers.mlb_statcast_pitcher_scraper import run as run_psc
            result = run_psc()
            log.info(f"Force-statcast complete: {result}")
        except Exception as e:
            log.warning(f"Force-statcast pitcher failed: {e}")
        try:
            from scrapers.mlb_statcast_scraper import run as run_sc
            result2 = run_sc()
            log.info(f"Force-statcast batters complete: {result2}")
        except Exception as e:
            log.warning(f"Force-statcast batters failed: {e}")
        try:
            from db.csv_sync import upload_all, storage_available
            if storage_available():
                n = upload_all()
                log.info(f"Statcast CSVs uploaded to R2: {n} file(s)")
        except Exception as e:
            log.warning(f"Statcast R2 upload failed: {e}")
        with _cache_lock:
            _cache["generated_at"] = 0
        _regenerate_in_background()
    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return {"status": "ok", "message": "Statcast scrape started — dashboard will refresh in ~90 seconds."}


@app.route("/force-lineups")
def force_lineups():
    """
    Manual refresh of BOTH batting lineups AND probable starting pitchers, then
    rebuild the dashboard. No Odds API calls (MLB schedule + lineup endpoints only).

    Batting order and starting pitcher come from DIFFERENT sources: the lineup
    scraper only updates the 1-9 order, while starters live in the schedule's
    probable_pitcher fields. The old button refreshed only the former, so a
    starter announced after 6am stayed "TBD" until the 4:40 afternoon refresh.
    This does both, synchronously, and reports what actually changed.
    """
    lineups_scraped = confirmed = starters_updated = 0

    # 1. Batting lineups (sync so we can report counts)
    try:
        from scrapers.mlb_lineup_scraper import run as run_lu
        result = run_lu() or []
        lineups_scraped = len(result)
        confirmed = sum(1 for g in result if isinstance(g, dict) and g.get("lineup_confirmed"))
        log.info(f"Force-lineups: {lineups_scraped} games scraped, {confirmed} confirmed")
    except Exception as e:
        log.warning(f"Force-lineups lineup fetch failed: {e}")

    # 2. Probable starting pitchers — the piece the button used to miss.
    #    upsert_schedule_pitchers returns the count of games whose starter
    #    changed/filled, which is exactly the "did any TBD resolve" signal.
    try:
        from scrapers.mlb_scraper import fetch_schedule
        from normalize.mlb_normalize import upsert_schedule_pitchers
        fresh_sched = fetch_schedule(days_ahead=1)
        starters_updated = upsert_schedule_pitchers(fresh_sched)
        log.info(f"Force-lineups: {starters_updated} starter(s) updated")
    except Exception as e:
        log.warning(f"Force-lineups starter refresh failed: {e}")

    # 2b. Real pitcher K lines from Pinnacle (free) — starters known, specials posted.
    k_lines = 0
    try:
        from scrapers.mlb_pinnacle_scraper import save_strikeout_lines
        k_lines = save_strikeout_lines()
        log.info(f"Force-lineups: {k_lines} Pinnacle K line(s)")
    except Exception as e:
        log.warning(f"Force-lineups K line pull failed: {e}")

    # 3. Hitter stats + R2 upload + dashboard rebuild — async (slow, not needed
    #    for the response summary).
    def _worker():
        try:
            from scrapers.mlb_hitter_scraper import run as run_hs
            run_hs()
        except Exception as e:
            log.warning(f"Force-lineups hitter scraper failed: {e}")
        try:
            from db.csv_sync import upload_all, storage_available
            if storage_available():
                upload_all()
        except Exception as e:
            log.warning(f"Force-lineups R2 upload failed: {e}")
        with _cache_lock:
            _cache["generated_at"] = 0
        _regenerate_in_background()
    threading.Thread(target=_worker, daemon=True).start()

    # Human-readable note for the toast.
    bits = []
    if confirmed:        bits.append(f"{confirmed} lineup(s) confirmed")
    if starters_updated: bits.append(f"{starters_updated} starter(s) updated")
    if k_lines:          bits.append(f"{k_lines} K line(s)")
    if bits:
        msg = "Updated: " + ", ".join(bits) + ". Dashboard refreshing…"
    else:
        msg = "No new lineups or starters posted yet. Dashboard refreshing…"

    return {"status": "ok", "lineups_scraped": lineups_scraped,
            "confirmed": confirmed, "starters_updated": starters_updated,
            "message": msg}


_ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "")

# ── Site password (stored in DB, falls back to SITE_PASSWORD env var) ────────
_SITE_PWD_KEY    = "site_password"
_site_pwd_cache  = [os.environ.get("SITE_PASSWORD", "")]  # list = mutable ref

def _load_site_password():
    """Pull site password from DB on startup; fall back to env var."""
    try:
        from db.connection import db_conn
        with db_conn() as conn:
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT value FROM site_config WHERE key = %s", (_SITE_PWD_KEY,))
                row = cur.fetchone()
                if row and row[0]:
                    _site_pwd_cache[0] = row[0]
                    log.info("Site password loaded from DB")
                    return
    except Exception as _spe:
        log.debug(f"Site password DB load skipped: {_spe}")
    log.info("Site password: using SITE_PASSWORD env var" if _site_pwd_cache[0] else "Site password: not set — site is open")

def _save_site_password(new_pw):
    """Persist new site password to DB and update cache."""
    from db.connection import db_conn
    with db_conn() as conn:
        if conn is None:
            raise RuntimeError("No DB connection available")
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO site_config (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (_SITE_PWD_KEY, new_pw))
    _site_pwd_cache[0] = new_pw



# ── Site-wide login wall ──────────────────────────────────────────────────────
_SITE_EXEMPT = {"/site-login", "/site-logout", "/health"}

@app.before_request
def require_site_auth():
    if not _site_pwd_cache[0]:
        return  # no password set — site is publicly open
    if request.path in _SITE_EXEMPT:
        return
    if session.get("site_auth"):
        return
    return redirect(f"/site-login?next={request.path}")

@app.route("/site-login", methods=["GET", "POST"])
def site_login():
    error = ""
    next_url = request.args.get("next", "/")
    if request.method == "POST":
        if request.form.get("password") == _site_pwd_cache[0] and _site_pwd_cache[0]:
            session["site_auth"] = True
            return redirect(request.form.get("next", "/"))
        error = "Incorrect password"
        next_url = request.form.get("next", "/")
    return Response(f"""<!DOCTYPE html>
<html><head><title>Statalizers — Login</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
  min-height:100vh;display:flex;align-items:center;justify-content:center;padding:1rem}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:2rem 2.5rem;
  width:100%;max-width:360px}}
.logo{{font-size:28px;margin-bottom:.25rem;text-align:center}}
h2{{font-size:18px;font-weight:600;text-align:center;margin-bottom:1.75rem;color:#e6edf3}}
label{{font-size:13px;color:#8b949e;display:block;margin-bottom:.4rem}}
input[type=password]{{width:100%;padding:.6rem .85rem;border-radius:7px;border:1px solid #30363d;
  background:#21262d;color:#e6edf3;font-size:15px;outline:none;transition:border-color .15s}}
input[type=password]:focus{{border-color:#58a6ff}}
.err{{color:#f85149;font-size:13px;margin-top:.75rem;min-height:1.1em;text-align:center}}
button{{margin-top:1.25rem;width:100%;padding:.65rem;background:#238636;color:#fff;
  font-weight:600;border:none;border-radius:7px;cursor:pointer;font-size:15px;
  transition:background .15s}}
button:hover{{background:#2ea043}}
</style></head>
<body><div class="card">
  <div class="logo">⚾</div>
  <h2>Statalizers</h2>
  <form method="post">
    <input type="hidden" name="next" value="{next_url}">
    <label>Password</label>
    <input type="password" name="password" placeholder="Enter site password" autofocus>
    <div class="err">{error}</div>
    <button type="submit">Sign in</button>
  </form>
</div></body></html>""", mimetype="text/html")

@app.route("/site-logout")
def site_logout():
    session.pop("site_auth", None)
    return redirect("/site-login")

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = ""
    next_url = request.args.get("next", "/admin/model-config")
    if request.method == "POST":
        if request.form.get("password") == _ADMIN_PASS and _ADMIN_PASS:
            session["admin_auth"] = True
            return redirect(request.form.get("next", "/admin/model-config"))
        error = "Invalid password"
        next_url = request.form.get("next", "/admin/model-config")
    return Response(f"""<!DOCTYPE html>
<html><head><title>Statalizers Admin</title>
<style>
body{{background:#0d1117;color:#e6edf3;font-family:-apple-system,sans-serif;
  display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:2rem 2.5rem;width:320px}}
h2{{margin:0 0 1.5rem;font-size:18px;font-weight:600}}
input{{width:100%;padding:.5rem .75rem;border-radius:6px;border:1px solid #30363d;
  background:#21262d;color:#e6edf3;font-size:14px;box-sizing:border-box}}
.err{{color:#f85149;font-size:13px;margin-top:.75rem;min-height:1.2em}}
button{{margin-top:1rem;width:100%;padding:.5rem;background:#58a6ff;color:#000;
  font-weight:600;border:none;border-radius:6px;cursor:pointer;font-size:14px}}
button:hover{{opacity:.85}}
</style></head>
<body><div class="box">
<h2>⚾ Statalizers admin</h2>
<form method="post">
  <input type="hidden" name="next" value="{next_url}">
  <input type="password" name="password" placeholder="Password" autofocus>
  <div class="err">{error}</div>
  <button type="submit">Sign in</button>
</form>
</div></body></html>""", mimetype="text/html")

@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_auth", None)
    return redirect("/")

@app.route("/admin")
def admin_hub():
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin")
    return Response("""<!DOCTYPE html>
<html><head><title>Statalizers Admin</title>
<style>
body{background:#0d1117;color:#e6edf3;font-family:-apple-system,sans-serif;margin:0}
nav{background:#161b22;border-bottom:1px solid #30363d;padding:.75rem 1.5rem;
  display:flex;align-items:center;gap:1.5rem}
.logo{font-size:15px;font-weight:600}
nav a{color:#8b949e;font-size:13px;text-decoration:none}
nav a:hover{color:#e6edf3}
.container{max-width:800px;margin:2rem auto;padding:0 1.5rem}
h1{font-size:20px;font-weight:600;margin-bottom:.25rem}
.sub{color:#8b949e;font-size:13px;margin-bottom:2rem}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
@media(max-width:540px){.grid{grid-template-columns:1fr}}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;
  padding:1.25rem 1.5rem;text-decoration:none;color:inherit;display:block;
  transition:border-color .15s}
.card:hover{border-color:#58a6ff}
.card-title{font-size:15px;font-weight:600;margin-bottom:.3rem}
.card-desc{font-size:13px;color:#8b949e}
.badge{display:inline-block;font-size:11px;padding:2px 7px;border-radius:20px;
  margin-bottom:.5rem;font-weight:600}
.badge-public{background:#1f3d2e;color:#3fb950}
.badge-admin{background:#2d2016;color:#d29922}
.logout{margin-top:2rem;font-size:13px;color:#8b949e}
.logout a{color:#8b949e}
</style></head>
<body>
<nav><span class="logo">⚾ Statalizers</span>
  <a href="/">Dashboard</a>
  <a href="/admin">Admin</a>
</nav>
<div class="container">
  <h1>Admin hub</h1>
  <p class="sub">All internal routes for Statalizers.com</p>
  <h2 style="font-size:14px;color:#8b949e;margin:1.5rem 0 .75rem;text-transform:uppercase;letter-spacing:.05em">Daily views</h2>
  <div class="grid">
    <a class="card" href="/"><span class="badge badge-public">Public</span>
      <div class="card-title">Main dashboard</div>
      <div class="card-desc">Today's picks, Best Bets, Daily Summary</div></a>
    <a class="card" href="/ask"><span class="badge badge-public">Public</span>
      <div class="card-title">🧠 Statalizer Bot</div>
      <div class="card-desc">Ask about any game, matchup, or prop</div></a>
    <a class="card" href="/performance-html"><span class="badge badge-public">Public</span>
      <div class="card-title">Performance tracker</div>
      <div class="card-desc">W/L/ROI by tier &amp; type, sharp action, 7-90d toggles</div></a>
  </div>

  <h2 style="font-size:14px;color:#8b949e;margin:1.75rem 0 .75rem;text-transform:uppercase;letter-spacing:.05em">Analysis &amp; performance</h2>
  <div class="grid">
    <a class="card" href="/admin/analysis"><span class="badge badge-admin">Admin</span>
      <div class="card-title">📋 Nightly analysis report</div>
      <div class="card-desc">Day review + trends. Date picker for ANY past day, download, email</div></a>
    <a class="card" href="/admin/calibration"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Calibration</div>
      <div class="card-desc">Predicted vs actual by confidence band, tier, type</div></a>
    <a class="card" href="/admin/signal-audit"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Signal audit</div>
      <div class="card-desc">Which model inputs actually vary across the slate</div></a>
    <a class="card" href="/admin/loss-analysis"><span class="badge badge-admin">Admin</span>
      <div class="card-title">🔍 Loss analysis</div>
      <div class="card-desc">Reverse-engineers losses: leak by type, tier, band, sharp</div></a>
    <a class="card" href="/analytics"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Analytics dashboard</div>
      <div class="card-desc">Natural-language DB queries over pick history</div></a>
    <a class="card" href="/admin/model-config"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Model control panel</div>
      <div class="card-desc">Tune signal weights, preview impact, save config</div></a>
  </div>

  <h2 style="font-size:14px;color:#8b949e;margin:1.75rem 0 .75rem;text-transform:uppercase;letter-spacing:.05em">Diagnostics</h2>
  <div class="grid">
    <a class="card" href="/admin/pinnacle-odds-test"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Pinnacle odds diagnostic</div>
      <div class="card-desc">Dry-run ML/RL/total parse, no writes, no quota</div></a>
    <a class="card" href="/admin/pinnacle-k-test"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Pinnacle K-line test</div>
      <div class="card-desc">Live strikeout lines + prices parse check</div></a>
    <a class="card" href="/admin/props-diag"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Props diagnostic</div>
      <div class="card-desc">Props scored today vs saved/graded per date (0-0 debug)</div></a>
    <a class="card" href="/status"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Pipeline status</div>
      <div class="card-desc">Last run, DB, R2 health</div></a>
    <a class="card" href="/schedule-status"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Schedule status</div>
      <div class="card-desc">Next pipeline, refresh, first pitch (JSON)</div></a>
  </div>

  <h2 style="font-size:14px;color:#8b949e;margin:1.75rem 0 .75rem;text-transform:uppercase;letter-spacing:.05em">Actions (run on click)</h2>
  <div class="grid">
    <a class="card" href="/force-pipeline"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Force pipeline</div>
      <div class="card-desc">Trigger the full 6am pipeline now</div></a>
    <a class="card" href="/force-odds"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Force odds snapshot</div>
      <div class="card-desc">Fresh Pinnacle ML/RL pull + dashboard rebuild</div></a>
    <a class="card" href="/admin/refresh-signals"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Refresh signals</div>
      <div class="card-desc">Umpire, bullpen, pitcher/team stats (no quota)</div></a>
    <a class="card" href="/admin/grade-backfill"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Grade backfill</div>
      <div class="card-desc">Import + grade past picks from R2 analysis JSONs</div></a>
    <a class="card" href="/unstick"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Unstick pipeline</div>
      <div class="card-desc">Clear stuck pipeline state</div></a>
    <a class="card" href="/admin/change-site-password"><span class="badge badge-admin">Admin</span>
      <div class="card-title">Change site password</div>
      <div class="card-desc">Update the public site login</div></a>
  </div>
  <p class="logout"><a href="/admin/logout">Sign out</a></p>
</div></body></html>""", mimetype="text/html")

@app.route("/admin/change-site-password", methods=["GET", "POST"])
def admin_change_site_password():
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/change-site-password")
    msg = ""
    msg_color = "#3fb950"
    if request.method == "POST":
        new_pw = request.form.get("new_password", "").strip()
        confirm = request.form.get("confirm_password", "").strip()
        if not new_pw:
            msg, msg_color = "Password cannot be empty.", "#f85149"
        elif new_pw != confirm:
            msg, msg_color = "Passwords do not match.", "#f85149"
        else:
            try:
                _save_site_password(new_pw)
                msg = "Site password updated successfully."
            except Exception as e:
                msg, msg_color = f"Error saving password: {e}", "#f85149"
    current_set = "Set" if _site_pwd_cache[0] else "Not set (site is open)"
    return Response(f"""<!DOCTYPE html>
<html><head><title>Site Password — Statalizers Admin</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{{box-sizing:border-box}}
body{{background:#0d1117;color:#e6edf3;font-family:-apple-system,sans-serif;margin:0}}
nav{{background:#161b22;border-bottom:1px solid #30363d;padding:.75rem 1.5rem;display:flex;align-items:center;gap:1.5rem}}
.logo{{font-size:15px;font-weight:600}}
nav a{{color:#8b949e;font-size:13px;text-decoration:none}}
nav a:hover{{color:#e6edf3}}
.container{{max-width:480px;margin:2rem auto;padding:0 1.5rem}}
h1{{font-size:20px;font-weight:600;margin-bottom:.25rem}}
.sub{{color:#8b949e;font-size:13px;margin-bottom:2rem}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:1.5rem}}
label{{font-size:13px;color:#8b949e;display:block;margin-bottom:.4rem;margin-top:1rem}}
label:first-of-type{{margin-top:0}}
input[type=password]{{width:100%;padding:.5rem .75rem;border-radius:6px;border:1px solid #30363d;
  background:#21262d;color:#e6edf3;font-size:14px}}
.status{{font-size:12px;color:#8b949e;margin-bottom:1.25rem}}
button{{margin-top:1.25rem;padding:.5rem 1.25rem;background:#238636;color:#fff;
  font-weight:600;border:none;border-radius:6px;cursor:pointer;font-size:14px}}
button:hover{{opacity:.85}}
.msg{{margin-top:1rem;font-size:13px;font-weight:600}}
.back{{display:inline-block;margin-top:1.25rem;font-size:13px;color:#58a6ff;text-decoration:none}}
</style></head>
<body>
<nav><span class="logo">⚾ Statalizers</span>
  <a href="/admin">← Admin hub</a>
  <a href="/admin/logout" style="margin-left:auto">Sign out</a>
</nav>
<div class="container">
  <h1>Site Password</h1>
  <p class="sub">Controls access to the entire statalizers.com site.</p>
  <div class="card">
    <p class="status">Current status: <strong style="color:#e6edf3">{current_set}</strong></p>
    <form method="post">
      <label>New password</label>
      <input type="password" name="new_password" placeholder="Enter new password" autofocus>
      <label>Confirm password</label>
      <input type="password" name="confirm_password" placeholder="Confirm new password">
      <button type="submit">Update password</button>
    </form>
    {f'<p class="msg" style="color:{msg_color}">{msg}</p>' if msg else ''}
    <a class="back" href="/admin">← Back to admin hub</a>
  </div>
</div></body></html>""", mimetype="text/html")

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



@app.route("/admin/kalshi-debug")
def kalshi_debug():
    """Show raw Kalshi market titles to diagnose matching failures."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/kalshi-debug")
    try:
        from scrapers.mlb_kalshi_scraper import _get_api_key, fetch_all_mlb_markets
        api_key = _get_api_key()
        markets = fetch_all_mlb_markets(api_key)
        rows = ""
        for m in markets[:50]:
            title  = m.get("title", "")
            ticker = m.get("ticker", "")
            status = m.get("status", "")
            yes_ask = m.get("yes_ask", "")
            rows += f"<tr><td>{ticker}</td><td>{status}</td><td>{yes_ask}</td><td>{title}</td></tr>"
        html = f"""<html><head><title>Kalshi Debug</title>
<style>body{{font-family:monospace;background:#0d1117;color:#e6edf3;padding:20px}}
table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #30363d;padding:6px 10px;text-align:left}}
th{{background:#161b22;color:#8b949e}}
tr:nth-child(even){{background:#161b22}}</style></head>
<body><h2>Kalshi Raw Markets ({len(markets)} total)</h2>
<table><tr><th>Ticker</th><th>Status</th><th>Yes Ask</th><th>Title</th></tr>
{rows}</table></body></html>"""
        return Response(html, mimetype="text/html")
    except Exception as e:
        return Response(f"<pre>Error: {e}</pre>", mimetype="text/html")


@app.route("/admin/regrade")
def regrade():
    """
    Re-run the grader for one date, then re-push grades to the DB.

    Distinct from /admin/grade-backfill, which imports already-graded results
    out of the R2 analysis JSONs. If that JSON was itself produced by a short
    grading run, re-importing it just reproduces the same short result. This
    route regrades from source: run_analysis.run() now picks whichever of the
    picks CSV or the DB holds more picks, so a truncated CSV no longer wins.

    Usage: /admin/regrade?date=2026-07-20
    Costs no Odds API quota — grading pulls results from MLB/ESPN only.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/regrade")

    date_str = request.args.get("date")
    if not date_str or len(date_str) != 10:
        return Response(
            "<h2>Missing date</h2><p>Usage: "
            "<code>/admin/regrade?date=YYYY-MM-DD</code></p>",
            mimetype="text/html"), 400

    import threading

    def _run():
        try:
            from run_analysis import run as grade_run
            result = grade_run(date_str)
            n = len(result.get("graded_picks", [])) if isinstance(result, dict) else 0
            log.info(f"regrade {date_str}: {n} picks graded")
        except Exception as e:
            import traceback
            log.warning(f"regrade {date_str} failed: {traceback.format_exc()}")

    threading.Thread(target=_run, daemon=True).start()
    return Response(
        f"<h2>Regrading {date_str}</h2>"
        f"<p>Pulling picks from whichever source has more (CSV vs DB), "
        f"refetching results, and pushing grades. Give it ~60 seconds.</p>"
        f"<p>Then check <a href='/db-diag'>/db-diag</a> — the count for "
        f"{date_str} should rise.</p>"
        f"<p>Watch the Railway logs for <code>Grading source:</code> to see "
        f"which source won.</p>",
        mimetype="text/html")


@app.route("/admin/grade-backfill")
def grade_backfill():
    """Run grader for any pick dates that have no graded results."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/grade-backfill")
    import threading
    def _run():
        """
        Rebuild the picks table from the graded analysis JSONs in R2.

        R2 is the authoritative record: the pipeline has uploaded a graded
        mlb_analysis_<date>.json every morning, while DB writes were silently
        failing whenever the connection pool was exhausted. Those JSONs already
        contain the grades, so this is a pure import -- no MLB/ESPN refetch and
        no Odds API usage.

        push_grades_to_db() only UPDATEs, so dates whose 6am save_picks failed
        have no rows to grade. This inserts what is missing, then grades it.

        Idempotent: matches on (pick_date, game, pick_type, label) rather than
        the table's UNIQUE (pick_date, game_id, pick_type), because game_id does
        not survive into graded_picks and NULL never conflicts in Postgres.
        """
        import json
        from db.csv_sync import _get_client, _bucket
        from db.connection import db_conn

        client = _get_client()
        if client is None:
            log.warning("r2-backfill: no storage client -- check STORAGE_* env vars")
            return
        bucket = _bucket()

        keys, token = [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": "picks/mlb_analysis_"}
            if token:
                kw["ContinuationToken"] = token
            resp = client.list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents", [])]
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        keys.sort()
        log.info(f"r2-backfill: {len(keys)} analysis files found in R2")

        today = datetime.now(ET).strftime("%Y-%m-%d")
        inserted = updated = skipped = files = 0

        with db_conn() as conn:
            if conn is None:
                log.warning("r2-backfill: no DB connection")
                return
            cur = conn.cursor()
            for key in keys:
                date_str = key.replace("picks/mlb_analysis_", "").replace(".json", "")
                if len(date_str) != 10 or date_str >= today:
                    continue
                try:
                    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
                    data = json.loads(body)
                except Exception as e:
                    log.warning(f"r2-backfill: {key} unreadable: {e}")
                    continue
                file_ins = file_upd = 0
                for gp in data.get("graded_picks", []):
                    result = gp.get("result", "")
                    game   = gp.get("game", "")
                    label  = gp.get("label", "")
                    ptype  = (gp.get("type") or "ML").upper()
                    if result not in ("WIN", "LOSS", "PUSH") or not game or not label:
                        skipped += 1
                        continue
                    cur.execute(
                        "SELECT id, actual_result FROM picks "
                        "WHERE pick_date=%s AND game=%s AND pick_type=%s AND label=%s",
                        (date_str, game, ptype, label),
                    )
                    row = cur.fetchone()
                    if row:
                        if row[1] != result:
                            cur.execute(
                                "UPDATE picks SET actual_result=%s, graded_at=NOW() WHERE id=%s",
                                (result, row[0]),
                            )
                            file_upd += 1
                    else:
                        # The analysis JSON stores conf on a 0-100 scale, while
                        # live save_picks stores 0-1. Normalize here so the picks
                        # table has ONE scale (0-1) — otherwise confidence-band
                        # calibration pools two scales and is meaningless.
                        _bf_conf = float(gp.get("conf") or 0)
                        if _bf_conf > 1.5:
                            _bf_conf = _bf_conf / 100.0
                        cur.execute(
                            "INSERT INTO picks (pick_date, game, pick_type, label, team, "
                            "conf, tier, reasoning, actual_result, graded_at) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                            (date_str, game, ptype, label, gp.get("team"),
                             round(_bf_conf, 4), (gp.get("tier") or "LEAN").upper(),
                             gp.get("reasoning", ""), result),
                        )
                        file_ins += 1
                # Commit per file. A single transaction across all ~60 files
                # means one bad row silently discards the entire backfill,
                # because db_conn() rolls back and logs the error as non-fatal.
                try:
                    conn.commit()
                    files += 1
                    inserted += file_ins
                    updated  += file_upd
                except Exception as e:
                    conn.rollback()
                    log.warning(f"r2-backfill: {date_str} rolled back: {e}")

        log.info(f"r2-backfill done: {files} files, {inserted} inserted, "
                 f"{updated} updated, {skipped} skipped")
    threading.Thread(target=_run, daemon=True).start()
    return Response("<h2>Grade backfill started</h2><p>Check /db-diag in ~60 seconds to see updated counts.</p><p><a href='/db-diag'>→ /db-diag</a></p>", mimetype="text/html")


@app.route("/admin/fix-conf-scale")
def fix_conf_scale():
    """
    One-time repair: backfilled picks stored confidence on a 0-100 scale while
    live picks use 0-1, so the picks table has two scales mixed. Any conf > 1.5
    is a 0-100 row (live max is ~0.90); divide those by 100. Idempotent — rerun
    is a no-op once normalized. Read-only-safe: only touches the conf column.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/fix-conf-scale")
    from db.connection import db_conn
    try:
        with db_conn() as conn:
            if conn is None:
                return Response("<h2>No DB connection</h2>", mimetype="text/html"), 500
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM picks WHERE conf > 1.5")
            before = cur.fetchone()[0]
            cur.execute("UPDATE picks SET conf = conf / 100.0 WHERE conf > 1.5")
            conn.commit()
            cur.execute("SELECT COUNT(*) FROM picks WHERE conf > 1.5")
            after = cur.fetchone()[0]
        return Response(
            f"<body style='background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:40px'>"
            f"<h2>Confidence scale normalized</h2>"
            f"<p>Rows on the 0-100 scale before: <b>{before}</b></p>"
            f"<p>Remaining after fix (should be 0): <b>{after}</b></p>"
            f"<p><a href='/admin/calibration' style='color:#58a6ff'>→ Calibration</a> · "
            f"<a href='/admin/analysis' style='color:#58a6ff'>Analysis</a></p></body>",
            mimetype="text/html")
    except Exception as e:
        import traceback
        return Response(f"<pre>{traceback.format_exc()}</pre>", mimetype="text/html"), 500


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


@app.route("/schedule-status")
def schedule_status():
    """JSON endpoint: next scheduled pipeline run, adaptive refresh, first pitch."""
    now = datetime.now(ET)
    def _fmt(dt):
        if dt is None:
            return None
        diff = (dt - now).total_seconds()
        if diff < 0:
            return {"time": dt.strftime("%-I:%M %p ET"), "in_seconds": int(diff), "label": "passed"}
        h = int(diff // 3600)
        m = int((diff % 3600) // 60)
        label = f"in {h}h {m}m" if h > 0 else f"in {m}m"
        return {"time": dt.strftime("%-I:%M %p ET"), "in_seconds": int(diff), "label": label}

    return jsonify({
        "next_pipeline":   _fmt(_schedule_state.get("next_pipeline_et")),
        "next_refresh":    _fmt(_schedule_state.get("next_refresh_et")),
        "first_pitch":     _fmt(_schedule_state.get("first_pitch_et")),
    })


@app.route("/db-diag")
def db_diag():
    """Quick DB diagnostic — picks table counts and dates."""
    try:
        from db.connection import db_conn as _db_conn
        with _db_conn() as conn:
            if conn is None:
                return {"error": "No DB connection"}, 503
            cur = conn.cursor()
            cur.execute("SELECT actual_result, COUNT(*) FROM picks GROUP BY actual_result ORDER BY actual_result")
            picks_by_status = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("SELECT DISTINCT pick_date FROM picks ORDER BY pick_date DESC LIMIT 10")
            recent_dates = [str(r[0]) for r in cur.fetchall()]
            cur.execute("SELECT run_date, status, completed_at FROM pipeline_runs ORDER BY run_date DESC LIMIT 7")
            cols = [d[0] for d in cur.description]
            pipeline_runs = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute("""
                SELECT pick_date, COUNT(*) as total,
                       COUNT(*) FILTER (WHERE actual_result='WIN') as wins,
                       COUNT(*) FILTER (WHERE actual_result='LOSS') as losses
                FROM picks
                WHERE pick_date >= CURRENT_DATE - INTERVAL '30 days'
                  AND actual_result != 'PENDING'
                GROUP BY pick_date ORDER BY pick_date DESC
            """)
            cols2 = [d[0] for d in cur.description]
            graded_by_date = [dict(zip(cols2, r)) for r in cur.fetchall()]
        return {"picks_by_status": picks_by_status, "recent_pick_dates": recent_dates,
                "pipeline_runs": pipeline_runs, "graded_by_date": graded_by_date}
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/admin/calibration")
def calibration():
    """
    Calibration report: does a 70% pick actually win 70%?

    Buckets every graded pick by predicted confidence and compares predicted
    vs actual win rate, with a 95% confidence interval on each bucket so small
    samples are not mistaken for signal. Breakdowns by pick_type and tier.

    Read-only. Never writes.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/calibration")

    import math
    from db.connection import db_conn

    BREAK_EVEN = 52.38  # -110 juice
    MIN_N      = 200    # below this, treat differences as noise

    try:
        with db_conn() as conn:
            if conn is None:
                return Response("<h2>No DB connection</h2>", mimetype="text/html"), 503
            cur = conn.cursor()
            # market_signal is present in production but absent from schema.py,
            # so it may not exist on a DB rebuilt via create_all(). Degrade
            # gracefully instead of 500-ing the whole report.
            base = ("SELECT pick_type, tier, conf, actual_result{extra} "
                    "FROM picks WHERE actual_result IN ('WIN','LOSS','PUSH') "
                    "AND conf IS NOT NULL")
            try:
                cur.execute(base.format(extra=", market_signal"))
                rows = cur.fetchall()
            except Exception:
                conn.rollback()
                cur = conn.cursor()
                cur.execute(base.format(extra=""))
                rows = [r + (None,) for r in cur.fetchall()]
    except Exception as e:
        return Response(f"<h2>Query failed</h2><pre>{e}</pre>", mimetype="text/html"), 500

    if not rows:
        return Response(
            "<h2>No graded picks</h2><p>Run /admin/grade-backfill first.</p>",
            mimetype="text/html")

    def norm(c):
        c = float(c or 0)
        return c * 100 if c <= 1 else c

    BANDS = [(0,50),(50,55),(55,60),(60,65),(65,70),(70,75),(75,80),(80,101)]

    def bucketize(subset):
        out = []
        for lo, hi in BANDS:
            sel = [r for r in subset if lo <= norm(r[2]) < hi]
            if not sel:
                continue
            w = sum(1 for r in sel if r[3] == "WIN")
            l = sum(1 for r in sel if r[3] == "LOSS")
            pu = sum(1 for r in sel if r[3] == "PUSH")
            n = w + l
            if n == 0:
                continue
            actual = w / n * 100
            pred   = sum(norm(r[2]) for r in sel) / len(sel)
            ci     = 1.96 * math.sqrt((actual/100) * (1 - actual/100) / n) * 100
            out.append({"band": f"{lo}-{hi if hi<101 else '100'}%", "n": n, "push": pu,
                        "pred": pred, "actual": actual, "gap": actual - pred, "ci": ci})
        return out

    def table(title, buckets, note=""):
        if not buckets:
            return ""
        h = [f'<h3>{title}</h3>']
        if note:
            h.append(f'<p class="note">{note}</p>')
        h.append('<table><tr><th>Conf band</th><th>N</th><th>Predicted</th>'
                 '<th>Actual</th><th>Gap</th><th>95% CI</th><th></th></tr>')
        for b in buckets:
            gap_cls = "good" if b["gap"] >= -1 else ("bad" if b["gap"] < -5 else "warn")
            thin    = '<span class="thin">thin sample</span>' if b["n"] < MIN_N else ""
            h.append(
                f'<tr><td>{b["band"]}</td><td>{b["n"]}</td>'
                f'<td>{b["pred"]:.1f}%</td><td>{b["actual"]:.1f}%</td>'
                f'<td class="{gap_cls}">{b["gap"]:+.1f}</td>'
                f'<td>&plusmn;{b["ci"]:.1f}</td><td>{thin}</td></tr>')
        h.append('</table>')
        return "".join(h)

    parts = [table("All graded picks", bucketize(rows))]

    for pt in sorted({r[0] for r in rows if r[0]}):
        sub = [r for r in rows if r[0] == pt]
        parts.append(table(f"Pick type: {pt}", bucketize(sub),
                           f"{len(sub)} graded"))

    for tr in sorted({r[1] for r in rows if r[1]}):
        sub = [r for r in rows if r[1] == tr]
        parts.append(table(f"Tier: {tr}", bucketize(sub), f"{len(sub)} graded"))

    sig = {}
    for r in rows:
        k = r[4] or "NONE"
        d = sig.setdefault(k, [0, 0])
        if r[3] == "WIN":  d[0] += 1
        if r[3] == "LOSS": d[1] += 1
    sig_rows = "".join(
        f'<tr><td>{k}</td><td>{v[0]}-{v[1]}</td>'
        f'<td>{(v[0]/(v[0]+v[1])*100 if v[0]+v[1] else 0):.1f}%</td></tr>'
        for k, v in sorted(sig.items()))

    tot_w = sum(1 for r in rows if r[3] == "WIN")
    tot_l = sum(1 for r in rows if r[3] == "LOSS")
    tot_wr = tot_w / (tot_w + tot_l) * 100 if tot_w + tot_l else 0

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Statalizers - Calibration</title><style>
body{{background:#0d1117;color:#c9d1d9;font-family:system-ui,-apple-system,sans-serif;
padding:24px;max-width:1000px;margin:0 auto}}
h2{{color:#58a6ff}} h3{{color:#79c0ff;margin-top:28px;border-bottom:1px solid #21262d;padding-bottom:6px}}
table{{border-collapse:collapse;width:100%;margin:10px 0 18px}}
th,td{{padding:7px 10px;text-align:left;border-bottom:1px solid #21262d;font-size:14px}}
th{{color:#8b949e;font-weight:600;font-size:12px;text-transform:uppercase}}
.good{{color:#3fb950}} .warn{{color:#d29922}} .bad{{color:#f85149}}
.thin{{color:#6e7681;font-size:11px;font-style:italic}}
.note{{color:#8b949e;font-size:13px;margin:4px 0}}
.hdr{{background:#161b22;padding:14px 18px;border-radius:8px;margin-bottom:8px}}
a{{color:#58a6ff}}</style></head><body>
<h2>Model Calibration</h2>
<div class="hdr">
<b>{tot_w}-{tot_l}</b> overall &middot; <b>{tot_wr:.1f}%</b> win rate &middot;
break-even <b>{BREAK_EVEN}%</b> at -110<br>
<span class="note">Gap = actual minus predicted. Negative means overconfident.
Buckets under {MIN_N} picks are flagged; their confidence intervals are too wide
to act on. Pushes excluded from win rate.</span>
</div>
{''.join(parts)}
<h3>Market signal</h3>
<table><tr><th>Signal</th><th>W-L</th><th>Win %</th></tr>{sig_rows}</table>
<p><a href="/admin">&larr; Admin</a> &middot; <a href="/performance-html">Performance</a></p>
</body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/admin/refresh-signals")
def refresh_signals():
    """
    Regenerate the three signal sources that were dead, without running the full
    pipeline (so zero Odds API usage).

      - mlb_umpires_<today>.json        (umpire scraper)
      - mlb_bullpen_fatigue_<today>.json (fatigue scraper)
      - mlb_pitcher_platoon_master.csv  (pitcher scraper + normalizer)

    Then uploads to R2 so a restart cannot lose them again, and rebuilds the
    dashboard so today's picks reflect the restored signals.

    None of these scrapers touch the Odds API. Verified: zero references to
    ODDS_API_KEY or the-odds-api in any of the three.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/refresh-signals")

    import threading
    today = datetime.now(ET).strftime("%Y-%m-%d")

    def _run():
        results = []

        try:
            from scrapers.mlb_umpire_scraper import run as _umps
            n = _umps(today)
            results.append(f"umpires: {len(n) if hasattr(n, '__len__') else n}")
        except Exception as e:
            results.append(f"umpires FAILED: {e}")

        try:
            from scrapers.mlb_bullpen_fatigue_scraper import run as _fatigue
            n = _fatigue(today)
            results.append(f"bullpen_fatigue: {len(n) if hasattr(n, '__len__') else n}")
        except Exception as e:
            results.append(f"bullpen_fatigue FAILED: {e}")

        try:
            from scrapers.mlb_pitcher_scraper import run as _pitchers
            r = _pitchers()
            results.append(f"pitcher_scrape: {r}")
        except Exception as e:
            results.append(f"pitcher_scrape FAILED: {e}")

        try:
            from normalize.mlb_pitcher_normalize import run as _pnorm
            r = _pnorm()
            results.append(f"pitcher_normalize: {r}")
        except Exception as e:
            results.append(f"pitcher_normalize FAILED: {e}")

        # Team hitting/pitching — pulls current season (SEASONS now includes 2026).
        # Master was frozen at 2023-2025 because this scraper is not in the daily
        # pipeline; without it K props score on stale opponent K-rates.
        try:
            from scrapers.mlb_team_scraper import run as _teams
            tr = _teams()
            results.append(f"team_scrape: {tr}")
        except Exception as e:
            results.append(f"team_scrape FAILED: {e}")

        try:
            from normalize.mlb_historical_normalize import normalize_team_stats as _tnorm
            th = _tnorm("hitting")
            tp = _tnorm("pitching")
            results.append(f"team_normalize: hitting +{th}, pitching +{tp}")
        except Exception as e:
            results.append(f"team_normalize FAILED: {e}")

        # Rebuild the pitcher STATS master with the current season (SEASONS now
        # includes 2026). The scraper writes raw mlb_pitcher_stats_2026.csv above,
        # but only the TEAM normalize was wired here — so 2026 pitchers (call-ups
        # like Jake Bennett) never reached the master and got NO K projection.
        try:
            from normalize.mlb_historical_normalize import normalize_pitcher_stats as _psnorm
            ps = _psnorm()
            results.append(f"pitcher_stats_normalize: +{ps} rows")
        except Exception as e:
            results.append(f"pitcher_stats_normalize FAILED: {e}")

        try:
            from db.csv_sync import upload_all as _up
            results.append(f"uploaded: {_up()} file(s)")
        except Exception as e:
            results.append(f"upload FAILED: {e}")

        log.info(f"refresh-signals done: {results}")

        # Rebuild so today's cards reflect the restored signals.
        with _cache_lock:
            _cache["generated_at"] = 0
            _cache["generating"]   = False
        _regenerate_in_background()

    threading.Thread(target=_run, daemon=True).start()
    return Response(
        "<h2>Signal refresh started</h2>"
        "<p>Running umpire, bullpen-fatigue and pitcher/platoon scrapers, then "
        "uploading to R2 and rebuilding the dashboard. Zero Odds API usage.</p>"
        "<p>Give it 2-4 minutes, then check "
        "<a href='/admin/signal-audit'>/admin/signal-audit</a> — the "
        "CONSTANT/MISSING count should drop.</p>",
        mimetype="text/html")


@app.route("/admin/platoon-debug")
def platoon_debug():
    """
    Why do the four platoon fields return None when the master CSV has rows?

    Dumps, side by side:
      - how many pitchers loaded into self.pitcher_platoon
      - a sample of those dict keys (the names as stored)
      - today's starting pitcher names as the schedule provides them
      - the exact lookup result per starter

    Read-only. Answers the name-matching question directly instead of inferring.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/platoon-debug")

    import html as _html
    date_str = request.args.get("date") or datetime.now(ET).strftime("%Y-%m-%d")

    try:
        from model.mlb_model import MLBModel
        model = MLBModel()
        model.load()
    except Exception:
        import traceback
        return Response(f"<pre>{_html.escape(traceback.format_exc())}</pre>",
                        mimetype="text/html"), 500

    pp = model.pitcher_platoon
    keys = list(pp.keys())

    # raw CSV inspection — does the file even have rows, and what is in `split`?
    import csv as _csv, os as _os
    csv_path = _os.path.join(BASE_DIR, "data", "clean", "mlb_pitcher_platoon_master.csv")
    csv_rows, csv_splits, csv_names, csv_seasons = 0, {}, [], {}
    if _os.path.exists(csv_path):
        with open(csv_path, encoding="utf-8") as f:
            for r in _csv.DictReader(f):
                csv_rows += 1
                s = (r.get("split") or "").strip()
                csv_splits[s] = csv_splits.get(s, 0) + 1
                se = (r.get("season") or "").strip()
                csv_seasons[se] = csv_seasons.get(se, 0) + 1
                if len(csv_names) < 8:
                    csv_names.append(r.get("player_name", ""))

    games = [g for g in model.schedule if g.get("game_date") == date_str]
    rows_html = []
    for g in games[:15]:
        for side in ("away", "home"):
            name = (g.get(f"{side}_probable_pitcher") or "").strip()
            if not name:
                continue
            in_dict = name in pp
            l = model.get_platoon(name, "vs. Left")
            r = model.get_platoon(name, "vs. Right")
            seasons = sorted(pp.get(name, {}).keys()) if in_dict else []
            splits  = sorted(pp[name][seasons[-1]].keys()) if seasons else []
            rows_html.append(
                f"<tr><td>{_html.escape(name)}</td>"
                f"<td class='{'ok' if in_dict else 'bad'}'>{in_dict}</td>"
                f"<td>{seasons}</td><td>{splits}</td>"
                f"<td class='{'ok' if l else 'bad'}'>{'era=' + str(l.get('era')) if l else 'EMPTY'}</td>"
                f"<td class='{'ok' if r else 'bad'}'>{'era=' + str(r.get('era')) if r else 'EMPTY'}</td></tr>")

    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Platoon Debug</title>
<style>body{{background:#0d1117;color:#c9d1d9;font-family:ui-monospace,monospace;
padding:22px;max-width:1150px;margin:0 auto;font-size:13px}}
h2,h3{{color:#58a6ff;font-family:system-ui}} td,th{{padding:5px 9px;
border-bottom:1px solid #21262d;text-align:left}} table{{border-collapse:collapse;width:100%}}
.ok{{color:#3fb950}} .bad{{color:#f85149}} .box{{background:#161b22;padding:12px 16px;
border-radius:8px;margin:10px 0}}</style></head><body>
<h2>Platoon Debug — {date_str}</h2>

<div class="box">
<b>CSV on disk:</b> {'FOUND' if csv_rows else 'MISSING OR EMPTY'} &middot;
{csv_rows} data rows<br>
<b>split values:</b> {csv_splits}<br>
<b>seasons:</b> {csv_seasons}<br>
<b>first names in CSV:</b> {[_html.escape(str(n)) for n in csv_names]}
</div>

<div class="box">
<b>model.pitcher_platoon:</b> {len(keys)} pitchers loaded<br>
<b>sample keys:</b> {[_html.escape(str(k)) for k in keys[:8]]}
</div>

<h3>Today's starters vs the loaded dict</h3>
<table><tr><th>schedule name</th><th>in dict?</th><th>seasons</th><th>split keys</th>
<th>vs. Left</th><th>vs. Right</th></tr>{''.join(rows_html) or '<tr><td colspan=6>no games</td></tr>'}</table>
<p><a href="/admin">&larr; Admin</a></p></body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/admin/pinnacle-k-test")
def pinnacle_k_test():
    """Verify fetch_strikeout_lines() parses real pitcher K lines + prices from
    Pinnacle (live, on Railway). Read-only. Gate before wiring into scoring."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/pinnacle-k-test")
    import html as _h
    try:
        from scrapers.mlb_pinnacle_scraper import fetch_strikeout_lines
        lines = fetch_strikeout_lines()
    except Exception as e:
        import traceback
        return Response(f"<pre>{_h.escape(traceback.format_exc())}</pre>",
                        mimetype="text/html"), 500
    rows = "".join(
        f"<tr><td>{_h.escape(p)}</td><td>{d.get('line')}</td>"
        f"<td>{d.get('over_price')}</td><td>{d.get('under_price')}</td></tr>"
        for p, d in sorted(lines.items()))
    ok = all(d.get("line") is not None and d.get("over_price") is not None
             and d.get("under_price") is not None for d in lines.values()) and bool(lines)
    verdict = ("✅ Parser works — line + both prices present" if ok
               else "⚠️ Missing line or prices on some rows — inspect below")
    html = f"""<!doctype html><html><head><meta charset=utf-8><title>Pinnacle K test</title>
<style>body{{background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:22px;max-width:720px;margin:0 auto}}
h2{{color:#58a6ff}} td,th{{padding:5px 12px;border-bottom:1px solid #21262d;font-size:13px;text-align:left}}
.v{{background:#161b22;padding:12px 16px;border-radius:8px;font-weight:700;margin:10px 0}}</style></head><body>
<h2>Pinnacle strikeout lines — parser check</h2>
<div class="v">{verdict}</div>
<p><b>{len(lines)}</b> pitcher K lines parsed.</p>
<table><tr><th>Pitcher</th><th>Line</th><th>Over</th><th>Under</th></tr>{rows}</table>
<p><a href="/admin">&larr; Admin</a></p></body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/admin/pinnacle-test")
def pinnacle_test():
    """
    One-off: does Pinnacle's guest API return CURRENT games + strikeout props
    when called WITH the scraper's real headers (Origin/Referer)? Sandbox fetches
    can't send those headers and got a stale May cache, so this must run on
    Railway to be trusted. Read-only, no writes, no Odds API.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/pinnacle-test")
    import html as _h
    from collections import Counter
    try:
        from scrapers.mlb_pinnacle_scraper import fetch_matchups
        m = fetch_matchups() or []
    except Exception as e:
        import traceback
        return Response(f"<pre>{_h.escape(traceback.format_exc())}</pre>",
                        mimetype="text/html"), 500

    dates = Counter()
    k_specials = []
    for x in m:
        if not isinstance(x, dict):
            continue
        st = str(x.get("startTime", ""))[:10]
        if st:
            dates[st] += 1
        if x.get("units") == "Strikeouts":
            desc = (x.get("special") or {}).get("description", "")
            k_specials.append((st, desc))

    today = datetime.now(ET).strftime("%Y-%m-%d")
    rows = "".join(f"<tr><td>{d}</td><td>{n}</td></tr>" for d, n in sorted(dates.items()))
    ks   = "".join(f"<tr><td>{_h.escape(st)}</td><td>{_h.escape(dc)}</td></tr>"
                   for st, dc in k_specials[:40])
    verdict = ("✅ LIVE — today is present" if today in dates
               else "⚠️ STALE — today NOT in the feed (dates: "
                    + ", ".join(sorted(dates)) + ")")
    html = f"""<!doctype html><html><head><meta charset=utf-8><title>Pinnacle test</title>
<style>body{{background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:22px;max-width:820px;margin:0 auto}}
h2{{color:#58a6ff}} td{{padding:5px 10px;border-bottom:1px solid #21262d;font-size:13px}}
.v{{background:#161b22;padding:12px 16px;border-radius:8px;font-weight:700;margin:10px 0}}</style></head><body>
<h2>Pinnacle guest API — live check ({today} ET)</h2>
<div class="v">{verdict}</div>
<p><b>{len(m)}</b> matchups, <b>{len(k_specials)}</b> strikeout specials.</p>
<h3>Matchups by start date</h3><table>{rows}</table>
<h3>Strikeout specials (first 40)</h3><table><tr><th>date</th><th>pitcher</th></tr>{ks}</table>
<p><a href="/admin">&larr; Admin</a></p></body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/admin/analysis")
def admin_analysis():
    """On-demand nightly analysis. ?date=YYYY-MM-DD (default yesterday).
    ?download=1 returns the report as a downloadable .md file."""
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/analysis")
    import html as _h
    from datetime import datetime as _dt, timedelta as _td
    date_str = request.args.get("date") or (
        (datetime.now(ET) - _td(days=1)).strftime("%Y-%m-%d"))
    try:
        from analysis_report import build_report
        rep = build_report(date_str)
    except Exception as e:
        import traceback
        return Response(f"<pre>{_h.escape(traceback.format_exc())}</pre>",
                        mimetype="text/html"), 500

    narrative = rep.get("narrative", "")
    data_text = rep.get("data_text", "")
    gen       = rep.get("generated_at", "")

    if request.args.get("email"):
        from analysis_report import email_report
        sent = email_report(date_str)
        msg = ("✅ Report emailed." if sent else
               "⚠️ Email not sent — check ALERT_EMAIL_* env vars in Railway.")
        return Response(
            f"<body style='background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:40px'>"
            f"<p>{msg}</p><p><a href='/admin/analysis?date={date_str}' "
            f"style='color:#58a6ff'>&larr; Back to report</a></p></body>",
            mimetype="text/html")

    if request.args.get("download"):
        md = (f"# Statalizers Analysis — {date_str}\n_Generated {gen}_\n\n"
              f"{narrative}\n\n---\n\n## Raw data\n```\n{data_text}\n```\n")
        return Response(md, mimetype="text/markdown",
                        headers={"Content-Disposition":
                                 f'attachment; filename="statalizers_analysis_{date_str}.md"'})

    # simple markdown-ish -> HTML (headings + line breaks)
    import re as _re
    def _fmt(t):
        t = _h.escape(t or "")
        t = _re.sub(r'(?m)^(\d+\.\s+[A-Z][^\n]*)$',
                    r'<h3 style="color:#58a6ff;margin:14px 0 6px">\1</h3>', t)
        return t.replace("\n\n", "</p><p>").replace("\n", "<br>")
    body = _fmt(narrative)

    # ── Claude-vs-Gemini debate over the same data ───────────────────────────
    debate_html = ""
    try:
        from analysis_report import build_debate
        deb = build_debate(data_text, narrative)
        gem = _fmt(deb.get("gemini_challenge", ""))
        resp = deb.get("claude_response", "")
        debate_html = (
            "<h2 style='margin-top:30px'>🔴 Second Opinion — Claude vs Gemini</h2>"
            "<div style='color:#6e7681;font-size:.82rem;margin-bottom:10px'>Same data, two model "
            "families. Gemini red-teams Claude's read; Claude answers back. Agreement = trust it; "
            "a real split = your flag for the day.</div>"
            f"<div class='voice v-gemini'><div class='voice-h'>🟠 Gemini challenges the read</div><p>{gem}</p></div>"
            + (f"<div class='voice v-claude'><div class='voice-h'>🔵 Claude responds</div><p>{_fmt(resp)}</p></div>" if resp else ""))
    except Exception as e:
        debate_html = f"<h2 style='margin-top:30px'>🔴 Second Opinion</h2><p style='color:#f85149'>Debate unavailable: {_h.escape(str(e))}</p>"

    html = f"""<!doctype html><html><head><meta charset=utf-8><title>Analysis {date_str}</title>
<style>body{{background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:24px;
max-width:820px;margin:0 auto;line-height:1.55}}h2{{color:#58a6ff}}
a,button{{font-family:inherit}} .bar{{display:flex;gap:10px;align-items:center;margin:8px 0 18px}}
.btn{{background:#238636;color:#fff;border:none;padding:8px 16px;border-radius:8px;
font-weight:700;cursor:pointer;text-decoration:none;font-size:.9rem}}
.date{{background:#161b22;border:1px solid #30363d;color:#c9d1d9;border-radius:8px;padding:7px 10px}}
pre{{background:#161b22;padding:14px;border-radius:8px;overflow:auto;font-size:12px;color:#8b949e}}
p{{margin:6px 0}}
.voice{{border-left:3px solid #30363d;padding:12px 16px;margin:10px 0;background:#161b22;border-radius:8px}}
.voice-h{{font-weight:700;font-size:.85rem;margin-bottom:6px}}
.v-claude{{border-left-color:#58a6ff}} .v-claude .voice-h{{color:#58a6ff}}
.v-gemini{{border-left-color:#f0883e}} .v-gemini .voice-h{{color:#f0883e}}</style></head><body>
<h2>Statalizers Analysis — {date_str}</h2>
<div style="color:#6e7681;font-size:.82rem">Generated {gen}</div>
<form class="bar" method="get" action="/admin/analysis">
  <input class="date" type="date" name="date" value="{date_str}">
  <button class="btn" type="submit">Run</button>
  <a class="btn" style="background:#1f6feb"
     href="/admin/analysis?date={date_str}&download=1">⬇ Download .md</a>
  <a class="btn" style="background:#8957e5"
     href="/admin/analysis?date={date_str}&email=1">✉ Email me</a>
</form>
<h2 style="margin-top:24px">🔵 Claude's read</h2>
<p>{body}</p>
{debate_html}
<details style="margin-top:24px"><summary style="cursor:pointer;color:#8b949e">Raw data</summary>
<pre>{_h.escape(data_text)}</pre></details>
<p style="margin-top:20px"><a href="/admin" style="color:#58a6ff">&larr; Admin</a></p>
</body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/admin/signal-audit")
def signal_audit():
    """
    Signal audit: for every model input, measure whether it actually VARIES
    across today's slate.

    A signal that returns the same value for all games contributes nothing to
    pick differentiation no matter how it is weighted. A signal pinned at a
    clamp boundary for every game is broken (see the k_rate bug, where every
    team maxed the 1.4 multiplier). This route distinguishes "working",
    "constant", and "all-default" without guessing.

    Read-only. Scores the slate in memory; writes nothing.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/signal-audit")

    import statistics as _stats

    date_str = request.args.get("date") or datetime.now(ET).strftime("%Y-%m-%d")

    try:
        from model.mlb_model import MLBModel
        model = MLBModel()
        model.load()
        games = [g for g in model.schedule if g.get("game_date") == date_str]
        seen, sched = set(), []
        for g in games:
            k = (g.get("away_team", ""), g.get("home_team", ""))
            if k not in seen:
                seen.add(k)
                sched.append(g)
        scored = [model.score_game(g) for g in sched]
    except Exception as e:
        import traceback
        return Response(f"<h2>Scoring failed</h2><pre>{traceback.format_exc()}</pre>",
                        mimetype="text/html"), 500

    if not scored:
        # Diagnose rather than dead-end: show what the loaded schedule actually
        # contains so a date mismatch is distinguishable from an empty load.
        from collections import Counter as _C
        counts = _C(g.get("game_date", "?") for g in model.schedule)
        near = sorted([d for d in counts if d >= date_str])[:5]
        far  = sorted(counts)[-5:]
        rows = "".join(
            f"<tr><td>{d}</td><td>{counts[d]} games</td></tr>"
            for d in sorted(set(near) | set(far))
        )
        return Response(f"""<!doctype html><html><head><meta charset="utf-8">
<title>Signal Audit</title><style>body{{background:#0d1117;color:#c9d1d9;
font-family:system-ui,sans-serif;padding:24px;max-width:800px;margin:0 auto}}
h2{{color:#58a6ff}} td{{padding:5px 12px;border-bottom:1px solid #21262d}}
a{{color:#58a6ff}} code{{color:#79c0ff}}</style></head><body>
<h2>No games scored for {date_str}</h2>
<p>Schedule loaded <b>{len(model.schedule)}</b> total rows across
<b>{len(counts)}</b> distinct dates.</p>
<p>Nearest and latest dates present:</p><table>{rows}</table>
<p style="color:#8b949e">If the dates look right but today is absent, the schedule
CSV on this container is stale. If the total row count is 0, <code>model.load()</code>
found no CSV at all. Retry a listed date with
<code>/admin/signal-audit?date=YYYY-MM-DD</code>.</p>
<p><a href="/admin">&larr; Admin</a></p></body></html>""", mimetype="text/html")

    # Signals grouped by the subsystem that feeds them, so a dead scraper is obvious.
    GROUPS = {
        "Park":            ["park_runs", "park_hr"],
        "Starting pitcher":["away_sp_era", "home_sp_era", "away_sp_fip", "home_sp_fip",
                            "away_sp_whip", "home_sp_whip", "away_sp_k9", "home_sp_k9"],
        "SP recent form":  ["away_sp_r_era", "home_sp_r_era", "away_sp_gs", "home_sp_gs",
                            "away_sp_trend", "home_sp_trend"],
        "Platoon splits":  ["away_era_vs_lhb", "away_era_vs_rhb",
                            "home_era_vs_lhb", "home_era_vs_rhb"],
        "Team offense":    ["away_rpg", "home_rpg", "away_ops", "home_ops",
                            "away_form_rpg", "home_form_rpg",
                            "away_form_wpct", "home_form_wpct"],
        "Bullpen":         ["away_bp_era", "home_bp_era", "away_bp_whip", "home_bp_whip",
                            "away_bp_found", "home_bp_found"],
        "Bullpen fatigue": ["away_fatigue_tier", "home_fatigue_tier",
                            "away_bp_pitches_1d", "home_bp_pitches_1d"],
        "Umpire":          ["hp_ump", "ump_factor", "ump_rpg"],
        "Weather":         ["weather_flag", "wind_component", "wind_label",
                            "wind_speed", "temp_f", "precip_prob", "has_roof"],
        "Lineups":         ["away_lineup_ops", "home_lineup_ops", "lineup_confirmed"],
        "Rest":            ["away_rest", "home_rest"],
        "Odds / movement": ["ml_away_odds", "ml_home_odds", "total_odds_line",
                            "ml_signal", "total_signal", "sharp_side",
                            "ml_move_away", "total_move"],
        "Polymarket":      ["poly_away_prob", "poly_home_prob",
                            "poly_market_signal", "poly_market_gap"],
        "Kalshi/combined": ["combined_away_prob", "combined_home_prob"],
        "ADJUSTMENTS":     ["ml_adj", "total_adj", "rest_ml_adj", "gap_adj",
                            "market_ml_adj", "conv_adj"],
        "Output":          ["exp_away", "exp_home", "exp_total",
                            "away_wp", "home_wp", "ml_conf"],
    }

    n = len(scored)

    def assess(field):
        vals = [g.get(field) for g in scored]
        present = [v for v in vals if v is not None]
        if not present:
            return {"status": "MISSING", "detail": "all None", "distinct": 0, "sample": ""}
        distinct = len({str(v) for v in present})
        nums = []
        for v in present:
            if isinstance(v, bool):
                nums.append(1.0 if v else 0.0)
            elif isinstance(v, (int, float)):
                nums.append(float(v))
        sample = ", ".join(str(v) for v in present[:3])
        if distinct == 1:
            return {"status": "CONSTANT", "detail": f"every game = {present[0]}",
                    "distinct": 1, "sample": sample}
        if nums and len(nums) == len(present):
            lo, hi = min(nums), max(nums)
            sd = _stats.pstdev(nums) if len(nums) > 1 else 0.0
            return {"status": "OK",
                    "detail": f"min {lo:.3g} / max {hi:.3g} / sd {sd:.3g}",
                    "distinct": distinct, "sample": sample}
        return {"status": "OK", "detail": f"{distinct} distinct values",
                "distinct": distinct, "sample": sample}

    rows = []
    for group, fields in GROUPS.items():
        rows.append(("GROUP", group, "", "", ""))
        for f in fields:
            a = assess(f)
            rows.append(("ROW", f, a["status"], a["detail"], a["sample"]))

    body = []
    for kind, a, b, c, d in rows:
        if kind == "GROUP":
            body.append(f'<tr class="grp"><td colspan="4">{a}</td></tr>')
        else:
            cls = {"OK": "ok", "CONSTANT": "warn", "MISSING": "bad"}.get(b, "")
            body.append(f'<tr><td class="f">{a}</td><td class="{cls}">{b}</td>'
                        f'<td>{c}</td><td class="s">{d}</td></tr>')

    dead = sum(1 for k, a, b, c, d in rows if k == "ROW" and b in ("CONSTANT", "MISSING"))
    total = sum(1 for k, *_ in rows if k == "ROW")

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Signal Audit</title><style>
body{{background:#0d1117;color:#c9d1d9;font-family:system-ui,sans-serif;padding:24px;
max-width:1100px;margin:0 auto}}
h2{{color:#58a6ff}} table{{border-collapse:collapse;width:100%;margin-top:12px}}
td{{padding:6px 10px;border-bottom:1px solid #21262d;font-size:13px;vertical-align:top}}
.grp td{{background:#161b22;color:#79c0ff;font-weight:700;font-size:12px;
text-transform:uppercase;letter-spacing:.5px;padding-top:12px}}
.f{{font-family:ui-monospace,monospace;color:#c9d1d9;width:200px}}
.ok{{color:#3fb950;font-weight:600;width:90px}}
.warn{{color:#d29922;font-weight:600}} .bad{{color:#f85149;font-weight:600}}
.s{{color:#6e7681;font-size:11px;font-family:ui-monospace,monospace}}
.hdr{{background:#161b22;padding:14px 18px;border-radius:8px}}
a{{color:#58a6ff}}</style></head><body>
<h2>Signal Audit — {date_str}</h2>
<div class="hdr"><b>{n} games scored</b> &middot; <b>{dead}/{total}</b> signals are
CONSTANT or MISSING across the slate.<br>
<span style="color:#8b949e;font-size:13px">CONSTANT means every game got the same
value, so the signal cannot differentiate picks regardless of its weight. MISSING
means the field was never populated. Both indicate a broken or unwired input, not
a tuning problem.</span></div>
<table>{''.join(body)}</table>
<p><a href="/admin">&larr; Admin</a> &middot; <a href="/admin/calibration">Calibration</a></p>
</body></html>"""
    return Response(html, mimetype="text/html")


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
    # Odds snapshots only run via the 6am pipeline and adaptive refresh
    # (2 hours before first pitch). No automatic every-2-hour loop.
    next_label = "Adaptive refresh only (2h before first pitch)"
    next_detail = "Manual: statalizers.com/force-odds"

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
    {status_row("Next Snapshot", next_label, None, next_detail)}
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

def _build_perf_from_json(days: int) -> list:
    """Fallback: build perf rows from daily analysis JSON files when DB has no grades."""
    import glob as _glob
    from datetime import date as _d, timedelta as _td
    cutoff = (_d.today() - _td(days=days)).isoformat()
    pattern = os.path.join(BASE_DIR, "picks", "mlb_analysis_*.json")
    rows = []
    agg = {}  # (pick_type, tier) -> {wins, losses, pushes, total_conf}
    for fpath in sorted(_glob.glob(pattern)):
        fname = os.path.basename(fpath)
        date_str = fname.replace("mlb_analysis_", "").replace(".json", "")
        if date_str < cutoff:
            continue
        try:
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)
            for p in data.get("graded_picks", []):
                result = p.get("result", "")
                if result not in ("WIN", "LOSS", "PUSH"):
                    continue
                key = (p.get("type", "ML"), p.get("tier", "LEAN"))
                if key not in agg:
                    agg[key] = {"wins": 0, "losses": 0, "pushes": 0, "conf_sum": 0, "count": 0}
                agg[key]["wins"]   += 1 if result == "WIN"  else 0
                agg[key]["losses"] += 1 if result == "LOSS" else 0
                agg[key]["pushes"] += 1 if result == "PUSH" else 0
                agg[key]["conf_sum"] += float(p.get("conf", 0))
                agg[key]["count"] += 1
        except Exception:
            continue
    for (pick_type, tier), v in agg.items():
        denom = v["wins"] + v["losses"]
        rows.append({
            "pick_type": pick_type, "tier": tier,
            "wins": v["wins"], "losses": v["losses"], "pushes": v["pushes"],
            "win_rate": round(v["wins"]/denom, 3) if denom else None,
            "avg_conf": round(v["conf_sum"]/v["count"], 4) if v["count"] else None,
        })
    return rows


@app.route("/performance")
def performance():
    """Browser → visual dashboard. API clients (Accept: application/json) → JSON."""
    accept = request.headers.get("Accept", "")
    if "text/html" in accept:
        from flask import redirect
        return redirect("/performance-html", code=302)

    # JSON API path (backward compatible)
    try:
        days = int(request.args.get("days", 1))
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

    # Fall back to daily analysis JSON files if DB has no grades yet
    _from_json = False
    if not rows:
        rows = _build_perf_from_json(days) or []
        _from_json = bool(rows)

    try:
        from db.picks_store import get_accuracy_by_market_signal, get_monthly_accuracy
        mkt_signal_rows = get_accuracy_by_market_signal(days=days) or []
        monthly_rows    = get_monthly_accuracy() or []
    except Exception:
        mkt_signal_rows = []
        monthly_rows    = []

    try:
        from db.picks_store import get_sharp_vs_model
        # Always show last 3 days — independent of the page day-toggle.
        # Rolling aggregate is already covered by Market Signal Breakdown above.
        sharp_vs_model_rows = get_sharp_vs_model(days=3) or []
    except Exception:
        sharp_vs_model_rows = []

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
        '<a href="/performance-html?days={d}" {cls}>{label}</a>'.format(
            d=d,
            label=("Yesterday" if d == 1 else f"{d} days"),
            cls='class="active"' if d == days else ""
        )
        for d in [1, 2, 3, 4, 5, 7, 14, 30]
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
        player_prop_rows = get_player_prop_accuracy(days=days, min_picks=3) or []
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
        '<tr><td colspan="5" style="color:#8b949e;padding:14px">Need 3+ picks per player to show.</td></tr>'
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
        '<span class="arr">&#9654;</span> Top Players (3+ picks)</div>'
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

    # ── Sharp vs Model section HTML ──────────────────────────────────────────
    _sharp_rows_html = ""
    _model_right = 0
    _sharp_right = 0
    _push_count  = 0
    _steam_count = 0
    _drift_count = 0

    for _sr in sharp_vs_model_rows:
        _date      = str(_sr.get("pick_date", ""))[:10]
        _game      = _sr.get("game", "")
        _model_lbl = _sr.get("model_label", "")
        _model_team= _sr.get("model_team", "")
        _sharp_side= _sr.get("sharp_side") or ""
        _signal    = _sr.get("ml_signal", "")
        _stance    = _sr.get("sharp_stance", "")
        _result    = _sr.get("actual_result", "")
        _winner    = _sr.get("who_was_right", "")
        _conf_pct  = f"{float(_sr.get('conf', 0)) * 100:.0f}%"
        _tier      = _sr.get("tier", "")

        if _signal == "STEAM":
            _steam_count += 1
            _signal_badge = '<span style="background:#f0883e22;color:#f0883e;padding:2px 6px;border-radius:4px;font-size:0.8em;font-weight:700">STEAM</span>'
        else:
            _drift_count += 1
            _signal_badge = '<span style="background:#58a6ff22;color:#58a6ff;padding:2px 6px;border-radius:4px;font-size:0.8em;font-weight:700">DRIFT</span>'

        if _stance == "agree":
            _sharp_display = f'<span style="color:#3fb950">✓ {_sharp_side} (agrees)</span>'
        else:
            _sharp_display = f'<span style="color:#f0883e">⚡ {_sharp_side} (fading model)</span>'

        if _winner == "model":
            _model_right += 1
            _badge = '<span style="color:#3fb950;font-weight:600">✓ Model</span>'
        elif _winner == "sharp":
            _sharp_right += 1
            _badge = '<span style="color:#f0883e;font-weight:600">⚡ Sharp</span>'
        elif _winner == "neither":
            _badge = '<span style="color:#f85149">✗ Neither</span>'
        else:
            _push_count += 1
            _badge = '<span style="color:#8b949e">— Push</span>'

        _tier_colors = {"LOCK": "#ffc107", "STRONG": "#42a5f5", "LEAN": "#66bb6a", "TOSSUP": "#a09ae0"}
        _tc = _tier_colors.get(_tier, "#8b949e")
        _result_color = "#3fb950" if _result == "WIN" else "#f85149" if _result == "LOSS" else "#8b949e"
        _sharp_rows_html += (
            f'<tr style="border-bottom:1px solid #21262d">'
            f'<td style="color:#8b949e;padding:8px">{_date}</td>'
            f'<td style="color:#e6edf3;padding:8px">{_game}</td>'
            f'<td style="padding:8px"><span style="color:{_tc};font-weight:600">{_tier}</span> {_model_lbl} <span style="color:#8b949e;font-size:0.82em">({_conf_pct})</span></td>'
            f'<td style="padding:8px">{_signal_badge}</td>'
            f'<td style="padding:8px">{_sharp_display}</td>'
            f'<td style="color:{_result_color};padding:8px;font-weight:600">{_result}</td>'
            f'<td style="padding:8px">{_badge}</td>'
            f'</tr>'
        )

    _total_sharp_games = len(sharp_vs_model_rows)
    _graded = _model_right + _sharp_right
    if _total_sharp_games > 0:
        _model_pct = f"{_model_right / _graded * 100:.0f}%" if _graded > 0 else "—"
        _sharp_pct = f"{_sharp_right / _graded * 100:.0f}%" if _graded > 0 else "—"
        _summary_bar = (
            f'<div style="display:flex;gap:20px;margin-bottom:14px;font-size:0.92em;flex-wrap:wrap">'
            f'<span>Games with movement: <b style="color:#e6edf3">{_total_sharp_games}</b></span>'
            f'<span style="color:#f0883e">STEAM: <b>{_steam_count}</b></span>'
            f'<span style="color:#58a6ff">DRIFT: <b>{_drift_count}</b></span>'
            f'<span style="color:#8b949e">|</span>'
            f'<span>Model: <b style="color:#3fb950">{_model_right} ({_model_pct})</b></span>'
            f'<span>Sharp: <b style="color:#f0883e">{_sharp_right} ({_sharp_pct})</b></span>'
            f'</div>'
        )
    else:
        _summary_bar = '<p style="color:#8b949e;margin:8px 0">No line movement data for this period — check back after games are graded.</p>'

    _no_sharp = '<tr><td colspan="7" style="color:#8b949e;padding:16px">No movement data in this period.</td></tr>'
    _sharp_section_html = (
        '<div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin-top:24px">'
        '<details open>'
        '<summary style="cursor:pointer;font-size:1.05em;font-weight:600;color:#e6edf3;list-style:none">'
        '<span class="arr">&#9654;</span> Sharp Action vs Model — Last 3 Days'
        '</summary>'
        '<p style="color:#8b949e;font-size:0.9em;margin:8px 0 12px">All games with line movement — model pick, signal type, sharp side, and who was right.</p>'
        + _summary_bar +
        '<div style="overflow-x:auto">'
        '<table style="width:100%;border-collapse:collapse;font-size:0.88em;min-width:700px">'
        '<thead><tr style="color:#8b949e;border-bottom:2px solid #30363d;background:#0d1117">'
        '<th style="text-align:left;padding:8px">Date</th>'
        '<th style="text-align:left;padding:8px">Game</th>'
        '<th style="text-align:left;padding:8px">Model Pick</th>'
        '<th style="text-align:left;padding:8px">Signal</th>'
        '<th style="text-align:left;padding:8px">Sharp Money</th>'
        '<th style="text-align:left;padding:8px">Result</th>'
        '<th style="text-align:left;padding:8px">Winner</th>'
        '</tr></thead>'
        f'<tbody>{_sharp_rows_html or _no_sharp}</tbody>'
        '</table></div></details></div>'
    )

    # Append sharp section after html is built (can't reference in f-string before it's defined)
    html = html.replace("</body>", _sharp_section_html + "\n</body>", 1)

    return Response(html, content_type="text/html; charset=utf-8")


# ── Scheduled 6am ET daily pipeline ──────────────────────────────────────────────
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
            _schedule_state["next_pipeline_et"] = datetime.now(ET) + timedelta(seconds=wait)
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


def _start_lineup_hourly_scheduler():
    """
    Every day 10am–3pm ET: re-scrapes lineups + hitter stats every hour.
    No Odds API calls — lineup + hitter scraper only.
    Auto-stops for the day once all games have confirmed lineups.
    """
    def _loop():
        while True:
            now = datetime.now(ET)
            target = now.replace(hour=10, minute=0, second=0, microsecond=0)
            # If already past 10am, and past 3pm, jump to tomorrow's 10am
            if now >= target and now.hour >= 15:
                target = (now + timedelta(days=1)).replace(
                    hour=10, minute=0, second=0, microsecond=0)
            # Sleep until target (unless we're between 10am–3pm right now)
            if now < target:
                wait = (target - now).total_seconds()
                log.info(f"Lineup hourly scheduler: next check at {target.strftime('%I:%M %p ET')} ({wait/3600:.1f}h away)")
                time.sleep(max(1, wait))

            # Poll hourly from 10am to 3pm ET
            while True:
                now = datetime.now(ET)
                if now.hour >= 15:
                    log.info("Lineup hourly scheduler: 3pm ET reached — done for today")
                    break
                try:
                    today = now.strftime("%Y-%m-%d")
                    from scrapers.mlb_lineup_scraper import run as run_lu
                    lineups = run_lu(target_date=today)
                    confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
                    total = len(lineups)
                    log.info(f"Lineup hourly check: {confirmed}/{total} games confirmed")
                    if confirmed > 0:
                        from scrapers.mlb_hitter_scraper import run as run_hs
                        run_hs(target_date=today)
                        log.info("Lineup hourly: hitter stats refreshed")
                        try:
                            from db.csv_sync import upload_all, storage_available
                            if storage_available():
                                upload_all()
                        except Exception:
                            pass
                        with _cache_lock:
                            _cache["generated_at"] = 0
                        _regenerate_in_background()
                    if total > 0 and confirmed == total:
                        log.info("Lineup hourly scheduler: all lineups confirmed — done for today")
                        break
                except Exception as e:
                    log.warning(f"Lineup hourly check failed (non-fatal): {e}")
                time.sleep(3600)  # wait 1 hour before next check

            # Wait until next day's 10am
            now = datetime.now(ET)
            tomorrow_10am = (now + timedelta(days=1)).replace(
                hour=10, minute=0, second=0, microsecond=0)
            wait = (tomorrow_10am - now).total_seconds()
            time.sleep(max(1, wait))

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
        # Real pitcher strikeout lines from Pinnacle (free, sharp) — starters are
        # announced by now so the specials are posted.
        try:
            from scrapers.mlb_pinnacle_scraper import save_strikeout_lines
            n_k = save_strikeout_lines()
            log.info(f"Afternoon Pinnacle K lines: {n_k} pitchers")
        except Exception as _ke:
            log.warning(f"Afternoon Pinnacle K lines failed (non-fatal): {_ke}")
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


_frequent_odds_started = [False]


def _start_frequent_odds():
    """
    Keep Sharp Action / line movement current: re-pull Pinnacle ML/RL every ~40
    min through the day up to the LAST first pitch, then stop. Free (Pinnacle, no
    quota). Games that have already started are frozen by the odds loader (it only
    uses snapshots taken before each game's first pitch), so a live line never
    replaces the pre-game number. Guarded so restarts don't stack threads.
    """
    if _frequent_odds_started[0]:
        return
    _frequent_odds_started[0] = True
    import csv as _csv
    from datetime import timezone as _tz

    def _last_first_pitch():
        today = datetime.now(ET).strftime("%Y-%m-%d")
        last_et = None
        try:
            with open(os.path.join(CLEAN_DIR, "mlb_schedule_master.csv"), encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    if row.get("game_date") != today:
                        continue
                    u = row.get("game_time_utc", "").strip()
                    if not u:
                        continue
                    try:
                        g = datetime.strptime(u, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_tz.utc).astimezone(ET)
                        if last_et is None or g > last_et:
                            last_et = g
                    except Exception:
                        pass
        except Exception:
            pass
        return last_et

    def _loop():
        import time as _t
        while True:
            _t.sleep(2400)  # 40 min
            try:
                last = _last_first_pitch()
                if last is None or datetime.now(ET) > last:
                    log.info("Frequent odds: all games started — done for today.")
                    _frequent_odds_started[0] = False
                    return
                from scrapers.mlb_pinnacle_scraper import run as run_pinnacle
                res = run_pinnacle()
                log.info(f"Frequent Pinnacle pull: {res}")
                with _cache_lock:
                    _cache["generated_at"] = 0
                _regenerate_in_background()
            except Exception as e:
                log.warning(f"Frequent odds loop error (non-fatal): {e}")

    threading.Thread(target=_loop, daemon=True).start()
    log.info("Frequent odds pulls started (every 40 min to last first pitch).")


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
                except Exception:
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
            _schedule_state["first_pitch_et"]  = earliest_et
            _schedule_state["next_refresh_et"] = target_et
            return

    _schedule_state["first_pitch_et"]  = earliest_et
    _schedule_state["next_refresh_et"] = target_et

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



# ── Startup ───────────────────────────────────────────────────────────────────────────────────────────
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

        # ── Step 1: DB schema ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
        if _DB_AVAILABLE:
            try:
                _db_create_all()
            except Exception as e:
                log.warning(f"DB schema init failed (non-fatal): {e}")

        # ── Step 2: CSV sync download ────────────────────────────────────────────────────────────────────────────────────────────────────────
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
            finally:
                # Always release the gate, even on failure — a stalled sync must
                # not block the dashboard forever.
                _csv_ready.set()
        else:
            _csv_ready.set()

        # ── Step 3: Pipeline ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
        if _needs_pipeline_run():
            log.info("No pipeline data for today -- running full pipeline on startup...")
            _run_full_pipeline()
            # _schedule_adaptive_refresh() is called inside _run_full_pipeline()
        else:
            log.info("Today's pipeline data exists -- skipping full pipeline run.")
            # Container may have restarted mid-day (deploy/crash) and lost the afternoon
            # refresh thread. Re-schedule it here so deploys after 6am don't silently
            # kill the lineups + odds refresh. _schedule_adaptive_refresh() is a no-op
            # if first pitch has already started.
            try:
                _schedule_adaptive_refresh()
            except Exception as _sar_e:
                log.warning(f"Adaptive refresh re-scheduling failed (non-fatal): {_sar_e}")

        # Keep Sharp Action fresh: recurring Pinnacle pulls up to last first pitch.
        try:
            _start_frequent_odds()
        except Exception as _fo_e:
            log.warning(f"Frequent odds start failed (non-fatal): {_fo_e}")

        # ── Step 3b: Seed cache from R2 HTML ────────────────────────────
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
                        _r2.download_file(_bucket, _r2_key, _tf_path)
                        with open(_tf_path, encoding="utf-8", errors="replace") as _hf:
                            _html = _hf.read()
                        os.unlink(_tf_path)
                        if len(_html) > 10_000:   # sanity: must be a real dashboard
                            with _cache_lock:
                                _cache["html"]         = _html
                                _cache["generated_at"] = time.time()
                                _cache["r2_seeded"]    = True
                            log.info(f"Startup: cache seeded from R2 ({_r2_key}) -- site ready immediately.")
                            break
                    except Exception:
                        continue
        except Exception as _r2e:
            log.debug(f"R2 cache seed skipped: {_r2e}")

        # -- Step 4: Dashboard cache
        log.info("Warming dashboard cache...")
        _regenerate_in_background()

    t = threading.Thread(target=_warm, daemon=True)
    t.start()


# Start schedulers and warm cache whether run via gunicorn or directly
_start_daily_scheduler()
_start_lineup_hourly_scheduler()
warm_cache()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

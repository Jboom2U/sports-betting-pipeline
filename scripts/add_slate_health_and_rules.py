"""
Keep the board alive through the postseason.

TWO PARTS
---------
1. CLAUDE.md gains a POSTSEASON OPERATING RULES section covering the three
   things that cost real time in September and will all recur in October.
2. A new /admin/slate-health route: one page that compares what MLB says
   exists today against what this system actually has.

WHY THE HEALTH ROUTE
--------------------
Every failure this month was the same shape: some stage produced nothing, no
stage validated it, and the board silently served the last page it could build.
An off day and a total outage look identical from the front page.

    2026-09-17  schedule master empty after restart, board blank
    2026-09-18  save_picks failing silently, two days of picks lost
    2026-09-20  schedule master empty after restart, board blank
    2026-09-28  no games (correct) but indistinguishable from broken
    2026-09-29  postseason invisible, gameType filter, board blank

Each one was found by a human noticing and then reading Railway logs. Each one
would have been caught instantly by asking MLB how many games there are today
and comparing that number to the schedule master, the picks table and the
rendered board.

That is the whole route. It has no opinions and no thresholds. It states four
numbers that should agree and says loudly when they do not.

    MLB API        4 games today
    schedule CSV   4
    picks table    7 picks across 3 games
    dashboard      built, 2026-09-29

The three interesting disagreements it names explicitly:
  * MLB has games, schedule has none      -> scraper or R2 overwrite
  * schedule has games, picks has none    -> save_picks failing (the percent bug)
  * picks exist, board date is stale      -> generation returning None

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\add_slate_health_and_rules.py
    python3 scripts\\predeploy_check.py

Then deploy AND force-pipeline, which this script also documents as a single
operation rather than two optional steps.
"""

import io
import os
import shutil
import sys
import datetime
import py_compile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
APP = os.path.join(REPO, "app.py")
DOC = os.path.join(REPO, "CLAUDE.md")
HUB = os.path.join(REPO, "admin_hub.py")

MARKER = "/admin/slate-health"


# ── CLAUDE.md ────────────────────────────────────────────────────────────────

DOC_ANCHOR = "## ⚠️ CRITICAL: Never Run ANY Git Command From the Sandbox\n"

DOC_NEW = '''## ⚠️ POSTSEASON OPERATING RULES (added 2026-09-29)

### A scraper deploy does nothing until the pipeline re-runs

Deploying changed scraper code and running the pipeline are ONE operation, not
two steps where the second is optional. The container skips re-scraping when it
sees `Today's pipeline data exists`, so new scraper code can sit live for hours
without ever being asked to fetch anything.

    git push                    <- wait for the Railway build to finish
    statalizers.com/force-pipeline   <- ONLY after the deploy is green

**Order matters.** On 2026-09-29 the postseason gameType fix was pushed, and
force-pipeline was run during the 14 minute build window. It executed against
the OLD container, found nothing, and left the pipeline marked as already run.
The fix was live for five hours doing nothing. Same command, wrong five minutes.

### gameType must include the postseason

`scrapers/mlb_scraper.py` filtered the MLB API to `gameType=R` in both
`fetch_scores` and `fetch_schedule`, with a comment reading "Regular season
only; add P for playoffs later". The regular season ended 2026-09-27 and the
board went blank: no postseason game could reach the schedule and no postseason
final could be fetched to grade one.

`GAME_TYPES = "R,F,D,L,W"` now covers regular season, Wild Card, Division
Series, LCS and World Series. It deliberately excludes S (spring training),
A (all-star) and E (exhibition).

### A blank board is ambiguous, and in October it will be common

There are genuine off days between rounds. The board returns `None` from
generation and keeps serving the last page it built, so an off day is visually
identical to an outage. Do not diagnose from the front page. Check
`/admin/slate-health`, which compares MLB's own game count for today against
the schedule master, the picks table and the rendered board.

### Slate sizes collapse

15 games a day becomes 4, then 2. Every band record, bucket and threshold in
this system was measured on regular season volume. Treat October records as a
separate era, the same way 2026-07-21 and 2026-08-19 are treated.

---

'''


# ── the route ────────────────────────────────────────────────────────────────

APP_ANCHOR = '@app.route("/admin/export/picks.csv")\n'

ROUTE = '''@app.route("/admin/slate-health")
def slate_health():
    """Does today's slate agree across MLB, the schedule, the picks and the board?

    Built 2026-09-29. Every outage this month was the same shape: a stage
    produced nothing, nothing validated it, and the board silently served the
    last page it could build. An off day and a dead pipeline look identical
    from the front page.

    This page has no thresholds and no opinions. It reports four numbers that
    should agree and names the disagreement when they do not.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/slate-health")

    import html as _h, json as _json, csv as _csv, urllib.request

    date = (request.args.get("date") or datetime.now(ET).strftime("%Y-%m-%d")).strip()
    rows, problems = [], []

    def add(label, value, note=""):
        rows.append((label, value, note))

    # 1. MLB, the external source of truth.
    mlb_n, mlb_detail = None, ""
    try:
        u = ("https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=" + date)
        rq = urllib.request.Request(u, headers={"User-Agent": "statalizers/1.0"})
        with urllib.request.urlopen(rq, timeout=20) as r:
            j = _json.loads(r.read().decode("utf-8"))
        games = [g for d in j.get("dates", []) for g in d.get("games", [])]
        mlb_n = len(games)
        types = sorted({g.get("gameType", "?") for g in games})
        live = sum(1 for g in games
                   if g.get("status", {}).get("abstractGameState") == "Live")
        fin = sum(1 for g in games
                  if g.get("status", {}).get("abstractGameState") == "Final")
        mlb_detail = ("types " + ",".join(types) + " | " + str(fin) + " final, "
                      + str(live) + " live") if games else "no games scheduled"
    except Exception as e:
        mlb_detail = "MLB API unreachable: " + str(e)
    add("MLB API", "-" if mlb_n is None else str(mlb_n), mlb_detail)

    # 2. The schedule master the model actually reads.
    sched_n = None
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "data", "clean", "mlb_schedule_master.csv")
        if os.path.exists(p):
            with open(p, encoding="utf-8", errors="replace") as f:
                sched_n = sum(1 for r in _csv.DictReader(f)
                              if (r.get("game_date") or "").strip() == date)
            add("schedule master", str(sched_n), os.path.basename(p))
        else:
            add("schedule master", "-", "file missing")
    except Exception as e:
        add("schedule master", "-", "read failed: " + str(e))

    # 3. Picks actually written.
    picks_n = saved_games = None
    try:
        from db.connection import db_conn as _dbc
        with _dbc() as conn:
            if conn is None:
                add("picks table", "-", "no database connection")
            else:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*), COUNT(DISTINCT game_id) "
                            "FROM picks WHERE pick_date = %s", (date,))
                picks_n, saved_games = cur.fetchone()
                cur.close()
                add("picks table", str(picks_n),
                    str(saved_games) + " distinct game(s)")
    except Exception as e:
        add("picks table", "-", "query failed: " + str(e))

    # 4. What the board is actually serving.
    board_date, board_len = None, 0
    try:
        with _cache_lock:
            html_cached = _cache.get("html")
        if html_cached:
            board_len = len(html_cached)
            import re as _re
            m = _re.search(r'const\\s+DATA_DATE\\s*=\\s*"([^"]+)"', html_cached)
            board_date = m.group(1) if m else None
        add("dashboard cache", board_date or "none",
            str(board_len) + " bytes")
    except Exception as e:
        add("dashboard cache", "-", "read failed: " + str(e))

    # ── The three disagreements that have actually happened ─────────────────
    if mlb_n is not None and sched_n is not None:
        if mlb_n > 0 and sched_n == 0:
            problems.append(
                "MLB has " + str(mlb_n) + " game(s) today and the schedule master "
                "has none. Either the scraper cannot see them (check GAME_TYPES) "
                "or a restart overwrote the schedule from R2. Run /force-pipeline.")
        elif mlb_n == 0:
            problems.append(
                "NO GAMES TODAY per MLB. A blank board is CORRECT. This is an "
                "off day, not an outage.")
    if sched_n and picks_n == 0:
        problems.append(
            "The schedule has " + str(sched_n) + " game(s) but zero picks are "
            "saved. This is the save_picks failure shape. Check the Railway log "
            "for 'save_picks DB write failed'.")
    if picks_n and board_date and board_date != date:
        problems.append(
            "Picks exist for " + date + " but the board is serving " +
            str(board_date) + ". Dashboard generation is returning None. "
            "Run /unstick.")
    if board_len and board_len < 60000:
        problems.append(
            "The cached page is only " + str(board_len) + " bytes, which is a "
            "fallback or warming page rather than a real board.")

    body = "".join(
        "<tr><td>" + _h.escape(a) + "</td><td class=v>" + _h.escape(b) +
        "</td><td class=n>" + _h.escape(c) + "</td></tr>" for a, b, c in rows)
    probs = "".join("<div class=bad>" + _h.escape(p) + "</div>" for p in problems) \\
        or "<div class=ok>Everything agrees. No action needed.</div>"

    return Response("""<!doctype html><html><head><meta charset=utf-8>
<title>Slate health</title><style>
body{background:#0d1117;color:#c9d1d9;font-family:system-ui;padding:22px;max-width:820px;margin:0 auto}
h2{color:#58a6ff} table{border-collapse:collapse;width:100%;margin:14px 0}
td{padding:7px 10px;border-bottom:1px solid #21262d;font-size:13.5px}
.v{font-weight:700;font-size:16px;text-align:right;width:90px}
.n{color:#8b949e;font-size:12px}
.bad{background:rgba(248,81,73,.09);border:1px solid rgba(248,81,73,.42);
     border-radius:8px;padding:11px 15px;margin:9px 0;color:#ffa198;font-size:13px}
.ok{background:rgba(63,185,80,.08);border:1px solid rgba(63,185,80,.32);
    border-radius:8px;padding:11px 15px;margin:9px 0;color:#7ee787;font-size:13px}
.note{color:#8b949e;font-size:12px;line-height:1.6}
</style></head><body>
<h2>Slate health &mdash; """ + _h.escape(date) + """</h2>
<p class="note">Four numbers that should agree. MLB is the external source of
truth; everything else is this system. Add <code>?date=YYYY-MM-DD</code> to
check another day.</p>
<table>""" + body + """</table>""" + probs + """
<p class="note" style="margin-top:18px">An off day and a dead pipeline look
identical from the front page. That ambiguity is what this page removes.</p>
<p><a href="/admin">&larr; Admin</a></p></body></html>""", mimetype="text/html")


'''

HUB_ANCHOR = '''        ("/admin/data-health", "Data health",
'''

HUB_CARD = '''        ("/admin/slate-health", "Slate health",
         "Does today's slate agree across MLB, the schedule, the picks and the board? Start here when the board looks wrong",
         "admin", "free"),
'''


def main():
    for p in (APP, DOC, HUB):
        if not os.path.exists(p):
            sys.exit("ABORT: %s not found. Run this from the repo." % p)

    if MARKER in io.open(APP, encoding="utf-8").read():
        print("Already applied. Nothing to do.")
        return 0

    plan = [
        (APP, "route",      APP_ANCHOR, ROUTE + APP_ANCHOR),
        (DOC, "claude.md",  DOC_ANCHOR, DOC_NEW + DOC_ANCHOR),
        (HUB, "admin card", HUB_ANCHOR, HUB_CARD + HUB_ANCHOR),
    ]

    resolved = {}
    for path, label, old, new in plan:
        src = io.open(path, encoding="utf-8").read()
        n = src.count(old)
        if n != 1:
            sys.exit("ABORT: %s anchor matched %d times in %s, expected 1. "
                     "NOTHING written." % (label, n, os.path.basename(path)))
        resolved[path] = (src.replace(old, new, 1), len(src))

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backups = {}
    for path in resolved:
        b = path + ".%s.bak" % stamp
        shutil.copy2(path, b)
        backups[path] = b
        print("backup   %s" % os.path.basename(b))

    for path, (src, before) in resolved.items():
        io.open(path, "w", encoding="utf-8").write(src)
        print("spliced  %-16s %d -> %d bytes" % (os.path.basename(path), before, len(src)))

    def restore(why):
        for p, b in backups.items():
            shutil.copy2(b, p)
        sys.exit("ABORT: %s. All backups restored." % why)

    for path in (APP, HUB):
        try:
            py_compile.compile(path, doraise=True)
        except Exception as e:
            restore("compile failed on %s\n%s" % (os.path.basename(path), e))
    print("compile  OK")

    app_src = io.open(APP, encoding="utf-8").read()
    doc_src = io.open(DOC, encoding="utf-8").read()
    hub_src = io.open(HUB, encoding="utf-8").read()

    # The percent-in-SQL bug cost two days. This route contains SQL.
    sql_ok = True
    try:
        seg = ROUTE.split('cur.execute("SELECT COUNT(*)', 1)[1].split("(date,)", 1)[0]
        sql_ok = "%" not in seg.replace("%s", "")
    except Exception:
        sql_ok = False

    checks = [
        ("route registered",      '@app.route("/admin/slate-health")' in app_src),
        ("compares MLB",          "statsapi.mlb.com" in app_src),
        ("reads schedule master", "mlb_schedule_master.csv" in app_src),
        ("reads picks table",     "FROM picks WHERE pick_date = %s" in app_src),
        ("reads board cache",     "_cache.get(\"html\")" in app_src),
        ("off-day message",       "NO GAMES TODAY per MLB" in app_src),
        ("no literal % in SQL",   sql_ok),
        ("claude.md section",     "POSTSEASON OPERATING RULES" in doc_src),
        ("deploy order rule",     "ONE operation, not" in doc_src),
        ("admin card",            "/admin/slate-health" in hub_src),
        ("git rule preserved",    "Never Run ANY Git Command" in doc_src),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        restore("%d post-check(s) failed" % len(bad))

    print("")
    print("Done. Deploy, WAIT for the build to go green, THEN force-pipeline.")
    print("Then check:  statalizers.com/admin/slate-health")
    return 0


if __name__ == "__main__":
    sys.exit(main())

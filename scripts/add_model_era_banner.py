"""
One-shot splice: make every ROI page state WHICH MODEL it is describing.

WHY THIS EXISTS
---------------
On 2026-09-20 Justin pointed out that evaluations were being made against
season aggregates that pool a model which had weather dead, pitcher Statcast
dead and the main total line wrong with the model running today. His words:
"this mistake could have destroyed this entire model."

He was right, and the tooling invited it. /admin/real-roi defaults to
?since=2026-08-11 and renders one number over everything after that date, with
nothing on the page indicating that the window spans three different models.

CLAUDE.md already documents this trap for the 2026-07-21 data boundary:
"Do not pool pre- and post-2026-07-21 picks in one calibration run. Doing so
averages a crippled model with a repaired one and will produce a conclusion
that is wrong about both."

Two more boundaries were created since and never written down:

    2026-07-21  data boundary. Before it, 8 master CSVs vanished on every
                container restart; umpire, bullpen fatigue, platoon and Kalshi
                were all dead.
    2026-08-19  four dead signals fixed: weather (roof read as the string
                'False', so the block never ran), pitcher Statcast (three wrong
                Savant column names), platoon refresh wired into the pipeline,
                power devig.
    2026-08-20  main total line selection fixed. Before it, a tie in the
                consensus count selected the LOWEST line on the board,
                publishing things like OVER 6.5 on a game projected at 9.5.
    2026-09-08  per-type calibration refit. ML A=0.067020 B=0.141158,
                RL A=2.657473 B=-1.141674. MODEL_VERSION bumped to 2026.09.08.

WHAT THIS CHANGES
-----------------
1. A new `_era_banner()` helper that, for any window, reads the picks table and
   reports how many graded picks it contains and which model_version values
   they carry, with counts. If the window spans more than one version, or
   contains unstamped rows, it renders a RED warning saying so.
2. /admin/real-roi gains `?until=` and `?era=` alongside `?since=`, and its
   default moves from 2026-08-11 to 2026-08-19, the input-fix boundary. The
   old default silently mixed the starved model into every number on the page.
3. One-click era links at the top so switching window is trivial.

The helper is written to be dropped into /admin/strategy-backtest and
/admin/calibration-fit next. Both have the same defect and the same
`?since=` shape.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It does not hide older data. `?era=all` still shows everything. The point is
that the page has to SAY which model it is describing, not that older picks
stop existing.

SAFE TO RUN
-----------
Timestamped backup, anchors must match exactly once or it aborts having written
nothing, idempotent, py_compile, backup restored on any failure.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\add_model_era_banner.py
    python3 scripts\\predeploy_check.py

DEPLOY IN THE MORNING, after the 6am run and before first pitch.
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

MARKER = "MODEL ERAS"


# ── SPLICE 1: the helper, inserted just before the real-roi route ────────────

ANCHOR_1 = '@app.route("/admin/real-roi")\n'

HELPER = '''# ── MODEL ERAS ────────────────────────────────────────────────────────────────
# Every boundary at which the model changed enough that pooling across it
# produces a number describing no model that ever existed.
#
# This is not bookkeeping. CLAUDE.md carries the same warning for the 07-21
# boundary and it was ignored anyway, because the ROI page defaulted to a
# window that crossed three of these and said nothing about it.
MODEL_ERAS = [
    ("2026-07-21", "data boundary",
     "8 master CSVs vanished on every restart; umpire, bullpen fatigue, "
     "platoon and Kalshi all dead"),
    ("2026-08-19", "input fixes",
     "weather (roof read as the string 'False'), pitcher Statcast (3 wrong "
     "Savant column names), platoon refresh, power devig"),
    ("2026-08-20", "main total fixed",
     "a tie in the consensus count had been selecting the LOWEST line on the "
     "board"),
    ("2026-09-08", "calibration refit",
     "ML A=0.067020 B=0.141158, RL A=2.657473 B=-1.141674; "
     "MODEL_VERSION 2026.09.08"),
]

# The default for anything measuring model QUALITY. Before this date the model
# was demonstrably starved, so pooling it in flatters nothing and misleads
# everything.
CURRENT_MODEL_SINCE = "2026-08-19"


def _era_banner(since, until=None, _h=None):
    """Report which model(s) a window actually contains.

    Reads model_version straight off the picks table rather than inferring it
    from dates, because the stamp is the only thing that knows what produced a
    row. Returns an HTML block. Never raises: a banner that takes the page down
    is worse than no banner.
    """
    import html as _html
    esc = (_h.escape if _h else _html.escape)
    try:
        from db.connection import db_conn as _dbc
        sql = ("SELECT COALESCE(model_version, '(unstamped)'), COUNT(*) "
               "FROM picks WHERE actual_result IN ('WIN','LOSS') "
               "AND pick_date >= %s")
        args = [since]
        if until:
            sql += " AND pick_date <= %s"
            args.append(until)
        sql += " GROUP BY 1 ORDER BY 2 DESC"
        with _dbc() as conn:
            if conn is None:
                return ("<div class='warn'>Model version unavailable: no "
                        "database connection. Treat every number below as "
                        "unattributed.</div>")
            cur = conn.cursor()
            cur.execute(sql, tuple(args))
            vers = cur.fetchall()
            cur.close()
    except Exception as _e:
        return ("<div class='warn'>Model version check failed: "
                + esc(str(_e)) + ". Treat every number below as unattributed.</div>")

    total = sum(n for _, n in vers) or 0
    win = esc(since) + (" to " + esc(until) if until else " to today")
    links = " &middot; ".join(
        "<a href='/admin/real-roi?since=" + d + "'>" + d + " " + esc(name) + "</a>"
        for d, name, _ in MODEL_ERAS
    ) + " &middot; <a href='/admin/real-roi?since=2026-01-01'>all</a>"

    rows = "".join(
        "<tr><td><code>" + esc(str(v)) + "</code></td><td>" + str(n) + "</td></tr>"
        for v, n in vers
    ) or "<tr><td colspan=2>no graded picks in this window</td></tr>"

    multi = len(vers) > 1
    head = (
        "<div class='warn'><b>This window spans " + str(len(vers)) +
        " model versions.</b> Every figure below is an average across them, "
        "which describes no model that ever ran. Pick one era before drawing "
        "a conclusion.</div>"
        if multi else
        "<div class='okbox'><b>Single model version.</b> The figures below "
        "describe one model.</div>"
    )

    return (
        "<div class='erabox'><b>Window:</b> " + win + " &nbsp;&middot;&nbsp; "
        "<b>" + str(total) + "</b> graded picks"
        "<table style='margin-top:8px;max-width:420px'>"
        "<tr><th>model_version</th><th>picks</th></tr>" + rows + "</table>"
        "<div class='note' style='margin-top:8px'>Jump to an era: " + links +
        "</div></div>" + head
    )


'''


# ── SPLICE 2: docstring + param handling ─────────────────────────────────────

ANCHOR_2 = '''      ?since=YYYY-MM-DD   default 2026-08-11, when prices started being stored
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/real-roi")
    import html as _h, traceback
    since = request.args.get("since", "2026-08-11")
'''

REPLACE_2 = '''      ?since=YYYY-MM-DD   default 2026-08-19, the input-fix boundary
      ?until=YYYY-MM-DD   optional upper bound
      ?era=all            everything, explicitly

    THE DEFAULT MOVED (2026-09-20). It used to be 2026-08-11, the date prices
    started being stored, which sounds sensible and is not: that window spans
    the 08-19 input fixes, the 08-20 total fix and the 09-08 calibration refit,
    so every number on the page averaged three different models together and
    the page said nothing about it. Real cost: a whole evaluation of LOCK and
    STRONG was argued from those pooled figures while the post-fix model was
    behaving completely differently.

    Older data is not hidden. ?era=all still shows it. It just has to be asked
    for, and the banner always says which models are in the window.
    """
    if _ADMIN_PASS and not session.get("admin_auth"):
        return redirect("/admin/login?next=/admin/real-roi")
    import html as _h, traceback
    if request.args.get("era") == "all":
        since = "2026-01-01"
    else:
        since = request.args.get("since", CURRENT_MODEL_SINCE)
    until = (request.args.get("until") or "").strip() or None
'''


# ── SPLICE 3: the SQL gets the upper bound ───────────────────────────────────

ANCHOR_3 = '''                SELECT pick_type, label, conf, actual_result, odds
                FROM picks
                WHERE actual_result IN ('WIN','LOSS')
                  AND conf IS NOT NULL AND odds IS NOT NULL
                  AND pick_date >= %s
            """, (since,))
'''

REPLACE_3 = '''                SELECT pick_type, label, conf, actual_result, odds
                FROM picks
                WHERE actual_result IN ('WIN','LOSS')
                  AND conf IS NOT NULL AND odds IS NOT NULL
                  AND pick_date >= %s
                  AND (%s IS NULL OR pick_date <= %s)
            """, (since, until, until))
'''


# ── SPLICE 4: render the banner ──────────────────────────────────────────────

ANCHOR_4 = '''<p class="note">Graded picks since <b>{_h.escape(since)}</b> that carry a stored
price.'''

REPLACE_4 = '''{_era_banner(since, until, _h)}
<p class="note">Graded picks since <b>{_h.escape(since)}</b> that carry a stored
price.'''


# ── SPLICE 5: styles for the new blocks ──────────────────────────────────────

ANCHOR_5 = '''.hero{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin:14px 0}}
.big{{font-size:30px;font-weight:700;color:{tcol}}}</style></head><body>'''

REPLACE_5 = '''.hero{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin:14px 0}}
.erabox{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px 16px;margin:14px 0;font-size:13px}}
.warn{{background:rgba(248,81,73,.09);border:1px solid rgba(248,81,73,.42);border-radius:8px;padding:12px 16px;margin:10px 0;font-size:13px;color:#ffa198}}
.okbox{{background:rgba(63,185,80,.08);border:1px solid rgba(63,185,80,.32);border-radius:8px;padding:12px 16px;margin:10px 0;font-size:13px;color:#7ee787}}
.big{{font-size:30px;font-weight:700;color:{tcol}}}</style></head><body>'''


SPLICES = [
    ("helper",        ANCHOR_1, HELPER + ANCHOR_1),
    ("params",        ANCHOR_2, REPLACE_2),
    ("sql bound",     ANCHOR_3, REPLACE_3),
    ("banner render", ANCHOR_4, REPLACE_4),
    ("styles",        ANCHOR_5, REPLACE_5),
]


def main():
    if not os.path.exists(APP):
        sys.exit("ABORT: %s not found. Run this from the repo." % APP)

    src = io.open(APP, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied. Nothing to do.")
        return 0

    for label, old, new in SPLICES:
        n = src.count(old)
        if n != 1:
            sys.exit("ABORT: %s anchor matched %d times, expected 1. "
                     "NOTHING written." % (label, n))
        src = src.replace(old, new, 1)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = APP + ".%s.bak" % stamp
    shutil.copy2(APP, backup)
    print("backup   %s" % os.path.basename(backup))

    io.open(APP, "w", encoding="utf-8").write(src)
    print("spliced  app.py  %d -> %d bytes" % (before, len(src)))

    try:
        py_compile.compile(APP, doraise=True)
    except Exception as e:
        shutil.copy2(backup, APP)
        sys.exit("ABORT: compile failed, backup restored.\n%s" % e)
    print("compile  OK")

    after = io.open(APP, encoding="utf-8").read()
    checks = [
        ("MODEL_ERAS defined",     "MODEL_ERAS = [" in after),
        ("helper defined",         "def _era_banner(since, until=None, _h=None):" in after),
        ("helper called",          "{_era_banner(since, until, _h)}" in after),
        ("default moved",          'request.args.get("since", CURRENT_MODEL_SINCE)' in after),
        ("old default gone",       'request.args.get("since", "2026-08-11")' not in after),
        ("until wired to SQL",     "(%s IS NULL OR pick_date <= %s)" in after),
        ("era=all supported",      'request.args.get("era") == "all"' in after),
        ("warn style added",       ".warn{{background:rgba(248,81,73" in after),
        ("no truncation",          len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, APP)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. After deploying, /admin/real-roi defaults to 2026-08-19 and")
    print("states which model versions are in the window. ?era=all for everything.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

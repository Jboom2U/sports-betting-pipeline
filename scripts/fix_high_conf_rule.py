"""
One-shot splice: make the High Confidence badge mean something.

WHY THIS EXISTS
---------------
Justin, 2026-09-20: "High Confidence I do not even look at because I am not
sure what its even working off anymore."

Three defects, all in the same direction, all making the badge look better than
the bets were.

1. NO DATE FILTER. compute_high_conf_rule() queried:

       SELECT pick_type, conf, actual_result FROM picks
       WHERE actual_result IN ('WIN','LOSS') AND conf IS NOT NULL

   which is why the log reads "high-conf rule from 3259 graded picks". That
   pool includes the 1,459 pre-2026-07-21 picks CLAUDE.md explicitly forbids
   pooling ("averages a crippled model with a repaired one and will produce a
   conclusion that is wrong about both"), plus everything from before weather
   and pitcher Statcast were fixed on 08-19. The threshold shown on tonight's
   card was chosen by a model that no longer exists.

2. FLAT -110 BREAK-EVEN. BREAK_EVEN = 52.38, TARGET = 56.0. But the ML picks
   this rule flags are priced -139 to -184, where break-even is 58.1% to 64.7%.
   Measured on /admin/real-roi, ML 65-70% returns +2.6% and ML 60-65% returns
   -12.7%. So the badge reading "TAGGED PROFITABLE, ML 65%+, 332-245, 57.5%"
   was calling a bucket profitable that needs roughly 60% at real prices.

3. THE LEGEND WAS HARDCODED AND WRONG. Line 4258 said "(ML 80%+ earns fire,
   70%+ earns chart)" while the code computed 85%+ and 65%+. The caption was
   typed once and never tracked the rule. The widget's own green/red test at
   line 4247 used the same 52.38.

WHAT THIS CHANGES
-----------------
  * compute_high_conf_rule(since=HIGH_CONF_SINCE), defaulting to 2026-08-19,
    the input-fix boundary.
  * A band now has to beat the break-even implied by the REAL prices of the
    picks in that band, plus a margin, AND return positive ROI. The pass test
    is measured, not assumed.
  * Record strings carry n, win rate, the break-even that band actually faced,
    and real ROI, so the widget can show all four.
  * The widget colours on measured break-even, and builds its legend from the
    rule the code computed, so the caption can never drift again.
  * No pre-fix fallback. If the current era cannot support a band, no badge
    shows. Borrowing credibility from a blind model is the bug, not the
    safety net.

EXPECT THE BADGE TO GET QUIETER. Sample drops from ~3,259 to roughly 800 and
the bar goes up. Justin, asked in advance: "quieter is fine."

SAFE TO RUN
-----------
Timestamped backup, anchors must match exactly once or it aborts having written
nothing, idempotent, py_compile, backup restored on any failure. run_picks_html
is the 221KB locked file, so this is splice-only by design.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_high_conf_rule.py
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
TARGET = os.path.join(REPO, "run_picks_html.py")

MARKER = "HIGH_CONF_SINCE"


OLD_FN = '''def compute_high_conf_rule() -> dict:
    """
    Derive the "high confidence" threshold from the model's OWN graded record
    instead of hardcoding a number.

    For each pick_type, walk 5-point confidence bands and find the lowest band
    where the model has actually beaten break-even on a usable sample. A band
    qualifies when n >= MIN_N and win rate >= TARGET. Everything at or above
    that confidence for that pick type gets flagged.

    Returns {"ML": 75.0, ...} plus a per-type record string for the tooltip.
    Falls back to ML>=75 if the DB is unavailable — that band is the only one
    that beat break-even across the pre-2026-07-21 sample.
    """
    BREAK_EVEN = 52.38
    TARGET     = 56.0   # margin above break-even so noise doesn't qualify a band
    MIN_N      = 30

    fallback = {"rule": {"ML": 75.0}, "record": {"ML": "ML 75%+ (pre-fix baseline)"},
                "rule_wide": {}, "record_wide": {}}

    try:
        from db.connection import db_conn
        with db_conn() as conn:
            if conn is None:
                return fallback
            cur = conn.cursor()
            cur.execute(
                "SELECT pick_type, conf, actual_result FROM picks "
                "WHERE actual_result IN ('WIN','LOSS') AND conf IS NOT NULL"
            )
            rows = cur.fetchall()
    except Exception as e:
        log.warning(f"high-conf rule: DB query failed, using fallback: {e}")
        return fallback

    if not rows:
        return fallback

    def norm(c):
        c = float(c or 0)
        return c * 100 if c <= 1 else c

    rule, record = {}, {}
    rule_wide, record_wide = {}, {}
    for ptype in sorted({r[0] for r in rows if r[0]}):
        sub = [(norm(r[1]), r[2]) for r in rows if r[0] == ptype]
        # Two tiers, both derived from the model's own record:
        #   ELITE — threshold with the highest win rate (fewest, strongest picks)
        #   WIDE  — lowest threshold still clearing TARGET (more volume, thinner)
        # These differ meaningfully. On pre-fix data ML>=75 nets 65.5% (n=87)
        # while ML>=70 nets 58.6% (n=145), because the 70-75 sub-band alone loses
        # at 48.3% and is carried by the bands above it. Both are profitable, so
        # surface both and let the bet size reflect the difference.
        elite = wide = None
        for thresh in range(90, 49, -5):
            sel = [r for r in sub if r[0] >= thresh]
            n = len(sel)
            if n < MIN_N:
                continue
            wins = sum(1 for r in sel if r[1] == "WIN")
            wr = wins / n * 100
            if wr >= TARGET:
                if elite is None or wr > elite[2]:
                    elite = (float(thresh), n, wr)
                wide = (float(thresh), n, wr)   # keeps descending to the lowest pass
        if elite:
            rule[ptype] = elite[0]
            record[ptype] = f"{ptype} {elite[0]:.0f}%+: {elite[1]} picks, {elite[2]:.1f}%"
        if wide and (not elite or wide[0] < elite[0]):
            rule_wide[ptype] = wide[0]
            record_wide[ptype] = f"{ptype} {wide[0]:.0f}%+: {wide[1]} picks, {wide[2]:.1f}%"

    if not rule and not rule_wide:
        log.info("high-conf rule: no pick type clears target — no badges will show")
        return {"rule": {}, "record": {}, "rule_wide": {}, "record_wide": {}}

    log.info(f"high-conf rule from {len(rows)} graded picks: elite={record} wide={record_wide}")
    return {"rule": rule, "record": record,
            "rule_wide": rule_wide, "record_wide": record_wide}
'''


NEW_FN = '''# The model era this rule is allowed to learn from. 2026-08-19 is the input-fix
# boundary: weather (roof read as the string 'False', so the block never ran),
# pitcher Statcast (three wrong Savant column names) and the platoon refresh
# were all repaired that day, and the main total line the day after.
#
# This used to have NO date bound at all, so the threshold was derived from
# 3,259 graded picks spanning every era including the 1,459 pre-2026-07-21
# picks CLAUDE.md forbids pooling. The badge on tonight's card was chosen by a
# model that has not existed since August.
HIGH_CONF_SINCE = "2026-08-19"


def compute_high_conf_rule(since: str = None) -> dict:
    """
    Derive the "high confidence" threshold from the model's own graded record,
    measured against the prices those picks actually carried.

    WHAT CHANGED (2026-09-20) AND WHY IT MATTERS

    This used to test each band against a flat 52.38% break-even. The ML bands
    it flags are priced -139 to -184, where break-even is 58.1% to 64.7%. So it
    was calling ML 65%+ "profitable" at a 57.5% win rate, which loses money at
    the prices on the board. /admin/real-roi measures that same band at +2.6%
    and the band below it at -12.7%.

    A band now has to clear the break-even implied by ITS OWN stored prices,
    plus a margin, AND return positive ROI. Both are measured from the `odds`
    column rather than assumed.

    There is deliberately NO pre-fix fallback. If the current era cannot support
    a band, no badge shows. A badge backed by a blind model is worse than no
    badge, because it looks identical to one that is earned.

    Returns {"rule": {...}, "record": {...}, "rule_wide": ..., "record_wide": ...}
    where each record string is
        "ML 75%+: 41 picks, 63.4% vs 60.1% needed, ROI +4.2%"
    """
    MARGIN = 3.0    # points clear of the band's OWN measured break-even
    MIN_N  = 30
    since  = since or HIGH_CONF_SINCE

    empty = {"rule": {}, "record": {}, "rule_wide": {}, "record_wide": {},
             "since": since, "n": 0}

    try:
        from db.connection import db_conn
        with db_conn() as conn:
            if conn is None:
                return empty
            cur = conn.cursor()
            cur.execute(
                "SELECT pick_type, conf, actual_result, odds FROM picks "
                "WHERE actual_result IN ('WIN','LOSS') AND conf IS NOT NULL "
                "AND odds IS NOT NULL AND pick_date >= %s",
                (since,)
            )
            rows = cur.fetchall()
    except Exception as e:
        log.warning(f"high-conf rule: DB query failed, no badges will show: {e}")
        return empty

    if not rows:
        log.info(f"high-conf rule: no graded priced picks since {since}")
        return empty

    def norm(c):
        c = float(c or 0)
        return c * 100 if c <= 1 else c

    def dec(o):
        """American to decimal. None for the corrupt sub-100 prices."""
        try:
            o = float(o)
        except Exception:
            return None
        if abs(o) < 100:
            return None
        return 1.0 + o / 100.0 if o > 0 else 1.0 + 100.0 / abs(o)

    clean = []
    for r in rows:
        d = dec(r[3])
        if d is not None:
            clean.append((r[0], norm(r[1]), r[2], d))

    rule, record = {}, {}
    rule_wide, record_wide = {}, {}
    for ptype in sorted({r[0] for r in clean if r[0]}):
        sub = [r for r in clean if r[0] == ptype]
        # ELITE is the threshold with the largest real-price edge, WIDE is the
        # lowest threshold that still clears its own break-even with a margin.
        elite = wide = None
        for thresh in range(90, 49, -5):
            sel = [r for r in sub if r[1] >= thresh]
            n = len(sel)
            if n < MIN_N:
                continue
            wins = sum(1 for r in sel if r[2] == "WIN")
            wr   = wins / n * 100
            be   = sum(1.0 / r[3] for r in sel) / n * 100
            roi  = sum((r[3] - 1.0) if r[2] == "WIN" else -1.0 for r in sel) / n * 100
            if wr >= be + MARGIN and roi > 0:
                edge = wr - be
                if elite is None or edge > elite[4]:
                    elite = (float(thresh), n, wr, be, edge, roi)
                wide = (float(thresh), n, wr, be, edge, roi)
        def _fmt(t):
            return (f"{ptype} {t[0]:.0f}%+: {t[1]} picks, {t[2]:.1f}% "
                    f"vs {t[3]:.1f}% needed, ROI {t[5]:+.1f}%")
        if elite:
            rule[ptype] = elite[0]
            record[ptype] = _fmt(elite)
        if wide and (not elite or wide[0] < elite[0]):
            rule_wide[ptype] = wide[0]
            record_wide[ptype] = _fmt(wide)

    if not rule and not rule_wide:
        log.info(f"high-conf rule: no band clears its own break-even since "
                 f"{since} on {len(clean)} priced picks — no badges will show")
        return empty

    log.info(f"high-conf rule since {since} from {len(clean)} priced graded picks: "
             f"elite={record} wide={record_wide}")
    return {"rule": rule, "record": record,
            "rule_wide": rule_wide, "record_wide": record_wide,
            "since": since, "n": len(clean)}
'''


OLD_WIDGET = '''    function rows(recs){
      if(!recs.length) return '<div class="tw-row"><span class="tw-none">building record…</span></div>';
      return recs.map(r=>{
        const i = r.indexOf(":");
        const rule = (i>=0 ? r.slice(0,i) : r).trim();         // e.g. "ML 80%+"
        const stat = (i>=0 ? r.slice(i+1) : "").trim();        // e.g. "51 picks, 64.7%"
        const nm = stat.match(/(\\d+)\\s*picks/);
        const wm = stat.match(/([\\d.]+)%/);
        const n  = nm ? parseInt(nm[1]) : null;
        const wr = wm ? parseFloat(wm[1]) : null;
        const col = wr==null ? "var(--sub)" : (wr>=52.38 ? "var(--green)" : "var(--red)");
        let rec = stat;
        if(n!=null && wr!=null){ const w=Math.round(n*wr/100); rec = `${w}-${n-w} · ${wr}%`; }
        return `<div class="tw-row"><span class="tw-lbl">${rule}</span>`+
               `<span class="tw-rec" style="color:${col}">${rec}</span></div>`;
      }).join("");
    }
    box.innerHTML = `<div class="tw-title">📊 Badge Track Record</div>`+
      `<div class="tw-sub">🔥 tagged High Confidence cards</div>${rows(elite)}`+
      `<div class="tw-sub">📈 tagged Profitable cards</div>${rows(wide)}`+
      `<div class="tw-note">Running graded W-L of every card that carried the badge `+
      `(ML 80%+ earns 🔥, 70%+ earns 📈). Green = beating break-even. `+
      `Grows as new tagged picks settle.</div>`;
'''

NEW_WIDGET = '''    // Colour on the break-even the band ACTUALLY faced, not a flat 52.38.
    // The old test marked ML 65%+ at 57.5% green while it needed ~60% at the
    // prices on those cards, so a losing bucket rendered as profitable.
    function rows(recs){
      if(!recs.length) return '<div class="tw-row"><span class="tw-none">no band clears its price yet</span></div>';
      return recs.map(r=>{
        const i = r.indexOf(":");
        const rule = (i>=0 ? r.slice(0,i) : r).trim();          // "ML 75%+"
        const stat = (i>=0 ? r.slice(i+1) : "").trim();
        const nm = stat.match(/(\\d+)\\s*picks/);
        const wm = stat.match(/([\\d.]+)%\\s*vs/);
        const bm = stat.match(/vs\\s*([\\d.]+)%\\s*needed/);
        const rm = stat.match(/ROI\\s*([+-][\\d.]+)%/);
        const n  = nm ? parseInt(nm[1]) : null;
        const wr = wm ? parseFloat(wm[1]) : null;
        const be = bm ? parseFloat(bm[1]) : null;
        const roi= rm ? parseFloat(rm[1]) : null;
        const col = (wr==null||be==null) ? "var(--sub)"
                  : (wr>=be ? "var(--green)" : "var(--red)");
        let rec = stat;
        if(n!=null && wr!=null){
          const w=Math.round(n*wr/100);
          rec = `${w}-${n-w} · ${wr}%`;
          if(be!=null) rec += ` <span style="color:var(--sub)">vs ${be}%</span>`;
          if(roi!=null) rec += ` <span style="color:${roi>=0?'var(--green)':'var(--red)'}">${roi>0?'+':''}${roi}%</span>`;
        }
        return `<div class="tw-row"><span class="tw-lbl">${rule}</span>`+
               `<span class="tw-rec" style="color:${col}">${rec}</span></div>`;
      }).join("");
    }
    // Build the caption FROM the computed rule. The old one was hardcoded as
    // "ML 80%+ earns fire, 70%+ earns chart" while the code was computing 85
    // and 65, so the legend described a rule that had not existed for weeks.
    const _ruleTxt = o => Object.entries(o||{})
        .map(([k,v]) => k+" "+Math.round(v)+"%+").join(", ") || "none";
    const _since = (HIGH_CONF && HIGH_CONF.since) ? HIGH_CONF.since : "?";
    const _nAll  = (HIGH_CONF && HIGH_CONF.n) ? HIGH_CONF.n : 0;
    box.innerHTML = `<div class="tw-title">📊 Badge Track Record</div>`+
      `<div class="tw-sub">🔥 tagged High Confidence cards</div>${rows(elite)}`+
      `<div class="tw-sub">📈 tagged Profitable cards</div>${rows(wide)}`+
      `<div class="tw-note">Graded W-L at the prices actually paid. `+
      `🔥 = ${_ruleTxt(HIGH_CONF&&HIGH_CONF.rule)}, 📈 = ${_ruleTxt(HIGH_CONF&&HIGH_CONF.rule_wide)}. `+
      `Green means the band beat the break-even ITS OWN prices demanded, not a flat -110. `+
      `<br><b>Measured on ${_nAll} priced picks since ${_since}</b> — the input-fix boundary. `+
      `Earlier picks are excluded on purpose: weather and pitcher Statcast were dead before then.</div>`;
'''


def main():
    if not os.path.exists(TARGET):
        sys.exit("ABORT: %s not found. Run this from the repo." % TARGET)

    src = io.open(TARGET, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied. Nothing to do.")
        return 0

    for label, old in (("rule fn", OLD_FN), ("widget", OLD_WIDGET)):
        n = src.count(old)
        if n != 1:
            sys.exit("ABORT: %s anchor matched %d times, expected 1. "
                     "NOTHING written." % (label, n))

    src = src.replace(OLD_FN, NEW_FN, 1).replace(OLD_WIDGET, NEW_WIDGET, 1)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = TARGET + ".%s.bak" % stamp
    shutil.copy2(TARGET, backup)
    print("backup   %s" % os.path.basename(backup))

    io.open(TARGET, "w", encoding="utf-8").write(src)
    print("spliced  run_picks_html.py  %d -> %d bytes" % (before, len(src)))

    try:
        py_compile.compile(TARGET, doraise=True)
    except Exception as e:
        shutil.copy2(backup, TARGET)
        sys.exit("ABORT: compile failed, backup restored.\n%s" % e)
    print("compile  OK")

    after = io.open(TARGET, encoding="utf-8").read()

    # The bug that cost two days of picks was a literal percent inside a query
    # string. This function now contains SQL, so check it the same way.
    sql_ok = True
    try:
        seg = after.split('"SELECT pick_type, conf, actual_result, odds FROM picks "', 1)[1]
        seg = seg.split("(since,)", 1)[0]
        sql_ok = "%" not in seg.replace("%s", "")
    except Exception:
        sql_ok = False

    checks = [
        ("HIGH_CONF_SINCE defined",  'HIGH_CONF_SINCE = "2026-08-19"' in after),
        ("since param on fn",        "def compute_high_conf_rule(since: str = None)" in after),
        ("date filter in SQL",       "AND pick_date >= %s" in after),
        ("odds pulled",              "SELECT pick_type, conf, actual_result, odds FROM picks" in after),
        ("flat 52.38 target gone",   "BREAK_EVEN = 52.38" not in after),
        ("widget 52.38 gone",        "wr>=52.38" not in after),
        ("pre-fix fallback gone",    "pre-fix baseline" not in after),
        ("legend built from rule",   "_ruleTxt(HIGH_CONF&&HIGH_CONF.rule)" in after),
        ("hardcoded legend gone",    "ML 80%+ earns" not in after),
        ("no literal % in new SQL",  sql_ok),
        ("no truncation",            len(after) > before - 2000),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, TARGET)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. After deploy, watch the log line:")
    print("  high-conf rule since 2026-08-19 from N priced graded picks: ...")
    print("If it says 'no band clears its own break-even', that is the honest")
    print("answer and no badge should show. Do not widen the rule to fill it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
One-shot splice: stop the Yesterday panel reading two graders.

WHY THIS EXISTS
---------------
On 2026-09-13 the Yesterday panel showed, stacked on top of each other:

    LOCK 3-1   STRONG 3-4   LEAN 7-5   TOSSUP 1-6   OVERALL 14-16
    17-17 Overall   +2.3% ROI   +$15.86

Both numbers were computed honestly. They grade different pick sets.

  * The tier chips and the OVERALL chip come from _ydayAgg(d), which reads
    d.graded_picks -- the DB rows, 31 of them on that date.
  * The line underneath comes from d.metrics.overall -- the JSON that
    run_analysis.py writes at 6am, which graded 35 picks. Its by_tier has no
    TOSSUP bucket at all and its LEAN is 9-6 where the DB says 7-5.

The 2026-08-18 "ONE SOURCE OF TRUTH FOR YESTERDAY" fix converted the chips and
the OVERALL chip to the DB. It did not convert `const m = d.metrics.overall`,
which still feeds the record, the ROI and the dollar profit on the row below.
Half the panel was migrated. This finishes it.

To be explicit about what was NOT wrong: the LOCK record. Both graders
independently report 4 LOCK picks on 2026-09-13 going 3-1, and exactly four
rows that day carry conf >= 0.68 (LOCK_THRESH), all four tagged LOCK.

SAFE TO RUN
-----------
  * Writes a timestamped .bak next to the file before touching anything.
  * Both anchors must match EXACTLY ONCE or it aborts having written nothing.
  * Idempotent: running it twice detects the marker and exits without changes.
  * Compiles the result and restores the backup if compilation fails.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_yesterday_overall.py
    python3 scripts\\predeploy_check.py
"""

import io
import os
import shutil
import sys
import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
TARGET = os.path.join(REPO, "run_picks_html.py")

MARKER = "ONE SOURCE OF TRUTH, PART 2"


# ── SPLICE 1: `m` comes from the DB rows, not the analysis JSON ──────────────
# Reassigning `m` itself is deliberate. Every downstream consumer (wr, roi,
# roiColor, roiSign, the record span, the profit span) then follows without a
# further edit, so there is no second place left that can quietly keep reading
# the JSON.

OLD_1 = """  const d   = DATA_YESTERDAY;
  const m   = d.metrics && d.metrics.overall;
  if(!m) return;
"""

NEW_1 = """  const d   = DATA_YESTERDAY;
  let   m   = d.metrics && d.metrics.overall;
  if(!m) return;

  // -- ONE SOURCE OF TRUTH, PART 2 (2026-09-14) ------------------------------
  // The 08-18 fix below rewired the tier chips and the OVERALL chip to
  // _ydayAgg (the DB rows). It left THIS line reading d.metrics.overall, the
  // analysis JSON written by run_analysis.py at 6am. So the panel still
  // carried two graders, one directly underneath the other.
  //
  // Seen on 2026-09-13: the chips summed to 14-16 over 31 DB rows while the
  // line beneath them read 17-17, with the ROI and dollar profit computed
  // over 35 JSON picks. Read together those two numbers look like the tier
  // records are wrong. They are not. LOCK 3-1 that day was confirmed correct
  // by both graders independently.
  //
  // Recompute the whole row from the SAME rows the chips are built from.
  const _pnl = _ydayPnl(d);
  if(_pnl) m = _pnl;
"""


# ── SPLICE 2: the helper, appended immediately after _ydayAgg ────────────────
# Declared after its call site, which is fine: function declarations hoist to
# the top of the enclosing function scope, and both live inside renderYesterday.

OLD_2 = """  const all = gp.map(norm);
  const w = all.filter(r => r.res === "WIN").length;
  const l = all.filter(r => r.res === "LOSS").length;
  return {
    overall: {wins:w, losses:l, total:w+l, win_rate:(w+l) ? w/(w+l) : 0},
    by_tier: roll("tier"),
    by_type: roll("type")
  };
}
"""

NEW_2 = OLD_2 + """
// Record, ROI and profit for the Yesterday row, derived from the same DB rows
// the tier chips are built from. Returns null when there are no DB rows, in
// which case the caller keeps the JSON metrics rather than showing nothing.
//
// Three decisions, each stated because each is a place this could drift back
// out of agreement with the chips above it:
//
//  1. FLAT ONE UNIT per pick, at the REAL stored price. Flat one unit is what
//     the JSON already used (its `staked` equals its `total`), so this changes
//     WHICH picks are counted and at WHAT price, not how they are staked. Real
//     price because of the standing rule: a number computed at an assumed -110
//     is not the number.
//  2. PUSH stakes nothing and returns nothing. Excluded from the record and
//     from the denominator. Never counted as a loss.
//  3. Rows with no tier are SKIPPED, mirroring roll() in _ydayAgg exactly. The
//     chips structurally cannot display an untiered pick, so counting one here
//     would put this row and those chips back out of sync, which is the entire
//     defect this function exists to close.
function _ydayPnl(d){
  const gp = (d && d.graded_picks) || [];
  if(!gp.length) return null;
  let w = 0, l = 0, pushes = 0, profit = 0, staked = 0, priced = 0, assumed = 0;
  gp.forEach(p => {
    if(!(p.tier || "").trim()) return;          // mirrors roll()'s `if(!k) return`
    const res = (p.actual_result || p.result || "").toUpperCase();
    if(res === "PUSH"){ pushes++; return; }
    if(res !== "WIN" && res !== "LOSS") return;
    // Anything graded before 2026-08-11 has no odds column, and |price| < 100
    // is corrupt data in this repo. Those fall back to -110 and are still
    // COUNTED -- dropping them would put this row back out of step with the
    // chips. `assumed` carries how many, so the caller can say so if needed.
    let o = (p.odds !== null && p.odds !== undefined && p.odds !== "")
              ? parseFloat(p.odds) : NaN;
    if(!isFinite(o) || Math.abs(o) < 100){ o = -110; assumed++; }
    else { priced++; }
    const dec = o > 0 ? 1 + o/100 : 1 + 100/Math.abs(o);
    staked += 1;
    if(res === "WIN"){ w++; profit += dec - 1; }
    else             { l++; profit -= 1; }
  });
  if(!(w + l)) return null;
  return {
    wins: w, losses: l, pushes: pushes, total: w + l,
    staked: staked, profit: profit,
    win_rate: w / (w + l),
    roi: staked ? profit / staked : 0,
    priced: priced, assumed: assumed
  };
}
"""


def main():
    if not os.path.exists(TARGET):
        sys.exit("ABORT: %s not found. Run this from the repo." % TARGET)

    src = io.open(TARGET, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied (found the PART 2 marker). Nothing to do.")
        return 0

    for label, old in (("SPLICE 1", OLD_1), ("SPLICE 2", OLD_2)):
        n = src.count(old)
        if n != 1:
            sys.exit(
                "ABORT: %s anchor matched %d times, expected exactly 1.\n"
                "Nothing was written. The file has drifted from what this\n"
                "script was built against -- re-read the anchor before forcing it."
                % (label, n)
            )

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = TARGET + ".%s.bak" % stamp
    shutil.copy2(TARGET, backup)
    print("backup   %s" % os.path.basename(backup))

    src = src.replace(OLD_1, NEW_1).replace(OLD_2, NEW_2)
    io.open(TARGET, "w", encoding="utf-8").write(src)
    print("spliced  %d -> %d bytes  (+%d)" % (before, len(src), len(src) - before))

    # Compile, and put the original back if we broke it.
    import py_compile
    try:
        py_compile.compile(TARGET, doraise=True)
    except Exception as e:
        shutil.copy2(backup, TARGET)
        sys.exit("ABORT: compile failed, backup restored.\n%s" % e)
    print("compile  OK")

    after = io.open(TARGET, encoding="utf-8").read()
    checks = [
        ("marker present",        MARKER in after),
        ("_ydayPnl defined",      "function _ydayPnl(d){" in after),
        ("_ydayPnl called",       "const _pnl = _ydayPnl(d);" in after),
        ("m is reassignable",     "let   m   = d.metrics && d.metrics.overall;" in after),
        ("old const m gone",      "const m   = d.metrics && d.metrics.overall;" not in after),
        ("_ydayAgg still intact", "function _ydayAgg(d){" in after),
        ("no truncation",         after.rstrip().endswith(("}", ")", '"', "'")) and len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = 0
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
        if not ok:
            bad += 1
    if bad:
        shutil.copy2(backup, TARGET)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % bad)

    print("\nDone. Next:  python3 scripts\\predeploy_check.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())

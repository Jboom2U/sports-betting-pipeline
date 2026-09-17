"""
One-shot splice: clear the last clamp / tier collision, on totals.

WHY THIS EXISTS
---------------
    model/mlb_model.py:1374   total_conf_base = min(0.68, 0.50 + abs(diff) / 16.0)
    model/mlb_picks.py:27     LOCK_THRESH     = 0.68

Same collision the run line had, and the clamp guard added on 2026-09-14 fails
the build on it. Measured against 412 graded TOTAL picks:

    TOTAL picks at exactly 0.680000 ......  4  (of 412)
    TOTAL LOCKs ..........................  10
    TOTAL LOCKs at exactly 0.680000 ......  4  (40%)

    the ten TOTAL LOCK confidences:
    0.7000  0.6956  0.6944  0.6900  0.6850  0.6825  0.6800  0.6800  0.6800  0.6800

Milder than the run line, where 72 of 77 LOCKs were the clamp. Six of these ten
are real computed numbers sitting ABOVE the cap, because total_adj and the other
adjustments are applied AFTER this clamp. The four at the floor are the bug: a
ceiling being read as a conviction.

WHY 0.675 AND NOT SOMETHING HIGHER
----------------------------------
CLAUDE.md carries a standing instruction: "Do NOT 'fix' this by raising the cap.
That just relabels the same non-information." That rule is about RAISING it to
let totals reach LOCK honestly, which would relabel noise. This LOWERS it by
0.005 purely to clear the tier boundary, so it does not cross that rule.

It is not a modelling opinion. The 2026-08-11 review found the fitted Platt
slope for totals is nearly flat (A = 0.179), and the refit on 2026-09-08 rejected
TOTAL calibration outright because out-of-sample Brier got WORSE (0.2473 to
0.2617). What the totals cap should actually be is an open question that needs
evidence, not a nudge. This only stops a saturated clamp from wearing a LOCK
badge.

EFFECT
------
Four historical picks would have read STRONG instead of LOCK. Totals that reach
0.68 or above THROUGH the adjustments are untouched, and should be: a computed
0.68 is a measurement, a clamped 0.68 is the model refusing to answer. Only the
second kind is being demoted.

SAFE TO RUN
-----------
Same guarantees as the other splice scripts: timestamped backup, anchor must
match exactly once or it aborts having written nothing, idempotent, py_compile,
and the backup is restored on any failure.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_total_cap_collision.py
    python3 scripts\\predeploy_check.py      # should now be green
"""

import io
import os
import shutil
import sys
import datetime
import py_compile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
MODEL = os.path.join(REPO, "model", "mlb_model.py")

MARKER = "TOTALS CLAMP / TIER COLLISION"

OLD = """        total_conf_base = min(0.68, 0.50 + abs(diff) / 16.0)
"""

NEW = """        # TOTALS CLAMP / TIER COLLISION, fixed 2026-09-16. Read before touching 0.675.
        #
        # This ceiling used to be 0.68, which is EXACTLY LOCK_THRESH in
        # model/mlb_picks.py, and tier() tests `conf >= LOCK_THRESH`. So a total
        # that saturated the clamp was stamped LOCK. On 412 graded TOTAL picks,
        # 4 sat at exactly 0.680000 and those 4 were 40% of all TOTAL LOCKs.
        #
        # The other six TOTAL LOCKs are genuine, sitting ABOVE the cap because
        # total_adj and friends are applied after this line. Those are untouched.
        # A computed 0.68 is a measurement; a clamped 0.68 is the model declining
        # to answer. Only the second kind loses the badge.
        #
        # 0.675 clears the boundary and nothing more. It is NOT a view on what the
        # totals ceiling should be. That question is open: the 2026-08-11 review
        # found the fitted Platt slope nearly flat (A=0.179), and the 2026-09-08
        # refit REJECTED TOTAL calibration because out-of-sample Brier got worse
        # (0.2473 -> 0.2617). CLAUDE.md's standing rule is not to RAISE this cap,
        # which would relabel non-information as confidence. Lowering it 0.005 to
        # clear a tier boundary is a different act.
        #
        # scripts/predeploy_check.py fails the build if this equals a tier
        # threshold again.
        total_conf_base = min(0.675, 0.50 + abs(diff) / 16.0)
"""


def main():
    if not os.path.exists(MODEL):
        sys.exit("ABORT: %s not found. Run this from the repo." % MODEL)

    src = io.open(MODEL, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied (found the totals marker). Nothing to do.")
        return 0

    n = src.count(OLD)
    if n != 1:
        sys.exit(
            "ABORT: anchor matched %d times, expected exactly 1. Nothing written." % n
        )

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = MODEL + ".%s.bak" % stamp
    shutil.copy2(MODEL, backup)
    print("backup   %s" % os.path.basename(backup))

    src = src.replace(OLD, NEW, 1)
    io.open(MODEL, "w", encoding="utf-8").write(src)
    print("spliced  mlb_model.py  %d -> %d bytes" % (before, len(src)))

    try:
        py_compile.compile(MODEL, doraise=True)
    except Exception as e:
        shutil.copy2(backup, MODEL)
        sys.exit("ABORT: compile failed, backup restored.\n%s" % e)
    print("compile  OK")

    after = io.open(MODEL, encoding="utf-8").read()
    checks = [
        ("totals marker present",   MARKER in after),
        ("cap now 0.675",           "total_conf_base = min(0.675," in after),
        ("old 0.68 cap gone",       "total_conf_base = min(0.68," not in after),
        ("RL dog cap still 0.675",  "RL_DOG_COVER_CAP = 0.675" in after),
        ("RL fav cap still 0.55",   "RL_FAV_COVER_CAP = 0.55" in after),
        ("no truncation",           len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [name for name, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, MODEL)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. predeploy_check should now report 0 collisions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

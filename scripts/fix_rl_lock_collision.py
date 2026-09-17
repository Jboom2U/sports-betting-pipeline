"""
One-shot splice: stop a saturated clamp from presenting as a LOCK.

WHY THIS EXISTS
---------------
    model/mlb_model.py:1484    RL_DOG_COVER_CAP = 0.68
    model/mlb_picks.py:27      LOCK_THRESH      = 0.68
    model/mlb_picks.py:41      if conf >= LOCK_THRESH: return "LOCK"

The ceiling on how often a dog is allowed to cover +1.5 sits on exactly the
same number as the LOCK tier boundary, and the tier comparison is `>=`, so
equality passes. Every run line that saturates the clamp is stamped LOCK.

Measured 2026-09-14 against the live database, n=561 graded RL picks:

    top ten RL confidences ever recorded
    0.7937  0.7128  0.6882  0.6836  0.6820  0.6800  0.6800  0.6800  0.6800  0.6800

    72 of the 77 RL LOCKs carry conf of EXACTLY 0.680000

Not close to it. Equal to it. Only five RL LOCKs in the entire history came
from a computed number. So "LOCK" on a run line has meant "the model wanted to
say something higher and was stopped" 94% of the time. It is the least
informative label on the board wearing the name of the most informative one.

WHAT THIS CHANGES
-----------------
1. model/mlb_model.py -- RL_DOG_COVER_CAP 0.68 -> 0.675, so a saturated dog
   cover lands in STRONG instead of LOCK. RL_FAV_COVER_CAP (0.55) is left
   alone: it collides with nothing.
2. scripts/predeploy_check.py -- a guard that fails the build if any clamp
   constant is equal to any tier threshold, so this class of bug cannot come
   back silently. This is the durable half of the fix.

WHAT THIS DELIBERATELY DOES NOT CHANGE
--------------------------------------
`total_conf_base = min(0.68, ...)` at mlb_model.py:1374 is the SAME collision,
and TOTAL has 10 LOCKs on record. It is left in place and the new guard will
report it, because totals carry adjustments after the cap so a saturated total
does not necessarily land exactly on 0.68, and CLAUDE.md carries a standing
instruction not to move the totals cap without evidence. Justin decides that
one with the guard output in front of him.

Also not changed: the VALUE of the cap as a piece of modelling. Today's base
rate test puts the real +1.5 cover rate for the dogs this model picks at
roughly 57-61%, which makes 0.675 still generous. Lowering it on that evidence
is a model change, not a collision fix, and is not in scope here.

SAFE TO RUN
-----------
  * Timestamped .bak for each file before it is touched.
  * Every anchor must match EXACTLY ONCE or it aborts having written nothing.
  * Idempotent: detects the marker and exits without changes on a second run.
  * py_compile on both files, and both backups restored if either fails.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_rl_lock_collision.py
    python3 scripts\\predeploy_check.py
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
PRECHK = os.path.join(HERE, "predeploy_check.py")

MARKER = "CLAMP / TIER COLLISION"


# ── SPLICE A: move the dog cover clamp off the LOCK boundary ─────────────────

OLD_A = """        RL_FAV_COVER_CAP = 0.55
        RL_DOG_COVER_CAP = 0.68
"""

NEW_A = """        #
        # CLAMP / TIER COLLISION, fixed 2026-09-14. Read before changing 0.675.
        #
        # RL_DOG_COVER_CAP used to be 0.68, which is EXACTLY LOCK_THRESH in
        # model/mlb_picks.py, and tier() tests `conf >= LOCK_THRESH`. So every
        # run line that saturated this clamp was stamped LOCK. Measured on 561
        # graded RL picks: 72 of the 77 RL LOCKs on record carry conf of
        # exactly 0.680000. Only five ever came from a computed number.
        #
        # A saturated clamp is the model saying "I do not know, this is my
        # ceiling". It was being displayed as maximum confidence. Moving the
        # cap off the boundary drops those into STRONG, where they belong.
        #
        # 0.675 is chosen only to clear the boundary. It is NOT an estimate of
        # how often these dogs cover. The 2026-09-14 base rate test puts that
        # at roughly 57-61%, so this ceiling is still generous. Lowering it on
        # that evidence is a separate, deliberate model change.
        #
        # scripts/predeploy_check.py now fails the build if any clamp here
        # equals any tier threshold, so this cannot silently return.
        RL_FAV_COVER_CAP = 0.55     # below STRONG_THRESH (0.62), collides with nothing
        RL_DOG_COVER_CAP = 0.675    # was 0.68 == LOCK_THRESH. Do not restore.
"""


# ── SPLICE B: the guard, so the collision cannot come back ───────────────────

OLD_B = "# ── Second-model review (advisory) ──"

NEW_B = '''# ── Clamp / tier collision guard ──────────────────────────────────────────────
# Added 2026-09-14. A clamp ceiling that is numerically equal to a tier
# threshold turns "the model hit its ceiling" into "the model is maximally
# confident", because tier() compares with >= and equality passes.
#
# This is not hypothetical. RL_DOG_COVER_CAP was 0.68 and LOCK_THRESH is 0.68,
# and 72 of 77 RL LOCKs ever recorded carried conf of exactly 0.680000. The
# label meant the opposite of what it said, for a whole season, on the board
# Justin was betting from.
#
# ERROR, not warning. A silent one of these is worth more than a route 500.
def _clamp_tier_collisions():
    import re as _re
    found, notes = [], []
    # NOTE: builtin open(), not io.open(). predeploy_check.py imports
    # ast/os/re/subprocess/sys/tempfile and NOT io, so io.open would raise
    # NameError, get swallowed by the except below, and this guard would print
    # "[skip]" forever while measuring nothing. That is the same shape as the
    # bug it exists to catch.
    try:
        _m = open(os.path.join(ROOT, "model", "mlb_model.py"), encoding="utf-8").read()
        _p = open(os.path.join(ROOT, "model", "mlb_picks.py"), encoding="utf-8").read()
    except Exception as _e:
        return [], ["[skip] clamp guard could not read sources: " + str(_e)]

    tiers = {}
    for name in ("LOCK_THRESH", "STRONG_THRESH", "LEAN_THRESH", "TOSSUP_THRESH"):
        mt = _re.search(r"^" + name + r"\\s*=\\s*([0-9.]+)", _p, _re.M)
        if mt:
            tiers[name] = float(mt.group(1))

    clamps = {}
    for name in ("RL_FAV_COVER_CAP", "RL_DOG_COVER_CAP"):
        mc = _re.search(r"^\\s*" + name + r"\\s*=\\s*([0-9.]+)", _m, _re.M)
        if mc:
            clamps[name] = float(mc.group(1))
    # `total_conf_base = min(0.68, ...)` -- the ceiling is the first argument.
    mt2 = _re.search(r"total_conf_base\\s*=\\s*min\\(\\s*([0-9.]+)", _m)
    if mt2:
        clamps["total_conf_base ceiling"] = float(mt2.group(1))

    if not tiers or not clamps:
        return [], ["[skip] clamp guard found no constants to compare"]

    for cname, cval in sorted(clamps.items()):
        for tname, tval in sorted(tiers.items()):
            if abs(cval - tval) < 1e-9:
                found.append(
                    "[CLAMP COLLISION]  " + cname + " = " + str(cval) + " equals "
                    + tname + ". Every pick that saturates this clamp will be "
                    "labelled " + tname.replace("_THRESH", "") + ". Move the clamp "
                    "off the boundary."
                )
    notes.append("  OK   clamp guard: " + str(len(clamps)) + " clamp(s) vs "
                 + str(len(tiers)) + " tier threshold(s), "
                 + (str(len(found)) + " collision(s)" if found else "no collisions"))
    return found, notes

_cc_errors, _cc_notes = _clamp_tier_collisions()
for _n in _cc_notes:
    print(_n)
errors.extend(_cc_errors)


# ── Second-model review (advisory) ──'''


def apply(path, old, new, label):
    src = io.open(path, encoding="utf-8").read()
    n = src.count(old)
    if n != 1:
        sys.exit(
            "ABORT: %s anchor matched %d times in %s, expected exactly 1.\n"
            "Nothing was written to any file."
            % (label, n, os.path.basename(path))
        )
    return src.replace(old, new, 1), len(src)


def main():
    for p in (MODEL, PRECHK):
        if not os.path.exists(p):
            sys.exit("ABORT: %s not found. Run this from the repo." % p)

    if MARKER in io.open(MODEL, encoding="utf-8").read():
        print("Already applied (found the collision marker). Nothing to do.")
        return 0

    # Resolve both splices BEFORE writing either, so a bad anchor on the second
    # file cannot leave the first one half-edited.
    new_model, old_model_len = apply(MODEL, OLD_A, NEW_A, "SPLICE A")
    new_prechk, old_prechk_len = apply(PRECHK, OLD_B, NEW_B, "SPLICE B")

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backups = {}
    for p in (MODEL, PRECHK):
        b = p + ".%s.bak" % stamp
        shutil.copy2(p, b)
        backups[p] = b
        print("backup   %s" % os.path.basename(b))

    io.open(MODEL, "w", encoding="utf-8").write(new_model)
    io.open(PRECHK, "w", encoding="utf-8").write(new_prechk)
    print("spliced  mlb_model.py       %d -> %d bytes" % (old_model_len, len(new_model)))
    print("spliced  predeploy_check.py %d -> %d bytes" % (old_prechk_len, len(new_prechk)))

    def restore(why):
        for p, b in backups.items():
            shutil.copy2(b, p)
        sys.exit("ABORT: %s. Both backups restored." % why)

    for p in (MODEL, PRECHK):
        try:
            py_compile.compile(p, doraise=True)
        except Exception as e:
            restore("compile failed on %s\n%s" % (os.path.basename(p), e))
    print("compile  OK  (both files)")

    after_m = io.open(MODEL, encoding="utf-8").read()
    after_p = io.open(PRECHK, encoding="utf-8").read()
    checks = [
        ("collision marker present",   MARKER in after_m),
        ("dog cap now 0.675",          "RL_DOG_COVER_CAP = 0.675" in after_m),
        ("old 0.68 dog cap gone",      "RL_DOG_COVER_CAP = 0.68\n" not in after_m),
        ("fav cap untouched at 0.55",  "RL_FAV_COVER_CAP = 0.55" in after_m),
        ("guard function defined",     "def _clamp_tier_collisions():" in after_p),
        ("guard wired into errors",    "errors.extend(_cc_errors)" in after_p),
        ("second-model review kept",   "Second-model review (advisory)" in after_p),
        ("no truncation",              len(after_m) > old_model_len and len(after_p) > old_prechk_len),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [name for name, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        restore("%d post-check(s) failed" % len(bad))

    print("")
    print("Done. Next:  python3 scripts\\predeploy_check.py")
    print("")
    print("EXPECT the guard to report ONE remaining collision:")
    print("  total_conf_base ceiling = 0.68 equals LOCK_THRESH")
    print("That one is left deliberately. See the header of this script.")
    print("It will FAIL the build until you decide on it, which is the point.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

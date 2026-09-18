"""
One-shot splice: stop the SQL percent guard firing on HTML templates.

WHY THIS EXISTS
---------------
The guard added on 2026-09-18 flagged 7 "problems" on its first real run, and
all 7 were false:

    app.py line 17   input[type=password]{{width:100%;...
    app.py line 29   .tier-bar-fill{{height:100%;...
    app.py line 30   .tier-bar-be{{...left:52.4%;...
    app.py line 36   .table-card table{{width:100%;...
    app.py line 89   Vertical line = break-even at 52.4% (standard -110 juice)
    app.py line 95   <th>Win %</th>...
    app.py line 107  <th>Win %</th>...

All CSS and HTML inside f-string templates. They never reach psycopg2, and the
`{{` escaping shows they are f-strings, not queries.

The bug was my filter. It accepted any triple-quoted block CONTAINING the word
INSERT, UPDATE, DELETE or SELECT anywhere in it, and a large HTML template
contains "<select>" or the word "Update" almost by accident.

THE FIX
-------
Require the block to BEGIN with a SQL keyword, which is how every real query in
this repo is written:

    cur.execute(
        \"\"\"
        INSERT INTO picks
        ...

An HTML template begins with "<!DOCTYPE", "<style", "<div" or similar and is
now excluded structurally rather than by luck.

WHY THIS MATTERS MORE THAN THE FALSE POSITIVES
----------------------------------------------
A check that cries wolf gets ignored, and an ignored check is worse than no
check, because it produces the feeling of coverage without the fact of it. The
guard exists to catch a bug that already cost two days of pick history. It has
to be trustworthy or it will be scrolled past the next time it is right.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_percent_guard_falsepos.py
    python3 scripts\\predeploy_check.py
"""

import io
import os
import shutil
import sys
import datetime
import py_compile

HERE = os.path.dirname(os.path.abspath(__file__))
PRECHK = os.path.join(HERE, "predeploy_check.py")

MARKER = "must BEGIN with a SQL keyword"

OLD = """            # Only look at blocks that are actually SQL.
            if not _re.search(r"\\b(INSERT|UPDATE|DELETE|SELECT)\\b", block, _re.I):
                continue
"""

NEW = """            # A real query must BEGIN with a SQL keyword. Matching the keyword
            # ANYWHERE (the first version of this guard) hit every large HTML
            # template in app.py, because one contains "<select>" and another
            # contains the word "Update". That produced 7 false positives on
            # the guard's first run: CSS like `width:100%` and table headers
            # like `Win %`, none of which ever reach psycopg2.
            #
            # Every genuine query in this repo is written as:
            #     cur.execute(
            #         \"\"\"
            #         INSERT INTO picks
            # so anchoring at the start excludes templates structurally rather
            # than by luck.
            if not _re.match(r"\\s*(INSERT|UPDATE|DELETE|SELECT|WITH|ALTER|CREATE)\\b",
                             block, _re.I):
                continue
"""


def main():
    if not os.path.exists(PRECHK):
        sys.exit("ABORT: %s not found. Run this from the repo." % PRECHK)

    src = io.open(PRECHK, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied. Nothing to do.")
        return 0

    n = src.count(OLD)
    if n != 1:
        sys.exit("ABORT: anchor matched %d times, expected 1. Nothing written." % n)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = PRECHK + ".%s.bak" % stamp
    shutil.copy2(PRECHK, backup)
    print("backup   %s" % os.path.basename(backup))

    src = src.replace(OLD, NEW, 1)
    io.open(PRECHK, "w", encoding="utf-8").write(src)
    print("spliced  predeploy_check.py  %d -> %d bytes" % (before, len(src)))

    try:
        py_compile.compile(PRECHK, doraise=True)
    except Exception as e:
        shutil.copy2(backup, PRECHK)
        sys.exit("ABORT: compile failed, backup restored.\n%s" % e)
    print("compile  OK")

    after = io.open(PRECHK, encoding="utf-8").read()
    checks = [
        ("marker present",        MARKER in after),
        ("anchored match used",   '_re.match(r"\\s*(INSERT|UPDATE|DELETE|SELECT|WITH|ALTER|CREATE)' in after),
        ("loose search gone",     '_re.search(r"\\b(INSERT|UPDATE|DELETE|SELECT)\\b", block' not in after),
        ("guard still wired",     "errors.extend(_sql_literal_percent())" in after),
        ("clamp guard intact",    "_clamp_tier_collisions" in after),
        ("no truncation",         len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, PRECHK)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. predeploy_check should now report 0 problems on the percent")
    print("guard. If it still flags something, read it: the guard is right more")
    print("often than it is wrong, and it was right about picks_store.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

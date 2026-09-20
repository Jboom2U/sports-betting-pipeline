"""
One-shot splice: remove the last literal percent sign from a SQL string.

WHAT THE GUARD FOUND
--------------------
    [SQL PERCENT]  db/schema.py SQL block line 32:
      -- external test had to synthesise it from an assumed 4% hold, which biases

This is inside the _PICKS CREATE TABLE string.

IS IT LIVE? NO. TODAY.
----------------------
_PICKS is run by `cur.execute(_PICKS)` at db/schema.py:294 with NO parameters,
and psycopg2 only percent-formats the query when a vars argument is supplied.
So this percent sign is inert right now.

SO WHY FIX IT
-------------
Because the day someone adds a parameter to that execute, it stops being inert,
and it fails inside create_all(), which is wrapped in:

    except Exception as e:
        log.warning(f"Schema creation failed (non-fatal): {e}")

That is a silent failure in the function that builds the tables. The same shape
of bug (a percent sign in a SQL comment, swallowed by a non-fatal except)
stopped every pick being written on 2026-09-17 and 2026-09-18 and was only
found by reading Railway logs three days later.

"No literal percent in any SQL string, ever" is a rule that can be followed
without thinking. "No literal percent in SQL strings that are executed with
parameters" is a rule that requires being right about which ones those are,
forever. The first rule costs one reworded comment.

The comment keeps its meaning. Only the symbol goes.

THIS EDITS A LOCKED FILE (db/schema.py). It changes a comment inside a
CREATE TABLE string and no column, type, constraint or index. Confirmed with
Justin 2026-09-20.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_schema_percent_landmine.py
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
SCHEMA = os.path.join(REPO, "db", "schema.py")

OLD = """    -- Price on the OTHER side of the same bet (added 2026-08-21). Without it
    -- no fade or two-sided CLV question is answerable from stored data, and an
    -- external test had to synthesise it from an assumed 4% hold, which biases
    -- the dog price optimistic. -196 does not imply +196.
"""

NEW = """    -- Price on the OTHER side of the same bet (added 2026-08-21). Without it
    -- no fade or two-sided CLV question is answerable from stored data, and an
    -- external test had to synthesise it from an assumed 4 percent hold, which
    -- biases the dog price optimistic. -196 does not imply +196.
    --
    -- The word "percent" is spelled out deliberately. psycopg2 formats the
    -- whole query string, so a literal percent symbol here becomes a landmine
    -- the moment this execute is ever given parameters, and it would fail
    -- inside create_all() where the except logs "non-fatal" and moves on.
    -- scripts/predeploy_check.py fails the build on any literal percent in a
    -- SQL string for exactly this reason.
"""


def main():
    if not os.path.exists(SCHEMA):
        sys.exit("ABORT: %s not found. Run this from the repo." % SCHEMA)

    src = io.open(SCHEMA, encoding="utf-8").read()
    before = len(src)

    if "assumed 4 percent hold" in src:
        print("Already applied. Nothing to do.")
        return 0

    n = src.count(OLD)
    if n != 1:
        sys.exit("ABORT: anchor matched %d times, expected 1. Nothing written." % n)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = SCHEMA + ".%s.bak" % stamp
    shutil.copy2(SCHEMA, backup)
    print("backup   %s" % os.path.basename(backup))

    src = src.replace(OLD, NEW, 1)
    io.open(SCHEMA, "w", encoding="utf-8").write(src)
    print("spliced  schema.py  %d -> %d bytes" % (before, len(src)))

    try:
        py_compile.compile(SCHEMA, doraise=True)
    except Exception as e:
        shutil.copy2(backup, SCHEMA)
        sys.exit("ABORT: compile failed, backup restored.\n%s" % e)
    print("compile  OK")

    after = io.open(SCHEMA, encoding="utf-8").read()

    # Every triple-quoted SQL block in this file must now survive the exact
    # operation psycopg2 performs, which is what the original bug did not.
    import re
    bad_blocks = []
    for i, block in enumerate(re.findall(r'"""(.*?)"""', after, re.DOTALL)):
        if not re.match(r"\s*(CREATE|ALTER|INSERT|UPDATE|SELECT|DELETE)\b", block, re.I):
            continue
        stripped = block.replace("%%", "").replace("%s", "")
        stripped = re.sub(r"%\(\w+\)s", "", stripped)
        if "%" in stripped:
            bad_blocks.append(i)

    checks = [
        ("percent symbol removed",  "assumed 4% hold" not in after),
        ("wording preserved",       "assumed 4 percent hold" in after),
        ("warning note added",      "spelled out deliberately" in after),
        ("opp_odds column intact",  "opp_odds        REAL," in after),
        ("no SQL block has a bare percent", not bad_blocks),
        ("no truncation",           len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, SCHEMA)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. predeploy_check should now be green on the percent guard.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

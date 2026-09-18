"""
URGENT one-shot splice: a literal percent sign in a SQL comment has been
stopping every pick from being saved since 2026-09-17.

WHAT HAPPENED
-------------
scripts/fix_firstpitch_snapshot.py added this comment INSIDE the save_picks
query string:

    -- from 80.9% to 81.2% after the game was under way. So

psycopg2 percent-formats the entire query before sending it, so a literal `%`
is read as a format specifier. With a tuple of arguments a stray `%` raises

    IndexError: tuple index out of range

save_picks catches every exception and logs one non-fatal WARNING, so the
pipeline carried on reporting "15 games | 28 picks" while writing NOTHING:

    2026-09-18 10:04:00 [WARNING] save_picks DB write failed (non-fatal): tuple index out of range
    2026-09-18 14:04:44 [WARNING] save_picks DB write failed (non-fatal): tuple index out of range
    2026-09-18 14:39:47 [WARNING] save_picks DB write failed (non-fatal): tuple index out of range
    2026-09-18 15:13:09 [WARNING] save_picks DB write failed (non-fatal): tuple index out of range
    2026-09-18 15:28:28 [WARNING] save_picks DB write failed (non-fatal): tuple index out of range

    [export] 0 graded picks since 2026-09-17

Every pick from 2026-09-17 and 2026-09-18 is missing from the database. The
boards rendered fine, because the dashboard is built from the model directly
and never reads the picks table. Only the RECORD was lost.

A SQL comment took down the write path. The arity was correct the whole time:
19 placeholders, 19 parameters, 21 columns, 21 value slots.

WHAT THIS CHANGES
-----------------
1. db/picks_store.py -- removes the two percent signs and replaces them with a
   warning that is itself percent-free.
2. scripts/predeploy_check.py -- scans every triple-quoted SQL literal in the
   DB modules for a literal `%` that is not `%s` or `%%`, and FAILS the build.
   That is the durable half. One instance fixed is worth less than the class
   being made impossible, which is the same reasoning as the clamp guard.

AFTERWARDS
----------
The 2026-09-18 picks will repair themselves: the pipeline re-saves on every
re-score, so the next run writes them. 2026-09-17 is gone from the DB, but NOT
gone: the pipeline uploads picks/mlb_picks_2026-09-17.csv to R2 every day, so
that slate can be backfilled from storage. Do that as a separate, deliberate
step.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_sql_percent_breakage.py
    python3 scripts\\predeploy_check.py
    git add db/picks_store.py scripts/predeploy_check.py
    git commit -m "Fix literal percent in save_picks SQL that blocked every write; guard against it"
    git push
"""

import io
import os
import shutil
import sys
import datetime
import py_compile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
STORE = os.path.join(REPO, "db", "picks_store.py")
PRECHK = os.path.join(HERE, "predeploy_check.py")

MARKER = "NEVER put a literal percent sign"


OLD_1 = """                        -- pitch". They do not: the board re-scored a Cubs card
                        -- from 80.9% to 81.2% after the game was under way. So
                        -- closing_odds could absorb a price captured DURING the
                        -- game, and every CLV number is measured against it.
                        -- A close that moves after the close is not a close.
"""

NEW_1 = """                        -- pitch". They do not: the board re-scored a Cubs card
                        -- from 80.9 to 81.2 after the game was under way. So
                        -- closing_odds could absorb a price captured DURING the
                        -- game, and every CLV number is measured against it.
                        -- A close that moves after the close is not a close.
                        --
                        -- NEVER put a literal percent sign in this string, not
                        -- even in a comment. psycopg2 percent-formats the whole
                        -- query before sending it, so "80.9 percent" written with
                        -- the symbol is parsed as a format specifier and
                        -- execute() dies with "tuple index out of range".
                        -- That is not hypothetical. The two symbols that used to
                        -- be on the line above stopped EVERY pick being saved for
                        -- two days (2026-09-17 and 2026-09-18) while the except
                        -- at the bottom of this function logged one non-fatal
                        -- WARNING per run and the pipeline cheerfully reported
                        -- "15 games | 28 picks". Escape as two percent signs if
                        -- one is ever genuinely needed.
"""


OLD_2 = "# ── Clamp / tier collision guard ──"

NEW_2 = '''# ── Literal percent in SQL guard ──────────────────────────────────────────────
# psycopg2 percent-formats the entire query string before sending it. A literal
# `%` that is not `%s` or `%%` is read as a format specifier, and with a tuple of
# arguments it raises IndexError("tuple index out of range").
#
# This is not hypothetical. On 2026-09-17 a SQL COMMENT reading "from 80.9% to
# 81.2%" stopped every pick being written to the database for two days. The
# arity was perfect: 19 placeholders, 19 parameters. save_picks swallows all
# exceptions and logs one non-fatal WARNING, so the pipeline kept reporting
# "15 games | 28 picks" while writing nothing, and the loss was only found by
# reading Railway logs three days later.
#
# ERROR, not warning. A silent write failure is the worst failure this project
# has.
def _sql_literal_percent():
    import re as _re
    found, scanned = [], 0
    targets = [("db", "picks_store.py"), ("db", "schema.py"),
               ("db", "pipeline_log.py"), ("db", "model_config.py"),
               ("app.py",)]
    for parts in targets:
        path = os.path.join(ROOT, *parts)
        if not os.path.exists(path):
            continue
        try:
            src = open(path, encoding="utf-8").read()
        except Exception:
            continue
        for block in _re.findall(r'"""(.*?)"""', src, _re.DOTALL):
            # Only look at blocks that are actually SQL.
            if not _re.search(r"\\b(INSERT|UPDATE|DELETE|SELECT)\\b", block, _re.I):
                continue
            scanned += 1
            # Strip the legal forms, then any surviving % is a literal.
            stripped = block.replace("%%", "").replace("%s", "")
            stripped = _re.sub(r"%\\(\\w+\\)s", "", stripped)
            if "%" in stripped:
                for ln, line in enumerate(block.splitlines(), 1):
                    bare = line.replace("%%", "").replace("%s", "")
                    bare = _re.sub(r"%\\(\\w+\\)s", "", bare)
                    if "%" in bare:
                        found.append(
                            "[SQL PERCENT]  " + os.path.join(*parts)
                            + " SQL block line " + str(ln)
                            + ": literal percent sign -> psycopg2 will raise "
                            + "'tuple index out of range' and the write will "
                            + "silently fail. Remove it or escape as two percent "
                            + "signs.  " + line.strip()[:90]
                        )
    print("  OK   sql percent guard: " + str(scanned) + " SQL block(s) scanned, "
          + (str(len(found)) + " problem(s)" if found else "none clean"))
    return found

errors.extend(_sql_literal_percent())


# ── Clamp / tier collision guard ──'''


PLAN = [
    (STORE, [("percent removal", OLD_1, NEW_1)]),
    (PRECHK, [("percent guard", OLD_2, NEW_2)]),
]


def main():
    for p in (STORE, PRECHK):
        if not os.path.exists(p):
            sys.exit("ABORT: %s not found. Run this from the repo." % p)

    if MARKER in io.open(STORE, encoding="utf-8").read():
        print("Already applied. Nothing to do.")
        return 0

    resolved = {}
    for path, splices in PLAN:
        src = io.open(path, encoding="utf-8").read()
        before = len(src)
        for label, old, new in splices:
            n = src.count(old)
            if n != 1:
                sys.exit(
                    "ABORT: %s anchor matched %d times in %s, expected 1.\n"
                    "NOTHING was written." % (label, n, os.path.basename(path)))
            src = src.replace(old, new, 1)
        resolved[path] = (src, before)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backups = {}
    for path in resolved:
        b = path + ".%s.bak" % stamp
        shutil.copy2(path, b)
        backups[path] = b
        print("backup   %s" % os.path.basename(b))

    for path, (src, before) in resolved.items():
        io.open(path, "w", encoding="utf-8").write(src)
        print("spliced  %-18s %d -> %d bytes" % (os.path.basename(path), before, len(src)))

    def restore(why):
        for p, b in backups.items():
            shutil.copy2(b, p)
        sys.exit("ABORT: %s. All backups restored." % why)

    for path in resolved:
        try:
            py_compile.compile(path, doraise=True)
        except Exception as e:
            restore("compile failed on %s\n%s" % (os.path.basename(path), e))
    print("compile  OK  (both files)")

    store = io.open(STORE, encoding="utf-8").read()
    prechk = io.open(PRECHK, encoding="utf-8").read()

    # The real test: simulate what psycopg2 does to the query.
    import re
    sql = None
    for block in re.findall(r'"""(.*?)"""', store, re.DOTALL):
        if "INSERT INTO picks" in block:
            sql = block
            break
    ok_fmt, fmt_err = False, ""
    if sql is not None:
        try:
            sql % tuple(["x"] * sql.count("%s"))
            ok_fmt = True
        except Exception as e:
            fmt_err = str(e)

    checks = [
        ("marker present",            MARKER in store),
        ("80.9 percent symbol gone",  "80.9%" not in store),
        ("81.2 percent symbol gone",  "81.2%" not in store),
        ("INSERT block found",        sql is not None),
        ("query percent-formats OK",  ok_fmt),
        ("guard defined",             "def _sql_literal_percent():" in prechk),
        ("guard wired into errors",   "errors.extend(_sql_literal_percent())" in prechk),
        ("clamp guard still there",   "_clamp_tier_collisions" in prechk),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if fmt_err:
        print("  format error was: " + fmt_err)
    if bad:
        restore("%d post-check(s) failed" % len(bad))

    print("")
    print("Done. The query now survives percent-formatting, which is the")
    print("exact operation psycopg2 performs and the one that was failing.")
    print("Next:  python3 scripts\\predeploy_check.py  then commit and push.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

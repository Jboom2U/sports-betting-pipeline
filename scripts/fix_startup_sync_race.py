"""
THE root cause of every board blackout this month. A startup race.

WHAT HAPPENS
------------
On any container restart, Railway's filesystem is empty and the app downloads
every CSV back from R2. That download takes about six minutes for ~1,435 files.
The cache regeneration waits 180 seconds for it, gives up, and builds the board
anyway against a half-empty data/clean/.

Observed on 2026-09-29, and the shape is identical on 09-17, 09-18 and 09-20:

    20:27:38  Scoring 3 upcoming games for 2026-09-29     <- healthy
    20:34:41  CSV sync upload complete: 977 file(s) pushed
    20:50:05  Starting Container                           <- deploy restart
    20:54:23  [WARNING] Cache regen: CSV sync not confirmed after 180s —
                        proceeding, model data may be incomplete.
    20:54:24  Scoring 0 upcoming games for 2026-09-29     <- built on empty data
    20:56:26  CSV sync download complete: 1435 file(s)    <- arrives 2 min later
    20:56:26  Today's pipeline data exists -- skipping full pipeline run.
    20:56:26  Scoring 0 upcoming games for 2026-09-29     <- still dead

Once that empty board is cached, nothing recovers it on its own:
`Today's pipeline data exists` blocks a re-scrape, and generation keeps
returning None, so the site serves a fallback page until a human runs
/force-pipeline by hand.

This is why the schedule master "kept emptying". It never did. R2 was correct
the whole time. The board was simply built before the data finished arriving,
and I spent days blaming the scraper, the R2 overwrite and the deploy window.

THE FIX
-------
1. CSV_SYNC_WAIT goes 180s -> 900s, comfortably past the observed ~360s.
2. On timeout it NO LONGER PROCEEDS. It keeps waiting up to a hard ceiling,
   and if the sync still has not landed it returns WITHOUT touching the cache.

   Building a board from partial data is strictly worse than building none.
   None leaves the previous cache intact and retries on the next cycle. A board
   scored against an empty data/clean/ poisons the cache, reports zero games,
   and sticks.

   This is the same principle already load-bearing elsewhere in this repo:
   a missing value is safe, a wrong value is not.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_startup_sync_race.py
    python3 scripts\\predeploy_check.py

Deploy, wait for green, then /force-pipeline. After the NEXT restart the board
should come back by itself with no intervention. That is the test.
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

MARKER = "CSV_SYNC_WAIT"

OLD = '''GENERATION_TIMEOUT = 4 * 60   # 4 minutes — if generation hangs past this, force-unblock


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
'''

NEW = '''GENERATION_TIMEOUT = 4 * 60   # 4 minutes — if generation hangs past this, force-unblock

# How long to wait for the startup R2 download before regenerating the board.
#
# WAS 180s, AND THAT WAS THE BUG. On Railway the startup sync pulls ~1,435 files
# and was measured at about SIX MINUTES on 2026-09-29. The old code waited 180s,
# logged "proceeding, model data may be incomplete", and built the board against
# a half-empty data/clean/. It scored 0 games, cached that, and the site served
# a fallback page until someone ran /force-pipeline by hand.
#
# That single race produced every board blackout in September (09-17, 09-18,
# 09-20, 09-29). It was repeatedly misdiagnosed as the scraper, an R2 overwrite,
# and deploying at the wrong time of day. R2 was correct every time.
CSV_SYNC_WAIT    = 15 * 60   # generous: the sync is ~6 min, give it room
CSV_SYNC_CEILING = 25 * 60   # past this, give up WITHOUT building anything


def _regenerate_in_background():
    """Kick off a background thread to refresh the cache without blocking requests."""
    def _worker():
        started = time.time()
        try:
            # Wait for the startup CSV sync before touching the model. Without
            # this the model can load an empty data/clean/ and score on defaults.
            #
            # DO NOT "PROCEED ANYWAY" ON TIMEOUT. Building a board from partial
            # data is strictly worse than building none: None leaves the previous
            # cache intact and the next cycle retries, whereas a board scored
            # against an empty data/clean/ caches zero games and sticks there.
            # Same principle as the price handling in this repo -- a missing
            # value is safe, a wrong value is not.
            if not _csv_ready.wait(timeout=CSV_SYNC_WAIT):
                log.warning(
                    f"Cache regen: CSV sync still running after {CSV_SYNC_WAIT}s. "
                    f"NOT building a board on partial data — continuing to wait.")
                if not _csv_ready.wait(timeout=CSV_SYNC_CEILING - CSV_SYNC_WAIT):
                    log.error(
                        f"Cache regen ABANDONED: CSV sync never completed within "
                        f"{CSV_SYNC_CEILING}s. Cache left untouched so the previous "
                        f"board keeps serving. Check storage connectivity, then run "
                        f"/force-pipeline.")
                    return
                log.info("CSV sync landed after the first wait — regenerating now.")
'''


def main():
    if not os.path.exists(APP):
        sys.exit("ABORT: %s not found. Run this from the repo." % APP)

    src = io.open(APP, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied. Nothing to do.")
        return 0

    n = src.count(OLD)
    if n != 1:
        sys.exit("ABORT: anchor matched %d times, expected 1. Nothing written." % n)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = APP + ".%s.bak" % stamp
    shutil.copy2(APP, backup)
    print("backup   %s" % os.path.basename(backup))

    src = src.replace(OLD, NEW, 1)
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
        ("CSV_SYNC_WAIT defined",    "CSV_SYNC_WAIT    = 15 * 60" in after),
        ("ceiling defined",          "CSV_SYNC_CEILING = 25 * 60" in after),
        ("180s wait gone",           "_csv_ready.wait(timeout=180)" not in after),
        ("no longer proceeds",       "proceeding, model data may be incomplete" not in after),
        ("returns without building", "Cache regen ABANDONED" in after),
        ("early return present",     "/force-pipeline.\")\n                    return" in after),
        ("generation still called",  "html = _generate()" in after),
        ("no truncation",            len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, APP)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. THE TEST FOR THIS FIX is the next restart: the board should")
    print("come back on its own without anyone running /force-pipeline.")
    print("Watch for this line, which should no longer appear:")
    print("  'proceeding, model data may be incomplete'")
    return 0


if __name__ == "__main__":
    sys.exit(main())

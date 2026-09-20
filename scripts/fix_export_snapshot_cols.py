"""
One-shot splice: expose the first-pitch snapshot columns in the CSV export.

WHY THIS EXISTS
---------------
scripts/fix_firstpitch_snapshot.py added four columns to `picks`:

    final_conf, final_tier, final_odds, pregame_locked_at

and nothing can read them. `/admin/export/picks.csv` builds its query from a
fixed COLS list that does not include them, and `/admin/data-health` was written
on 2026-08-22, before they existed. So the snapshot could be silently failing to
write and there would be no way to find out.

That is the exact failure this project keeps repeating: a value is produced,
nothing validates it, and the gap stays invisible. Adding the column to the
writer without adding it to a reader is half a feature.

WHAT THIS CHANGES
-----------------
app.py, one list. COLS drives both the SELECT and the CSV header, so naming the
four columns there is the whole change. No new route, no new query.

HOW TO USE IT AFTERWARDS
------------------------
Open https://statalizers.com/admin/export/picks.csv and look at the four new
right-hand columns on a date whose games have started:

  * pregame_locked_at non-null  -> the snapshot is working.
  * null everywhere             -> game_time_utc is not reaching game_data and
                                   _game_started is failing safe by design.
                                   Nothing is lost or misgraded, but the snapshot
                                   is not happening. That is the thing to fix.
  * final_conf equal to conf on rows where tier_locked is true -> expected, the
    two freezes agree. They should only diverge on games whose lineups never
    confirmed, which is the hole the snapshot exists to close.

SAFE TO RUN
-----------
Same guarantees as the other splice scripts: timestamped backup, the anchor must
match exactly once or it aborts having written nothing, idempotent, py_compile,
backup restored on any failure.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_export_snapshot_cols.py
    python3 scripts\\predeploy_check.py
    git add app.py
    git commit -m "Expose first-pitch snapshot columns in the picks export"
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
APP = os.path.join(REPO, "app.py")

MARKER = "final_conf"

OLD = """    COLS = ["pick_date", "game_id", "game", "pick_type", "label", "team",
            "conf", "tier", "market_signal", "tier_locked", "was_best_bet",
            "model_version", "odds", "odds_at", "closing_odds", "closing_odds_at",
            "actual_result", "away_final", "home_final", "graded_at"]
"""

NEW = """    # final_* and pregame_locked_at added 2026-09-17. They are the immutable
    # first-pitch snapshot written by db/picks_store.save_picks. Exported because
    # a column nothing can read is a column nobody can verify: the snapshot could
    # fail to write for a whole season and look identical to it working.
    # pregame_locked_at is the one to watch. Null everywhere means game_time_utc
    # is not reaching game_data and _game_started is failing safe.
    COLS = ["pick_date", "game_id", "game", "pick_type", "label", "team",
            "conf", "tier", "market_signal", "tier_locked", "was_best_bet",
            "model_version", "odds", "odds_at", "closing_odds", "closing_odds_at",
            "final_conf", "final_tier", "final_odds", "pregame_locked_at",
            "actual_result", "away_final", "home_final", "graded_at"]
"""


def main():
    if not os.path.exists(APP):
        sys.exit("ABORT: %s not found. Run this from the repo." % APP)

    src = io.open(APP, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied (COLS already names final_conf). Nothing to do.")
        return 0

    n = src.count(OLD)
    if n != 1:
        sys.exit(
            "ABORT: anchor matched %d times, expected exactly 1. Nothing written.\n"
            "The COLS list has drifted. Re-read it before forcing this." % n
        )

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
    block = after.split("COLS = [", 1)[1].split("]", 1)[0]
    n_cols = len([c for c in block.replace("\n", " ").split(",") if c.strip()])

    checks = [
        ("final_conf in COLS",        '"final_conf"' in block),
        ("final_tier in COLS",        '"final_tier"' in block),
        ("final_odds in COLS",        '"final_odds"' in block),
        ("pregame_locked_at in COLS", '"pregame_locked_at"' in block),
        ("graded_at still last",      block.rstrip().endswith('"graded_at"')),
        ("24 columns (was 20)",       n_cols == 24),
        ("no truncation",             len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [name for name, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, APP)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. Commit and push, then open:")
    print("  https://statalizers.com/admin/export/picks.csv")
    print("and look at pregame_locked_at on a date whose games have started.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

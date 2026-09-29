"""
URGENT: the scraper cannot see postseason games. Wild Card starts tomorrow.

WHAT IS WRONG
-------------
scrapers/mlb_scraper.py filters the MLB API to regular season in two places:

    line  50   "gameType": "R"    # Regular season only; add "P" for playoffs later
    line 202   "gameType": "R"

The TODO in that comment came due on 2026-09-27, when the regular season ended.

Consequences, both already visible in the Railway logs tonight:

  * fetch_schedule() returns nothing for 2026-09-29, so the board logs
    "Scoring 0 upcoming games for 2026-09-29" even though the MLB API has four
    Wild Card games that day. No postseason game can ever appear on the board.
  * fetch_scores() would not return a postseason final either, so even if a
    pick somehow existed it could never be graded.

This is NOT a date bug and NOT a cache bug. The engine knows today is the 28th
and correctly pivots to the 29th. It finds nothing there because the scraper
asked the API to exclude it.

THE FIX
-------
A single GAME_TYPES constant used by both calls:

    R  regular season
    F  Wild Card
    D  Division Series
    L  League Championship Series
    W  World Series

Deliberately NOT included: S (spring training), A (all-star), E (exhibition).
Dropping the filter entirely would pull those in every February.

VERIFIED BEFORE IT WRITES
-------------------------
This script calls the live MLB API first and refuses to touch your files unless
it can prove three things:

  1. gameType=R really does return 0 games on 2026-09-29  (the bug is real)
  2. the new filter really does return those games        (the fix works)
  3. the API accepts a comma-separated gameType at all    (no guessing)

If the API disagrees with any of that, nothing is modified and it tells you why.
I could not confirm this from my side, so the check runs on yours.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_postseason_gametype.py
    python3 scripts\\predeploy_check.py

Deploy tonight. There are no games today, so a restart cannot interrupt a slate,
and tomorrow's 6am run needs this in place to see the Wild Card round.
"""

import io
import json
import os
import shutil
import sys
import datetime
import py_compile
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SCRAPER = os.path.join(REPO, "scrapers", "mlb_scraper.py")

MARKER = "GAME_TYPES"
NEW_TYPES = "R,F,D,L,W"
PROBE_DATE = "2026-09-29"


def api_games(date, game_type=None):
    url = ("https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=" + date)
    if game_type:
        url += "&gameType=" + game_type
    req = urllib.request.Request(url, headers={"User-Agent": "statalizers-fixcheck/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode("utf-8"))
    out = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            out.append((g.get("gameType", "?"),
                        g["teams"]["away"]["team"]["name"],
                        g["teams"]["home"]["team"]["name"]))
    return out


OLD_1 = '''    params = {
        "sportId": 1,
        "date": date,
        "hydrate": "linescore,decisions,probablePitcher",
        "gameType": "R"          # Regular season only; add "P" for playoffs later
    }
'''

NEW_1 = '''    params = {
        "sportId": 1,
        "date": date,
        "hydrate": "linescore,decisions,probablePitcher",
        "gameType": GAME_TYPES
    }
'''

OLD_2 = '''        params = {
            "sportId": 1,
            "date": target_date,
            "hydrate": "probablePitcher,venue",
            "gameType": "R"
        }
'''

NEW_2 = '''        params = {
            "sportId": 1,
            "date": target_date,
            "hydrate": "probablePitcher,venue",
            "gameType": GAME_TYPES
        }
'''


def main():
    if not os.path.exists(SCRAPER):
        sys.exit("ABORT: %s not found. Run this from the repo." % SCRAPER)

    src = io.open(SCRAPER, encoding="utf-8").read()
    before = len(src)

    if MARKER in src:
        print("Already applied. Nothing to do.")
        return 0

    # ── Prove the bug and the fix against the live API BEFORE writing ────────
    print("Checking the live MLB API for %s ..." % PROBE_DATE)
    try:
        none_f = api_games(PROBE_DATE)
        only_r = api_games(PROBE_DATE, "R")
        new_f  = api_games(PROBE_DATE, NEW_TYPES)
    except Exception as e:
        sys.exit("ABORT: could not reach the MLB API, so the fix cannot be "
                 "verified. Nothing written.\n%s" % e)

    print("  no filter        %d game(s)" % len(none_f))
    print("  gameType=R       %d game(s)   <- what the code asks for today" % len(only_r))
    print("  gameType=%s  %d game(s)   <- what the fix asks for" % (NEW_TYPES, len(new_f)))
    for gt, a, h in new_f:
        print("      [%s] %s @ %s" % (gt, a, h))

    if len(none_f) == 0:
        sys.exit("ABORT: the API reports no games at all on %s, so this probe "
                 "cannot prove anything. Nothing written." % PROBE_DATE)
    if len(only_r) != 0:
        sys.exit("ABORT: gameType=R already returns %d game(s) on %s, so the "
                 "diagnosis is wrong. Nothing written." % (len(only_r), PROBE_DATE))
    if len(new_f) != len(none_f):
        sys.exit("ABORT: the new filter returns %d game(s) but the unfiltered "
                 "call returns %d. The gameType list is incomplete. Nothing "
                 "written." % (len(new_f), len(none_f)))
    print("  verified: the bug is real and this filter fixes it.")
    print("")

    for label, old in (("fetch_scores", OLD_1), ("fetch_schedule", OLD_2)):
        n = src.count(old)
        if n != 1:
            sys.exit("ABORT: %s anchor matched %d times, expected 1. "
                     "Nothing written." % (label, n))

    # Insert the constant after the MLB_API definition.
    import re as _re
    m = _re.search(r"^MLB_API\s*=.*$", src, _re.M)
    if not m:
        sys.exit("ABORT: could not find the MLB_API constant. Nothing written.")

    const_block = m.group(0) + '''

# Which MLB game types the scrapers are allowed to see.
#   R  regular season
#   F  Wild Card
#   D  Division Series
#   L  League Championship Series
#   W  World Series
# Deliberately excludes S (spring training), A (all-star) and E (exhibition):
# dropping the filter entirely would pull those in every February.
#
# This was "R" alone until 2026-09-28, with a comment reading "Regular season
# only; add P for playoffs later". The regular season ended on 09-27 and the
# board logged "Scoring 0 upcoming games for 2026-09-29" while the MLB API had
# four Wild Card games that day. No postseason game could reach the board and
# no postseason final could be fetched to grade one.
GAME_TYPES = "%s"''' % NEW_TYPES

    src = src.replace(m.group(0), const_block, 1)
    src = src.replace(OLD_1, NEW_1, 1).replace(OLD_2, NEW_2, 1)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = SCRAPER + ".%s.bak" % stamp
    shutil.copy2(SCRAPER, backup)
    print("backup   %s" % os.path.basename(backup))

    io.open(SCRAPER, "w", encoding="utf-8").write(src)
    print("spliced  mlb_scraper.py  %d -> %d bytes" % (before, len(src)))

    try:
        py_compile.compile(SCRAPER, doraise=True)
    except Exception as e:
        shutil.copy2(backup, SCRAPER)
        sys.exit("ABORT: compile failed, backup restored.\n%s" % e)
    print("compile  OK")

    after = io.open(SCRAPER, encoding="utf-8").read()
    checks = [
        ("GAME_TYPES defined",      'GAME_TYPES = "%s"' % NEW_TYPES in after),
        ("scores uses it",          '"hydrate": "linescore,decisions,probablePitcher",\n        "gameType": GAME_TYPES' in after),
        ("schedule uses it",        '"hydrate": "probablePitcher,venue",\n            "gameType": GAME_TYPES' in after),
        ('no hardcoded "R" left',   '"gameType": "R"' not in after),
        ("no truncation",           len(after) > before),
    ]
    width = max(len(c[0]) for c in checks)
    bad = [n for n, ok in checks if not ok]
    for name, ok in checks:
        print("  %-*s  %s" % (width, name, "ok" if ok else "FAILED"))
    if bad:
        shutil.copy2(backup, SCRAPER)
        sys.exit("ABORT: %d post-check(s) failed, backup restored." % len(bad))

    print("")
    print("Done. Commit, push, and after the deploy hit /force-pipeline so the")
    print("schedule master picks up the Wild Card games tonight rather than")
    print("waiting for 6am.")
    print("")
    print("STILL OPEN, not fixed here: fetch_schedule only looks 2 days ahead.")
    print("That is fine within a round but may miss the next round across a")
    print("multi-day gap. Worth widening once the board is working.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

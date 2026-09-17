"""
One-shot splice: give every pick an immutable first-pitch snapshot.

WHY THIS EXISTS
---------------
`tier_locked` freezes conf/tier when a game's LINEUPS confirm, roughly two hours
before first pitch. That was the right instinct and it leaves two holes:

  1. A game whose lineups never confirm never freezes at all. On 2026-09-13,
     Boston Red Sox ML carried tier_locked = False, so its stored confidence
     kept moving until the last write of the night.
  2. The BOARD does not read the frozen row. It re-renders from a fresh scoring
     run, so what is displayed keeps drifting after the record has stopped. The
     Cubs ML card read 80.9% in a screenshot and 81.2% on the board an hour
     later, same pick, same night.

The consequence is the thing Justin keeps running into: he remembers what the
board said, the record says something else, and there is no stored value that
settles it. "I looked at it yesterday and it had like 8 Locks" is unanswerable
today, because nothing ever wrote down what the board said at bet time.

CLAUDE.md has carried this since 2026-08-18, under the paid-service note:
"A published record has to be immutable... nothing writes a timestamped
pre game snapshot that is never edited. A track record that can move after the
fact is not one worth selling."

WHAT THIS CHANGES
-----------------
1. db/schema.py -- four additive nullable columns on `picks`:
       final_conf REAL, final_tier TEXT, final_odds REAL,
       pregame_locked_at TIMESTAMPTZ
   Same idempotent ADD COLUMN IF NOT EXISTS pattern as every column added since
   2026-08-11. Existing rows stay NULL, which correctly reads as "produced
   before snapshots existed".

2. db/picks_store.py --
   a. New `_game_started(p)` helper reading `game_time_utc` off game_data.
   b. save_picks writes the four columns on INSERT, and on CONFLICT uses
      COALESCE(picks.x, EXCLUDED.x) so the FIRST non-null value wins and can
      never be overwritten. That is the immutability guarantee, enforced by the
      database rather than by remembering to be careful.
   c. conf/tier/odds additionally freeze once pregame_locked_at is set, closing
      the lineups-never-confirmed hole.
   d. closing_odds STOPS moving at first pitch. It currently keeps updating on
      every later write. The comment above it says "re-scores stop at first
      pitch", and the Cubs card proves they do not, so CLV has been measured
      against a price that could be captured after the game started. This is a
      correctness fix, not a nicety.

FAIL SAFE BY DESIGN
-------------------
`_game_started` returns False on any missing or unparseable time, which
reproduces exactly today's behaviour. A snapshot that never fires is
recoverable later from the odds history. One that fires EARLY would freeze a
pre-lineup number into the permanent record, which is not recoverable. So every
ambiguous case is resolved toward doing nothing.

NOT IN THIS SCRIPT
------------------
Displaying the frozen number on the card once a game has started. That needs
run_picks_html.py (221 KB, splice only) and should wait until these columns are
confirmed filling correctly in production, which takes one slate. Phase 2.

SAFE TO RUN
-----------
  * Timestamped .bak for each file before it is touched.
  * Every anchor must match EXACTLY ONCE or it aborts having written nothing.
  * Both files resolved BEFORE either is written, so a bad second anchor cannot
    leave the first half-edited.
  * Idempotent: detects the marker and exits without changes on a second run.
  * py_compile on both, and both backups restored if either fails.

USAGE (from PowerShell, in the repo root)
-----------------------------------------
    cd C:\\Users\\Jskel\\GitHub\\sports-betting-pipeline
    python3 scripts\\fix_firstpitch_snapshot.py
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
STORE = os.path.join(REPO, "db", "picks_store.py")

MARKER = "FIRST PITCH SNAPSHOT"


# ── SPLICE 1: the four columns ───────────────────────────────────────────────

OLD_1 = """            for _col, _type in (("odds", "REAL"), ("odds_at", "TIMESTAMPTZ"),
                                ("closing_odds", "REAL"), ("closing_odds_at", "TIMESTAMPTZ")):
                cur.execute(f"ALTER TABLE picks ADD COLUMN IF NOT EXISTS {_col} {_type}")
"""

NEW_1 = OLD_1 + """            # FIRST PITCH SNAPSHOT (2026-09-16). tier_locked freezes conf/tier
            # at LINEUP confirmation, which leaves two holes: a game whose
            # lineups never confirm never freezes at all, and the board keeps
            # re-scoring after the freeze so the displayed number drifts away
            # from the graded one. These four are written exactly ONCE, the
            # first time save_picks runs at or after first pitch, and are never
            # updated again. Nullable and additive: existing rows stay NULL,
            # which correctly reads as "produced before snapshots existed".
            for _col, _type in (("final_conf", "REAL"), ("final_tier", "TEXT"),
                                ("final_odds", "REAL"),
                                ("pregame_locked_at", "TIMESTAMPTZ")):
                cur.execute(f"ALTER TABLE picks ADD COLUMN IF NOT EXISTS {_col} {_type}")
"""


# ── SPLICE 2: the helper ─────────────────────────────────────────────────────

OLD_2 = """import logging
from datetime import datetime
"""

NEW_2 = """import logging
from datetime import datetime, timezone
"""


OLD_3 = "def save_picks("

NEW_3 = '''def _game_started(p: dict) -> bool:
    """Has first pitch passed for this pick's game?

    Reads `game_time_utc` off game_data, which model/mlb_picks.py attaches to
    every pick as the whole scored-game dict.

    FAIL SAFE. Any missing, empty or unparseable time returns False, which
    reproduces today's behaviour exactly. The asymmetry is deliberate: a
    snapshot that never fires can be reconstructed later from the odds history,
    but one that fires EARLY freezes a pre-lineup number into a record that is
    supposed to be permanent, and that cannot be undone. Every ambiguous case
    resolves toward doing nothing.
    """
    g = p.get("game_data") or {}
    raw = str(g.get("game_time_utc") or "").strip()
    if not raw:
        return False
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) >= dt


def save_picks('''


# ── SPLICE 3: the write path ─────────────────────────────────────────────────

OLD_4 = """                _lineups_set = bool((p.get("game_data") or {}).get("lineup_confirmed"))
"""

NEW_4 = """                _lineups_set = bool((p.get("game_data") or {}).get("lineup_confirmed"))
                # FIRST PITCH SNAPSHOT. Computed per pick, once, here.
                _started   = _game_started(p)
                _snap_conf = round(float(p.get("conf", 0)), 4) if _started else None
                _snap_tier = p.get("tier", "") if _started else None
                _snap_odds = _pick_price(p) if _started else None
                _snap_at   = datetime.now(timezone.utc) if _started else None
"""


OLD_5 = """                         conf, tier, reasoning, market_signal, tier_locked,
                         was_best_bet, odds, odds_at, opp_odds, model_version, actual_result)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, 'PENDING')
"""

NEW_5 = """                         conf, tier, reasoning, market_signal, tier_locked,
                         was_best_bet, odds, odds_at, opp_odds, model_version,
                         final_conf, final_tier, final_odds, pregame_locked_at,
                         actual_result)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s,
                         %s, %s, %s, %s, 'PENDING')
"""


OLD_6 = """                        conf          = CASE WHEN picks.tier_locked THEN picks.conf ELSE EXCLUDED.conf END,
                        tier          = CASE WHEN picks.tier_locked THEN picks.tier ELSE EXCLUDED.tier END,
"""

NEW_6 = """                        -- FIRST PITCH SNAPSHOT (2026-09-16). Frozen once EITHER
                        -- the lineups locked the tier OR first pitch has passed.
                        -- The second arm closes the hole where a game whose
                        -- lineups never confirm never froze at all.
                        --
                        -- On the statement that FIRST sets pregame_locked_at,
                        -- picks.pregame_locked_at is still NULL (ON CONFLICT sees
                        -- the pre-update row), so conf updates one final time to
                        -- the first-pitch value and the snapshot stores that same
                        -- value. They agree by construction.
                        conf          = CASE WHEN picks.pregame_locked_at IS NOT NULL
                                               OR picks.tier_locked
                                             THEN picks.conf ELSE EXCLUDED.conf END,
                        tier          = CASE WHEN picks.pregame_locked_at IS NOT NULL
                                               OR picks.tier_locked
                                             THEN picks.tier ELSE EXCLUDED.tier END,
"""


OLD_7 = """                        closing_odds    = COALESCE(EXCLUDED.odds, picks.closing_odds),
                        closing_odds_at = CASE WHEN EXCLUDED.odds IS NOT NULL
                                               THEN NOW() ELSE picks.closing_odds_at END
"""

NEW_7 = """                        --
                        -- CLOSING PRICE STOPS AT FIRST PITCH (fixed 2026-09-16).
                        -- The note above used to assert "re-scores stop at first
                        -- pitch". They do not: the board re-scored a Cubs card
                        -- from 80.9% to 81.2% after the game was under way. So
                        -- closing_odds could absorb a price captured DURING the
                        -- game, and every CLV number is measured against it.
                        -- A close that moves after the close is not a close.
                        closing_odds    = CASE
                                            WHEN picks.pregame_locked_at IS NOT NULL
                                              THEN picks.closing_odds
                                            ELSE COALESCE(EXCLUDED.odds, picks.closing_odds)
                                          END,
                        closing_odds_at = CASE
                                            WHEN picks.pregame_locked_at IS NOT NULL
                                              THEN picks.closing_odds_at
                                            WHEN EXCLUDED.odds IS NOT NULL
                                              THEN NOW()
                                            ELSE picks.closing_odds_at
                                          END,
                        -- Write ONCE. COALESCE keeps the first non-null value, so
                        -- a later write physically cannot overwrite the snapshot.
                        -- The immutability is enforced by the database, not by
                        -- anyone remembering to be careful.
                        final_conf        = COALESCE(picks.final_conf,        EXCLUDED.final_conf),
                        final_tier        = COALESCE(picks.final_tier,        EXCLUDED.final_tier),
                        final_odds        = COALESCE(picks.final_odds,        EXCLUDED.final_odds),
                        pregame_locked_at = COALESCE(picks.pregame_locked_at, EXCLUDED.pregame_locked_at)
"""


OLD_8 = """                        _opp_price(p),
                        _model_version(),
                    )
"""

NEW_8 = """                        _opp_price(p),
                        _model_version(),
                        _snap_conf,
                        _snap_tier,
                        _snap_odds,
                        _snap_at,
                    )
"""


PLAN = [
    (SCHEMA, [("S1 columns", OLD_1, NEW_1)]),
    (STORE, [
        ("P1 timezone import", OLD_2, NEW_2),
        ("P2 helper",          OLD_3, NEW_3),
        ("P3 snapshot vars",   OLD_4, NEW_4),
        ("P4 insert columns",  OLD_5, NEW_5),
        ("P5 conflict freeze", OLD_6, NEW_6),
        ("P6 closing + snap",  OLD_7, NEW_7),
        ("P7 param tuple",     OLD_8, NEW_8),
    ]),
]


def main():
    for p in (SCHEMA, STORE):
        if not os.path.exists(p):
            sys.exit("ABORT: %s not found. Run this from the repo." % p)

    if MARKER in io.open(STORE, encoding="utf-8").read():
        print("Already applied (found the snapshot marker). Nothing to do.")
        return 0

    # Resolve EVERY splice in BOTH files before writing anything.
    resolved = {}
    for path, splices in PLAN:
        src = io.open(path, encoding="utf-8").read()
        before = len(src)
        for label, old, new in splices:
            n = src.count(old)
            if n != 1:
                sys.exit(
                    "ABORT: %s anchor matched %d times in %s, expected exactly 1.\n"
                    "NOTHING was written to any file."
                    % (label, n, os.path.basename(path))
                )
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
        print("spliced  %-16s %d -> %d bytes" % (os.path.basename(path), before, len(src)))

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
    schema = io.open(SCHEMA, encoding="utf-8").read()

    # The bug this project has shipped three times is a DictWriter-style
    # positional mismatch. Count the INSERT placeholders against the columns.
    ins = store.split("INSERT INTO picks", 1)[1].split("ON CONFLICT", 1)[0]
    cols_txt = ins.split("(", 1)[1].split(")", 1)[0]
    n_cols = len([c for c in cols_txt.replace("\n", " ").split(",") if c.strip()])
    vals_txt = ins.split("VALUES", 1)[1]
    n_vals = vals_txt.count("%s") + vals_txt.count("NOW()") + vals_txt.count("'PENDING'")

    checks = [
        ("snapshot marker present",     MARKER in store),
        ("4 columns in schema",         all(c in schema for c in
                                            ("final_conf", "final_tier", "final_odds",
                                             "pregame_locked_at"))),
        ("timezone imported",           "from datetime import datetime, timezone" in store),
        ("_game_started defined",       "def _game_started(p: dict) -> bool:" in store),
        ("_game_started called",        "_started   = _game_started(p)" in store),
        ("COALESCE immutability x4",    store.count("COALESCE(picks.final") == 3
                                        and "COALESCE(picks.pregame_locked_at" in store),
        ("closing_odds gated",          "WHEN picks.pregame_locked_at IS NOT NULL\n"
                                        "                                              THEN picks.closing_odds" in store),
        ("INSERT cols == values (%d/%d)" % (n_cols, n_vals), n_cols == n_vals),
        ("no truncation",               len(store) > resolved[STORE][1]
                                        and len(schema) > resolved[SCHEMA][1]),
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
    print("AFTER THE FIRST SLATE, verify it is actually writing:")
    print("  pregame_locked_at should be non-null for every started game,")
    print("  and final_conf should equal conf on rows where tier_locked was true.")
    print("  If pregame_locked_at is null everywhere, game_time_utc is not")
    print("  reaching game_data and _game_started is failing safe. That is the")
    print("  first thing to check, not the SQL.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

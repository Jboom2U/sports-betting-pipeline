"""
scrapers/odds_schema.py — ONE canonical schema + writer for mlb_odds_master.csv.

WHY THIS EXISTS (created 2026-08-14)
Two scrapers write to the same file: mlb_odds_scraper (The Odds API, needed for
the game total) and mlb_pinnacle_scraper (free, sharper, drives ML and RL). They
each declared their OWN SNAPSHOT_FIELDNAMES list, and the lists had drifted apart
by four columns after the per-handicap run line prices were added to Pinnacle's
copy on 2026-08-11.

That is not a cosmetic difference. csv.DictWriter writes values POSITIONALLY in
fieldname order. So the Odds API scraper appended 25 values under a 29 column
header, and every consumer reading by name got shifted data:

    header index 13 = rl_home_m15_price   <- received the Odds API's total_line
    header index 17 = total_line          <- received total_line_max

This is the third instance of the same failure in this repo:
  1. 2026-07-29  normalize/append_to_master, schedule master, shift-by-one on the
                 probable-pitcher-ID columns. home_team read a player id.
  2. 2026-08-11  save_snapshot appending under a stale header. A -207 run line
                 price rendered as "UNDER 207.0" with a 197 run edge.
  3. 2026-08-14  this one: two writers, two schemas, one file.

The lesson each time is the same, so it is encoded here rather than in a comment
someone has to remember: a file's schema must have exactly ONE owner, and the
writer must verify the on-disk header before appending.

RULES
- Add a column HERE and nowhere else. Both scrapers import this list.
- write_snapshot_rows() compares the on-disk header first, and reacts in one of
  three ways (revised 2026-08-17):
    header matches            -> plain append
    header is a SUBSET (grew) -> rewrite, MIGRATING existing rows by name with
                                 "" in the new columns
    header CONFLICTS          -> rewrite and drop, because values that cannot be
                                 trusted by name poison every downstream price
- The migrate case exists because the drop-everything rule cost real data: adding
  `books_json` wiped a day of snapshots, and since MLBModel freezes each game at
  first pitch, games already underway lost their pre-game price for good.
- NEVER let a column be inserted mid-list by a second writer. That is the case
  the drop branch is for, and it is why this file is the only schema owner.
"""
from __future__ import annotations

import csv
import logging
import os

log = logging.getLogger(__name__)

# THE canonical column list for data/clean/mlb_odds_master.csv.
SNAPSHOT_FIELDNAMES = [
    "snapshot_id", "snapshot_time", "game_id", "game_date", "game_time_utc",
    "away_team", "home_team",
    # Moneyline
    "ml_away", "ml_home",
    # Run line, legacy pairing (favorite -1.5 / dog +1.5). DISPLAY ONLY.
    "rl_away_line", "rl_away_price", "rl_home_line", "rl_home_price",
    # Run line, explicit per handicap. ANYTHING COMPUTING EV MUST USE THESE.
    # A price is only meaningful next to the handicap it was quoted for.
    "rl_home_m15_price", "rl_home_p15_price",
    "rl_away_m15_price", "rl_away_p15_price",
    # Totals
    "total_line", "total_over_price", "total_under_price",
    "total_line_min", "total_line_max",
    "books_used",
    # DraftKings specific (softest public book — best for value spotting)
    "dk_ml_away", "dk_ml_home", "dk_total",
    "disc_ml_away", "disc_ml_home", "disc_total",
    # Per book prices as compact JSON. One column rather than books x markets
    # columns, so adding a book never changes the schema. Pinnacle leaves this
    # empty; only the Odds API pull populates it.
    "books_json",
    # Which scraper wrote this row (added 2026-08-18). Both scrapers always KNEW
    # their own source and returned it in their result dict, but it was never
    # written down, so "which lines were pulled today, and when" could not be
    # answered from the stored data at all. APPENDED, never inserted, so the
    # additive path below migrates every existing row instead of dropping it.
    "source",
]

MOVEMENT_FIELDNAMES = [
    "game_id", "away_team", "home_team", "game_date",
    "snap1_time", "snap2_time",
    "ml_away_open", "ml_away_now", "ml_away_move",
    "ml_home_open", "ml_home_now", "ml_home_move",
    "total_open", "total_now", "total_move",
    "ml_signal", "total_signal",
    "sharp_side",
    "timestamp",
]


def write_snapshot_rows(master_path: str, rows: list, source: str = "odds") -> None:
    """Append snapshot rows, rewriting the file if the on-disk schema differs.

    Never appends blindly. See the module docstring for why.
    """
    if not rows:
        return

    # Stamp provenance in the one place every writer already passes through,
    # rather than in each scraper. Two callers exist today and both already pass
    # `source`, so a third one cannot ship having forgotten it.
    for _r in rows:
        if not _r.get("source"):
            _r["source"] = source

    existing_hdr, existing_rows = None, []
    if os.path.exists(master_path):
        try:
            with open(master_path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                existing_hdr = list(r.fieldnames or [])
                if existing_hdr == list(SNAPSHOT_FIELDNAMES):
                    existing_hdr = None          # schema matches, plain append
                else:
                    existing_rows = list(r)
        except Exception as e:
            log.warning(f"[{source}] could not read odds master, rewriting: {e}")
            existing_hdr, existing_rows = [], []

    if existing_hdr is None and os.path.exists(master_path):
        with open(master_path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDNAMES,
                           extrasaction="ignore").writerows(rows)
        log.info(f"[{source}] Saved {len(rows)} snapshot rows to "
                 f"{os.path.basename(master_path)}")
        return

    # MIGRATE vs DROP (refined 2026-08-17). The original rule dropped every
    # existing row on any schema change, on the grounds that rows written under
    # a different header are positionally misaligned and unrecoverable.
    #
    # That is true when a column is INSERTED mid-list, which is what happened on
    # 08-11. It is NOT true when a column is merely APPENDED: DictReader read
    # those rows using the file's own header, so every value is already correctly
    # keyed by name and can be re-emitted safely.
    #
    # Dropping them anyway cost real data. Adding `books_json` wiped the whole
    # day's snapshots, and because MLBModel freezes each game at first pitch, any
    # game already underway lost its pre-game price permanently — the pick simply
    # went priceless mid-afternoon. It also threw away a paid Odds API pull.
    #
    # So: if the old header is a SUBSET of the new one, the change is additive
    # and the rows are carried forward with "" in the new columns. Only a genuine
    # conflict still drops.
    old_cols = set(existing_hdr or [])
    additive = bool(old_cols) and old_cols.issubset(set(SNAPSHOT_FIELDNAMES))
    carried = existing_rows if additive else []
    dropped = 0 if additive else len(existing_rows)

    os.makedirs(os.path.dirname(master_path), exist_ok=True)
    with open(master_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDNAMES,
                           extrasaction="ignore")
        w.writeheader()
        for r in carried:
            w.writerow({k: r.get(k, "") for k in SNAPSHOT_FIELDNAMES})
        w.writerows(rows)

    if additive and carried:
        log.info(f"[{source}] odds master schema grew "
                 f"({len(existing_hdr)} -> {len(SNAPSHOT_FIELDNAMES)} cols, "
                 f"additive). MIGRATED {len(carried)} existing row(s); new "
                 f"columns are blank on them.")
    if dropped:
        log.warning(f"[{source}] odds master schema CONFLICTS "
                    f"({len(existing_hdr or [])} cols, not a subset of "
                    f"{len(SNAPSHOT_FIELDNAMES)}). Rewrote and DROPPED "
                    f"{dropped} row(s) whose values cannot be trusted by name.")
    log.info(f"[{source}] Saved {len(rows)} snapshot rows to "
             f"{os.path.basename(master_path)}")

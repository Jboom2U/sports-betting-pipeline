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
- write_snapshot_rows() compares the on-disk header first. If it does not match,
  the file is rewritten under the current schema and misaligned rows are DROPPED,
  never migrated. Their values are positionally wrong and there is no safe way to
  recover them. Snapshots re-pull for free from Pinnacle; a silently misaligned
  row poisons every downstream price.
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

    dropped = len(existing_rows)
    os.makedirs(os.path.dirname(master_path), exist_ok=True)
    with open(master_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SNAPSHOT_FIELDNAMES,
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    if dropped:
        log.warning(f"[{source}] odds master schema changed "
                    f"({len(existing_hdr or [])} -> {len(SNAPSHOT_FIELDNAMES)} "
                    f"cols). Rewrote file and DROPPED {dropped} misaligned "
                    f"row(s). Snapshots re-pull for free; re-run the odds pull.")
    log.info(f"[{source}] Saved {len(rows)} snapshot rows to "
             f"{os.path.basename(master_path)}")

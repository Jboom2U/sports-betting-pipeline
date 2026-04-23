"""
normalize/mlb_bullpen_normalize.py
Normalizes bullpen stats into data/clean/mlb_bullpen_master.csv.
Deduplicates on (team_id, season) — always takes the latest run.
"""

import os, csv, json, logging
from datetime import datetime

log = logging.getLogger(__name__)

SEASON    = datetime.now().year
RAW_PATH  = os.path.join(os.path.dirname(__file__), "..", "data", "raw",
                         f"mlb_bullpen_raw_{SEASON}.json")
CLEAN_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean")
OUT_PATH  = os.path.join(CLEAN_DIR, "mlb_bullpen_master.csv")

FIELDS = [
    "team_id", "team_name", "season",
    "bullpen_era", "bullpen_whip", "bullpen_k9",
    "bullpen_bb9", "bullpen_hr9",
    "bullpen_saves", "bullpen_blown", "bullpen_holds",
    "bullpen_save_pct", "bullpen_ip",
]


def run():
    os.makedirs(CLEAN_DIR, exist_ok=True)

    if not os.path.exists(RAW_PATH):
        log.warning(f"No bullpen raw file found at {RAW_PATH} — skipping normalize")
        return

    with open(RAW_PATH, encoding="utf-8") as f:
        new_rows = json.load(f)

    # Load existing master
    existing: dict[tuple, dict] = {}
    if os.path.exists(OUT_PATH):
        with open(OUT_PATH, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (row["team_id"], row["season"])
                existing[key] = row

    # Upsert — new data overwrites same (team_id, season)
    updated = 0
    for row in new_rows:
        key = (str(row["team_id"]), str(row["season"]))
        if key not in existing:
            updated += 1
        existing[key] = {k: row.get(k, "") for k in FIELDS}

    # Write master
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(existing.values())

    log.info(f"Bullpen master: {len(existing)} teams ({updated} upserted) → {OUT_PATH}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()

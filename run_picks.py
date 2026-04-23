"""
run_picks.py
Daily MLB betting picks entry point.

Usage:
    python run_picks.py                        # Today's picks
    python run_picks.py --date 2026-04-15      # Picks for a specific date
    python run_picks.py --save                 # Also save to picks/ folder

Output:
    Prints ranked individual picks + 2 and 3-leg parlay recommendations.
    Optionally saves to picks/mlb_picks_YYYY-MM-DD.txt and .csv
"""

import sys
import os
import logging
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

PICKS_DIR = os.path.join(os.path.dirname(__file__), "picks")


def main():
    parser = argparse.ArgumentParser(description="MLB Daily Betting Picks")
    parser.add_argument("--date", type=str, default=None,
                        help="Target date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--save", action="store_true",
                        help="Save picks to picks/ folder as .txt and .csv")
    args = parser.parse_args()

    target = args.date or datetime.now().strftime("%Y-%m-%d")

    # ── Load model ────────────────────────────────────────────────────────────
    from model.mlb_model import MLBModel
    from model.mlb_picks import (generate_picks, build_parlays,
                                  format_output, save_picks_csv)

    model = MLBModel()
    model.load()

    # ── Score games ───────────────────────────────────────────────────────────
    scored, actual_date = model.score_today(target)

    if not scored:
        print(f"\nNo upcoming games found on or after {target}.")
        print("Run python run_pipeline.py first to refresh the schedule.")
        sys.exit(0)

    # ── Generate picks ────────────────────────────────────────────────────────
    picks      = generate_picks(scored)
    parlays_2  = build_parlays(picks, legs=2, max_parlays=3)
    parlays_3  = build_parlays(picks, legs=3, max_parlays=3)

    # ── Print output ──────────────────────────────────────────────────────────
    output = format_output(picks, parlays_2, parlays_3, scored, actual_date)
    print("\n" + output)

    # ── Save (optional) ───────────────────────────────────────────────────────
    if args.save:
        os.makedirs(PICKS_DIR, exist_ok=True)

        txt_path = os.path.join(PICKS_DIR, f"mlb_picks_{actual_date}.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(output)
        log.info(f"Text output saved: {txt_path}")

        csv_path = save_picks_csv(picks, actual_date, PICKS_DIR)
        log.info(f"CSV saved: {csv_path}")

    log.info(
        f"Done | {len(scored)} games scored | "
        f"{sum(1 for p in picks if p['tier'] != 'PASS')} picks generated"
    )


if __name__ == "__main__":
    main()

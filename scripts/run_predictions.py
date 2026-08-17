"""Manual runner for the predictive engine.

Usage:
    python scripts/run_predictions.py --league MLB --date 2026-08-08
    python scripts/run_predictions.py --league NFL --date 2026-08-08
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run predictive engine manually")
    parser.add_argument("--league", required=True, choices=["MLB", "NFL"], help="League")
    parser.add_argument("--date", required=True, help="Game date (YYYY-MM-DD)")
    parser.add_argument("--player", action="append", help="Player name (repeatable)")
    parser.add_argument("--team", action="append", help="Team (repeatable)")
    parser.add_argument("--prop", action="append", help="Prop type (repeatable)")
    parser.add_argument("--line", action="append", type=float, help="Prop line (repeatable)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    game_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    from predictive.engine import run_predictions
    from predictive.props import PlayerProp

    if not args.player or not args.team or not args.prop or not args.line:
        logger.error(
            "Must provide --player, --team, --prop, and --line (each can be repeated to form pairs)"
        )
        sys.exit(1)

    if not (len(args.player) == len(args.team) == len(args.prop) == len(args.line)):
        logger.error("--player, --team, --prop, and --line must have the same count")
        sys.exit(1)

    props = [
        PlayerProp(
            player_name=p,
            team=t,
            prop_type=pr,
            line=l,
            league=args.league,
            game_date=game_date,
        )
        for p, t, pr, l in zip(args.player, args.team, args.prop, args.line)
    ]

    logger.info(
        "Running %s predictions for %s (%d props)",
        args.league,
        game_date,
        len(props),
    )

    from app.config import create_app

    app = create_app()
    with app.app_context():
        results = run_predictions(args.league, game_date, props)

        for r in results:
            flag = " [HIGH VALUE]" if r.is_high_value else ""
            print(
                f"{r.prop.player_name} ({r.prop.team}) | {r.prop.prop_type} "
                f"| Line: {r.prop.line} | Pred: {r.predicted_value:.2f} "
                f"| Over: {r.probability_over:.1%} | Under: {r.probability_under:.1%} "
                f"| Edge: {r.edge:.2f} | {r.recommendation}{flag}"
            )
            if r.reasoning:
                print(f"  -> {r.reasoning}")

        logger.info("Completed %d predictions", len(results))


if __name__ == "__main__":
    main()

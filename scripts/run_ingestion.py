"""Manual runner for MLB and NFL ingestion jobs."""

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
    parser = argparse.ArgumentParser(description="Run ingestion manually")
    parser.add_argument("--league", required=True, choices=["MLB", "NFL"], help="League")
    parser.add_argument("--date", required=True, help="Game date (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="Enable backfill mode")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    game_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    from app.config import create_app

    app = create_app()
    with app.app_context():
        if args.league == "MLB":
            from ingestion.mlb import run_ingestion as run_mlb
            result = run_mlb(game_date)
        else:
            from ingestion.nfl import run_ingestion as run_nfl
            result = run_nfl(game_date)

        logger.info("Ingestion result: %s", result)


if __name__ == "__main__":
    main()

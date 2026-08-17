"""Deploy marketing assets to Cloudflare R2.

Usage:
    python scripts/deploy_marketing.py --league MLB --date 2026-08-08
    python scripts/deploy_marketing.py --league NFL --date 2026-08-08
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
    parser = argparse.ArgumentParser(description="Deploy marketing files to Cloudflare R2")
    parser.add_argument("--league", required=True, choices=["MLB", "NFL"], help="League")
    parser.add_argument("--date", required=True, help="Game date (YYYY-MM-DD)")
    parser.add_argument("--skip-upload", action="store_true", help="Generate files locally but skip R2 upload")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    game_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    from app.config import create_app

    app = create_app()
    with app.app_context():
        from predictive.engine import get_high_value_predictions
        from marketing.generator import write_marketing_files
        from storage.r2_client import upload_directory

        predictions = get_high_value_predictions(league=args.league, game_date=game_date, limit=500)
        if not predictions:
            logger.warning("No predictions found for %s on %s", args.league, game_date)
            sys.exit(0)

        logger.info("Generating marketing files for %d predictions", len(predictions))

        files = write_marketing_files(args.league, game_date, predictions)
        logger.info("Generated: %s", files)

        if args.skip_upload:
            logger.info("Skipping R2 upload (--skip-upload)")
            return

        storage_cfg = {}
        try:
            import yaml
            config_path = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
            if config_path.exists():
                with open(config_path, "r", encoding="utf-8") as f:
                    storage_cfg = yaml.safe_load(f) or {}
        except Exception as exc:
            logger.warning("Failed to load config for R2: %s", exc)

        r2_config = storage_cfg.get("storage", {})
        bucket = r2_config.get("r2_bucket")
        if not bucket:
            logger.error("R2 bucket not configured in config.yaml")
            sys.exit(1)

        output_dir = files["html"].parent
        prefix = f"marketing/{args.league.lower()}/{game_date.strftime('%Y-%m-%d')}"

        try:
            results = upload_directory(output_dir, bucket, prefix)
            logger.info("Uploaded %d files to R2 bucket %s", len(results), bucket)
            for r in results:
                logger.info("  %s -> %s", r["local"], r["url"])
        except Exception as exc:
            logger.exception("R2 upload failed: %s", exc)
            sys.exit(1)


if __name__ == "__main__":
    main()

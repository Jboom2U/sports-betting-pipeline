"""Test runner for social video generation."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    from social.video_generator import generate_test_short

    output = str(Path(__file__).resolve().parents[1] / "data" / "processed" / "shorts" / "test_short.mp4")
    logger.info("Generating test short video asset...")
    path = generate_test_short(output_path=output)
    logger.info("SUCCESS: Video asset generated at %s", path)


if __name__ == "__main__":
    main()

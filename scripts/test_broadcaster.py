"""Dry-run test of social/broadcaster.py chaining.

Generates a test video asset, then attempts to broadcast across
X, TikTok, and YouTube. Since no real credentials are loaded,
each platform will report its configuration status.
"""

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
    from dotenv import load_dotenv
    load_dotenv()

    from social.video_generator import generate_test_short
    from social.broadcaster import SocialBroadcaster

    video_path = str(Path(__file__).resolve().parents[1] / "data" / "processed" / "shorts" / "test_short.mp4")

    logger.info("Step 1: Generating test video asset...")
    try:
        generated_path = generate_test_short(output_path=video_path)
        logger.info("Video asset ready: %s", generated_path)
    except Exception as exc:
        logger.error("Video generation failed: %s", exc)
        sys.exit(1)

    logger.info("Step 2: Initializing SocialBroadcaster...")
    broadcaster = SocialBroadcaster()

    platforms = ["x", "tiktok", "youtube"]
    logger.info("Step 3: Broadcasting dry-run to %s...", ", ".join(platforms))

    results = broadcaster.broadcast(
        text="Daily MLB/NFL prop breakdown: Casey Mize UNDER 5.5 Ks flagged as HIGH VALUE",
        video_path=generated_path,
        media_paths=None,
        platforms=platforms,
        youtube_title="Sports Pipeline Daily Prop Breakdown",
        youtube_description="AI-powered sports prop predictions for MLB and NFL.",
        tiktok_title="Daily Prop Breakdown",
        tiktok_description="Check out today's high-value sports prop predictions.",
    )

    print("\n" + "=" * 60)
    print("BROADCAST DRY-RUN RESULTS")
    print("=" * 60)

    for platform, result in results.items():
        status = "SUCCESS" if result.get("success") else "FAILED / NOT CONFIGURED"
        post_id = result.get("post_id", "N/A")
        url = result.get("url", "N/A")
        error = result.get("error", "")
        print(f"\nPlatform : {platform.upper()}")
        print(f"Status   : {status}")
        print(f"Post ID  : {post_id}")
        print(f"URL      : {url}")
        if error:
            print(f"Error    : {error}")

    print("\n" + "=" * 60)
    configured = sum(1 for r in results.values() if r.get("success"))
    logger.info("Dry-run complete: %d/%d platforms configured", configured, len(results))


if __name__ == "__main__":
    main()

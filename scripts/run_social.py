"""Manual runner for social media broadcasting."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Broadcast to social media platforms")
    parser.add_argument("--text", required=True, help="Text content to post")
    parser.add_argument("--video", help="Local path to video file (for TikTok/YouTube)")
    parser.add_argument("--media", nargs="*", help="Local paths to media files (for X)")
    parser.add_argument("--platforms", nargs="+", choices=["x", "tiktok", "youtube"], help="Platforms to post to")
    parser.add_argument("--youtube-title", help="YouTube-specific title")
    parser.add_argument("--youtube-description", help="YouTube-specific description")
    parser.add_argument("--tiktok-title", help="TikTok-specific title")
    parser.add_argument("--tiktok-description", help="TikTok-specific description")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    from social.broadcaster import SocialBroadcaster

    broadcaster = SocialBroadcaster()
    results = broadcaster.broadcast(
        text=args.text,
        video_path=args.video,
        media_paths=args.media,
        platforms=args.platforms,
        youtube_title=args.youtube_title or "",
        youtube_description=args.youtube_description or "",
        tiktok_title=args.tiktok_title or "",
        tiktok_description=args.tiktok_description or "",
    )

    for platform, result in results.items():
        status = "SUCCESS" if result.get("success") else "FAILED"
        post_id = result.get("post_id", "N/A")
        url = result.get("url", "N/A")
        error = result.get("error", "")
        logger.info("%s (%s): post_id=%s url=%s error=%s", platform.upper(), status, post_id, url, error)


if __name__ == "__main__":
    main()

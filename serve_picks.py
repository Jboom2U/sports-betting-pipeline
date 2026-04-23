"""
serve_picks.py
Local server for Sports Betting Parlay Genius.

Serves today's picks dashboard at http://localhost:8765
and exposes a /refresh endpoint the page's Refresh button calls
to re-run the pipeline and regenerate picks on demand.

Usage:
    python serve_picks.py          # starts server + opens browser
    python serve_picks.py --port 9000
"""

import sys, os, subprocess, threading, webbrowser, argparse, logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from pathlib import Path

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
PICKS_DIR = os.path.join(BASE_DIR, "picks")
DEFAULT_PORT = 8765

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

_refresh_lock = threading.Lock()
_refreshing   = False


def latest_picks_file():
    """Return path to the most recent HTML picks file."""
    picks = sorted(Path(PICKS_DIR).glob("mlb_picks_*.html"), reverse=True)
    return str(picks[0]) if picks else None


def run_refresh():
    """Run pipeline + HTML generator, return (success, message)."""
    global _refreshing
    with _refresh_lock:
        if _refreshing:
            return False, "Refresh already in progress"
        _refreshing = True

    try:
        log.info("Refresh triggered — running pipeline...")
        r1 = subprocess.run(
            [sys.executable, "run_pipeline.py"],
            cwd=BASE_DIR, capture_output=True, text=True, timeout=120
        )
        if r1.returncode != 0:
            log.error(f"Pipeline failed:\n{r1.stderr}")
            return False, f"Pipeline error: {r1.stderr[-300:]}"

        log.info("Pipeline done — generating picks...")
        r2 = subprocess.run(
            [sys.executable, "run_picks_html.py", "--no-open"],
            cwd=BASE_DIR, capture_output=True, text=True, timeout=60
        )
        if r2.returncode != 0:
            log.error(f"Picks gen failed:\n{r2.stderr}")
            return False, f"Picks error: {r2.stderr[-300:]}"

        log.info("Refresh complete.")
        return True, "OK"
    except subprocess.TimeoutExpired:
        return False, "Timeout — pipeline took too long"
    except Exception as e:
        return False, str(e)
    finally:
        _refreshing = False


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass   # silence default request logs

    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_cors()
        self.end_headers()

    def do_GET(self):
        # ── /refresh  (trigger pipeline) ────────────────────────────────────
        if self.path == "/refresh":
            success, msg = run_refresh()
            code = 200 if success else 500
            body = msg.encode()
            self.send_response(code)
            self.send_cors()
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # ── /status  (is a refresh running?) ────────────────────────────────
        if self.path == "/status":
            body = b"busy" if _refreshing else b"idle"
            self.send_response(200)
            self.send_cors()
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # ── /  (serve latest picks HTML) ────────────────────────────────────
        picks_file = latest_picks_file()
        if not picks_file:
            body = b"No picks file found. Run python run_picks_html.py first."
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        with open(picks_file, "rb") as f:
            body = f.read()

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args()

    url = f"http://localhost:{args.port}"
    server = HTTPServer(("localhost", args.port), Handler)
    log.info(f"Serving picks at {url}")
    log.info("Refresh button in the dashboard will re-run the pipeline.")
    log.info("Press Ctrl+C to stop.")

    if not args.no_open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Server stopped.")


if __name__ == "__main__":
    main()

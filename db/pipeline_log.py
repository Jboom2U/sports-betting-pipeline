"""
db/pipeline_log.py
Tracks daily pipeline runs in PostgreSQL, replacing pipeline_run_date.txt.

When DATABASE_URL is set, all read/write operations hit the DB.
When it's not set (or DB is down), falls back to the file-based marker
so local development and Railway deploys without DB still work.

Public API:
    pipeline_ran_today()     -> bool
    mark_pipeline_started()  -> None
    mark_pipeline_complete() -> None
    mark_pipeline_failed(notes) -> None
    get_last_run_date()      -> str | None   ("YYYY-MM-DD" or None)
"""

import os
import logging
from datetime import datetime, date
from zoneinfo import ZoneInfo

from db.connection import db_conn, db_available

log = logging.getLogger(__name__)

ET        = ZoneInfo("America/New_York")
BASE_DIR  = os.path.dirname(os.path.dirname(__file__))
MARKER    = os.path.join(BASE_DIR, "data", "pipeline_run_date.txt")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def _file_ran_today() -> bool:
    """Legacy file-based check."""
    if not os.path.exists(MARKER):
        return False
    with open(MARKER) as f:
        return f.read().strip() == _today_et()


def _file_mark_ran():
    """Legacy file-based write."""
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
    with open(MARKER, "w") as f:
        f.write(_today_et())


# ── Public API ────────────────────────────────────────────────────────────────

def pipeline_ran_today() -> bool:
    """
    Returns True if the pipeline completed successfully today (ET).
    Checks DB first; falls back to the txt marker file.
    """
    today = _today_et()

    with db_conn() as conn:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT 1 FROM pipeline_runs
                    WHERE run_date = %s AND status = 'complete'
                    LIMIT 1
                    """,
                    (today,)
                )
                if cur.fetchone():
                    return True
                # Fall through to file check even if DB says no
                # (avoids double-run if pipeline partially wrote to file but not DB)
            except Exception as e:
                log.debug(f"pipeline_ran_today DB check failed: {e}")

    # File-based fallback
    return _file_ran_today()


def mark_pipeline_started():
    """
    Record that the pipeline started today. Upserts the row with status='running'.
    Also writes the legacy txt marker so the file check never blocks a run.
    """
    today = _today_et()

    with db_conn() as conn:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO pipeline_runs (run_date, started_at, status)
                    VALUES (%s, NOW(), 'running')
                    ON CONFLICT (run_date) DO UPDATE
                        SET started_at = NOW(), status = 'running'
                    """,
                    (today,)
                )
            except Exception as e:
                log.debug(f"mark_pipeline_started DB write failed: {e}")


def mark_pipeline_complete():
    """
    Mark today's pipeline as successfully completed. Also writes the legacy txt marker.
    """
    today = _today_et()

    with db_conn() as conn:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO pipeline_runs (run_date, started_at, completed_at, status)
                    VALUES (%s, NOW(), NOW(), 'complete')
                    ON CONFLICT (run_date) DO UPDATE
                        SET completed_at = NOW(), status = 'complete'
                    """,
                    (today,)
                )
                log.info(f"Pipeline run logged to DB for {today}.")
            except Exception as e:
                log.debug(f"mark_pipeline_complete DB write failed: {e}")

    # Always write the file marker too — works even without DB
    _file_mark_ran()


def mark_pipeline_failed(notes: str = ""):
    """Mark today's pipeline as failed (non-fatal — just logs to DB if available)."""
    today = _today_et()

    with db_conn() as conn:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO pipeline_runs (run_date, started_at, completed_at, status, notes)
                    VALUES (%s, NOW(), NOW(), 'failed', %s)
                    ON CONFLICT (run_date) DO UPDATE
                        SET completed_at = NOW(), status = 'failed', notes = %s
                    """,
                    (today, notes, notes)
                )
            except Exception as e:
                log.debug(f"mark_pipeline_failed DB write failed: {e}")


def get_last_run_date() -> str | None:
    """Return the most recent successful pipeline run date as 'YYYY-MM-DD', or None."""
    with db_conn() as conn:
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT run_date FROM pipeline_runs
                    WHERE status = 'complete'
                    ORDER BY run_date DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    d = row[0]
                    return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
            except Exception as e:
                log.debug(f"get_last_run_date DB query failed: {e}")

    # File fallback
    if os.path.exists(MARKER):
        with open(MARKER) as f:
            return f.read().strip() or None
    return None

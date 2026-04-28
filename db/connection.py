"""
db/connection.py
PostgreSQL connection pool for Statalizers.

Uses DATABASE_URL env var (set automatically by Railway when you add a
Postgres plugin). Falls back gracefully if the var is absent or the DB
is unreachable — callers check `get_conn()` for None.

Usage:
    from db.connection import get_conn, release_conn

    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            ...
        finally:
            release_conn(conn)
"""

import os
import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)

_pool = None


def _build_pool():
    """Build a simple psycopg2 connection pool. Called once on first use."""
    global _pool
    if _pool is not None:
        return _pool

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        log.debug("DATABASE_URL not set — DB persistence disabled.")
        return None

    try:
        import psycopg2
        from psycopg2 import pool as pg_pool

        # Railway Postgres URLs start with postgres:// but psycopg2 wants postgresql://
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]

        _pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=url,
            connect_timeout=10,
        )
        log.info("PostgreSQL connection pool established.")
        return _pool

    except ImportError:
        log.warning("psycopg2 not installed — DB persistence disabled. Run: pip install psycopg2-binary")
        return None
    except Exception as e:
        log.warning(f"PostgreSQL connection failed (non-fatal): {e}")
        return None


def get_conn():
    """
    Get a connection from the pool.
    Returns None if the pool is unavailable (no DATABASE_URL, no psycopg2, etc.).
    Caller must call release_conn(conn) when done.
    """
    pool = _build_pool()
    if pool is None:
        return None
    try:
        return pool.getconn()
    except Exception as e:
        log.warning(f"Could not get DB connection: {e}")
        return None


def release_conn(conn):
    """Return a connection to the pool."""
    pool = _build_pool()
    if pool and conn:
        try:
            pool.putconn(conn)
        except Exception:
            pass


@contextmanager
def db_conn():
    """
    Context manager that yields a connection and handles commit/rollback.

    Usage:
        with db_conn() as conn:
            if conn:
                cur = conn.cursor()
                cur.execute(...)
                # auto-commits on exit, rolls back on exception
    """
    conn = get_conn()
    try:
        yield conn
        if conn:
            conn.commit()
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        log.warning(f"DB operation failed (non-fatal): {e}")
    finally:
        if conn:
            release_conn(conn)


def db_available() -> bool:
    """Quick check — returns True if a DB connection can be obtained."""
    conn = get_conn()
    if conn:
        release_conn(conn)
        return True
    return False

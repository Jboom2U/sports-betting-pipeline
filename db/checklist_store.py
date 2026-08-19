"""
db/checklist_store.py: persistence for the two parts of the checklist that
cannot live in code.

EVERY FUNCTION HERE DEGRADES TO EMPTY WHEN THE DATABASE IS UNREACHABLE.
The admin page must render during a Railway outage, which is exactly when it is
most likely to be opened. A checklist that takes the admin hub down with it is
worse than no checklist.

TWO KINDS OF STATE, AND THE DISTINCTION MATTERS:

  checklist_state  Status for items model/checklist.py cannot probe. These
                   render as ASSERTED, never as measured. A probed item ignores
                   this table completely, so no row here can turn a red item
                   green while the code still says otherwise.

  checklist_notes  Items Justin types on the admin page. These survive the
                   session, which is the entire point: Cowork keeps roughly the
                   last 50 sessions and older ones age out with no recovery, so
                   an idea raised in chat and not written down is gone.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

try:
    from db.connection import db_conn, db_available
except Exception:                                  # import must never break the page
    db_conn = None

    def db_available() -> bool:
        return False


def _ok() -> bool:
    try:
        return db_conn is not None and db_available()
    except Exception:
        return False


# ------------------------------------------------------------- asserted state

def get_state() -> dict:
    """{item_id: {"status","note","updated_at"}} for non probeable items."""
    if not _ok():
        return {}
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT item_id, status, note, updated_at FROM checklist_state")
            return {r[0]: {"status": r[1], "note": r[2], "updated_at": r[3]}
                    for r in cur.fetchall()}
    except Exception as exc:
        log.warning("checklist get_state failed: %s", exc)
        return {}


def set_state(item_id: str, status: str, note: str = "") -> bool:
    if not _ok() or not item_id:
        return False
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO checklist_state (item_id, status, note, updated_at) "
                "VALUES (%s, %s, %s, NOW()) "
                "ON CONFLICT (item_id) DO UPDATE SET "
                "  status = EXCLUDED.status, note = EXCLUDED.note, updated_at = NOW()",
                (item_id, status, note or ""))
            conn.commit()
        return True
    except Exception as exc:
        log.warning("checklist set_state failed: %s", exc)
        return False


# --------------------------------------------------------------- user's items

def add_note(title: str, detail: str = "") -> bool:
    title = (title or "").strip()
    if not title:
        return False
    if not _ok():
        log.warning("checklist add_note dropped, no database: %r", title[:80])
        return False
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO checklist_notes (title, detail) VALUES (%s, %s)",
                (title[:500], (detail or "")[:4000]))
            conn.commit()
        return True
    except Exception as exc:
        log.warning("checklist add_note failed: %s", exc)
        return False


def get_notes(include_done: bool = True) -> list:
    if not _ok():
        return []
    try:
        sql = ("SELECT id, title, detail, created_at, status, response, responded_at "
               "FROM checklist_notes ")
        if not include_done:
            sql += "WHERE status <> 'done' "
        sql += "ORDER BY (status = 'new') DESC, created_at DESC"
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return [{"id": r[0], "title": r[1], "detail": r[2], "created_at": r[3],
                     "status": r[4], "response": r[5], "responded_at": r[6]}
                    for r in cur.fetchall()]
    except Exception as exc:
        log.warning("checklist get_notes failed: %s", exc)
        return []


def respond_to_note(note_id: int, status: str, response: str) -> bool:
    """Record a review of one of Justin's items.

    status is 'accepted' (promoted into model/checklist.py ITEMS), 'declined'
    (response must say why) or 'done'.
    """
    if not _ok():
        return False
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE checklist_notes SET status=%s, response=%s, responded_at=NOW() "
                "WHERE id=%s", (status, response or "", int(note_id)))
            conn.commit()
        return True
    except Exception as exc:
        log.warning("checklist respond_to_note failed: %s", exc)
        return False


def delete_note(note_id: int) -> bool:
    if not _ok():
        return False
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM checklist_notes WHERE id=%s", (int(note_id),))
            conn.commit()
        return True
    except Exception as exc:
        log.warning("checklist delete_note failed: %s", exc)
        return False


def new_count() -> int:
    """How many of Justin's items are waiting on a review. Cheap, for the header."""
    if not _ok():
        return 0
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM checklist_notes WHERE status='new'")
            return int(cur.fetchone()[0])
    except Exception:
        return 0

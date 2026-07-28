"""
player_data.py — Player search + per-game history for the Players section.

Backs two views:
  - /players : searchable directory (by name / team) of everyone we have logs for
  - /player/<id> : per-game trend charts (hits, TB, HR, RBI, K, SB) L5/L10/L20

Reads from player_game_logs (per-game box-score stats) and player_prop_history
(prop line vs actual). All queries read-only.
"""
import logging

log = logging.getLogger(__name__)

# Stat columns we chart, in display order: (col, label)
STAT_COLS = [
    ("h",   "Hits"),
    ("tb",  "Total Bases"),
    ("hr",  "Home Runs"),
    ("rbi", "RBIs"),
    ("k",   "Strikeouts"),
    ("sb",  "Stolen Bases"),
]


def search_players(query: str = "", team: str = "", limit: int = 60) -> list:
    """Distinct players from the logs, most-recently-seen first, name/team filtered."""
    from db.connection import db_conn
    out = []
    try:
        with db_conn() as conn:
            if conn is None:
                return out
            cur = conn.cursor()
            where, params = [], []
            if query:
                where.append("player_name ILIKE %s")
                params.append(f"%{query.strip()}%")
            if team:
                where.append("team ILIKE %s")
                params.append(f"%{team.strip()}%")
            wsql = ("WHERE " + " AND ".join(where)) if where else ""
            cur.execute(f"""
                SELECT player_id, player_name, MAX(team) AS team,
                       MAX(game_date) AS last_seen, COUNT(*) AS games
                FROM player_game_logs
                {wsql}
                GROUP BY player_id, player_name
                ORDER BY last_seen DESC, games DESC
                LIMIT %s
            """, (*params, limit))
            cols = [d[0] for d in cur.description]
            out = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as e:
        log.warning(f"search_players failed: {e}")
    return out


def get_player(player_id: int) -> dict:
    """Player header + last-20 game logs (most recent first)."""
    from db.connection import db_conn
    info = {"player_id": player_id, "player_name": "", "team": "", "games": []}
    try:
        with db_conn() as conn:
            if conn is None:
                return info
            cur = conn.cursor()
            cur.execute("""
                SELECT game_date, opponent, venue, pitcher_hand,
                       ab, h, hr, rbi, bb, k, tb, sb, team, player_name
                FROM player_game_logs
                WHERE player_id = %s
                ORDER BY game_date DESC
                LIMIT 20
            """, (player_id,))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if rows:
                info["player_name"] = rows[0].get("player_name", "")
                info["team"]        = rows[0].get("team", "")
            # oldest-first for left-to-right charting
            info["games"] = list(reversed(rows))
    except Exception as e:
        log.warning(f"get_player failed: {e}")
    return info

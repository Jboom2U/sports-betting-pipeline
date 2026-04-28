"""
db/picks_store.py
Saves picks and scored game snapshots to PostgreSQL.

This is the foundation for backtesting — every pick is stored at generation
time with actual_result = 'PENDING'. The grading step (Priority 2) will fill
in actual_result, away_final, home_final after games complete.

Public API:
    save_picks(picks, date)              -> int (rows inserted)
    save_scored_games(scored_games, date) -> int (rows inserted)
    get_picks(date)                      -> list[dict]
    get_pending_picks()                  -> list[dict]  (ungraded picks)
    grade_pick(pick_id, result, away_score, home_score) -> bool
"""

import logging
from datetime import datetime

from db.connection import db_conn

log = logging.getLogger(__name__)


# ── Write ─────────────────────────────────────────────────────────────────────

def save_picks(picks: list, pick_date: str) -> int:
    """
    Upsert all picks for a given date into the picks table.
    Uses ON CONFLICT DO NOTHING so re-runs don't duplicate rows.
    Returns number of rows inserted.
    """
    if not picks:
        return 0

    inserted = 0
    with db_conn() as conn:
        if conn is None:
            log.debug("No DB — picks not saved to DB.")
            return 0
        try:
            cur = conn.cursor()
            for p in picks:
                cur.execute(
                    """
                    INSERT INTO picks
                        (pick_date, game_id, game, pick_type, label, team,
                         conf, tier, reasoning, actual_result)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    ON CONFLICT (pick_date, game_id, pick_type) DO NOTHING
                    """,
                    (
                        pick_date,
                        str(p.get("game_id", "")),
                        p.get("game", ""),
                        p.get("type", ""),
                        p.get("label", ""),
                        p.get("team", ""),
                        round(float(p.get("conf", 0)), 4),
                        p.get("tier", ""),
                        p.get("reasoning", ""),
                    )
                )
                inserted += cur.rowcount
            log.info(f"Picks saved to DB: {inserted} new rows for {pick_date}.")
        except Exception as e:
            log.warning(f"save_picks DB write failed (non-fatal): {e}")
            return 0

    return inserted


def save_scored_games(scored_games: list, score_date: str) -> int:
    """
    Upsert model scoring output for all games on score_date.
    Returns number of rows inserted.
    """
    if not scored_games:
        return 0

    inserted = 0
    with db_conn() as conn:
        if conn is None:
            return 0
        try:
            cur = conn.cursor()
            for g in scored_games:
                cur.execute(
                    """
                    INSERT INTO scored_games (
                        score_date, game_id, away_team, home_team,
                        exp_away, exp_home, exp_total, home_wp, away_wp,
                        ml_team, ml_conf, total_pick, total_line, total_conf,
                        rl_team, rl_pick, rl_conf,
                        ml_signal, total_signal, sharp_side,
                        ml_adj, total_adj, gap_adj, conv_adj,
                        park_runs, weather_flag,
                        away_sp, home_sp, away_sp_era, home_sp_era
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (score_date, game_id) DO NOTHING
                    """,
                    (
                        score_date,
                        str(g.get("game_id", "")),
                        g.get("away_team", ""),
                        g.get("home_team", ""),
                        g.get("exp_away"),
                        g.get("exp_home"),
                        g.get("exp_total"),
                        g.get("home_wp"),
                        g.get("away_wp"),
                        g.get("ml_team"),
                        g.get("ml_conf"),
                        g.get("total_pick"),
                        g.get("total_line"),
                        g.get("total_conf"),
                        g.get("rl_team"),
                        g.get("rl_pick"),
                        g.get("rl_conf"),
                        g.get("ml_signal"),
                        g.get("total_signal"),
                        g.get("sharp_side"),
                        g.get("ml_adj"),
                        g.get("total_adj"),
                        g.get("gap_adj"),
                        g.get("conv_adj"),
                        g.get("park_runs"),
                        g.get("weather_flag"),
                        g.get("away_sp"),
                        g.get("home_sp"),
                        g.get("away_sp_era_adj"),
                        g.get("home_sp_era_adj"),
                    )
                )
                inserted += cur.rowcount
            log.info(f"Scored games saved to DB: {inserted} new rows for {score_date}.")
        except Exception as e:
            log.warning(f"save_scored_games DB write failed (non-fatal): {e}")
            return 0

    return inserted


# ── Read ──────────────────────────────────────────────────────────────────────

def get_picks(pick_date: str) -> list:
    """Return all picks for a given date (YYYY-MM-DD)."""
    rows = []
    with db_conn() as conn:
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, pick_date, game_id, game, pick_type, label, team,
                       conf, tier, reasoning, actual_result, graded_at,
                       away_final, home_final, created_at
                FROM picks
                WHERE pick_date = %s
                ORDER BY conf DESC
                """,
                (pick_date,)
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                rows.append(dict(zip(cols, row)))
        except Exception as e:
            log.warning(f"get_picks failed: {e}")
    return rows


def get_pending_picks(max_age_days: int = 7) -> list:
    """
    Return picks with actual_result = 'PENDING' from the last max_age_days days.
    Used by the grading step to know what needs to be scored against results.
    """
    rows = []
    with db_conn() as conn:
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, pick_date, game_id, game, pick_type, label,
                       team, conf, tier
                FROM picks
                WHERE actual_result = 'PENDING'
                  AND pick_date >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY pick_date DESC
                """,
                (max_age_days,)
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                rows.append(dict(zip(cols, row)))
        except Exception as e:
            log.warning(f"get_pending_picks failed: {e}")
    return rows


def grade_pick(pick_id: int, result: str,
               away_score: int = None, home_score: int = None) -> bool:
    """
    Set the actual_result on a pick after game results are in.
    result: 'WIN' | 'LOSS' | 'PUSH'
    Returns True if the row was updated.
    """
    with db_conn() as conn:
        if conn is None:
            return False
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE picks
                SET actual_result = %s,
                    graded_at     = NOW(),
                    away_final    = %s,
                    home_final    = %s
                WHERE id = %s
                """,
                (result.upper(), away_score, home_score, pick_id)
            )
            return cur.rowcount > 0
        except Exception as e:
            log.warning(f"grade_pick failed for id={pick_id}: {e}")
            return False


def get_accuracy_summary(days: int = 30) -> dict:
    """
    Returns a summary dict of model accuracy over the last N days.
    Used by the backtesting dashboard (Priority 2).
    """
    with db_conn() as conn:
        if conn is None:
            return {}
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    pick_type,
                    tier,
                    COUNT(*) FILTER (WHERE actual_result = 'WIN')  AS wins,
                    COUNT(*) FILTER (WHERE actual_result = 'LOSS') AS losses,
                    COUNT(*) FILTER (WHERE actual_result = 'PUSH') AS pushes,
                    COUNT(*) FILTER (WHERE actual_result = 'PENDING') AS pending,
                    ROUND(AVG(conf)::numeric, 3) AS avg_conf
                FROM picks
                WHERE pick_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND actual_result != 'PENDING'
                GROUP BY pick_type, tier
                ORDER BY pick_type, tier
                """,
                (days,)
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            log.warning(f"get_accuracy_summary failed: {e}")
            return {}

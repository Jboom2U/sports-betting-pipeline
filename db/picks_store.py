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

    ON CONFLICT DO UPDATE keeps the DB in sync with what's actually displayed.
    The afternoon refresh regenerates picks (updated odds, pitcher, lineups) and
    may change which team is picked for a game — this ensures the DB reflects
    the latest displayed pick so the grader can match them correctly.

    IMPORTANT: actual_result (the grade) is intentionally excluded from the
    UPDATE SET so that grades written by run_analysis.py are never overwritten.
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
                         conf, tier, reasoning, market_signal, actual_result)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    ON CONFLICT (pick_date, game_id, pick_type) DO UPDATE SET
                        label         = EXCLUDED.label,
                        team          = EXCLUDED.team,
                        game          = EXCLUDED.game,
                        conf          = EXCLUDED.conf,
                        tier          = EXCLUDED.tier,
                        reasoning     = EXCLUDED.reasoning,
                        market_signal = COALESCE(picks.market_signal, EXCLUDED.market_signal)
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
                        _infer_market_signal(p.get("reasoning", "")),
                    )
                )
                inserted += cur.rowcount
            log.info(f"Picks upserted to DB: {inserted} rows for {pick_date}.")
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



# ── Market Signal helpers ──────────────────────────────────────────────────

def _infer_market_signal(reasoning: str) -> str:
    """
    Infer market signal from pick reasoning text.
    Returns 'CONFIRM', 'DIVERGE', or 'NEUTRAL'.
    CONFIRM = Kalshi/Polymarket agreed with the model.
    DIVERGE = markets disagreed with the model.
    NEUTRAL = no market data or ambiguous signal.
    """
    if not reasoning:
        return "NEUTRAL"
    r = reasoning.upper()
    if "CONFIRM" in r:
        return "CONFIRM"
    if "DIVERGE" in r:
        return "DIVERGE"
    return "NEUTRAL"


def backfill_market_signals() -> int:
    """
    For every pick that has no market_signal set, infer it from the reasoning
    text and write it back.  Safe to call repeatedly -- only touches NULL rows.
    Returns the number of rows updated.
    """
    updated = 0
    with db_conn() as conn:
        if conn is None:
            return 0
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, reasoning FROM picks WHERE market_signal IS NULL"
            )
            rows = cur.fetchall()
            for pick_id, reasoning in rows:
                signal = _infer_market_signal(reasoning or "")
                cur.execute(
                    "UPDATE picks SET market_signal = %s WHERE id = %s",
                    (signal, pick_id),
                )
                updated += cur.rowcount
            log.info(f"backfill_market_signals: {updated} row(s) updated.")
        except Exception as e:
            log.warning(f"backfill_market_signals failed: {e}")
    return updated


def get_accuracy_by_market_signal(days: int = 30) -> list:
    """
    Returns W/L/ROI grouped by market_signal (CONFIRM / DIVERGE / NEUTRAL)
    over the last N days.
    """
    with db_conn() as conn:
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    COALESCE(market_signal, 'NEUTRAL') AS market_signal,
                    COUNT(*) FILTER (WHERE actual_result = 'WIN')  AS wins,
                    COUNT(*) FILTER (WHERE actual_result = 'LOSS') AS losses,
                    COUNT(*) FILTER (WHERE actual_result = 'PUSH') AS pushes,
                    ROUND(AVG(conf)::numeric, 3) AS avg_conf
                FROM picks
                WHERE pick_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND actual_result IN ('WIN', 'LOSS', 'PUSH')
                GROUP BY COALESCE(market_signal, 'NEUTRAL')
                ORDER BY market_signal
                """,
                (days,)
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            log.warning(f"get_accuracy_by_market_signal failed: {e}")
            return []


def get_monthly_accuracy() -> list:
    """
    Returns W/L/ROI grouped by calendar month for all graded picks.
    Sorted most-recent first.
    """
    with db_conn() as conn:
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    TO_CHAR(pick_date, 'YYYY-MM') AS month,
                    COUNT(*) FILTER (WHERE actual_result = 'WIN')  AS wins,
                    COUNT(*) FILTER (WHERE actual_result = 'LOSS') AS losses,
                    COUNT(*) FILTER (WHERE actual_result = 'PUSH') AS pushes,
                    ROUND(AVG(conf)::numeric, 3) AS avg_conf
                FROM picks
                WHERE actual_result IN ('WIN', 'LOSS', 'PUSH')
                GROUP BY TO_CHAR(pick_date, 'YYYY-MM')
                ORDER BY month DESC
                """
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            log.warning(f"get_monthly_accuracy failed: {e}")
            return []


# ── Prop history ──────────────────────────────────────────────────────────────

def save_prop_pick(game_date, player_name, team, away_team, home_team,
                   prop_type, line, model_conf):
    """Insert a prop pick row (ungraded). Ignores duplicates."""
    with db_conn() as conn:
        if conn is None:
            log.debug("No DB — prop pick not saved.")
            return
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO player_prop_history
                    (game_date, player_name, team, away_team, home_team,
                     prop_type, line, model_conf)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_date, player_name, prop_type, line) DO NOTHING
                """,
                (game_date, player_name, team, away_team, home_team,
                 prop_type, float(line), round(float(model_conf), 3))
            )
        except Exception as e:
            log.warning(f"save_prop_pick failed: {e}")


def grade_prop_pick(game_date, player_name, prop_type, line, actual_value):
    """Set actual_value and result (OVER/UNDER/PUSH) for a prop row."""
    line_f   = float(line)
    actual_f = float(actual_value)
    if actual_f > line_f:
        result = "OVER"
    elif actual_f < line_f:
        result = "UNDER"
    else:
        result = "PUSH"

    with db_conn() as conn:
        if conn is None:
            return
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE player_prop_history
                SET actual_value = %s,
                    result       = %s,
                    graded_at    = NOW()
                WHERE game_date   = %s
                  AND player_name = %s
                  AND prop_type   = %s
                  AND line        = %s
                  AND result IS NULL
                """,
                (actual_f, result, game_date, player_name, prop_type, line_f)
            )
        except Exception as e:
            log.warning(f"grade_prop_pick failed ({player_name}/{prop_type}): {e}")


def get_prop_accuracy(days: int = 30) -> list:
    """
    Return prop hit rates by prop_type and overall.
    Returns list of dicts: {prop_type, total, hits, hit_rate, avg_conf}
    """
    rows = []
    with db_conn() as conn:
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    prop_type,
                    COUNT(*) FILTER (WHERE result IN ('OVER','UNDER','PUSH')) AS total,
                    COUNT(*) FILTER (WHERE result = 'OVER')  AS hits,
                    ROUND(
                        COUNT(*) FILTER (WHERE result = 'OVER')::numeric /
                        NULLIF(COUNT(*) FILTER (WHERE result IN ('OVER','UNDER','PUSH')), 0),
                        3
                    ) AS hit_rate,
                    ROUND(AVG(model_conf)::numeric, 3) AS avg_conf
                FROM player_prop_history
                WHERE game_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND result IS NOT NULL
                GROUP BY prop_type
                ORDER BY hit_rate DESC NULLS LAST
                """,
                (days,)
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            log.warning(f"get_prop_accuracy failed: {e}")
    return rows


def get_player_prop_accuracy(days: int = 30, min_picks: int = 5) -> list:
    """
    Return per-player hit rates for each prop type.
    Returns list of dicts: {player_name, prop_type, total, hits, hit_rate, avg_conf}
    Filtered to players with >= min_picks graded results.
    """
    rows = []
    with db_conn() as conn:
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    player_name,
                    prop_type,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE result = 'OVER') AS hits,
                    ROUND(
                        COUNT(*) FILTER (WHERE result = 'OVER')::numeric /
                        NULLIF(COUNT(*), 0),
                        3
                    ) AS hit_rate,
                    ROUND(AVG(model_conf)::numeric, 3) AS avg_conf
                FROM player_prop_history
                WHERE game_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND result IS NOT NULL
                GROUP BY player_name, prop_type
                HAVING COUNT(*) >= %s
                ORDER BY hit_rate DESC NULLS LAST
                """,
                (days, min_picks)
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        except Exception as e:
            log.warning(f"get_player_prop_accuracy failed: {e}")
    return rows


def get_player_trailing_hit_rate(player_name: str, prop_type: str,
                                  line, days: int = 30):
    """
    Return trailing hit rate (0.0-1.0) for a specific player/prop/line combo.
    Returns None if fewer than 5 graded results exist.
    """
    with db_conn() as conn:
        if conn is None:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE result = 'OVER') AS hits
                FROM player_prop_history
                WHERE player_name = %s
                  AND prop_type   = %s
                  AND line        = %s
                  AND game_date  >= CURRENT_DATE - INTERVAL '%s days'
                  AND result IS NOT NULL
                """,
                (player_name, prop_type, float(line), days)
            )
            row = cur.fetchone()
            if not row or row[0] < 5:
                return None
            total, hits = row
            return round(hits / total, 3) if total > 0 else None
        except Exception as e:
            log.warning(f"get_player_trailing_hit_rate failed: {e}")
            return None


def get_sharp_vs_model(days: int = 30) -> list:
    """
    Returns rows where sharp STEAM action contradicted the model ML pick.
    Joins picks (model pick + result) with scored_games (sharp_side, ml_signal).
    For each row: date, game, model_pick, sharp_pick, result, who_was_right.
    """
    with db_conn() as conn:
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    p.pick_date,
                    p.game,
                    p.label           AS model_label,
                    sg.sharp_side,
                    sg.ml_signal,
                    p.tier,
                    p.conf,
                    p.actual_result,
                    CASE
                        WHEN p.actual_result = 'WIN'  THEN 'model'
                        WHEN p.actual_result = 'LOSS' THEN 'sharp'
                        ELSE 'push'
                    END AS who_was_right
                FROM picks p
                JOIN scored_games sg
                  ON sg.score_date = p.pick_date
                 AND sg.game_id    = p.game_id
                WHERE p.pick_type = 'ML'
                  AND sg.ml_signal = 'STEAM'
                  AND sg.sharp_side IS NOT NULL
                  AND sg.sharp_side <> ''
                  AND sg.sharp_side <> p.team
                  AND p.pick_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND p.actual_result IN ('WIN', 'LOSS', 'PUSH')
                ORDER BY p.pick_date DESC
                """,
                (days,)
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            return rows
        except Exception as e:
            log.warning(f"get_sharp_vs_model failed: {e}")
            return []

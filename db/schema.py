"""
db/schema.py
Creates all Statalizers tables if they don't exist.

Call create_all() once on startup (idempotent — safe to re-run).

Tables:
  pipeline_runs   — tracks daily pipeline executions (replaces pipeline_run_date.txt)
  picks           — every pick generated, with backtest result fields for later grading
  scored_games    — full model output snapshot per game per day
"""

import logging
from db.connection import db_conn

log = logging.getLogger(__name__)

# ── Table definitions ─────────────────────────────────────────────────────────

_PIPELINE_RUNS = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_date        DATE        NOT NULL UNIQUE,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'running',
    notes           TEXT
);
"""

_PICKS = """
CREATE TABLE IF NOT EXISTS picks (
    id              SERIAL PRIMARY KEY,
    pick_date       DATE        NOT NULL,
    game_id         TEXT,
    game            TEXT        NOT NULL,
    pick_type       TEXT        NOT NULL,   -- ML | TOTAL | RL
    label           TEXT        NOT NULL,   -- e.g. "Yankees ML", "OVER 8.5"
    team            TEXT,
    conf            REAL        NOT NULL,
    tier            TEXT        NOT NULL,   -- LOCK | STRONG | LEAN
    reasoning       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Backtesting fields — filled by the grading step after game results are in
    actual_result   TEXT,                   -- WIN | LOSS | PUSH | PENDING
    graded_at       TIMESTAMPTZ,
    away_final      INTEGER,
    home_final      INTEGER,

    UNIQUE (pick_date, game_id, pick_type)  -- prevent duplicate picks for same game/type/day
);
"""

_SCORED_GAMES = """
CREATE TABLE IF NOT EXISTS scored_games (
    id              SERIAL PRIMARY KEY,
    score_date      DATE        NOT NULL,
    game_id         TEXT,
    away_team       TEXT        NOT NULL,
    home_team       TEXT        NOT NULL,

    -- Model outputs
    exp_away        REAL,
    exp_home        REAL,
    exp_total       REAL,
    home_wp         REAL,
    away_wp         REAL,

    -- Picks
    ml_team         TEXT,
    ml_conf         REAL,
    total_pick      TEXT,
    total_line      REAL,
    total_conf      REAL,
    rl_team         TEXT,
    rl_pick         TEXT,
    rl_conf         REAL,

    -- Key signals (for future analysis)
    ml_signal       TEXT,
    total_signal    TEXT,
    sharp_side      TEXT,
    ml_adj          REAL,
    total_adj       REAL,
    gap_adj         REAL,
    conv_adj        REAL,
    park_runs       REAL,
    weather_flag    TEXT,
    away_sp         TEXT,
    home_sp         TEXT,
    away_sp_era     REAL,
    home_sp_era     REAL,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (score_date, game_id)
);
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_picks_pick_date    ON picks(pick_date);",
    "CREATE INDEX IF NOT EXISTS idx_picks_actual_result ON picks(actual_result);",
    "CREATE INDEX IF NOT EXISTS idx_scored_score_date  ON scored_games(score_date);",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_run_date  ON pipeline_runs(run_date);",
]


def create_all():
    """
    Create all tables and indexes. Idempotent — safe to call on every startup.
    Silently skips if no DB connection is available.
    """
    with db_conn() as conn:
        if conn is None:
            log.debug("No DB connection — skipping schema creation.")
            return

        try:
            cur = conn.cursor()
            cur.execute(_PIPELINE_RUNS)
            cur.execute(_PICKS)
            cur.execute(_SCORED_GAMES)
            for idx in _INDEXES:
                cur.execute(idx)
            log.info("DB schema verified / created.")
        except Exception as e:
            log.warning(f"Schema creation failed (non-fatal): {e}")

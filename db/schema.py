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
    market_signal   TEXT,                   -- CONFIRM | DIVERGE | NEUTRAL (Kalshi/Poly)
    tier_locked     BOOLEAN     NOT NULL DEFAULT FALSE,  -- freezes conf/tier once lineups confirm
    -- Latched TRUE the first time a pick qualifies as a Best Bet at ANY
    -- price during the day. Best Bets is path-dependent: lines move, so a
    -- pick can qualify at 2pm and not at 6pm. Only two prices are stored
    -- (open and close), so evaluating either one alone misses most of what
    -- actually surfaced. Latching at save time records it as it happens.
    was_best_bet BOOLEAN DEFAULT FALSE,

    -- PRICE (added 2026-08-11). Without this, EV/ROI/CLV cannot be measured or
    -- backfilled, which is why the 2026-08-11 review could only ever report win
    -- RATE. odds = American price on the side actually picked, captured at save
    -- time. closing_odds = the last pre-first-pitch price, for CLV.
    odds            REAL,
    odds_at         TIMESTAMPTZ,
    closing_odds    REAL,
    closing_odds_at TIMESTAMPTZ,
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

_PLAYER_PROP_HISTORY = """
CREATE TABLE IF NOT EXISTS player_prop_history (
    id              SERIAL PRIMARY KEY,
    game_date       DATE NOT NULL,
    player_name     TEXT NOT NULL,
    team            TEXT,
    away_team       TEXT,
    home_team       TEXT,
    prop_type       TEXT NOT NULL,
    line            NUMERIC(4,1) NOT NULL,
    actual_value    NUMERIC(5,1),
    result          TEXT,
    pick_side       TEXT DEFAULT 'OVER',   -- direction bet: OVER / UNDER
    model_conf      NUMERIC(5,3),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    graded_at       TIMESTAMPTZ,
    UNIQUE (game_date, player_name, prop_type, line)
);
"""

_MODEL_CONFIG = """
CREATE TABLE IF NOT EXISTS model_config (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    value       REAL NOT NULL,
    min_val     REAL,
    max_val     REAL,
    step        REAL,
    label       TEXT,
    group_name  TEXT,
    description TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

_SITE_CONFIG = """
CREATE TABLE IF NOT EXISTS site_config (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""

_BETS = """
-- BETS YOU ACTUALLY PLACED (added 2026-08-18).
--
-- Distinct from `picks`, and the distinction is the whole point. `picks` is what
-- the model published. This is what money was really put on: YOUR price at YOUR
-- book at the moment YOU placed it. Those differ constantly — the board quotes
-- Pinnacle, bets get placed at Hard Rock or DraftKings, and the line moves in
-- between.
--
-- Without this table the only answerable question is "was the model right".
-- With it you can also ask "did I make money" and, more usefully, "did I beat
-- the closing number", which converges far faster than win rate.
--
-- Grading is not duplicated here. result is filled by joining back to picks on
-- (bet_date, game_id, pick_type), so there is exactly one grader.
CREATE TABLE IF NOT EXISTS bets (
    id           SERIAL PRIMARY KEY,
    bet_date     DATE NOT NULL,
    game_id      TEXT NOT NULL,
    game         TEXT,
    pick_type    TEXT NOT NULL,
    label        TEXT NOT NULL,
    team         TEXT,
    price        NUMERIC NOT NULL,      -- American odds YOU got
    stake        NUMERIC NOT NULL,      -- dollars risked
    book         TEXT,                  -- where it was placed
    placed_at    TIMESTAMPTZ DEFAULT NOW(),
    note         TEXT,
    -- filled at grade time from picks / scored_games
    result       TEXT DEFAULT 'PENDING',
    closing_odds NUMERIC,
    graded_at    TIMESTAMPTZ
);
"""


_PLAYER_GAME_LOGS = """
CREATE TABLE IF NOT EXISTS player_game_logs (
    id              SERIAL PRIMARY KEY,
    game_date       DATE        NOT NULL,
    player_name     TEXT        NOT NULL,
    player_id       INTEGER,
    team            TEXT,
    opponent        TEXT,
    venue           TEXT,
    pitcher_hand    TEXT,
    ab              INTEGER     DEFAULT 0,
    h               INTEGER     DEFAULT 0,
    hr              INTEGER     DEFAULT 0,
    rbi             INTEGER     DEFAULT 0,
    bb              INTEGER     DEFAULT 0,
    k               INTEGER     DEFAULT 0,
    tb              INTEGER     DEFAULT 0,
    sb              INTEGER     DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (game_date, player_name, team)
);
"""

_CHECKLIST_STATE = """
-- STATUS OVERRIDES FOR CHECKLIST ITEMS THAT CANNOT BE PROBED (added 2026-08-18).
--
-- model/checklist.py verifies most items by reading the repo, and for those the
-- evidence always wins: nothing in this table can mark a probed item done. That
-- is deliberate. A hand set flag is exactly how CLAUDE.md ended up describing
-- tier thresholds of 75/68/60/48 for weeks while the code used 68/62/52/48.
--
-- This table exists only for the judgement calls, where no probe is possible
-- (rotate an external key, rebuild a projection, decide a tradeoff). Those
-- render on the page as ASSERTED with the date and the note, never as measured,
-- so it is always obvious which half of the list is which.
CREATE TABLE IF NOT EXISTS checklist_state (
    item_id     TEXT PRIMARY KEY,
    status      TEXT NOT NULL,
    note        TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_CHECKLIST_NOTES = """
-- ITEMS JUSTIN ADDS FROM THE ADMIN PAGE (added 2026-08-18).
--
-- The problem this solves, in his words: "Things that I thought we already have
-- apparently have just been sitting there and I was never prompted to add
-- those." Ideas raised in a chat session die with that session, since Cowork
-- keeps only the last ~50 and older ones age out unrecoverably.
--
-- Anything typed here survives the session, the deploy and the context window.
-- status: 'new' until reviewed, then 'accepted' (promoted into model/checklist.py
-- ITEMS), 'declined' (with a reason in response), or 'done'.
CREATE TABLE IF NOT EXISTS checklist_notes (
    id           SERIAL PRIMARY KEY,
    title        TEXT NOT NULL,
    detail       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status       TEXT NOT NULL DEFAULT 'new',
    response     TEXT,
    responded_at TIMESTAMPTZ
);
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_picks_pick_date    ON picks(pick_date);",
    "CREATE INDEX IF NOT EXISTS idx_picks_actual_result ON picks(actual_result);",
    "CREATE INDEX IF NOT EXISTS idx_scored_score_date  ON scored_games(score_date);",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_run_date  ON pipeline_runs(run_date);",
    "CREATE INDEX IF NOT EXISTS idx_prop_history_player ON player_prop_history (player_name, prop_type);",
    "CREATE INDEX IF NOT EXISTS idx_prop_history_date   ON player_prop_history (game_date);",
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
            cur.execute(_PLAYER_PROP_HISTORY)
            cur.execute(_MODEL_CONFIG)
            cur.execute(_SITE_CONFIG)
            cur.execute(_PLAYER_GAME_LOGS)
            cur.execute(_BETS)
            cur.execute(_CHECKLIST_STATE)
            cur.execute(_CHECKLIST_NOTES)
            for idx in _INDEXES:
                cur.execute(idx)
            # Migration: pick_side = the DIRECTION bet (OVER/UNDER). Existing rows
            # are all historical OVER props, so default 'OVER' keeps them correct.
            cur.execute(
                "ALTER TABLE player_prop_history "
                "ADD COLUMN IF NOT EXISTS pick_side TEXT DEFAULT 'OVER'")
            # market_signal existed in production but was never in schema.py —
            # add it so a fresh create_all() DB matches production.
            cur.execute(
                "ALTER TABLE picks ADD COLUMN IF NOT EXISTS market_signal TEXT")
            # tier_locked freezes conf/tier once a game's lineups confirm (bet-time
            # tier), so intraday re-scores can't downgrade a LOCK before grading.
            cur.execute(
                "ALTER TABLE picks ADD COLUMN IF NOT EXISTS tier_locked BOOLEAN NOT NULL DEFAULT FALSE")
            # was_best_bet latches TRUE the first time a pick qualifies as a Best
            # Bet at ANY price during the day. See model/best_bets.py for why a
            # single stored price cannot answer that question.
            cur.execute(
                "ALTER TABLE picks ADD COLUMN IF NOT EXISTS was_best_bet BOOLEAN NOT NULL DEFAULT FALSE")
            # Price columns (2026-08-11). Without a stored price, EV, ROI and CLV
            # cannot be measured or backfilled — which is why the 2026-08-11
            # review could only ever report win RATE, never whether the model
            # beat the number it bet into.
            for _col, _type in (("odds", "REAL"), ("odds_at", "TIMESTAMPTZ"),
                                ("closing_odds", "REAL"), ("closing_odds_at", "TIMESTAMPTZ")):
                cur.execute(f"ALTER TABLE picks ADD COLUMN IF NOT EXISTS {_col} {_type}")
            log.info("DB schema verified / created.")
        except Exception as e:
            log.warning(f"Schema creation failed (non-fatal): {e}")

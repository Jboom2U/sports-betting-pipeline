"""
db/model_config.py
Load and save model weight configuration from PostgreSQL.
Falls back to hardcoded defaults when DB is unavailable.
"""

import logging
from db.connection import db_conn

log = logging.getLogger(__name__)

# Default weights — mirrors the hardcoded values in mlb_model.py / mlb_picks.py
DEFAULTS = {
    # Core model
    "pythagorean_exp":       {"value": 1.83,  "min": 1.50, "max": 2.20, "step": 0.01,  "label": "Pythagorean exponent",     "group": "core"},
    "home_field_boost":      {"value": 0.025, "min": 0.00, "max": 0.06, "step": 0.005, "label": "Home field boost",          "group": "core"},
    # Pitcher blending
    "recent_starts_weight":  {"value": 0.30,  "min": 0.00, "max": 0.60, "step": 0.05,  "label": "Recent form blend",         "group": "pitcher"},
    "split_weight":          {"value": 0.30,  "min": 0.00, "max": 0.60, "step": 0.05,  "label": "Home/away split weight",    "group": "pitcher"},
    # Statcast
    "statcast_xwoba_weight": {"value": 0.25,  "min": 0.00, "max": 0.40, "step": 0.05,  "label": "Statcast xwOBA weight",    "group": "pitcher"},
    "statcast_whiff_weight": {"value": 0.10,  "min": 0.00, "max": 0.20, "step": 0.02,  "label": "Whiff rate weight",         "group": "pitcher"},
    # Market signals
    "kalshi_delta":          {"value": 0.015, "min": 0.00, "max": 0.05, "step": 0.005, "label": "Kalshi confidence delta",   "group": "market"},
    "polymarket_delta":      {"value": 0.015, "min": 0.00, "max": 0.05, "step": 0.005, "label": "Polymarket delta",          "group": "market"},
    "sharp_action_delta":    {"value": 0.020, "min": 0.00, "max": 0.05, "step": 0.005, "label": "Sharp action delta",        "group": "market"},
    # Environmental
    "umpire_blend":          {"value": 0.40,  "min": 0.00, "max": 0.60, "step": 0.05,  "label": "Umpire RPG blend weight",   "group": "environment"},
    "wind_runs_per_mph":     {"value": 0.04,  "min": 0.00, "max": 0.10, "step": 0.01,  "label": "Wind runs per mph",         "group": "environment"},
    "cold_penalty":          {"value": 0.012, "min": 0.00, "max": 0.030,"step": 0.001, "label": "Cold temp penalty/°F",      "group": "environment"},
    "precip_penalty":        {"value": 0.003, "min": 0.00, "max": 0.010,"step": 0.001, "label": "Precip penalty/%",          "group": "environment"},
    # Bullpen fatigue
    "fatigue_tired_adj":     {"value": 0.12,  "min": 0.00, "max": 0.25, "step": 0.01,  "label": "TIRED bullpen ERA adj",     "group": "bullpen"},
    "fatigue_spent_adj":     {"value": 0.20,  "min": 0.00, "max": 0.35, "step": 0.01,  "label": "SPENT bullpen ERA adj",     "group": "bullpen"},
    "fatigue_fresh_adj":     {"value": -0.05, "min": -0.10,"max": 0.00, "step": 0.01,  "label": "FRESH bullpen ERA adj",     "group": "bullpen"},
    # Confidence thresholds
    "tier_lock":             {"value": 0.68,  "min": 0.60, "max": 0.80, "step": 0.01,  "label": "LOCK threshold",            "group": "tiers"},
    "tier_strong":           {"value": 0.62,  "min": 0.55, "max": 0.70, "step": 0.01,  "label": "STRONG threshold",          "group": "tiers"},
    "tier_lean":             {"value": 0.52,  "min": 0.48, "max": 0.62, "step": 0.01,  "label": "LEAN threshold",            "group": "tiers"},
}

# Named presets
PRESETS = {
    "default-v1": {k: v["value"] for k, v in DEFAULTS.items()},
    "sharp-heavy": {
        **{k: v["value"] for k, v in DEFAULTS.items()},
        "kalshi_delta": 0.030,
        "polymarket_delta": 0.030,
        "sharp_action_delta": 0.040,
    },
    "market-lean": {
        **{k: v["value"] for k, v in DEFAULTS.items()},
        "kalshi_delta": 0.040,
        "polymarket_delta": 0.040,
        "sharp_action_delta": 0.010,
        "umpire_blend": 0.20,
    },
    "stats-only": {
        **{k: v["value"] for k, v in DEFAULTS.items()},
        "kalshi_delta": 0.000,
        "polymarket_delta": 0.000,
        "sharp_action_delta": 0.000,
    },
}


def load_config() -> dict:
    """
    Load model config from DB. Returns a flat {name: value} dict.
    Falls back to DEFAULTS for any missing key.
    """
    cfg = {k: v["value"] for k, v in DEFAULTS.items()}
    with db_conn() as conn:
        if conn is None:
            return cfg
        try:
            cur = conn.cursor()
            cur.execute("SELECT name, value FROM model_config")
            for name, value in cur.fetchall():
                if name in cfg:
                    cfg[name] = float(value)
        except Exception as e:
            log.warning(f"Could not load model_config from DB (using defaults): {e}")
    return cfg


def save_config(cfg: dict) -> bool:
    """
    Upsert a {name: value} dict into model_config.
    Only updates keys that exist in DEFAULTS.
    """
    with db_conn() as conn:
        if conn is None:
            return False
        try:
            cur = conn.cursor()
            for name, value in cfg.items():
                if name not in DEFAULTS:
                    continue
                d = DEFAULTS[name]
                cur.execute("""
                    INSERT INTO model_config
                        (name, value, min_val, max_val, step, label, group_name, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (name) DO UPDATE
                        SET value = EXCLUDED.value, updated_at = NOW()
                """, (name, float(value),
                      d.get("min"), d.get("max"), d.get("step"),
                      d.get("label"), d.get("group")))
            return True
        except Exception as e:
            log.warning(f"Could not save model_config: {e}")
            return False


def get_defaults() -> dict:
    """Return full DEFAULTS dict (includes metadata: min/max/step/label/group)."""
    return DEFAULTS


def get_presets() -> dict:
    """Return named preset configurations."""
    return PRESETS

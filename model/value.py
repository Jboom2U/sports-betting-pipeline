"""
model/value.py — Pick VALUE / EV against the real market price.

Confidence answers "how likely is this to win?". Value answers "is the price
wrong?". A 72% team at -240 is high confidence but near-zero value: -240 needs
70.6% just to break even. This module de-vigs the two-way market to get the
market's true probability, compares it to the model's probability, and computes
EV per $1 staked. Display/advisory only — it does NOT change pick generation.

All functions are pure and side-effect free so they're easy to unit test.
"""
from __future__ import annotations


def american_to_decimal(american: float) -> float | None:
    if american in (None, 0, ""):
        return None
    a = float(american)
    return 1 + (a / 100.0 if a > 0 else 100.0 / abs(a))


def implied_prob(american: float) -> float | None:
    """Raw (vig-inclusive) implied probability from American odds."""
    d = american_to_decimal(american)
    return None if d is None else 1.0 / d


def devig_two_way(price_pick: float, price_other: float) -> float | None:
    """
    No-vig probability for the PICKED side, given both sides' American prices.
    Falls back to the raw implied prob if the other side's price is missing.
    """
    ip = implied_prob(price_pick)
    if ip is None:
        return None
    io = implied_prob(price_other)
    if io is None or (ip + io) <= 0:
        return ip
    return ip / (ip + io)


def ev_per_unit(model_p: float, american: float) -> float | None:
    """
    Expected profit per $1 staked at these odds if the true win prob is model_p.
    +0.05 means +5% EV (a $1 bet returns $1.05 on average).
    """
    d = american_to_decimal(american)
    if d is None or model_p is None:
        return None
    return model_p * (d - 1.0) - (1.0 - model_p)


# EV thresholds for the value tag
_EV_GOOD = 0.03    # +3% EV or better = real value
_EV_OK   = 0.00    # break-even to +3% = fair/thin
_CHALK_PRICE = -180  # at or below this, a favorite is "chalk" — value is scarce


def classify(model_p: float, price_pick: float, price_other: float) -> dict:
    """
    Return {market_prob, edge, ev, tag, chalk} for a pick.
      market_prob — de-vigged market probability for the picked side
      edge        — model_p - market_prob (positive = model sees more than market)
      ev          — expected value per $1 at the picked price
      tag         — "VALUE" | "FAIR" | "NO VALUE" | "" (no price)
      chalk       — True when the picked price is a heavy favorite (<= -180)
    """
    if price_pick in (None, 0, ""):
        return {"market_prob": None, "edge": None, "ev": None, "tag": "", "chalk": False}
    market_prob = devig_two_way(price_pick, price_other)
    ev = ev_per_unit(model_p, price_pick)
    edge = (model_p - market_prob) if (market_prob is not None) else None
    if ev is None:
        tag = ""
    elif ev >= _EV_GOOD:
        tag = "VALUE"
    elif ev >= _EV_OK:
        tag = "FAIR"
    else:
        tag = "NO VALUE"
    chalk = float(price_pick) <= _CHALK_PRICE
    return {"market_prob": market_prob, "edge": edge, "ev": ev, "tag": tag, "chalk": chalk}


def value_for_pick(pick: dict) -> dict:
    """
    Compute value for a generated pick dict (ML / RL / TOTAL) using the prices on
    pick['game_data']. Returns the classify() dict; empty tag when no price.
    """
    g = pick.get("game_data", {}) or {}
    ptype = pick.get("type")
    model_p = pick.get("conf")

    if ptype == "ML":
        away = g.get("away_team", "")
        picked_away = (pick.get("side") == "away") or (pick.get("team") == away)
        price_pick  = g.get("ml_away_odds") if picked_away else g.get("ml_home_odds")
        price_other = g.get("ml_home_odds") if picked_away else g.get("ml_away_odds")

    elif ptype == "RL":
        away = g.get("away_team", "")
        picked_away = pick.get("team") == away
        price_pick  = g.get("rl_away_price") if picked_away else g.get("rl_home_price")
        price_other = g.get("rl_home_price") if picked_away else g.get("rl_away_price")

    elif ptype == "TOTAL":
        is_over = (pick.get("side") == "over") or ("OVER" in (pick.get("label", "").upper()))
        price_pick  = g.get("total_over_price")  if is_over else g.get("total_under_price")
        price_other = g.get("total_under_price") if is_over else g.get("total_over_price")

    else:
        return {"market_prob": None, "edge": None, "ev": None, "tag": "", "chalk": False}

    return classify(model_p, price_pick, price_other)

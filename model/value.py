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
    # American odds cannot sit strictly between -100 and +100. A value like -57
    # is corrupt data (e.g. the odds scraper averaging a +180 and a -220 run-line
    # price in a game where books split on the favorite). Reject it so no EV is
    # built on a fake price.
    if abs(a) < 100:
        return None
    return 1 + (a / 100.0 if a > 0 else 100.0 / abs(a))


def implied_prob(american: float) -> float | None:
    """Raw (vig-inclusive) implied probability from American odds."""
    d = american_to_decimal(american)
    return None if d is None else 1.0 / d


def _power_k(p1: float, p2: float) -> float:
    """Exponent k where p1**k + p2**k == 1. Bisection; both inputs are in (0,1)
    so x**k falls as k rises, the sum is monotonic, and this always converges."""
    lo, hi = 0.5, 4.0
    for _ in range(60):
        k = (lo + hi) / 2.0
        if (p1 ** k + p2 ** k) > 1.0:
            lo = k
        else:
            hi = k
    return (lo + hi) / 2.0


def devig_two_way(price_pick: float, price_other: float,
                  method: str = "power") -> float | None:
    """
    No-vig probability for the PICKED side, from both sides' American prices.

    POWER METHOD, not proportional (changed 2026-08-19).

    Proportional devig (ip / (ip + io)) assumes vig is spread evenly across both
    sides. It is not. Longshots are overbet, books load more juice on the dog, and
    removing it proportionally strips too much from the favourite.

    The DIRECTION of that error is the point. It understates the favourite's true
    probability, which inflates the model's apparent edge over the market on
    favourites. This model's ML picks skew to -130/-160 favourites that returned
    -13.5% at real prices on /admin/real-roi, so the old method was flattering
    exactly the picks that lose money.

    Size: about 0.4 points at -150, about 1.2 points at -240, growing as the
    market gets more lopsided. Modest, and in the safe direction. It makes
    marginal chalk look worse, never better.
    """
    ip = implied_prob(price_pick)
    io = implied_prob(price_other)
    if ip is None:
        return None
    if io is None or (ip + io) <= 0:
        # Was: return ip. That handed back the RAW vig inclusive probability in a
        # field labelled no-vig, putting two different quantities in one variable.
        # That exact pattern produced the -109 run line and the column shift, so
        # refuse instead. A missing value is safe, a wrong value is not.
        return None
    if method == "proportional":
        return ip / (ip + io)
    try:
        k = _power_k(ip, io)
        v = ip ** k
        if not (0.0 < v < 1.0):
            raise ValueError("power devig out of range")
        return v
    except Exception:
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
        # Price by the ACTUAL handicap (2026-08-11). rl_away_price/rl_home_price
        # are keyed to the MARKET favorite, so on a game where the model and the
        # market disagree they hand a "+1.5" pick the "-1.5" price. That produced
        # impossible EV numbers at the top of Best Bets.
        away = g.get("away_team", "")
        picked_away = pick.get("team") == away
        side  = "away" if picked_away else "home"
        other = "home" if picked_away else "away"
        hcap  = "p15" if "+1.5" in (pick.get("label", "") or "") else "m15"
        opp   = "m15" if hcap == "p15" else "p15"
        # NO legacy fallback (removed 2026-08-14). rl_home_price/rl_away_price
        # are the Odds API's AVERAGE of every book's run line price, pooled
        # regardless of which line each book quoted. That average is not a price
        # anyone offers: on 2026-08-14 it produced an identical -109 on two
        # different games while Pinnacle, DraftKings and Hard Rock all sat near
        # -160, inflating every run line EV roughly threefold.
        price_pick  = g.get(f"rl_{side}_{hcap}_price")
        price_other = g.get(f"rl_{other}_{opp}_price")

    elif ptype == "TOTAL":
        is_over = (pick.get("side") == "over") or ("OVER" in (pick.get("label", "").upper()))
        price_pick  = g.get("total_over_price")  if is_over else g.get("total_under_price")
        price_other = g.get("total_under_price") if is_over else g.get("total_over_price")

    else:
        return {"market_prob": None, "edge": None, "ev": None, "tag": "", "chalk": False}

    return classify(model_p, price_pick, price_other)

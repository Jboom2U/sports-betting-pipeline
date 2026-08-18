"""
model/staking.py — bet sizing for a MICRO bankroll, where the minimum bet is the
binding constraint rather than Kelly.

WHY THIS MODULE EXISTS (created 2026-08-18)

The dashboard has always sized bets with:

    _b = 100 / 110            # assumes every bet is priced -110
    _p = p["conf"]            # RAW model confidence
    kelly = (_b*_p - (1-_p)) / _b
    half_kelly = kelly * 0.5

Three defects, each independently fatal:

  1. The price is assumed. Almost nothing is actually -110. A run line at -154
     and a dog at +231 were both sized as though they paid -110.
  2. The probability is raw confidence, which overstates badly. On 2026-08-17 a
     card showed 72.8% while its own calibrated read was 56.1%.
  3. It sized bets on picks that had NO PRICE AT ALL. Before the day's odds pull,
     cards displayed "LOCK 72.8% - Bet $4.30 (21.5% Half-Kelly)".

THE MICRO-BANKROLL PROBLEM, WHICH IS THE REAL ONE

At a $20 bankroll with a $1 minimum stake, the smallest bet that can physically
be placed is 5.0% of the roll. Quarter-Kelly only justifies 5% when full Kelly
reaches 20%, which needs roughly 13% EV at -154 and 18% EV at -110.

So on a micro bankroll the question is never "what does Kelly say". It is
"is the minimum bet I am forced to place smaller than what Kelly permits". A
2.5% edge at -110 sizes to $0.14. Betting $1 there is 7.3x the correct stake —
that is not a small rounding, it is a different strategy with a much larger ruin
probability.

THE RULE THIS IMPLEMENTS

    stake = quarter_kelly * bankroll
    if stake >= min_unit:            -> round DOWN to the unit grid, bet it
    if stake >= min_unit / tolerance -> bet the minimum, flagged as over-bet
    otherwise                        -> NO BET (the edge cannot carry the minimum)

Rounding DOWN, never up: rounding up is how a disciplined stake silently becomes
an over-bet. `tolerance` defaults to 1.5, i.e. accept at most a 50% over-bet.

Probability must be an HONEST one — an observed band rate or a calibrated value,
never raw confidence. This module will not guess it for you; pass it in.
"""
from __future__ import annotations

from dataclasses import dataclass


def american_to_decimal(price) -> float | None:
    """American odds -> decimal. None for anything that is not a real price.

    |price| < 100 is impossible in American odds and has historically meant
    corrupt data in this repo (the averaged-across-handicaps bug produced -57
    and -109). Refuse it rather than computing on it.
    """
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if abs(p) < 100:
        return None
    return 1.0 + p/100.0 if p > 0 else 1.0 + 100.0/abs(p)


@dataclass
class Stake:
    bet: bool
    amount: float          # dollars, already snapped to the unit grid
    fraction: float        # amount / bankroll
    kelly_full: float      # full-Kelly fraction, for reference
    kelly_target: float    # the fraction we actually wanted
    ev: float              # expected value per $1 staked
    overbet_x: float       # amount / ideal; 1.0 is perfect, >1 is over-betting
    reason: str            # human-readable, always populated


def size_bet(prob: float,
             price,
             bankroll: float,
             *,
             kelly_fraction: float = 0.25,
             min_unit: float = 1.0,
             unit_step: float = 1.0,
             max_unit: float = 5.0,
             max_fraction: float = 0.08,
             overbet_tolerance: float = 1.5,
             min_ev: float = 0.0) -> Stake:
    """Size one bet. Returns a Stake that always explains itself.

    prob      HONEST probability (observed band rate / calibrated value). NOT
              raw model confidence.
    price     American odds actually available to you. No price -> no bet.
    bankroll  current roll in dollars.

    kelly_fraction  0.25 = quarter Kelly. Kelly assumes prob is known exactly;
                    here it is estimated off a few dozen graded picks, so the
                    edge itself has error bars. Quarter is the standard fraction
                    for an estimated edge and is what Best Bets already uses.
    min_unit/max_unit  the real-world range you can place ($1-$5).
    unit_step   grid you must bet on ($1 increments).
    max_fraction  hard ceiling as a share of bankroll, regardless of Kelly.
    overbet_tolerance  how far above the ideal stake the forced minimum may go
                    before the bet is refused entirely.
    min_ev      refuse anything below this EV per $1.
    """
    d = american_to_decimal(price)
    if d is None:
        return Stake(False, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                     "no usable price — a stake cannot be computed without one")
    if not (0.0 < prob < 1.0):
        return Stake(False, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                     "probability outside (0,1)")
    if bankroll <= 0:
        return Stake(False, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "no bankroll")

    b  = d - 1.0
    ev = prob*b - (1.0 - prob)              # EV per $1 staked

    if ev <= 0:
        return Stake(False, 0.0, 0.0, 0.0, 0.0, ev, 0.0,
                     f"negative EV ({ev*100:+.1f}%) — the price is too short")
    if ev < min_ev:
        return Stake(False, 0.0, 0.0, 0.0, 0.0, ev, 0.0,
                     f"EV {ev*100:.1f}% is under the {min_ev*100:.1f}% floor")

    kelly_full = ev / b
    target_f   = min(kelly_full * kelly_fraction, max_fraction)
    ideal      = bankroll * target_f

    # Snap DOWN to the unit grid. Rounding up is exactly how a disciplined
    # stake turns into an over-bet without anyone noticing.
    snapped = (int(ideal / unit_step)) * unit_step
    snapped = min(snapped, max_unit)

    if snapped >= min_unit:
        return Stake(True, snapped, snapped/bankroll, kelly_full, target_f, ev,
                     snapped/ideal if ideal else 0.0,
                     f"quarter-Kelly {target_f*100:.1f}% of ${bankroll:,.0f}")

    # Ideal is under one unit. Betting the minimum means over-betting; decide
    # whether the overshoot is tolerable rather than silently rounding up.
    overbet = min_unit / ideal if ideal > 0 else float("inf")
    if overbet <= overbet_tolerance:
        return Stake(True, min_unit, min_unit/bankroll, kelly_full, target_f, ev,
                     overbet,
                     f"below one unit (ideal ${ideal:.2f}); betting the ${min_unit:.0f} "
                     f"minimum at {overbet:.1f}x the correct stake")
    return Stake(False, 0.0, 0.0, kelly_full, target_f, ev, overbet,
                 f"ideal stake is ${ideal:.2f}; the ${min_unit:.0f} minimum would be "
                 f"{overbet:.1f}x that. Edge too small to carry your minimum bet")


def min_ev_for_minimum_bet(price, bankroll: float,
                           *, min_unit: float = 1.0,
                           kelly_fraction: float = 0.25) -> float | None:
    """The EV at which the minimum bet becomes CORRECTLY sized.

    This is the edge floor your bankroll actually implies, and it is far above
    the 2-3% people usually pick. Derivation:

        stake_fraction = kelly_fraction * EV / b   and we need it >= min_unit/bankroll
        => EV >= (min_unit / bankroll) * b / kelly_fraction
    """
    d = american_to_decimal(price)
    if d is None or bankroll <= 0:
        return None
    b = d - 1.0
    return (min_unit / bankroll) * b / kelly_fraction


def daily_plan(candidates: list, bankroll: float, *, daily_cap: float = 20.0,
               **kw) -> dict:
    """Size a whole slate against a daily exposure cap.

    Bets are taken best-EV first. When the cap binds, the remainder are refused
    rather than shrunk: a stake below the minimum is not placeable, so silently
    shrinking would just reintroduce the over-bet problem at the tail.
    """
    scored = []
    for c in candidates:
        s = size_bet(c["prob"], c["price"], bankroll, **kw)
        scored.append({**c, "stake": s})
    scored.sort(key=lambda x: (-x["stake"].ev if x["stake"].bet else 1e9))

    spent, taken, refused = 0.0, [], []
    for row in scored:
        s = row["stake"]
        if not s.bet:
            refused.append(row)
            continue
        if spent + s.amount > daily_cap:
            row["stake"] = Stake(False, 0.0, 0.0, s.kelly_full, s.kelly_target,
                                 s.ev, 0.0,
                                 f"daily cap ${daily_cap:.0f} reached "
                                 f"(${spent:.0f} already committed)")
            refused.append(row)
            continue
        spent += s.amount
        taken.append(row)
    return {"taken": taken, "refused": refused, "staked": spent,
            "cap": daily_cap, "bankroll": bankroll}

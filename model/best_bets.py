"""
model/best_bets.py — the Best Bets rule, in Python, as the source of truth.

WHY THIS EXISTS (created 2026-08-18)

Two problems, one cause.

PROBLEM 1: THE RULE LIVED ONLY IN JAVASCRIPT.
bestBetEval / bandFor were defined inside run_picks_html.py's embedded JS, so
the rule ran in the browser and nothing server-side could see its verdict. That
is how the same quantity ended up computed in two places with different answers
more than once during 2026-08-17.

PROBLEM 2: BEST BETS IS PATH-DEPENDENT AND THE PATH WAS NEVER RECORDED.
A pick qualifies at the price available AT THAT MOMENT. Lines move all day, so
a pick can qualify at 2pm at -140 and stop qualifying by 6pm at -170, or the
reverse. Justin watched three or four different picks surface through the day
and bet them.

The picks table stores only two prices: `odds` (the FIRST price seen, frozen
once tier_locked) and `closing_odds` (the last before first pitch). Evaluating
the rule against `odds` alone answers "did this qualify at the opening number",
which is not the question. It reported 1-0 for a day with several qualifiers.

THE FIX: LATCH IT WHEN IT HAPPENS.
The board re-scores on every lineup change and every 40-minute price pull. Each
of those regenerations calls save_picks. So evaluate the rule at save time and
latch `was_best_bet` to TRUE the first time a pick qualifies, exactly the way
tier_locked latches. The flag then means "this surfaced as a Best Bet at some
point today", which is what was actually on the board.

KEEP IN LOCKSTEP WITH THE JS. The band tables and thresholds below mirror
run_picks_html.py. If one changes, change both in the same commit. The long-term
answer is for the server to compute the verdict and the client to render it, so
the JS copy can be deleted; that is a follow-up, not this change.
"""
from __future__ import annotations

# Observed records since 2026-07-21. `be` is the break-even the band's win rate
# was actually MEASURED at, from /admin/real-roi — used to reject picks whose
# price profile is nothing like the population the rate came from.
ML_BAND_RATES = [
    {"lo": 0.50, "hi": 0.55, "rate": 0.589, "n": 56, "be": 0.518},
    {"lo": 0.55, "hi": 0.60, "rate": 0.350, "n": 80, "be": 0.512},
    {"lo": 0.60, "hi": 0.65, "rate": 0.419, "n": 62, "be": 0.544},
    {"lo": 0.65, "hi": 0.70, "rate": 0.540, "n": 63, "be": 0.568},
    {"lo": 0.70, "hi": 0.75, "rate": 0.656, "n": 32, "be": 0.607},
    {"lo": 0.75, "hi": 0.80, "rate": 0.484, "n": 31, "be": 0.620},
    {"lo": 0.80, "hi": 1.01, "rate": 0.737, "n": 19},
]
RL_BAND_RATES = [
    {"lo": 0.60, "hi": 0.65,  "rate": 0.662, "n": 65, "be": 0.574},
    {"lo": 0.65, "hi": 0.701, "rate": 0.648, "n": 71, "be": 0.625},
]
TOTAL_BAND_RATES = [
    {"lo": 0.50, "hi": 0.55, "rate": 0.491, "n": 108, "be": 0.531},
    {"lo": 0.55, "hi": 0.60, "rate": 0.620, "n": 50,  "be": 0.506},
    {"lo": 0.60, "hi": 0.65, "rate": 0.412, "n": 17},
    {"lo": 0.65, "hi": 1.01, "rate": 0.467, "n": 15},
]

MIN_BAND_N  = 30      # a rate off fewer graded picks is noise wearing a probability
PLATEAU_TOL = 0.08    # an adjacent band must agree within 8 points
PRICE_TOL   = 0.10    # break-even within 10 points of the band's measured one
BET_MIN_EV  = 0.08    # require a cushion, not break-even


def _dec(price):
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if abs(p) < 100:          # not a valid American price; historically corrupt data
        return None
    return 1.0 + p/100.0 if p > 0 else 1.0 + 100.0/abs(p)


def _table(bet_type: str):
    return {"ML": ML_BAND_RATES, "RL": RL_BAND_RATES,
            "TOTAL": TOTAL_BAND_RATES}.get((bet_type or "").upper())


def band_for(conf: float, bet_type: str, price=None):
    """The usable band for this pick, or (None, reason).

    Applies both guards added after the rule flipped a bet on itself:
      PLATEAU  — an isolated spike beside a 24-point cliff is sampling noise.
      PRICE    — a rate measured on -119 favourites does not transfer to a
                 +231 underdog just because the payout is larger.
    """
    tbl = _table(bet_type)
    if not tbl:
        return None, "no band table for this bet type"
    c = float(conf)
    if c > 1.5:
        c /= 100.0
    idx = next((i for i, b in enumerate(tbl) if b["lo"] <= c < b["hi"]), -1)
    if idx < 0:
        return None, "confidence outside every measured band"
    b = tbl[idx]
    if b.get("n") is not None and b["n"] < MIN_BAND_N:
        return None, f"band has only {b['n']} graded picks (need {MIN_BAND_N})"

    nbrs = [x for x in (tbl[idx-1] if idx > 0 else None,
                        tbl[idx+1] if idx+1 < len(tbl) else None)
            if x and (x.get("n") is None or x["n"] >= MIN_BAND_N)]
    if not any(abs(x["rate"] - b["rate"]) <= PLATEAU_TOL for x in nbrs):
        return None, (f"{int(b['lo']*100)}-{int(b['hi']*100)}% is an isolated spike; "
                      f"neighbours disagree by more than {int(PLATEAU_TOL*100)} pts")

    d = _dec(price)
    if b.get("be") is not None and d is not None:
        if abs(1.0/d - b["be"]) > PRICE_TOL:
            return None, ("price is outside the band's measured profile; its win "
                          "rate does not transfer")
    return b, ""


def evaluate(pick: dict, price=None) -> dict:
    """Would this pick qualify as a Best Bet at `price` right now?

    Returns {"bet": bool, "ev": float|None, "rate": float|None, "reason": str}.
    `price` defaults to the pick's own price so callers can also ask "at the
    price on this pick".
    """
    bet_type = (pick.get("type") or pick.get("pick_type") or "").upper()
    conf     = pick.get("conf")
    if conf is None:
        return {"bet": False, "ev": None, "rate": None, "reason": "no confidence"}
    if price is None:
        price = pick.get("pick_price", pick.get("odds"))

    d = _dec(price)
    if d is None:
        return {"bet": False, "ev": None, "rate": None,
                "reason": "no clean price from the odds feed"}

    band, why = band_for(conf, bet_type, price)
    if band is None:
        return {"bet": False, "ev": None, "rate": None, "reason": why}

    # A run line that covers +1.5 must be MORE likely than the same team winning
    # outright. A price saying otherwise is corrupt, not an opportunity.
    if bet_type == "RL" and "+1.5" in (pick.get("label") or ""):
        g = pick.get("game_data") or {}
        ml = g.get("ml_away_odds") if pick.get("team") == g.get("away_team") \
            else g.get("ml_home_odds")
        dm = _dec(ml)
        if dm is not None and (1.0/d) <= (1.0/dm):
            return {"bet": False, "ev": None, "rate": band["rate"],
                    "reason": "run line price contradicts its own moneyline"}

    rate = band["rate"]
    ev   = rate*(d - 1.0) - (1.0 - rate)
    if ev <= 0:
        return {"bet": False, "ev": ev, "rate": rate,
                "reason": f"price too short (needs {100.0/d*100:.1f}%, band wins {rate*100:.1f}%)"}
    if ev < BET_MIN_EV:
        return {"bet": False, "ev": ev, "rate": rate,
                "reason": f"only {ev*100:.1f}% EV, under the {BET_MIN_EV*100:.0f}% cushion"}
    return {"bet": True, "ev": ev, "rate": rate,
            "reason": f"{ev*100:.1f}% EV at {int(float(price)):+d}"}

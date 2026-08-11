#!/usr/bin/env python3
"""
scripts/ev_gate_preview.py — show what the EV gate would do BEFORE enabling it.

Read-only. Touches no data, no DB, no deploy. Run this, read the table, then
decide whether to set EV_GATE=1 on Railway.

    python3 scripts/ev_gate_preview.py                 # today's live board
    python3 scripts/ev_gate_preview.py --demo          # baked-in 2026-08-11 board

Why this exists: the gate uses Platt-calibrated confidence, and the calibration
was fitted on 121 graded picks. That is a small sample. Look at the sensitivity
table before trusting one setting.
"""
import argparse, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from model.mlb_picks import calibrated_conf, honest_ev, _price_for
from model.value import value_for_pick, american_to_decimal

# The real published board from 2026-08-11 (from the consensus-db dossier).
DEMO = [
    ("New York Yankees ML",   0.742, -128,  116, "home", "New York Yankees",   "Seattle Mariners",    "New York Yankees"),
    ("Houston Astros ML",     0.699, -194,  163, "away", "Houston Astros",     "Houston Astros",      "San Francisco Giants"),
    ("Atlanta Braves ML",     0.685, -124,  112, "home", "Atlanta Braves",     "New York Mets",       "Atlanta Braves"),
    ("Texas Rangers ML",      0.685, -142,  121, "away", "Texas Rangers",      "Texas Rangers",       "Los Angeles Angels"),
    ("Milwaukee Brewers ML",  0.654, -135,  122, "away", "Milwaukee Brewers",  "Milwaukee Brewers",   "San Diego Padres"),
    ("Chicago White Sox ML",  0.645, -155,  139, "home", "Chicago White Sox",  "Cincinnati Reds",     "Chicago White Sox"),
    ("Detroit Tigers ML",     0.641, -121,  109, "home", "Detroit Tigers",     "Cleveland Guardians", "Detroit Tigers"),
    ("Tampa Bay Rays ML",     0.628, -163,  138, "away", "Tampa Bay Rays",     "Tampa Bay Rays",      "Athletics"),
    ("Miami Marlins ML",      0.611,  102, -113, "home", "Miami Marlins",      "Pittsburgh Pirates",  "Miami Marlins"),
    ("Chicago Cubs ML",       0.585, -169,  142, "away", "Chicago Cubs",       "Chicago Cubs",        "Washington Nationals"),
]


def demo_picks():
    out = []
    for label, conf, pp, po, side, team, away, home in DEMO:
        g = {"away_team": away, "home_team": home,
             "ml_away_odds": pp if side == "away" else po,
             "ml_home_odds": pp if side == "home" else po}
        p = {"type": "ML", "label": label, "conf": conf,
             "side": side, "team": team, "game_data": g}
        p["value"] = value_for_pick(p)
        out.append(p)
    return out


def live_picks():
    from model.mlb_model import MLBModel
    from model.mlb_picks import generate_picks
    m = MLBModel(); m.load()
    games, _ = m.score_today()
    return generate_picks(games)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true", help="use the baked-in 2026-08-11 board")
    a = ap.parse_args()

    picks = demo_picks() if a.demo else live_picks()
    gated = [p for p in picks if p.get("type") in ("ML", "TOTAL")]
    if not gated:
        print("No ML/TOTAL picks on the board."); return

    print("=" * 78)
    print("  PER-PICK")
    print("=" * 78)
    print(f"{'PICK':28s} {'conf':>6s} {'cal':>6s} {'price':>7s} {'brkeven':>8s} {'EV':>8s}  verdict")
    print("-" * 78)
    favs = 0
    for p in sorted(gated, key=lambda x: -x["conf"]):
        h = honest_ev(p)
        pr = _price_for(p)
        if pr is not None and float(pr) < 0:
            favs += 1
        d = american_to_decimal(pr)
        be = (1.0 / d * 100) if d else float("nan")
        cal = (h["cal_p"] or 0) * 100
        ev = h["ev"]
        print(f"{p.get('label','')[:28]:28s} {p['conf']*100:5.1f}% {cal:5.1f}% "
              f"{(float(pr) if pr else float('nan')):+7.0f} {be:7.1f}% "
              f"{(ev if ev is not None else float('nan')):+8.3f}  "
              f"{'PUBLISH' if h['passes'] else 'drop'}")

    print()
    print(f"  favorites (negative price): {favs}/{len(gated)}")
    print("  A model that only bets favorites needs its confidence to be RIGHT,")
    print("  because the price already demands a high win rate to break even.")

    print()
    print("=" * 78)
    print("  SENSITIVITY — how many survive at each setting")
    print("=" * 78)
    print("  min EV is the EV_GATE_MIN_EV env var. cal off = trust raw conf (not advised).")
    print()
    print(f"  {'min EV':>8s} | {'calibrated':>11s} | {'raw conf':>9s}")
    print("  " + "-" * 34)
    for thr in (-0.10, -0.05, -0.02, 0.0, 0.02, 0.05):
        n_cal = n_raw = 0
        for p in gated:
            d = american_to_decimal(_price_for(p))
            if not d:
                continue
            cp = calibrated_conf(p["conf"]); rp = p["conf"]
            if cp * (d - 1) - (1 - cp) > thr: n_cal += 1
            if rp * (d - 1) - (1 - rp) > thr: n_raw += 1
        print(f"  {thr:+8.2f} | {n_cal:>7d}/{len(gated):<3d} | {n_raw:>5d}/{len(gated):<3d}")

    print()
    print("  Reading this: if the calibrated column is 0 at min EV 0.00, the model")
    print("  had no genuinely +EV play on this board. That is a legitimate answer,")
    print("  not a broken gate. Publishing nothing beats publishing -EV.")
    print()
    print("  Refit the calibration monthly:")
    print("    python3 fit_calibration.py --database-url \"...\" --since 2026-07-21")
    print("  then override with CAL_A / CAL_B env vars.")


if __name__ == "__main__":
    main()

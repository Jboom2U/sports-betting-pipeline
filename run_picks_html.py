"""
run_picks_html.py
Generates a self-contained HTML dashboard for today's MLB betting picks.

Usage:
    python run_picks_html.py                  # Today's picks
    python run_picks_html.py --date 2026-04-15
    python run_picks_html.py --no-open        # Don't auto-open browser
"""

import sys, os, json, logging, argparse, webbrowser, requests
from datetime import datetime
from zoneinfo import ZoneInfo

# Baseball days are ET days. Railway runs Python in UTC, so a bare
# datetime.now() rolls to tomorrow at 8pm ET and the pick path then looks for
# tomorrow's dated files. Always resolve "today" in ET.
_ET_TZ = ZoneInfo("America/New_York")


def _today_et() -> str:
    """Today's date in ET, as YYYY-MM-DD."""
    return datetime.now(_ET_TZ).strftime("%Y-%m-%d")


sys.path.insert(0, os.path.dirname(__file__))
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

PICKS_DIR = os.path.join(os.path.dirname(__file__), "picks")


# ─────────────────────────────────────────────────────────────────────────────
# DATA PREP
# ─────────────────────────────────────────────────────────────────────────────
def fetch_live_scores(date: str) -> list:
    """
    Pull today's completed and in-progress scores directly from the MLB Stats API.
    Returns fresh data regardless of when the pipeline last ran.
    """
    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {
        "sportId":  1,
        "date":     date,
        "hydrate":  "linescore,decisions",
        "gameType": "R",
    }
    try:
        resp = requests.get(url, params=params,
                            headers={"User-Agent": "mlb-betting-pipeline/1.0"},
                            timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Live scores fetch failed: {e}")
        return []

    results = []
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            status     = game.get("status", {})
            abstract   = status.get("abstractGameState", "")
            detailed   = status.get("detailedState", "")

            # Only include Final or In Progress
            if abstract not in ("Final", "Live"):
                continue

            away       = game["teams"]["away"]
            home       = game["teams"]["home"]
            linescore  = game.get("linescore", {})
            inning     = linescore.get("currentInning", 9)
            inning_half= linescore.get("inningHalf", "")

            away_score = away.get("score", 0) or 0
            home_score = home.get("score", 0) or 0

            results.append({
                "away_team":   away["team"]["name"],
                "home_team":   home["team"]["name"],
                "away_score":  away_score,
                "home_score":  home_score,
                "status":      abstract,
                "detailed":    detailed,
                "inning":      inning,
                "inning_half": inning_half,
            })

    log.info(f"Live scores fetched: {len(results)} games")
    return results


def prep_scores_ticker(scores: list) -> list:
    """Prepare live/completed game scores for the ticker."""
    def city(name):
        parts = name.split()
        return parts[0] if parts else name

    out = []
    for s in scores:
        away   = s.get("away_team", "")
        home   = s.get("home_team", "")
        ascore = s.get("away_score", 0)
        hscore = s.get("home_score", 0)
        if not away or not home:
            continue

        is_live   = s.get("status") == "Live"
        inning    = s.get("inning", 9)
        inh       = s.get("inning_half", "")
        label     = f"{inh[:3]} {inning}" if is_live else "Final"
        if inning and int(inning) > 9 and not is_live:
            label = f"F/{inning}"

        out.append({
            "away":       away,
            "home":       home,
            "away_city":  city(away),
            "home_city":  city(home),
            "away_score": str(ascore),
            "home_score": str(hscore),
            "is_live":    is_live,
            "label":      label,
        })
    return out


def prep_picks(picks, kalshi_data: dict = None):
    """
    Serialize picks for HTML embedding.
    Adds warning flags and Kalshi consensus signals when available.

    Warning flags:
      tbd_sp        — either starting pitcher is TBD
      thin_edge     — TOTAL pick with <0.8 run edge
      heavy_fav     — ML pick where model conf implies >67% (~-200 or worse)
      unconfirmed   — lineup not confirmed at time of HTML generation

    Kalshi fields:
      kalshi_prob   — Kalshi implied win probability for picked team (0-100)
      kalshi_signal — AGREE / DISAGREE / NEUTRAL
    """
    import re as _re
    out = []
    for p in picks:
        gd      = p.get("game_data", {})
        ptype   = p["type"]
        conf    = round(p["conf"] * 100, 1)
        reasoning = p["reasoning"]

        # ── Warning flags ─────────────────────────────────────────────────
        away_sp = gd.get("away_sp", "TBD") or "TBD"
        home_sp = gd.get("home_sp", "TBD") or "TBD"
        tbd_sp  = (away_sp.strip().upper() == "TBD" or
                   home_sp.strip().upper() == "TBD")

        # Thin edge: TOTAL picks where run edge < 0.8
        thin_edge = False
        if ptype == "TOTAL":
            m = _re.search(r'\(([\d.]+)\s*run\s*edge\)', reasoning, _re.IGNORECASE)
            if m:
                edge = float(m.group(1))
                thin_edge = edge < 0.8

        # Heavy favorite: ML conf implies worse than -200 odds (>67% implied)
        heavy_fav = (ptype == "ML" and p["conf"] >= 0.70)

        # Favorite tier — applies to every pick
        #   HEAVY  : conf >= 68%  (equiv ~-210 or worse ML line)
        #   MEDIUM : conf 58-68%  (equiv ~-138 to -210)
        #   NEUTRAL: conf < 58%   (near pick'em territory)
        if p["conf"] >= 0.68:
            fav_tier = "HEAVY"
        elif p["conf"] >= 0.58:
            fav_tier = "MEDIUM"
        else:
            fav_tier = "NEUTRAL"

        # Unconfirmed lineup
        unconfirmed = not bool(gd.get("lineup_confirmed", False))

        # ── Kalshi signal ─────────────────────────────────────────────────
        kalshi_prob   = None
        kalshi_signal = None
        if kalshi_data:
            away_team = gd.get("away_team", "")
            home_team = gd.get("home_team", "")
            key = tuple(sorted([away_team, home_team]))
            kd  = kalshi_data.get(key)
            # load_kalshi_for_date() defaults missing/unparseable probs to exactly
            # 0.5/0.5 as a "no data" sentinel. A 0.5/0.5 is NOT a market opinion —
            # treating it as real made every card whose Kalshi data was missing or
            # stale (e.g. after games start, or unpriced future markets) show
            # "Kalshi 50% Market Disagrees" against the model. Skip the sentinel.
            if kd and abs(float(kd.get("kalshi_away_prob", 0.5)) - 0.5) < 1e-6 \
                   and abs(float(kd.get("kalshi_home_prob", 0.5)) - 0.5) < 1e-6:
                kd = None
            if kd:
                # Determine Kalshi prob for the PICKED team
                if ptype in ("ML", "RL"):
                    team_picked = p["team"].lower()
                    if team_picked in kd["away_team"].lower() or \
                       kd["away_team"].lower() in team_picked:
                        kp = kd["kalshi_away_prob"]
                    else:
                        kp = kd["kalshi_home_prob"]
                elif ptype == "TOTAL":
                    # For totals: if model says OVER and Kalshi total_prob > 0.5 → AGREE
                    # Use home team win prob as a proxy (imperfect but informative)
                    kp = kd["kalshi_home_prob"]  # placeholder
                else:
                    kp = None

                if kp is not None:
                    kalshi_prob = round(kp * 100, 1)
                    # Signal
                    model_wp  = p["conf"]
                    diff      = abs(model_wp - kp)
                    same_side = (model_wp > 0.50) == (kp > 0.50)
                    if diff <= 0.04:
                        kalshi_signal = "NEUTRAL"
                    elif same_side:
                        kalshi_signal = "AGREE"
                    else:
                        kalshi_signal = "DISAGREE"

        # ── Kelly Criterion (Half-Kelly at -110 standard juice) ─────────────
        # b = net units won per unit staked; at -110 you win 100 for every 110 risked
        _b = 100 / 110   # 0.9091
        _p = p["conf"]   # raw probability 0-1 (before percentage conversion)
        _kelly_full = (_b * _p - (1 - _p)) / _b
        kelly_pct = round(max(0.0, _kelly_full * 0.5) * 100, 1)   # Half-Kelly

        # ── Market VALUE / EV — de-vigged price awareness (advisory only) ────
        _val = p.get("value") or {}
        if not _val:
            try:
                from model.value import value_for_pick
                _val = value_for_pick(p)
            except Exception:
                _val = {}
        # American price of the actual bet picked (for card display)
        if ptype == "ML":
            _pa = (p.get("side") == "away") or (p["team"] == gd.get("away_team"))
            _pick_price = gd.get("ml_away_odds") if _pa else gd.get("ml_home_odds")
        elif ptype == "RL":
            # Price by the ACTUAL handicap in the label, not by who the market
            # thinks is favored. See the 2026-08-11 fix in mlb_model.py: reading
            # rl_away_price/rl_home_price gave a "+1.5" pick its own "-1.5" price
            # whenever the model and the market disagreed on the favorite.
            _pa   = p["team"] == gd.get("away_team")
            _side = "away" if _pa else "home"
            _hcap = "p15" if "+1.5" in (p.get("label", "") or "") else "m15"
            # NO legacy fallback (removed 2026-08-14) — see model/value.py.
            # The legacy field is a cross-book average, not a quotable price.
            _pick_price = gd.get(f"rl_{_side}_{_hcap}_price")
        elif ptype == "TOTAL":
            _isover = "OVER" in (p.get("label", "").upper())
            _pick_price = gd.get("total_over_price") if _isover else gd.get("total_under_price")
        else:
            _pick_price = None
        # Reject impossible American prices (|p| < 100) — corrupt odds data.
        try:
            if _pick_price is not None and abs(float(_pick_price)) < 100:
                _pick_price = None
        except (TypeError, ValueError):
            _pick_price = None

        # ── HONEST READ (2026-08-11) ──────────────────────────────────────────
        # The displayed conf is systematically inflated: across 121 graded ML
        # picks it averaged 71.9% while realizing 49.6%. cal_conf maps it through
        # the Platt fit onto its historically observed win rate; breakeven is what
        # the ACTUAL price demands. Together they answer the only question that
        # matters on a card — is the honest probability above what I am charged?
        _cal_conf = _breakeven = _honest_ev = None
        _verdict  = ""
        # PER-TYPE ONLY. The Platt fit was estimated on ML picks and is valid for
        # ML picks. It must NOT be applied to RL or TOTAL:
        #   RL   — rl_conf is a Poisson cover probability, already capped at
        #          0.55/0.68. Different quantity, different scale, its own bias.
        #   TOTAL— total_conf comes from abs(exp_total - line)/16 capped at 0.68,
        #          a distance-to-line heuristic, not a win probability at all.
        # All three descend from the same exp_runs, so their errors are correlated
        # but NOT identical, and one curve cannot correct all of them. Until each
        # type has its own fit from fit_calibration.py --per-type, only ML shows a
        # calibrated number. The others say so rather than showing a wrong one.
        try:
            from model.mlb_picks import calibrated_conf as _cc
            from model.value import american_to_decimal as _a2d
            _cal = _cc(_p, ptype)   # returns None for types with no trusted fit (RL)
            _cal_conf = round(_cal * 100, 1) if _cal is not None else None
            _d = _a2d(_pick_price)
            if _d and _cal is not None:
                _breakeven = round(100.0 / _d, 1)
                _honest_ev = round(_cal * (_d - 1.0) - (1.0 - _cal), 4)
                _m = _cal_conf - _breakeven
                if   _m >=  4: _verdict = "PLAYABLE"
                elif _m >=  0: _verdict = "THIN"
                elif _m >= -8: _verdict = "NO EDGE"
                else:          _verdict = "AVOID"
        except Exception:
            pass

        out.append({
            "type":           ptype,
            "label":          p["label"],
            "team":           p["team"],
            "conf":           conf,
            "tier":           p["tier"],
            "game":           p["game"],
            "game_id":        p["game_id"],
            "venue":          p["venue"],
            "reasoning":      reasoning,
            "exp_total":      p["exp_total"],
            "away":           gd.get("away_team", ""),
            "home":           gd.get("home_team", ""),
            # Warning flags
            "tbd_sp":         tbd_sp,
            "thin_edge":      thin_edge,
            "heavy_fav":      heavy_fav,
            "unconfirmed":    unconfirmed,
            "fav_tier":       fav_tier,
            # Kalshi
            "kalshi_prob":    kalshi_prob,
            "kalshi_signal":  kalshi_signal,
            # Kelly Criterion (0 for TOSSUP picks)
            "kelly_pct":      kelly_pct if p["tier"] != "TOSSUP" else 0,
            # Total line range (for line shopping)
            "total_line_min": gd.get("total_line_min"),
            "total_line_max": gd.get("total_line_max"),
            # Opponent info for TOSSUP card split display
            "opp_team":       p.get("opp_team", ""),
            "opp_conf":       round((1 - p["conf"]) * 100, 1),
            # Sportsbook ML odds (American format, from Odds API)
            "ml_away_odds":   gd.get("ml_away_odds"),
            "ml_home_odds":   gd.get("ml_home_odds"),
            "narrative":      p.get("narrative", ""),
            # ── Market VALUE / EV (advisory — price-awareness, not a filter) ──
            "cal_conf":     _cal_conf,
            "breakeven":    _breakeven,
            "honest_ev":    _honest_ev,
            "read_verdict": _verdict,
            "value_tag":    _val.get("tag", ""),
            "value_ev":     _val.get("ev"),
            "value_edge":   _val.get("edge"),
            "market_prob":  _val.get("market_prob"),
            "value_chalk":  _val.get("chalk", False),
            "pick_price":   _pick_price,
            "books":        gd.get("books") or {},
        })
    return out


def prep_games(scored):
    from datetime import timezone as _tz, timedelta as _td
    _EDT = _tz(_td(hours=-4))   # April–Oct = EDT (UTC-4)
    out = []
    for g in scored:
        t = g.get("game_time_utc", "")
        time_str = ""
        if t:
            try:
                from datetime import datetime as _dt
                utc_dt = _dt.fromisoformat(t.replace("Z", "+00:00"))
                et_dt  = utc_dt.astimezone(_EDT)
                hour   = et_dt.hour % 12 or 12
                ampm   = "AM" if et_dt.hour < 12 else "PM"
                time_str = f"{hour}:{et_dt.minute:02d} {ampm} ET"
            except Exception:
                time_str = t[11:16] + " ET"
        pf = g["park_runs"]
        if pf >= 112:   park_tag = "Extreme Hitter Park"
        elif pf >= 106: park_tag = "Hitter-Friendly"
        elif pf <= 96:  park_tag = "Pitcher-Friendly"
        else:           park_tag = "Neutral"

        def era_str(v): return f"{float(v):.2f}" if v else "N/A"

        out.append({
            "game_id":   g.get("game_id", ""),
            "away":      g["away_team"],
            "home":      g["home_team"],
            "venue":     g["venue"],
            "time":      time_str,
            "park_runs": pf,
            "park_tag":  park_tag,
            "away_sp":   g.get("away_sp", "TBD"),
            "home_sp":   g.get("home_sp", "TBD"),
            "away_era":  era_str(g.get("away_sp_era_adj")),
            "home_era":  era_str(g.get("home_sp_era_adj")),
            "away_fip":  era_str(g.get("away_sp_fip")),
            "home_fip":  era_str(g.get("home_sp_fip")),
            "away_rpg":  round(g.get("away_rpg", 4.5), 2),
            "home_rpg":  round(g.get("home_rpg", 4.5), 2),
            "away_ops":  g.get("away_ops") or "N/A",
            "home_ops":  g.get("home_ops") or "N/A",
            "away_form": round(g.get("away_form_rpg", 4.5), 1),
            "home_form": round(g.get("home_form_rpg", 4.5), 1),
            "exp_away":  g["exp_away"],
            "exp_home":  g["exp_home"],
            "exp_total": g["exp_total"],
            "away_wp":   round(g["away_wp"] * 100, 1),
            "home_wp":   round(g["home_wp"] * 100, 1),
            "total_pick":g["total_pick"],
            "total_line":g["total_line"],
            "total_conf":round(g["total_conf"] * 100, 1),
            "rl_pick":   g.get("rl_pick", ""),
            "rl_team":   g.get("rl_team", ""),
            "rl_conf":   round(g.get("rl_conf", 0) * 100, 1),
            # Weather
            "weather_flag":   g.get("weather_flag", "NORMAL"),
            "wind_label":     g.get("wind_label", ""),
            "wind_speed":     round(g.get("wind_speed", 0) or 0, 1),
            "wind_component": round(g.get("wind_component", 0) or 0, 1),
            "temp_f":         round(g.get("temp_f", 70) or 70, 0),
            "precip_prob":    round(g.get("precip_prob", 0) or 0, 0),
            "has_roof":       bool(g.get("has_roof")),
            # Bullpen
            "away_bp_era":    round(g.get("away_bp_era", 4.20) or 4.20, 2),
            "home_bp_era":    round(g.get("home_bp_era", 4.20) or 4.20, 2),
            "lineup_confirmed": bool(g.get("lineup_confirmed")),
            "away_lineup_ops":  g.get("away_lineup_ops"),
            "home_lineup_ops":  g.get("home_lineup_ops"),
        })
    return out


def load_standings() -> dict:
    """Load latest W-L record for every team from the standings master."""
    path = os.path.join(os.path.dirname(__file__), "data", "clean",
                        "mlb_standings_master.csv")
    records: dict[str, dict] = {}
    if not os.path.exists(path):
        return records
    import csv as _csv
    with open(path, encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            team = row.get("team", "").strip()
            if team:
                existing = records.get(team)
                if not existing or row.get("game_date", "") >= existing.get("game_date", ""):
                    records[team] = row
    return records


def prep_schedule_view(all_games: list, live_scores: list, standings: dict) -> list:
    """
    Build a full today's-games list combining schedule data,
    live API status, and team records.

    all_games  — all scored game dicts for the date (including already-started)
    live_scores — from fetch_live_scores() with status/score/inning
    standings  — team_name -> standings row
    """
    # Index live scores by (away, home) for quick lookup
    live_idx: dict[tuple, dict] = {}
    for s in live_scores:
        key = (s.get("away_team", ""), s.get("home_team", ""))
        live_idx[key] = s

    def record_str(team: str) -> str:
        row = standings.get(team, {})
        if not row:
            # Partial name match
            tl = team.lower()
            for k, v in standings.items():
                if k.lower() in tl or tl in k.lower():
                    row = v
                    break
        if row:
            w = row.get("wins", "?")
            l = row.get("losses", "?")
            streak = row.get("streak", "")
            last10 = row.get("last_10", "")
            return {"record": f"{w}-{l}", "streak": streak, "last10": last10}
        return {"record": "—", "streak": "", "last10": ""}

    out = []
    for g in all_games:
        away = g.get("away_team", "")
        home = g.get("home_team", "")
        key  = (away, home)
        live = live_idx.get(key, {})

        # Status
        if live.get("status") == "Final":
            status = "Final"
        elif live.get("status") == "Live":
            inh   = live.get("inning_half", "")[:3]
            inn   = live.get("inning", "")
            status = f"Live — {inh} {inn}"
        else:
            status = "Upcoming"

        away_rec = record_str(away)
        home_rec = record_str(home)

        out.append({
            "game_id":      g.get("game_id", ""),
            "away_team":    away,
            "home_team":    home,
            "away_record":  away_rec["record"],
            "away_streak":  away_rec["streak"],
            "away_last10":  away_rec["last10"],
            "home_record":  home_rec["record"],
            "home_streak":  home_rec["streak"],
            "home_last10":  home_rec["last10"],
            "venue":        g.get("venue", ""),
            "game_time_utc": g.get("game_time_utc", ""),
            "status":       status,
            "away_score":   live.get("away_score", ""),
            "home_score":   live.get("home_score", ""),
            "away_sp":      g.get("away_sp", "TBD"),
            "home_sp":      g.get("home_sp", "TBD"),
        })

    # Sort by game time
    out.sort(key=lambda x: x.get("game_time_utc", ""))
    return out


def prep_team_schedule(today: str) -> dict:
    """
    Build a team → next_game lookup covering today + next 7 days.
    Used by the Teams tab to always show each team's upcoming game
    even when they don't play today.
    Returns dict keyed by team name with game details.
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    import csv as _csv

    _EDT = _tz(_td(hours=-4))
    today_dt  = _dt.fromisoformat(today)
    cutoff_dt = today_dt + _td(days=8)

    sched_path = os.path.join(os.path.dirname(__file__), "data", "clean",
                              "mlb_schedule_master.csv")
    if not os.path.exists(sched_path):
        return {}

    # Collect all upcoming games in the window, sorted by date
    games = []
    with open(sched_path, encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            gdate = row.get("game_date", "")
            if not gdate:
                continue
            try:
                gdt = _dt.fromisoformat(gdate)
            except ValueError:
                continue
            if today_dt <= gdt < cutoff_dt:
                # Convert UTC time to ET for display
                utc_str = row.get("game_time_utc", "")
                time_str = ""
                day_label = ""
                try:
                    utc_dt = _dt.fromisoformat(utc_str.replace("Z", "+00:00"))
                    et_dt  = utc_dt.astimezone(_EDT)
                    hr     = et_dt.hour % 12 or 12
                    ampm   = "AM" if et_dt.hour < 12 else "PM"
                    time_str = f"{hr}:{et_dt.minute:02d} {ampm} ET"
                    delta = (gdt.date() - today_dt.date()).days
                    if delta == 0:
                        day_label = "Today"
                    elif delta == 1:
                        day_label = "Tomorrow"
                    else:
                        day_label = et_dt.strftime("%A, %b %-d")
                except Exception:
                    delta = (gdt.date() - today_dt.date()).days
                    day_label = "Today" if delta == 0 else ("Tomorrow" if delta == 1 else gdate)

                games.append({
                    "game_date":  gdate,
                    "game_id":    row.get("game_id", ""),
                    "away_team":  row.get("away_team", ""),
                    "home_team":  row.get("home_team", ""),
                    "away_sp":    row.get("away_probable_pitcher", "TBD") or "TBD",
                    "home_sp":    row.get("home_probable_pitcher", "TBD") or "TBD",
                    "venue":      row.get("venue", ""),
                    "time_str":   time_str,
                    "day_label":  day_label,
                    "is_today":   gdate == today,
                })

    # Sort by date then time
    games.sort(key=lambda x: x["game_date"])

    # Build team → first upcoming game lookup
    team_next: dict[str, dict] = {}
    for g in games:
        for team in (g["away_team"], g["home_team"]):
            if team and team not in team_next:
                team_next[team] = g

    return team_next


def compute_high_conf_rule() -> dict:
    """
    Derive the "high confidence" threshold from the model's OWN graded record
    instead of hardcoding a number.

    For each pick_type, walk 5-point confidence bands and find the lowest band
    where the model has actually beaten break-even on a usable sample. A band
    qualifies when n >= MIN_N and win rate >= TARGET. Everything at or above
    that confidence for that pick type gets flagged.

    Returns {"ML": 75.0, ...} plus a per-type record string for the tooltip.
    Falls back to ML>=75 if the DB is unavailable — that band is the only one
    that beat break-even across the pre-2026-07-21 sample.
    """
    BREAK_EVEN = 52.38
    TARGET     = 56.0   # margin above break-even so noise doesn't qualify a band
    MIN_N      = 30

    fallback = {"rule": {"ML": 75.0}, "record": {"ML": "ML 75%+ (pre-fix baseline)"},
                "rule_wide": {}, "record_wide": {}}

    try:
        from db.connection import db_conn
        with db_conn() as conn:
            if conn is None:
                return fallback
            cur = conn.cursor()
            cur.execute(
                "SELECT pick_type, conf, actual_result FROM picks "
                "WHERE actual_result IN ('WIN','LOSS') AND conf IS NOT NULL"
            )
            rows = cur.fetchall()
    except Exception as e:
        log.warning(f"high-conf rule: DB query failed, using fallback: {e}")
        return fallback

    if not rows:
        return fallback

    def norm(c):
        c = float(c or 0)
        return c * 100 if c <= 1 else c

    rule, record = {}, {}
    rule_wide, record_wide = {}, {}
    for ptype in sorted({r[0] for r in rows if r[0]}):
        sub = [(norm(r[1]), r[2]) for r in rows if r[0] == ptype]
        # Two tiers, both derived from the model's own record:
        #   ELITE — threshold with the highest win rate (fewest, strongest picks)
        #   WIDE  — lowest threshold still clearing TARGET (more volume, thinner)
        # These differ meaningfully. On pre-fix data ML>=75 nets 65.5% (n=87)
        # while ML>=70 nets 58.6% (n=145), because the 70-75 sub-band alone loses
        # at 48.3% and is carried by the bands above it. Both are profitable, so
        # surface both and let the bet size reflect the difference.
        elite = wide = None
        for thresh in range(90, 49, -5):
            sel = [r for r in sub if r[0] >= thresh]
            n = len(sel)
            if n < MIN_N:
                continue
            wins = sum(1 for r in sel if r[1] == "WIN")
            wr = wins / n * 100
            if wr >= TARGET:
                if elite is None or wr > elite[2]:
                    elite = (float(thresh), n, wr)
                wide = (float(thresh), n, wr)   # keeps descending to the lowest pass
        if elite:
            rule[ptype] = elite[0]
            record[ptype] = f"{ptype} {elite[0]:.0f}%+: {elite[1]} picks, {elite[2]:.1f}%"
        if wide and (not elite or wide[0] < elite[0]):
            rule_wide[ptype] = wide[0]
            record_wide[ptype] = f"{ptype} {wide[0]:.0f}%+: {wide[1]} picks, {wide[2]:.1f}%"

    if not rule and not rule_wide:
        log.info("high-conf rule: no pick type clears target — no badges will show")
        return {"rule": {}, "record": {}, "rule_wide": {}, "record_wide": {}}

    log.info(f"high-conf rule from {len(rows)} graded picks: elite={record} wide={record_wide}")
    return {"rule": rule, "record": record,
            "rule_wide": rule_wide, "record_wide": record_wide}


# Prop types suppressed from the BETTABLE Player Props surface. These are the
# OVER-only, fixed-0.5x-projection, no-real-price batter props — they are graded
# against a FICTIONAL line (0.5x the model's own projection), not a sportsbook
# number, so the record measures beating a made-up line and they structurally lose
# (HR ~7%, SB ~17%, RBI ~26%, R ~32% over the post-fix window). Pruned 2026-08-02.
# KEPT bettable: K (real Pinnacle line + EV, K UNDER 62%) and HITS (the one batter
# prop showing a positive read, 15/19 — small sample, on watch).
# Suppression is DISPLAY-ONLY: HR Watch (raw props) is unaffected and grading
# continues (save loops read raw props), so the record keeps accruing to revisit.
# The real fix is a free/affordable batter-prop LINE source; then these become real
# bets we can price + calibrate like K props, and can come off this list.
# HITS added 2026-08-11. It was the LAST batter prop still presented as a real
# bet while being scored against a hardcoded 0.5 line that no book offered.
# score_hits_prop (model/mlb_props_model.py:276) sets "line": 0.5 unconditionally
# and computes P(at least one hit) from a Poisson with lambda ~1.0, which lands
# most regulars at 65-70%. Real books price Over 0.5 Hits around -250 to -350,
# where 68% is a LOSING bet — but the record logged it as a win, which is exactly
# why the props column looked healthy while producing nothing. Same disease as
# the fabricated totals line: grading against a number that was never available.
# K props are unaffected — they run on real Pinnacle lines with real prices and
# stay bettable. Suppression is DISPLAY-ONLY: HITS stays visible in the Player
# Props tab as a research projection, and grading still accrues underneath.
# Remove from this set once a real batter-prop line source is wired in.
SUPPRESS_BETTABLE_PROPS = {"HR", "SB", "RBI", "R", "TB", "HITS"}


def prep_props(props: list) -> list:
    """Serialize player props for HTML embedding."""
    out = []
    for p in props:
        conf_raw = p["confidence"]
        ptype    = p["prop_type"]
        # Suppressed types stay OFF the bettable/Best-Bets surface, but remain VISIBLE
        # in the Player Props tab as research PROJECTIONS (flagged, not hidden) so they
        # can still be looked at and researched. They're graded vs a fictional 0.5x line.
        # CONDITIONAL on a real book line (2026-08-11), not on a static type list.
        # HR and TB now have REAL Pinnacle lines and two-way prices, so when a
        # given player is actually listed by the book that prop becomes a real
        # bet and should be surfaced. When the book does not list him, the same
        # prop type falls back to the model's invented line and stays a research
        # projection. So suppression is per-PICK, not per-TYPE.
        # `bettable` is set by reprice_prop_with_book only when a real line and
        # both prices resolved AND it cleared the price floor and mirage guard.
        _has_real_line = bool(p.get("book_line") is not None and p.get("bettable"))
        projection_only = (ptype in SUPPRESS_BETTABLE_PROPS) and not _has_real_line

        # Lineup/starter flags (carried from score_all_props if present)
        unconfirmed = p.get("lineup_unconfirmed", False)
        tbd_sp      = p.get("tbd_sp", False)

        # Edge strength — shown as fav-tier equivalent
        # Each prop type has calibrated thresholds matching the model
        if ptype == "HR":
            if conf_raw >= 0.20:   fav_tier = "HEAVY"
            elif conf_raw >= 0.15: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        elif ptype == "SB":
            if conf_raw >= 0.28:   fav_tier = "HEAVY"
            elif conf_raw >= 0.20: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        elif ptype in ("RBI", "R"):
            if conf_raw >= 0.50:   fav_tier = "HEAVY"
            elif conf_raw >= 0.42: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        elif ptype == "TB":
            if conf_raw >= 0.62:   fav_tier = "HEAVY"
            elif conf_raw >= 0.55: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"
        else:   # HITS, K — standard scale
            if conf_raw >= 0.68:   fav_tier = "HEAVY"
            elif conf_raw >= 0.58: fav_tier = "MEDIUM"
            else:                  fav_tier = "NEUTRAL"

        out.append({
            "prop_type":    ptype,
            "projection_only": projection_only,
            "player_name":  p["player_name"],
            "player_id":    p.get("player_id"),
            "line":         p["line"],
            "proj":         p["proj"],
            "conf":         round(conf_raw * 100, 1),
            "tier":         p["tier"],
            "game":         p.get("game", ""),
            "game_id":      p.get("game_id", ""),
            "away_team":    p.get("away_team", ""),
            "home_team":    p.get("home_team", ""),
            "side":         p.get("side", ""),
            "batting_order":p.get("batting_order", ""),
            "reasoning":    p.get("reasoning", ""),
            "fav_tier":     fav_tier,
            "unconfirmed":  unconfirmed,
            "tbd_sp":       tbd_sp,
            "projected":    p.get("projected", False),
            # Real sportsbook (Pinnacle) line prices for K props, shown on card.
            "pick_side":    p.get("pick_side"),
            "over_price":   p.get("over_price"),
            "under_price":  p.get("under_price"),
            "ev":           p.get("ev"),
            # Real-book-line fields (None when the book does not list this prop)
            "book_line":    p.get("book_line"),
            "model_line":   p.get("model_line"),
            "market_prob":  p.get("market_prob"),
            "bettable":     p.get("bettable", False),
            "blocked":      p.get("blocked"),
            "price":        p.get("price"),
        })
    return out


def prep_parlays(parlays):
    out = []
    for p in parlays:
        out.append({
            "n_legs":   p["n_legs"],
            "combined": round(p["combined"] * 100, 1),
            "payout":   p["payout"],
            "book_odds": p.get("book_odds"),
            "legs": [
                {"label": l["label"], "conf": round(l["conf"] * 100, 1),
                 "tier": l["tier"], "game": l["game"]}
                for l in p["legs"]
            ],
        })
    return out


def prep_thematic_parlays(parlays):
    out = []
    for p in parlays:
        out.append({
            "thesis":      p["thesis"],
            "thesis_desc": p["thesis_desc"],
            "n_legs":      p["n_legs"],
            "combined":    round(p["combined"] * 100, 1),
            "payout":      p["payout"],
            "book_odds":    p.get("book_odds"),
            "legs": [
                {"label": l["label"], "conf": round(l["conf"] * 100, 1),
                 "tier": l["tier"], "game": l["game"]}
                for l in p["legs"]
            ],
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# KALSHI + ANALYSIS LOADERS
# ─────────────────────────────────────────────────────────────────────────────

def load_kalshi(date: str) -> dict:
    """
    Load Kalshi market data for date. Returns dict keyed by sorted team tuple.
    Returns empty dict if Kalshi scraper hasn't been configured/run yet.
    """
    try:
        from scrapers.mlb_kalshi_scraper import load_kalshi_for_date
        data = load_kalshi_for_date(date)
        if data:
            log.info(f"Kalshi data loaded: {len(data)} markets")
        return data
    except Exception:
        return {}


def load_line_movement(date: str) -> list:
    """
    Load line movement for date and merge in Kalshi movement.
    Returns list of movement dicts — includes games where EITHER sportsbooks
    OR Kalshi show meaningful movement (STEAM / DRIFT).
    """
    import csv as _csv
    clean_dir = os.path.join(os.path.dirname(__file__), "data", "clean")

    def _n(v):
        try: return float(v)
        except: return None

    # ── Sportsbook movement ──────────────────────────────────────────────────
    sb_map = {}
    sb_path = os.path.join(clean_dir, f"mlb_line_movement_{date}.csv")
    if os.path.exists(sb_path):
        try:
            with open(sb_path, encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
            latest = {}
            for r in rows:
                k = (r.get("away_team",""), r.get("home_team",""))
                if k not in latest or r.get("snap2_time","") > latest[k].get("snap2_time",""):
                    latest[k] = r
            for k, r in latest.items():
                sb_map[k] = {
                    "away":          r.get("away_team",""),
                    "home":          r.get("home_team",""),
                    "ml_away_open":  _n(r.get("ml_away_open")),
                    "ml_away_now":   _n(r.get("ml_away_now")),
                    "ml_away_move":  _n(r.get("ml_away_move")),
                    "ml_home_open":  _n(r.get("ml_home_open")),
                    "ml_home_now":   _n(r.get("ml_home_now")),
                    "ml_home_move":  _n(r.get("ml_home_move")),
                    "total_open":    _n(r.get("total_open")),
                    "total_now":     _n(r.get("total_now")),
                    "total_move":    _n(r.get("total_move")),
                    "ml_signal":     r.get("ml_signal", "NO_DATA"),
                    "total_signal":  r.get("total_signal", "NO_DATA"),
                    "sharp_side":    r.get("sharp_side",""),
                    "snap1_time":    r.get("snap1_time",""),
                    "snap2_time":    r.get("snap2_time",""),
                    # Kalshi fields default to None until merged
                    "kalshi_away_open":  None,
                    "kalshi_away_now":   None,
                    "kalshi_away_move":  None,
                    "kalshi_home_open":  None,
                    "kalshi_home_now":   None,
                    "kalshi_home_move":  None,
                    "kalshi_signal":     "NO_DATA",
                    "kalshi_sharp_side": "",
                }
        except Exception as e:
            log.warning(f"Sportsbook movement load failed: {e}")

    # ── Kalshi movement ──────────────────────────────────────────────────────
    try:
        from scrapers.mlb_kalshi_scraper import load_kalshi_movement as _load_km
        km_rows = _load_km(date)
        for r in km_rows:
            k = (r.get("away_team",""), r.get("home_team",""))
            kalshi_fields = {
                "kalshi_away_open":  _n(r.get("kalshi_away_open")),
                "kalshi_away_now":   _n(r.get("kalshi_away_now")),
                "kalshi_away_move":  _n(r.get("kalshi_away_move")),
                "kalshi_home_open":  _n(r.get("kalshi_home_open")),
                "kalshi_home_now":   _n(r.get("kalshi_home_now")),
                "kalshi_home_move":  _n(r.get("kalshi_home_move")),
                "kalshi_signal":     r.get("kalshi_signal", "STABLE"),
                "kalshi_sharp_side": r.get("kalshi_sharp_side", ""),
            }
            if k in sb_map:
                sb_map[k].update(kalshi_fields)
            else:
                # Kalshi moved but sportsbooks didn't — still worth surfacing
                sb_map[k] = {
                    "away":  r.get("away_team",""),
                    "home":  r.get("home_team",""),
                    "ml_away_open": None, "ml_away_now": None, "ml_away_move": None,
                    "ml_home_open": None, "ml_home_now": None, "ml_home_move": None,
                    "total_open": None,   "total_now":   None, "total_move":   None,
                    "ml_signal":    "NO_DATA",
                    "total_signal": "NO_DATA",
                    "sharp_side":   "",
                    "snap1_time":   r.get("snap1_time",""),
                    "snap2_time":   r.get("snap2_time",""),
                    **kalshi_fields,
                }
    except Exception as e:
        log.warning(f"Kalshi movement merge failed: {e}")

    # ── Filter to games with at least one meaningful signal ──────────────────
    out = []
    for r in sb_map.values():
        sb_notable = (r.get("ml_signal") in ("STEAM","DRIFT") or
                      r.get("total_signal") in ("STEAM","DRIFT"))
        k_notable  = r.get("kalshi_signal") in ("STEAM","DRIFT")
        if sb_notable or k_notable:
            out.append(r)

    log.info(f"Line movement loaded: {len(out)} games with notable moves (sportsbook + Kalshi)")
    return out


def _build_yesterday_from_db(date: str) -> dict:
    """Build a yesterday_data dict from graded picks in the DB (fallback for missing JSON)."""
    try:
        from db.picks_store import get_picks
        from datetime import datetime as _dt, timedelta as _td
        rows = []
        for days_back in range(1, 4):
            check = (_dt.strptime(date, "%Y-%m-%d") - _td(days=days_back)).strftime("%Y-%m-%d")
            r = get_picks(check)
            graded = [p for p in r if p.get("actual_result") in ("WIN","LOSS","PUSH")]
            if graded:
                rows = graded
                check_date = check
                break
        if not rows:
            return {}
        # Build minimal metrics structure
        wins = sum(1 for p in rows if p["actual_result"]=="WIN")
        losses = sum(1 for p in rows if p["actual_result"]=="LOSS")
        pushes = sum(1 for p in rows if p["actual_result"]=="PUSH")
        denom = wins + losses
        wr = round(wins/denom, 3) if denom > 0 else None
        # Half-Kelly profit estimate at -110
        def _profit(p):
            conf = float(p.get("conf") or 0)
            if conf <= 1: conf *= 100
            edge = max(0, conf - 52.4) / 100
            kelly = edge / 0.909 * 0.5  # half Kelly
            stake = max(0.01, kelly)
            if p["actual_result"]=="WIN": return stake * 0.909
            if p["actual_result"]=="LOSS": return -stake
            return 0
        profit = round(sum(_profit(p) for p in rows), 3)
        staked = round(sum(abs(_profit(p)) for p in rows), 3) or 1
        overall = {
            "wins": wins, "losses": losses, "pushes": pushes,
            "total": wins+losses+pushes, "staked": staked,
            "profit": profit, "win_rate": wr,
            "roi": round(profit/staked, 3) if staked else None,
        }
        findings = [f"Overall: {wins}-{losses} ({wr*100:.1f}% WR) via DB fallback" if wr else f"Overall: {wins}-{losses} via DB fallback"]
        log.info(f"Yesterday panel: built from DB for {check_date} ({wins}W-{losses}L)")
        try:
            from db.picks_store import get_graded_detail
            graded_detail = get_graded_detail(check_date)
        except Exception:
            graded_detail = []
        return {"date": check_date, "metrics": {"overall": overall, "by_tier": {}, "by_type": {}},
                "findings": findings, "recommendations": [], "graded_picks": graded_detail}
    except Exception as _e:
        log.debug(f"DB yesterday fallback failed: {_e}")
        return {}


def load_yesterday_analysis(date: str) -> dict:
    """
    Load the most recent analysis JSON (for yesterday's picks) to show in
    the 'Yesterday' panel.  Falls back to DB if JSON is missing.
    """
    import glob as _glob
    from datetime import datetime as _dt, timedelta as _td

    # Try yesterday first, then walk back up to 3 days
    for days_back in range(1, 4):
        check = (_dt.strptime(date, "%Y-%m-%d") - _td(days=days_back)).strftime("%Y-%m-%d")
        path = os.path.join(PICKS_DIR, f"mlb_analysis_{check}.json")
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                # Verify it has real graded picks (not empty)
                if data.get("metrics") and data["metrics"].get("overall", {}).get("total", 0) > 0:
                    log.info(f"Yesterday's analysis loaded: {path}")
                    data.setdefault("date", check)
                    # Prefer the DB detail (it carries result, final score, sharp_side
                    # and market_signal). The analysis JSON's own graded_picks use a
                    # thinner/older schema (result/type, conf 0-100, no score) — the JS
                    # tolerates both, but the DB version renders the fuller card. Only
                    # keep the JSON's list if the DB has nothing for that date.
                    try:
                        from db.picks_store import get_graded_detail
                        _gd = get_graded_detail(data.get("date", check))
                        if _gd:
                            data["graded_picks"] = _gd
                    except Exception:
                        pass
                    return data
            except Exception:
                pass

    # JSON missing or empty — try DB
    log.info("Yesterday JSON missing or empty — trying DB fallback")
    db_data = _build_yesterday_from_db(date)
    if db_data:
        return db_data
    return {}


PROJ_LINEUPS_CACHE = os.path.join(os.path.dirname(__file__), "data", "clean", "mlb_projected_lineups.json")


def save_projected_lineups_cache(lineups: dict) -> None:
    """
    Persist projected lineups to data/clean/ so they survive git pushes to Railway.
    Called by run_pipeline.py after building lineups from raw hitter stats files.
    """
    try:
        os.makedirs(os.path.dirname(PROJ_LINEUPS_CACHE), exist_ok=True)
        with open(PROJ_LINEUPS_CACHE, "w", encoding="utf-8") as f:
            json.dump(lineups, f)
        log.info(f"Projected lineups cache saved: {len(lineups)} teams → {PROJ_LINEUPS_CACHE}")
    except Exception as e:
        log.warning(f"Could not save projected lineups cache: {e}")


def load_projected_lineups(today: str) -> dict:
    """
    Return the most recent confirmed batting order per team from previous days.
    Used to pre-populate the Teams tab before today's lineups are officially posted.

    Returns dict keyed by team full name:
      { "Boston Red Sox": {"players": [{"name": "...", "order": 1, "pos": "CF"}, ...], "date": "2026-04-22"}, ... }
    """
    import glob as _glob

    raw_dir = os.path.join(os.path.dirname(__file__), "data", "raw")
    files = sorted(
        _glob.glob(os.path.join(raw_dir, "mlb_hitter_stats_*.json")),
        reverse=True  # most recent first
    )

    team_lineups: dict = {}
    for filepath in files:
        date_str = os.path.basename(filepath).replace("mlb_hitter_stats_", "").replace(".json", "")
        if date_str >= today:
            continue  # skip today or future files
        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            for game in data.get("hitters", []):
                if not game.get("lineup_confirmed"):
                    continue
                for team, key in [
                    (game.get("away_team", ""), "away_lineup"),
                    (game.get("home_team", ""), "home_lineup"),
                ]:
                    if not team or team in team_lineups:
                        continue
                    players = sorted(game.get(key, []), key=lambda p: p.get("batting_order", 99))
                    if players:
                        team_lineups[team] = {
                            # Full player dicts so the props model can score them
                            "players": [
                                {
                                    **p,                               # all stats
                                    "name": p.get("player_name", ""), # JS display alias
                                    "pos":  p.get("position", ""),    # JS display alias
                                }
                                for p in players
                            ],
                            "date": date_str,
                        }
        except Exception:
            continue
        if len(team_lineups) >= 30:
            break

    if team_lineups:
        log.info(f"Projected lineups loaded: {len(team_lineups)} teams from raw hitter stats")
        # Auto-save to data/clean/ so Railway gets them on next git push
        save_projected_lineups_cache(team_lineups)
    else:
        # No raw files found — try the pre-saved cache (present on Railway)
        if os.path.exists(PROJ_LINEUPS_CACHE):
            try:
                with open(PROJ_LINEUPS_CACHE, encoding="utf-8") as f:
                    team_lineups = json.load(f)
                log.info(f"Projected lineups loaded from cache: {len(team_lineups)} teams")
            except Exception as e:
                log.warning(f"Projected lineups cache load failed: {e}")
        else:
            log.info("No projected lineups available (no raw files or cache)")
    return team_lineups


# ─────────────────────────────────────────────────────────────────────────────
# HTML TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<meta http-equiv="cache-control" content="no-cache, no-store, must-revalidate"/>
<meta http-equiv="pragma" content="no-cache"/>
<meta http-equiv="expires" content="0"/>
<title>Sports Betting Parlay Genius</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
:root {
  --bg:        #07090f;
  --bg2:       #0d1117;
  --card:      #111827;
  --card2:     #161f2e;
  --border:    #1e2d44;
  --green:     #00e676;
  --gold:      #ffc107;
  --blue:      #42a5f5;
  --purple:    #ab47bc;
  --red:       #ef5350;
  --text:      #e2e8f0;
  --sub:       #7a8899;
  --lock-c:    #ffc107;
  --strong-c:  #42a5f5;
  --lean-c:    #00e676;
  --radius:    12px;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;padding-bottom:60px}

/* ── HEADER ── */
.header{
  background:linear-gradient(135deg,#0d1b2e 0%,#0a1628 50%,#0d1b2e 100%);
  border-bottom:1px solid var(--border);
  padding:28px 24px 20px;
  text-align:center;
  position:relative;
  overflow:hidden;
}
.header::before{
  content:'';position:absolute;inset:0;
  background:radial-gradient(ellipse at 50% 0%,rgba(0,230,118,.08) 0%,transparent 70%);
  pointer-events:none;
}
.header h1{
  font-size:clamp(1.6rem,4vw,2.6rem);font-weight:800;letter-spacing:-0.5px;
  background:linear-gradient(90deg,#00e676,#42a5f5,#ffc107);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
}
.header-sub{color:var(--sub);margin-top:6px;font-size:.9rem;font-weight:500}
.header-stats{
  display:flex;justify-content:center;gap:28px;margin-top:16px;flex-wrap:wrap;align-items:center;
}
.stat-pill{
  background:rgba(255,255,255,.05);border:1px solid var(--border);
  border-radius:20px;padding:5px 14px;font-size:.82rem;font-weight:600;color:var(--text);
}
.stat-pill span{color:var(--green)}
/* ── REFRESH BUTTON ── */
#refreshBtn{
  display:none;align-items:center;gap:8px;
  background:linear-gradient(135deg,#00e676,#00b248);
  border:none;border-radius:24px;padding:9px 22px;
  font-size:.88rem;font-weight:700;color:#000;cursor:pointer;
  font-family:'Inter',sans-serif;transition:opacity .2s,transform .15s;
  box-shadow:0 0 20px rgba(0,230,118,.3);
}
#refreshBtn:hover{opacity:.88;transform:scale(1.03)}
#refreshBtn:disabled{opacity:.5;cursor:not-allowed;transform:none}
#refreshBtn svg{width:15px;height:15px;flex-shrink:0}
.spin{animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
#refreshStatus{
  font-size:.75rem;color:var(--sub);margin-top:6px;
  min-height:16px;text-align:center;
}

/* ── FILTERS ── */
.filters{
  position:sticky;top:0;z-index:100;
  background:rgba(7,9,15,.92);backdrop-filter:blur(12px);
  border-bottom:1px solid var(--border);
  padding:12px 24px;display:flex;gap:16px;flex-wrap:wrap;align-items:center;
}
.filter-group{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.filter-label{color:var(--sub);font-size:.78rem;font-weight:600;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}
.filter-btn{
  background:transparent;border:1px solid var(--border);color:var(--sub);
  padding:5px 13px;border-radius:20px;font-size:.8rem;font-weight:600;cursor:pointer;
  transition:all .2s;font-family:inherit;
}
.filter-btn:hover{border-color:var(--green);color:var(--text)}
.filter-btn.active{background:var(--green);border-color:var(--green);color:#000}
.filter-btn.active-gold{background:var(--gold);border-color:var(--gold);color:#000}
.filter-btn.active-blue{background:var(--blue);border-color:var(--blue);color:#000}
.search-wrap{margin-left:auto}
.search-input{
  background:var(--card);border:1px solid var(--border);color:var(--text);
  padding:6px 14px;border-radius:20px;font-size:.82rem;font-family:inherit;
  outline:none;width:180px;transition:border-color .2s;
}
.search-input:focus{border-color:var(--green)}
.search-input::placeholder{color:var(--sub)}

/* ── MAIN LAYOUT ── */
.main{max-width:1400px;margin:0 auto;padding:28px 20px}
.section-title{
  font-size:1rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;
  color:var(--sub);margin-bottom:16px;display:flex;align-items:center;gap:8px;
}
.section-title::after{content:'';flex:1;height:1px;background:var(--border)}

/* ── WARNING FLAGS ── */
.warn-row{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:8px}
.warn-badge{
  font-size:.62rem;font-weight:700;padding:2px 7px;border-radius:4px;
  letter-spacing:.4px;text-transform:uppercase;transition:opacity .15s;
}
/* Active (condition triggered) */
.warn-tbd.active    {background:rgba(255,152,0,.18);color:#ffb74d;border:1px solid rgba(255,152,0,.45)}
.warn-thin.active   {background:rgba(239,83,80,.15);color:#ef9a9a;border:1px solid rgba(239,83,80,.4)}
.warn-heavy.active  {background:rgba(171,71,188,.18);color:#ce93d8;border:1px solid rgba(171,71,188,.45)}
.warn-lineup.active {background:rgba(66,165,245,.12);color:#90caf9;border:1px solid rgba(66,165,245,.3)}
/* Inactive (condition not triggered — always shown, just dim) */
.warn-badge.inactive{background:rgba(255,255,255,.03);color:rgba(255,255,255,.2);border:1px solid rgba(255,255,255,.08);opacity:.55}

/* ── FAVORITE TIER ── */
.fav-row{display:flex;gap:6px;align-items:center;margin-bottom:8px}
.fav-label{font-size:.6rem;font-weight:600;color:rgba(255,255,255,.3);text-transform:uppercase;letter-spacing:.5px;margin-right:2px}
.fav-badge{
  font-size:.62rem;font-weight:700;padding:2px 8px;border-radius:4px;
  letter-spacing:.4px;text-transform:uppercase;
}
.fav-HEAVY  {background:rgba(239,83,80,.15);color:#ef9a9a;border:1px solid rgba(239,83,80,.4)}
.fav-MEDIUM {background:rgba(255,152,0,.13);color:#ffb74d;border:1px solid rgba(255,152,0,.35)}
.fav-NEUTRAL{background:rgba(0,230,118,.1);color:#69f0ae;border:1px solid rgba(0,230,118,.3)}

/* ── LINE MOVEMENT ── */
.move-row{
  display:flex;align-items:center;gap:8px;flex-wrap:wrap;
  margin-top:8px;padding:6px 10px;border-radius:6px;
  background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);
}
.move-icon{font-size:.9rem}
.move-label{font-size:.72rem;font-weight:700;color:var(--sub);text-transform:uppercase;letter-spacing:.4px}
.move-detail{font-size:.75rem;color:var(--text);flex:1}
.move-badge-steam{font-size:.65rem;font-weight:800;padding:2px 7px;border-radius:4px;
  background:rgba(239,83,80,.2);color:#ef5350;border:1px solid rgba(239,83,80,.4);text-transform:uppercase}
.move-badge-drift{font-size:.65rem;font-weight:800;padding:2px 7px;border-radius:4px;
  background:rgba(255,152,0,.2);color:#ffa726;border:1px solid rgba(255,152,0,.4);text-transform:uppercase}
.move-confirm{font-size:.68rem;font-weight:700;color:#69f0ae}
.move-reverse{font-size:.68rem;font-weight:700;color:#ef5350}

/* ── SHARP MONEY PANEL ── */
.sharp-panel{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:20px 24px;margin-bottom:28px}
.sharp-game{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-start;
  padding:14px 0;border-bottom:1px solid rgba(255,255,255,.06)}
.sharp-game:last-child{border-bottom:none;padding-bottom:0}
.sharp-matchup{font-size:.9rem;font-weight:700;color:var(--text);min-width:180px}
.sharp-desc{font-size:.82rem;color:var(--sub);flex:1;line-height:1.5}
.sharp-agree{font-size:.75rem;font-weight:700;color:#69f0ae;white-space:nowrap}
.sharp-reverse{font-size:.75rem;font-weight:700;color:#ef5350;white-space:nowrap}
.sharp-neutral{font-size:.75rem;font-weight:700;color:var(--sub);white-space:nowrap}
.sharp-badge{font-size:.65rem;font-weight:800;padding:3px 9px;border-radius:4px;
  text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}
.sharp-badge-steam{background:rgba(239,83,80,.2);color:#ef5350;border:1px solid rgba(239,83,80,.4)}
.sharp-badge-drift{background:rgba(255,152,0,.2);color:#ffa726;border:1px solid rgba(255,152,0,.4)}

/* ── KALSHI SIGNAL ── */
.kalshi-row{
  display:flex;align-items:center;gap:8px;
  margin-top:8px;padding:5px 8px;border-radius:6px;
  background:rgba(255,255,255,.03);border:1px solid var(--border);
  font-size:.72rem;
}
.kalshi-label{color:var(--sub);font-weight:600}
.kalshi-prob {font-weight:700;color:var(--text)}
.signal-AGREE    {color:#00e676;font-weight:700}
.signal-DISAGREE {color:#ef5350;font-weight:700}
.signal-NEUTRAL  {color:var(--sub);font-weight:600}

/* ── YESTERDAY PANEL ── */
.yesterday-banner{
  background:linear-gradient(135deg,rgba(13,23,46,.9),rgba(13,27,46,.9));
  border:1px solid var(--border);border-radius:var(--radius);
  padding:18px 22px;margin-bottom:28px;
}
.yesterday-title{
  font-size:.8rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;
  color:var(--sub);margin-bottom:12px;display:flex;align-items:center;gap:8px;
}
.yesterday-stats{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:12px}
.yday-stat{text-align:center}
.yday-num{font-size:1.5rem;font-weight:800;color:var(--text)}
.yday-lbl{font-size:.7rem;color:var(--sub);font-weight:600;text-transform:uppercase;letter-spacing:.5px}
.yday-wins   .yday-num{color:var(--green)}
.yday-losses .yday-num{color:var(--red)}
.yday-roi    .yday-num{color:var(--gold)}
.yday-findings{display:flex;flex-direction:column;gap:4px;border-top:1px solid var(--border);padding-top:10px;margin-top:4px}
.yday-finding{font-size:.74rem;color:var(--sub);line-height:1.5}
.yday-finding.warn{color:#ffb74d}
.yday-tier-row{display:flex;gap:10px;flex-wrap:wrap;margin:8px 0 10px}
.yday-tier-chip{background:rgba(255,255,255,.04);border:1px solid var(--border);border-radius:8px;padding:8px 14px;min-width:90px;text-align:center}
.yday-tier-name{font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.yday-tier-record{font-size:1.1rem;font-weight:800}
.yday-tier-pct{font-size:.7rem;color:var(--sub);margin-top:2px}
.yday-overall-row{display:flex;gap:16px;flex-wrap:wrap;align-items:center;padding:8px 0;border-top:1px solid var(--border);margin-top:4px;font-size:.8rem}
.yday-overall-rec{font-weight:700;color:var(--text)}
.yday-overall-roi{font-weight:700}
.yday-overall-profit{color:var(--sub)}
.yday-overall-props{color:#a78bfa}
.yday-props-row{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.yday-prop-chip{font-size:.72rem;padding:3px 8px;border-radius:4px;font-weight:600}
.yday-prop-chip.win{background:rgba(63,185,80,.15);color:#3fb950;border:1px solid rgba(63,185,80,.3)}
.yday-prop-chip.loss{background:rgba(248,81,73,.12);color:#f85149;border:1px solid rgba(248,81,73,.25)}
.yday-bankroll-wrap{display:flex;gap:14px;align-items:flex-start;margin-bottom:0}
.yday-bankroll-wrap #yesterdayBanner{flex:1;min-width:0}
.bankroll-widget{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px 16px;min-width:175px;max-width:210px;flex-shrink:0}
.tracker-widget{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px 16px;min-width:200px;max-width:240px;flex-shrink:0}
.tw-title{font-size:.82rem;font-weight:800;color:var(--gold);margin-bottom:10px}
.tw-sub{font-size:.68rem;font-weight:700;color:var(--sub);text-transform:uppercase;letter-spacing:.04em;margin:8px 0 3px}
.tw-row{display:flex;justify-content:space-between;align-items:baseline;gap:8px;font-size:.78rem;padding:2px 0}
.tw-lbl{color:var(--text);font-weight:600}
.tw-rec{font-weight:700}
.tw-none{color:var(--sub);font-style:italic;font-size:.72rem}
.tw-note{font-size:.64rem;color:var(--sub);line-height:1.4;margin-top:9px;border-top:1px solid var(--border);padding-top:8px}
.bk-title{font-size:.7rem;color:#8b949e;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px}
.bk-input-row{display:flex;align-items:center;gap:6px;margin-bottom:8px}
.bk-dollar{font-size:1rem;font-weight:700;color:#e6edf3}
.bk-input{background:#161b22;border:1px solid #30363d;border-radius:6px;color:#e6edf3;font-size:1.05rem;font-weight:700;padding:6px 8px;width:100%;outline:none;-moz-appearance:textfield}
.bk-input::-webkit-outer-spin-button,.bk-input::-webkit-inner-spin-button{-webkit-appearance:none}
.bk-input:focus{border-color:#58a6ff}
.bk-unit-row{font-size:.75rem;color:#8b949e;margin-bottom:4px;display:none}
.bk-unit-row strong{color:#e6edf3}
.bk-note{font-size:.64rem;color:#484f58;margin-top:6px;line-height:1.4}

/* ── PICKS GRID ── */
.picks-grid{
  display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
  gap:14px;margin-bottom:36px;
}
.pick-card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;cursor:pointer;transition:transform .15s,border-color .15s;
  position:relative;overflow:hidden;
}
.pick-card:hover{transform:translateY(-2px)}
.pick-card.expanded{border-color:var(--green)}
.pick-card-props-toggle{
  display:flex;align-items:center;gap:5px;font-size:.7rem;font-weight:600;
  color:var(--sub);margin-top:8px;padding-top:8px;
  border-top:1px solid rgba(255,255,255,.05);cursor:pointer;
  transition:color .15s;user-select:none;
}
.pick-card-props-toggle:hover{color:var(--green)}
.pick-card-props-panel{
  display:none;margin-top:10px;padding-top:10px;
  border-top:1px solid rgba(0,230,118,.2);
}
.pick-card-props-panel.open{display:block}
.analysis-toggle{color:var(--gold)}
.analysis-toggle:hover{color:var(--green)}
.analysis-body{font-size:.82rem;line-height:1.6;color:var(--text);
  background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:12px 14px}
.analysis-body b{color:var(--text);font-weight:700}
.bb-wrap{background:linear-gradient(180deg,rgba(255,193,7,.06),rgba(255,193,7,.01));
  border:1px solid rgba(255,193,7,.3);border-radius:12px;padding:14px 16px;margin:0 0 16px}
.bb-head{font-size:1.02rem;font-weight:800;color:var(--gold);margin-bottom:10px}
.bb-sub{font-size:.68rem;font-weight:500;color:var(--sub);margin-left:6px;letter-spacing:.02em}
.bb-empty{font-size:.82rem;color:var(--sub);line-height:1.5}
.bb-rule-row{display:flex;align-items:center;gap:9px;flex-wrap:wrap}
.bb-price-in{width:92px;padding:4px 8px;border-radius:6px;border:1px solid #30363d;
  background:#0d1117;color:#e6edf3;font-size:.85rem;font-family:inherit;text-align:center}
.bb-price-in:focus{outline:none;border-color:#4fc3f7}
.bb-verdict{font-size:.85rem;font-weight:700;color:var(--sub)}
.bb-v-sub{font-size:.7rem;font-weight:400;color:var(--sub)}
.bb-v-yes{color:#3fb950}.bb-v-thin{color:#d29922}.bb-v-no{color:#f85149}
.bb-rule-yes{background:rgba(63,185,80,.14);border-left-color:#3fb950}
.bb-rule-thin{background:rgba(210,153,34,.12);border-left-color:#d29922}
.bb-rule-no{background:rgba(248,81,73,.10);border-left-color:#f85149}
.bb-rule{margin:8px 0 2px;padding:8px 11px;background:rgba(63,185,80,.10);
  border-left:3px solid var(--green);border-radius:0;line-height:1.5}
.bb-rule-lead{display:inline;font-size:.68rem;font-weight:700;letter-spacing:.06em;color:var(--green)}
.bb-rule-price{display:inline;font-size:.95rem;font-weight:700;color:var(--green);margin-left:6px}
.bb-rule-note{display:block;font-size:.68rem;color:var(--sub);margin-top:3px}
.bb-stake{color:var(--green);font-size:.72rem;font-weight:600}
.bb-basis{color:var(--sub);font-size:.68rem;line-height:1.4}
.bb-why{margin-top:10px;border-top:1px solid var(--border);padding-top:8px}
.bb-why-row{display:flex;gap:9px;align-items:baseline;font-size:.72rem;color:var(--sub);padding:2px 0}
.bb-why-n{min-width:22px;text-align:right;font-weight:700;color:var(--text)}
.bb-why-bad{color:#d29922;font-weight:600}
.bb-why-foot{margin-top:8px;font-size:.68rem;color:var(--sub)}
.pick-bestbet{border-color:#e3b341;box-shadow:0 0 0 1px rgba(227,179,65,.35)}
.bb-flag{position:absolute;top:-1px;left:-1px;background:#e3b341;color:#3d2c00;
  font-size:.6rem;font-weight:700;letter-spacing:.08em;padding:3px 9px;border-radius:12px 0 12px 0}
.kelly-nostake{opacity:.75}
.kelly-nostake .kelly-note{color:var(--sub)}
.bb-note{font-size:.7rem;color:var(--sub);line-height:1.45;margin-top:10px;padding-top:9px;border-top:1px solid var(--border)}
/* Universal price check — on EVERY card. Neutral grey until a price is typed,
   so it does not read as a recommendation the way the green Best Bets block
   does. It turns green/amber/red only in response to a real number. */
.shop{margin:8px 0 2px;background:rgba(56,139,253,.07);border-left:3px solid #388bfd;border-radius:0}
.shop-sum{list-style:none;cursor:pointer;padding:7px 11px;display:flex;align-items:baseline;gap:8px;flex-wrap:wrap}
.shop-sum:hover{background:rgba(56,139,253,.06)}
.shop-caret{font-size:.7rem;color:#58a6ff;transition:transform .12s}
details[open] .shop-caret{transform:rotate(90deg)}
.shop-count{font-size:.66rem;color:var(--sub);border:1px solid var(--border);border-radius:9px;padding:1px 7px}
.shop-sum::-webkit-details-marker{display:none}
.shop-lead{font-size:.66rem;font-weight:700;letter-spacing:.06em;color:#58a6ff}
.shop-best{font-size:.95rem;font-weight:700;color:#58a6ff}
.shop-book{font-size:.72rem;color:var(--text)}
.shop-mine{font-size:.68rem;color:#d29922;margin-left:auto}
.shop-mine-ok{color:#3fb950}
.shop-body{padding:2px 11px 9px}
.shop-row{display:flex;justify-content:space-between;font-size:.72rem;color:var(--sub);padding:2px 0}
.shop-row-mine{color:#e6edf3;font-weight:600}
.shop-px{font-variant-numeric:tabular-nums}
.shop-foot{font-size:.66rem;color:var(--sub);margin-top:6px;padding-top:5px;border-top:1px solid var(--border)}
.calc-wrap{margin:0 0 14px;background:rgba(139,148,158,.06);border:1px solid var(--border);border-radius:12px}
.calc-sum{list-style:none;cursor:pointer;padding:10px 14px;display:flex;align-items:baseline;gap:9px;font-size:.85rem}
.calc-sum::-webkit-details-marker{display:none}
.calc-sum:hover{background:rgba(139,148,158,.05)}
.calc-caret{font-size:.7rem;color:var(--sub);transition:transform .12s}
details[open] .calc-caret{transform:rotate(90deg)}
.calc-sub{color:var(--sub);font-size:.72rem}
.calc-body{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;padding:0 14px 14px}
.calc-card{background:var(--card,#0d1117);border:1px solid var(--border);border-radius:8px;padding:11px 12px}
.calc-h{font-size:.72rem;font-weight:700;letter-spacing:.04em;color:var(--sub);margin-bottom:8px}
.calc-in2,.calc-in3{display:flex;gap:7px;flex-wrap:wrap}
.calc-in2 .bb-price-in,.calc-in3 .bb-price-in{flex:1;min-width:88px}
.calc-out{margin-top:9px;font-size:.78rem;line-height:1.6}
.calc-row{display:flex;justify-content:space-between;gap:10px;padding:2px 0}
.calc-row>span:first-child{color:var(--sub);font-size:.72rem}
.calc-foot{margin-top:7px;padding-top:6px;border-top:1px solid var(--border);font-size:.68rem;color:var(--sub);line-height:1.5}
.calc-good{color:#3fb950}.calc-warn{color:#d29922}.calc-bad{color:#f85149}.calc-mute{color:var(--sub)}
.betlog{margin:8px 0 2px;background:rgba(63,185,80,.07);border-left:3px solid #238636;border-radius:0}
.betlog.betlog-done{background:rgba(63,185,80,.14)}
.betlog-sum{list-style:none;cursor:pointer;padding:7px 11px;display:flex;align-items:baseline;gap:8px;flex-wrap:wrap}
.betlog-sum::-webkit-details-marker{display:none}
.betlog-sum:hover{background:rgba(63,185,80,.05)}
.betlog-caret{font-size:.7rem;color:#3fb950;transition:transform .12s}
details[open] .betlog-caret{transform:rotate(90deg)}
.betlog-lead{font-size:.66rem;font-weight:700;letter-spacing:.06em;color:#3fb950}
.betlog-hint{font-size:.68rem;color:var(--sub)}
.betlog-body{padding:2px 11px 10px}
.betlog-row{display:flex;gap:7px;flex-wrap:wrap;align-items:center}
.betlog-sel{background:#0d1117;color:#e6edf3;border:1px solid #30363d;border-radius:6px;
  padding:4px 7px;font-size:.78rem;font-family:inherit}
.betlog-btn{background:#238636;border:0;color:#fff;border-radius:6px;padding:5px 14px;
  font-size:.78rem;font-weight:700;cursor:pointer;font-family:inherit}
.betlog-btn:hover{background:#2ea043}
.betlog-out{margin-top:7px;font-size:.68rem;color:var(--sub);line-height:1.5}
.betlog-ok{color:#3fb950;font-weight:600}.betlog-bad{color:#f85149}
.betlog-link{color:#58a6ff;text-decoration:none}
.pc-rule{margin:8px 0 2px;padding:8px 11px;background:rgba(139,148,158,.08);
  border-left:3px solid #30363d;border-radius:0;line-height:1.5}
.pc-lead{display:inline;font-size:.68rem;font-weight:700;letter-spacing:.06em;color:#8b949e}
.pc-feed{font-size:.68rem;color:var(--sub)}
.pc-feed-bad{color:#d29922;font-weight:600}
.bb-row{display:flex;flex-direction:column;gap:2px;padding:8px 0;border-top:1px solid var(--border)}
.bb-row:first-of-type{border-top:none}
.bb-main{display:flex;align-items:center;gap:10px}
.bb-team{font-weight:700;color:var(--text);font-size:.92rem}
.bb-price{font-weight:800;color:#90caf9;font-size:.82rem;background:rgba(66,165,245,.12);
  border:1px solid rgba(66,165,245,.3);border-radius:6px;padding:1px 8px}
.bb-meta{display:flex;align-items:center;gap:12px;font-size:.74rem;color:var(--sub);flex-wrap:wrap}
.bb-ev{color:var(--green);font-weight:700}
.bb-win{font-weight:600}
.inline-prop{
  display:flex;justify-content:space-between;align-items:center;
  padding:5px 0;border-bottom:1px solid rgba(255,255,255,.04);
  font-size:.75rem;
}
.inline-prop:last-child{border-bottom:none}
.inline-prop-player{color:var(--text);font-weight:600}
.inline-prop-line{color:var(--sub)}
.inline-prop-conf{font-weight:700;white-space:nowrap;margin-left:8px}
.inline-prop-tier-LOCK  {color:var(--gold)}
.inline-prop-tier-STRONG{color:var(--blue)}
.inline-prop-tier-LEAN  {color:var(--green)}
.inline-props-empty{font-size:.74rem;color:var(--sub);padding:6px 0;font-style:italic}
.inline-prop-add{background:none;border:1px solid rgba(255,255,255,.18);border-radius:4px;color:var(--sub);cursor:pointer;font-size:.70rem;padding:1px 6px;margin-left:6px;transition:background .15s,color .15s;flex-shrink:0}
.inline-prop-add:hover{background:rgba(255,255,255,.08);color:var(--text)}
.inline-prop-add.prop-selected{background:var(--green);border-color:var(--green);color:#000;font-weight:700}
.inline-prop.prop-in-parlay{background:rgba(74,222,128,.06);border-radius:4px}
.pick-card.tier-LOCK  {border-top:3px solid var(--gold)}
.pick-card.tier-STRONG{border-top:3px solid var(--blue)}
.pick-card.tier-LEAN  {border-top:3px solid var(--green);opacity:.82}
.pick-card.tier-TOSSUP{border-left:3px solid rgba(127,119,221,.75);border-radius:0 8px 8px 0;opacity:.78}
.pick-card.hidden{display:none}

.pick-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;flex-wrap:wrap;gap:4px;padding-right:54px}
.pick-type-badge{
  font-size:.68rem;font-weight:700;letter-spacing:.8px;padding:3px 9px;border-radius:4px;
  text-transform:uppercase;
}
.badge-ML   {background:rgba(66,165,245,.15);color:#42a5f5;border:1px solid rgba(66,165,245,.3)}
.badge-RL   {background:rgba(171,71,188,.15);color:#ce93d8;border:1px solid rgba(171,71,188,.3)}
.badge-TOTAL{background:rgba(0,230,118,.15);color:#00e676;border:1px solid rgba(0,230,118,.3)}
.badge-PROP {background:rgba(255,183,77,.15);color:#ffb74d;border:1px solid rgba(255,183,77,.3)}
.hc-badge{
  background:linear-gradient(135deg,rgba(255,107,0,.18),rgba(255,183,77,.14));
  color:#ff9d3c;border:1px solid rgba(255,157,60,.45);
  padding:2px 8px;border-radius:999px;font-size:.66rem;font-weight:800;
  letter-spacing:.4px;white-space:nowrap;
}
.pb-badge{
  background:rgba(63,185,80,.12);color:#3fb950;border:1px solid rgba(63,185,80,.4);
  padding:2px 8px;border-radius:999px;font-size:.66rem;font-weight:800;
  letter-spacing:.4px;white-space:nowrap;
}
.pick-card.pick-highconf{
  border-color:rgba(255,157,60,.55);
  box-shadow:0 0 0 1px rgba(255,157,60,.25), 0 4px 18px rgba(255,107,0,.10);
}
.tier-badge{
  font-size:.7rem;font-weight:700;padding:3px 9px;border-radius:4px;
}
.tb-LOCK  {background:rgba(255,193,7,.15);color:var(--gold);border:1px solid rgba(255,193,7,.3)}
.tb-STRONG{background:rgba(66,165,245,.15);color:var(--blue);border:1px solid rgba(66,165,245,.3)}
.tb-LEAN  {background:rgba(0,230,118,.15);color:var(--green);border:1px solid rgba(0,230,118,.3)}
.tb-TOSSUP{background:rgba(127,119,221,.12);color:#a09ae0;border:1px solid rgba(127,119,221,.3)}

.pick-label{font-size:1.08rem;font-weight:700;color:var(--text);margin-bottom:4px;padding-right:60px}
.pick-team-emblem{position:absolute;top:8px;right:10px;width:40px;height:40px;object-fit:contain;opacity:.55;pointer-events:none;z-index:0;filter:drop-shadow(0 0 1px rgba(255,255,255,.5))}
.pick-game{font-size:.78rem;color:var(--sub);margin-bottom:12px;padding-right:60px}

.conf-row{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.conf-bar-wrap{flex:1;height:7px;background:rgba(255,255,255,.07);border-radius:4px;overflow:hidden}
.conf-bar{height:100%;border-radius:4px;transition:width .4s ease}
.bar-LOCK  {background:linear-gradient(90deg,#e65100,var(--gold))}
.bar-STRONG{background:linear-gradient(90deg,#0d47a1,var(--blue))}
.bar-LEAN  {background:linear-gradient(90deg,#1b5e20,var(--green))}
.bar-TOSSUP{background:rgba(127,119,221,.45)}
.conf-pct{font-size:.95rem;font-weight:700;white-space:nowrap}
.pct-LOCK  {color:var(--gold)}
.pct-STRONG{color:var(--blue)}
.pct-LEAN  {color:var(--green)}
.pct-TOSSUP{color:#a09ae0}
.tossup-split{display:flex;gap:8px;margin:10px 0 6px}
.tossup-side{flex:1;padding:8px 10px;border-radius:6px;
  border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.03)}
.tossup-side.favored{border-color:rgba(127,119,221,.4);background:rgba(127,119,221,.08)}
.tossup-team{font-size:.8rem;font-weight:600;color:var(--text)}
.tossup-pct-fav{font-size:.75rem;color:#a09ae0;margin-top:2px}
.tossup-pct-other{font-size:.75rem;color:var(--sub);margin-top:2px}
.tossup-note{font-size:.7rem;color:var(--sub);font-style:italic;margin-top:4px}

.kelly-row{display:flex;align-items:center;gap:6px;font-size:.72rem;color:var(--sub);
  margin:6px 0 2px;padding:4px 8px;background:rgba(76,175,80,.07);
  border-radius:4px;border-left:2px solid rgba(76,175,80,.4)}
.kelly-note{opacity:.55;font-style:italic;font-size:.67rem}

.line-shop-row{font-size:.72rem;color:var(--sub);margin:5px 0 2px;padding:4px 8px;
  background:rgba(66,165,245,.07);border-radius:4px;border-left:2px solid rgba(66,165,245,.4)}
.line-shop-tip{opacity:.7;font-style:italic}

.pick-reasoning{
  font-size:.74rem;color:var(--sub);line-height:1.5;
  border-top:1px solid var(--border);padding-top:8px;margin-top:4px;
}

.pick-narrative{
  font-size:.80rem;color:var(--text);line-height:1.6;
  background:rgba(255,255,255,.03);border-left:3px solid var(--green);
  border-radius:0 6px 6px 0;padding:8px 12px;margin-top:8px;
  font-style:italic;
}

.thematic-parlay-card{border-top:3px solid var(--blue) !important}
.thematic-thesis-badge{
  font-size:.78rem;font-weight:700;letter-spacing:.5px;text-transform:uppercase;
  color:var(--blue);margin-bottom:4px;
}
.thematic-desc{
  font-size:.74rem;color:var(--sub);line-height:1.5;margin-bottom:2px;font-style:italic;
}

/* ── PARLAY CARDS ── */
.parlay-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px;margin-bottom:36px}
.parlay-card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;position:relative;overflow:hidden;
}
.parlay-card::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--green),var(--blue),var(--gold));
}
.parlay-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.parlay-tag{font-size:.7rem;font-weight:700;letter-spacing:.6px;color:var(--sub);text-transform:uppercase}
.parlay-payout{
  font-size:.95rem;font-weight:800;color:var(--green);
  background:rgba(0,230,118,.1);border:1px solid rgba(0,230,118,.25);
  padding:3px 10px;border-radius:6px;
}
.parlay-conf{font-size:1.5rem;font-weight:800;color:var(--text);margin-bottom:12px}
.parlay-conf span{font-size:.85rem;font-weight:500;color:var(--sub);margin-left:4px}
.parlay-legs{display:flex;flex-direction:column;gap:8px}
.parlay-leg{
  background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:9px 12px;
  display:flex;align-items:center;gap:10px;
}
.leg-conf{
  font-size:.78rem;font-weight:700;white-space:nowrap;padding:2px 8px;border-radius:4px;
}
.leg-LOCK  {background:rgba(255,193,7,.15);color:var(--gold)}
.leg-STRONG{background:rgba(66,165,245,.15);color:var(--blue)}
.leg-LEAN  {background:rgba(0,230,118,.15);color:var(--green)}
.leg-info{flex:1}
.leg-label{font-size:.85rem;font-weight:600;color:var(--text)}
.leg-game {font-size:.72rem;color:var(--sub);margin-top:1px}

/* ── GAME BREAKDOWN ── */
.games-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(380px,1fr));gap:14px}
.game-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.game-header{
  background:var(--card2);padding:12px 16px;display:flex;justify-content:space-between;align-items:center;
  border-bottom:1px solid var(--border);
}
.matchup{font-size:.95rem;font-weight:700}
.game-time{font-size:.75rem;color:var(--sub)}
.game-body{padding:14px 16px;display:flex;flex-direction:column;gap:8px}
.game-row{display:flex;justify-content:space-between;align-items:center;font-size:.8rem}
.row-label{color:var(--sub);font-weight:600;min-width:48px}
.row-val{color:var(--text);font-weight:500;text-align:right}
.park-badge{
  display:inline-block;font-size:.68rem;font-weight:700;padding:2px 7px;border-radius:4px;margin-left:6px;
}
.park-hitter {background:rgba(239,83,80,.15);color:#ef9a9a}
.park-pitcher{background:rgba(66,165,245,.15);color:#90caf9}
.park-neutral{background:rgba(255,255,255,.06);color:var(--sub)}
.wp-row{display:flex;gap:6px;margin-top:4px}
.wp-bar-outer{flex:1;background:rgba(255,255,255,.06);border-radius:4px;overflow:hidden;height:6px;margin-top:2px}
.wp-bar-inner{height:100%;background:var(--green);border-radius:4px}
.wp-team{font-size:.78rem;font-weight:600}
.wp-pct {font-size:.85rem;font-weight:700;color:var(--green)}

/* ── EMPTY STATE ── */
.empty{text-align:center;color:var(--sub);padding:48px;font-size:.95rem}

/* ── TABS ── */
.section-tabs{display:flex;gap:0;margin-bottom:16px;border:1px solid var(--border);border-radius:8px;overflow:hidden;width:fit-content}
.section-tab{
  padding:7px 18px;font-size:.82rem;font-weight:600;cursor:pointer;
  background:transparent;border:none;color:var(--sub);font-family:inherit;
  transition:all .15s;
}
.section-tab.active{background:var(--green);color:#000}
.section-tab:hover:not(.active){background:rgba(255,255,255,.05);color:var(--text)}

/* ── RESULTS COUNT ── */
.results-count{font-size:.8rem;color:var(--sub);margin-bottom:10px}
.results-count b{color:var(--text)}

/* ── SCORES TICKER ── */
.ticker-wrap{
  background:#080c14;border-bottom:1px solid var(--border);
  padding:0;height:34px;display:flex;align-items:center;
}
.ticker-label{
  background:var(--green);color:#000;font-weight:800;font-size:.72rem;
  letter-spacing:.8px;padding:0 12px;height:100%;display:flex;
  align-items:center;white-space:nowrap;flex-shrink:0;text-transform:uppercase;
}
.ticker-outer{
  flex:1;overflow:hidden;height:100%;display:flex;align-items:center;
}
.ticker-track{
  display:inline-flex;white-space:nowrap;
  animation:ticker 35s linear infinite;
  will-change:transform;
}
.ticker-track:hover{animation-play-state:paused}
@keyframes ticker{
  0%  {transform:translateX(0)}
  100%{transform:translateX(-50%)}
}
.ticker-item{
  display:inline-flex;align-items:center;gap:6px;
  padding:0 24px;font-size:.78rem;font-weight:600;color:var(--text);
  border-right:1px solid var(--border);
}
.ticker-score{font-weight:800;font-size:.85rem}
.ticker-score.win{color:var(--green)}
.ticker-score.loss{color:var(--sub)}
.ticker-final{font-size:.65rem;color:var(--sub);text-transform:uppercase;letter-spacing:.5px}
/* ── Completed pick card overlay ── */
.pick-card.pick-done{opacity:.72;border-style:dashed}
.pick-card.pick-done:hover{opacity:.9}
.pick-done-banner{
  display:flex;align-items:center;gap:6px;
  padding:6px 10px;border-radius:6px;margin-top:10px;
  font-size:.75rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;
}
.pick-done-banner.win-banner {background:rgba(46,160,67,.15);color:#3fb950;border:1px solid rgba(46,160,67,.3)}
.pick-done-banner.loss-banner{background:rgba(248,81,73,.12);color:#f85149;border:1px solid rgba(248,81,73,.25)}
.ticker-live{font-size:.65rem;color:#ff6b35;font-weight:700;text-transform:uppercase;letter-spacing:.5px}
.ticker-empty{color:var(--sub);font-size:.78rem;padding:0 20px}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* ── PROPS SECTION ── */
.props-filter-row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:16px}
.props-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(310px,1fr));gap:14px;margin-bottom:36px}
.prop-card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;transition:transform .15s,border-color .15s;position:relative;overflow:hidden;
}
.proj-only-badge{background:rgba(139,148,158,.15);color:var(--sub);font-size:.62rem;font-weight:700;padding:2px 7px;border-radius:5px;letter-spacing:.03em}
.proj-note{font-size:.66rem;color:var(--sub);line-height:1.4;margin:6px 0 2px;padding:6px 8px;background:rgba(139,148,158,.06);border-radius:6px}
.prop-card:hover{transform:translateY(-2px)}
.prop-card.tier-LOCK  {border-top:3px solid var(--gold)}
.prop-card.tier-STRONG{border-top:3px solid var(--blue)}
.prop-card.tier-LEAN  {border-top:3px solid var(--green)}
.prop-card.hidden{display:none}
.prop-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px}
.prop-type-badge{
  font-size:.7rem;font-weight:700;letter-spacing:.8px;padding:3px 9px;border-radius:4px;text-transform:uppercase;
}
.badge-HR   {background:rgba(239,83,80,.15);color:#ef9a9a;border:1px solid rgba(239,83,80,.3)}
.badge-HITS {background:rgba(66,165,245,.15);color:#90caf9;border:1px solid rgba(66,165,245,.3)}
.badge-K    {background:rgba(171,71,188,.15);color:#ce93d8;border:1px solid rgba(171,71,188,.3)}
.badge-TB   {background:rgba(255,167,38,.15);color:#ffcc80;border:1px solid rgba(255,167,38,.3)}
.badge-RBI  {background:rgba(38,166,154,.15);color:#80cbc4;border:1px solid rgba(38,166,154,.3)}
.badge-R    {background:rgba(102,187,106,.15);color:#a5d6a7;border:1px solid rgba(102,187,106,.3)}
.badge-SB   {background:rgba(41,182,246,.15);color:#81d4fa;border:1px solid rgba(41,182,246,.3)}

/* ── TEAMS TAB ── */
.team-btn{padding:7px 13px;border-radius:6px;border:1px solid var(--border);background:var(--card);
  color:var(--sub);font-size:.8rem;font-weight:600;cursor:pointer;transition:all .15s;white-space:nowrap}
.team-btn:hover{border-color:var(--green);color:var(--text)}
.team-btn.active{background:var(--green);color:#fff !important;border-color:var(--green)}
.team-btn.playing{border-color:rgba(0,230,118,.4);color:var(--green)}
.team-btn.playing.active{color:#fff !important}

.team-game-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:24px;margin-bottom:20px}
.team-matchup-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;flex-wrap:wrap;gap:12px}
.team-matchup-teams{display:flex;align-items:center;gap:16px;flex:1}
.team-name-block{text-align:center;min-width:140px}
.team-name-big{font-size:1.15rem;font-weight:800;color:var(--text)}
.team-name-sp{font-size:.75rem;color:var(--sub);margin-top:3px}
.team-vs{font-size:1.4rem;color:var(--sub);font-weight:300;padding:0 8px}
.team-game-meta{text-align:right;font-size:.8rem;color:var(--sub);line-height:1.7}
.team-day-label{font-size:1rem;font-weight:700;color:var(--green);display:block}

.team-pred-row{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:18px}
.team-pred-box{background:var(--bg);border-radius:8px;padding:14px;text-align:center}
.team-pred-label{font-size:.72rem;color:var(--sub);margin-bottom:6px;letter-spacing:.5px;text-transform:uppercase}
.team-pred-conf{font-size:1.6rem;font-weight:800}
.team-pred-bar{height:6px;background:rgba(255,255,255,.08);border-radius:3px;margin:8px 0 4px}
.team-pred-fill{height:6px;border-radius:3px;transition:width .4s}
.team-pred-pick{font-size:.75rem;font-weight:700;margin-top:4px}

.team-props-section{margin-top:18px}
.team-props-header{font-size:.85rem;font-weight:700;color:var(--sub);margin-bottom:10px;
  text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid var(--border);padding-bottom:6px}
.team-props-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.team-prop-mini{background:var(--bg);border-radius:8px;padding:11px 14px;border:1px solid var(--border)}
.team-prop-mini-top{display:flex;align-items:center;justify-content:space-between;margin-bottom:4px}
.team-prop-mini-name{font-size:.9rem;font-weight:700;color:var(--text)}
.team-prop-mini-type{font-size:.7rem;padding:2px 7px;border-radius:4px;font-weight:700}
.team-prop-mini-line{font-size:.78rem;color:var(--sub);margin-bottom:6px}
.team-prop-mini-conf{font-size:.85rem;font-weight:700}
.team-no-props{color:var(--sub);font-size:.85rem;padding:12px 0;font-style:italic}

.prop-type-filters{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px}
.ptype-btn{padding:4px 11px;border-radius:20px;border:1px solid var(--border);background:transparent;
  color:var(--sub);font-size:.75rem;font-weight:600;cursor:pointer;transition:all .15s}
.ptype-btn:hover{border-color:var(--green);color:var(--text)}
.ptype-btn.active{background:rgba(0,230,118,.15);border-color:var(--green);color:var(--green)}

.prop-player{font-size:1.05rem;font-weight:700;color:var(--text);margin-bottom:2px}
.prop-game  {font-size:.75rem;color:var(--sub);margin-bottom:10px}
.prop-line-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;
  background:rgba(255,255,255,.04);border-radius:6px;padding:6px 10px}
.prop-line-label{font-size:.72rem;color:var(--sub);font-weight:600}
.prop-line-val{font-size:.92rem;font-weight:700;color:var(--text)}
.prop-book-row{display:flex;justify-content:space-between;align-items:center;margin:8px 0;
  padding-top:8px;border-top:1px solid rgba(255,255,255,.06)}
.prop-book-vals{display:flex;gap:6px}
.prop-book-chip{font-size:.74rem;font-weight:700;padding:2px 8px;border-radius:8px;
  background:rgba(255,255,255,.05);color:var(--sub);border:1px solid rgba(255,255,255,.08)}
.prop-book-chip.chip-pick{background:rgba(63,185,80,.15);color:#3fb950;border-color:rgba(63,185,80,.35)}
.prop-proj-val{font-size:.8rem;font-weight:600;color:var(--green)}
.prop-reasoning{
  font-size:.73rem;color:var(--sub);line-height:1.5;
  border-top:1px solid var(--border);padding-top:8px;margin-top:4px;
}
.section-nav{
  display:flex;gap:0;margin-bottom:24px;border-bottom:2px solid var(--border);
}
.section-nav-btn{
  padding:10px 22px;font-size:.88rem;font-weight:700;cursor:pointer;
  background:transparent;border:none;border-bottom:2px solid transparent;
  color:var(--sub);font-family:inherit;transition:all .15s;margin-bottom:-2px;
}
.section-nav-btn.active{color:var(--green);border-bottom-color:var(--green)}
.section-nav-btn:hover:not(.active){color:var(--text)}
.section-panel{display:none}
.section-panel.active{display:block}
/* Stats & Trends bar charts */
.trend-chart{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px 18px;margin-bottom:16px}
.trend-chart-title{font-size:.95rem;font-weight:800;color:var(--text);margin-bottom:3px}
.trend-chart-sub{font-size:.72rem;color:var(--sub);margin-bottom:14px}
.trend-row{display:grid;grid-template-columns:96px 1fr 120px;align-items:center;gap:10px;margin:7px 0}
.trend-label{font-size:.8rem;color:var(--text);font-weight:600;text-align:right}
.trend-bar-track{position:relative;height:20px;background:var(--bg);border-radius:5px;overflow:hidden}
.trend-bar{height:100%;border-radius:5px;transition:width .4s ease}
.trend-be{position:absolute;top:0;bottom:0;width:2px;background:rgba(255,255,255,.35);z-index:2}
.trend-be-lbl{position:absolute;top:-15px;font-size:.55rem;color:var(--sub);transform:translateX(-50%)}
.trend-val{font-size:.76rem;font-weight:700;white-space:nowrap}
.trend-empty{color:var(--sub);font-style:italic;padding:20px;text-align:center}
.prop-name-row{display:flex;align-items:center;gap:10px}
.prop-face{width:46px;height:46px;border-radius:50%;object-fit:cover;background:var(--bg);border:1px solid var(--border);flex-shrink:0}
.prop-link{color:inherit;text-decoration:none;border-bottom:1px dotted var(--sub)}
.prop-link:hover{color:var(--blue);border-bottom-color:var(--blue)}
.lineup-status{margin:6px 0 2px}
.ls-badge{font-size:.7rem;font-weight:700;padding:3px 9px;border-radius:20px;display:inline-block}
.ls-yes{color:var(--green);background:rgba(63,185,80,.12);border:1px solid rgba(63,185,80,.35)}
.ls-no{color:#ffb74d;background:rgba(255,183,77,.1);border:1px solid rgba(255,183,77,.3)}
.gs-badge{font-size:.72rem;font-weight:800;padding:3px 10px;border-radius:20px;display:inline-block;letter-spacing:.03em}
.gs-bad{color:#ff6b6b;background:rgba(255,107,107,.14);border:1px solid rgba(255,107,107,.45)}
.gs-warn{color:#ffb74d;background:rgba(255,183,77,.14);border:1px solid rgba(255,183,77,.45)}
.gs-banner{margin:4px 0 8px;padding:7px 12px;border-radius:8px;background:rgba(255,107,107,.08);
  border:1px solid rgba(255,107,107,.3);font-size:.8rem;color:#ff9d9d;display:flex;align-items:center;gap:8px}

/* ── TODAY'S GAMES ── */
.schedule-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px;margin-bottom:36px}
.sched-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.sched-status-bar{
  padding:5px 14px;font-size:.7rem;font-weight:700;letter-spacing:.6px;text-transform:uppercase;
  display:flex;align-items:center;gap:6px;
}
.status-upcoming{background:rgba(66,165,245,.12);color:var(--blue)}
.status-live    {background:rgba(255,107,53,.15);color:#ff6b35}
.status-final   {background:rgba(255,255,255,.06);color:var(--sub)}
.live-dot{width:7px;height:7px;border-radius:50%;background:#ff6b35;animation:pulse 1.5s infinite;flex-shrink:0}
.sched-matchup{padding:14px 16px 10px}
.sched-team-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.sched-team-info{display:flex;flex-direction:column;gap:2px}
.sched-team-name{font-size:1rem;font-weight:700;color:var(--text)}
.sched-team-record{font-size:.78rem;font-weight:600;color:var(--sub)}
.sched-team-streak{font-size:.68rem;color:var(--sub)}
.sched-score-block{text-align:center;min-width:54px}
.sched-score{font-size:1.5rem;font-weight:800;color:var(--text)}
.sched-score.winner{color:var(--green)}
.sched-vs{font-size:.85rem;color:var(--sub);font-weight:600}
.sched-at-label{font-size:.62rem;color:var(--sub);text-transform:uppercase;letter-spacing:.5px;margin-top:2px}
.sched-divider{height:1px;background:var(--border);margin:0 16px}
.sched-footer{padding:8px 16px;display:flex;justify-content:space-between;align-items:center;font-size:.75rem}
.sched-time{font-weight:700;color:var(--text)}
.sched-venue{color:var(--sub);text-align:right;max-width:55%}
.sched-pitchers{padding:4px 16px 10px;font-size:.72rem;color:var(--sub);
  display:flex;justify-content:space-between}

/* ── SCROLLBAR ── */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#2e4060}

/* ── SHARP ACTION TAB ── */
.sharp-action-card{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:20px 24px;margin-bottom:16px;transition:border-color .15s}
.sharp-action-card:hover{border-color:rgba(239,83,80,.3)}
.sharp-explain-box{background:rgba(239,83,80,.06);border:1px solid rgba(239,83,80,.2);
  border-radius:10px;padding:14px 18px;margin-bottom:24px;font-size:.82rem;color:var(--sub);line-height:1.6}
.sharp-odds-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.sharp-odds-box{background:var(--bg);border-radius:8px;padding:12px;text-align:center}
.sharp-odds-team-lbl{font-size:.68rem;color:var(--sub);margin-bottom:8px;
  text-transform:uppercase;letter-spacing:.5px;font-weight:700}
.sharp-odds-row{display:flex;justify-content:space-around;align-items:center;gap:4px}
.sharp-odds-cell{text-align:center}
.sharp-odds-cell-lbl{font-size:.6rem;color:var(--sub)}
.sharp-odds-cell-num{font-size:1.05rem;font-weight:800}
.sharp-odds-arrow{color:var(--sub);font-size:1.1rem;align-self:center}
.sharp-total-row{background:var(--bg);border-radius:8px;padding:10px 14px;margin-bottom:12px;
  display:flex;align-items:center;gap:12px;font-size:.82rem;flex-wrap:wrap}
.sharp-bet-call{background:rgba(0,230,118,.06);border:1px solid rgba(0,230,118,.2);
  border-radius:8px;padding:12px 16px;display:flex;justify-content:space-between;
  align-items:center;flex-wrap:wrap;gap:8px}
.sharp-bet-call-lbl{font-size:.68rem;color:var(--sub);text-transform:uppercase;
  letter-spacing:.5px;margin-bottom:4px}
.sharp-bet-call-text{font-size:.95rem;font-weight:800;color:var(--text)}
.sharp-empty{text-align:center;padding:60px 20px}

/* ── STATUS LINK ── */
a.status-link{
  font-size:.72rem;color:var(--sub);text-decoration:none;
  border:1px solid var(--border);border-radius:12px;padding:3px 11px;
  transition:all .2s;
}
a.status-link:hover{color:var(--green);border-color:var(--green)}

/* ── DATE TOGGLE ── */
.date-toggle{display:flex;gap:8px;margin:8px 0 4px}
.date-btn{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);color:var(--sub);
  border-radius:20px;padding:5px 16px;font-size:.8rem;font-weight:600;cursor:pointer;transition:all .18s}
.date-btn:hover{background:rgba(255,255,255,.1);color:var(--text)}
.date-btn.active{background:rgba(0,230,118,.15);border-color:var(--green);color:var(--green)}
/* ── ODDS PILL ── */
.honest-read{display:block;margin:7px 0 0;padding:7px 10px;border-radius:6px;
  border-left:3px solid;font-size:.72rem;line-height:1.45}
.honest-read .hr-title{display:block;font-weight:700;letter-spacing:.06em;font-size:.63rem;margin-bottom:2px}
.honest-read .hr-body{color:#c9d1d9}
.hr-play{background:rgba(63,185,80,.09);border-color:#3fb950}
.hr-play .hr-title{color:#3fb950}
.hr-thin{background:rgba(210,153,34,.09);border-color:#d29922}
.hr-thin .hr-title{color:#d29922}
.hr-none{background:rgba(139,148,158,.09);border-color:#8b949e}
.hr-none .hr-title{color:#8b949e}
.hr-avoid{background:rgba(248,81,73,.09);border-color:#f85149}
.hr-avoid .hr-title{color:#f85149}
.value-row{display:flex;align-items:center;gap:8px;margin:6px 0 2px;flex-wrap:wrap;font-size:.74rem}
.value-tag{font-weight:800;letter-spacing:.03em;border:1px solid;border-radius:6px;padding:2px 7px}
.value-ev{font-weight:700}
.value-meta{color:var(--sub)}
.val-chalk{font-size:.64rem;font-weight:800;color:#ffb74d;background:rgba(255,183,77,.12);
  border:1px solid rgba(255,183,77,.35);border-radius:6px;padding:1px 6px;letter-spacing:.05em}
.val-warn-badge{font-size:.64rem;font-weight:800;color:#ff6b6b;background:rgba(255,107,107,.12);
  border:1px solid rgba(255,107,107,.4);border-radius:8px;padding:2px 7px;letter-spacing:.04em}
.val-unproven{font-size:.66rem;font-weight:800;color:#8b949e;background:rgba(139,148,158,.1);
  border:1px solid rgba(139,148,158,.3);border-radius:6px;padding:2px 7px;letter-spacing:.04em}
.odds-row{display:flex;align-items:center;gap:6px;margin:6px 0 2px;flex-wrap:wrap}
.odds-label{font-size:.68rem;color:var(--sub);text-transform:uppercase;letter-spacing:.04em;margin-right:2px}
.odds-pill{font-size:.72rem;font-weight:700;padding:2px 8px;border-radius:10px;
  background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.12);color:var(--sub)}
.odds-pill.odds-picked{background:rgba(66,165,245,.15);border-color:rgba(66,165,245,.4);color:#90caf9}
/* ── ADD TO PARLAY BUTTON ── */
.add-leg-btn{width:100%;margin-top:10px;padding:7px 0;background:rgba(0,230,118,.08);
  border:1px solid rgba(0,230,118,.25);border-radius:6px;color:var(--green);font-size:.78rem;
  font-weight:600;cursor:pointer;transition:all .18s;letter-spacing:.02em}
.add-leg-btn:hover{background:rgba(0,230,118,.18);border-color:var(--green)}
.add-leg-btn.leg-selected{background:rgba(0,230,118,.25);border-color:var(--green);color:#fff}
/* ── PARLAY DRAWER ── */
#parlayDrawer{position:fixed;bottom:0;left:0;right:0;z-index:999;
  background:#1a2030;border-top:2px solid var(--green);padding:12px 20px 16px;
  box-shadow:0 -4px 24px rgba(0,0,0,.5);max-height:45vh;overflow-y:auto}
.drawer-header{display:flex;align-items:center;gap:16px;margin-bottom:10px;flex-wrap:wrap}
.drawer-title{font-weight:700;font-size:.9rem;color:var(--text)}
.drawer-payout{font-size:.82rem;color:var(--green);font-weight:600;margin-left:auto}
.drawer-clear{background:rgba(239,83,80,.15);border:1px solid rgba(239,83,80,.3);color:#ef5350;
  border-radius:6px;padding:3px 10px;font-size:.72rem;cursor:pointer}
.drawer-legs{display:flex;flex-direction:column;gap:6px}
.drawer-leg{display:flex;align-items:center;gap:10px;padding:5px 8px;
  background:rgba(255,255,255,.04);border-radius:6px;font-size:.78rem}
.drawer-leg-label{flex:1;color:var(--text);font-weight:600}
.drawer-leg-odds{color:var(--green);font-weight:700;white-space:nowrap}
.drawer-remove{background:none;border:none;color:var(--sub);cursor:pointer;font-size:.9rem;padding:0 4px}
.drawer-remove:hover{color:#ef5350}
/* ── PARLAY EV ── */
.parlay-ev{font-size:.72rem;font-weight:700;color:var(--green);
  background:rgba(0,230,118,.1);border:1px solid rgba(0,230,118,.2);
  border-radius:4px;padding:2px 9px;white-space:nowrap;margin-top:4px;display:inline-block}
.parlay-breakeven{font-size:.7rem;color:var(--sub);margin-top:4px}
</style>
</head>
<body>

<div class="header">
  <h1>⚾ Sports Betting Parlay Genius</h1>
  <div class="header-sub" id="dateStr"></div>
  <div class="date-toggle" id="dateToggle"></div>
  <div class="header-stats">
    <div class="stat-pill">Games <span id="gameCount">—</span></div>
    <div class="stat-pill">Picks <span id="pickCount">—</span></div>
    <div class="stat-pill">Locks <span id="lockCount">—</span></div>
    <div class="stat-pill">Top Pick <span id="topPick">—</span></div>
    <a href="/performance" class="status-link">📊 Performance</a>
    <a href="/ask" class="status-link">🧠 Statalizer Bot</a>
    <a href="/players" class="status-link">👤 Players</a>
    <a href="/admin/analysis" class="status-link">📋 Analysis</a>
    <a href="/admin" class="status-link">🗂 Menu</a>
    <a href="/status" class="status-link">⚙ Status</a>
    <button id="refreshBtn" onclick="doRefresh()">
      <svg id="refreshIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
      </svg>
      Refresh Picks
    </button>
  </div>
  <div id="refreshStatus"></div>
</div>

<!-- Stale board banner: shown when the server has rescored since page load -->
<div id="staleBoardBar" style="display:none;background:rgba(210,153,34,.14);border-bottom:1px solid rgba(210,153,34,.4);padding:8px 16px;align-items:center;gap:14px;font-size:.85rem;color:#e6edf3">
  <span>🔄 <b>New lines are in.</b> Lineups or prices have changed since you loaded this page, so Best Bets and the EV numbers below are out of date. Picks that did not qualify may qualify now.</span>
  <button onclick="location.reload()" style="background:rgba(210,153,34,.25);border:1px solid rgba(210,153,34,.6);color:#ffd479;border-radius:12px;padding:4px 14px;font-size:.8rem;font-weight:700;cursor:pointer;font-family:inherit">Reload board</button>
</div>

<!-- Schedule Status Bar -->
<div id="scheduleBar" style="background:#161b22;border-bottom:1px solid #30363d;padding:6px 16px;display:flex;gap:24px;font-size:0.82em;color:#8b949e;flex-wrap:wrap">
  <span>⏰ <b style="color:#e6edf3">Next pipeline:</b> <span id="sched-pipeline">loading...</span></span>
  <span>🔄 <b style="color:#e6edf3">Lineup refresh:</b> <span id="sched-refresh">loading...</span></span>
  <span>⚾ <b style="color:#e6edf3">First pitch:</b> <span id="sched-pitch">loading...</span></span>
  <span style="margin-left:auto;display:flex;align-items:center;gap:10px">
    <button id="forceLineupsBtn" onclick="forceLineupsRefresh()" style="background:rgba(79,195,247,.15);border:1px solid rgba(79,195,247,.4);color:#4fc3f7;border-radius:12px;padding:3px 12px;font-size:.78rem;font-weight:700;cursor:pointer;font-family:inherit">📋 Refresh Lineups</button>
    <span id="forceLineupsStatus" style="font-size:.76rem"></span>
  </span>
</div>
<script>
(function() {
  function loadSchedule() {
    fetch('/schedule-status').then(r => r.json()).then(d => {
      var p = d.next_pipeline;
      var r = d.next_refresh;
      var fp = d.first_pitch;
      document.getElementById('sched-pipeline').textContent = p ? (p.time + ' (' + p.label + ')') : '6am ET daily';
      document.getElementById('sched-refresh').textContent  = r ? (r.time + ' (' + r.label + ')') : 'after 6am pipeline';
      window._schedRefreshTime = r ? r.time : 'lineup refresh';
      document.getElementById('sched-pitch').textContent    = fp ? fp.time : '—';

      // ── STALE BOARD DETECTION (2026-08-11) ────────────────────────────────
      // Best Bets, the honest read and every EV number are computed when the
      // page is RENDERED. The server rescores at the lineup refresh and on each
      // ~40 min Pinnacle odds pull, so a tab left open all evening shows stale
      // picks while confidences and prices have moved underneath. As lineups
      // confirm, cards that did not qualify can start qualifying and vice versa.
      // Rather than silently re-render a partial view, tell the user plainly and
      // let them reload to get a fully consistent board.
      if (d.board_version) {
        if (window._boardVersion == null) {
          window._boardVersion = d.board_version;
        } else if (d.board_version !== window._boardVersion) {
          var bar = document.getElementById('staleBoardBar');
          if (bar) bar.style.display = 'flex';
        }
      }
    }).catch(function() {
      document.getElementById('sched-pipeline').textContent = '6am ET daily';
      document.getElementById('sched-refresh').textContent  = 'after 6am pipeline';
    });
  }
  loadSchedule();
  setInterval(loadSchedule, 60000);
})();
</script>

<div class="ticker-wrap" id="scoreTicker">
  <div class="ticker-label">⚾ Scores</div>
  <div class="ticker-outer">
    <div class="ticker-track" id="tickerTrack">
      <span class="ticker-empty">No completed games yet today</span>
    </div>
  </div>
</div>

<div class="filters">
  <div class="filter-group">
    <span class="filter-label">Bet Type</span>
    <button class="filter-btn active" data-group="type" data-val="all">All</button>
    <button class="filter-btn" data-group="type" data-val="ML">Win Bet (Moneyline)</button>
    <button class="filter-btn" data-group="type" data-val="RL">Spread (Run Line)</button>
    <button class="filter-btn" data-group="type" data-val="TOTAL">Over/Under</button>
  </div>
  <div class="filter-group">
    <span class="filter-label">Confidence</span>
    <button class="filter-btn active" data-group="tier" data-val="all">All</button>
    <button class="filter-btn" data-group="tier" data-val="HIGHCONF">🔥 High Confidence</button>
    <button class="filter-btn" data-group="tier" data-val="PROFIT">📈 Profitable</button>
    <button class="filter-btn" data-group="tier" data-val="LOCK">🔒 Lock — Best Bets</button>
    <button class="filter-btn" data-group="tier" data-val="STRONG">⭐⭐ Strong</button>
    <button class="filter-btn" data-group="tier" data-val="LEAN">⭐ Lean — Watch Only</button>
    <button class="filter-btn" data-group="tier" data-val="TOSSUP">≈ Toss Up</button>
  </div>
  <div class="search-wrap">
    <input class="search-input" id="teamSearch" placeholder="Search team…" type="text"/>
  </div>
</div>

<div class="main">

  <!-- SECTION NAV -->
  <div class="section-nav">
    <button class="section-nav-btn active" data-panel="panel-picks">🎯 Game Picks</button>
    <button class="section-nav-btn" data-panel="panel-schedule">📅 Today's Games</button>
    <button class="section-nav-btn" data-panel="panel-props">👤 Player Props</button>
    <button class="section-nav-btn" data-panel="panel-teams">⚾ Teams</button>
    <button class="section-nav-btn" data-panel="panel-games">📊 Game Breakdown</button>
    <button class="section-nav-btn" data-panel="panel-yesterday" id="yesterdayTab" style="display:none">📈 Yesterday</button>
    <button class="section-nav-btn" data-panel="panel-sharp">🔥 Sharp Action</button>
    <button class="section-nav-btn" data-panel="panel-hr-watch">💣 HR Watch</button>
    <button class="section-nav-btn" data-panel="panel-monte-carlo">🎲 Monte Carlo</button>
    <button class="section-nav-btn" data-panel="panel-daily-summary">📋 Daily Summary</button>
    <button class="section-nav-btn" data-panel="panel-trends">📊 Stats &amp; Trends</button>
  </div>

  <!-- PANEL: STATS & TRENDS -->
  <div class="section-panel" id="panel-trends">
    <div class="section-title">📊 Stats &amp; Trends <span style="font-size:.7rem;color:var(--sub);font-weight:500">— last 21 days of graded picks</span></div>
    <div id="trendsContent"></div>
  </div>

  <!-- PANEL: GAME PICKS -->
  <div class="section-panel active" id="panel-picks">
    <div class="yday-bankroll-wrap">
      <div class="tracker-widget" id="trackerWidget"></div>
      <div id="yesterdayBanner"></div>
      <div class="bankroll-widget" id="bankrollWidget">
        <div class="bk-title">💰 My Bankroll</div>
        <div class="bk-input-row">
          <span class="bk-dollar">$</span>
          <input type="number" id="bankrollInput" class="bk-input" placeholder="1000" min="1" step="100">
        </div>
        <div class="bk-unit-row" id="bkUnitRow">1 unit = <strong id="bkUnit">$10.00</strong></div>
        <div class="bk-note">Enter your bankroll — Kelly bets &amp; profits update everywhere.</div>
      </div>
    </div>
    <div class="section-title">🎯 Individual Picks</div>
    <div class="results-count" id="pickResults"></div>
    <div id="todayInProgressBanner" style="display:none;background:#1c2128;border:1px solid #30363d;border-left:3px solid #f59e0b;border-radius:8px;padding:12px 16px;margin-bottom:12px;color:#8b949e;font-size:.85rem">
      Every game today has started &mdash; nothing left to bet. Results are in
      <strong style="color:#f59e0b">Daily Summary</strong>; use the date buttons
      above for tomorrow's slate.
    </div>
    <div id="hcTrackRecord" style="margin:0 0 14px;display:none;gap:10px;flex-wrap:wrap"></div>
    <details class="calc-wrap">
      <summary class="calc-sum"><span class="calc-caret">&#9656;</span>
        <b>Calculators</b><span class="calc-sub">de-vig &middot; stake sizing &middot; odds conversion</span></summary>
      <div class="calc-body">
        <div class="calc-card">
          <div class="calc-h">Remove the vig</div>
          <div class="calc-in2">
            <input id="dvA" class="bb-price-in" placeholder="side A e.g. -154" oninput="calcDevig()">
            <input id="dvB" class="bb-price-in" placeholder="side B e.g. +130" oninput="calcDevig()">
          </div>
          <div id="dvOut" class="calc-out"><span class="calc-mute">enter both sides</span></div>
        </div>
        <div class="calc-card">
          <div class="calc-h">What should I actually bet?</div>
          <div class="calc-in3">
            <input id="stP" class="bb-price-in" placeholder="win % e.g. 66.2" oninput="calcStake()">
            <input id="stPrice" class="bb-price-in" placeholder="price e.g. -154" oninput="calcStake()">
            <input id="stBR" class="bb-price-in" placeholder="bankroll e.g. 20" oninput="calcStake()">
          </div>
          <div id="stOut" class="calc-out"><span class="calc-mute">quarter-Kelly, $1 grid, $5 cap</span></div>
        </div>
        <div class="calc-card">
          <div class="calc-h">Odds conversion</div>
          <div class="calc-in2"><input id="cvIn" class="bb-price-in" placeholder="e.g. -154" oninput="calcConvert()"></div>
          <div id="cvOut" class="calc-out"><span class="calc-mute">enter a price</span></div>
        </div>
      </div>
    </details>
    <div id="bestBets"></div>
    <div class="picks-grid" id="picksGrid"></div>

    <!-- Sharp Money Panel — only shown when movement data exists -->
    <div id="sharpMoneySection" style="display:none">
      <div class="section-title">💰 Where the Pro Money Is Going</div>
      <div class="sharp-panel" id="sharpMoneyGrid"></div>
    </div>

    <!-- LEAN PICKS -- collapsed by default -->
    <div id="leanSection" style="margin-top:4px;margin-bottom:8px">
      <div id="leanToggle" onclick="toggleLean()" style="cursor:pointer;color:var(--sub);font-size:.78rem;padding:8px 0;user-select:none;display:none">
        <span id="leanArrow">▶</span> <span id="leanLabel">Show Lean picks</span>
      </div>
      <div id="leanBody" style="display:none">
        <div class="picks-grid" id="leanGrid"></div>
      </div>
    </div>

    <!-- SURFACED PROPS -- BETTABLE props only (real book line + price). -->
    <!-- Fixed 2026-08-11: this surface used to ignore projection_only and was -->
    <!-- filling with Hits/TB/RBI props priced off a fabricated 0.5 or 1.5 line. -->
    <div id="surfacedPropsSection" style="display:none;margin-bottom:16px">
      <div class="section-title">👤 Top Props <span style="font-size:.72rem;font-weight:400;color:var(--sub)">— real sportsbook lines only</span></div>
      <div class="picks-grid" id="surfacedPropsGrid"></div>
      <div id="surfacedPropsEmpty" style="display:none;background:rgba(255,183,77,.07);border:1px solid rgba(255,183,77,.25);border-radius:8px;padding:14px 16px;font-size:.82rem;color:var(--sub);line-height:1.5"></div>
    </div>

    <div class="section-title">🎯 Thematic Parlays</div>
    <div style="font-size:.78rem;color:var(--sub);margin:-8px 0 12px">Grouped by shared thesis — each parlay has a single reason to exist.</div>
    <div class="parlay-grid" id="thematicGrid"></div>

    <div class="section-title" style="margin-top:24px">🔥 Best Parlays by Confidence</div>
    <div class="section-tabs">
      <button class="section-tab active" data-parlay="2">2-Leg (+260)</button>
      <button class="section-tab" data-parlay="3">3-Leg (+595)</button>
    </div>
    <div class="parlay-grid" id="parlayGrid"></div>
  </div>

  <!-- PANEL: TODAY'S GAMES -->
  <div class="section-panel" id="panel-schedule">
    <div class="schedule-grid" id="scheduleGrid"></div>
  </div>

  <!-- PANEL: PLAYER PROPS -->
  <div class="section-panel" id="panel-props">
    <div class="props-filter-row">
      <span class="filter-label">Prop Type</span>
      <button class="filter-btn active" data-pgroup="ptype" data-pval="all">All</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="HR">⚡ HRs</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="HITS">🎯 Hits</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="TB">💥 Tot Bases</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="RBI">🏅 RBIs</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="R">🏃 Runs</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="SB">💨 Stolen Bases</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="K">🔥 Strikeouts</button>
      <span class="filter-label" style="margin-left:12px">Confidence</span>
      <button class="filter-btn active" data-pgroup="ptier" data-pval="all">All</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="LOCK">🔒 Lock</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="STRONG">⭐⭐ Strong</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="LEAN">⭐ Lean</button>
    </div>
    <div id="propsBanner" style="display:none;background:rgba(255,183,77,.08);border:1px solid rgba(255,183,77,.2);border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:.82rem;color:#ffb74d">
      📋 <b>Projected lineups</b> — props are based on last known lineups.
      Confirmed props update at <span id="propsRefreshTime">lineup refresh</span>.
    </div>
    <div class="results-count" id="propResults"></div>
    <div class="props-grid" id="propsGrid"></div>
  </div>

  <!-- PANEL: TEAMS -->
  <div class="section-panel" id="panel-teams">
    <div class="section-title">⚾ Team Lookup</div>
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:12px;flex-wrap:wrap">
      <span style="font-size:.75rem;color:var(--sub)">
        <span style="color:var(--green);font-weight:700">●</span> Playing today
        &nbsp;&nbsp;
        <span style="color:var(--sub);font-weight:700">●</span> Off today
      </span>
      <span style="font-size:.72rem;color:var(--sub)">Lineups post ~2–3 hrs before first pitch · Props appear once confirmed</span>
    </div>
    <div id="teamButtonGrid" style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px"></div>
    <div id="teamGameView"></div>
  </div>

  <!-- PANEL: GAME BREAKDOWN -->
  <div class="section-panel" id="panel-games">
    <div class="section-title">📊 Game Breakdown</div>
    <div class="games-grid" id="gamesGrid"></div>
  </div>

  <!-- PANEL: YESTERDAY'S RESULTS -->
  <div class="section-panel" id="panel-yesterday">
    <div id="yesterdayFull"></div>
    <div id="learnFromYesterday"></div>
  </div>

  <!-- PANEL: SHARP ACTION -->
  <div class="section-panel" id="panel-sharp">
    <div id="sharpActionContent"></div>
  </div>

  <!-- PANEL: HR WATCH -->
  <div class="section-panel" id="panel-hr-watch">
    <div class="section-title">💣 HR Watch &mdash; Top Candidates Today</div>
    <p style="color:#8b949e;font-size:.83rem;margin-bottom:16px">Ranked by Poisson HR probability. Red = unfavorable signal, green = favorable. Only confirmed lineups shown.</p>
    __HR_WATCH__
  </div>

  <!-- PANEL: MONTE CARLO -->
  <div class="section-panel" id="panel-monte-carlo">
    <div class="section-title">🎲 Monte Carlo &mdash; Hit-Rate Simulation (1000 Trials)</div>
    __MONTE_CARLO__
  </div>

  <div class="section-panel" id="panel-daily-summary">
    <div id="dailySummaryContent"></div>
  </div>

</div>

<script>
// ── Embedded Data ────────────────────────────────────────────────────────────
const DATA_DATE      = "__DATE__";
const DATA_PICKS     = __PICKS__;
const DATA_TODAY_PICKS = __TODAY_PICKS__;   // full today slate for Daily Summary
const DATA_GAMES     = __GAMES__;
const DATA_TEAM_SCHED = __TEAM_SCHED__;
const DATA_P2        = __P2__;
const DATA_P3        = __P3__;
const DATA_THEMATIC      = __THEMATIC__;
const DATA_THEMATIC_NEXT = __THEMATIC_NEXT__;
const DATA_SCORES    = __SCORES__;
const DATA_PROPS     = __PROPS__;
const DATA_SCHEDULE  = __SCHEDULE__;
const DATA_YESTERDAY     = __YESTERDAY__;
const DATA_MOVEMENT      = __MOVEMENT__;
const DATA_PROJ_LINEUPS  = __PROJ_LINEUPS__;
// Tomorrow slate
const DATA_NEXT_DATE     = "__NEXT_DATE__";
const DATA_PICKS_NEXT    = __PICKS_NEXT__;
const DATA_P2_NEXT       = __P2_NEXT__;
const DATA_P3_NEXT       = __P3_NEXT__;
const DATA_SCHEDULE_NEXT = __SCHEDULE_NEXT__;
// Active slate pointers (reassigned by switchSlate)
let currentSlate    = "today";
let ACTIVE_PICKS    = DATA_PICKS;
let ACTIVE_P2       = DATA_P2;
let ACTIVE_P3       = DATA_P3;
let ACTIVE_THEMATIC = DATA_THEMATIC;
let ACTIVE_SCHEDULE = DATA_SCHEDULE;
let ACTIVE_DATE     = DATA_DATE;

// ── State ────────────────────────────────────────────────────────────────────
let filterType = "all", filterTier = "all", filterTeam = "";
let showParlay = 2;

// ── Init ─────────────────────────────────────────────────────────────────────
const _picksDate = new Date(DATA_DATE + "T12:00:00");
const _today = new Date();
_today.setHours(12,0,0,0);
const _diffDays = Math.round((_picksDate - _today) / 86400000);
const _dateLabel = _picksDate.toLocaleDateString("en-US",
  {weekday:"long",year:"numeric",month:"long",day:"numeric"});
const _tomorrowBadge = _diffDays === 1
  ? ' <span style="background:#f59e0b;color:#000;font-size:.7rem;font-weight:700;padding:2px 10px;border-radius:10px;vertical-align:middle;margin-left:8px;letter-spacing:.05em">TOMORROW</span>'
  : '';
document.getElementById("dateStr").innerHTML = _dateLabel + _tomorrowBadge;

document.getElementById("gameCount").textContent = DATA_GAMES.length;
document.getElementById("pickCount").textContent = ACTIVE_PICKS.length;
document.getElementById("lockCount").textContent = ACTIVE_PICKS.filter(p=>p.tier==="LOCK").length;
if(ACTIVE_PICKS.length){
  const tp = ACTIVE_PICKS[0];
  const awayCity = tp.away ? tp.away.split(" ").slice(-1)[0] : "";
  const homeCity = tp.home ? tp.home.split(" ").slice(-1)[0] : "";
  const gameCtx  = (awayCity && homeCity) ? ` · ${awayCity} @ ${homeCity}` : "";
  document.getElementById("topPick").innerHTML =
    `<span style="color:var(--gold)">${tp.label} (${tp.conf}%)</span><span style="color:var(--sub);font-size:.75rem">${gameCtx}</span>`;
}

// ── Render Picks ─────────────────────────────────────────────────────────────
// ── Slate switching (Today / Tomorrow) ─────────────────
function switchSlate(slate){
  currentSlate    = slate;
  ACTIVE_PICKS    = slate === "today" ? DATA_PICKS    : DATA_PICKS_NEXT;
  ACTIVE_P2       = slate === "today" ? DATA_P2       : DATA_P2_NEXT;
  ACTIVE_P3       = slate === "today" ? DATA_P3       : DATA_P3_NEXT;
  ACTIVE_THEMATIC = slate === "today" ? DATA_THEMATIC : DATA_THEMATIC_NEXT;
  ACTIVE_SCHEDULE = slate === "today" ? DATA_SCHEDULE : DATA_SCHEDULE_NEXT;
  ACTIVE_DATE     = slate === "today" ? DATA_DATE     : DATA_NEXT_DATE;
  // Update the header date when switching slates
  const _slateDate = new Date(ACTIVE_DATE + "T12:00:00");
  const _slateLabel = _slateDate.toLocaleDateString("en-US",{weekday:"long",year:"numeric",month:"long",day:"numeric"});
  const _slateTomorrow = slate === "tomorrow" ? ' <span style="background:#f59e0b;color:#000;font-size:.7rem;font-weight:700;padding:2px 10px;border-radius:10px;vertical-align:middle;margin-left:8px;letter-spacing:.05em">TOMORROW</span>' : '';
  document.getElementById("dateStr").innerHTML = _slateLabel + _slateTomorrow;
  document.querySelectorAll(".date-btn").forEach(b=>{
    b.classList.toggle("active", b.dataset.slate === slate);
  });
  document.getElementById("pickCount").textContent = ACTIVE_PICKS.length;
  document.getElementById("lockCount").textContent = ACTIVE_PICKS.filter(p=>p.tier==="LOCK").length;
  if(ACTIVE_PICKS.length){
    const tp = ACTIVE_PICKS[0];
    const awayCity = tp.away ? tp.away.split(" ").slice(-1)[0] : "";
    const homeCity = tp.home ? tp.home.split(" ").slice(-1)[0] : "";
    const gameCtx  = (awayCity && homeCity) ? ` · ${awayCity} @ ${homeCity}` : "";
    document.getElementById("topPick").innerHTML =
      `<span style="color:var(--gold)">${tp.label} (${tp.conf}%)</span><span style="color:var(--sub);font-size:.75rem">${gameCtx}</span>`;
  } else {
    document.getElementById("topPick").textContent = "—";
  }
  renderPicks();
  renderSchedule();
  renderThematic();
  renderParlays();
  selectedLegs = [];
  updateParlayDrawer();
}

(function initDateToggle(){
  const el = document.getElementById("dateToggle");
  if(!el) return;
  const fmtDate = d => new Date(d + "T12:00:00").toLocaleDateString("en-US",{weekday:"short",month:"short",day:"numeric"});
  // Guard against both buttons landing on the same day. That is always a data
  // fault upstream (it was score_today pivoting past pivot=False), but a toggle
  // offering the same date twice is a dead control, so refuse to draw it.
  if(DATA_NEXT_DATE && DATA_NEXT_DATE !== DATA_DATE){
    el.innerHTML = `
      <button class="date-btn active" data-slate="today"    onclick="switchSlate('today')"   >${fmtDate(DATA_DATE)}</button>
      <button class="date-btn"        data-slate="tomorrow" onclick="switchSlate('tomorrow')">${fmtDate(DATA_NEXT_DATE)} &rarr;</button>
    `;
  } else {
    el.innerHTML = `<button class="date-btn active" data-slate="today">${fmtDate(DATA_DATE)}</button>`;
  }
  // NO AUTO-SWITCH (removed 2026-08-17, at Justin's call).
  //
  // This used to silently flip to tomorrow's slate the moment today's bettable
  // count hit zero. Combined with score_today ignoring pivot=False, the result
  // late on 08-17 was tomorrow's games rendered under today's heading with both
  // date buttons reading "Tue, Aug 18".
  //
  // Today's grid now simply empties as games start, which is the honest state:
  // there is nothing left to bet. Tomorrow is one deliberate click away.
  if(DATA_PICKS.length === 0){
    const _todayBanner = document.getElementById("todayInProgressBanner");
    if(_todayBanner) _todayBanner.style.display = "block";
  }
})();

// Format American odds integer: +150 or -110
function fmtOdds(o){ if(o == null) return null; o = Math.round(o); return o > 0 ? "+"+o : String(o); }

// Convert model confidence (0-1) to implied American odds
function confToAmerican(conf){
  const p = conf / 100;
  if(p <= 0 || p >= 1) return null;
  if(p > 0.5) return Math.round(-100 * p / (1 - p));
  return Math.round(100 * (1 - p) / p);
}

// Show sportsbook odds on pick card (away vs home ML line)
function oddsHtml(p){
  if(p.ml_away_odds == null && p.ml_home_odds == null) return "";
  const ao = fmtOdds(p.ml_away_odds);
  const ho = fmtOdds(p.ml_home_odds);
  // Determine which side is picked for ML/RL picks
  const awayTeam = (p.away||"").split(" ").slice(-1)[0];
  const homeTeam = (p.home||"").split(" ").slice(-1)[0];
  const pickedAway = p.type !== "TOTAL" && p.team && p.team.toLowerCase().includes(awayTeam.toLowerCase());
  const aoClass = pickedAway ? "odds-picked" : "";
  const hoClass = !pickedAway && p.type !== "TOTAL" ? "odds-picked" : "";
  return `<div class="odds-row">
    <span class="odds-label">Sportsbook ML</span>
    ${ao ? `<span class="odds-pill ${aoClass}">${awayTeam} ${ao}</span>` : ""}
    ${ho ? `<span class="odds-pill ${hoClass}">${homeTeam} ${ho}</span>` : ""}
  </div>`;
}

// Market VALUE / EV row — de-vigged price awareness. Confidence says how likely;
// value says whether the PRICE is wrong. Chalk = heavy favorite, value scarce.
// ── HONEST READ (2026-08-11) ────────────────────────────────────────────────
// The big number on a card is the model's stated confidence, which across 121
// graded ML picks averaged 71.9% while realizing 49.6%. So the card was
// answering "how sure is the model" when the useful question is "is the honest
// probability above what this price charges me".
//
//   calibrated  = stated conf mapped through the Platt fit onto realized results
//   breakeven   = what the actual price demands to break even
//   verdict     = calibrated minus breakeven
//
// This is the read, not a filter. Every pick still shows; you decide.
function honestReadHtml(p){
  if(p.cal_conf==null){
    // No calibration for this bet type yet. Say so plainly. A missing number is
    // information; a borrowed one from another bet type would be a lie.
    if(p.type==="RL"){
      const be = (p.breakeven!=null) ? ` Price needs <b>${p.breakeven}%</b>.` : "";
      // RL is intentionally uncalibrated. Fitted 2026-08-11 on 229 graded run
      // lines: stated 58.9% vs actual 55.0%, only a 3.9 point gap, and the
      // out-of-sample Brier got WORSE when a curve was applied. There is little
      // miscalibration to correct, which is what you expect from the one bet
      // type already gated on EV against a real price.
      return `<div class="honest-read hr-play">
        <span class="hr-title">HONEST READ &middot; ROUGHLY CALIBRATED</span>
        <span class="hr-body">Run line confidence has been close to honest:
          <b>126-103 (55.0%)</b> since 2026-07-21 against a stated 58.9%, a gap of
          under 4 points. No correction applied because fitting one made
          out-of-sample accuracy worse.${be}
          <br><span style="color:#8b949e">Above the 52.4% break-even, though n=229
          is not yet statistically conclusive (p=0.23).</span></span></div>`;
    }
    return "";
  }
  if(p.breakeven==null){
    return `<div class="honest-read hr-none">
      <span class="hr-title">HONEST READ</span>
      <span class="hr-body">Model ${p.conf}% &rarr; calibrated <b>${p.cal_conf}%</b>
      &middot; no clean price, so no breakeven to compare against</span></div>`;
  }
  if(p.type==="TOTAL"){
    // Structural, not a per-pick judgement: total_conf is hard capped at 0.68 in
    // mlb_model.py and the calibrated value at that cap is 48.8%, under the
    // 52.38% break-even. No total the model can currently produce is +EV. The
    // fitted slope is also nearly flat (30 pts of stated conf -> under 7 pts of
    // real probability), so the number barely ranks. Record 72-83 (46.5%).
    return `<div class="honest-read hr-avoid">
      <span class="hr-title">HONEST READ &middot; NOT A BET</span>
      <span class="hr-body">Model ${p.conf}% &rarr; calibrated <b>${p.cal_conf}%</b>.
        Totals confidence is capped at 68%, which calibrates to 48.8% — below the
        52.4% break-even, so <b>no total can currently clear it</b>. Record 72-83
        (46.5%) since 2026-07-21.
        <br><span style="color:#8b949e">Shown for research. The run projection
        needs rebuilding before totals are bettable.</span></span></div>`;
  }
  const m = +(p.cal_conf - p.breakeven).toFixed(1);
  const v = p.read_verdict || "";
  const cls = v==="PLAYABLE" ? "hr-play" : v==="THIN" ? "hr-thin"
            : v==="NO EDGE"  ? "hr-none" : "hr-avoid";
  const evTxt = (p.honest_ev==null) ? "" :
    ` &middot; EV ${p.honest_ev>0?"+":""}${(p.honest_ev*100).toFixed(1)}%`;
  const priceStr = (p.pick_price==null) ? "" :
    ` at ${p.pick_price>0?"+":""}${Math.round(p.pick_price)}`;
  return `<div class="honest-read ${cls}">
    <span class="hr-title">HONEST READ &middot; ${v}</span>
    <span class="hr-body">
      Model says <b>${p.conf}%</b> &rarr; calibrated <b>${p.cal_conf}%</b>.
      Price${priceStr} needs <b>${p.breakeven}%</b>.
      <b>${m>0?"+":""}${m} pts</b> ${m>=0?"above":"below"} breakeven${evTxt}.
    </span></div>`;
}

function valueHtml(p){
  // Run line is a rebuilt, unproven market sitting on a shaky price feed. Do NOT
  // show a loud VALUE/EV number for it yet — it reads as a fake edge. Show the
  // model's cover read and price, flagged as unproven, until it earns trust.
  if(p.type==="RL"){
    const priceStr = (p.pick_price==null) ? "no clean price" :
      `@ ${p.pick_price>0?"+":""}${Math.round(p.pick_price)}`;
    return `<div class="value-row">
      <span class="val-unproven">RUN LINE · UNPROVEN</span>
      <span class="value-meta">Model cover ${p.conf}% ${priceStr} · tracking results, no verdict yet</span>
    </div>`;
  }
  if(!p.value_tag) return "";
  const col = p.value_tag==="VALUE" ? "var(--green)"
            : p.value_tag==="NO VALUE" ? "var(--red)" : "var(--sub)";
  const evStr  = (p.value_ev==null) ? "—" : (p.value_ev>=0?"+":"")+(p.value_ev*100).toFixed(1)+"%";
  const mktStr = (p.market_prob==null) ? "—" : Math.round(p.market_prob*100)+"%";
  const priceStr = (p.pick_price==null) ? "" :
      ` @ ${p.pick_price>0?"+":""}${Math.round(p.pick_price)}`;
  const chalk = p.value_chalk
    ? `<span class="val-chalk" title="Heavy favorite — value is scarce at this price">CHALK</span>` : "";
  return `<div class="value-row">
    <span class="value-tag" style="color:${col};border-color:${col}">${p.value_tag}${priceStr}</span>
    <span class="value-ev" style="color:${col}">EV ${evStr}</span>
    <span class="value-meta">Model ${p.conf}% vs Market ${mktStr}</span>
    ${chalk}
  </div>`;
}

// Per-card plain-English honest read. Encodes the framework so you don't have
// to: market breakeven vs price, model-vs-market gap credibility, calibration
// band, chalk warning, and a bottom-line verdict. Pure function of the pick's
// own numbers — no API, instant.
function buildAnalysis(p){
  const fmtP = x => (x==null ? "—" : Math.round(x*100)+"%");
  const price = p.pick_price;
  const priceStr = (price==null) ? "" : (price>0?"+":"")+Math.round(price);

  // Run line gets an honest, muted read — it's a rebuilt market on a shaky price
  // feed, so no EV verdict yet regardless of what the raw number says.
  if(p.type==="RL"){
    const cov = Math.round(p.conf);
    const pr = (price==null) ? "The price feed for this game looks unreliable (close games make books disagree on the favorite), so there's no trustworthy price to value it against."
      : `Priced ${priceStr}.`;
    return `<div class="analysis-body">The model reads this run line as about a <b>${cov}%</b> cover.
      ${pr} But the run line was just rebuilt and its run-margin projections still run hot, so treat this
      number as information, not an edge. It's kept out of Best Bets on purpose and won't get a verdict until
      a couple weeks of graded results prove it out. <b>Bottom line:</b> watch, don't bet it on these numbers.</div>`;
  }

  // No price -> can't judge value (TOSSUP / unpriced)
  if(price==null || !p.value_tag){
    return `<div class="analysis-body">The model sees this as close to a coin flip and
      there's no clean market price attached, so there's no value edge to lean on.
      Not a spot the numbers argue for.</div>`;
  }

  const model = p.conf;                              // 0-100
  const market = (p.market_prob==null) ? null : p.market_prob*100;
  const ev = (p.value_ev==null) ? null : p.value_ev*100;
  const be = price<0 ? Math.abs(price)/(Math.abs(price)+100)*100
                     : 100/(price+100)*100;
  const gap = (market==null) ? null : (model - market);
  const S = [];

  // 1) market read + breakeven
  if(market!=null){
    S.push(`The market prices this around <b>${Math.round(market)}%</b>. At ${priceStr} you need <b>${be.toFixed(1)}%</b> just to break even.`);
    if(market < be)
      S.push(`By the market's own read that's a losing number — the entire edge here is your model's <b>${Math.round(model)}%</b>.`);
    else
      S.push(`The market already clears that, and your model is a bit higher at <b>${Math.round(model)}%</b>.`);
  }

  // 2) gap credibility
  if(gap!=null){
    if(gap >= 15) S.push(`Your model sits <b>${Math.round(gap)} points</b> above the market. A gap that wide is usually the model running hot, not a real edge — be suspicious.`);
    else if(gap >= 8) S.push(`Your model is <b>${Math.round(gap)} points</b> above the market: a moderate edge worth a look, not a lock.`);
    else if(gap >= 0) S.push(`Your model is only a few points above the market, which is a believable, honest edge.`);
    else S.push(`Your model is actually <b>below</b> the market here — you'd be betting against the sharper number.`);
  }

  // 3) calibration band
  if(model >= 85) S.push(`At ${Math.round(model)}% this lands in the 85-90% band, the one range your calibration actually trusts.`);
  else if(model >= 75) S.push(`At ${Math.round(model)}% this is the 75-84% band, where the model has been overconfident (predicted ~80%, hit ~64%). Discount it.`);
  else if(model >= 70) S.push(`At ${Math.round(model)}% this is the 70-75% band, historically your worst (predicted ~73%, hit ~49%). Be skeptical.`);

  // 4) chalk
  if(p.value_chalk) S.push(`And it's chalk: you'd lay ${priceStr} to win $100, so one loss erases several wins.`);

  // 5) type caution
  if(p.type==="RL") S.push(`This is the rebuilt run line — still unproven. Give it a couple weeks of graded results before trusting the number.`);
  else if(p.type==="TOTAL") S.push(`Totals are currently overprojecting, so treat a big total edge as suspect until that's fixed.`);

  // 6) bottom line
  let bl;
  if(p.value_tag==="NO VALUE") bl = `<b>Bottom line:</b> even the model doesn't beat this price. Pass.`;
  else if(ev!=null && ev > 25) bl = `<b>Bottom line:</b> the +${ev.toFixed(0)}% EV is almost certainly the model overreaching, not free money. Skip or tiny.`;
  else if(p.value_tag==="FAIR") bl = `<b>Bottom line:</b> priced about right, no real edge. Pass unless you love it.`;
  else if(p.value_tag==="VALUE"){
    if(model >= 85 && !p.value_chalk && p.type==="ML") bl = `<b>Bottom line:</b> real edge, trustworthy band, sane price. This is the kind of spot to actually play.`;
    else if(!p.value_chalk && gap!=null && gap < 15 && p.type==="ML") bl = `<b>Bottom line:</b> a credible value lean. Playable at a small size.`;
    else bl = `<b>Bottom line:</b> shows value, but on shaky footing (chalk, inflated model, or an unproven market). Size down or pass.`;
  } else bl = `<b>Bottom line:</b> go by the reasoning above.`;
  S.push(bl);

  return `<div class="analysis-body">${S.join(" ")}</div>`;
}

// ── BEST BETS ─────────────────────────────────────────────────────────────
// The model does the filtering so you don't have to. A pick qualifies only if
// it's ML, priced no worse than the ceiling, the model-vs-market gap is credible
// (not a mirage), and the EV is still positive when we blend the model 50/50 with
// the market (which strips out the model's known overconfidence). Ranked by that
// honest blended EV — deep chalk and 20-point mirages can never make the list.
const BEST_BET_PRICE_FLOOR = -180;   // most chalk allowed (adjust to taste)
const BEST_BET_MAX_GAP     = 15;     // points above market before it's a mirage
const BEST_BET_TYPES       = ["ML"]; // ML path below; RL has its own rule

// ── RUN LINE BEST BETS (2026-08-11) ──────────────────────────────────────────
// The run line is the ONLY bet type with a walk-forward validated edge:
// threshold chosen on the first half of the period, applied to the second half
// it never saw, returned 51-21 (70.8%), p=0.0011. It is also a broad plateau
// across adjacent thresholds rather than one lucky cut, which is what makes it
// believable.
//
// RL is deliberately NOT Platt-calibrated (its fit made out-of-sample Brier
// worse), so instead of a curve we use the ACTUAL observed rate for the band
// the pick falls in, straight from /admin/calibration-fit, and require positive
// EV against the real price.
//
//   conf band   actual record   rate
//   55-60%      40-35           53.3%
//   60-65%      29-14           67.4%
//   65-70%      31-18           63.3%   <- excluded, see below
//
// Band ceiling is 65%. Above that you approach the 0.68 cover cap where the
// record collapses to 15-14 (51.7%), and prices there are typically -180 or
// worse. Floor is 57% because 55-60% only returns 53.3%, which most prices eat.
// ── BANDS REFIT 2026-08-17 on n=296 RL / n=360 ML graded picks ─────────────
// The old window was 57-65% fitted on n=94. With three times the data the
// picture sharpened: 55-60% is a coin flip (51.6%) and does not belong, while
// 65-70% is strong (64.8%) and was being thrown away. Net effect is a WIDER
// usable band, 60-70%, not a narrower one.
const RL_BEST_BET_MIN  = 0.60;
const RL_BEST_BET_MAX  = 0.70;
const RL_BAND_RATES = [
  { lo: 0.60, hi: 0.65,  rate: 0.662, rec: "43-22", n: 65, be: 0.574 },
  { lo: 0.65, hi: 0.701, rate: 0.648, rec: "46-25", n: 71, be: 0.625 }
];

// ── MONEYLINE: OBSERVED RECORD, NOT THE FITTED CURVE (2026-08-17) ──────────
// Best Bets used to score ML off the Platt-calibrated probability. That curve
// is monotonic by construction, but the real ML record is not:
//
//   stated 55-60% -> 35.0% actual    stated 70-75% -> 65.6% actual
//   stated 60-65% -> 41.9% actual    stated 75-80% -> 48.4% actual
//   stated 65-70% -> 54.0% actual    stated 80%+   -> 73.7% actual
//
// A smooth rising line cannot fit a zig-zag, so it splits the difference and
// is wrong everywhere. Its practical effect: the first stated confidence whose
// calibrated value clears the 52.38% break-even is 69%, and the model rarely
// produces an ML above 65%. So Best Bets returned nothing essentially every
// day. An empty shortlist is not caution, it is a broken instrument.
//
// Replaced with the WALK-FORWARD VALIDATED threshold: choose the cut on the
// first half of the period, apply it to the second half it never saw.
// ML >= 68% returned 30-24 (55.6%) out of sample. We use that 55.6%, NOT the
// 60.2% in-sample rate, because the in-sample number is the one the threshold
// was chosen on and would overstate the edge.
//
// At 55.6% the break-even price is -125, so an ML at 68%+ is a bet only when
// priced better than that. The EV check still does the real work.
// SUPERSEDED THE SAME DAY by /admin/real-roi. The 68% threshold came from a
// walk-forward backtest that assumed a flat -110. At the prices actually paid,
// ML 70-75% averaged -154 and 75%+ averaged -163, and the type lost 13.5% over
// 82 priced picks. A threshold on confidence alone cannot see that, because the
// thing that kills ML is the PRICE, not the pick. Kept only for display.
const ML_MIN_CONF  = 0.68;
const ML_OOS_RATE  = 0.556;
const ML_OOS_REC   = "30-24 (55.6%) out of sample";

// Observed TOTAL record per stated band, n=202 since 2026-07-21.
// Totals were excluded from Best Bets outright on the grounds that the 0.68
// confidence cap calibrates below break-even. /admin/real-roi says otherwise:
// at real prices totals returned +5.6% and +19.8% in the two bands that carry
// any volume. The a priori exclusion was costing money.
const TOTAL_BAND_RATES = [
  { lo: 0.50, hi: 0.55,  rate: 0.491, rec: "53-55", n: 108, be: 0.531 },
  { lo: 0.55, hi: 0.60,  rate: 0.620, rec: "31-19", n: 50, be: 0.506 },
  { lo: 0.60, hi: 0.65,  rate: 0.412, rec: "7-10",  n: 17  },
  { lo: 0.65, hi: 1.01,  rate: 0.467, rec: "7-8",   n: 15  }
];

// One table per bet type. A band is usable only with enough graded picks behind
// it; below MIN_BAND_N the rate is noise dressed as a probability.
const MIN_BAND_N = 30;

// ── TWO GUARDS, ADDED 2026-08-17 AFTER THE RULE FLIPPED A BET ON ITSELF ─────
//
// A pick sat at -116 all afternoon. The model rescored, confidence moved
// 54.5% -> 62.0%, and it crossed from the 50-55% band (58.9%) into the 60-65%
// band (41.9%). Same bet, same price: EV +9.7% -> -22%. Getting MORE confident
// made it a worse bet. Nothing was broken — the rule did what it was told.
//
// The fault is that these bands are a sawtooth, not a curve:
//     58.9   35.0   41.9   54.0   65.6   48.4   73.7
// Adjacent bands disagree by up to 24 points on n=31..80. That is sampling
// noise, and scoring off a 5-point band treats the noise as signal.
//
// GUARD 1 — PLATEAU. Trust a band only if an ADJACENT band agrees within
// PLATEAU_TOL. This is exactly the test the 2026-08-11 review used to accept
// the run line ("a broad plateau across 55-65%, not one lucky cut") and that I
// failed to apply to my own moneyline table. A lone spike next to a 24-point
// cliff is not an edge.
//
// GUARD 2 — PRICE PROFILE. A band's win rate was measured on picks at a
// particular PRICE. ML 60-65% was measured on picks averaging about -119
// (break-even 54.4%). Applying that same 41.9% to a +231 underdog produced
// "+38.7% EV" purely because the payout is larger. That is the same error as
// pooling run line prices across handicaps, one level up: a number measured on
// one population applied to another. So require the pick's break-even to sit
// within PRICE_TOL of the break-even the band was actually measured at.
const PLATEAU_TOL = 0.08;   // adjacent band must agree within 8 points
const PRICE_TOL   = 0.10;   // break-even within 10 points of the band's own

function bandFor(p, why){
  const R = r => { if(why) why.reason = r; return null; };
  const c = (p.conf||0)/100;
  const t = p.type;
  const tbl = t === "ML" ? ML_BAND_RATES
            : t === "RL" ? RL_BAND_RATES
            : t === "TOTAL" ? TOTAL_BAND_RATES : null;
  if(!tbl) return R("no band table for this bet type");
  const i = tbl.findIndex(x => c >= x.lo && c < x.hi);
  if(i < 0) return R("confidence sits outside every measured band");
  const b = tbl[i];
  if(b.n != null && b.n < MIN_BAND_N)
    return R(`band has only ${b.n} graded picks (need ${MIN_BAND_N})`);

  const nbrs = [tbl[i-1], tbl[i+1]].filter(x => x && (x.n == null || x.n >= MIN_BAND_N));
  if(!nbrs.some(x => Math.abs(x.rate - b.rate) <= PLATEAU_TOL))
    return R(`${Math.round(b.lo*100)}-${Math.round(b.hi*100)}% is an isolated spike — `
           + `neighbouring bands disagree by over ${Math.round(PLATEAU_TOL*100)} pts, so its `
           + `${(b.rate*100).toFixed(1)}% is probably noise`);

  if(b.be != null && p.pick_price != null && Math.abs(p.pick_price) >= 100){
    const d  = p.pick_price > 0 ? 1 + p.pick_price/100 : 1 + 100/Math.abs(p.pick_price);
    const be = 1/d;
    if(Math.abs(be - b.be) > PRICE_TOL)
      return R(`price is outside this band's measured profile (this bet needs `
             + `${(be*100).toFixed(1)}%, the band was measured near `
             + `${(b.be*100).toFixed(1)}%) — its win rate does not transfer`);
  }
  return b;
}

// Observed ML record per stated band. Used by the per-card price checker so a
// card tells you the truth about its own band even when it is not a Best Bet.
const ML_BAND_RATES = [
  { lo: 0.50, hi: 0.55, rate: 0.589, rec: "33-23", n: 56, be: 0.518 },
  { lo: 0.55, hi: 0.60, rate: 0.350, rec: "28-52", n: 80, be: 0.512 },
  { lo: 0.60, hi: 0.65, rate: 0.419, rec: "26-36", n: 62, be: 0.544 },
  { lo: 0.65, hi: 0.70, rate: 0.540, rec: "34-29", n: 63, be: 0.568 },
  { lo: 0.70, hi: 0.75, rate: 0.656, rec: "21-11", n: 32, be: 0.607 },
  { lo: 0.75, hi: 0.80, rate: 0.484, rec: "15-16", n: 31, be: 0.62 },
  { lo: 0.80, hi: 1.01, rate: 0.737, rec: "14-5",  n: 19 }
];

// Tiny stable hash so each Best Bet input gets its own id.
function hashStr(x){ let h=0; for(let i=0;i<(x||"").length;i++){ h=((h<<5)-h+x.charCodeAt(i))|0; } return h; }

// Verdict for a price the user typed. p is the band's OBSERVED rate.
// Requires +8% EV rather than break-even, because that rate is estimated from
// 94 graded picks and betting at the exact break-even leaves no room for the
// estimate being a couple of points off.
function bbCheck(id, p){
  const el = document.getElementById(id+"_in");
  const out = document.getElementById(id+"_out");
  const note = document.getElementById(id+"_note");
  const wrap = document.getElementById(id+"_wrap");
  if(!el || !out) return;
  const raw = (el.value||"").trim().replace(/[^0-9+\-]/g,"");
  const a = parseInt(raw,10);
  if(!raw || isNaN(a) || Math.abs(a) < 100){
    out.textContent = "enter a price"; out.className = "bb-verdict";
    wrap.className = "bb-rule";
    if(note) note.style.opacity = 1;
    return;
  }
  const d = a > 0 ? 1 + a/100 : 1 + 100/Math.abs(a);
  const ev = p*(d-1) - (1-p);
  const f = Math.max(0, Math.min(0.08, ((p*(d-1)-(1-p))/(d-1))/4));
  const bank = (typeof BANKROLL !== "undefined" && BANKROLL > 0) ? BANKROLL : null;
  if(ev >= 0.08){
    out.innerHTML = "&#10004; BET IT &nbsp;<span class='bb-v-sub'>"
      + (ev*100).toFixed(1) + "% EV &middot; stake " + (f*100).toFixed(1) + "%"
      + (bank ? " &middot; $" + (bank*f).toFixed(2) : "") + "</span>";
    out.className = "bb-verdict bb-v-yes";
    wrap.className = "bb-rule bb-rule-yes";
  } else if(ev > 0){
    out.innerHTML = "&#9888; TOO THIN &nbsp;<span class='bb-v-sub'>only "
      + (ev*100).toFixed(1) + "% EV at that price &middot; pass</span>";
    out.className = "bb-verdict bb-v-thin";
    wrap.className = "bb-rule bb-rule-thin";
  } else {
    out.innerHTML = "&#10008; PASS &nbsp;<span class='bb-v-sub'>"
      + (ev*100).toFixed(1) + "% EV &middot; price is too short</span>";
    out.className = "bb-verdict bb-v-no";
    wrap.className = "bb-rule bb-rule-no";
  }
  if(note) note.style.opacity = .5;
}

// ── LINE SHOPPING PER BOOK (2026-08-17) ─────────────────────────────────────
// The consensus price is not bettable anywhere — it is an average of numbers
// several different books offered. What matters is the best price YOU can
// actually reach, and how your own book compares to it.
//
// Books are requested BY NAME from the Odds API (<=10 bills as one region), so
// this costs nothing extra over the pull that was already happening. Hard Rock
// lives in region us2 and had never been requested, which is why it was absent.
const BOOK_LABELS = {
  hardrockbet: "Hard Rock", draftkings: "DraftKings", fanduel: "FanDuel",
  betmgm: "BetMGM", betrivers: "BetRivers", espnbet: "theScore",
  ballybet: "Bally", betparx: "betPARX", fliff: "Fliff", bovada: "Bovada"
};
const MY_BOOK = "hardrockbet";

function amDec(a){ return a > 0 ? 1 + a/100 : 1 + 100/Math.abs(a); }
function fmtAm(a){ return (a > 0 ? "+" : "") + Math.round(a); }

// Which per-book key corresponds to the bet on THIS card.
function bookKeyFor(p){
  if(p.type === "ML")    return (p.team === p.away) ? "ml_a" : "ml_h";
  if(p.type === "TOTAL") return ((p.label||"").toUpperCase().indexOf("OVER") >= 0) ? "ov" : "un";
  if(p.type === "RL"){
    const side = (p.team === p.away) ? "a" : "h";
    const hcap = ((p.label||"").indexOf("+1.5") >= 0) ? "p15" : "m15";
    return "rl_" + side + "_" + hcap;
  }
  return null;
}

function bookShopHtml(p){
  const books = p.books || {};
  const key = bookKeyFor(p);
  if(!key) return "";

  // TOTALS: a price is only comparable AT THE SAME LINE (fixed 2026-08-17).
  // The first version compared the over price across every book regardless of
  // which total each was quoting, and immediately produced a false positive on
  // the live board: an OVER 8.0 card advertised "BEST +105" from books that
  // were actually quoting 8.5. Over 8.5 pays plus money because it is a HARDER
  // bet, not because it is a better price. Taking that "upgrade" would have
  // been switching bets while believing you were shopping one.
  //
  // Same error as the run line prices averaged across handicaps, and the totals
  // pooled across 8.5 and 9.0 in the scraper. A number is only comparable to
  // another number describing the identical wager.
  let pickLine = null;
  if(p.type === "TOTAL"){
    const m = (p.label || "").match(/(\d+(?:\.\d+)?)/);
    if(m) pickLine = parseFloat(m[1]);
  }

  const rows = [], offLine = [];
  for(const b in books){
    const v = books[b] && books[b][key];
    if(v == null || Math.abs(v) < 100) continue;
    if(pickLine != null){
      const bl = books[b].tot;
      if(bl == null || Math.abs(bl - pickLine) > 0.001){
        offLine.push({ b, line: bl, price: v });
        continue;
      }
    }
    rows.push({ b, price: v, dec: amDec(v) });
  }
  if(!rows.length){
    if(!offLine.length) return "";
    // Every book is on a different number. Say so plainly instead of
    // presenting an incomparable price as if it were an upgrade.
    const alt = offLine.map(r =>
      `<div class="shop-row"><span>${BOOK_LABELS[r.b] || r.b}</span>
         <span class="shop-px">${r.line} @ ${fmtAm(r.price)}</span></div>`).join("");
    return `<details class="shop">
      <summary class="shop-sum">
        <span class="shop-lead">NO BOOK ON ${pickLine}</span>
        <span class="shop-mine">${offLine.length} book(s) quoting a different total</span>
      </summary>
      <div class="shop-body">${alt}
        <div class="shop-foot">A different total is a different bet, not a
          better price.</div></div></details>`;
  }
  rows.sort((x, y) => y.dec - x.dec);          // best for the bettor first
  const best = rows[0];
  const mine = rows.find(r => r.b === MY_BOOK);

  // The spread between best and worst IS the edge, on most of these bets.
  const worst = rows[rows.length - 1];
  const spreadPts = rows.length > 1
    ? ((best.dec - worst.dec) / worst.dec * 100).toFixed(1) : null;

  let mineTxt = "";
  if(mine && mine.b !== best.b && Math.abs(mine.dec - best.dec) > 1e-9){
    const lost = ((best.dec - mine.dec) / mine.dec * 100).toFixed(1);
    mineTxt = `<span class="shop-mine">Hard Rock ${fmtAm(mine.price)}`
            + ` &middot; <b>${lost}% worse</b></span>`;
  } else if(mine){
    mineTxt = `<span class="shop-mine shop-mine-ok">Hard Rock matches the best price</span>`;
  } else {
    mineTxt = `<span class="shop-mine">Hard Rock not quoting this</span>`;
  }

  const table = rows.map(r =>
    `<div class="shop-row${r.b === MY_BOOK ? " shop-row-mine" : ""}">
       <span>${BOOK_LABELS[r.b] || r.b}</span>
       <span class="shop-px">${fmtAm(r.price)}</span>
     </div>`).join("")
   + (offLine.length ? `<div class="shop-foot">Not compared, quoting a different
       total: ${offLine.map(r => `${BOOK_LABELS[r.b] || r.b} ${r.line}`).join(", ")}
       &mdash; a different line is a different bet.</div>` : "");

  // The caret and the book count exist because without them this read as a
  // static line and nobody clicked it. An affordance that is not visible is
  // not an affordance.
  return `<details class="shop">
    <summary class="shop-sum">
      <span class="shop-caret">&#9656;</span>
      <span class="shop-lead">BEST</span>
      <span class="shop-best">${fmtAm(best.price)}</span>
      <span class="shop-book">${BOOK_LABELS[best.b] || best.b}</span>
      <span class="shop-count">${rows.length} books</span>
      ${mineTxt}
    </summary>
    <div class="shop-body">${table}
      ${spreadPts ? `<div class="shop-foot">Best to worst is
        <b>${spreadPts}%</b> of return on the same bet.</div>` : ""}
    </div>
  </details>`;
}

// ── CALCULATORS (2026-08-18) ────────────────────────────────────────────────
// Deliberately NOT a second EV engine. Every card already carries a CHECK YOUR
// PRICE box; duplicating that math in a second place is exactly how two surfaces
// drift apart and start disagreeing, which is most of what went wrong on 08-17.
//
// These fill the two gaps nothing else covers:
//   DE-VIG   what the MARKET really thinks, once the book's margin is removed.
//            This is the benchmark every edge claim is measured against and the
//            board has never shown it.
//   STAKE    a mirror of model/staking.py. On a $20 roll with a $1 minimum the
//            correct stake is usually UNDER one unit, which is invisible until
//            you compute it. Keep this in lockstep with staking.py; if the
//            Python changes, change it here in the same commit.
//
// amDec / fmtAm are reused from the line-shopping block above rather than
// redefined, so a fix to odds conversion lands everywhere at once.

function calcConvert(){
  const raw = (document.getElementById("cvIn").value||"").trim().replace(/[^0-9+\-.]/g,"");
  const a = parseFloat(raw);
  const out = document.getElementById("cvOut");
  if(!raw || isNaN(a) || Math.abs(a) < 100){
    out.innerHTML = raw ? "<span class='calc-bad'>not a valid American price</span>"
                        : "<span class='calc-mute'>enter a price</span>";
    return;
  }
  const d = amDec(a), ip = 100/d;
  out.innerHTML = `<b>${ip.toFixed(1)}%</b> implied &nbsp;&middot;&nbsp; `
                + `${d.toFixed(3)} decimal &nbsp;&middot;&nbsp; `
                + `breaks even at <b>${ip.toFixed(1)}%</b>`;
}

function calcDevig(){
  const g = id => { const v=(document.getElementById(id).value||"").trim().replace(/[^0-9+\-.]/g,"");
                    const n=parseFloat(v); return (!v||isNaN(n)||Math.abs(n)<100) ? null : n; };
  const a = g("dvA"), b = g("dvB");
  const out = document.getElementById("dvOut");
  if(a === null || b === null){
    out.innerHTML = "<span class='calc-mute'>enter BOTH sides &mdash; vig can only be "
                  + "removed from a two-way market</span>";
    return;
  }
  const ia = 100/amDec(a), ib = 100/amDec(b);
  const book = ia + ib;                     // >100 by the size of the margin
  const fa = ia/book*100, fb = ib/book*100;
  // Fair price for each side, back in American terms.
  const toAm = pct => { const d = 100/pct; return d >= 2 ? (d-1)*100 : -100/(d-1); };
  const vig = book - 100;
  const col = vig > 6 ? "calc-bad" : vig > 4 ? "calc-warn" : "calc-good";
  out.innerHTML =
    `<div class="calc-row"><span>book margin</span>
       <span class="${col}"><b>${vig.toFixed(1)}%</b></span></div>
     <div class="calc-row"><span>side A fair</span>
       <span><b>${fa.toFixed(1)}%</b> &nbsp;(${fmtAm(toAm(fa))})</span></div>
     <div class="calc-row"><span>side B fair</span>
       <span><b>${fb.toFixed(1)}%</b> &nbsp;(${fmtAm(toAm(fb))})</span></div>
     <div class="calc-foot">You beat this market only when your honest probability
       is above the fair number, not above the raw implied one. The gap between
       them is the book's cut, and it is why a "60% pick at -150" is usually
       nothing: -150 implies 60.0% raw but about ${fa.toFixed(1)}% fair here.</div>`;
}

function calcStake(){
  const num = id => { const v=(document.getElementById(id).value||"").trim().replace(/[^0-9+\-.]/g,"");
                      const n=parseFloat(v); return isNaN(n) ? null : n; };
  const p  = num("stP"), pr = num("stPrice"), br = num("stBR");
  const out = document.getElementById("stOut");
  if(p === null || pr === null || br === null || Math.abs(pr) < 100 || br <= 0
     || !(p > 0 && p < 100)){
    out.innerHTML = "<span class='calc-mute'>enter win probability %, American price, "
                  + "and bankroll</span>";
    return;
  }
  const MIN = 1, STEP = 1, MAX = 5, KF = 0.25, MAXF = 0.08, TOL = 1.5;
  const prob = p/100, d = amDec(pr), b = d - 1;
  const ev = prob*b - (1-prob);
  if(ev <= 0){
    out.innerHTML = `<div class="calc-row"><span>EV</span>
      <span class="calc-bad"><b>${(ev*100).toFixed(1)}%</b></span></div>
      <div class="calc-foot">Negative. The price demands ${(100/d).toFixed(1)}% and you
      have ${p}%. No stake.</div>`;
    return;
  }
  const kFull = ev/b, target = Math.min(kFull*KF, MAXF), ideal = br*target;
  let snapped = Math.min(Math.floor(ideal/STEP)*STEP, MAX);
  let verdict, cls;
  if(snapped >= MIN){
    verdict = `Bet <b>$${snapped.toFixed(0)}</b>`; cls = "calc-good";
  } else {
    const over = MIN/ideal;
    if(over <= TOL){ snapped = MIN; cls = "calc-warn";
      verdict = `Bet the <b>$1</b> minimum &mdash; ${over.toFixed(1)}x the correct stake`; }
    else { snapped = 0; cls = "calc-bad";
      verdict = `<b>No bet.</b> Ideal is $${ideal.toFixed(2)}; $1 would be ${over.toFixed(1)}x that`; }
  }
  // The EV at which $1 becomes correctly sized on THIS bankroll and price.
  const floorEv = (MIN/br)*b/KF;
  out.innerHTML =
    `<div class="calc-row"><span>EV per $1</span><span class="calc-good"><b>+${(ev*100).toFixed(1)}%</b></span></div>
     <div class="calc-row"><span>full Kelly</span><span>${(kFull*100).toFixed(1)}%</span></div>
     <div class="calc-row"><span>quarter Kelly</span><span>${(target*100).toFixed(2)}% = <b>$${ideal.toFixed(2)}</b></span></div>
     <div class="calc-row"><span>verdict</span><span class="${cls}">${verdict}</span></div>
     <div class="calc-foot">At $${br.toFixed(0)} with a $1 minimum, a bet is only
       correctly sized once EV reaches <b>${(floorEv*100).toFixed(1)}%</b> at this price.
       Below that the minimum forces an over-bet.</div>`;
}

// ── LOG A BET YOU ACTUALLY PLACED (2026-08-18) ──────────────────────────────
// Everything else on this board answers "was the model right". This answers
// "did I make money", which is a different question and has been unanswerable.
//
// The price you type is YOUR price at YOUR book, not the board's. Those differ
// constantly: the card quotes Pinnacle, you bet Hard Rock or DraftKings, and
// the line moves in between. Recording the board's number instead would make
// the ledger a second copy of the model's record rather than yours.
//
// Defaults to the BEST book price found for this exact bet, since that is the
// number you most likely shopped to. Always editable.
function betDefaults(p){
  const key = bookKeyFor(p);
  let best = null, bestBook = "";
  if(key && p.books){
    for(const b in p.books){
      const v = p.books[b][key];
      if(v == null || Math.abs(v) < 100) continue;
      if(best === null || amDec(v) > amDec(best)){ best = v; bestBook = b; }
    }
  }
  if(best === null && p.pick_price != null && Math.abs(p.pick_price) >= 100){
    best = Math.round(p.pick_price); bestBook = "";
  }
  return {price: best, book: bestBook};
}

function betLogHtml(p, id){
  const d = betDefaults(p);
  const books = ["hardrockbet","draftkings","fanduel","betmgm","betrivers",
                 "espnbet","ballybet","betparx","fliff","bovada"];
  const opts = books.map(b =>
    `<option value="${b}"${b === d.book ? " selected" : ""}>${BOOK_LABELS[b]||b}</option>`).join("");
  return `<details class="betlog" id="${id}_wrap">
    <summary class="betlog-sum"><span class="betlog-caret">&#9656;</span>
      <span class="betlog-lead">&#43; LOG A BET</span>
      <span class="betlog-hint">record what you actually placed</span></summary>
    <div class="betlog-body">
      <div class="betlog-row">
        <input id="${id}_px" class="bb-price-in" placeholder="your price"
               value="${d.price != null ? fmtAm(d.price) : ""}">
        <input id="${id}_st" class="bb-price-in" placeholder="stake $" value="1">
        <select id="${id}_bk" class="betlog-sel">${opts}</select>
        <button class="betlog-btn" onclick="logBet('${id}', ${p._idx})">Log</button>
      </div>
      <div class="betlog-out" id="${id}_out">Your price at your book, not the board's.</div>
    </div>
  </details>`;
}

function logBet(id, idx){
  const p = (typeof pickData !== "undefined" && pickData[idx]) ? pickData[idx] : null;
  const out = document.getElementById(id + "_out");
  if(!p){ out.textContent = "could not resolve this pick"; return; }
  const px = parseInt((document.getElementById(id+"_px").value||"").replace(/[^0-9+\-]/g,""), 10);
  const st = parseFloat((document.getElementById(id+"_st").value||"").replace(/[^0-9.]/g,""));
  if(isNaN(px) || Math.abs(px) < 100){ out.innerHTML = "<span class='betlog-bad'>enter a real American price</span>"; return; }
  if(isNaN(st) || st <= 0){ out.innerHTML = "<span class='betlog-bad'>enter a stake above zero</span>"; return; }
  const body = {
    bet_date: (typeof DATA_DATE !== "undefined" ? DATA_DATE : ""),
    game_id: p.game_id || "", game: p.game || "",
    pick_type: p.type || "", label: p.label || "", team: p.team || "",
    price: px, stake: st,
    book: document.getElementById(id+"_bk").value
  };
  out.textContent = "saving…";
  fetch("/api/bet", {method:"POST", headers:{"Content-Type":"application/json"},
                     body: JSON.stringify(body)})
    .then(r => r.json())
    .then(j => {
      if(j && j.ok){
        out.innerHTML = `<span class='betlog-ok'>&#10004; logged &mdash; `
                      + `${fmtAm(px)} for $${st.toFixed(2)}</span> `
                      + `<a href="/admin/bets" class="betlog-link">ledger</a>`;
        document.getElementById(id+"_wrap").classList.add("betlog-done");
      } else {
        out.innerHTML = `<span class='betlog-bad'>${(j && j.error) || "save failed"}</span>`;
      }
    })
    .catch(e => { out.innerHTML = `<span class='betlog-bad'>${e}</span>`; });
}

// ── UNIVERSAL PRICE CHECK (2026-08-14) ──────────────────────────────────────
// Every card gets this, not just Best Bets.
//
// WHY. The site cannot know what YOUR book is charging. It shows a consensus
// price, and on 2026-08-13 that consensus was fabricated: run line prices were
// averaged across books without pairing each price to the line it was quoted
// for, so two unrelated games both published -109, a number no book offered.
// The scraper is fixed, but the deeper point stands and is permanent — Hard
// Rock, DraftKings and Pinnacle disagree by 10 to 20 cents on the same game,
// which is often the entire edge. On 2026-08-13 the Pirates were -144 at DK and
// -155 at Hard Rock: same bet, and only one of those two prices is playable.
//
// So the only honest answer to "should I bet this" comes from the price YOU can
// actually get. Type it, get a verdict and a stake sized on your number.
//
// The probability used is per bet type, never borrowed across types:
//   RL  in the 57-65% band -> that band's OBSERVED record (uncalibrated by
//                             design; its Platt fit made out-of-sample Brier
//                             worse, so there is little to correct)
//   RL  outside the band   -> the overall realized RL rate, flagged as weaker
//   ML / TOTAL             -> the per-type calibrated probability
function pcProbFor(p){
  if(p.type === "RL"){
    const c = (p.conf||0)/100;
    const band = RL_BAND_RATES.find(b => c >= b.lo && c < b.hi);
    if(band) return { prob: band.rate, strong: true,
      basis: `RL ${Math.round(band.lo*100)}-${Math.round(band.hi*100)}% band has gone `
           + `<b>${band.rec}</b> (${(band.rate*100).toFixed(0)}%)` };
    return { prob: 0.550, strong: false,
      basis: `outside the validated 57-65% band — using the overall RL rate `
           + `<b>126-103 (55.0%)</b>, which is much weaker evidence` };
  }
  if(p.type === "ML"){
    const c = (p.conf||0)/100;
    const b = ML_BAND_RATES.find(x => c >= x.lo && c < x.hi);
    if(b) return { prob: b.rate, strong: c >= ML_MIN_CONF,
      basis: `moneylines at ${Math.round(b.lo*100)}-${Math.round(b.hi*100)}% stated `
           + `have gone <b>${b.rec}</b> (${(b.rate*100).toFixed(1)}%) since 2026-07-21`
           + (c >= ML_MIN_CONF ? `, and the ${Math.round(ML_MIN_CONF*100)}%+ cut `
               + `held up walk-forward at ${ML_OOS_REC}`
             : `. This is below the validated ${Math.round(ML_MIN_CONF*100)}% cut`) };
  }
  if(p.cal_conf == null) return null;
  if(p.type === "TOTAL")
    return { prob: p.cal_conf/100, strong: false,
      basis: `calibrated <b>${p.cal_conf}%</b> — totals are capped at 68% stated, `
           + `which calibrates below break-even, so expect PASS` };
  return { prob: p.cal_conf/100, strong: false,
    basis: `calibrated <b>${p.cal_conf}%</b> — note ML did not survive `
         + `walk-forward testing (p=0.47), so treat any green here with caution` };
}

// Verdict for a typed price. Demands +8% EV rather than break-even: these rates
// are estimated from a few hundred graded picks, and betting at the exact
// break-even leaves no room for the estimate being a couple of points off.
function pcCheck(id, prob){
  const el   = document.getElementById(id+"_in");
  const out  = document.getElementById(id+"_out");
  const wrap = document.getElementById(id+"_wrap");
  if(!el || !out || !wrap) return;
  const raw = (el.value||"").trim().replace(/[^0-9+\-]/g,"");
  const a = parseInt(raw,10);
  if(!raw || isNaN(a) || Math.abs(a) < 100){
    out.textContent = raw ? "not a valid price" : "enter a price";
    out.className = "bb-verdict";
    wrap.className = "pc-rule";
    return;
  }
  const d  = a > 0 ? 1 + a/100 : 1 + 100/Math.abs(a);
  const ev = prob*(d-1) - (1-prob);
  const f  = Math.max(0, Math.min(0.08, ((prob*(d-1)-(1-prob))/(d-1))/4));
  const bank = (typeof BANKROLL !== "undefined" && BANKROLL > 0) ? BANKROLL : null;
  if(ev >= 0.08){
    out.innerHTML = "&#10004; BET IT &nbsp;<span class='bb-v-sub'>"
      + (ev*100).toFixed(1) + "% EV &middot; stake " + (f*100).toFixed(1) + "%"
      + (bank ? " &middot; $" + (bank*f).toFixed(2) : "") + "</span>";
    out.className = "bb-verdict bb-v-yes";
    wrap.className = "pc-rule bb-rule-yes";
  } else if(ev > 0){
    out.innerHTML = "&#9888; TOO THIN &nbsp;<span class='bb-v-sub'>only "
      + (ev*100).toFixed(1) + "% EV at that price &middot; pass</span>";
    out.className = "bb-verdict bb-v-thin";
    wrap.className = "pc-rule bb-rule-thin";
  } else {
    out.innerHTML = "&#10008; PASS &nbsp;<span class='bb-v-sub'>"
      + (ev*100).toFixed(1) + "% EV &middot; price is too short</span>";
    out.className = "bb-verdict bb-v-no";
    wrap.className = "pc-rule bb-rule-no";
  }
}

function priceCheckHtml(p){
  const info = pcProbFor(p);
  if(!info) return "";
  const id = "pc" + Math.abs(hashStr((p.label||"") + (p.game||"") + (p.type||"")));

  // The price that makes this bet clear +8% EV. This is the number to shop for.
  const dMin = (1 + 0.08) / info.prob;
  const amMin = dMin >= 2 ? Math.round((dMin-1)*100) : Math.round(-100/(dMin-1));
  const maxPrice = (amMin > 0 ? "+" : "") + amMin;

  // Show the feed price for reference, and say plainly when it is unusable
  // rather than printing a number that cannot be bet.
  const fp = p.pick_price;
  const feed = (fp == null)
    ? `<span class="pc-feed pc-feed-bad">feed has no clean price for this line</span>`
    : (Math.abs(fp) < 100)
      ? `<span class="pc-feed pc-feed-bad">feed price ${Math.round(fp)} is corrupt — ignore it</span>`
      : `<span class="pc-feed">feed shows ${fp>0?"+":""}${Math.round(fp)}</span>`;

  return `<div class="pc-rule" id="${id}_wrap">
    <div class="bb-rule-row">
      <span class="pc-lead">CHECK YOUR PRICE</span>
      <input type="text" inputmode="text" id="${id}_in" class="bb-price-in"
             placeholder="e.g. -144" oninput="pcCheck('${id}', ${info.prob})">
      <span class="bb-verdict" id="${id}_out">enter a price</span>
    </div>
    <span class="bb-rule-note">Bet only at <b>${maxPrice}</b> or better${info.strong?"":" (weak basis)"}
      &middot; ${feed}<br>${info.basis}</span>
  </div>`;
}

function rlBestBetEval(p, why){
  // `why` is an optional out-param. Every rejection records its reason so
  // the empty state can explain itself instead of always claiming the model
  // is protecting you. "No clean price" is a DATA failure and must never be
  // reported as a judgement call.
  const R = r => { if(why) why.reason = r; return null; };
  if(p.type !== "RL") return R("not a run line");
  const c = (p.conf||0)/100;
  if(c < RL_BEST_BET_MIN || c > RL_BEST_BET_MAX)
    return R("run line outside the validated 57-65% band");
  if(p.pick_price == null) return R("NO CLEAN PRICE from the odds feed");
  const band = RL_BAND_RATES.find(b => c >= b.lo && c < b.hi);
  if(!band) return R("run line outside the validated 57-65% band");
  const price = p.pick_price;
  // Sanity: an American price under 100 in absolute terms is corrupt data (the
  // documented cross-book averaging bug). Never bet a price we cannot verify.
  if(Math.abs(price) < 100) return R("NO CLEAN PRICE from the odds feed");

  // ── RUN LINE / MONEYLINE COHERENCE (2026-08-11) ──────────────────────────
  // Covering +1.5 includes EVERY outcome where the team wins outright, plus the
  // ones where they lose by exactly 1. So P(cover +1.5) > P(win) is a hard
  // mathematical requirement, and the implied probabilities must respect it.
  //
  // They frequently do not, because mlb_odds_scraper averages run-line prices
  // across books WITHOUT pairing each price to its own line. Live examples from
  // 2026-08-11 that this rule caught:
  //   Pirates  ML -113 (53.1%) but +1.5 at +154 (39.4%)  -> 13.7 pts backwards
  //   Dodgers  ML -267 (72.8%) but +1.5 at -120 (54.5%)  -> 18.2 pts backwards
  // Both showed as huge +EV Best Bets off a price no book would offer. The
  // Cardinals at ML +148 (40.3%) with +1.5 at -117 (53.9%) is coherent.
  //
  // This is the same class of bug as the -57 averaging issue and the phantom
  // 6.5 totals: a fabricated number producing a fabricated edge. Real fix is
  // pairing line+price per book in the scraper. Until then, refuse to publish a
  // run line whose price contradicts its own moneyline.
  const _isAway = (p.team === p.away);
  const _ml = _isAway ? p.ml_away_odds : p.ml_home_odds;
  if(_ml != null && Math.abs(_ml) >= 100){
    const _impML = _ml < 0 ? Math.abs(_ml)/(Math.abs(_ml)+100) : 100/(_ml+100);
    const _impRL = price < 0 ? Math.abs(price)/(Math.abs(price)+100) : 100/(price+100);
    if(_impRL <= _impML)
      return R("run line price contradicts its own moneyline (corrupt price)");
  }
  const dec = price > 0 ? 1 + price/100 : 1 + 100/Math.abs(price);
  const need = 100/dec;                       // break-even %
  const ev = band.rate*(dec-1) - (1-band.rate);
  if(ev <= 0) return R("price too short for the run line edge");
  return { ev, trueP: band.rate, need, band, price };
}

function bestBetEval(p, why){
  // PRICE FIRST, for every bet type (rewritten 2026-08-17 off /admin/real-roi).
  //
  // The old version accepted only moneylines, gated them on a confidence
  // threshold, and excluded totals outright. Measured against the prices
  // actually paid on 167 graded picks, all three of those choices were wrong:
  //   ML     38-44, REAL ROI -13.5%   (negative in five of six bands)
  //   RL+1.5 20-9,  REAL ROI +14.9%
  //   TOTAL  band ROIs +5.6% and +19.8%, while being suppressed
  //
  // What separates them is not the pick, it is what the book charges. ML loses
  // because its picks average -131 to -163; totals win because they sit near
  // -100. So the rule is now identical for every type: take the band's OBSERVED
  // win rate as the probability, and require positive EV against the REAL price
  // with an 8% cushion. No type is admitted or excluded in advance.
  const R = r => { if(why) why.reason = r; return null; };
  const _bw = {};
  const band = bandFor(p, _bw);
  if(!band) return R(_bw.reason || "no usable band");
  if(p.pick_price == null) return R("NO CLEAN PRICE from the odds feed");
  const price = p.pick_price;
  if(Math.abs(price) < 100) return R("NO CLEAN PRICE from the odds feed");

  if(p.type === "RL"){
    // Covering +1.5 includes every outcome where the team wins outright plus
    // the ones where it loses by exactly 1, so P(cover) > P(win) is a hard
    // requirement. A price that says otherwise is corrupt, not an opportunity.
    const _isAway = (p.team === p.away);
    const _ml = _isAway ? p.ml_away_odds : p.ml_home_odds;
    if(_ml != null && Math.abs(_ml) >= 100 && (p.label||"").indexOf("+1.5") >= 0){
      const _impML = _ml < 0 ? Math.abs(_ml)/(Math.abs(_ml)+100) : 100/(_ml+100);
      const _impRL = price < 0 ? Math.abs(price)/(Math.abs(price)+100) : 100/(price+100);
      if(_impRL <= _impML)
        return R("run line price contradicts its own moneyline (corrupt price)");
    }
  }

  const dec  = price > 0 ? 1 + price/100 : 1 + 100/Math.abs(price);
  const need = 100/dec;
  const ev   = band.rate*(dec-1) - (1-band.rate);
  if(ev <= 0) return R(`price too short — needs ${need.toFixed(1)}%, band wins ${(band.rate*100).toFixed(1)}%`);
  if(ev < 0.08) return R(`only ${(ev*100).toFixed(1)}% EV at this price — under the 8% cushion`);
  return { ev, trueP: band.rate, need, band, price };
}

// ── Team logo helper (MLBAM stable team ids) ─────────────────────────────────
const _TEAM_LOGOS = [
  ["diamondbacks",109],["d-backs",109],["braves",144],["orioles",110],
  ["red sox",111],["white sox",145],["cubs",112],["reds",113],
  ["guardians",114],["rockies",115],["tigers",116],["astros",117],
  ["royals",118],["angels",108],["dodgers",119],["marlins",146],
  ["brewers",158],["twins",142],["mets",121],["yankees",147],
  ["athletics",133],["phillies",143],["pirates",134],["padres",135],
  ["giants",137],["mariners",136],["cardinals",138],["rays",139],
  ["rangers",140],["blue jays",141],["nationals",120]
];
function teamLogoUrl(name){
  const n=(name||"").toLowerCase();
  for(const [k,id] of _TEAM_LOGOS){ if(n.includes(k)) return `https://www.mlbstatic.com/team-logos/${id}.svg`; }
  return "";
}
function teamLogo(name, size){
  const u=teamLogoUrl(name); if(!u) return "";
  const s=size||18;
  // Faint white outline so dark logos (Yankees navy, Brewers) stay visible on the dark bg.
  return `<img src="${u}" alt="" loading="lazy" style="width:${s}px;height:${s}px;vertical-align:middle;margin-right:3px;object-fit:contain;filter:drop-shadow(0 0 .6px rgba(255,255,255,.55))" onerror="this.style.display='none'">`;
}
// Large emblem of the PICKED team, anchored in the card's empty top-right corner.
function teamEmblem(name){
  const u=teamLogoUrl(name); if(!u) return "";
  return `<img class="pick-team-emblem" src="${u}" alt="" loading="lazy" onerror="this.style.display='none'">`;
}
// Team name -> standard MLB abbreviation (nickname substring match, sox disambiguated).
const _TEAM_ABBR = [
  ["diamondbacks","ARI"],["d-backs","ARI"],["braves","ATL"],["orioles","BAL"],
  ["red sox","BOS"],["white sox","CWS"],["cubs","CHC"],["reds","CIN"],
  ["guardians","CLE"],["rockies","COL"],["tigers","DET"],["astros","HOU"],
  ["royals","KC"],["angels","LAA"],["dodgers","LAD"],["marlins","MIA"],
  ["brewers","MIL"],["twins","MIN"],["mets","NYM"],["yankees","NYY"],
  ["athletics","ATH"],["phillies","PHI"],["pirates","PIT"],["padres","SD"],
  ["giants","SF"],["mariners","SEA"],["cardinals","STL"],["rays","TB"],
  ["rangers","TEX"],["blue jays","TOR"],["nationals","WSH"]
];
function teamAbbr(name){
  const n=(name||"").toLowerCase();
  for(const [k,a] of _TEAM_ABBR){ if(n.includes(k)) return a; }
  return (name||"").split(" ").slice(-1)[0].slice(0,3).toUpperCase();
}

function renderBestBets(){
  const box = document.getElementById("bestBets");
  if(!box) return;
  const src = (typeof ACTIVE_PICKS!=="undefined" && ACTIVE_PICKS.length) ? ACTIVE_PICKS : DATA_PICKS;
  const _bbReasons = [];
  // Two independent rules. RL uses its observed band rate (validated edge);
  // ML uses the Platt-calibrated probability. Both must clear positive EV
  // against the REAL price. Tagged so the card can explain which applied.
  const ranked = (src||[])
    .map(p => {
      const w = {};
      const e = bestBetEval(p, w);
      if(e) return {p, e, kind: p.type};
      _bbReasons.push(w.reason || "rejected");
      return null;
    })
    .filter(x => x)
    .sort((a,b) => b.e.ev - a.e.ev)
    .slice(0, 6);
  if(!ranked.length){
    // WHY IT IS EMPTY (2026-08-15). This used to assert, always, that nothing
    // qualified and the model was protecting you. That is only true when picks
    // were rejected on JUDGEMENT (band, chalk, EV). When they are rejected for
    // having NO PRICE, the board is broken, not cautious, and saying otherwise
    // hides a data outage behind a reassuring sentence.
    const tally = {};
    _bbReasons.forEach(r => { tally[r] = (tally[r]||0) + 1; });
    const rows = Object.entries(tally).sort((a,b) => b[1]-a[1]);
    const noPrice = rows.filter(r => r[0].indexOf("NO CLEAN PRICE") >= 0)
                        .reduce((n,r) => n + r[1], 0);
    const total = _bbReasons.length;
    const lead = (total === 0)
      ? `<b>There are no picks on the board at all.</b> The pipeline has not
         produced a slate — check <a href="/status">/status</a>.`
      : (noPrice >= Math.max(3, total * 0.4))
        ? `<b>This is a data problem, not a judgement.</b> ${noPrice} of ${total}
           picks were dropped because the odds feed carried no usable price.
           Run <a href="/admin/pinnacle-odds-test">/admin/pinnacle-odds-test</a>
           and check that mlb_odds_master.csv has the
           <code>rl_*_m15_price</code> / <code>rl_*_p15_price</code> columns.`
        : `Nothing qualified today. Every pick was rejected on its merits, which
           is the filter working. Prices were present and readable.`;
    box.innerHTML = `<div class="bb-wrap"><div class="bb-head">⭐ Best Bets</div>
      <div class="bb-empty">${lead}
        <div class="bb-why">${rows.map(([r,n]) =>
          `<div class="bb-why-row"><span class="bb-why-n">${n}</span>
             <span class="${r.indexOf("NO CLEAN PRICE")>=0?"bb-why-bad":""}">${r}</span></div>`
        ).join("")}</div>
        <div class="bb-why-foot">Every pick is still on the board below, with a
          CHECK YOUR PRICE box. Best Bets is a shortlist, not the whole card.</div>
      </div></div>`;
    return;
  }
  const rows = ranked.map(({p, e, kind}) => {
    const priceStr = (p.pick_price>0?"+":"")+Math.round(p.pick_price);
    // Say WHERE the number came from. An RL best bet rests on an observed band
    // record; an ML one rests on a fitted calibration curve. Those are different
    // levels of evidence and the card should not blur them.
    // CALIBRATED QUARTER-KELLY. The card's own Kelly figure is computed off raw
    // confidence, which overstates by ~14 points on ML, so it sizes far too big.
    // Kelly f* = (p*(d-1) - (1-p)) / (d-1). Using the band's OBSERVED rate for RL
    // and the Platt-calibrated probability for ML, both against the real price.
    //
    // QUARTER, not half. Kelly assumes you know p exactly. Here p is estimated
    // from 94 graded picks, so the edge itself has error bars, and overbetting a
    // mis-estimated edge is how bankrolls die. Quarter-Kelly is the standard
    // fraction for an uncertain edge.
    //
    // The 8% cap is a backstop, deliberately set high enough that it rarely
    // binds. A first attempt used a 5% cap and it flattened everything to 5%,
    // which destroyed the whole point: a +25% EV bet and a +9% EV bet showed the
    // SAME stake. The stake has to move with the edge or it teaches nothing.
    const _dec = p.pick_price > 0 ? 1 + p.pick_price/100 : 1 + 100/Math.abs(p.pick_price);
    const _b   = _dec - 1;
    const _f   = (e.trueP * _b - (1 - e.trueP)) / _b;
    const _half = Math.max(0, Math.min(0.08, _f / 4));
    const _bank = (typeof BANKROLL !== "undefined" && BANKROLL > 0) ? BANKROLL : null;
    const stake = `<span class="bb-stake">stake ${(_half*100).toFixed(1)}% of bankroll`
      + (_bank ? ` &middot; $${(_bank*_half).toFixed(2)} on $${_bank}` : "")
      + `</span>`;
      // ── MAX PRICE YOU CAN ACCEPT ────────────────────────────────────────────
    // The single number that makes this card actionable. The model's own price
    // comes from Pinnacle and is often NOT what your book offers — on
    // 2026-08-14 the board showed -109 on two games while Pinnacle, DraftKings
    // and Hard Rock all sat near -160. Rather than asking you to redo the EV
    // maths, we invert it: what is the WORST price at which this is still worth
    // betting? Then you only have to compare one number to your book.
    //
    // Solve p*(d-1) - (1-p) = MIN_EV  ->  d = (1 + MIN_EV) / p
    // At the band's 67.4%, break-even is -207. We require +8% EV instead, which
    // gives -166, leaving margin for the 67.4% itself being an estimate from 94
    // picks rather than a known truth.
    const BET_MIN_EV = 0.08;
    const _dMin = (1 + BET_MIN_EV) / e.trueP;
    const _amMin = _dMin >= 2 ? Math.round((_dMin - 1) * 100)
                              : Math.round(-100 / (_dMin - 1));
    const _maxPrice = (_amMin > 0 ? "+" : "") + _amMin;
    // Interactive check. The site CANNOT know your book's price, so asking for
    // it is the only way to give a real yes/no. Type the price, get a verdict
    // and a stake sized on YOUR number, not Pinnacle's.
    const _id = "bb" + Math.abs(hashStr(p.label + p.game));
    const priceRule = `<div class="bb-rule" id="${_id}_wrap">
      <div class="bb-rule-row">
        <span class="bb-rule-lead">YOUR BOOK'S PRICE</span>
        <input type="text" inputmode="text" id="${_id}_in" class="bb-price-in"
               placeholder="e.g. -144"
               oninput="bbCheck('${_id}', ${e.trueP})">
        <span class="bb-verdict" id="${_id}_out">enter a price</span>
      </div>
      <span class="bb-rule-note" id="${_id}_note">Anything better than
        <b>${_maxPrice}</b> is a bet. The ${p.pick_price > 0 ? "+" : ""}${Math.round(p.pick_price)}
        above is Pinnacle's price, not necessarily yours.</span>
    </div>`;
    const basis = (kind === "RL")
      ? `<span class="bb-basis">RL ${Math.round(e.band.lo*100)}-${Math.round(e.band.hi*100)}% band has gone <b>${e.band.rec}</b> (${(e.band.rate*100).toFixed(0)}%) · needs ${e.need.toFixed(1)}%</span>`
      : `<span class="bb-basis">calibrated ${Math.round(e.trueP*100)}% vs market</span>`;
    return `<div class="bb-row">
      <div class="bb-main">
        <span class="bb-team">${kind === "RL" ? "🎯 " : ""}${p.label}</span>
        <span class="bb-price">${priceStr}</span>
      </div>
      <div class="bb-meta">
        <span class="bb-ev">+${(e.ev*100).toFixed(1)}% EV</span>
        <span class="bb-win">${Math.round(e.trueP*100)}% fair win</span>
        <span class="bb-game">${teamLogo(p.away,14)}${teamLogo(p.home,14)}${p.game}</span>
      </div>
      <div class="bb-meta">${basis}</div>
      <div class="bb-meta">${stake}</div>
      ${priceRule}
    </div>`;
  }).join("");
  box.innerHTML = `<div class="bb-wrap">
    <div class="bb-head">⭐ Best Bets <span class="bb-sub">🎯 = run line, the one walk-forward validated edge (51-21, 70.8%, p=0.0011) · everything ranked by EV against the real price</span></div>
    ${rows}
    <div class="bb-note">ℹ️ This list re-ranks through the day. Best Bets is scored off live Pinnacle prices, which we re-pull every ~40 min until first pitch. When a line moves, a pick's value (EV) changes, so the order shifts and picks can drop on or off. That's the list tracking the real closing market, not a bug — it locks once games start.</div>
  </div>`;
}

// ── Parlay Drawer ─────────────────────────────────────────────
let selectedLegs = [];  // [{id, type, label, conf, odds, away, home}]
let pickData = [];       // registry for + buttons -- reset on each renderPicks()

function toggleLeg(evt, gameId, type, label, conf, mlAway, mlHome, away, home, team){
  evt.stopPropagation();
  const idx = selectedLegs.findIndex(l => l.id === gameId && l.type === type);
  if(idx !== -1){
    selectedLegs.splice(idx, 1);
  } else {
    // Use sportsbook odds when available, fall back to model-implied
    const awayLast = (away||"").split(" ").slice(-1)[0];
    const pickedAway = team && team.toLowerCase().includes(awayLast.toLowerCase());
    let sbOdds = null;
    if(type !== "TOTAL"){
      sbOdds = pickedAway ? mlAway : mlHome;
    }
    const odds = (sbOdds != null) ? sbOdds : confToAmerican(conf);
    selectedLegs.push({id:gameId, type, label, conf, odds, away, home});
  }
  updateParlayDrawer();
  // Update button appearance
  evt.target.classList.toggle("leg-selected", idx === -1);
}

function toggleLegByIdx(evt, idx){
  const p = pickData[idx];
  if(!p) return;
  toggleLeg(evt, p.game_id, p.type, p.label, p.conf,
            p.ml_away_odds, p.ml_home_odds, p.away, p.home, p.team);
}

function parlayPayout(legs){
  // Convert each American odds to decimal, multiply, convert back
  let decimal = 1;
  for(const leg of legs){
    const o = leg.odds;
    if(o == null) { decimal *= 1 / (leg.conf / 100); continue; }
    decimal *= o > 0 ? (o / 100 + 1) : (100 / Math.abs(o) + 1);
  }
  // Convert combined decimal back to American odds
  const net = decimal - 1;
  const american = net >= 1 ? Math.round(net * 100) : -Math.round(100 / net);
  const payout100 = Math.round(net * 100);
  return {american, payout100, decimal};
}

function updateParlayDrawer(){
  const drawer = document.getElementById("parlayDrawer");
  if(!drawer) return;
  if(selectedLegs.length === 0){ drawer.style.display = "none"; return; }
  drawer.style.display = "block";
  const {american, payout100} = parlayPayout(selectedLegs);
  const americanStr = american > 0 ? "+"+american : String(american);
  const legsHtml = selectedLegs.map((l,i) => {
    const oddsStr = l.odds != null ? (l.odds > 0 ? "+"+l.odds : String(l.odds)) : "(model)";
    const typeBadge = l.type === "PROP" ? `<span style="font-size:.65rem;background:rgba(74,222,128,.2);color:var(--green);border-radius:3px;padding:1px 4px;margin-right:4px">PROP</span>` : "";
    return `<div class="drawer-leg">
      <span class="drawer-leg-label">${typeBadge}${l.label}</span>
      <span class="drawer-leg-odds">${oddsStr}</span>
      <button class="drawer-remove" onclick="removeLeg(${i})">×</button>
    </div>`;
  }).join("");
  drawer.innerHTML = `
    <div class="drawer-header">
      <span class="drawer-title">📋 Parlay Builder (${selectedLegs.length} leg${selectedLegs.length===1?"":"s"})</span>
      <span class="drawer-payout">${americanStr} &nbsp;·&nbsp; $100 wins <strong>$${payout100}</strong></span>
      <button class="drawer-clear" onclick="clearLegs()">Clear All</button>
    </div>
    <div class="drawer-legs">${legsHtml}</div>
  `;
}

function removeLeg(i){ selectedLegs.splice(i,1); updateParlayDrawer(); refreshPropButtons(); }
function clearLegs(){ selectedLegs=[]; updateParlayDrawer(); refreshPropButtons(); }

function togglePropLeg(evt, propId, label, conf){
  evt.stopPropagation();
  const btn = evt.currentTarget;
  const existing = selectedLegs.findIndex(l => l.id === propId);
  if(existing !== -1){
    selectedLegs.splice(existing, 1);
    btn.textContent = "+";
    btn.classList.remove("prop-selected");
    btn.closest(".inline-prop").classList.remove("prop-in-parlay");
  } else {
    const odds = confToAmerican(conf);
    selectedLegs.push({id: propId, type: "PROP", label, conf, odds, away: "", home: ""});
    btn.textContent = "✓";
    btn.classList.add("prop-selected");
    btn.closest(".inline-prop").classList.add("prop-in-parlay");
  }
  updateParlayDrawer();
}

function refreshPropButtons(){
  // Sync all prop + buttons after external removals (clear all, remove from drawer)
  document.querySelectorAll(".inline-prop-add").forEach(btn => {
    const onclick = btn.getAttribute("onclick") || "";
    const m = onclick.match(/togglePropLeg\(event,'([^']+)'/);
    if(!m) return;
    const propId = m[1];
    const inParlay = selectedLegs.some(l => l.id === propId);
    btn.textContent = inParlay ? "✓" : "+";
    btn.classList.toggle("prop-selected", inParlay);
    btn.closest(".inline-prop").classList.toggle("prop-in-parlay", inParlay);
  });
}

// High-confidence band: the ONLY segment with measured edge over two months.
// ML at 75%+ won 67.5% (n=40) and 63.8% (n=47) vs a 52.38% break-even, and it
// held even while the model was missing umpire/fatigue/platoon data. Everything
// else (all TOTAL, all RL, ML under 75%) landed 44-55% — indistinguishable from
// a coin flip. Threshold should be re-derived from post-2026-07-21 calibration.
const HIGH_CONF = __HIGHCONF__;   // {rule:{ML:75}, record:{ML:"ML 75%+: 87 picks, 65.5%"}}
const DATA_TRENDS = __TRENDS__;   // {conf_band:[], by_tier:[], market:[], props:[]}

// Show the badge tiers' live graded record so the "best pick" claim is verifiable.
// Records are derived server-side from the model's own graded history.
(function renderHCTrackRecord(){
  try{
    const box = document.getElementById("hcTrackRecord");
    if(!box || !HIGH_CONF) return;
    const chips = [];
    const elite = HIGH_CONF.record ? Object.values(HIGH_CONF.record) : [];
    const wide  = HIGH_CONF.record_wide ? Object.values(HIGH_CONF.record_wide) : [];
    elite.forEach(r => chips.push(
      `<span style="background:linear-gradient(135deg,rgba(255,107,0,.18),rgba(255,183,77,.14));`+
      `color:#ff9d3c;border:1px solid rgba(255,157,60,.45);padding:4px 12px;border-radius:999px;`+
      `font-size:.8rem;font-weight:700">🔥 ${r}</span>`));
    wide.forEach(r => chips.push(
      `<span style="background:rgba(63,185,80,.12);color:#3fb950;border:1px solid rgba(63,185,80,.4);`+
      `padding:4px 12px;border-radius:999px;font-size:.8rem;font-weight:700">📈 ${r}</span>`));
    if(chips.length){
      box.innerHTML = `<span style="color:#8b949e;font-size:.78rem;align-self:center">Badge track record `+
        `(graded):</span>` + chips.join("");
      box.style.display = "flex";
    }
  }catch(e){}
})();

// Left-side tracker: running graded record of the two badge categories over time,
// so you can judge whether High Confidence / Profitable picks are worth leaning on
// (e.g. as parlay pieces). Data is derived from the model's own graded history.
(function renderTracker(){
  try{
    const box = document.getElementById("trackerWidget");
    if(!box || !HIGH_CONF){ if(box) box.style.display="none"; return; }
    const elite = HIGH_CONF.record ? Object.values(HIGH_CONF.record) : [];
    const wide  = HIGH_CONF.record_wide ? Object.values(HIGH_CONF.record_wide) : [];
    function rows(recs){
      if(!recs.length) return '<div class="tw-row"><span class="tw-none">building record…</span></div>';
      return recs.map(r=>{
        const i = r.indexOf(":");
        const rule = (i>=0 ? r.slice(0,i) : r).trim();         // e.g. "ML 80%+"
        const stat = (i>=0 ? r.slice(i+1) : "").trim();        // e.g. "51 picks, 64.7%"
        const nm = stat.match(/(\d+)\s*picks/);
        const wm = stat.match(/([\d.]+)%/);
        const n  = nm ? parseInt(nm[1]) : null;
        const wr = wm ? parseFloat(wm[1]) : null;
        const col = wr==null ? "var(--sub)" : (wr>=52.38 ? "var(--green)" : "var(--red)");
        let rec = stat;
        if(n!=null && wr!=null){ const w=Math.round(n*wr/100); rec = `${w}-${n-w} · ${wr}%`; }
        return `<div class="tw-row"><span class="tw-lbl">${rule}</span>`+
               `<span class="tw-rec" style="color:${col}">${rec}</span></div>`;
      }).join("");
    }
    box.innerHTML = `<div class="tw-title">📊 Badge Track Record</div>`+
      `<div class="tw-sub">🔥 tagged High Confidence cards</div>${rows(elite)}`+
      `<div class="tw-sub">📈 tagged Profitable cards</div>${rows(wide)}`+
      `<div class="tw-note">Running graded W-L of every card that carried the badge `+
      `(ML 80%+ earns 🔥, 70%+ earns 📈). Green = beating break-even. `+
      `Grows as new tagged picks settle.</div>`;
  }catch(e){ const box=document.getElementById("trackerWidget"); if(box) box.style.display="none"; }
})();

// Stats & Trends — visual bar charts of the last 21 days of graded picks.
function renderTrends(){
  const box = document.getElementById("trendsContent");
  if(!box) return;
  const T = DATA_TRENDS || {};
  const BE = 52.38;
  const num = x => (x==null ? 0 : +x);
  function bar(label, pct, valText){
    const w = Math.max(2, Math.min(100, pct));
    const col = pct>=BE ? "var(--green)" : "var(--red)";
    return `<div class="trend-row">
      <span class="trend-label">${label}</span>
      <div class="trend-bar-track">
        <div class="trend-bar" style="width:${w}%;background:${col}"></div>
        <div class="trend-be" style="left:${BE}%"></div>
      </div>
      <span class="trend-val" style="color:${col}">${valText}</span>
    </div>`;
  }
  function chart(title, sub, rows){
    return `<div class="trend-chart"><div class="trend-chart-title">${title}</div>
      <div class="trend-chart-sub">${sub}</div>
      ${rows || '<div class="trend-empty">No graded data yet.</div>'}</div>`;
  }
  // 1. by confidence band
  let cb = "";
  (T.conf_band||[]).forEach(r=>{
    const w=num(r.w), l=num(r.l), n=w+l; if(!n) return;
    const pct=w/n*100;
    cb += bar(`${Math.round(num(r.lo)*100)}-${Math.round(num(r.hi)*100)}%`, pct, `${pct.toFixed(1)}% (${w}-${l})`);
  });
  // 2. by bet type  3. by tier  (both aggregated from by_tier)
  const typeAgg={}, tierAgg={};
  (T.by_tier||[]).forEach(r=>{
    const ty=r.pick_type||"?"; (typeAgg[ty]=typeAgg[ty]||{w:0,l:0}); typeAgg[ty].w+=num(r.w); typeAgg[ty].l+=num(r.l);
    const ti=r.tier||"?";      (tierAgg[ti]=tierAgg[ti]||{w:0,l:0}); tierAgg[ti].w+=num(r.w); tierAgg[ti].l+=num(r.l);
  });
  const rowsFrom = (agg, order) => Object.keys(agg).sort((a,b)=>((order?order[a]:0)??9)-((order?order[b]:0)??9))
    .map(k=>{const{w,l}=agg[k];const n=w+l;if(!n)return"";const pct=w/n*100;return bar(k,pct,`${pct.toFixed(1)}% (${w}-${l})`);}).join("");
  const bt = rowsFrom(typeAgg, null);
  const tr = rowsFrom(tierAgg, {LOCK:0,STRONG:1,LEAN:2,TOSSUP:3});
  // 4. props hit rate
  let pr = "";
  (T.props||[]).forEach(r=>{
    const h=num(r.hits), tot=num(r.total); if(!tot) return;
    const pct=h/tot*100;
    pr += bar(`${r.prop_type} ${r.side||''}`.trim(), pct, `${pct.toFixed(0)}% (${h}/${tot})`);
  });
  box.innerHTML =
    chart("Win rate by confidence band","Does higher model confidence actually win more? The white line marks 52.4% break-even.", cb) +
    chart("Win rate by bet type","Moneyline vs run line vs totals.", bt) +
    chart("Win rate by tier","LOCK / STRONG / LEAN / TOSSUP.", tr) +
    chart("Prop hit rate by type","Player props, side-aware.", pr);
}
renderTrends();

function _isHighConf(p){
  if(!p || !HIGH_CONF || !HIGH_CONF.rule) return false;
  const t = HIGH_CONF.rule[p.type];
  return t !== undefined && Number(p.conf) >= t;
}

function _isProfitBand(p){
  if(!p || !HIGH_CONF || !HIGH_CONF.rule_wide) return false;
  if(_isHighConf(p)) return false;          // elite badge already shown
  const t = HIGH_CONF.rule_wide[p.type];
  return t !== undefined && Number(p.conf) >= t;
}

function _profitBandTitle(p){
  const r = HIGH_CONF && HIGH_CONF.record_wide ? HIGH_CONF.record_wide[p.type] : "";
  return r ? "Profitable but thinner band — " + r : "Profitable band";
}

function _highConfTitle(p){
  const r = HIGH_CONF && HIGH_CONF.record ? HIGH_CONF.record[p.type] : "";
  return r ? "Derived from this model's graded record — " + r
           : "High confidence band";
}

function renderPicks(){
  pickData = [];  // reset registry for fresh + button indices
  const grid = document.getElementById("picksGrid");
  grid.innerHTML = "";
  const leanGridEl = document.getElementById("leanGrid");
  if(leanGridEl) leanGridEl.innerHTML = "";
  // Also collapse lean body on re-render
  const leanBodyEl = document.getElementById("leanBody");
  if(leanBodyEl) leanBodyEl.style.display = "none";
  const leanArrowEl = document.getElementById("leanArrow");
  if(leanArrowEl) leanArrowEl.textContent = "▶";
  let visible = 0;
  // Sort: completed games sink to bottom within each tier
  const _sorted = [...ACTIVE_PICKS].sort((a, b) => {
    const aFinal = _isFinal(_findScore(a));
    const bFinal = _isFinal(_findScore(b));
    if(aFinal === bFinal) return 0;
    return aFinal ? 1 : -1;
  });
  _sorted.forEach(p=>{
    // TBD starter: \u26a0 badge already warns users -- picks remain visible in their model tier
    const show = (filterType==="all" || p.type===filterType)
              && (filterTier==="all"
                  || (filterTier==="HIGHCONF" ? _isHighConf(p)
                      // PROFIT is the inclusive band: the elite threshold sits
                      // above the wide one, so a 🔥 pick is also profitable. The
                      // card shows only one icon, but the filter shows both.
                      : filterTier==="PROFIT" ? (_isHighConf(p) || _isProfitBand(p))
                      : p.tier===filterTier))
              && (!filterTeam || p.away.toLowerCase().includes(filterTeam)
                             || p.home.toLowerCase().includes(filterTeam)
                             || p.team.toLowerCase().includes(filterTeam));
    if(!show) return;
    visible++;

    // Confidence bar — scaled from 50% baseline so visual = edge above coin flip
    const barPct = Math.min(100, Math.max(0, (p.conf - 50) * 2));

    // Warning badges — always shown, lit when active / dim when not
    const wb = (cls, active, label) =>
      `<span class="warn-badge ${cls} ${active ? 'active' : 'inactive'}">${label}</span>`;
    const warnHtml = `<div class="warn-row">
      ${wb('warn-tbd',    p.tbd_sp,      '⚠ Starter Unknown')}
      ${wb('warn-thin',   p.thin_edge,   'Close Call')}
      ${wb('warn-heavy',  p.heavy_fav,   'Heavy Favorite')}
      ${wb('warn-lineup', p.unconfirmed, 'Lineup Not Set')}
      ${p.lineup_projected ? `<span class="warn-badge warn-lineup active" style="background:rgba(255,183,77,.12);color:#ffb74d;border-color:rgba(255,183,77,.3)">📋 Projected Lineup</span>` : ""}
    </div>`;

    // Favorite tier — always shown on every card
    const favLabel = p.fav_tier === 'HEAVY'   ? '🔴 Heavy Fav'
                   : p.fav_tier === 'MEDIUM'  ? '🟡 Medium Fav'
                   : '🟢 Neutral';
    const favHtml = `<div class="fav-row">
      <span class="fav-label">Line:</span>
      <span class="fav-badge fav-${p.fav_tier||'NEUTRAL'}">${favLabel}</span>
    </div>`;

    // Line shopping range — shown on TOTAL picks when books disagree
    let lineShopHtml = "";
    if(p.type === "TOTAL" && p.total_line_min != null && p.total_line_max != null
       && p.total_line_min !== p.total_line_max){
      const bestFor = p.label && p.label.startsWith("OVER")
        ? `Shop for ${p.total_line_min} (lowest line = best for OVER)`
        : `Shop for ${p.total_line_max} (highest line = best for UNDER)`;
      lineShopHtml = `<div class="line-shop-row">📊 Books: <strong>${p.total_line_min}–${p.total_line_max}</strong> &nbsp;<span class="line-shop-tip">${bestFor}</span></div>`;
    }

    // Kelly Criterion sizing
    // NO PRICE, NO STAKE (2026-08-17).
    //
    // kelly_pct is computed server-side as half-Kelly at a HARDCODED -110 using
    // RAW confidence. Both inputs are fictional when there is no price: on the
    // 08-18 slate, before any odds pull, a card read "LOCK 72.8% - Bet $4.30
    // (21.5% Half-Kelly)" while its own HONEST READ line said calibrated 56.1%
    // and no clean price. A stake is a claim about how much to risk; making one
    // up from an assumed price is the flat -110 error wearing a dollar sign.
    //
    // Best Bets already sizes correctly (band rate against the REAL price), so
    // the card just declines rather than duplicating that here.
    const _hasPrice = p.pick_price != null && Math.abs(p.pick_price) >= 100;
    const kellyHtml = (p.kelly_pct > 0 && _hasPrice)
      ? `<div class="kelly-row" data-kelly="${p.kelly_pct}">💰 Bet <strong class="kelly-amt">$${window._BR ? (window._BR*p.kelly_pct/100).toFixed(2) : (p.kelly_pct/100).toFixed(2)}</strong> <span class="kelly-note">${window._BR ? "on $"+window._BR.toLocaleString() : "per $1 bankroll"} (${p.kelly_pct}% Half-Kelly)</span></div>`
      : (p.kelly_pct > 0)
      ? `<div class="kelly-row kelly-nostake">&#8709; No stake &mdash; <span class="kelly-note">no price for this line yet, so there is nothing to size against</span></div>`
      : "";

    // Kalshi signal
    let kalshiHtml = "";
    if(p.kalshi_prob !== null && p.kalshi_prob !== undefined){
      const sigClass = `signal-${p.kalshi_signal||"NEUTRAL"}`;
      const sigLabel = p.kalshi_signal === "AGREE"    ? "✓ Market Agrees"
                     : p.kalshi_signal === "DISAGREE" ? "⚠ Market Disagrees"
                     : "Market Neutral";
      kalshiHtml = `
        <div class="kalshi-row">
          <span class="kalshi-label">Kalshi</span>
          <span class="kalshi-prob">${p.kalshi_prob}%</span>
          <span class="${sigClass}">${sigLabel}</span>
        </div>`;
    }

    // Line movement for this game
    let moveHtml = "";
    const mv = DATA_MOVEMENT.find(m => m.away === p.away && m.home === p.home);
    if(mv){
      const sig     = mv.ml_signal === "STEAM" ? mv.ml_signal
                    : mv.total_signal === "STEAM" ? mv.total_signal
                    : mv.ml_signal === "DRIFT" ? mv.ml_signal : mv.total_signal;
      const sigBadge = sig === "STEAM"
        ? `<span class="move-badge-steam">🔥 Heavy Action</span>`
        : `<span class="move-badge-drift">📈 Notable Shift</span>`;

      const sharp     = mv.sharp_side || "";
      const isML      = p.type === "ML" || p.type === "RL";
      const pickedTeam = p.team || "";

      // Did sharp money go WITH or AGAINST our pick?
      const sharpAgrees = sharp && pickedTeam &&
        (sharp.toLowerCase().includes(pickedTeam.toLowerCase()) ||
         pickedTeam.toLowerCase().includes(sharp.split(" ").slice(-1)[0].toLowerCase()));
      const sharpLabel = !sharp ? ""
        : sharpAgrees
          ? `<span class="move-confirm">✓ Pro money agrees</span>`
          : `<span class="move-reverse">⚠ Pro money going other way</span>`;

      // Build readable movement description
      let moveDesc = "";
      if(mv.ml_signal === "STEAM" || mv.ml_signal === "DRIFT"){
        const isSideAway = sharp && mv.away && sharp.toLowerCase().includes(mv.away.split(" ").slice(-1)[0].toLowerCase());
        const openOdds = isSideAway ? mv.ml_away_open : mv.ml_home_open;
        const nowOdds  = isSideAway ? mv.ml_away_now  : mv.ml_home_now;
        const pts      = Math.abs(isSideAway ? mv.ml_away_move : mv.ml_home_move || 0);
        const fmtOdds  = o => o ? (o > 0 ? `+${Math.round(o)}` : Math.round(o)) : "?";
        if(openOdds && nowOdds && sharp){
          moveDesc = `${sharp.split(" ").slice(-1)[0]} odds: opened ${fmtOdds(openOdds)} → now ${fmtOdds(nowOdds)} (${pts} pt shift)`;
        }
      } else if(mv.total_signal === "STEAM" || mv.total_signal === "DRIFT"){
        const dir = (mv.total_move || 0) > 0 ? "higher" : "lower";
        moveDesc = `Total moved ${Math.abs(mv.total_move || 0).toFixed(1)} runs ${dir}: opened ${mv.total_open} → now ${mv.total_now}`;
      }

      moveHtml = `<div class="move-row">
        <span class="move-icon">💰</span>
        ${sigBadge}
        ${moveDesc ? `<span class="move-detail">${moveDesc}</span>` : ""}
        ${sharpLabel}
      </div>`;
    }

    // TOSSUP cards get a special split layout
    if(p.tier === "TOSSUP") {
      const typeLabel = p.type==="TOTAL"?"Over/Under":p.type==="ML"?"Win Bet":p.type==="RL"?"Spread":p.type;
      grid.innerHTML += `
        <div class="pick-card tier-TOSSUP" data-type="${p.type}" data-tier="TOSSUP">
          <div class="pick-top">
            <span class="pick-type-badge badge-${p.type}">${typeLabel}</span>
            <span class="tier-badge tb-TOSSUP">≈ TOSS UP</span>
          </div>
          <div class="pick-label">${teamLogo(p.away,16)}${teamLogo(p.home,16)}${p.game}</div>
          <div class="pick-game">Model has no clear edge — shown for coverage</div>
          <div class="tossup-split">
            <div class="tossup-side favored">
              <div class="tossup-team">${p.team}</div>
              <div class="tossup-pct-fav">${p.conf}% — slight lean</div>
            </div>
            <div class="tossup-side">
              <div class="tossup-team">${p.opp_team}</div>
              <div class="tossup-pct-other">${p.opp_conf}%</div>
            </div>
          </div>
          <div class="tossup-note">No bet sizing — check sharp action tab before considering</div>
          <div class="pick-card-props-toggle analysis-toggle" onclick="toggleCardProps(event, this)">
            <span class="toggle-arrow">▶</span> 🔎 Analysis — is this a good bet?
          </div>
          <div class="pick-card-props-panel">
            ${buildAnalysis(p)}
          </div>
          <div class="pick-card-props-toggle" onclick="toggleCardProps(event, this)">
            <span class="toggle-arrow">▶</span> View Player Props for this game
          </div>
          <div class="pick-card-props-panel">
            ${buildInlineProps(p.away, p.home)}
          </div>
        </div>`;
    } else {
    // A BEST BET IS NEVER HIDDEN (fixed 2026-08-17).
    //
    // Tier and Best Bets answer different questions. Tier is the model's raw
    // confidence; Best Bets is EV against the real price. They disagree by
    // design — the ML band that actually makes money is 50-55% stated, which
    // lands in LEAN. So the top of the shortlist was being routed into a
    // collapsed drawer labelled "lower confidence — watch only", telling the
    // reader to bet it and ignore it on the same screen.
    //
    // Anything the shortlist promotes stays in the main grid regardless of tier.
    const _isBB = !!bestBetEval(p, {});
    const _tg = (p.tier === "LEAN" && filterTier === "all" && !_isBB)
      ? (leanGridEl || grid) : grid;
    pickData.push(p);
    const _legIdx = pickData.length - 1;
    p._idx = _legIdx;                 // so the bet-log control can resolve it back
    _tg.innerHTML += `
      <div class="pick-card tier-${p.tier}${_isBB?' pick-bestbet':''}${_isFinal(_findScore(p))?' pick-done':''}${_isHighConf(p)?' pick-highconf':''}" data-type="${p.type}" data-tier="${p.tier}" data-highconf="${_isHighConf(p)?'1':'0'}">
        ${_isBB ? '<div class="bb-flag">&#9733; BEST BET</div>' : ''}
        ${teamEmblem(p.team)}
        <div class="pick-top">
          <span class="pick-type-badge badge-${p.type}">${p.type==="TOTAL"?"Over/Under":p.type==="ML"?"Win Bet":p.type==="RL"?"Spread":p.type}</span>
          ${_isHighConf(p)?`<span class="hc-badge" title="${_highConfTitle(p)}">🔥 HIGH CONFIDENCE</span>`:""}
          ${_isProfitBand(p)?`<span class="pb-badge" title="${_profitBandTitle(p)}">📈 PROFITABLE</span>`:""}
          ${(p.type!=="RL" && (p.value_tag==="NO VALUE"||p.value_chalk))?`<span class="val-warn-badge" title="Model does not beat this price by enough — priced too short">⚠ ${p.value_chalk?"CHALK":"NO VALUE"}</span>`:""}
          <span class="tier-badge tb-${p.tier}">${tierIcon(p.tier)} ${p.tier}${p.tier==="LEAN"?" — thin edge":""}</span>
        </div>
        <div class="pick-label">${p.label}</div>
        <div class="pick-game">${teamLogo(p.away,16)}${teamLogo(p.home,16)}${p.game}</div>
        ${(()=>{ const st=_statusFor(p.away, p.home); return st ? `<div class="gs-banner">${st.badge} <span>This game is ${st.detail.toLowerCase()} — the bet may not stand.</span></div>` : ""; })()}
        <div class="conf-row">
          <div class="conf-bar-wrap">
            <div class="conf-bar bar-${p.tier}" style="width:${barPct}%"></div>
          </div>
          <span class="conf-pct pct-${p.tier}">${p.conf}%</span>
        </div>
        ${favHtml}
        ${warnHtml}
        ${kalshiHtml}
        ${moveHtml}
        ${lineShopHtml}
        ${kellyHtml}
        ${oddsHtml(p)}
        ${valueHtml(p)}${honestReadHtml(p)}
        ${bookShopHtml(p)}
        ${priceCheckHtml(p)}
        ${betLogHtml(p, 'bl' + Math.abs(hashStr((p.label||'') + (p.game||'') + (p.type||''))))}
        <div class="pick-reasoning">${p.reasoning}</div>
        ${p.narrative ? `<div class="pick-narrative">${p.narrative}</div>` : ""}
        ${(()=>{
          const _sc = _findScore(p);
          if(!_isFinal(_sc)) return "";
          const _res = _pickResult(p, _sc);
          if(_res==="win")  return '<div class="pick-done-banner win-banner">✓ WIN</div>';
          if(_res==="loss") return '<div class="pick-done-banner loss-banner">✗ LOSS</div>';
          if(_res==="push") return '<div class="pick-done-banner" style="background:rgba(139,148,158,.12);color:#8b949e;border:1px solid rgba(139,148,158,.25)">— PUSH</div>';
          return "";
        })()}
        <div class="pick-card-props-toggle analysis-toggle" onclick="toggleCardProps(event, this)">
          <span class="toggle-arrow">▶</span> 🔎 Analysis — is this a good bet?
        </div>
        <div class="pick-card-props-panel">
          ${buildAnalysis(p)}
        </div>
        <div class="pick-card-props-toggle" onclick="toggleCardProps(event, this)">
          <span class="toggle-arrow">▶</span> View Player Props for this game
        </div>
        <div class="pick-card-props-panel">
          ${buildInlineProps(p.away, p.home)}
        </div>
        <button class="add-leg-btn" data-leg="${_legIdx}" onclick="toggleLegByIdx(event,+this.dataset.leg)" title="Add to parlay">➕ Add to Parlay</button>
      </div>`;
    }
  });
  // Route LEAN picks to collapsed section when showing all tiers
  const leanGrid   = document.getElementById("leanGrid");
  const leanToggle = document.getElementById("leanToggle");
  const leanLabel  = document.getElementById("leanLabel");
  const leanCount  = leanGrid ? leanGrid.querySelectorAll(".pick-card").length : 0;
  if(leanToggle){
    if(filterTier === "all" && leanCount > 0){
      leanToggle.style.display = "block";
      leanLabel.textContent = `Show ${leanCount} Lean pick${leanCount===1?"":"s"} `
        + `(lower confidence, and none of them cleared Best Bets)`;
    } else {
      leanToggle.style.display = "none";
    }
  }
  document.getElementById("pickResults").innerHTML =
    `Showing <b>${visible}</b> of <b>${ACTIVE_PICKS.length}</b> picks`;
  if(visible===0) grid.innerHTML = `<div class="empty">No picks match the current filters.</div>`;
  renderBestBets();
}

function tierIcon(t){ return t==="LOCK"?"🔒":t==="STRONG"?"⭐⭐":t==="TOSSUP"?"≈":"⭐"; }

function toggleLean(){
  const body  = document.getElementById("leanBody");
  const arrow = document.getElementById("leanArrow");
  const label = document.getElementById("leanLabel");
  const open  = body.style.display === "none";
  body.style.display  = open ? "block" : "none";
  arrow.textContent   = open ? "▼" : "▶";
  const cnt = document.getElementById("leanGrid")
    ? document.getElementById("leanGrid").querySelectorAll(".pick-card").length : 0;
  label.textContent = open
    ? `Hide Lean picks`
    : `Show ${cnt} Lean pick${cnt===1?"":"s"} (lower confidence — watch only)`;
}

function renderSurfacedProps(){
  const section = document.getElementById("surfacedPropsSection");
  const grid    = document.getElementById("surfacedPropsGrid");
  const empty   = document.getElementById("surfacedPropsEmpty");
  if(!section || !grid || !DATA_PROPS || !DATA_PROPS.length){
    if(section) section.style.display = "none";
    return;
  }
  // BETTABLE ONLY. This surface previously filtered on confidence alone, so it
  // filled with Hits Over 0.5, Total Bases Over 1.5 and RBIs Over 0.5 — props
  // scored against a line the model invents, not one a book offers. A 68% read
  // on Over 0.5 Hits is a LOSING bet at the -250 to -350 books actually price
  // it at, so this panel was ranking by how fake the line was. prep_props
  // already sets projection_only from SUPPRESS_BETTABLE_PROPS; the flag was on
  // the data and this renderer just ignored it. Now it does not.
  // A prop reaches this surface only with a REAL book line that cleared the
  // price floor and the mirage guard. projection_only is now per-pick, so a
  // player Pinnacle lists gets through while the same prop type for an unlisted
  // player stays a projection.
  // DROP PROPS WHOSE GAME HAS STARTED (2026-08-17). Top Props was still
  // offering Total Bases lines for games that had already finished, because
  // this filter only ever asked whether the prop had a real book line — never
  // whether the game was still ahead. Same mistake as the pick grid: filtering
  // on "is it a valid bet" without asking "is it still available".
  //
  // Uses the live score feed the ticker already maintains, so it self-corrects
  // every couple of minutes without another data source.
  const _liveByGame = {};
  (_liveScores() || []).forEach(g => {
    const key = ((g.away||"") + " @ " + (g.home||"")).trim();
    _liveByGame[key] = g;
  });
  const _gameStillAhead = (p) => {
    const g = _liveByGame[((p.away||"") + " @ " + (p.home||"")).trim()];
    if(!g) return true;                       // unknown -> do not hide it
    const st = (g.detailedState || g.status || "").toLowerCase();
    if(!st) return true;
    return !(st.includes("progress") || st.includes("final") ||
             st.includes("game over") || st.includes("completed"));
  };
  const bettable = DATA_PROPS
    .filter(p => !p.projection_only && p.bettable)
    .filter(_gameStillAhead);
  // Rank by EV, not confidence. On a real two-way line, a high probability at a
  // heavily juiced price is worth less than a modest probability at a fair one —
  // the same confidence-is-not-value trap that made the old board misleading.
  // No confidence floor: EV already encodes whether it is worth betting.
  const topProps = bettable
    .filter(p => p.ev != null && p.ev > 0)
    .sort((a,b) => (b.ev||0)-(a.ev||0));

  section.style.display = "block";
  if(!topProps.length){
    // Say WHY it is empty rather than hiding the panel. An empty bettable
    // surface is real information: it means nothing today has both a genuine
    // book line and an edge. Silently vanishing reads as a broken page.
    const suppressed = DATA_PROPS.filter(p => p.projection_only && p.conf >= 65).length;
    grid.style.display = "none";
    if(empty){
      empty.style.display = "block";
      empty.innerHTML = "<strong style='color:#ffb74d'>No bettable props right now.</strong><br>"
        + "Only props with a real sportsbook line and price appear here — today that means "
        + "strikeout props off Pinnacle. Hits, Total Bases, RBI, Runs, HR and SB are scored "
        + "against a line the model sets itself, so they are research projections, not plays, "
        + "and they live in the Player Props tab."
        + (suppressed ? "<br><br><span style='color:var(--sub)'>" + suppressed
            + " projection-only prop" + (suppressed===1?"":"s") + " at 65%+ moved to that tab.</span>" : "")
        + "<br><br><span style='color:var(--sub)'>Pitcher K lines post through the morning, so "
        + "this often fills in later. Batter props need a real line source before they can "
        + "appear here at all.</span>";
    }
    return;
  }
  grid.style.display = "";
  if(empty) empty.style.display = "none";
  grid.innerHTML = "";
  const _sfxBuf = [];
  topProps.forEach(p => {
    const overUnder = p.pick_side || (p.proj >= p.line ? "OVER" : "UNDER");
    const label     = propLabel(p.prop_type, p.line, overUnder);
    const projColor = overUnder === "OVER" ? "var(--green)" : "var(--blue)";
    const _bp       = overUnder === "UNDER" ? p.under_price : p.over_price;
    const _price = (p.price != null) ? p.price : _bp;
    const bookRow   = (p.line != null && _price != null)
      ? `<div class="prop-line-row"><span class="prop-line-label">📕 REAL LINE (Pinnacle)</span><span class="prop-line-val">${label} &nbsp;${_price>0?"+":""}${_price}</span></div>`
        + (p.ev != null ? `<div class="prop-line-row"><span class="prop-line-label">EV</span><span class="prop-line-val" style="color:${p.ev>0?'var(--green)':'var(--red)'}">${p.ev>0?"+":""}${(p.ev*100).toFixed(1)}%${p.market_prob!=null?` · model ${(p.conf).toFixed(0)}% vs market ${(p.market_prob*100).toFixed(0)}%`:""}</span></div>` : "")
      : "";
    const barPct    = Math.min(100, Math.max(0, (p.conf - 50) * 2));
    const projBanner = p.projected
      ? `<div style="background:rgba(255,183,77,.1);border-bottom:1px solid rgba(255,183,77,.2);padding:3px 10px;font-size:.68rem;color:#ffb74d;letter-spacing:.04em">📋 Lineup not confirmed yet</div>`
      : "";
    _sfxBuf.push(`
      <div class="pick-card tier-${p.tier}" data-type="PROP" data-tier="${p.tier}">
        ${projBanner}
        <div class="pick-top">
          <span class="pick-type-badge badge-PROP">👤 PROP · ${p.prop_type}</span>
          <span class="tier-badge tb-${p.tier}">${tierIcon(p.tier)} ${p.tier}</span>
        </div>
        <div class="prop-name-row">
          ${p.player_id ? `<a href="/player/${p.player_id}" title="View ${p.player_name}'s trends"><img class="prop-face" alt="" loading="lazy" src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/${p.player_id}/headshot/67/current" onerror="this.style.display='none'"></a>` : ""}
          <div class="pick-label">${p.player_id ? `<a href="/player/${p.player_id}" class="prop-link">${p.player_name}</a>` : p.player_name} — ${label}</div>
        </div>
        <div class="pick-game">${p.game}</div>
        <div class="conf-row">
          <div class="conf-bar-wrap">
            <div class="conf-bar bar-${p.tier}" style="width:${barPct}%"></div>
          </div>
          <span class="conf-pct pct-${p.tier}">${p.conf}%</span>
        </div>
        ${bookRow}
        <div style="font-size:.76rem;color:var(--sub);margin-top:6px">
          Model projection: <span style="color:${projColor};font-weight:600">${p.proj} (${overUnder})</span>
        </div>
        <div class="pick-reasoning">${p.reasoning||""}</div>
      </div>`);
  });
  grid.innerHTML = _sfxBuf.join('');   // one DOM write, not N
}


// ── Inline props for pick card ────────────────────────────────────────────────
function buildInlineProps(away, home){
  if(!DATA_PROPS || !DATA_PROPS.length){
    return `<div class="inline-props-empty">Player props will appear here once lineups are confirmed — check back closer to game time.</div>`;
  }
  const gameProps = DATA_PROPS.filter(p =>
    (p.away === away && p.home === home) ||
    (p.game && p.game.includes(away) && p.game.includes(home))
  );
  if(!gameProps.length){
    return `<div class="inline-props-empty">No props available for this game yet.</div>`;
  }
  // Sort by conf descending, show top 8
  const sorted = [...gameProps].sort((a,b) => (b.conf||0)-(a.conf||0)).slice(0,8);
  return sorted.map(p => {
    const betDesc = propLabel(p.prop_type, p.line, p.pick_side);   // side-aware (K can be Under)
    const tierCls = `inline-prop-tier-${p.tier||"LEAN"}`;
    const propId = `prop::${p.away}::${p.home}::${p.player_name}::${p.prop_type}::${p.line}`;
    const propLabel2 = `${p.player_name||"?"} ${betDesc}`;
    return `<div class="inline-prop" id="prop-row-${CSS.escape(propId)}">
      <span class="inline-prop-player">${p.player_name||"—"}</span>
      <span class="inline-prop-line">${betDesc}</span>
      <span class="inline-prop-conf ${tierCls}">${p.conf||"—"}% ${tierIcon(p.tier||"LEAN")}</span>
      <button class="inline-prop-add" onclick="togglePropLeg(event,'${propId.replace(/'/g,"\'")}','${propLabel2.replace(/'/g,"\'")}',${p.conf||50})" title="Add prop to parlay">+</button>
    </div>`;
  }).join("");
}

function toggleCardProps(event, el){
  event.stopPropagation();
  const panel = el.nextElementSibling;
  const arrow = el.querySelector(".toggle-arrow");
  const isOpen = panel.classList.toggle("open");
  arrow.textContent = isOpen ? "▼" : "▶";
  el.closest(".pick-card").classList.toggle("expanded", isOpen);
}

// ── Sharp Money Panel ─────────────────────────────────────────────────────────
function renderSharpMoney(){
  const section = document.getElementById("sharpMoneySection");
  const grid    = document.getElementById("sharpMoneyGrid");
  if(!DATA_MOVEMENT || DATA_MOVEMENT.length === 0){
    section.style.display = "none";
    return;
  }
  section.style.display = "block";
  grid.innerHTML = "";

  DATA_MOVEMENT.forEach(mv => {
    const awayName = mv.away.split(" ").slice(-1)[0];
    const homeName = mv.home.split(" ").slice(-1)[0];

    // Determine the dominant signal
    const sig = mv.ml_signal === "STEAM" ? "STEAM"
              : mv.total_signal === "STEAM" ? "STEAM"
              : mv.ml_signal === "DRIFT"  ? "DRIFT" : "DRIFT";
    const sigBadge = sig === "STEAM"
      ? `<span class="sharp-badge sharp-badge-steam">🔥 Heavy Action</span>`
      : `<span class="sharp-badge sharp-badge-drift">📈 Notable Shift</span>`;

    // Plain-English description
    let desc = "";
    const sharp = mv.sharp_side || "";
    const sharpNick = sharp ? sharp.split(" ").slice(-1)[0] : "";

    const fmtO = o => o ? (o > 0 ? `+${Math.round(o)}` : `${Math.round(o)}`) : "?";
    if((mv.ml_signal === "STEAM" || mv.ml_signal === "DRIFT") && sharp){
      const isSideAway = mv.away && sharpNick.toLowerCase() === mv.away.split(" ").slice(-1)[0].toLowerCase();
      const openOdds   = isSideAway ? mv.ml_away_open : mv.ml_home_open;
      const nowOdds    = isSideAway ? mv.ml_away_now  : mv.ml_home_now;
      const pts        = Math.abs((isSideAway ? mv.ml_away_move : mv.ml_home_move) || 0);
      const word       = sig === "STEAM" ? "heavily" : "noticeably";
      desc = `Professional bettors are ${word} backing the <strong>${sharpNick}</strong> to win. `;
      desc += `The win odds opened at ${fmtO(openOdds)} and have moved to ${fmtO(nowOdds)} — a ${pts}-point shift in their favor`;
      if(mv.total_signal === "STEAM" || mv.total_signal === "DRIFT"){
        const tdir = (mv.total_move||0) > 0 ? "higher" : "lower";
        desc += `. The game total also shifted ${Math.abs(mv.total_move||0).toFixed(1)} runs ${tdir} (${mv.total_open} → ${mv.total_now})`;
      }
      desc += ".";
    } else if(mv.total_signal === "STEAM" || mv.total_signal === "DRIFT"){
      const tdir = (mv.total_move||0) > 0 ? "higher (more runs expected)" : "lower (fewer runs expected)";
      const pts  = Math.abs(mv.total_move||0).toFixed(1);
      desc = `The expected total runs for this game shifted ${pts} runs ${tdir} — opened at ${mv.total_open}, now at ${mv.total_now}. Large bets came in on the ${(mv.total_move||0)>0?"Over":"Under"}.`;
    } else if(mv.kalshi_signal === "STEAM" || mv.kalshi_signal === "DRIFT"){
      const kSharp = mv.kalshi_sharp_side || "";
      const kNick  = kSharp ? kSharp.split(" ").slice(-1)[0] : "";
      const kWord  = mv.kalshi_signal === "STEAM" ? "strongly" : "noticeably";
      desc = kNick
        ? `The Kalshi prediction market shifted ${kWord} toward the <strong>${kNick}</strong>. Sportsbook lines haven't moved yet — this can be an early signal before books adjust.`
        : `The Kalshi prediction market repriced this game — sportsbook lines haven't moved yet.`;
    }

    // ── Kalshi confirmation badge ─────────────────────────────────────────
    let kalshiHtml = "";
    if(mv.kalshi_signal === "STEAM" || mv.kalshi_signal === "DRIFT"){
      const kSharp    = mv.kalshi_sharp_side || "";
      const kNick     = kSharp ? kSharp.split(" ").slice(-1)[0] : "";
      const kMovePct  = mv.kalshi_away_move != null
        ? Math.round(Math.abs(mv.kalshi_away_move) * 100) : 0;
      const kSigColor = mv.kalshi_signal === "STEAM" ? "#ef5350" : "#ffa726";
      const kLabel    = mv.kalshi_signal === "STEAM" ? "STEAM" : "DRIFT";

      // Does Kalshi sharp side match sportsbook sharp side?
      const sbSharpNick = (mv.sharp_side||"").split(" ").slice(-1)[0].toLowerCase();
      const ksMatch = kNick && sbSharpNick && kNick.toLowerCase() === sbSharpNick;
      const ksBadge = ksMatch
        ? `<span style="color:#69f0ae;font-size:.72rem;font-weight:700">✓ Confirms sportsbook signal</span>`
        : (mv.sharp_side
          ? `<span style="color:#ffa726;font-size:.72rem;font-weight:700">⚠ Diverges from sportsbook</span>`
          : "");

      kalshiHtml = `<div style="margin-top:8px;padding:8px 10px;background:rgba(100,181,246,.07);
        border-left:3px solid #42a5f5;border-radius:4px;font-size:.78rem">
        <span style="color:#42a5f5;font-weight:700">📊 Kalshi Market</span>
        <span style="margin-left:8px;color:${kSigColor};font-weight:700">${kLabel}</span>
        ${kNick ? `<span style="color:#e2e8f0"> — Market moving toward <strong>${kNick}</strong>` +
          (kMovePct ? ` (+${kMovePct}pp)` : "") + `</span>` : ""}
        ${ksBadge ? `<span style="margin-left:10px">${ksBadge}</span>` : ""}
      </div>`;
    }

    // Does sharp money agree with any of our picks for this game?
    const gamePick = ACTIVE_PICKS.find(p => p.away === mv.away && p.home === mv.home);
    let agreeHtml = "";
    if(gamePick && sharp){
      const pickTeamNick = gamePick.team ? gamePick.team.split(" ").slice(-1)[0] : "";
      const sharpMatch   = sharpNick && pickTeamNick &&
        (sharpNick.toLowerCase() === pickTeamNick.toLowerCase());
      if(sharpMatch){
        agreeHtml = `<span class="sharp-agree">✓ Agrees with our ${gamePick.tier} pick</span>`;
      } else if(gamePick.type === "ML" || gamePick.type === "RL"){
        agreeHtml = `<span class="sharp-reverse">⚠ Goes against our ${gamePick.tier} pick — use caution</span>`;
      }
    } else if(gamePick){
      agreeHtml = `<span class="sharp-neutral">No model pick for this game</span>`;
    }

    grid.innerHTML += `<div class="sharp-game">
      <div>
        <div class="sharp-matchup">${awayName} @ ${homeName} ${sigBadge}</div>
        ${agreeHtml ? `<div style="margin-top:4px">${agreeHtml}</div>` : ""}
      </div>
      <div class="sharp-desc">${desc}${kalshiHtml}</div>
    </div>`;
  });
}

// ── Yesterday's Results Banner ────────────────────────────────────────────────
// ── LEARN FROM YESTERDAY ─────────────────────────────────────────────────────
// Applies the SAME checks to every graded run line pick, side by side, on games
// whose outcome is already known. Built so the checks can be practised where the
// answer is visible, rather than only on live picks where it is not.
//
// The three checks, in the order they should be applied:
//   1. Is the price coherent? P(cover +1.5) must exceed P(win outright), since
//      covering includes every win PLUS losing by one. If not, the price is
//      broken and nothing else matters.
//   2. Is the confidence inside the 57-65% band?
//   3. Does the band's observed rate beat what the price demands?
//
// Deliberately shows losses that passed every check. Those exist and always
// will: a 67% bet loses a third of the time. Hunting for a cause in them is how
// a model gets tuned on noise.
const LEARN_BANDS = [
  { lo: 57, hi: 60, rate: 53.3, rec: "40-35" },
  { lo: 60, hi: 65.1, rate: 67.4, rec: "29-14" }
];
function _imp(a){ return a < 0 ? Math.abs(a)/(Math.abs(a)+100) : 100/(a+100); }

function renderLearn(){
  const box = document.getElementById("learnFromYesterday");
  if(!box || !DATA_YESTERDAY) return;
  const gp = DATA_YESTERDAY.graded_picks || [];
  const rl = gp.filter(p => (p.pick_type || p.type) === "RL");
  if(!rl.length){ box.innerHTML = ""; return; }

  const rows = rl.map(p => {
    const conf = (p.conf > 1.5 ? p.conf : p.conf * 100);
    const res  = (p.actual_result || p.result || "").toUpperCase();
    const band = LEARN_BANDS.find(b => conf >= b.lo && conf < b.hi);
    const price = p.odds != null ? p.odds : null;
    const needs = price != null ? _imp(price) * 100 : null;
    const sharp = p.sharp_side && p.team
      ? (p.sharp_side.trim() === p.team.trim() ? "agrees" : "against") : "—";
    let verdict, vcolor;
    if(!band){ verdict = "outside band"; vcolor = "var(--sub)"; }
    else if(price == null){ verdict = "price not captured"; vcolor = "var(--sub)"; }
    else if(band.rate <= needs){ verdict = "price too steep"; vcolor = "var(--red)"; }
    else { verdict = "playable"; vcolor = "var(--green)"; }
    const score = (p.away_final != null && p.home_final != null)
      ? `${p.away_final}-${p.home_final}` : "—";
    const rc = res === "WIN" ? "var(--green)" : res === "LOSS" ? "var(--red)" : "var(--sub)";
    return `<tr>
      <td style="padding:9px 10px;font-weight:600">${p.label || ""}</td>
      <td style="padding:9px 10px">${conf.toFixed(1)}%</td>
      <td style="padding:9px 10px">${band ? "yes" : "<span style='color:var(--sub)'>no</span>"}</td>
      <td style="padding:9px 10px">${price != null ? (price>0?"+":"")+Math.round(price) : "<span style='color:var(--sub)'>—</span>"}</td>
      <td style="padding:9px 10px">${needs != null ? needs.toFixed(1)+"%" : "—"}</td>
      <td style="padding:9px 10px">${band ? band.rate+"%" : "—"}</td>
      <td style="padding:9px 10px;color:${sharp==='agrees'?'var(--green)':sharp==='against'?'var(--red)':'var(--sub)'}">${sharp}</td>
      <td style="padding:9px 10px;color:${vcolor}">${verdict}</td>
      <td style="padding:9px 10px">${score}</td>
      <td style="padding:9px 10px;font-weight:700;color:${rc}">${res||"—"}</td>
    </tr>`;
  }).join("");

  const played = rl.filter(p => {
    const c = (p.conf > 1.5 ? p.conf : p.conf*100);
    const b = LEARN_BANDS.find(x => c >= x.lo && c < x.hi);
    return b && p.odds != null && b.rate > _imp(p.odds)*100;
  });
  const pw = played.filter(p => (p.actual_result||p.result||"").toUpperCase()==="WIN").length;
  const summary = played.length
    ? `Following the rule would have bet <b>${played.length}</b> of these and gone <b>${pw}-${played.length-pw}</b>.`
    : `No pick cleared all three checks. Prices were not stored before 2026-08-11, so older days show blanks.`;

  box.innerHTML = `
    <div style="margin-top:22px">
      <div class="section-title">🎓 Learn from yesterday — run lines</div>
      <div style="font-size:.8rem;color:var(--sub);margin:-6px 0 12px;line-height:1.55">
        The same three checks applied to picks whose result you already know.
        Read left to right: is the price sane, is the confidence in the band,
        does the band's real record beat what the price demands.
      </div>
      <div style="overflow-x:auto">
      <table style="width:100%;border-collapse:collapse;font-size:.78rem">
        <tr style="color:var(--sub);text-align:left">
          <th style="padding:8px 10px">Pick</th><th style="padding:8px 10px">Conf</th>
          <th style="padding:8px 10px">In band</th><th style="padding:8px 10px">Price</th>
          <th style="padding:8px 10px">Needs</th><th style="padding:8px 10px">Band rate</th>
          <th style="padding:8px 10px">Sharp</th><th style="padding:8px 10px">Verdict</th>
          <th style="padding:8px 10px">Score</th><th style="padding:8px 10px">Result</th>
        </tr>
        ${rows}
      </table></div>
      <div style="font-size:.8rem;color:var(--sub);margin-top:12px;line-height:1.55">
        ${summary}
        <br><br>Some losses will have passed every check. That is expected, not a
        flaw: a 67% bet loses roughly one time in three. The picks worth learning
        from are the ones the checks <i>rejected</i> before the game started.
      </div>
    </div>`;
}

function renderYesterday(){
  if(!DATA_YESTERDAY || !DATA_YESTERDAY.date) return;

  const d   = DATA_YESTERDAY;
  const m   = d.metrics && d.metrics.overall;
  if(!m) return;

  const wr  = m.win_rate ? (m.win_rate * 100).toFixed(1) : "—";
  const roi = m.roi      ? (m.roi * 100).toFixed(1)      : "—";
  const roiColor = (m.roi || 0) >= 0 ? "var(--green)" : "var(--red)";
  const roiSign  = (m.roi || 0) >= 0 ? "+" : "";


// ── ONE SOURCE OF TRUTH FOR YESTERDAY (2026-08-18) ──────────────────────────
// The yesterday panel was reading TWO different graders on the same screen:
//   graded_picks  -> the DB (get_graded_detail), what was actually published
//   metrics.*     -> the analysis JSON, written by run_analysis.py at 6am
// On 08-17 the tier chips totalled 14-8 while the tab below them said 11-10.
// Both were computed honestly; they simply grade different pick sets.
//
// load_yesterday_analysis already prefers the DB for graded_picks. So derive
// EVERY aggregate on this panel from that same list, and fall back to the JSON
// metrics only when the DB gave us nothing. Aggregates must never come from a
// different source than the rows they claim to summarise.
function _ydayAgg(d){
  const gp = (d && d.graded_picks) || [];
  if(!gp.length) return null;                 // no DB rows -> caller uses JSON
  const norm = p => ({
    tier: (p.tier || "").toUpperCase(),
    type: (p.pick_type || p.type || "").toUpperCase(),
    res:  (p.actual_result || p.result || "").toUpperCase()
  });
  const roll = key => {
    const out = {};
    gp.map(norm).forEach(r => {
      const k = r[key]; if(!k) return;
      const b = out[k] || (out[k] = {wins:0, losses:0, pushes:0, total:0});
      if(r.res === "WIN")       { b.wins++;   b.total++; }
      else if(r.res === "LOSS") { b.losses++; b.total++; }
      else if(r.res === "PUSH") { b.pushes++; }
    });
    Object.values(out).forEach(b => {
      b.win_rate = b.total ? b.wins / b.total : 0;
    });
    return out;
  };
  const all = gp.map(norm);
  const w = all.filter(r => r.res === "WIN").length;
  const l = all.filter(r => r.res === "LOSS").length;
  return {
    overall: {wins:w, losses:l, total:w+l, win_rate:(w+l) ? w/(w+l) : 0},
    by_tier: roll("tier"),
    by_type: roll("type")
  };
}

  // Tier breakdown chips — LOCK / STRONG / LEAN each get their own W-L card

// ── HOW THE SHORTLIST WOULD HAVE DONE (2026-08-18) ──────────────────────────
// Model accuracy and "what you were told to bet" are different questions, and
// blending them hides both: a 13-4 accuracy day next to a 1-0 recommended day
// collapses into a number that means neither.
//
// get_graded_detail already carries conf, pick_type, label and odds, which is
// everything bestBetEval needs, so this needs no new persistence.
//
// HONEST CAVEAT, stated on the panel: this applies TODAY's rule (current band
// table, plateau and price-profile guards) to past picks. It is therefore a
// backtest of the rule as it stands, not a log of what the board said at the
// time. That is the more useful question — "would my current rule have made
// money on these" — but it is not history, and the label says so.
function _ydayBestBets(d){
  const gp = (d && d.graded_picks) || [];
  if(!gp.length) return null;
  let w = 0, l = 0, n = 0;
  gp.forEach(p => {
    let c = parseFloat(p.conf);
    if(!isFinite(c)) return;
    if(c <= 1.5) c *= 100;                       // DB stores 0-1, board uses 0-100
    const shim = {
      type: (p.pick_type || p.type || "").toUpperCase(),
      conf: c,
      label: p.label || "",
      pick_price: (p.odds != null ? parseFloat(p.odds) : null),
      team: p.team || "", away: p.away_team || ""
    };
    if(!bestBetEval(shim, {})) return;
    n++;
    const r = (p.actual_result || p.result || "").toUpperCase();
    if(r === "WIN") w++; else if(r === "LOSS") l++;
  });
  return n ? {picks:n, wins:w, losses:l} : {picks:0, wins:0, losses:0};
}

  // TOSSUP included at Justin's call (2026-08-18): it is shown on the board, so
  // it belongs in the record. OVERALL is rendered last and must equal the sum of
  // the four — if it ever does not, the panel is reading two datasets again and
  // the discrepancy is visible instead of silent.
  const TIER_ORDER = ["LOCK","STRONG","LEAN","TOSSUP"];
  const TIER_ICON  = {LOCK:"🔒",STRONG:"⭐⭐",LEAN:"⭐",TOSSUP:"≈"};
  const TIER_COL   = {LOCK:"#ffc107",STRONG:"#42a5f5",LEAN:"#66bb6a",TOSSUP:"#8b949e"};
  const _agg       = _ydayAgg(d);
  const byTier     = (_agg && _agg.by_tier) || (d.metrics && d.metrics.by_tier) || {};

  const tierChips = TIER_ORDER.map(t => {
    const b = byTier[t];
    const hasData = b && b.total > 0;
    const recColor = hasData
      ? ((b.wins||0) > (b.losses||0) ? "var(--green)"
          : (b.losses||0) > (b.wins||0) ? "var(--red)" : "var(--sub)")
      : "var(--border)";
    const record = hasData ? `${b.wins||0}-${b.losses||0}` : "—";
    const pct    = hasData && b.win_rate ? `${(b.win_rate*100).toFixed(0)}%` : "";
    return `<div class="yday-tier-chip">
      <div class="yday-tier-name" style="color:${TIER_COL[t]}">${TIER_ICON[t]} ${t}</div>
      <div class="yday-tier-record" style="color:${recColor}">${record}</div>
      ${pct ? `<div class="yday-tier-pct">${pct}</div>` : ""}
    </div>`;
  }).join("") + (() => {
    // OVERALL, rendered from the SAME byTier object the chips came from, so the
    // arithmetic is self-checking on screen. If these ever stop summing, the
    // panel is reading two datasets and you can see it immediately.
    const tw = TIER_ORDER.reduce((a,t) => a + ((byTier[t]||{}).wins   || 0), 0);
    const tl = TIER_ORDER.reduce((a,t) => a + ((byTier[t]||{}).losses || 0), 0);
    if(!(tw + tl)) return "";
    const col = tw > tl ? "var(--green)" : tl > tw ? "var(--red)" : "var(--sub)";
    const bb  = _ydayBestBets(d);
    const bbTxt = (bb && bb.picks)
      ? `<div class="yday-tier-pct" title="Today's Best Bets rule applied to these picks. A backtest of the current rule, not a log of what the board said at the time.">&#9733; best bets ${bb.wins}-${bb.losses}</div>`
      : (bb ? `<div class="yday-tier-pct">&#9733; none qualified</div>` : "");
    return `<div class="yday-tier-chip" style="border-color:var(--border-strong,#484f58)">
      <div class="yday-tier-name" style="color:#e6edf3">OVERALL</div>
      <div class="yday-tier-record" style="color:${col}">${tw}-${tl}</div>
      <div class="yday-tier-pct">${(100*tw/(tw+tl)).toFixed(0)}%</div>
      ${bbTxt}
    </div>`;
  })();

  // Props summary from yesterday
  const py = (d.props_yesterday) || {};
  const propWins   = py.wins   != null ? py.wins   : null;
  const propLosses = py.losses != null ? py.losses : null;
  const propRecord = propWins != null
    ? `🎯 Props ${propWins}-${propLosses}`
    : "";
  const topPropsHtml = (py.top || []).slice(0,4).map(p =>
    `<span class="yday-prop-chip ${p.hit ? "win":"loss"}">${p.player} ${p.prop_type} ${p.hit ? "✓":"✗"}</span>`
  ).join("");

  // Mini banner on picks panel
  document.getElementById("yesterdayBanner").innerHTML = `
    <div class="yesterday-banner">
      <div class="yesterday-title">📈 Yesterday (${d.date})</div>
      <div class="yday-tier-row">${tierChips}</div>
      <div class="yday-overall-row">
        <span class="yday-overall-rec">${m.wins}-${m.losses} Overall</span>
        <span class="yday-overall-roi" style="color:${roiColor}">${roiSign}${roi}% ROI</span>
        <span class="yday-overall-profit" id="ydayProfit" data-profit="${(m.profit||0).toFixed(4)}">${(m.profit||0)>=0?"+":""}$${(window._BR ? Math.abs((m.profit||0)*window._BR) : Math.abs(m.profit||0)).toFixed(2)}${window._BR && window._BR!==1 ? "" : " <small style='color:#484f58'>($1 bankroll)</small>"}</span>
        ${propRecord ? `<span class="yday-overall-props">${propRecord}</span>` : ""}
      </div>
      ${topPropsHtml ? `<div class="yday-props-row">${topPropsHtml}</div>` : ""}
    </div>`;

  // Show Yesterday tab
  document.getElementById("yesterdayTab").style.display = "";

  // Full yesterday panel
  const tierRows = Object.entries((_agg && _agg.by_tier) || (d.metrics && d.metrics.by_tier) || {}).map(([t, b]) => {
    if(!b.total) return "";
    const twr = b.win_rate ? (b.win_rate*100).toFixed(1)+"%" : "—";
    return `<tr>
      <td>${t}</td><td>${b.wins}-${b.losses}</td><td>${twr}</td>
      <td>${b.roi ? (b.roi*100).toFixed(1)+"%" : "—"}</td>
      <td>${(b.profit||0) >= 0 ? "+":"−"}$${Math.abs(b.profit||0).toFixed(2)}</td>
    </tr>`;
  }).join("");

  const typeRows = Object.entries((_agg && _agg.by_type) || (d.metrics && d.metrics.by_type) || {}).map(([t, b]) => {
    if(!b.total) return "";
    const twr = b.win_rate ? (b.win_rate*100).toFixed(1)+"%" : "—";
    return `<tr>
      <td>${t}</td><td>${b.wins}-${b.losses}</td><td>${twr}</td>
      <td>${b.roi ? (b.roi*100).toFixed(1)+"%" : "—"}</td>
      <td>${(b.profit||0) >= 0 ? "+":"−"}$${Math.abs(b.profit||0).toFixed(2)}</td>
    </tr>`;
  }).join("");

  const tblStyle = "width:100%;border-collapse:collapse;font-size:.8rem;margin-bottom:20px";
  const thStyle  = "text-align:left;padding:6px 10px;color:var(--sub);font-size:.72rem;font-weight:700;text-transform:uppercase;border-bottom:1px solid var(--border)";
  const tdStyle  = "padding:6px 10px;border-bottom:1px solid rgba(255,255,255,.04)";

  const allFindings = (d.findings || []).map(f => {
    const isWarn = f.includes("⚠");
    return `<div class="yday-finding${isWarn ? " warn" : ""}" style="padding:4px 0">${f}</div>`;
  }).join("");

  const allRecs = (d.recommendations || []).map(r => `
    <div style="margin-bottom:12px;padding:10px 14px;background:rgba(255,255,255,.03);border:1px solid var(--border);border-radius:8px">
      <div style="font-size:.72rem;font-weight:700;color:${r.priority==='HIGH'?'var(--red)':r.priority==='MED'?'var(--gold)':'var(--sub)'};text-transform:uppercase;margin-bottom:4px">[${r.priority}] ${r.area}</div>
      <div style="font-size:.78rem;color:var(--sub)">${r.action}</div>
    </div>`).join("");

  // ── Per-pick graded cards (Daily-Summary-style detail for the completed day) ──
  const TIER_COLOR_Y = {LOCK:"#ffc107",STRONG:"#42a5f5",LEAN:"#66bb6a",TOSSUP:"#a09ae0"};
  const gp = d.graded_picks || [];
  let gradedCardsHtml = "";
  if(gp.length){
    let gw=0, gl=0, gpsh=0;
    const cards = gp.map(p => {
      // Handle BOTH schemas: DB detail uses actual_result/pick_type; the analysis
      // JSON uses result/type. Missing this made every card default to PUSH (0-0).
      const res = (p.actual_result||p.result||"").toUpperCase();
      if(res==="WIN") gw++; else if(res==="LOSS") gl++; else if(res==="PUSH") gpsh++;
      const border = res==="WIN"?"#3fb950":res==="LOSS"?"#f85149":"#8b949e";
      const badgeBg = res==="WIN"?"rgba(63,185,80,.15)":res==="LOSS"?"rgba(248,81,73,.15)":"rgba(139,148,158,.1)";
      const badgeCol= res==="WIN"?"#3fb950":res==="LOSS"?"#f85149":"#8b949e";
      const badgeTxt= res==="WIN"?"WIN ✓":res==="LOSS"?"LOSS ✗":"PUSH";
      const tc = TIER_COLOR_Y[p.tier]||"#8b949e";
      let conf = +p.conf||0; if(conf<=1) conf*=100;
      const _pt = p.pick_type||p.type||"";
      const typeLbl = _pt==="TOTAL"?"Total":_pt==="ML"?"Moneyline":_pt==="RL"?"Run Line":_pt;
      // away/home for logos + score abbreviations come from the "game" string ("Away @ Home")
      const _sp = (p.game||"").split(" @ ");
      const sc = (p.away_final!=null && p.home_final!=null)
        ? `Final: ${teamAbbr(_sp[0]||"")} ${p.away_final} – ${teamAbbr(_sp[1]||"")} ${p.home_final}` : "";
      // Market signal + sharp context
      const ms = (p.market_signal||"").toUpperCase();
      const msBadge = ms==="CONFIRM" ? `<span style="background:rgba(63,185,80,.12);color:#3fb950;font-size:.64rem;font-weight:700;padding:2px 7px;border-radius:4px">CONFIRM</span>`
                    : ms==="DIVERGE" ? `<span style="background:rgba(255,167,38,.12);color:#ffa726;font-size:.64rem;font-weight:700;padding:2px 7px;border-radius:4px">DIVERGE</span>`
                    : "";
      let sharpTxt = "";
      if(p.sharp_side){
        const sig = (p.ml_signal==="STEAM"||p.ml_signal==="DRIFT") ? p.ml_signal : "MOVE";
        const sn  = (p.sharp_side||"").split(" ").slice(-1)[0];
        const pn  = (p.team||"").split(" ").slice(-1)[0].toLowerCase();
        const agrees = sn.toLowerCase()===pn;
        sharpTxt = `<span style="font-size:.66rem;color:${agrees?"#3fb950":"#ffa726"}">💰 ${sig} → ${sn}${agrees?" ✓ agrees":" ⚡ vs pick"}</span>`;
      }
      // away/home for logos come from the "game" string ("Away @ Home")
      const parts = (p.game||"").split(" @ ");
      const aLogo = parts[0]?teamLogo(parts[0],14):"";
      const hLogo = parts[1]?teamLogo(parts[1],14):"";
      return `
      <div style="background:#161b22;border:1px solid #30363d;border-left:3px solid ${border};border-radius:10px;padding:11px 15px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
        <div style="flex:1;min-width:180px">
          <div style="font-size:.72rem;color:#8b949e;margin-bottom:3px">${aLogo}${hLogo}${p.game||""}</div>
          <div style="font-size:.9rem;font-weight:700"><span style="color:${tc}">${p.tier||""}</span> <span style="color:#8b949e;font-weight:500;font-size:.72rem">${typeLbl}</span> ${p.label||""}</div>
          <div style="font-size:.66rem;color:#8b949e;margin-top:3px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">${conf.toFixed(0)}% conf ${msBadge} ${sharpTxt}</div>
        </div>
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
          ${sc?`<div style="font-size:.75rem;color:#e6edf3;font-weight:600">${sc}</div>`:""}
          <span style="background:${badgeBg};color:${badgeCol};font-size:.76rem;font-weight:700;padding:3px 12px;border-radius:6px">${badgeTxt}</span>
        </div>
      </div>`;
    }).join("");
    const gwr = (gw+gl)>0 ? Math.round(gw/(gw+gl)*100) : 0;
    gradedCardsHtml = `
      <div style="display:flex;align-items:center;gap:12px;margin:4px 0 12px">
        <span style="font-size:1.2rem;font-weight:800;color:${gw>gl?"#3fb950":gw<gl?"#f85149":"#e6edf3"}">${gw}–${gl}${gpsh?` (${gpsh} push)`:""}</span>
        <span style="font-size:.8rem;color:#8b949e">${(gw+gl)>0?gwr+"% win rate":""} · every graded pick</span>
      </div>
      <div style="margin-bottom:22px">${cards}</div>`;
  }

  document.getElementById("yesterdayFull").innerHTML = `
    <div class="section-title">📈 Yesterday's Performance — ${d.date}</div>
    ${gradedCardsHtml}
    ${gp.length ? `<div class="section-title" style="font-size:.78rem">Breakdown</div>` : ""}
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px">
      <div>
        <div class="section-title" style="font-size:.78rem">By Tier</div>
        <table style="${tblStyle}">
          <thead><tr>
            <th style="${thStyle}">Tier</th><th style="${thStyle}">Record</th>
            <th style="${thStyle}">Win%</th><th style="${thStyle}">ROI</th><th style="${thStyle}">P/L</th>
          </tr></thead>
          <tbody style="">${tierRows.replace(/<td>/g, `<td style="${tdStyle}">`).replace(/<th>/g, `<th style="${thStyle}">`)}</tbody>
        </table>
      </div>
      <div>
        <div class="section-title" style="font-size:.78rem">By Type</div>
        <table style="${tblStyle}">
          <thead><tr>
            <th style="${thStyle}">Type</th><th style="${thStyle}">Record</th>
            <th style="${thStyle}">Win%</th><th style="${thStyle}">ROI</th><th style="${thStyle}">P/L</th>
          </tr></thead>
          <tbody>${typeRows.replace(/<td>/g, `<td style="${tdStyle}">`).replace(/<th>/g, `<th style="${thStyle}">`)}</tbody>
        </table>
      </div>
    </div>
    <div class="section-title" style="font-size:.78rem">Findings</div>
    <div style="margin-bottom:20px">${allFindings}</div>
    <div class="section-title" style="font-size:.78rem">Recommendations</div>
    ${allRecs}`;
}

// ── Render Thematic Parlays ───────────────────────────────────────────────────
const THESIS_ICONS = {
  "Pitching Mismatch": "⚾",
  "Home Dog Value":    "🐶",
  "Sharp Action":      "💰",
  "Bullpen Edge":      "💪",
  "Market Confirm":    "📊",
  "Hot Team":          "🔥",
};
function renderThematic(){
  const grid = document.getElementById("thematicGrid");
  if(!grid) return;
  grid.innerHTML = "";
  if(!ACTIVE_THEMATIC || !ACTIVE_THEMATIC.length){
    grid.innerHTML = `<div class="empty" style="padding:20px 0;color:var(--sub);font-size:.85rem">Not enough qualifying legs with a shared thesis today — check back after lineups confirm.</div>`;
    return;
  }
  ACTIVE_THEMATIC.forEach((par)=>{
    const icon = THESIS_ICONS[par.thesis] || "\u{1F3AF}";
    const legsHtml = par.legs.map(l=>`
      <div class="parlay-leg">
        <span class="leg-conf leg-${l.tier}">${l.conf}%</span>
        <div class="leg-info">
          <div class="leg-label">${l.label}</div>
          <div class="leg-game">${l.game}</div>
        </div>
      </div>`).join("");
    const o    = parlayOdds(par);
    const edge = (par.combined - o.be).toFixed(1);
    const oddsLbl = o.real ? "combined odds" : "est. payout";
    grid.innerHTML += `
      <div class="parlay-card thematic-parlay-card">
        <div class="parlay-header">
          <span class="thematic-thesis-badge">${icon} ${par.thesis}</span>
          <span class="parlay-payout">${o.oddsStr} · ${oddsLbl}</span>
        </div>
        <div class="thematic-desc">${par.thesis_desc}</div>
        <div class="parlay-conf" style="margin-top:10px">${par.combined}%<span>combined confidence</span></div>
        <div class="parlay-ev">+${edge}% edge vs break-even</div>
        <div class="parlay-breakeven">Break-even at ${o.oddsStr}: ${o.be}% — you're at ${par.combined}%</div>
        <div class="parlay-legs" style="margin-top:12px">${legsHtml}</div>
      </div>`;
  });
}

// ── Parlay odds helper: prefer REAL combined book odds, fall back to generic payout
function parlayOdds(par){
  const s = par.book_odds || par.payout || "+200";
  const n = parseInt(s, 10);
  const dec = s.trim()[0] === "-" ? 1 + 100/Math.abs(n) : 1 + n/100;
  return { oddsStr: s, dec: dec, be: +(100/dec).toFixed(1), real: !!par.book_odds };
}

// ── Render Parlays ────────────────────────────────────────────────────────────
function renderParlays(){
  const data = showParlay===2 ? ACTIVE_P2 : ACTIVE_P3;
  const grid = document.getElementById("parlayGrid");
  grid.innerHTML = "";
  if(!data.length){
    grid.innerHTML=`<div class="empty">Not enough qualified legs for ${showParlay}-leg parlays today.</div>`;
    return;
  }
  // Break-even thresholds by payout string
  const BREAKEVEN = {"+260": 27.8, "+595": 14.4, "+1228": 7.5, "+2435": 3.9};

  data.forEach((par,i)=>{
    const legsHtml = par.legs.map(l=>`
      <div class="parlay-leg">
        <span class="leg-conf leg-${l.tier}">${l.conf}%</span>
        <div class="leg-info">
          <div class="leg-label">${l.label}</div>
          <div class="leg-game">${l.game}</div>
        </div>
      </div>`).join("");

    // Edge vs break-even — makes clear why a "low" combined % is still a great bet
    const o     = parlayOdds(par);
    const edge  = (par.combined - o.be).toFixed(1);
    const oddsLbl = o.real ? "combined odds" : "est. payout";
    const evHtml = `<div class="parlay-ev">+${edge}% edge vs break-even</div>
      <div class="parlay-breakeven">Break-even at ${o.oddsStr}: ${o.be}% — you're at ${par.combined}%</div>`;

    grid.innerHTML += `
      <div class="parlay-card">
        <div class="parlay-header">
          <span class="parlay-tag">Parlay ${i+1} &bull; ${par.n_legs} Legs</span>
          <span class="parlay-payout">${o.oddsStr} · ${oddsLbl}</span>
        </div>
        <div class="parlay-conf">${par.combined}%<span>combined confidence</span></div>
        ${evHtml}
        <div class="parlay-legs" style="margin-top:12px">${legsHtml}</div>
      </div>`;
  });
}

// ── Render Games ─────────────────────────────────────────────────────────────
function parkClass(tag){
  if(tag.includes("Hitter")) return "park-hitter";
  if(tag.includes("Pitcher")) return "park-pitcher";
  return "park-neutral";
}

function weatherDisplay(g){
  if(g.has_roof) return {text:"Retractable Roof / Dome", color:"var(--sub)", icon:"🏟️"};
  const flag = g.weather_flag;
  const temp = g.temp_f + "°F";
  const precip = g.precip_prob > 10 ? ` | ${g.precip_prob}% precip` : "";
  if(flag==="WIND_OUT"){
    const mph = Math.abs(g.wind_component).toFixed(1);
    return {text:`${temp} | Wind ${mph} mph blowing OUT (+${(g.wind_component*0.04*0.5).toFixed(2)} runs)${precip}`, color:"#ef9a9a", icon:"💨"};
  }
  if(flag==="WIND_IN"){
    const mph = Math.abs(g.wind_component).toFixed(1);
    return {text:`${temp} | Wind ${mph} mph blowing IN (suppresses scoring)${precip}`, color:"#90caf9", icon:"🌬️"};
  }
  if(flag==="COLD"){
    return {text:`${temp} — Cold conditions suppress scoring${precip}`, color:"#90caf9", icon:"🥶"};
  }
  if(flag==="PRECIP"){
    return {text:`${temp} | ${g.precip_prob}% chance of rain`, color:"#ffcc80", icon:"🌧️"};
  }
  // NORMAL
  if(g.wind_speed > 5){
    const dir = g.wind_label || "";
    return {text:`${temp} | Wind ${g.wind_speed} mph ${dir}`, color:"var(--sub)", icon:"⛅"};
  }
  return {text:`${temp} | Clear conditions`, color:"var(--sub)", icon:"☀️"};
}

function renderGames(){
  const grid = document.getElementById("gamesGrid");
  grid.innerHTML = "";
  const _gameBuf = [];
  DATA_GAMES.forEach(g=>{
    const homeW    = g.home_wp, awayW = g.away_wp;
    const totalDir = g.exp_total > g.total_line ? "↑ OVER" : "↓ UNDER";
    const totalCol = g.exp_total > g.total_line ? "color:var(--red)" : "color:var(--blue)";
    const wx       = weatherDisplay(g);

    // Lineup OPS display
    const lineupRow = g.lineup_confirmed
      ? `<div class="game-row"><span class="row-label">Lineup OPS</span>
           <span class="row-val" style="color:var(--green)">
             ${g.away} <b>${g.away_lineup_ops||"—"}</b> &nbsp;|&nbsp;
             ${g.home} <b>${g.home_lineup_ops||"—"}</b>
             <span style="color:var(--sub);font-size:.68rem"> (confirmed)</span>
           </span></div>`
      : "";

    _gameBuf.push(`
      <div class="game-card">
        <div class="game-header">
          <span class="matchup">${teamLogo(g.away,18)}${g.away} @ ${teamLogo(g.home,18)}${g.home}</span>
          <span class="game-time">${g.time}</span>
        </div>
        <div class="lineup-status">
          ${(()=>{ const st=_statusFor(g.away, g.home); return st ? st.badge+" " : ""; })()}
          ${g.lineup_confirmed
            ? `<span class="ls-badge ls-yes">✓ Lineups confirmed</span>`
            : `<span class="ls-badge ls-no">⏳ Lineups not set — refresh closer to first pitch</span>`}
        </div>
        <div class="game-body">
          <div class="game-row">
            <span class="row-label">Park</span>
            <span class="row-val">${g.venue.length>30?g.venue.slice(0,28)+"…":g.venue}
              <span class="park-badge ${parkClass(g.park_tag)}">${g.park_tag} (${g.park_runs})</span>
            </span>
          </div>
          <div class="game-row">
            <span class="row-label">Weather</span>
            <span class="row-val" style="color:${wx.color}">${wx.icon} ${wx.text}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Away SP</span>
            <span class="row-val">${g.away_sp} &bull; ERA ${g.away_era} / FIP ${g.away_fip}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Home SP</span>
            <span class="row-val">${g.home_sp} &bull; ERA ${g.home_era} / FIP ${g.home_fip}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Bullpen</span>
            <span class="row-val">${g.away} BP ERA ${g.away_bp_era} &nbsp;|&nbsp; ${g.home} BP ERA ${g.home_bp_era}</span>
          </div>
          <div class="game-row">
            <span class="row-label">Offense</span>
            <span class="row-val">${g.away} ${g.away_rpg} RPG &nbsp;|&nbsp; ${g.home} ${g.home_rpg} RPG</span>
          </div>
          ${lineupRow}
          <div class="game-row">
            <span class="row-label">Projected</span>
            <span class="row-val">${g.away} <b>${g.exp_away}</b> &nbsp;|&nbsp; ${g.home} <b>${g.exp_home}</b></span>
          </div>
          <div class="game-row">
            <span class="row-label">Total</span>
            <span class="row-val">
              <b style="${totalCol}">${totalDir}</b> &nbsp;
              Model ${g.exp_total} vs ${g.total_line} line &nbsp;
              <span style="color:var(--sub)">(${g.total_conf}% conf)</span>
            </span>
          </div>
          ${g.rl_team ? `<div class="game-row"><span class="row-label">Run Line</span>
            <span class="row-val">${g.rl_pick} &nbsp;<span style="color:var(--sub)">(${g.rl_conf}% conf)</span></span></div>` : ""}
          <div style="margin-top:6px">
            <div style="display:flex;justify-content:space-between;font-size:.76rem;color:var(--sub);margin-bottom:4px">
              <span>${g.away} ${g.away_wp}%</span><span>${g.home} ${g.home_wp}%</span>
            </div>
            <div style="display:flex;height:8px;border-radius:4px;overflow:hidden;background:rgba(255,255,255,.06)">
              <div style="width:${g.away_wp}%;background:var(--blue)"></div>
              <div style="width:${g.home_wp}%;background:var(--green)"></div>
            </div>
            <div style="display:flex;justify-content:space-between;font-size:.68rem;color:var(--sub);margin-top:3px">
              <span>Away</span><span>Home</span>
            </div>
          </div>
        </div>
      </div>`);
  });
  grid.innerHTML = _gameBuf.join('');   // one DOM write, not N
}

// ── Section Nav ───────────────────────────────────────────────────────────────
document.querySelectorAll(".section-nav-btn").forEach(btn=>{
  btn.addEventListener("click",()=>{
    document.querySelectorAll(".section-nav-btn").forEach(b=>b.classList.remove("active"));
    document.querySelectorAll(".section-panel").forEach(p=>p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(btn.dataset.panel).classList.add("active");
  });
});

// ── Render Today's Games ──────────────────────────────────────────────────────
function localGameTime(utcStr){
  if(!utcStr) return "TBD";
  try{
    const d = new Date(utcStr.endsWith("Z") ? utcStr : utcStr + "Z");
    return d.toLocaleTimeString("en-US", {
      hour:"numeric", minute:"2-digit",
      timeZone:"America/New_York", timeZoneName:"short"
    });
  } catch(e){ return utcStr; }
}

// ── TEAMS TAB ─────────────────────────────────────────────────────────────────
// Alphabetical — Athletics listed by nickname since they moved to Sacramento
const ALL_TEAMS = [
  "Arizona Diamondbacks","Athletics","Atlanta Braves","Baltimore Orioles",
  "Boston Red Sox","Chicago Cubs","Chicago White Sox","Cincinnati Reds",
  "Cleveland Guardians","Colorado Rockies","Detroit Tigers","Houston Astros",
  "Kansas City Royals","Los Angeles Angels","Los Angeles Dodgers","Miami Marlins",
  "Milwaukee Brewers","Minnesota Twins","New York Mets","New York Yankees",
  "Philadelphia Phillies","Pittsburgh Pirates","San Diego Padres","San Francisco Giants",
  "Seattle Mariners","St. Louis Cardinals","Tampa Bay Rays","Texas Rangers",
  "Toronto Blue Jays","Washington Nationals"
];

// Unambiguous short labels — no duplicate "Sox", "Angels", etc.
const TEAM_SHORT = {
  "Arizona Diamondbacks":  "ARI D-backs",
  "Athletics":             "ATH Athletics",
  "Atlanta Braves":        "ATL Braves",
  "Baltimore Orioles":     "BAL Orioles",
  "Boston Red Sox":        "BOS Red Sox",
  "Chicago Cubs":          "CHC Cubs",
  "Chicago White Sox":     "CWS White Sox",
  "Cincinnati Reds":       "CIN Reds",
  "Cleveland Guardians":   "CLE Guards",
  "Colorado Rockies":      "COL Rockies",
  "Detroit Tigers":        "DET Tigers",
  "Houston Astros":        "HOU Astros",
  "Kansas City Royals":    "KC Royals",
  "Los Angeles Angels":    "LAA Angels",
  "Los Angeles Dodgers":   "LAD Dodgers",
  "Miami Marlins":         "MIA Marlins",
  "Milwaukee Brewers":     "MIL Brewers",
  "Minnesota Twins":       "MIN Twins",
  "New York Mets":         "NYM Mets",
  "New York Yankees":      "NYY Yankees",
  "Philadelphia Phillies": "PHI Phillies",
  "Pittsburgh Pirates":    "PIT Pirates",
  "San Diego Padres":      "SD Padres",
  "San Francisco Giants":  "SF Giants",
  "Seattle Mariners":      "SEA Mariners",
  "St. Louis Cardinals":   "STL Cards",
  "Tampa Bay Rays":        "TB Rays",
  "Texas Rangers":         "TEX Rangers",
  "Toronto Blue Jays":     "TOR Blue Jays",
  "Washington Nationals":  "WSH Nats",
};

let activeTeam = null;
let teamPropTypeFilter = "all";

function initTeamsTab(){
  const grid = document.getElementById("teamButtonGrid");
  ALL_TEAMS.forEach(team => {
    const hasGame = !!DATA_TEAM_SCHED[team];
    const isToday = hasGame && DATA_TEAM_SCHED[team].is_today;
    const btn = document.createElement("button");
    btn.className = "team-btn" + (isToday ? " playing" : "");
    btn.textContent = teamShort(team);
    btn.title = team + (hasGame ? " — " + DATA_TEAM_SCHED[team].day_label : " — No upcoming game found");
    btn.addEventListener("click", () => {
      document.querySelectorAll(".team-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      activeTeam = team;
      teamPropTypeFilter = "all";
      renderTeamView(team);
    });
    grid.appendChild(btn);
  });
}

function teamShort(full){
  return TEAM_SHORT[full] || full.split(" ").pop();
}

function renderTeamView(team){
  const view = document.getElementById("teamGameView");
  const game = DATA_TEAM_SCHED[team];

  if(!game){
    view.innerHTML = `<div class="team-game-card" style="text-align:center;color:var(--sub);padding:40px">
      No upcoming game found for ${team} in the next 7 days.</div>`;
    return;
  }

  const away = game.away_team;
  const home = game.home_team;
  const isAway = away === team;
  const opp   = isAway ? home : away;

  // Find pick for this game
  const gamePicks = ACTIVE_PICKS.filter(p =>
    (p.away === away && p.home === home) ||
    (p.game && p.game.includes(away) && p.game.includes(home))
  );
  const mlPick    = gamePicks.find(p => p.type === "ML");
  const totalPick = gamePicks.find(p => p.type === "TOTAL");

  // Look up raw win probabilities from DATA_GAMES (available for ALL games)
  const gameData = DATA_GAMES.find(g => g.away === away && g.home === home);

  // Prediction bars — use DATA_GAMES wp for all games, overlay pick tier if one exists
  let awayConf = gameData ? gameData.away_wp : 50;
  let homeConf = gameData ? gameData.home_wp : 50;
  let awayPick = "", homePick = "";
  if(mlPick){
    const pickedHome = mlPick.label && mlPick.label.includes(home.split(" ").pop());
    if(pickedHome){
      homePick = `✓ Model Pick (${mlPick.tier})`;
    } else {
      awayPick = `✓ Model Pick (${mlPick.tier})`;
    }
  }

  const favColor = c => c >= 68 ? "var(--red)" : c >= 58 ? "#ffb74d" : "var(--green)";
  const isToday = game.is_today;

  // Props for this game — both teams
  const gameProps = DATA_PROPS.filter(p =>
    (p.away_team === away && p.home_team === home) ||
    (p.game && p.game.includes(away) && p.game.includes(home))
  );
  // Filter by side field — each prop knows which lineup it came from
  const awayProps = gameProps.filter(p => p.side === "away");
  const homeProps = gameProps.filter(p => p.side === "home");
  // If side data missing fall back to show all under clicked team only
  const clickedIsAway = team === away;
  const primaryProps  = clickedIsAway
    ? (awayProps.length ? awayProps : gameProps)
    : (homeProps.length ? homeProps : gameProps);
  const otherProps    = clickedIsAway
    ? (homeProps.length ? homeProps : [])
    : (awayProps.length ? awayProps : []);
  const primaryLabel  = clickedIsAway ? away : home;
  const otherLabel    = clickedIsAway ? home : away;

  const propTypes = [...new Set(gameProps.map(p => p.prop_type))].sort();
  const typeFilterHtml = propTypes.length > 1 ? `
    <div class="prop-type-filters" id="teamPropFilters">
      <button class="ptype-btn active" data-ptype="all">All</button>
      ${propTypes.map(t => `<button class="ptype-btn" data-ptype="${t}">${propIcon(t)} ${t}</button>`).join("")}
    </div>` : "";

  function miniProps(props){
    const filtered = teamPropTypeFilter === "all" ? props : props.filter(p => p.prop_type === teamPropTypeFilter);
    if(!filtered.length) return `<div class="team-no-props">No props available yet</div>`;
    return filtered.map(p => {
      const barPct = p.prop_type === "HR" ? Math.min(100, p.conf * 5)
        : p.prop_type === "SB" ? Math.min(100, p.conf * 3.57)
        : p.prop_type === "RBI" || p.prop_type === "R" ? Math.min(100, Math.max(0,(p.conf-30)*1.43))
        : p.prop_type === "TB" ? Math.min(100, Math.max(0,(p.conf-40)*1.67))
        : Math.min(100, Math.max(0,(p.conf-50)*2));
      const tc = p.tier==="LOCK"?"var(--green)":p.tier==="STRONG"?"var(--blue)":"#ffb74d";
      const projTag = p.projected ? `<span style="font-size:.6rem;background:rgba(255,183,77,.18);
        color:#ffb74d;border:1px solid rgba(255,183,77,.35);border-radius:3px;padding:1px 4px;
        margin-left:4px;letter-spacing:.03em">PROJ</span>` : "";
      return `<div class="team-prop-mini" style="${p.projected ? 'border:1px dashed rgba(255,183,77,.25)' : ''}">
        <div class="team-prop-mini-top">
          <span class="team-prop-mini-name">${p.player_name}${projTag}</span>
          <span class="team-prop-mini-type badge-${p.prop_type}">${propIcon(p.prop_type)} ${p.prop_type}</span>
        </div>
        <div class="team-prop-mini-line">${propLabel(p.prop_type, p.line)}</div>
        <div style="display:flex;align-items:center;gap:8px">
          <div style="flex:1;height:5px;background:rgba(255,255,255,.08);border-radius:3px">
            <div style="width:${barPct}%;height:5px;border-radius:3px;background:${tc}"></div>
          </div>
          <span class="team-prop-mini-conf" style="color:${tc}">${p.conf}%</span>
          <span style="font-size:.68rem;color:var(--sub)">${p.tier}</span>
        </div>
      </div>`;
    }).join("");
  }

  // Projected props: gameProps exist but are all tagged projected
  const allProjected = gameProps.length > 0 && gameProps.every(p => p.projected);

  const noLineupMsg = !isToday ? `
    <div style="background:rgba(66,165,245,.07);border:1px solid rgba(66,165,245,.2);border-radius:8px;
      padding:12px 16px;font-size:.82rem;color:#90caf9;margin-bottom:14px">
      📅 Next game: ${game.day_label} — full prop analysis runs on game day once lineups confirm.
    </div>` :
    allProjected ? `
    <div style="background:rgba(255,183,77,.07);border:1px solid rgba(255,183,77,.25);border-radius:8px;
      padding:12px 16px;margin-bottom:14px;display:flex;align-items:flex-start;gap:10px">
      <div style="font-size:1.1rem;flex-shrink:0">📋</div>
      <div>
        <div style="font-size:.85rem;font-weight:700;color:#ffb74d;margin-bottom:4px">
          Projected Props — Official Lineup Pending
        </div>
        <div style="font-size:.78rem;color:var(--sub);line-height:1.6">
          These props are based on the most recent confirmed batting order. If today's
          lineup matches yesterday's, the numbers stay the same. Cards marked
          <span style="background:rgba(255,183,77,.18);color:#ffb74d;border:1px solid rgba(255,183,77,.35);
            border-radius:3px;padding:1px 5px;font-size:.68rem">PROJ</span>
          will automatically update when the official order posts.
        </div>
      </div>
    </div>` :
    (gameProps.length === 0 ? (() => {
      // Try to show projected lineup from most recent confirmed order
      const projAway = DATA_PROJ_LINEUPS[away];
      const projHome = DATA_PROJ_LINEUPS[home];
      const proj     = projAway || projHome;
      const projTeam = projAway ? away : home;

      const projSection = proj ? `
        <div style="margin-top:14px">
          <div style="font-size:.78rem;font-weight:700;color:#90caf9;margin-bottom:8px;letter-spacing:.04em;text-transform:uppercase">
            📋 Projected Lineup — ${projTeam}
            <span style="font-weight:400;color:var(--sub);margin-left:6px;text-transform:none">Based on ${proj.date} · Updates when official order posts</span>
          </div>
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:6px">
            ${proj.players.map(p => `
              <div style="background:rgba(66,165,245,.07);border:1px solid rgba(66,165,245,.15);border-radius:6px;
                padding:6px 10px;font-size:.78rem;display:flex;align-items:center;gap:8px">
                <span style="color:var(--sub);font-size:.7rem;min-width:14px;text-align:right">${p.order}</span>
                <span style="color:var(--text)">${p.name}</span>
                ${p.pos ? `<span style="color:var(--sub);font-size:.7rem;margin-left:auto">${p.pos}</span>` : ""}
              </div>`).join("")}
          </div>
        </div>` : "";

      return `
      <div style="background:rgba(255,152,0,.08);border:1px solid rgba(255,152,0,.2);border-radius:8px;
        padding:12px 16px;margin-bottom:14px">
        <div style="font-size:.85rem;font-weight:700;color:#ffb74d;margin-bottom:6px">⏳ Batting Order Not Yet Posted</div>
        <div style="font-size:.78rem;color:var(--sub);line-height:1.6">
          Probable starters are shown above. Teams typically post their batting order
          2–3 hours before first pitch. Player props will populate automatically once
          the lineup is confirmed — no refresh needed.
        </div>
        ${projSection}
      </div>`;
    })() : "");

  view.innerHTML = `
    <div class="team-game-card">
      <!-- Matchup header -->
      <div class="team-matchup-header">
        <div class="team-matchup-teams">
          <div class="team-name-block">
            <div class="team-name-big">${away}</div>
            <div class="team-name-sp">SP: ${game.away_sp}</div>
          </div>
          <div class="team-vs">@</div>
          <div class="team-name-block">
            <div class="team-name-big">${home}</div>
            <div class="team-name-sp">SP: ${game.home_sp}</div>
          </div>
        </div>
        <div class="team-game-meta">
          <span class="team-day-label">${game.day_label}</span>
          ${game.time_str}<br>
          ${game.venue || ""}
        </div>
      </div>

      <!-- Prediction bars (today's games only — always shown when DATA_GAMES has the game) -->
      ${isToday && gameData ? `
      <div class="team-pred-row">
        <div class="team-pred-box">
          <div class="team-pred-label">${away}</div>
          <div class="team-pred-conf" style="color:${favColor(awayConf)}">${awayConf}%</div>
          <div class="team-pred-bar"><div class="team-pred-fill" style="width:${awayConf}%;background:${favColor(awayConf)}"></div></div>
          <div class="team-pred-pick" style="color:${favColor(awayConf)}">${awayPick}</div>
        </div>
        <div class="team-pred-box">
          <div class="team-pred-label">${home}</div>
          <div class="team-pred-conf" style="color:${favColor(homeConf)}">${homeConf}%</div>
          <div class="team-pred-bar"><div class="team-pred-fill" style="width:${homeConf}%;background:${favColor(homeConf)}"></div></div>
          <div class="team-pred-pick" style="color:${favColor(homeConf)}">${homePick}</div>
        </div>
      </div>
      ${totalPick ? `<div style="text-align:center;font-size:.82rem;color:var(--sub);margin:-4px 0 14px">
        📊 Total: ${totalPick.label} — proj ${totalPick.exp_total || "?"} runs
        <span class="tier-badge tb-${totalPick.tier}" style="margin-left:8px">${totalPick.tier}</span>
      </div>` : ""}` :
      (isToday ? `<div style="color:var(--sub);font-size:.85rem;padding:10px 0 14px;text-align:center">
        Prediction not available for this game.</div>` :
      `<div style="background:rgba(0,230,118,.05);border:1px solid rgba(0,230,118,.15);border-radius:8px;
        padding:12px 16px;font-size:.82rem;color:var(--sub);margin-bottom:14px">
        📅 Full model prediction runs on game day.</div>`)}

      <!-- Props -->
      <div class="team-props-section">
        ${noLineupMsg}
        ${typeFilterHtml}
        <div id="teamPropsContent">
          <div class="team-props-header">${primaryLabel} — Player Props</div>
          <div class="team-props-grid" id="awayPropsGrid">${miniProps(primaryProps)}</div>
          ${otherProps.length ? `
          <div class="team-props-header" style="margin-top:16px">${otherLabel} — Player Props</div>
          <div class="team-props-grid" id="homePropsGrid">${miniProps(otherProps)}</div>` : ""}
        </div>
      </div>
    </div>`;

  // Prop type filter buttons
  document.querySelectorAll("#teamPropFilters .ptype-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("#teamPropFilters .ptype-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      teamPropTypeFilter = btn.dataset.ptype;
      document.getElementById("awayPropsGrid").innerHTML = miniProps(primaryProps);
      const hpg = document.getElementById("homePropsGrid");
      if(hpg) hpg.innerHTML = miniProps(otherProps);
    });
  });
}

// Auto-select a team playing today when tab is opened
document.querySelectorAll(".section-nav-btn[data-panel='panel-teams']").forEach(btn => {
  btn.addEventListener("click", () => {
    if(!activeTeam){
      // Pick first team with a game today
      const todayTeam = ALL_TEAMS.find(t => DATA_TEAM_SCHED[t] && DATA_TEAM_SCHED[t].is_today);
      if(todayTeam){
        activeTeam = todayTeam;
        const teamBtns = document.querySelectorAll(".team-btn");
        teamBtns.forEach((b,i) => { if(ALL_TEAMS[i] === todayTeam) b.classList.add("active"); });
        renderTeamView(todayTeam);
      }
    }
  });
});

function renderSchedule(){
  const grid = document.getElementById("scheduleGrid");
  grid.innerHTML = "";
  if(!ACTIVE_SCHEDULE || ACTIVE_SCHEDULE.length === 0){
    grid.innerHTML = `<div class="empty">No games found for today.</div>`;
    return;
  }
  const _schedBuf = [];
  ACTIVE_SCHEDULE.forEach(g=>{
    const isLive    = g.status.startsWith("Live");
    const isFinal   = g.status === "Final";
    const isUpcoming= g.status === "Upcoming";

    const statusClass = isLive ? "status-live" : isFinal ? "status-final" : "status-upcoming";
    const statusLabel = isLive
      ? `<span class="live-dot"></span> LIVE &bull; ${g.status.replace("Live — ","")}`
      : isFinal ? "⚾ Final" : `⏰ ${localGameTime(g.game_time_utc)}`;

    // Score display
    let awayScoreHtml = "", homeScoreHtml = "", vsHtml = "";
    if(isFinal || isLive){
      const awayW = parseInt(g.away_score) > parseInt(g.home_score);
      const homeW = parseInt(g.home_score) > parseInt(g.away_score);
      awayScoreHtml = `<div class="sched-score${awayW?' winner':''}">${g.away_score}</div>`;
      homeScoreHtml = `<div class="sched-score${homeW?' winner':''}">${g.home_score}</div>`;
      vsHtml        = `<div class="sched-vs">-</div>`;
    } else {
      vsHtml = `<div><div class="sched-vs">@</div><div class="sched-time">${localGameTime(g.game_time_utc)}</div></div>`;
    }

    const awayStreak = g.away_streak ? ` &bull; ${g.away_streak}` : "";
    const homeStreak = g.home_streak ? ` &bull; ${g.home_streak}` : "";
    const awayL10    = g.away_last10 ? ` (L10: ${g.away_last10})` : "";
    const homeL10    = g.home_last10 ? ` (L10: ${g.home_last10})` : "";

    const venueTrunc = g.venue.length > 28 ? g.venue.slice(0,26)+"…" : g.venue;

    _schedBuf.push(`
      <div class="sched-card">
        <div class="sched-status-bar ${statusClass}">${statusLabel}</div>
        <div class="sched-matchup">
          <div class="sched-team-row">
            <div class="sched-team-info">
              <div class="sched-team-name">${g.away_team}</div>
              <div class="sched-team-record">${g.away_record}${awayStreak}</div>
              ${awayL10 ? `<div class="sched-team-streak">${awayL10}</div>` : ""}
              <div class="sched-at-label">Away</div>
            </div>
            <div class="sched-score-block">
              ${awayScoreHtml}
              ${vsHtml}
              ${homeScoreHtml}
            </div>
            <div class="sched-team-info" style="text-align:right;align-items:flex-end">
              <div class="sched-team-name">${g.home_team}</div>
              <div class="sched-team-record">${g.home_record}${homeStreak}</div>
              ${homeL10 ? `<div class="sched-team-streak">${homeL10}</div>` : ""}
              <div class="sched-at-label">Home</div>
            </div>
          </div>
        </div>
        <div class="sched-divider"></div>
        <div class="sched-pitchers">
          <span>SP: ${g.away_sp||"TBD"}</span>
          <span>SP: ${g.home_sp||"TBD"}</span>
        </div>
        <div class="sched-divider"></div>
        <div class="sched-footer">
          <span class="sched-time">${isFinal||isLive ? g.status : localGameTime(g.game_time_utc)}</span>
          <span class="sched-venue">${venueTrunc}</span>
        </div>
      </div>`);
  });
  grid.innerHTML = _schedBuf.join('');   // one DOM write, not N
}

// ── Render Props ──────────────────────────────────────────────────────────────
let propFilterType = "all", propFilterTier = "all";

function propIcon(t){
  const icons = {HR:"⚡",HITS:"🎯",TB:"💥",RBI:"🏅",R:"🏃",SB:"💨",K:"🔥"};
  return icons[t] || "📊";
}
function propLabel(t,line,side){
  const d = (side==="UNDER") ? "Under" : "Over";
  if(t==="HR")   return `HR ${d} ${line}`;
  if(t==="HITS") return `Hits ${d} ${line}`;
  if(t==="TB")   return `Total Bases ${d} ${line}`;
  if(t==="RBI")  return `RBIs ${d} ${line}`;
  if(t==="R")    return `Runs Scored ${d} ${line}`;
  if(t==="SB")   return `Stolen Bases ${d} ${line}`;
  if(t==="K")    return `Ks ${d} ${line}`;
  return `${d} ${line}`;
}

function renderProps(){
  const grid = document.getElementById("propsGrid");
  grid.innerHTML = "";
  let visible = 0;
  const hasProjected = DATA_PROPS.some(p => p.projected);
  const banner = document.getElementById("propsBanner");
  if(banner) {
    banner.style.display = hasProjected ? "block" : "none";
    const timeEl = document.getElementById("propsRefreshTime");
    if(timeEl && window._schedRefreshTime) timeEl.textContent = window._schedRefreshTime;
  }
  // Bettable props first, research-only projections after.
  const _propBuf = [];
  [...DATA_PROPS].sort((a,b)=>(a.projection_only?1:0)-(b.projection_only?1:0)).forEach(p=>{
    const show = (propFilterType==="all" || p.prop_type===propFilterType)
              && (propFilterTier==="all" || p.tier===propFilterTier);
    if(!show) return;
    visible++;
    const label    = propLabel(p.prop_type, p.line);
    const overUnder= p.proj >= p.line ? "OVER" : "UNDER";
    const projColor= overUnder==="OVER" ? "var(--green)" : "var(--blue)";
    const orderStr = p.batting_order ? ` &bull; Bats ${p.batting_order}` : "";
    const sideStr  = p.side==="pitcher" ? " (SP)" : (p.side ? ` (${p.side})` : "");

    // Edge tier badge (always shown)
    const edgeLabel = p.fav_tier==="HEAVY"  ? "🔴 Strong Edge"
                    : p.fav_tier==="MEDIUM" ? "🟡 Solid Edge"
                    : "🟢 Slight Edge";
    const propFavHtml = `<div class="fav-row">
      <span class="fav-label">Edge:</span>
      <span class="fav-badge fav-${p.fav_tier||'NEUTRAL'}">${edgeLabel}</span>
    </div>`;

    // Warning badges — always shown, lit when active
    const pwb = (cls, active, label) =>
      `<span class="warn-badge ${cls} ${active ? 'active' : 'inactive'}">${label}</span>`;
    const propWarnHtml = `<div class="warn-row">
      ${pwb('warn-tbd',    p.tbd_sp,      '⚠ TBD SP')}
      ${pwb('warn-lineup', p.unconfirmed, 'Lineups TBD')}
    </div>`;

    // Confidence bar — each prop type scaled to its realistic ceiling
    let propBarPct;
    if(p.prop_type === "HR"){
      propBarPct = Math.min(100, p.conf * 5);      // 20% max → 100%
    } else if(p.prop_type === "SB"){
      propBarPct = Math.min(100, p.conf * 3.57);   // 28% max → 100%
    } else if(p.prop_type === "RBI" || p.prop_type === "R"){
      propBarPct = Math.min(100, Math.max(0, (p.conf - 30) * 1.43)); // 30-100% range
    } else if(p.prop_type === "TB"){
      propBarPct = Math.min(100, Math.max(0, (p.conf - 40) * 1.67)); // 40-100% range
    } else {
      propBarPct = Math.min(100, Math.max(0, (p.conf - 50) * 2));    // HITS/K: 50-100%
    }

    const projBanner = p.projected ? `<div style="background:rgba(255,183,77,.1);border-bottom:1px solid rgba(255,183,77,.2);
      padding:4px 10px;font-size:.68rem;color:#ffb74d;letter-spacing:.04em">
      📋 PROJECTED — based on last confirmed lineup</div>` : "";
    _propBuf.push(`
      <div class="prop-card tier-${p.tier}" style="${p.projected ? 'border:1px dashed rgba(255,183,77,.3)' : ''}">
        ${projBanner}
        <div class="prop-top">
          <span class="prop-type-badge badge-${p.prop_type}">${propIcon(p.prop_type)} ${p.prop_type}</span>
          ${p.projection_only ? `<span class="proj-only-badge">📋 PROJECTION</span>` : ""}
          <span class="tier-badge tb-${p.tier}">${tierIcon(p.tier)} ${p.tier}</span>
        </div>
        ${p.projection_only ? `<div class="proj-note">Research projection — graded against the model's own 0.5 line, not a sportsbook number, so it's not a play. Shown so you can still look at it.</div>` : ""}
        <div class="prop-name-row">
          ${p.player_id ? `<a href="/player/${p.player_id}" title="View ${p.player_name}'s trends"><img class="prop-face" alt="" loading="lazy" src="https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/${p.player_id}/headshot/67/current" onerror="this.style.display='none'"></a>` : ""}
          <div class="prop-player">${p.player_id ? `<a href="/player/${p.player_id}" class="prop-link">${p.player_name}</a>` : p.player_name}${sideStr}</div>
        </div>
        <div class="prop-game">${p.game}${orderStr}</div>
        <div class="prop-line-row">
          <div>
            <div class="prop-line-label">Line</div>
            <div class="prop-line-val">${label}</div>
          </div>
          <div style="text-align:right">
            <div class="prop-line-label">Projection</div>
            <div class="prop-proj-val" style="color:${projColor}">${p.proj} (${overUnder})</div>
          </div>
        </div>
        <div class="conf-row">
          <div class="conf-bar-wrap">
            <div class="conf-bar bar-${p.tier}" style="width:${propBarPct}%"></div>
          </div>
          <span class="conf-pct pct-${p.tier}">${p.conf}%</span>
        </div>
        ${propFavHtml}
        ${propWarnHtml}
        ${(p.over_price!=null||p.under_price!=null) ? `
        <div class="prop-book-row">
          <span class="prop-line-label">SPORTSBOOK (Pinnacle)</span>
          <span class="prop-book-vals">
            <span class="prop-book-chip ${p.pick_side==='OVER'?'chip-pick':''}">O ${p.line} ${fmtOdds(p.over_price)}</span>
            <span class="prop-book-chip ${p.pick_side==='UNDER'?'chip-pick':''}">U ${p.line} ${fmtOdds(p.under_price)}</span>
          </span>
        </div>` : ""}
        <div class="prop-reasoning">${p.reasoning}</div>
      </div>`);
  });
  // ONE DOM write. `innerHTML +=` in a loop re-serialises and re-parses the
  // ENTIRE grid on every iteration, so rendering N props costs O(N^2). With
  // ~259 props on a full slate that froze the main thread for 10-20s, which
  // is what made the page feel like it was hanging on load. (2026-08-11)
  grid.innerHTML = _propBuf.join('');
  document.getElementById("propResults").innerHTML =
    `Showing <b>${visible}</b> of <b>${DATA_PROPS.length}</b> props`;
  if(!visible){
    const msg = DATA_PROPS.length===0
      ? "Player props require confirmed lineups — check back closer to game time."
      : "No props match the current filters.";
    grid.innerHTML = `<div class="empty">${msg}</div>`;
  }
}

// ── Filter Handlers ───────────────────────────────────────────────────────────
document.querySelectorAll(".filter-btn[data-pgroup]").forEach(btn=>{
  btn.addEventListener("click",()=>{
    const group = btn.dataset.pgroup, val = btn.dataset.pval;
    document.querySelectorAll(`.filter-btn[data-pgroup="${group}"]`).forEach(b=>{
      b.classList.remove("active","active-gold","active-blue");
    });
    if(val==="LOCK") btn.classList.add("active-gold");
    else if(val==="STRONG") btn.classList.add("active-blue");
    else btn.classList.add("active");
    if(group==="ptype") propFilterType = val;
    else                propFilterTier = val;
    renderProps();
  });
});

document.querySelectorAll(".filter-btn[data-group]").forEach(btn=>{
  btn.addEventListener("click",()=>{
    const group = btn.dataset.group, val = btn.dataset.val;
    document.querySelectorAll(`.filter-btn[data-group="${group}"]`).forEach(b=>{
      b.classList.remove("active","active-gold","active-blue");
    });
    if(val==="HIGHCONF" || val==="LOCK") btn.classList.add("active-gold");
    else if(val==="PROFIT") btn.classList.add("active");
    else if(val==="STRONG") btn.classList.add("active-blue");
    else btn.classList.add("active");
    if(group==="type") filterType = val;
    else               filterTier = val;
    renderPicks();
  });
});

document.getElementById("teamSearch").addEventListener("input", e=>{
  filterTeam = e.target.value.trim().toLowerCase();
  renderPicks();
});

document.querySelectorAll(".section-tab[data-parlay]").forEach(tab=>{
  tab.addEventListener("click",()=>{
    document.querySelectorAll(".section-tab").forEach(t=>t.classList.remove("active"));
    tab.classList.add("active");
    showParlay = parseInt(tab.dataset.parlay);
    renderParlays();
  });
});

// ── Refresh Button (only shown when served locally) ───────────────────────────
const LOCAL_API = "http://localhost:8765";
const isLocal   = location.hostname === "localhost" || location.hostname === "127.0.0.1";

if(isLocal){
  document.getElementById("refreshBtn").style.display = "inline-flex";
}

async function doRefresh(){
  const btn    = document.getElementById("refreshBtn");
  const icon   = document.getElementById("refreshIcon");
  const status = document.getElementById("refreshStatus");

  btn.disabled = true;
  icon.classList.add("spin");
  status.style.color = "var(--sub)";
  status.textContent = "Running pipeline… this takes about 30 seconds";

  try{
    const res = await fetch(LOCAL_API + "/refresh", {method:"GET"});
    if(res.ok){
      status.style.color = "var(--green)";
      status.textContent = "✓ Done! Reloading picks...";
      setTimeout(()=> location.reload(), 1200);
    } else {
      const msg = await res.text();
      status.style.color = "var(--red)";
      status.textContent = "Error: " + msg.slice(0,120);
      btn.disabled = false;
      icon.classList.remove("spin");
    }
  } catch(e){
    status.style.color = "#ef9a9a";
    status.textContent = "Could not reach local server. Is serve_picks.py running?";
    btn.disabled = false;
    icon.classList.remove("spin");
  }
}

// ── Scores Ticker ─────────────────────────────────────────────────────────────
function renderTicker(){
  // Live scores are fetched directly from MLB Stats API in the browser
  // so they update in real time without needing the server to regenerate HTML
  const track = document.getElementById("tickerTrack");

  function cityName(fullName) {
    return fullName ? fullName.split(" ").slice(0,-1).join(" ") || fullName : fullName;
  }

  function buildTickerHTML(games) {
    if (!games || games.length === 0) {
      return `<span class="ticker-empty">No games in progress yet today — check back later</span>`;
    }
    let html = "";
    for (let pass = 0; pass < 2; pass++) {
      games.forEach(g => {
        const awayScore = g.away_score;
        const homeScore = g.home_score;
        const awayWon   = awayScore > homeScore;
        const isLive    = g.is_live;
        const liveStyle = isLive ? "color:#ff6b35" : "";
        const liveClass = isLive ? "ticker-live" : "ticker-final";
        html += `
          <div class="ticker-item">
            <span class="${awayWon ? 'ticker-score win' : 'ticker-score loss'}">${g.away_city} ${awayScore}</span>
            <span style="color:var(--sub)">@</span>
            <span class="${!awayWon ? 'ticker-score win' : 'ticker-score loss'}">${g.home_city} ${homeScore}</span>
            <span class="${liveClass}" style="${liveStyle}">${g.label}</span>
          </div>`;
      });
    }
    return html;
  }

  async function fetchLiveScores() {
    const today = new Date().toLocaleDateString("en-CA"); // YYYY-MM-DD in local time
    const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${today}&hydrate=linescore&gameType=R`;
    try {
      const resp = await fetch(url);
      const data = await resp.json();
      const games = [];
      for (const dateEntry of (data.dates || [])) {
        for (const game of (dateEntry.games || [])) {
          const abstract = game?.status?.abstractGameState;
          const detailed = game?.status?.detailedState || "";
          // Weather / other disruptions: surface these instead of dropping them.
          const abnormal = /postpon|cancel|delay|suspend/i.test(detailed);
          if (abstract !== "Final" && abstract !== "Live" && !abnormal) continue;
          const away      = game.teams.away;
          const home      = game.teams.home;
          const linescore = game.linescore || {};
          const inning    = linescore.currentInning || 9;
          const half      = linescore.inningHalf || "";
          const isLive    = abstract === "Live";
          let label       = isLive ? `${half.slice(0,3)} ${inning}` : "Final";
          if (!isLive && inning > 9) label = `F/${inning}`;
          games.push({
            away_name:  away.team.name,           // full name, for pick matching
            home_name:  home.team.name,
            away:       away.team.name,           // alias used by pickResult
            home:       home.team.name,
            away_city:  cityName(away.team.name), // short name, for the ticker
            home_city:  cityName(home.team.name),
            away_score: away.score ?? 0,
            home_score: home.score ?? 0,
            is_live:    isLive,
            label:      label,
            status:     detailed,                 // detailed game state
            abnormal:   abnormal,                 // postponed/cancelled/delayed/suspended
          });
        }
      }
      return games;
    } catch(e) {
      console.warn("Live scores fetch failed:", e);
      return null;
    }
  }

  async function refreshTicker() {
    const games = await fetchLiveScores();
    if (games !== null) {
      window._liveGames = games;                 // feed _liveScores() / Daily Summary
      track.innerHTML = buildTickerHTML(games);
      const duration = Math.max(20, games.length * 5);
      track.style.animationDuration = duration + "s";
      try { if (typeof renderDailySummary === "function") renderDailySummary(); } catch(e){}
      // Refresh Today's Games so cancel/delay + lineup status stay current.
      try { if (typeof renderGames === "function") renderGames(); } catch(e){}
      // Re-render Game Picks ONCE on the first live fetch so status banners appear
      // (they can't render before the async score feed lands). Not repeated, so it
      // won't keep collapsing expanded cards every 2 minutes.
      if (!window._picksStatusPainted) {
        window._picksStatusPainted = true;
        try { if (typeof renderPicks === "function") renderPicks(); } catch(e){}
      }
      // Recompute Best Bets each cycle. Cheap, and it keeps the list consistent
      // with any client-side filter or state change without collapsing the
      // expanded pick cards the way a full renderPicks() would.
      try { if (typeof renderBestBets === "function") renderBestBets(); } catch(e){}
    }
  }

  // Initial load + refresh every 2 minutes
  refreshTicker();
  setInterval(refreshTicker, 2 * 60 * 1000);
}

// ── Force Odds Snapshot (AJAX — stays on page) ───────────────────────────────
async function forceOddsSnapshot(){
  const btn    = document.getElementById("forceOddsBtn");
  const status = document.getElementById("forceOddsStatus");
  if(!btn) return;

  btn.disabled = true;
  btn.textContent = "⏳ Requesting snapshot…";
  if(status) status.textContent = "";

  try{
    const res = await fetch("/force-odds");
    if(res.ok){
      let secs = 70;
      if(status){
        status.style.color = "var(--green)";
        status.textContent = `✓ Snapshot started — dashboard refreshes in ${secs}s`;
        const timer = setInterval(() => {
          secs--;
          if(secs <= 0){
            clearInterval(timer);
            location.reload(true);
          } else {
            status.textContent = `✓ Snapshot started — dashboard refreshes in ${secs}s`;
          }
        }, 1000);
      } else {
        setTimeout(() => location.reload(true), 70000);
      }
    } else {
      if(status){ status.style.color="var(--red)"; status.textContent="Request failed — try again in a moment."; }
      btn.disabled = false;
      btn.textContent = "🔄 Pull Odds Snapshot Now";
    }
  } catch(e){
    if(status){ status.style.color="var(--red)"; status.textContent="Could not reach server."; }
    btn.disabled = false;
    btn.textContent = "🔄 Pull Odds Snapshot Now";
  }
}

// ── Force Lineup Refresh (AJAX) ───────────────────────────────────────────────
async function forceLineupsRefresh(){
  const btn    = document.getElementById("forceLineupsBtn");
  const status = document.getElementById("forceLineupsStatus");
  if(!btn) return;

  btn.disabled = true;
  btn.textContent = "⏳ Refreshing…";
  if(status) status.textContent = "";

  try{
    const res = await fetch("/force-lineups");
    if(res.ok){
      let note = "";
      try { const j = await res.json(); note = j.message || ""; } catch(e){}
      let secs = 90;
      if(status){
        status.style.color = "#4fc3f7";
        // Show what actually changed (from the server), then count down to reload.
        const base = note ? note.replace(/\s*Dashboard refreshing…\s*$/, "") : "Refreshing";
        status.textContent = `${base} — reloading in ${secs}s…`;
        const timer = setInterval(() => {
          secs--;
          if(secs <= 0){
            clearInterval(timer);
            location.reload(true);
          } else {
            status.textContent = `${base} — reloading in ${secs}s…`;
          }
        }, 1000);
      } else {
        setTimeout(() => location.reload(true), 90000);
      }
    } else {
      if(status){ status.style.color="#ef5350"; status.textContent="Request failed."; }
      btn.disabled = false;
      btn.textContent = "📋 Refresh Lineups";
    }
  } catch(e){
    if(status){ status.style.color="#ef5350"; status.textContent="Could not reach server."; }
    btn.disabled = false;
    btn.textContent = "📋 Refresh Lineups";
  }
}

// ── Sharp Action Tab ──────────────────────────────────────────────────────────
function renderSharpAction(){
  const el = document.getElementById("sharpActionContent");
  if(!DATA_MOVEMENT || DATA_MOVEMENT.length === 0){
    el.innerHTML = `<div class="sharp-empty">
      <div style="font-size:2rem;margin-bottom:16px">📊</div>
      <div style="color:var(--text);font-size:1.1rem;font-weight:700;margin-bottom:8px">No Line Movement Data Yet</div>
      <div style="color:var(--sub);font-size:.88rem;max-width:420px;margin:0 auto;line-height:1.7">
        Sharp money tracking needs two odds snapshots to compare — the 6am pipeline takes the first one,
        then the server checks every 2 hours (8am–10pm ET).<br><br>
        If you're seeing this mid-day, hit the button below to pull a fresh snapshot right now.
        Line movement will appear on the next dashboard refresh (~60 seconds).
      </div>
      <div style="margin-top:24px">
        <button onclick="forceOddsSnapshot()" id="forceOddsBtn" style="display:inline-block;padding:10px 26px;
          background:linear-gradient(135deg,var(--green),#00b248);border-radius:24px;
          color:#000;font-weight:800;font-size:.88rem;border:none;cursor:pointer;font-family:inherit">
          🔄 Pull Odds Snapshot Now
        </button>
        <div id="forceOddsStatus" style="margin-top:12px;color:var(--sub);font-size:.82rem"></div>
      </div>
    </div>`;
    return;
  }

  const fmtO = o => (!o && o !== 0) ? "—" : (o > 0 ? `+${Math.round(o)}` : `${Math.round(o)}`);
  const fmtSnap = t => {
    if(!t) return "—";
    try{
      const d = new Date(t.endsWith("Z") ? t : t+"Z");
      return d.toLocaleTimeString("en-US",{hour:"numeric",minute:"2-digit",timeZone:"America/New_York"}) + " ET";
    } catch(e){ return t; }
  };

  let html = `<div class="sharp-explain-box">
    <strong style="color:var(--text)">What is sharp money?</strong>
    Professional bettors — called "sharps" — place large, well-researched bets.
    When a line moves significantly, it signals where pros are putting their money.
    Sportsbook signals: <span style="color:#ef5350;font-weight:700">STEAM</span> = 8+ point move,
    <span style="color:#ffa726;font-weight:700">DRIFT</span> = 3–7 points.
    Kalshi signals: <span style="color:#ef5350;font-weight:700">STEAM</span> = 5+ pp probability shift,
    <span style="color:#ffa726;font-weight:700">DRIFT</span> = 2–4 pp.
    <span style="color:#42a5f5;font-weight:700">Kalshi</span> is a regulated prediction market — it often moves before sportsbooks adjust.
    When both sources agree, that's the highest-conviction signal.
  </div>`;

  DATA_MOVEMENT.forEach(mv => {
    const awayNick = mv.away ? mv.away.split(" ").slice(-1)[0] : mv.away;
    const homeNick = mv.home ? mv.home.split(" ").slice(-1)[0] : mv.home;
    const mlSig    = mv.ml_signal    || "";
    const totSig   = mv.total_signal || "";
    const hasML    = mlSig === "STEAM" || mlSig === "DRIFT";
    const hasTot   = totSig === "STEAM" || totSig === "DRIFT";
    const dominant = (mlSig === "STEAM" || totSig === "STEAM") ? "STEAM" : "DRIFT";

    const sigBadge = dominant === "STEAM"
      ? `<span class="sharp-badge sharp-badge-steam">🔥 STEAM — Heavy Sharp Action</span>`
      : `<span class="sharp-badge sharp-badge-drift">📈 DRIFT — Notable Movement</span>`;

    // Moneyline odds comparison table
    let oddsHtml = "";
    if(hasML){
      const awayMove = mv.ml_away_move || 0;
      const homeMove = mv.ml_home_move || 0;
      const awayMoveColor = Math.abs(awayMove) >= 8 ? "#ef5350" : Math.abs(awayMove) >= 3 ? "#ffa726" : "var(--sub)";
      const homeMoveColor = Math.abs(homeMove) >= 8 ? "#ef5350" : Math.abs(homeMove) >= 3 ? "#ffa726" : "var(--sub)";
      const awayDir = awayMove > 0 ? "↑" : awayMove < 0 ? "↓" : "";
      const homeDir = homeMove > 0 ? "↑" : homeMove < 0 ? "↓" : "";
      oddsHtml = `<div class="sharp-odds-grid">
        <div class="sharp-odds-box">
          <div class="sharp-odds-team-lbl">${mv.away}</div>
          <div class="sharp-odds-row">
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Opening</div>
              <div class="sharp-odds-cell-num" style="color:var(--sub)">${fmtO(mv.ml_away_open)}</div>
            </div>
            <div class="sharp-odds-arrow">→</div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Current</div>
              <div class="sharp-odds-cell-num" style="color:var(--text)">${fmtO(mv.ml_away_now)}</div>
            </div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Move</div>
              <div class="sharp-odds-cell-num" style="color:${awayMoveColor}">${awayDir}${Math.abs(awayMove)}</div>
            </div>
          </div>
        </div>
        <div class="sharp-odds-box">
          <div class="sharp-odds-team-lbl">${mv.home}</div>
          <div class="sharp-odds-row">
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Opening</div>
              <div class="sharp-odds-cell-num" style="color:var(--sub)">${fmtO(mv.ml_home_open)}</div>
            </div>
            <div class="sharp-odds-arrow">→</div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Current</div>
              <div class="sharp-odds-cell-num" style="color:var(--text)">${fmtO(mv.ml_home_now)}</div>
            </div>
            <div class="sharp-odds-cell">
              <div class="sharp-odds-cell-lbl">Move</div>
              <div class="sharp-odds-cell-num" style="color:${homeMoveColor}">${homeDir}${Math.abs(homeMove)}</div>
            </div>
          </div>
        </div>
      </div>`;
    }

    // Total line movement row
    let totalHtml = "";
    if(hasTot){
      const tdir   = (mv.total_move || 0) > 0 ? "▲" : "▼";
      const tColor = totSig === "STEAM" ? "#ef5350" : "#ffa726";
      const tSide  = (mv.total_move || 0) > 0 ? "Over" : "Under";
      totalHtml = `<div class="sharp-total-row">
        <span style="color:var(--sub);font-weight:600">Over/Under</span>
        <span style="color:var(--sub)">${mv.total_open} → ${mv.total_now}</span>
        <span style="color:${tColor};font-weight:800">${tdir} ${Math.abs(mv.total_move||0).toFixed(1)} pts (${totSig})</span>
        <span style="color:var(--sub)">Heavy action on the ${tSide}</span>
      </div>`;
    }

    // ── Kalshi market signal panel ─────────────────────────────────────────
    let kalshiCardHtml = "";
    const kSig   = mv.kalshi_signal || "";
    const hasKal = kSig === "STEAM" || kSig === "DRIFT";
    if(hasKal){
      const kSharp    = mv.kalshi_sharp_side || "";
      const kNick     = kSharp ? kSharp.split(" ").slice(-1)[0] : "";
      const kMovePct  = mv.kalshi_away_move != null
        ? Math.round(Math.abs(mv.kalshi_away_move) * 100) : 0;
      const kSigColor = kSig === "STEAM" ? "#ef5350" : "#ffa726";
      const kLabel    = kSig === "STEAM" ? "🔥 STEAM" : "📈 DRIFT";

      const kAwayOpen = mv.kalshi_away_open != null ? Math.round(mv.kalshi_away_open * 100) + "%" : "—";
      const kAwayNow  = mv.kalshi_away_now  != null ? Math.round(mv.kalshi_away_now  * 100) + "%" : "—";
      const kHomeOpen = mv.kalshi_home_open != null ? Math.round(mv.kalshi_home_open * 100) + "%" : "—";
      const kHomeNow  = mv.kalshi_home_now  != null ? Math.round(mv.kalshi_home_now  * 100) + "%" : "—";

      // Kalshi vs sportsbook agreement
      const sbSharp   = mv.sharp_side || "";
      const sbNick    = sbSharp ? sbSharp.split(" ").slice(-1)[0].toLowerCase() : "";
      const kMatch    = kNick && sbNick && kNick.toLowerCase() === sbNick;
      const kMismatch = kNick && sbNick && kNick.toLowerCase() !== sbNick;
      const agreeLbl  = kMatch
        ? `<span style="background:rgba(105,240,174,.12);color:#69f0ae;border:1px solid rgba(105,240,174,.3);
             padding:2px 8px;border-radius:4px;font-size:.68rem;font-weight:700">✓ Confirms sportsbook</span>`
        : (kMismatch
          ? `<span style="background:rgba(255,167,38,.1);color:#ffa726;border:1px solid rgba(255,167,38,.3);
               padding:2px 8px;border-radius:4px;font-size:.68rem;font-weight:700">⚠ Diverges from sportsbook</span>`
          : "");

      kalshiCardHtml = `<div style="margin:12px 0;padding:12px;background:rgba(66,165,245,.07);
        border:1px solid rgba(66,165,245,.25);border-radius:8px">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap">
          <span style="color:#42a5f5;font-weight:800;font-size:.85rem">📊 Kalshi Prediction Market</span>
          <span style="color:${kSigColor};font-weight:800;font-size:.82rem">${kLabel}</span>
          ${agreeLbl}
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:.8rem">
          <div style="background:rgba(66,165,245,.08);border-radius:6px;padding:8px;text-align:center">
            <div style="color:var(--sub);font-size:.68rem;margin-bottom:4px">${mv.away}</div>
            <div style="color:var(--sub)">${kAwayOpen}</div>
            <div style="color:#42a5f5;font-size:.7rem">→</div>
            <div style="color:var(--text);font-weight:700">${kAwayNow}</div>
          </div>
          <div style="background:rgba(66,165,245,.08);border-radius:6px;padding:8px;text-align:center">
            <div style="color:var(--sub);font-size:.68rem;margin-bottom:4px">${mv.home}</div>
            <div style="color:var(--sub)">${kHomeOpen}</div>
            <div style="color:#42a5f5;font-size:.7rem">→</div>
            <div style="color:var(--text);font-weight:700">${kHomeNow}</div>
          </div>
        </div>
        ${kNick ? `<div style="margin-top:8px;color:var(--sub);font-size:.75rem">
          Market shifted ${kMovePct}pp toward <strong style="color:var(--text)">${kSharp}</strong>
          ${kSig === "STEAM" ? "— strong early signal before books adjust" : "— moderate market repricing"}
        </div>` : ""}
      </div>`;
    }

    // Bet call + model agreement
    const sharp = mv.sharp_side || "";
    let betCallHtml = "";
    if(sharp && hasML){
      const sharpNick  = sharp.split(" ").slice(-1)[0];
      const gamePick   = ACTIVE_PICKS.find(p => p.away === mv.away && p.home === mv.home && (p.type==="ML"||p.type==="RL"));
      let modelNoteHtml = "";
      if(gamePick){
        const pickNick = gamePick.team ? gamePick.team.split(" ").slice(-1)[0] : "";
        const agrees   = sharpNick.toLowerCase() === pickNick.toLowerCase();
        modelNoteHtml  = agrees
          ? `<span style="color:#69f0ae;font-weight:700;font-size:.78rem">✓ Our ${gamePick.tier} pick agrees</span>`
          : `<span style="color:#ef9a9a;font-weight:700;font-size:.78rem">⚠ Our ${gamePick.tier} pick is ${gamePick.team} — use caution</span>`;
      }
      const urgency = dominant === "STEAM" ? "Strong Sharp Signal" : "Moderate Sharp Signal";
      betCallHtml = `<div class="sharp-bet-call">
        <div>
          <div class="sharp-bet-call-lbl">${urgency}</div>
          <div class="sharp-bet-call-text">Follow: Bet <span style="color:var(--green)">${sharp} ML</span></div>
        </div>
        ${modelNoteHtml}
      </div>`;
    } else if(hasTot){
      const tBetSide = (mv.total_move || 0) > 0 ? "Over" : "Under";
      const tLine    = mv.total_now || mv.total_open;
      betCallHtml = `<div class="sharp-bet-call">
        <div>
          <div class="sharp-bet-call-lbl">${dominant === "STEAM" ? "Strong Sharp Signal" : "Moderate Sharp Signal"}</div>
          <div class="sharp-bet-call-text">Follow: Bet <span style="color:var(--green)">${tBetSide} ${tLine}</span></div>
        </div>
      </div>`;
    }

    html += `<div class="sharp-action-card">
      <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:16px">
        <div>
          <div style="font-size:1.05rem;font-weight:800;color:var(--text)">${mv.away} @ ${mv.home}</div>
          <div style="font-size:.72rem;color:var(--sub);margin-top:3px">
            Compared: ${fmtSnap(mv.snap1_time)} vs ${fmtSnap(mv.snap2_time)}
          </div>
        </div>
        ${sigBadge}
      </div>
      ${oddsHtml}
      ${totalHtml}
      ${kalshiCardHtml}
      ${betCallHtml}
    </div>`;
  });

  el.innerHTML = html;
}

// ── Boot ─────────────────────────────────────────────────────────────────────
// Render critical above-fold content immediately, defer heavy tabs

// ── Daily Summary Tab ──────────────────────────────────────────────────────────
// ── Global score helpers (used by renderPicks + renderDailySummary) ──────────
function _liveScores(){ return window._liveGames || DATA_SCORES || []; }
function _scoreAway(s){ return s.away_name || s.away || ""; }
function _scoreHome(s){ return s.home_name || s.home || ""; }
// Weather/other disruption badge from the live feed's detailed game state.
function statusBadge(detailed){
  if(!detailed) return "";
  const d = detailed.toLowerCase();
  if(d.includes("postpon")) return `<span class="gs-badge gs-bad">⛔ POSTPONED</span>`;
  if(d.includes("cancel"))  return `<span class="gs-badge gs-bad">🚫 CANCELLED</span>`;
  if(d.includes("suspend")) return `<span class="gs-badge gs-warn">⏸ SUSPENDED</span>`;
  if(d.includes("delay"))   return `<span class="gs-badge gs-warn">⏳ ${detailed.toUpperCase()}</span>`;
  return "";
}
function _statusFor(away, home){
  const s = _liveScores().find(x =>
    (_scoreAway(x)===away && _scoreHome(x)===home) ||
    (_scoreAway(x)===home && _scoreHome(x)===away));
  return (s && s.abnormal) ? { detail: s.status, badge: statusBadge(s.status) } : null;
}
function _findScore(pick){
  return _liveScores().find(s =>
    (_scoreAway(s)===pick.away && _scoreHome(s)===pick.home) ||
    (_scoreAway(s)===pick.home && _scoreHome(s)===pick.away)
  );
}
function _isFinal(score){
  if(!score) return false;
  const lbl = (score.label||"").trim();
  return lbl==="Final" || lbl.startsWith("F/") || lbl==="F";
}
function _pickResult(pick, score){
  if(!score || score.abnormal || !_isFinal(score)) return null;  // postponed/cancelled = no action
  const awayScore = parseInt(score.away_score||0);
  const homeScore = parseInt(score.home_score||0);
  // MLB games don't end in ties
  const awayWon = awayScore > homeScore;
  if(pick.type==="ML"){
    const pickNick = (pick.team||"").split(" ").slice(-1)[0].toLowerCase();
    const awayFull = _scoreAway(score).toLowerCase();
    const pickedAway = awayFull.endsWith(pickNick) || awayFull.includes(pickNick);
    return (pickedAway && awayWon)||(!pickedAway && !awayWon) ? "win" : "loss";
  }
  if(pick.type==="RL"){
    // Grade against the 1.5 spread in the label ("Team -1.5" / "Team +1.5").
    const sp = parseFloat((String(pick.label).match(/[+-][\d.]+/)||[])[0]);
    if(isNaN(sp)) return null;
    const pickNick = (pick.team||"").split(" ").slice(-1)[0].toLowerCase();
    const awayFull = _scoreAway(score).toLowerCase();
    const pickedAway = awayFull.endsWith(pickNick) || awayFull.includes(pickNick);
    const margin = (pickedAway ? awayScore-homeScore : homeScore-awayScore) + sp;
    return margin > 0 ? "win" : (margin === 0 ? "push" : "loss");
  }
  if(pick.type==="TOTAL"){
    const total = awayScore + homeScore;
    // Bet line lives in the label ("OVER 8.5"), NOT the model projection.
    const line  = parseFloat((String(pick.label).match(/[\d.]+/)||[])[0]);
    if(!line) return null;
    if(total===line) return "push";
    const overPick = (pick.label||"").toLowerCase().includes("over");
    return (overPick && total>line)||(!overPick && total<line) ? "win" : "loss";
  }
  return null;
}

function renderDailySummary(){
  const el = document.getElementById("dailySummaryContent");
  if(!el) return;

  const TIER_COLOR = {LOCK:"#ffc107",STRONG:"#42a5f5",LEAN:"#66bb6a",TOSSUP:"#a09ae0"};

  // Match a pick to a score entry — use the LIVE feed, not static DATA_SCORES,
  // so games flow in as they finish during the evening. _liveScores() returns
  // window._liveGames (updated by the ticker) and falls back to DATA_SCORES.
  function findScore(pick){
    return _liveScores().find(s =>
      (_scoreAway(s)===pick.away && _scoreHome(s)===pick.home) ||
      (_scoreAway(s)===pick.home && _scoreHome(s)===pick.away)
    );
  }

  // Is this score final?
  function isFinal(score){
    if(!score) return false;
    const lbl = (score.label||"").trim();
    return lbl==="Final" || lbl.startsWith("F/") || lbl==="F";
  }

  // Determine WIN/LOSS/PUSH from a final score
  function pickResult(pick, score){
    if(!score || score.abnormal || !isFinal(score)) return null;  // postponed/cancelled = no action
    const awayScore = parseInt(score.away_score||0);
    const homeScore = parseInt(score.home_score||0);
    if(awayScore===homeScore) return "push";
    const awayWon = awayScore > homeScore;
    if(pick.type==="ML"){
      const pickNick = (pick.team||"").split(" ").slice(-1)[0].toLowerCase();
      const awayNick = (score.away||"").split(" ").slice(-1)[0].toLowerCase();
      const pickedAway = pickNick===awayNick || (score.away||"").toLowerCase().includes(pickNick);
      return (pickedAway && awayWon)||(!pickedAway && !awayWon) ? "win" : "loss";
    }
    if(pick.type==="RL"){
      const sp = parseFloat((String(pick.label).match(/[+-][\d.]+/)||[])[0]);
      if(isNaN(sp)) return null;
      const pickNick = (pick.team||"").split(" ").slice(-1)[0].toLowerCase();
      const awayNick = (score.away||"").split(" ").slice(-1)[0].toLowerCase();
      const pickedAway = pickNick===awayNick || (score.away||"").toLowerCase().includes(pickNick);
      const margin = (pickedAway ? awayScore-homeScore : homeScore-awayScore) + sp;
      return margin > 0 ? "win" : (margin === 0 ? "push" : "loss");
    }
    if(pick.type==="TOTAL"){
      const total = awayScore + homeScore;
      // Grade against the actual bet line in the label, not the model projection.
      const line  = parseFloat((String(pick.label).match(/[\d.]+/)||[])[0]);
      if(!line) return null;
      if(total===line) return "push";
      const pickOver = (pick.label||"").toUpperCase().includes("OVER");
      return (pickOver&&total>line)||(!pickOver&&total<line) ? "win" : "loss";
    }
    return null;
  }

  // Score display
  function scoreStr(score){
    if(!score) return "";
    const a = score.away_city||score.away||"";
    const h = score.home_city||score.home||"";
    return `${a.split(" ").slice(-1)[0]} ${score.away_score} – ${h.split(" ").slice(-1)[0]} ${score.home_score}`;
  }

  // ── Collect completed picks only ──────────────────────────────────────────
  const completed = [];
  (DATA_TODAY_PICKS||DATA_PICKS||[]).forEach(p => {
    const sc  = findScore(p);
    const res = pickResult(p, sc);
    if(res) completed.push({pick:p, score:sc, result:res});
  });

  const wins   = completed.filter(c=>c.result==="win").length;
  const losses = completed.filter(c=>c.result==="loss").length;
  const pushes = completed.filter(c=>c.result==="push").length;
  const total  = wins+losses;
  const wr     = total>0 ? Math.round(wins/total*100) : 0;
  const lockW  = completed.filter(c=>c.result==="win"&&c.pick.tier==="LOCK").length;
  const lockL  = completed.filter(c=>c.result==="loss"&&c.pick.tier==="LOCK").length;
  // IN-PROGRESS COUNT — MEASURED, NOT INFERRED (rewritten 2026-08-17).
  //
  // Two wrong answers before this one, both from the same bad assumption:
  //   pending = DATA_PICKS.length - completed.length   ->  -17
  //   pending = DATA_TODAY_PICKS.length - completed.length -> 7
  // Subtraction assumes every pick is either finished or playing. It is not.
  // A pick drops out of `completed` if findScore cannot match its game, or if
  // pickResult cannot grade it (unparseable spread, missing total line). Those
  // failures then masquerade as live games. "7 still in progress" meant seven
  // picks failed to grade, at a moment when nothing was being played.
  //
  // So count what is actually being asked: GAMES the live feed reports as under
  // way. If the feed says nothing is in progress, the answer is zero, whatever
  // the pick arithmetic implies.
  const _inProgressGames = new Set();
  (_liveScores() || []).forEach(g => {
    const st = (g.detailedState || g.status || "").toLowerCase();
    if(st.includes("progress") || st.includes("warmup") || st.includes("delayed"))
      _inProgressGames.add(((g.away||"") + " @ " + (g.home||"")).trim());
  });
  const pending = _inProgressGames.size;

  // DELIBERATELY NOT SHOWN: the gap between _todayAll and completed.
  //
  // I briefly surfaced it as an "ungraded" card. It is noise on this panel.
  // DATA_TODAY_PICKS is the full slate RE-SCORED at render time, so it is "what
  // the model would pick now", not "what was on the board today". Rescoring
  // through the day changes picks, so a gap here mostly reflects that drift
  // rather than a grading failure, and the reader has no way to act on it.
  //
  // If picks genuinely fail to grade that IS worth tracking — but against the
  // PUBLISHED picks in the DB, on an admin page, not on the results panel.
  // See the note in CLAUDE.md about summary grading a re-derived pick set.

  // ── Header ────────────────────────────────────────────────────────────────
  let html = `
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px">
    <span style="background:#1f2d40;color:#58a6ff;padding:3px 10px;border-radius:6px;font-size:.78rem;font-weight:600">${DATA_DATE||"Today"}</span>
    <span style="color:#8b949e;font-size:.78rem">Updates live as games finish</span>
  </div>

  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px">
    <div class="stat-card">
      <div style="font-size:1.4rem;font-weight:700;color:${wins>losses?"#3fb950":wins<losses?"#f85149":"#e6edf3"}">${wins}–${losses}</div>
      <div style="font-size:.7rem;color:#8b949e;margin-top:4px">Record (final)</div>
    </div>
    <div class="stat-card">
      <div style="font-size:1.4rem;font-weight:700">${total>0?wr+"%":"—"}</div>
      <div style="font-size:.7rem;color:#8b949e;margin-top:4px">Win rate</div>
    </div>
    <div class="stat-card">
      <div style="font-size:1.4rem;font-weight:700;color:#ffc107">${lockW}–${lockL}</div>
      <div style="font-size:.7rem;color:#8b949e;margin-top:4px">🔒 Lock record</div>
    </div>
    <div class="stat-card">
      <div style="font-size:1.4rem;font-weight:700;color:#8b949e">${pending}</div>
      <div style="font-size:.7rem;color:#8b949e;margin-top:4px">Still in progress</div>
    </div>

  </div>`;

  // ── No picks yet ──────────────────────────────────────────────────────────
  if(completed.length===0){
    html += `<div style="color:#8b949e;text-align:center;padding:60px 0;font-size:.88rem">
      ${pending>0 ? `⚾ ${pending} pick${pending>1?"s":""} in progress — check back as games finish.` : "No picks yet today."}
    </div>`;
    el.innerHTML = html;
    return;
  }

  // ── Completed pick cards ──────────────────────────────────────────────────
  html += `<div style="font-size:.7rem;font-weight:600;color:#8b949e;text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px">Final picks — ${completed.length} game${completed.length!==1?"s":""}</div>`;

  completed.forEach(({pick:p, score:sc, result:res})=>{
    const borderColor = res==="win"?"#3fb950":res==="loss"?"#f85149":"#8b949e";
    const badgeBg     = res==="win"?"rgba(63,185,80,.15)":res==="loss"?"rgba(248,81,73,.15)":"rgba(139,148,158,.1)";
    const badgeColor  = res==="win"?"#3fb950":res==="loss"?"#f85149":"#8b949e";
    const badgeText   = res==="win"?"WIN ✓":res==="loss"?"LOSS ✗":"PUSH";
    const tc = TIER_COLOR[p.tier]||"#8b949e";
    const sc2 = scoreStr(sc);

    html += `
    <div style="background:#161b22;border:1px solid #30363d;border-left:3px solid ${borderColor};border-radius:10px;padding:12px 16px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
      <div style="flex:1;min-width:160px">
        <div style="font-size:.72rem;color:#8b949e;margin-bottom:3px">${teamLogo(p.away,14)}${p.away||""} @ ${teamLogo(p.home,14)}${p.home||""}</div>
        <div style="font-size:.9rem;font-weight:700"><span style="color:${tc}">${p.tier}</span> ${p.label}</div>
        <div style="font-size:.68rem;color:#8b949e;margin-top:3px">${p.conf}% conf${p.kelly_pct!==undefined?" &nbsp;|&nbsp; Kelly: "+p.kelly_pct+"%":""}</div>
      </div>
      <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
        ${sc2?`<div style="font-size:.75rem;color:#e6edf3;font-weight:600">${sc2}</div>`:""}
        <span style="background:${badgeBg};color:${badgeColor};font-size:.78rem;font-weight:700;padding:3px 12px;border-radius:6px">${badgeText}</span>
      </div>
    </div>`;
  });

  // ── Parlays (completed only) ───────────────────────────────────────────────
  const allParlays = [...(DATA_P2||[]), ...(DATA_P3||[])];
  const doneParlays = allParlays.filter(par => {
    const legs = par.picks||par.legs||par;
    if(!Array.isArray(legs)) return false;
    return legs.every(l => { const s=findScore(l); return isFinal(s); });
  });

  if(doneParlays.length>0){
    html += `<div style="font-size:.7rem;font-weight:600;color:#8b949e;text-transform:uppercase;letter-spacing:.06em;margin:16px 0 10px">Parlays — final</div>`;
    doneParlays.forEach(par=>{
      const legs = par.picks||par.legs||par;
      const results = legs.map(l=>pickResult(l,findScore(l)));
      const res = results.every(r=>r==="win")?"win":results.some(r=>r==="loss"||r==="push")?"loss":"pending";
      const borderColor = res==="win"?"#3fb950":res==="loss"?"#f85149":"#8b949e";
      const badgeText   = res==="win"?"WIN ✓":res==="loss"?"LOSS ✗":"—";
      const badgeColor  = res==="win"?"#3fb950":"#f85149";
      html += `<div style="background:#161b22;border:1px solid #30363d;border-left:3px solid ${borderColor};border-radius:10px;padding:12px 16px;margin-bottom:8px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
          <div style="font-weight:700;font-size:.83rem">${Array.isArray(legs)?legs.length:2}-leg parlay</div>
          <span style="color:${badgeColor};font-weight:700;font-size:.8rem">${badgeText}</span>
        </div>`;
      if(Array.isArray(legs)){
        legs.forEach(leg=>{
          const sc3=findScore(leg); const lr=pickResult(leg,sc3);
          const lc=lr==="win"?"#3fb950":lr==="loss"?"#f85149":"#8b949e";
          const icon=lr==="win"?"✓":lr==="loss"?"✗":"◦";
          html+=`<div style="font-size:.75rem;color:${lc};padding:3px 0;border-bottom:1px solid #21262d">${icon} ${leg.label||leg.team||""}${sc3?` — ${scoreStr(sc3)}`:""}</div>`;
        });
      }
      html += `</div>`;
    });
  }

  // ── Sharp action — final games only ──────────────────────────────────────
  const finalMovement = (DATA_MOVEMENT||[]).filter(mv=>{
    const sc = (DATA_SCORES||[]).find(s=>s.away===mv.away&&s.home===mv.home);
    return sc && isFinal(sc);
  });

  if(finalMovement.length>0){
    let ssW=0,ssL=0,bw=0,mW=0,mL=0;
    const sharpRows = finalMovement.map(mv=>{
      const sig = mv.ml_signal==="STEAM"||mv.ml_signal==="DRIFT" ? mv.ml_signal : (mv.total_signal||"MOVE");
      const sigColor = sig==="STEAM"?"#ef5350":sig==="DRIFT"?"#ffa726":"#8b949e";
      const sc = (DATA_SCORES||[]).find(s=>s.away===mv.away&&s.home===mv.home);
      const awayFinal=parseInt(sc?.away_score||0), homeFinal=parseInt(sc?.home_score||0);
      const actualWinner = awayFinal>homeFinal ? mv.away : mv.home;
      const winnerNick = (actualWinner||"").split(" ").slice(-1)[0];
      const sharpNick  = (mv.sharp_side||"").split(" ").slice(-1)[0].toLowerCase();
      const winnerNickL = winnerNick.toLowerCase();
      const pick = (DATA_PICKS||[]).find(p=>p.away===mv.away&&p.home===mv.home&&(p.type==="ML"||p.type==="RL"));
      const modelStr = pick
        ? `<span style="color:${TIER_COLOR[pick.tier]||"#8b949e"}">${pick.tier}</span> ${(pick.team||"").split(" ").slice(-1)[0]}`
        : `<span style="color:#8b949e">Pass</span>`;
      let winnerStr="—",wc="#8b949e";
      if(mv.sharp_side && actualWinner){
        const pickNick = pick?(pick.team||"").split(" ").slice(-1)[0].toLowerCase():"";
        if(sharpNick===winnerNickL && pickNick===winnerNickL){ winnerStr="Agree ✓";wc="#3fb950";ssW++;mW++; }
        else if(sharpNick===winnerNickL && pickNick!==winnerNickL && pick){ winnerStr="⚡ Sharp";wc="#ffa726";ssW++;mL++; }
        else if(sharpNick!==winnerNickL && pickNick===winnerNickL && pick){ winnerStr="✓ Model";wc="#3fb950";ssL++;mW++; }
        else if(sharpNick!==winnerNickL && pick && pickNick!==winnerNickL){ winnerStr='<span style="background:rgba(239,83,80,.15);color:#ef5350;padding:1px 6px;border-radius:3px;font-size:.68rem">Both Wrong</span>';bw++;ssL++;mL++; }
        else if(sharpNick!==winnerNickL){ winnerStr="✗ Sharp";wc="#f85149";ssL++; }
      }
      return `<tr>
        <td style="padding:7px 10px;border-bottom:1px solid #21262d;font-size:.75rem">${(mv.away||"").split(" ").slice(-1)[0]} @ ${(mv.home||"").split(" ").slice(-1)[0]}</td>
        <td style="padding:7px 10px;border-bottom:1px solid #21262d;color:${sigColor};font-weight:700;font-size:.72rem">${sig}</td>
        <td style="padding:7px 10px;border-bottom:1px solid #21262d;font-size:.75rem">${mv.sharp_side?(mv.sharp_side||"").split(" ").slice(-1)[0]:"—"}</td>
        <td style="padding:7px 10px;border-bottom:1px solid #21262d;font-size:.75rem">${modelStr}</td>
        <td style="padding:7px 10px;border-bottom:1px solid #21262d;font-weight:600;font-size:.75rem">${winnerNick||"—"}</td>
        <td style="padding:7px 10px;border-bottom:1px solid #21262d;color:${wc};font-weight:700;font-size:.75rem">${winnerStr}</td>
      </tr>`;
    }).join("");

    const mD=mW+mL, sD=ssW+ssL;
    html += `
    <div style="font-size:.7rem;font-weight:600;color:#8b949e;text-transform:uppercase;letter-spacing:.06em;margin:20px 0 10px">Sharp action — final games</div>
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:12px">
      <div class="stat-card"><div style="font-size:1.1rem;font-weight:700">${mD>0?mW+"/"+mD:"—"}</div><div style="font-size:.68rem;color:#8b949e;margin-top:3px">Model W/L</div></div>
      <div class="stat-card"><div style="font-size:1.1rem;font-weight:700">${sD>0?ssW+"/"+sD:"—"}</div><div style="font-size:.68rem;color:#8b949e;margin-top:3px">Sharp W/L</div></div>
      <div class="stat-card"><div style="font-size:1.1rem;font-weight:700;color:#ef5350">${bw}</div><div style="font-size:.68rem;color:#8b949e;margin-top:3px">Both Wrong</div></div>
    </div>
    <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;overflow:hidden;margin-bottom:16px">
      <table style="width:100%;border-collapse:collapse">
        <thead><tr style="border-bottom:1px solid #30363d">
          <th style="padding:8px 10px;text-align:left;color:#8b949e;font-size:.65rem;text-transform:uppercase">Game</th>
          <th style="padding:8px 10px;text-align:left;color:#8b949e;font-size:.65rem;text-transform:uppercase">Signal</th>
          <th style="padding:8px 10px;text-align:left;color:#8b949e;font-size:.65rem;text-transform:uppercase">Sharp side</th>
          <th style="padding:8px 10px;text-align:left;color:#8b949e;font-size:.65rem;text-transform:uppercase">Model</th>
          <th style="padding:8px 10px;text-align:left;color:#8b949e;font-size:.65rem;text-transform:uppercase">Winner</th>
          <th style="padding:8px 10px;text-align:left;color:#8b949e;font-size:.65rem;text-transform:uppercase">Edge</th>
        </tr></thead>
        <tbody>${sharpRows}</tbody>
      </table>
    </div>`;
  }

  el.innerHTML = html;
}

renderTicker();
renderYesterday();
renderLearn();
renderPicks();
renderSurfacedProps();

// ── Bankroll Calculator ──────────────────────────────────────────────────────
window._BR = null;  // global bankroll — pick cards read this at render time

function applyBankroll(br) {
  window._BR = (br && br > 0) ? br : null;
  const inp = document.getElementById('bankrollInput');
  const unitRow = document.getElementById('bkUnitRow');
  const unitEl  = document.getElementById('bkUnit');
  if (window._BR) {
    localStorage.setItem('statalizers_bankroll', window._BR);
    if (unitRow) unitRow.style.display = '';
    if (unitEl)  unitEl.textContent = '$' + (window._BR / 100).toFixed(2);
    if (inp && inp.value != window._BR) inp.value = window._BR;
  }

  // Update Kelly rows already in DOM
  document.querySelectorAll('.kelly-row[data-kelly]').forEach(el => {
    const pct = parseFloat(el.dataset.kelly || 0);
    if (!pct) return;
    const betAmt = window._BR ? (window._BR * pct / 100).toFixed(2) : (pct / 100).toFixed(2);
    const note   = window._BR ? `on $${window._BR.toLocaleString()}` : 'per $1 bankroll';
    const strong = el.querySelector('.kelly-amt');
    if (strong) strong.textContent = '$' + betAmt;
    const kn = el.querySelector('.kelly-note');
    if (kn) kn.textContent = `${note} (${pct}% Half-Kelly)`;
  });

  // Update yesterday profit span
  const profEl = document.getElementById('ydayProfit');
  if (profEl) {
    const units = parseFloat(profEl.dataset.profit || 0);
    const val   = window._BR ? units * window._BR : units;
    const sign  = val >= 0 ? '+' : '';
    const note  = (!window._BR || window._BR === 1) ? ' <small style="color:#484f58">($1 bankroll)</small>' : '';
    profEl.innerHTML = `${sign}$${Math.abs(val).toFixed(2)}${note}`;
  }
}

// Wire up input
const _bkInp = document.getElementById('bankrollInput');
if (_bkInp) {
  _bkInp.addEventListener('input', function() {
    const v = parseFloat(this.value);
    if (!isNaN(v) && v > 0) applyBankroll(v);
  });
  // Restore from localStorage
  const saved = parseFloat(localStorage.getItem('statalizers_bankroll') || '');
  if (saved && saved > 0) { _bkInp.value = saved; applyBankroll(saved); }
}
// ──────────────────────────────────────────────────────────────────────────────
// Defer heavy renders so browser doesn't block on initial paint
setTimeout(() => {
  renderSharpMoney();
  renderSharpAction();
  renderThematic();
  renderParlays();
  renderSchedule();
  renderGames();
  renderProps();
  initTeamsTab();
  renderDailySummary();
  renderPicks();
}, 0);

// Auto-reload every 15 minutes to pick up fresh odds, lineups, and scores
// Only runs between 9am and midnight ET
(function() {
  const RELOAD_MS = 15 * 60 * 1000;
  function scheduleReload() {
    const now = new Date();
    const etHour = new Date(now.toLocaleString("en-US", {timeZone: "America/New_York"})).getHours();
    if (etHour >= 9 && etHour < 24) {
      setTimeout(() => { location.reload(true); }, RELOAD_MS);
    }
  }
  scheduleReload();
})();
</script>
<div id="parlayDrawer" style="display:none"></div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main(date=None, no_open=False):
    # argparse is handled in __main__ block only — never called from here.
    target = date or _today_et()

    # NOTE: Odds snapshot is handled by app.py _run_odds_snapshot() before
    # this function is called — do NOT call run_odds() here or it runs twice,
    # doubles hang time, and burns API quota.

    from model.mlb_model import MLBModel
    from model.mlb_picks import generate_picks, build_parlays, build_thematic_parlays
    from model.mlb_props_model import score_all_props

    model = MLBModel()
    model.load()

    # score_today filters to upcoming games only — we also need all games for the schedule tab
    all_schedule = [g for g in model.schedule if g.get("game_date") == (
        date or _today_et()
    )]

    # Deduplicate all_schedule — schedule CSV can accumulate duplicate rows across pipeline runs
    seen_sched = set()
    deduped_sched = []
    for g in all_schedule:
        key = (g.get("away_team", ""), g.get("home_team", ""))
        if key not in seen_sched:
            seen_sched.add(key)
            deduped_sched.append(g)
    if len(deduped_sched) < len(all_schedule):
        log.warning(f"Removed {len(all_schedule) - len(deduped_sched)} duplicate game(s) from all_schedule")
    all_schedule = deduped_sched

    scored, actual_date = model.score_today(target, pivot=False)

    # Deduplicate scored games by (away_team, home_team) — schedule CSV sometimes has duplicate rows
    seen_games = set()
    deduped = []
    for g in scored:
        key = (g.get("away_team", ""), g.get("home_team", ""))
        if key not in seen_games:
            seen_games.add(key)
            deduped.append(g)
    if len(deduped) < len(scored):
        log.warning(f"Removed {len(scored) - len(deduped)} duplicate game(s) from scored list")
    scored = deduped

    # Score tomorrow’s slate for the date toggle (uses same loaded model, no extra I/O)
    from datetime import timedelta as _td
    tomorrow_str = (datetime.strptime(target, "%Y-%m-%d") + _td(days=1)).strftime("%Y-%m-%d")
    try:
        scored_next, _ = model.score_today(tomorrow_str)
    except Exception:
        scored_next = []
    # Tomorrow schedule for the schedule tab
    all_schedule_next = [g for g in model.schedule if g.get("game_date") == tomorrow_str]
    seen_sched_next = set()
    deduped_sched_next = []
    for g in all_schedule_next:
        key = (g.get("away_team", ""), g.get("home_team", ""))
        if key not in seen_sched_next:
            seen_sched_next.add(key)
            deduped_sched_next.append(g)
    all_schedule_next = deduped_sched_next

    if not scored and not all_schedule:
        log.warning(f"No games found for {target}. Run python run_pipeline.py first.")
        return None   # let app.py serve the "check back soon" page — never sys.exit inside Flask

    # Fetch live scores FIRST — filter finished games before generating picks
    today_scores = fetch_live_scores(actual_date)
    if not today_scores:
        today_scores = model.get_today_scores(actual_date)

    # Remove games the MLB API confirms as Final BEFORE picks are generated
    finished = set(
        (s["away_team"], s["home_team"])
        for s in today_scores
        if s.get("status") == "Final"
    )
    if finished:
        before = len(scored)
        scored  = [g for g in scored
                   if (g["away_team"], g["home_team"]) not in finished]
        removed = before - len(scored)
        if removed:
            log.info(f"Removed {removed} finished game(s) confirmed by live API")

    # Now generate picks from the fully filtered game list
    picks     = generate_picks(scored)

    # Daily Summary needs TODAY's picks all evening, INCLUDING games that already
    # finished. `picks` comes from score_today(), which deliberately drops
    # completed/in-progress games (mlb_model.py:1483) — so using it here made
    # finished games vanish from the summary the moment they went Final (the
    # long-standing "Daily Summary not working" bug). all_schedule holds the FULL
    # slate incl. started/finished, so ALWAYS score that for the summary dataset.
    _today_scored = []
    for _g in all_schedule:
        try:
            _today_scored.append(model.score_game(_g))
        except Exception:
            pass
    today_picks_all = generate_picks(_today_scored) if _today_scored else picks

    # ── GRADE WHAT WAS PUBLISHED, NOT WHAT WE WOULD PICK NOW ─────────────────
    # (2026-08-18, after Daily Summary said 13-6 and the Yesterday tab said 11-10
    # for the same date.)
    #
    # _today_scored above re-scores EVERY game on the schedule, finished ones
    # included, at page-render time. generate_picks then produces a pick set
    # from that. At 11pm the model re-picks a game that ended at 10pm using
    # confirmed lineups and closing lines the 6am model never had, and the Daily
    # Summary grades THAT. It is not lookahead at results, but it is a
    # better-informed model than the one that actually published, so the record
    # comes out flattered. 13-6 vs the real 11-10.
    #
    # The `picks` table holds what was really published: rescoring stops at first
    # pitch, so each row settles on the last pre-game pick — the one that was
    # actually available to bet. Restrict to those. Picks the re-score no longer
    # produces are logged rather than silently dropped.
    try:
        from db.picks_store import get_picks as _get_published
        _published = _get_published(actual_date) or []
    except Exception as _pe:
        log.warning(f"Published-pick lookup failed, summary falls back to "
                    f"re-scored set (record may be optimistic): {_pe}")
        _published = []

    if _published:
        _pub_label = {(r.get("game", ""), r.get("pick_type", "")): (r.get("label", "") or "")
                      for r in _published}
        _kept, _matched = [], set()
        for _p in today_picks_all:
            _k = (_p.get("game", ""), _p.get("type", ""))
            if _k in _pub_label:
                _q = dict(_p)
                # The DB label is authoritative: it is the side that was live
                # pre-game. A re-score may have changed its mind since.
                _q["label"] = _pub_label[_k] or _q.get("label", "")
                _kept.append(_q)
                _matched.add(_k)
        _missing = [k for k in _pub_label if k not in _matched]
        log.info(f"Daily Summary pick set: {len(_kept)} published pick(s) kept, "
                 f"{len(today_picks_all) - len(_kept)} re-score artefact(s) dropped"
                 + (f", {len(_missing)} published pick(s) not regenerated" if _missing else ""))
        today_picks_all = _kept

    parlays_2        = build_parlays(picks, legs=2, max_parlays=5)
    parlays_3        = build_parlays(picks, legs=3, max_parlays=5)
    thematic_parlays = build_thematic_parlays(picks)

    # Tomorrow picks and parlays
    picks_next          = generate_picks(scored_next)
    parlays_2_next      = build_parlays(picks_next, legs=2, max_parlays=5)
    parlays_3_next      = build_parlays(picks_next, legs=3, max_parlays=5)
    thematic_next       = build_thematic_parlays(picks_next)

    # Load standings for team records
    standings = load_standings()

    # Build schedule view (all games today, not just unstarted)
    schedule_games = all_schedule if all_schedule else scored
    schedule_json  = json.dumps(prep_schedule_view(schedule_games, today_scores, standings))

    # ── Refresh lineups + hitter stats before props ───────────────────────────
    # Lineups refresh every run. Hitter stats only re-fetch if the file is
    # missing or older than 2 hours — avoids the slow 270-player API call on
    # every 10-minute cache refresh (the file persists within a deployment).
    try:
        from scrapers.mlb_lineup_scraper import run as run_lineups
        lineups = run_lineups(target_date=actual_date)
        confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
        log.info(f"Lineup refresh: {len(lineups)} games, {confirmed} confirmed")
        if confirmed > 0:
            import time as _time
            _stats_path = os.path.join(os.path.dirname(__file__), "data", "raw",
                                       f"mlb_hitter_stats_{actual_date}.json")
            _stats_age  = (_time.time() - os.path.getmtime(_stats_path)
                           if os.path.exists(_stats_path) else float("inf"))
            if _stats_age > 4 * 3600:   # Re-fetch if missing or older than 4 hours
                from scrapers.mlb_hitter_scraper import run as run_hitters
                run_hitters(target_date=actual_date)
                log.info("Hitter stats refreshed for props")
            else:
                log.info(f"Hitter stats are {int(_stats_age/60)}m old — skipping re-fetch")
    except Exception as e:
        log.warning(f"Lineup refresh failed (non-fatal): {e}")

    # Projected lineups (loaded early — needed for projected props below)
    proj_lineups_data = load_projected_lineups(actual_date)

    # Player props (non-fatal — works only when lineups confirmed)
    try:
        props = score_all_props(actual_date)
        log.info(f"Props generated: {len(props)} confirmed props")
    except Exception as e:
        log.warning(f"Props generation failed (non-fatal): {e}")
        props = []

    # Projected props — fill in any teams that don't have confirmed props yet
    try:
        from model.mlb_props_model import score_projected_props
        # Find teams that already have confirmed props
        confirmed_games = {(p["away_team"], p["home_team"]) for p in props}
        # Only run projected scoring if we have projected lineups AND some games are uncovered
        if proj_lineups_data:
            projected = score_projected_props(proj_lineups_data, actual_date)
            # Merge: keep projected props only for games without confirmed props
            added = 0
            for p in projected:
                key = (p.get("away_team", ""), p.get("home_team", ""))
                if key not in confirmed_games:
                    props.append(p)
                    added += 1
            if added:
                log.info(f"Projected props added: {added} props for {len(set((p['away_team'],p['home_team']) for p in props if p.get('projected')))} games")
    except Exception as e:
        log.warning(f"Projected props generation failed (non-fatal): {e}")

    props_json = json.dumps(prep_props(props))
    log.info(f"Props total: {len(props)} ({sum(1 for p in props if not p.get('projected'))} confirmed, {sum(1 for p in props if p.get('projected'))} projected)")

    # ── HR Watch + Monte Carlo sections ──────────────────────────────────
    try:
        from model.mlb_analysis_sections import build_hr_watch, build_monte_carlo
        hr_watch_html    = build_hr_watch(props, picks)
        monte_carlo_html = build_monte_carlo(picks)
    except Exception as _sec_e:
        log.warning(f"Analysis sections failed (non-fatal): {_sec_e}")
        hr_watch_html    = "<p>HR Watch unavailable.</p>"
        monte_carlo_html = "<p>Monte Carlo unavailable.</p>"

    # Kalshi market data (optional — works only when API key configured)
    kalshi_data = load_kalshi(actual_date)
    if not kalshi_data:
        # Try to run Kalshi scraper on-the-fly if key is available
        try:
            from scrapers.mlb_kalshi_scraper import run as run_kalshi
            kalshi_data = run_kalshi(target_date=actual_date)
        except Exception:
            pass
    # Guard: scraper run() returns a result string, not a dict — normalize to dict
    if not isinstance(kalshi_data, dict):
        kalshi_data = {}

    movement_data = load_line_movement(actual_date)
    movement_json = json.dumps(movement_data)

    # Yesterday's analysis panel
    yesterday_data = load_yesterday_analysis(actual_date)
    # Attach yesterday's prop results so banner can show props W-L
    try:
        from datetime import datetime as _dt2, timedelta as _td2
        _yday_str = (_dt2.strptime(actual_date, "%Y-%m-%d") - _td2(days=1)).strftime("%Y-%m-%d")
        # NOTE: must use the db_conn() context manager. Calling get_conn() and
        # then conn.close() leaks the pool slot permanently (psycopg2 keeps it
        # marked in-use), which exhausts maxconn=5 after ~5 dashboard rebuilds.
        from db.connection import db_conn as _dbc2
        with _dbc2() as _conn2:
            if _conn2:
                with _conn2.cursor() as _cur2:
                    # player_prop_history columns are game_date and result (TEXT),
                    # not prop_date/hit. The old names raised
                    # 'column "hit" does not exist' on every single run, so this
                    # panel has never populated. result is 'WIN'/'LOSS'/'PUSH',
                    # so it must be compared, not treated as a boolean.
                    _cur2.execute(
                        "SELECT player_name, prop_type, result, "
                        "COALESCE(pick_side,'OVER') FROM player_prop_history "
                        "WHERE game_date = %s AND result IS NOT NULL "
                        "ORDER BY player_name",
                        (_yday_str,)
                    )
                    _prop_rows = _cur2.fetchall()
                # WIN = outcome direction (r[2]=OVER/UNDER/PUSH) matches the
                # picked side (r[3]). PUSH is neither win nor loss.
                def _phit(r): return r[2] == r[3]
                _pw = sum(1 for r in _prop_rows if _phit(r))
                _pl = sum(1 for r in _prop_rows if r[2] != r[3] and r[2] != "PUSH")
                _top = [{"player": r[0], "prop_type": r[1], "hit": _phit(r)}
                        for r in _prop_rows[:6]]
                yesterday_data["props_yesterday"] = {"wins": _pw, "losses": _pl, "top": _top}
    except Exception as _pe:
        log.debug(f"Yesterday props query failed: {_pe}")
    # default=str so a stray Decimal/date from the DB can never break the whole
    # dashboard render again (it silently degrades that one value to a string).
    yesterday_json = json.dumps(yesterday_data, default=str)

    # Serialize projected lineups (loaded earlier for props; reuse here for JS injection)
    proj_lineups_json = json.dumps(proj_lineups_data)

    # Tomorrow schedule view (no live scores needed — future games)
    schedule_next_games = all_schedule_next if all_schedule_next else scored_next
    schedule_next_json  = json.dumps(prep_schedule_view(schedule_next_games, [], standings))

    # Stats & Trends — 21-day graded trends for the visual bar charts.
    trends_data = {}
    try:
        from analysis_report import build_data_pack as _bdp
        _tp = _bdp(_today_et())
        if not _tp.get("error"):
            trends_data = {
                "conf_band": _tp.get("trend_by_conf_band", []),
                "by_tier":   _tp.get("trend_by_tier", []),
                "market":    _tp.get("trend_market_signal", []),
                "props":     _tp.get("trend_props", []),
            }
    except Exception as _te:
        log.debug(f"Trends pack failed: {_te}")
    trends_json = json.dumps(trends_data, default=str)

    # Serialize all data for HTML template injection
    high_conf_json  = json.dumps(compute_high_conf_rule())
    picks_json      = json.dumps(prep_picks(picks, kalshi_data=kalshi_data))
    today_picks_json = json.dumps(prep_picks(today_picks_all, kalshi_data={}))
    games_json      = json.dumps(prep_games(scored))
    p2_json         = json.dumps(prep_parlays(parlays_2))
    p3_json         = json.dumps(prep_parlays(parlays_3))
    thematic_json   = json.dumps(prep_thematic_parlays(thematic_parlays))
    scores_json     = json.dumps(prep_scores_ticker(today_scores))
    team_sched_json = json.dumps(prep_team_schedule(actual_date))
    # Tomorrow data
    picks_next_json = json.dumps(prep_picks(picks_next, kalshi_data={}))
    p2_next_json    = json.dumps(prep_parlays(parlays_2_next))
    p3_next_json    = json.dumps(prep_parlays(parlays_3_next))
    thematic_next_json = json.dumps(prep_thematic_parlays(thematic_next))

    html = (HTML
            .replace("__DATE__",         actual_date)
            .replace("__HIGHCONF__",     high_conf_json)
            .replace("__TRENDS__",       trends_json)
            .replace("__TODAY_PICKS__",  today_picks_json)
            .replace("__PICKS__",        picks_json)
            .replace("__GAMES__",        games_json)
            .replace("__P2__",           p2_json)
            .replace("__THEMATIC__",      thematic_json)
            .replace("__THEMATIC_NEXT__", thematic_next_json)
            .replace("__P3__",           p3_json)
            .replace("__SCORES__",       scores_json)
            .replace("__PROPS__",        props_json)
            .replace("__SCHEDULE__",     schedule_json)
            .replace("__YESTERDAY__",    yesterday_json)
            .replace("__MOVEMENT__",     movement_json)
            .replace("__PROJ_LINEUPS__", proj_lineups_json)
            .replace("__TEAM_SCHED__",   team_sched_json)
            .replace("__NEXT_DATE__",     tomorrow_str)
            .replace("__PICKS_NEXT__",    picks_next_json)
            .replace("__P2_NEXT__",       p2_next_json)
            .replace("__P3_NEXT__",       p3_next_json)
            .replace("__SCHEDULE_NEXT__", schedule_next_json)
            .replace("__HR_WATCH__",      hr_watch_html)
            .replace("__MONTE_CARLO__",   monte_carlo_html))

    os.makedirs(PICKS_DIR, exist_ok=True)
    out_path = os.path.join(PICKS_DIR, f"mlb_picks_{actual_date}.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Always write a fixed-name copy so bookmarks never break
    latest_path = os.path.join(PICKS_DIR, "mlb_picks_latest.html")
    with open(latest_path, "w", encoding="utf-8") as f:
        f.write(html)

    # Write CSV of picks so run_analysis.py can grade them tomorrow
    import csv as _csv
    csv_path = os.path.join(PICKS_DIR, f"mlb_picks_{actual_date}.csv")
    csv_fields = ["date", "game", "type", "label", "conf", "tier", "reasoning"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = _csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        for p in picks:
            writer.writerow({
                "date":      actual_date,
                "game":      p.get("game", ""),
                "type":      p.get("type", ""),
                "label":     p.get("label", ""),
                "conf":      p.get("conf", ""),
                "tier":      p.get("tier", ""),
                "reasoning": p.get("reasoning", ""),
            })

    # Props CSV for the consensus worker (non-fatal)
    props_csv_path = os.path.join(PICKS_DIR, f"mlb_props_{actual_date}.csv")
    try:
        prop_fields = ["date", "game", "away_team", "home_team", "prop_type",
                       "player_name", "line", "proj", "confidence", "tier", "reasoning"]
        with open(props_csv_path, "w", newline="", encoding="utf-8") as pf:
            pwriter = _csv.DictWriter(pf, fieldnames=prop_fields, extrasaction="ignore")
            pwriter.writeheader()
            for pr in (props or []):
                prow = {k: pr.get(k, "") for k in prop_fields}
                prow["date"] = actual_date
                pwriter.writerow(prow)
        log.info(f"Props CSV: {props_csv_path} ({len(props or [])} rows)")
    except Exception as _pe:
        log.warning(f"Props CSV write failed (non-fatal): {_pe}")

    log.info(f"Dashboard saved: {out_path}")
    log.info(f"Picks CSV: {csv_path} ({len(picks)} rows)")
    log.info(f"Latest copy: {latest_path}")

    # Upload the picks HTML to R2 so it survives Railway restarts.
    # Non-fatal — pipeline continues even if storage is unavailable.
    try:
        from db.csv_sync import upload_file as _upload_file
        _upload_file(out_path,    f"picks/mlb_picks_{actual_date}.html")
        _upload_file(latest_path, "picks/mlb_picks_latest.html")
        _upload_file(csv_path,    f"picks/mlb_picks_{actual_date}.csv")
        _upload_file(props_csv_path, f"picks/mlb_props_{actual_date}.csv")
        log.info(f"Picks HTML + CSV uploaded to R2 (picks/mlb_picks_{actual_date}.html/.csv)")
    except Exception as _e:
        log.warning(f"R2 HTML upload failed (non-fatal): {_e}")

    # Upsert picks to DB so it always mirrors what's displayed on the dashboard.
    # This runs on EVERY regeneration (6am pipeline AND afternoon refresh) so that
    # if the afternoon refresh changes which team is picked for a game, the DB
    # label updates too — keeping it in sync with the CSV that run_analysis.py grades.
    # actual_result (the grade) is preserved by the ON CONFLICT clause in save_picks().
    try:
        from db.picks_store import save_picks as _db_save_picks
        _db_save_picks(picks, actual_date)
    except Exception as _dbe:
        log.warning(f"DB picks upsert failed (non-fatal): {_dbe}")

    log.info(f"{len(scored)} games | {len(picks)} picks | "
             f"{len(parlays_2)} 2-leg parlays | {len(parlays_3)} 3-leg parlays")

    # Alert if a full slate scored very few games — means upstream data broke
    try:
        from db.csv_sync import storage_available as _storage_ok
        _on_railway = _storage_ok()
    except Exception:
        _on_railway = False
    if _on_railway and len(scored) < 5 and len(picks) < 8:
        try:
            from alerts import send_alert
            send_alert(
                f"Low pick count: only {len(scored)} games scored ({len(picks)} picks)",
                f"Expected 10+ games on a full slate but only {len(scored)} scored.\n"
                "This usually means the schedule scraper, normalizer, or model data broke.\n"
                "Check Railway logs for errors in Steps 1-2 of the pipeline.",
            )
        except Exception:
            pass

    if not no_open:
        import webbrowser as _wb
        _wb.open(f"file:///{os.path.abspath(latest_path)}")
        log.info("Opening in browser...")

    return html


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MLB Picks HTML Dashboard")
    parser.add_argument("--date", default=None, help="Date to generate (YYYY-MM-DD)")
    parser.add_argument("--no-open", action="store_true", help="Don't open browser")
    args, _ = parser.parse_known_args()
    main(date=args.date, no_open=args.no_open)

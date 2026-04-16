"""
model/mlb_props_model.py
Player prop probability engine for MLB betting.

Prop types supported:
  HR   — player hits 0.5+ home runs (standard DraftKings / FanDuel line)
  HITS — player records 0.5+ hits  (standard line)
  K    — starting pitcher strikeout total (over/under a given number)

Confidence scoring:
  Each prop returns a dict with:
    player_name, prop_type, line, proj, confidence (0-1),
    tier (LOCK/STRONG/LEAN), reasoning

Tier thresholds (same scale as game picks):
  LOCK   ≥ 68%
  STRONG 62-68%
  LEAN   55-62%
  SKIP   < 55%
"""

import os, json, math, logging, csv
from datetime import datetime

log = logging.getLogger(__name__)

SEASON   = datetime.now().year
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# League-average baselines (2024 MLB)
LEAGUE_HR_PER_PA  = 0.034   # ~1 HR per 29 PA
LEAGUE_H_PER_PA   = 0.270   # batting average proxy
LEAGUE_K9_SP      = 9.1     # average SP K/9

# Park HR factors from park_factors.csv (loaded on demand)
_PARK_HR_FACTORS: dict[str, float] = {}

# Tier cutoffs
LOCK_THRESH   = 0.68
STRONG_THRESH = 0.62
LEAN_THRESH   = 0.55


def _load_park_factors():
    global _PARK_HR_FACTORS
    if _PARK_HR_FACTORS:
        return
    path = os.path.join(DATA_DIR, "park_factors.csv")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            team  = row.get("team", "").strip()
            try:
                hr_f  = float(row.get("hr_factor", 1.0) or 1.0)
            except ValueError:
                hr_f  = 1.0
            _PARK_HR_FACTORS[team] = hr_f


def _tier(conf: float) -> str:
    if conf >= LOCK_THRESH:   return "LOCK"
    if conf >= STRONG_THRESH: return "STRONG"
    if conf >= LEAN_THRESH:   return "LEAN"
    return "SKIP"


# ─────────────────────────────────────────────────────────────────────────────
# HOME RUN PROPS
# ─────────────────────────────────────────────────────────────────────────────

def _poisson_p_at_least_one(lam: float) -> float:
    """P(X >= 1) for Poisson with mean lam."""
    if lam <= 0:
        return 0.0
    return 1.0 - math.exp(-lam)


def score_hr_prop(player: dict, pitcher_opp: dict, home_team: str,
                  is_home: bool, weather: dict = None) -> dict | None:
    """
    Score a home run prop for one player.
    Uses a Poisson model:
        lambda = PA_expected × adj_hr_per_pa

    Adjustments applied:
        1. Pitcher HR/9 vs league average (pitcher factor)
        2. Park HR factor
        3. Wind component (WIND_OUT +12%, WIND_IN -10%)
        4. Home/away split if available
    """
    _load_park_factors()

    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None   # insufficient sample

    # Base rate — prefer platoon-adjusted if we know pitcher hand
    # For now use season rate; platoon can be added when pitcher hand is stored
    base_rate = player.get("hr_per_pa", LEAGUE_HR_PER_PA)

    # Home/away adjustment
    if is_home:
        ha_rate = player.get("home_hr_per_pa", base_rate)
    else:
        ha_rate = player.get("away_hr_per_pa", base_rate)
    # Blend 60/40 season vs split
    hr_rate = 0.60 * base_rate + 0.40 * ha_rate

    # Pitcher factor: pitcher's HR/9 vs league avg HR/9 (≈1.20)
    pitcher_hr9    = pitcher_opp.get("hr_per_9", 1.20)
    pitcher_factor = pitcher_hr9 / 1.20  # >1 = gives up more HRs
    pitcher_factor = max(0.5, min(2.0, pitcher_factor))

    # Park factor
    park_factor = _PARK_HR_FACTORS.get(home_team, 1.0)

    # Weather
    wind_factor = 1.0
    wind_note   = ""
    if weather:
        flag = weather.get("weather_flag", "NORMAL")
        if flag == "WIND_OUT":
            wind_factor = 1.12
            wind_note   = "Wind blowing out"
        elif flag == "WIND_IN":
            wind_factor = 0.90
            wind_note   = "Wind blowing in"
        elif flag == "COLD":
            wind_factor = 0.95
            wind_note   = "Cold temps suppress HRs"

    # Expected PA per game (lineup position matters — top 3 get ~4.2 PA, 7-9 get ~3.5 PA)
    batting_order = player.get("batting_order", 5)
    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = hr_rate * pitcher_factor * park_factor * wind_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_one(lam)

    # Build reasoning
    parts = []
    if pitcher_factor >= 1.15:
        parts.append(f"pitcher allows {pitcher_hr9:.2f} HR/9 (above avg)")
    elif pitcher_factor <= 0.85:
        parts.append(f"pitcher suppresses HRs ({pitcher_hr9:.2f} HR/9)")
    if park_factor >= 1.10:
        parts.append(f"hitter-friendly park (HR factor {park_factor:.2f})")
    elif park_factor <= 0.90:
        parts.append(f"pitcher park (HR factor {park_factor:.2f})")
    if wind_note:
        parts.append(wind_note)
    parts.append(f"{pa} PA sample this season ({base_rate:.3f} HR/PA)")

    reasoning = " | ".join(parts) if parts else f"{pa} PA, {base_rate:.3f} HR/PA"

    tier = _tier(prob)
    if tier == "SKIP":
        return None

    return {
        "prop_type":   "HR",
        "line":        0.5,
        "player_name": pname,
        "batting_order": batting_order,
        "proj":        round(lam, 3),
        "confidence":  round(prob, 4),
        "tier":        tier,
        "reasoning":   reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# HITS PROPS
# ─────────────────────────────────────────────────────────────────────────────

def score_hits_prop(player: dict, pitcher_opp: dict,
                    is_home: bool) -> dict | None:
    """
    Score a 0.5 hits prop for one hitter.
    Poisson model: lambda = PA_expected × adj_h_per_pa
    """
    pname = player.get("player_name", "Unknown")
    pa    = player.get("pa", 0)
    if pa < 20:
        return None

    base_rate = player.get("h_per_pa", LEAGUE_H_PER_PA)

    # Home/away split blend
    if is_home:
        ha_rate = player.get("home_h_per_pa", base_rate)
    else:
        ha_rate = player.get("away_h_per_pa", base_rate)
    h_rate = 0.60 * base_rate + 0.40 * ha_rate

    # Pitcher factor: pitcher's H/9 vs league avg H/9 (≈8.5)
    pitcher_h9     = pitcher_opp.get("h_per_9", 8.5)
    pitcher_factor = pitcher_h9 / 8.5
    pitcher_factor = max(0.6, min(1.6, pitcher_factor))

    # Expected PA
    batting_order = player.get("batting_order", 5)
    exp_pa = max(3.3, 4.5 - (batting_order - 1) * 0.12)

    adj_rate = h_rate * pitcher_factor
    lam      = exp_pa * adj_rate
    prob     = _poisson_p_at_least_one(lam)

    # Reasoning
    parts = []
    if base_rate >= 0.290:
        parts.append(f"high contact hitter ({base_rate:.3f} H/PA)")
    elif base_rate <= 0.230:
        parts.append(f"low contact hitter ({base_rate:.3f} H/PA)")
    if pitcher_factor >= 1.12:
        parts.append(f"pitcher allows lots of hits ({pitcher_h9:.1f} H/9)")
    elif pitcher_factor <= 0.88:
        parts.append(f"stingy pitcher ({pitcher_h9:.1f} H/9)")
    pitcher_avg = pitcher_opp.get("opp_avg", 0)
    if pitcher_avg:
        parts.append(f"opp BA .{int(pitcher_avg*1000):03d}")
    if not parts:
        parts.append(f"{pa} PA, {base_rate:.3f} H/PA")

    reasoning = " | ".join(parts)
    tier      = _tier(prob)
    if tier == "SKIP":
        return None

    return {
        "prop_type":   "HITS",
        "line":        0.5,
        "player_name": pname,
        "batting_order": batting_order,
        "proj":        round(lam, 3),
        "confidence":  round(prob, 4),
        "tier":        tier,
        "reasoning":   reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PITCHER STRIKEOUT PROPS
# ─────────────────────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Approximate standard normal CDF."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def score_k_prop(pitcher_name: str, pitcher_stats: dict,
                 opp_team_k_rate: float,
                 innings_expected: float = 5.5,
                 line: float = 5.5,
                 weather: dict = None) -> dict | None:
    """
    Score a pitcher strikeout over/under prop.

    Model:
        proj_k = (pitcher_k9 / 9) × innings_expected × team_k_rate_adj

    Uses normal distribution around projection to compute P(K > line).
    """
    k9 = float(pitcher_stats.get("k9", pitcher_stats.get("strikeoutsPer9Inn", 0)) or 0)
    if k9 < 1.0:
        return None   # no data

    # Opponent team K rate adjustment vs league average (≈22%)
    league_k_rate  = 0.220
    opp_k_factor   = opp_team_k_rate / league_k_rate if opp_team_k_rate > 0 else 1.0
    opp_k_factor   = max(0.7, min(1.4, opp_k_factor))

    # Weather: COLD reduces K rate slightly
    weather_factor = 1.0
    if weather and weather.get("weather_flag") == "COLD":
        weather_factor = 0.97

    proj_k_per_9 = k9 * opp_k_factor * weather_factor
    proj_k       = (proj_k_per_9 / 9.0) * innings_expected

    # Std dev for K props — roughly sqrt(proj_k) × 1.1
    std_dev = max(1.2, math.sqrt(proj_k) * 1.1)

    # P(K > line) using normal approximation, continuity correction
    z    = (line + 0.5 - proj_k) / std_dev
    prob = 1.0 - _norm_cdf(z)   # P(K > line)

    tier = _tier(prob)
    if tier == "SKIP":
        return None

    # Reasoning
    parts = []
    if k9 >= 10.5:
        parts.append(f"elite swing-miss stuff ({k9:.1f} K/9)")
    elif k9 >= 8.5:
        parts.append(f"above-avg strikeout rate ({k9:.1f} K/9)")
    else:
        parts.append(f"{k9:.1f} K/9 this season")
    if opp_k_factor >= 1.10:
        parts.append(f"opponent strikes out a lot ({opp_team_k_rate:.1%})")
    elif opp_k_factor <= 0.90:
        parts.append(f"opponent makes contact ({opp_team_k_rate:.1%} K rate)")
    parts.append(f"proj {proj_k:.1f} Ks in {innings_expected} inn vs line {line}")

    reasoning = " | ".join(parts)

    return {
        "prop_type":   "K",
        "line":        line,
        "player_name": pitcher_name,
        "proj":        round(proj_k, 2),
        "confidence":  round(prob, 4),
        "tier":        tier,
        "reasoning":   reasoning,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCORER — combines everything for a date
# ─────────────────────────────────────────────────────────────────────────────

def score_all_props(target_date: str = None) -> list[dict]:
    """
    Load today's hitter stats + lineup data + pitcher stats,
    score all supported props, and return a ranked list.

    Each result dict:
      game, away_team, home_team, prop_type, line, player_name,
      batting_order, proj, confidence, tier, reasoning
    """
    today    = target_date or datetime.now().strftime("%Y-%m-%d")
    raw_path = os.path.join(DATA_DIR, "raw", f"mlb_hitter_stats_{today}.json")

    if not os.path.exists(raw_path):
        log.warning(f"No hitter stats file for {today} — run mlb_hitter_scraper.py first")
        return []

    with open(raw_path, encoding="utf-8") as f:
        data = json.load(f)

    games        = data.get("hitters", [])
    pitcher_opps = {p["pitcher_id"]: p for p in data.get("pitcher_opp", [])
                    if "pitcher_id" in p}

    # Load pitcher stats master for K/9, and team k_rate from team stats master
    pitcher_stats: dict[str, dict] = {}
    ps_path = os.path.join(DATA_DIR, "clean", "mlb_pitcher_stats_master.csv")
    if os.path.exists(ps_path):
        with open(ps_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pname = row.get("player_name", "").strip()
                if pname:
                    pitcher_stats[pname] = row

    # Team K rate from team stats master
    team_k_rate: dict[str, float] = {}
    ts_path = os.path.join(DATA_DIR, "clean", "mlb_team_stats_master.csv")
    if os.path.exists(ts_path):
        with open(ts_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                tname = row.get("team_name", "").strip()
                try:
                    # strikeout_rate column if available, else estimate from SO and PA
                    kr = float(row.get("strikeout_rate", 0) or 0)
                    if kr == 0:
                        so = float(row.get("so", 0) or 0)
                        pa = float(row.get("pa", 1) or 1)
                        kr = so / pa if pa > 0 else 0.220
                    team_k_rate[tname] = kr
                except (ValueError, ZeroDivisionError):
                    team_k_rate[tname] = 0.220

    # Load weather
    weather_data: dict[int, dict] = {}
    w_path = os.path.join(DATA_DIR, "clean", "mlb_weather_master.csv")
    if os.path.exists(w_path):
        with open(w_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("game_date") == today:
                    try:
                        gid = int(row.get("game_id", 0))
                        weather_data[gid] = row
                    except (ValueError, TypeError):
                        pass

    # Load today's schedule for probable pitcher IDs
    sched_path = os.path.join(DATA_DIR, "clean", "mlb_schedule_master.csv")
    game_pitchers: dict[int, dict] = {}   # game_id -> {away_pid, home_pid, away_sp, home_sp}
    if os.path.exists(sched_path):
        with open(sched_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("game_date") == today:
                    try:
                        gid = int(row.get("game_id", 0))
                        game_pitchers[gid] = {
                            "away_sp":  row.get("away_probable_pitcher", "TBD"),
                            "home_sp":  row.get("home_probable_pitcher", "TBD"),
                            "away_pid": row.get("away_probable_pitcher_id", ""),
                            "home_pid": row.get("home_probable_pitcher_id", ""),
                        }
                    except (ValueError, TypeError):
                        pass

    all_props = []

    for game in games:
        if not game.get("lineup_confirmed"):
            continue

        game_id   = game.get("game_id", 0)
        away_team = game.get("away_team", "")
        home_team = game.get("home_team", "")
        game_str  = f"{away_team} @ {home_team}"
        weather   = weather_data.get(game_id)

        gp = game_pitchers.get(game_id, {})
        away_sp  = gp.get("away_sp",  "TBD")
        home_sp  = gp.get("home_sp",  "TBD")

        # Pitcher opp stats by pitcher_id
        def get_pitcher_opp(pid_str):
            try:
                pid = int(pid_str)
                return pitcher_opps.get(pid, {})
            except (ValueError, TypeError):
                return {}

        away_pitcher_opp = get_pitcher_opp(gp.get("away_pid", ""))
        home_pitcher_opp = get_pitcher_opp(gp.get("home_pid", ""))

        # ── Hitter props (away batters face home SP, home batters face away SP)
        for player in game.get("away_lineup", []):
            hr_prop   = score_hr_prop(player, home_pitcher_opp, home_team,
                                      is_home=False, weather=weather)
            hits_prop = score_hits_prop(player, home_pitcher_opp, is_home=False)
            for prop in (hr_prop, hits_prop):
                if prop:
                    all_props.append({
                        "game":      game_str,
                        "game_id":   game_id,
                        "away_team": away_team,
                        "home_team": home_team,
                        "side":      "away",
                        **prop,
                    })

        for player in game.get("home_lineup", []):
            hr_prop   = score_hr_prop(player, away_pitcher_opp, home_team,
                                      is_home=True, weather=weather)
            hits_prop = score_hits_prop(player, away_pitcher_opp, is_home=True)
            for prop in (hr_prop, hits_prop):
                if prop:
                    all_props.append({
                        "game":      game_str,
                        "game_id":   game_id,
                        "away_team": away_team,
                        "home_team": home_team,
                        "side":      "home",
                        **prop,
                    })

        # ── Pitcher K props ──────────────────────────────────────────────────
        for sp_name, opp_team in ((away_sp, home_team), (home_sp, away_team)):
            if not sp_name or sp_name == "TBD":
                continue
            sp_row   = pitcher_stats.get(sp_name, {})
            if not sp_row:
                continue
            opp_kr   = team_k_rate.get(opp_team, 0.220)

            # Determine a reasonable line — use projection to set market-like line
            k9  = float(sp_row.get("k9", 0) or 0)
            exp = (k9 / 9.0) * 5.5
            # Round to nearest half for realistic line
            line = round(exp * 2) / 2

            k_prop = score_k_prop(
                pitcher_name=sp_name,
                pitcher_stats=sp_row,
                opp_team_k_rate=opp_kr,
                innings_expected=5.5,
                line=line,
                weather=weather,
            )
            if k_prop:
                all_props.append({
                    "game":      game_str,
                    "game_id":   game_id,
                    "away_team": away_team,
                    "home_team": home_team,
                    "side":      "pitcher",
                    **k_prop,
                })

    # Sort by confidence descending
    all_props.sort(key=lambda x: x["confidence"], reverse=True)
    log.info(f"Props scored: {len(all_props)} total | "
             f"{sum(1 for p in all_props if p['tier']=='LOCK')} LOCKs | "
             f"{sum(1 for p in all_props if p['tier']=='STRONG')} STRONGs")
    return all_props


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    props = score_all_props()
    for p in props[:10]:
        print(f"[{p['tier']:6s}] {p['prop_type']:4s} {p['player_name']:25s} "
              f"{p['confidence']*100:.1f}%  proj={p['proj']}  line={p['line']}  {p['game']}")

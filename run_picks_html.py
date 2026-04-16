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


def prep_picks(picks):
    out = []
    for p in picks:
        gd = p.get("game_data", {})
        out.append({
            "type":      p["type"],
            "label":     p["label"],
            "team":      p["team"],
            "conf":      round(p["conf"] * 100, 1),
            "tier":      p["tier"],
            "game":      p["game"],
            "game_id":   p["game_id"],
            "venue":     p["venue"],
            "reasoning": p["reasoning"],
            "exp_total": p["exp_total"],
            "away":      gd.get("away_team", ""),
            "home":      gd.get("home_team", ""),
        })
    return out


def prep_games(scored):
    out = []
    for g in scored:
        t = g.get("game_time_utc", "")
        time_str = t[11:16] + " UTC" if t else ""
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


def prep_props(props: list) -> list:
    """Serialize player props for HTML embedding."""
    out = []
    for p in props:
        out.append({
            "prop_type":    p["prop_type"],
            "player_name":  p["player_name"],
            "line":         p["line"],
            "proj":         p["proj"],
            "conf":         round(p["confidence"] * 100, 1),
            "tier":         p["tier"],
            "game":         p.get("game", ""),
            "game_id":      p.get("game_id", ""),
            "away_team":    p.get("away_team", ""),
            "home_team":    p.get("home_team", ""),
            "side":         p.get("side", ""),
            "batting_order":p.get("batting_order", ""),
            "reasoning":    p.get("reasoning", ""),
        })
    return out


def prep_parlays(parlays):
    out = []
    for p in parlays:
        out.append({
            "n_legs":   p["n_legs"],
            "combined": round(p["combined"] * 100, 1),
            "payout":   p["payout"],
            "legs": [
                {"label": l["label"], "conf": round(l["conf"] * 100, 1),
                 "tier": l["tier"], "game": l["game"]}
                for l in p["legs"]
            ],
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# HTML TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
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

/* ── PICKS GRID ── */
.picks-grid{
  display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
  gap:14px;margin-bottom:36px;
}
.pick-card{
  background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;cursor:default;transition:transform .15s,border-color .15s;
  position:relative;overflow:hidden;
}
.pick-card:hover{transform:translateY(-2px)}
.pick-card.tier-LOCK  {border-top:3px solid var(--gold)}
.pick-card.tier-STRONG{border-top:3px solid var(--blue)}
.pick-card.tier-LEAN  {border-top:3px solid var(--green)}
.pick-card.hidden{display:none}

.pick-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
.pick-type-badge{
  font-size:.68rem;font-weight:700;letter-spacing:.8px;padding:3px 9px;border-radius:4px;
  text-transform:uppercase;
}
.badge-ML   {background:rgba(66,165,245,.15);color:#42a5f5;border:1px solid rgba(66,165,245,.3)}
.badge-RL   {background:rgba(171,71,188,.15);color:#ce93d8;border:1px solid rgba(171,71,188,.3)}
.badge-TOTAL{background:rgba(0,230,118,.15);color:#00e676;border:1px solid rgba(0,230,118,.3)}
.tier-badge{
  font-size:.7rem;font-weight:700;padding:3px 9px;border-radius:4px;
}
.tb-LOCK  {background:rgba(255,193,7,.15);color:var(--gold);border:1px solid rgba(255,193,7,.3)}
.tb-STRONG{background:rgba(66,165,245,.15);color:var(--blue);border:1px solid rgba(66,165,245,.3)}
.tb-LEAN  {background:rgba(0,230,118,.15);color:var(--green);border:1px solid rgba(0,230,118,.3)}

.pick-label{font-size:1.08rem;font-weight:700;color:var(--text);margin-bottom:4px}
.pick-game{font-size:.78rem;color:var(--sub);margin-bottom:12px}

.conf-row{display:flex;align-items:center;gap:10px;margin-bottom:10px}
.conf-bar-wrap{flex:1;height:7px;background:rgba(255,255,255,.07);border-radius:4px;overflow:hidden}
.conf-bar{height:100%;border-radius:4px;transition:width .4s ease}
.bar-LOCK  {background:linear-gradient(90deg,#e65100,var(--gold))}
.bar-STRONG{background:linear-gradient(90deg,#0d47a1,var(--blue))}
.bar-LEAN  {background:linear-gradient(90deg,#1b5e20,var(--green))}
.conf-pct{font-size:.95rem;font-weight:700;white-space:nowrap}
.pct-LOCK  {color:var(--gold)}
.pct-STRONG{color:var(--blue)}
.pct-LEAN  {color:var(--green)}

.pick-reasoning{
  font-size:.74rem;color:var(--sub);line-height:1.5;
  border-top:1px solid var(--border);padding-top:8px;margin-top:4px;
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
.prop-player{font-size:1.05rem;font-weight:700;color:var(--text);margin-bottom:2px}
.prop-game  {font-size:.75rem;color:var(--sub);margin-bottom:10px}
.prop-line-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;
  background:rgba(255,255,255,.04);border-radius:6px;padding:6px 10px}
.prop-line-label{font-size:.72rem;color:var(--sub);font-weight:600}
.prop-line-val{font-size:.92rem;font-weight:700;color:var(--text)}
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
</style>
</head>
<body>

<div class="header">
  <h1>⚾ Sports Betting Parlay Genius</h1>
  <div class="header-sub" id="dateStr"></div>
  <div class="header-stats">
    <div class="stat-pill">Games <span id="gameCount">—</span></div>
    <div class="stat-pill">Picks <span id="pickCount">—</span></div>
    <div class="stat-pill">Locks <span id="lockCount">—</span></div>
    <div class="stat-pill">Top Pick <span id="topPick">—</span></div>
    <button id="refreshBtn" onclick="doRefresh()">
      <svg id="refreshIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
      </svg>
      Refresh Picks
    </button>
  </div>
  <div id="refreshStatus"></div>
</div>

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
    <button class="filter-btn" data-group="type" data-val="ML">Moneyline</button>
    <button class="filter-btn" data-group="type" data-val="RL">Run Line</button>
    <button class="filter-btn" data-group="type" data-val="TOTAL">Over/Under</button>
  </div>
  <div class="filter-group">
    <span class="filter-label">Confidence</span>
    <button class="filter-btn active" data-group="tier" data-val="all">All</button>
    <button class="filter-btn" data-group="tier" data-val="LOCK">🔒 Lock</button>
    <button class="filter-btn" data-group="tier" data-val="STRONG">⭐⭐ Strong</button>
    <button class="filter-btn" data-group="tier" data-val="LEAN">⭐ Lean</button>
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
    <button class="section-nav-btn" data-panel="panel-games">📊 Game Breakdown</button>
  </div>

  <!-- PANEL: GAME PICKS -->
  <div class="section-panel active" id="panel-picks">
    <div class="section-title">🎯 Individual Picks</div>
    <div class="results-count" id="pickResults"></div>
    <div class="picks-grid" id="picksGrid"></div>
    <div class="section-title">🔥 Parlay Recommendations</div>
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
      <button class="filter-btn" data-pgroup="ptype" data-pval="HR">⚡ Home Runs</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="HITS">🎯 Hits</button>
      <button class="filter-btn" data-pgroup="ptype" data-pval="K">🔥 Strikeouts</button>
      <span class="filter-label" style="margin-left:12px">Confidence</span>
      <button class="filter-btn active" data-pgroup="ptier" data-pval="all">All</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="LOCK">🔒 Lock</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="STRONG">⭐⭐ Strong</button>
      <button class="filter-btn" data-pgroup="ptier" data-pval="LEAN">⭐ Lean</button>
    </div>
    <div class="results-count" id="propResults"></div>
    <div class="props-grid" id="propsGrid"></div>
  </div>

  <!-- PANEL: GAME BREAKDOWN -->
  <div class="section-panel" id="panel-games">
    <div class="section-title">📊 Game Breakdown</div>
    <div class="games-grid" id="gamesGrid"></div>
  </div>

</div>

<script>
// ── Embedded Data ────────────────────────────────────────────────────────────
const DATA_DATE    = "__DATE__";
const DATA_PICKS   = __PICKS__;
const DATA_GAMES   = __GAMES__;
const DATA_P2      = __P2__;
const DATA_P3      = __P3__;
const DATA_SCORES  = __SCORES__;
const DATA_PROPS    = __PROPS__;
const DATA_SCHEDULE = __SCHEDULE__;

// ── State ────────────────────────────────────────────────────────────────────
let filterType = "all", filterTier = "all", filterTeam = "";
let showParlay = 2;

// ── Init ─────────────────────────────────────────────────────────────────────
document.getElementById("dateStr").textContent =
  new Date(DATA_DATE + "T12:00:00").toLocaleDateString("en-US",
    {weekday:"long",year:"numeric",month:"long",day:"numeric"});

document.getElementById("gameCount").textContent = DATA_GAMES.length;
document.getElementById("pickCount").textContent = DATA_PICKS.length;
document.getElementById("lockCount").textContent = DATA_PICKS.filter(p=>p.tier==="LOCK").length;
if(DATA_PICKS.length){
  document.getElementById("topPick").textContent =
    DATA_PICKS[0].label + " (" + DATA_PICKS[0].conf + "%)";
}

// ── Render Picks ─────────────────────────────────────────────────────────────
function renderPicks(){
  const grid = document.getElementById("picksGrid");
  grid.innerHTML = "";
  let visible = 0;
  DATA_PICKS.forEach(p=>{
    const show = (filterType==="all" || p.type===filterType)
              && (filterTier==="all" || p.tier===filterTier)
              && (!filterTeam || p.away.toLowerCase().includes(filterTeam)
                             || p.home.toLowerCase().includes(filterTeam)
                             || p.team.toLowerCase().includes(filterTeam));
    if(!show) return;
    visible++;
    const w = p.conf; // already %
    grid.innerHTML += `
      <div class="pick-card tier-${p.tier}" data-type="${p.type}" data-tier="${p.tier}">
        <div class="pick-top">
          <span class="pick-type-badge badge-${p.type}">${p.type==="TOTAL"?"O/U":p.type}</span>
          <span class="tier-badge tb-${p.tier}">${tierIcon(p.tier)} ${p.tier}</span>
        </div>
        <div class="pick-label">${p.label}</div>
        <div class="pick-game">${p.game}</div>
        <div class="conf-row">
          <div class="conf-bar-wrap">
            <div class="conf-bar bar-${p.tier}" style="width:${w}%"></div>
          </div>
          <span class="conf-pct pct-${p.tier}">${w}%</span>
        </div>
        <div class="pick-reasoning">${p.reasoning}</div>
      </div>`;
  });
  document.getElementById("pickResults").innerHTML =
    `Showing <b>${visible}</b> of <b>${DATA_PICKS.length}</b> picks`;
  if(visible===0) grid.innerHTML = `<div class="empty">No picks match the current filters.</div>`;
}

function tierIcon(t){ return t==="LOCK"?"🔒":t==="STRONG"?"⭐⭐":"⭐"; }

// ── Render Parlays ────────────────────────────────────────────────────────────
function renderParlays(){
  const data = showParlay===2 ? DATA_P2 : DATA_P3;
  const grid = document.getElementById("parlayGrid");
  grid.innerHTML = "";
  if(!data.length){
    grid.innerHTML=`<div class="empty">Not enough qualified legs for ${showParlay}-leg parlays today.</div>`;
    return;
  }
  data.forEach((par,i)=>{
    const legsHtml = par.legs.map(l=>`
      <div class="parlay-leg">
        <span class="leg-conf leg-${l.tier}">${l.conf}%</span>
        <div class="leg-info">
          <div class="leg-label">${l.label}</div>
          <div class="leg-game">${l.game}</div>
        </div>
      </div>`).join("");
    grid.innerHTML += `
      <div class="parlay-card">
        <div class="parlay-header">
          <span class="parlay-tag">Parlay ${i+1} &bull; ${par.n_legs} Legs</span>
          <span class="parlay-payout">${par.payout}</span>
        </div>
        <div class="parlay-conf">${par.combined}%<span>combined confidence</span></div>
        <div class="parlay-legs">${legsHtml}</div>
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

    grid.innerHTML += `
      <div class="game-card">
        <div class="game-header">
          <span class="matchup">${g.away} @ ${g.home}</span>
          <span class="game-time">${g.time}</span>
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
      </div>`;
  });
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
    return d.toLocaleTimeString("en-US", {hour:"numeric", minute:"2-digit", timeZoneName:"short"});
  } catch(e){ return utcStr; }
}

function renderSchedule(){
  const grid = document.getElementById("scheduleGrid");
  grid.innerHTML = "";
  if(!DATA_SCHEDULE || DATA_SCHEDULE.length === 0){
    grid.innerHTML = `<div class="empty">No games found for today.</div>`;
    return;
  }
  DATA_SCHEDULE.forEach(g=>{
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

    grid.innerHTML += `
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
      </div>`;
  });
}

// ── Render Props ──────────────────────────────────────────────────────────────
let propFilterType = "all", propFilterTier = "all";

function propIcon(t){ return t==="HR"?"⚡":t==="HITS"?"🎯":"🔥"; }
function propLabel(t,line){
  if(t==="HR")   return `HR Over ${line}`;
  if(t==="HITS") return `Hits Over ${line}`;
  if(t==="K")    return `Ks Over ${line}`;
  return `Over ${line}`;
}

function renderProps(){
  const grid = document.getElementById("propsGrid");
  grid.innerHTML = "";
  let visible = 0;
  DATA_PROPS.forEach(p=>{
    const show = (propFilterType==="all" || p.prop_type===propFilterType)
              && (propFilterTier==="all" || p.tier===propFilterTier);
    if(!show) return;
    visible++;
    const label    = propLabel(p.prop_type, p.line);
    const overUnder= p.proj >= p.line ? "OVER" : "UNDER";
    const projColor= overUnder==="OVER" ? "var(--green)" : "var(--blue)";
    const orderStr = p.batting_order ? ` &bull; Bats ${p.batting_order}` : "";
    const sideStr  = p.side==="pitcher" ? " (SP)" : (p.side ? ` (${p.side})` : "");
    grid.innerHTML += `
      <div class="prop-card tier-${p.tier}">
        <div class="prop-top">
          <span class="prop-type-badge badge-${p.prop_type}">${propIcon(p.prop_type)} ${p.prop_type}</span>
          <span class="tier-badge tb-${p.tier}">${tierIcon(p.tier)} ${p.tier}</span>
        </div>
        <div class="prop-player">${p.player_name}${sideStr}</div>
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
            <div class="conf-bar bar-${p.tier}" style="width:${p.conf}%"></div>
          </div>
          <span class="conf-pct pct-${p.tier}">${p.conf}%</span>
        </div>
        <div class="prop-reasoning">${p.reasoning}</div>
      </div>`;
  });
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
    if(val==="LOCK") btn.classList.add("active-gold");
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
  const track = document.getElementById("tickerTrack");
  if(!DATA_SCORES || DATA_SCORES.length === 0){
    track.innerHTML = `<span class="ticker-empty">No completed games yet today — check back later</span>`;
    return;
  }
  // Build items twice so the loop is seamless
  let html = "";
  for(let pass=0; pass<2; pass++){
    DATA_SCORES.forEach(s=>{
      const awayWon  = parseInt(s.away_score) > parseInt(s.home_score);
      const liveStyle= s.is_live ? "color:#ff6b35;animation:pulse 1.5s infinite" : "";
      const liveClass= s.is_live ? "ticker-live" : "ticker-final";
      html += `
        <div class="ticker-item">
          <span class="${awayWon?'ticker-score win':'ticker-score loss'}">${s.away_city} ${s.away_score}</span>
          <span style="color:var(--sub)">@</span>
          <span class="${!awayWon?'ticker-score win':'ticker-score loss'}">${s.home_city} ${s.home_score}</span>
          <span class="${liveClass}" style="${liveStyle}">${s.label}</span>
        </div>`;
    });
  }
  track.innerHTML = html;
  // Adjust animation speed based on number of items
  const duration = Math.max(20, DATA_SCORES.length * 5);
  track.style.animationDuration = duration + "s";
}

// ── Boot ──────────────────────────────────────────────────────────────────────
renderTicker();
renderPicks();
renderParlays();
renderSchedule();
renderGames();
renderProps();
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="MLB Picks HTML Dashboard")
    parser.add_argument("--date", default=None)
    parser.add_argument("--no-open", action="store_true", help="Don't auto-open browser")
    args = parser.parse_args()

    target = args.date or datetime.now().strftime("%Y-%m-%d")

    from model.mlb_model import MLBModel
    from model.mlb_picks import generate_picks, build_parlays
    from model.mlb_props_model import score_all_props

    model = MLBModel()
    model.load()

    # score_today filters to upcoming games only — we also need all games for the schedule tab
    all_schedule = [g for g in model.schedule if g.get("game_date") == (
        args.date or datetime.now().strftime("%Y-%m-%d")
    )]

    scored, actual_date = model.score_today(target)
    if not scored and not all_schedule:
        print(f"No games found for {target}. Run python run_pipeline.py first.")
        sys.exit(0)

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
    parlays_2 = build_parlays(picks, legs=2, max_parlays=5)
    parlays_3 = build_parlays(picks, legs=3, max_parlays=5)

    # Load standings for team records
    standings = load_standings()

    # Build schedule view (all games today, not just unstarted)
    schedule_games = all_schedule if all_schedule else scored
    schedule_json  = json.dumps(prep_schedule_view(schedule_games, today_scores, standings))

    # ── Refresh lineups + hitter stats before props ───────────────────────────
    # Run every time run_picks_html.py is called so props stay current.
    # Lineups typically confirm 1-3 hours before first pitch — this ensures
    # we always have the freshest data regardless of when the pipeline last ran.
    try:
        from scrapers.mlb_lineup_scraper import run as run_lineups
        lineups = run_lineups(target_date=actual_date)
        confirmed = sum(1 for g in lineups if g.get("lineup_confirmed"))
        log.info(f"Lineup refresh: {len(lineups)} games, {confirmed} confirmed")
        if confirmed > 0:
            from scrapers.mlb_hitter_scraper import run as run_hitters
            run_hitters(target_date=actual_date)
            log.info("Hitter stats refreshed for props")
    except Exception as e:
        log.warning(f"Lineup refresh failed (non-fatal): {e}")

    # Player props (non-fatal — works only when lineups confirmed)
    try:
        props      = score_all_props(actual_date)
        props_json = json.dumps(prep_props(props))
        log.info(f"Props generated: {len(props)} total")
    except Exception as e:
        log.warning(f"Props generation failed (non-fatal): {e}")
        props_json = "[]"

    # Serialize
    picks_json  = json.dumps(prep_picks(picks))
    games_json  = json.dumps(prep_games(scored))
    p2_json     = json.dumps(prep_parlays(parlays_2))
    p3_json     = json.dumps(prep_parlays(parlays_3))
    scores_json = json.dumps(prep_scores_ticker(today_scores))

    html = (HTML
            .replace("__DATE__",     actual_date)
            .replace("__PICKS__",    picks_json)
            .replace("__GAMES__",    games_json)
            .replace("__P2__",       p2_json)
            .replace("__P3__",       p3_json)
            .replace("__SCORES__",   scores_json)
            .replace("__PROPS__",    props_json)
            .replace("__SCHEDULE__", schedule_json))

    os.makedirs(PICKS_DIR, exist_ok=True)
    out_path = os.path.join(PICKS_DIR, f"mlb_picks_{actual_date}.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    log.info(f"Dashboard saved: {out_path}")
    log.info(f"{len(scored)} games | {len(picks)} picks | "
             f"{len(parlays_2)} 2-leg parlays | {len(parlays_3)} 3-leg parlays")

    if not args.no_open:
        webbrowser.open(f"file:///{os.path.abspath(out_path)}")
        log.info("Opening in browser...")


if __name__ == "__main__":
    main()

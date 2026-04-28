# Statalizers — Project Context for Claude

## What This Is
MLB betting dashboard at **statalizers.com**, deployed on **Railway.app**. Built by Justin Skelly (jskellly@gmail.com). Flask app that runs a full data pipeline every morning at 6am ET, scores today's MLB games across moneyline, run line, totals, and player props, and serves an HTML dashboard with picks tiered by confidence (LOCK / STRONG / LEAN).

The goal is to drive pick confidence as high as possible using every available data signal — stats, Statcast, odds, sharp action, Kalshi prediction markets, weather, lineups, park factors, and platoon splits.

---

## Deployment
- **Platform:** Railway.app (service name: Jboom2u Picks)
- **Domain:** statalizers.com (DNS through Cloudflare — nameservers: doug.ns.cloudflare.com, savanna.ns.cloudflare.com)
- **Deploy command:** `railway up`
- **Environment variables (set via Railway CLI):** `ODDS_API_KEY`, `KALSHI_API_KEY`
- **Traffic monitoring:** Cloudflare dashboard → statalizers.com → Analytics & logs → HTTP Traffic

### Critical Known Issue — Data Persistence
Every `railway up` wipes the runtime filesystem. This destroys:
- All CSV files in `data/raw/` and `data/clean/`
- The `pipeline_run_date.txt` marker
- Any picks history or past analysis

**Planned fix (not yet implemented):** Decouple data from the app. Use Railway-attached PostgreSQL for structured data (picks history, model outputs) and Cloudflare R2 or Railway object storage for CSV snapshots. Deploy should only affect code, not data.

---

## Repo Structure
```
sports-betting-pipeline/
├── app.py                        # Flask server — serves dashboard, schedules pipeline
├── run_pipeline.py               # Main pipeline entry point (all steps)
├── run_picks.py                  # Generate picks from scored games
├── run_picks_html.py             # Dashboard HTML generator
├── run_afternoon.py              # Mid-day refresh (lineups + odds)
├── run_analysis.py               # Analysis utilities
├── run_historical.py             # Backfill historical data
├── serve_picks.py                # Local serving utility
├── kalshi_debug.py               # Kalshi API debugging
│
├── scrapers/
│   ├── mlb_scraper.py            # Core MLB stats scraper (Baseball Reference)
│   ├── mlb_pitcher_scraper.py    # Pitcher stats + recent starts (Baseball Savant)
│   ├── mlb_team_scraper.py       # Team hitting/pitching stats
│   ├── mlb_hitter_scraper.py     # Individual hitter stats for lineup players
│   ├── mlb_bullpen_scraper.py    # Bullpen ERA, WHIP, usage stats
│   ├── mlb_statcast_scraper.py   # Statcast quality-of-contact (Baseball Savant)
│   ├── mlb_lineup_scraper.py     # Confirmed lineups
│   ├── mlb_odds_scraper.py       # Live odds + sharp action (Odds API)
│   ├── mlb_kalshi_scraper.py     # Kalshi prediction market probabilities
│   ├── mlb_weather_scraper.py    # Game-time weather (wind, temp, precip)
│   └── mlb_historical_scraper.py # Historical game results for backtesting
│
├── normalize/
│   ├── mlb_normalize.py          # Normalizes raw scrape → clean master CSVs
│   ├── mlb_pitcher_normalize.py  # Pitcher-specific normalization
│   ├── mlb_bullpen_normalize.py  # Bullpen normalization
│   └── mlb_historical_normalize.py
│
├── model/
│   ├── mlb_model.py              # Core game scoring model
│   ├── mlb_picks.py              # Pick generation + parlay builder
│   └── mlb_props_model.py        # Player prop probability engine
│
├── data/
│   ├── park_factors.csv          # Static park run/HR factors by venue
│   ├── raw/                      # Daily raw scrape outputs (wiped on Railway deploy)
│   └── clean/                    # Normalized master CSVs (wiped on Railway deploy)
│
└── logs/                         # Pipeline logs
```

---

## Pipeline Flow (run_pipeline.py)
Runs daily at 6am ET via app.py scheduler. Steps in order:

1. **mlb_scraper** — scrapes yesterday's results + today's schedule from Baseball Reference
2. **mlb_normalize** — normalizes raw → appends to clean master CSVs
3. **mlb_odds_scraper** — pulls live odds + line movement via Odds API; detects sharp action
4. **mlb_weather_scraper** — fetches game-time weather for today's games
5. **mlb_pitcher_scraper** — fetches recent starts for today's probable pitchers
6. **mlb_pitcher_normalize** — normalizes pitcher recent starts
7. **mlb_team_scraper** — team hitting + pitching season stats
8. **mlb_bullpen_scraper + normalize** — bullpen stats
9. **mlb_statcast_scraper** — Statcast batter metrics from Baseball Savant
10. **mlb_lineup_scraper** — confirmed lineups
11. **mlb_hitter_scraper** — individual hitter stats for lineup players
12. **mlb_kalshi_scraper** — Kalshi implied probabilities
13. **run_picks_html** — scores all games, generates picks, builds HTML dashboard

Mid-day refresh (run_afternoon.py / _needs_lineup_refresh in app.py): re-runs lineup + hitter + odds steps after 10am ET when lineups confirm.

---

## Core Model (model/mlb_model.py)

**Architecture:** Pythagorean win expectation (exponent 1.83)
- Expected runs = team RPG × pitcher suppression × park factor × weather adjustment
- Win probability from Pythagorean formula drives moneyline confidence
- Home field boost: +0.025

**Data signals used:**
- Pitcher season ERA/FIP/WHIP
- Pitcher home/away splits (weighted 30% splits / 70% season)
- Pitcher platoon splits (vs. Left / vs. Right batters)
- Pitcher recent form: last 3 starts blended 30% recent / 70% season
- Team offensive RPG, OPS
- Team pitching ERA
- Bullpen ERA + WHIP
- Park factors (run factor + HR factor by venue)
- Weather: wind MPH toward/away CF (+0.04 runs per mph out), temp (−1.2% per °F below 65), precip probability
- Live odds + line movement (sharp action detection)
- Kalshi implied probability as a market signal
- Statcast: barrel rate, exit velocity, hard hit %, xBA, xSLG, xwOBA (batter quality)
- Confirmed lineups (lineup-weighted offensive scoring)

**League-average baselines (2023-2025):**
- ERA: 4.20, FIP: 4.20, RPG: 4.50, OPS: 0.720

---

## Pick Generation (model/mlb_picks.py)

**Confidence tiers:**
- LOCK: 68%+ — strongest model signal
- STRONG: 62-68%
- LEAN: 55-62%
- PASS: <55% — not shown

**Parlay rules:**
- Minimum 57% confidence per leg
- No two picks from the same game
- Ranked by combined probability

**Prop types supported (mlb_props_model.py):**
HR (0.5+), HITS (0.5+), TB (1.5+), RBI (0.5+), R (0.5+), SB (0.5+), K (SP strikeout total over/under)

---

## app.py Behavior
- Serves dashboard from cache; never blocks a request
- Cache TTL: 10 minutes (regenerates in background)
- Full pipeline: runs at 6am ET daily, also on startup if today's data is missing
- Odds snapshot: every 2 hours between 8am-10pm ET (stays under 500/month Odds API free limit)
- Lineup refresh: triggered after 10am ET if hitter stats file is missing or >4 hours old
- gzip compression via flask-compress (570KB HTML → ~80KB)

---

## Data Sources
| Source | What | Auth |
|--------|------|------|
| Baseball Reference | Game results, schedule, team/pitcher stats | None (scrape) |
| Baseball Savant | Statcast metrics (xBA, barrel rate, exit velo) | None (CSV endpoint) |
| Odds API | Live odds, line movement, sharp action | ODDS_API_KEY env var |
| Kalshi | Prediction market implied probabilities | KALSHI_API_KEY env var |
| Open-Meteo | Game-time weather | None |
| Rotowire / MLB.com | Confirmed lineups | None (scrape) |

---

## Cloudflare Setup
- Account: jskellly@gmail.com at dash.cloudflare.com
- DNS: statalizers.com CNAME → Railway URL (proxied, orange cloud)
- www CNAME → jboom2u-picks Railway URL (proxied)
- MX records: eforward1-5.registrar-servers.com (DNS only, for email)
- Traffic analytics: dash.cloudflare.com → statalizers.com → Analytics & logs → HTTP Traffic
- Nameservers at Namecheap updated to: doug.ns.cloudflare.com + savanna.ns.cloudflare.com

---

## Roadmap (Planned — Not Yet Built)

### Priority 1 — Data Persistence (blocks everything else)
- Add Railway PostgreSQL for picks history, model outputs, scored games
- Add Cloudflare R2 or Railway object storage for CSV snapshots
- Modify pipeline to read/write from persistent store instead of local filesystem
- Goal: `railway up` deploys code only, data survives

### Priority 2 — New Data Signals (confidence improvement)
1. **Umpire data scraper** — home plate ump historically affects totals by 0.5+ runs; scrape from UmpScorecards.com (umpscorecards.com). Wire into totals model.
2. **Pitcher Statcast pitch mix / stuff metrics** — currently only pull batter-side Statcast. Need pitcher arsenal leaderboard: spin rate, velocity by pitch type, whiff rate, chase rate. Velocity drop from career avg is a major signal not yet captured in ERA. Pull from Baseball Savant pitcher leaderboard endpoint.
3. **Bullpen fatigue / workload** — have bullpen season stats but not recent pitch counts. Need to track relievers' last 2-3 days of usage. A closer who threw 30+ pitches in back-to-back games is a meaningful signal for run line + totals late.
4. **Polymarket as second prediction market signal** — already have Kalshi. Adding Polymarket gives a second implied probability; divergence between the two markets is itself a signal. Use arbbets/Prediction-Markets-Data pattern.
5. **Backtesting loop** — no system yet to track each pick against actual outcomes and compute ROI / accuracy over time. Need to: store all picks at generation time, ingest final scores nightly, score each pick as win/loss, compute accuracy by tier/type/confidence band. This is the only way to validate and tune model weights empirically.

### Priority 3 — Model Improvements
- Tune Pythagorean weights using backtesting results
- Add Kelly Criterion bet sizing recommendations
- Consider ensemble approach (Random Forest alongside Pythagorean) for validation

---

## Key Coding Conventions
- All scrapers have a `run()` function as entry point, return a result summary
- Data flows: raw scrape → `data/raw/` → normalize → `data/clean/` master CSVs
- Model loads all clean CSVs once via `MLBModel.load()`, then scores on demand
- Non-fatal steps (odds, weather, Kalshi) use try/except + log.warning — pipeline continues if they fail
- All times in ET (America/New_York) via zoneinfo
- Flask never blocks on pipeline — everything runs in background threads

---

## Previous Session Notes
- Cloudflare was set up from scratch in this session (April 2026)
- Railway persistence issue discussed — fix is planned but not yet implemented
- Compared against similar GitHub projects: dylankelder/MLB-Projects, callmevojtko/Recommended-Bets-By-Email-MLB, kylejohnson363/Predicting-MLB-Games-with-Machine-Learning
- Statalizers is more advanced than 90% of public MLB betting repos — unique combination of Statcast + Kalshi + sharp action + weather + platoon splits in one pipeline
- New chat sessions: paste this file path — `C:\Users\Jskel\OneDrive\Documents\GitHub\sports-betting-pipeline\CLAUDE.md` — or just say "read CLAUDE.md" and Claude will load full context

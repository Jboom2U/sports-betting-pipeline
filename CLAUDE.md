# Statalizers — Project Context for Claude

## ⚠️ CRITICAL: Never Run Git Commands From the Sandbox
**The Cowork sandbox cannot delete lock files on the Windows-mounted repo.** Running `git add` or `git commit` from bash leaves `.git/index.lock` and `.git/HEAD.lock` stranded, breaking the next commit.
- **Claude: make file edits only from the sandbox. Never run git add/commit/push from bash.**
- Tell the user to run all git operations from their own PowerShell terminal.
- Correct pattern: edit files in sandbox → "run `git commit` and `git push` from your terminal"

---

## ⚠️ CRITICAL: Log Timestamps Are UTC — Always Convert to ET
**Railway logs always show UTC time. NEVER read a log timestamp as ET.**
- Subtract 4 hours (EDT) or 5 hours (EST) to get ET
- Example: log shows `22:32` → actual time is `6:32pm ET`

---

## ⚠️ CRITICAL: Pre-Deploy Checklist — Run Before Every railway up

**Always run `python scripts/predeploy_check.py` before giving the user a commit.**

The script now does 4 things:
1. Syntax / null bytes / truncation check on all 18 critical files
2. Field-name bug checks (BOM strip, Monte Carlo conf fallback)
3. SQL column validation — cross-checks queries against schema.py column names (only scans SQL string literals, not Python code)
4. **Live route simulation against committed code** — does `git stash`, tests /, /performance-html, /schedule-status, /status, then `git stash pop`. This catches NameErrors and route crashes that syntax checks miss.

**Why the stash matters:** Without stashing, the sim tests your local patched files, not what Railway will actually run. Always stash first.

**File truncation risk on Windows-mounted filesystem:** The Edit tool and Write tool can truncate large files. Use `cat > file << 'EOF'` via bash for any file over ~100 lines. Always verify with `python3 scripts/predeploy_check.py` after edits.

---

## ⚠️ CRITICAL: SQL Queries — Cross-Check Column Names Against schema.py
Before writing any SQL query, verify the column names exist in `db/schema.py`.
Key table column names to remember:
- `scored_games`: uses `score_date` (NOT `game_date`), `game_id`, `ml_signal`, `sharp_side`
- `picks`: uses `pick_date`, `game_id`, `game`, `pick_type`, `label`, `team`, `conf`, `tier`, `actual_result`, `market_signal`
- Join picks ↔ scored_games on: `sg.score_date = p.pick_date AND sg.game_id = p.game_id`

---

## What This Is
MLB betting dashboard at **statalizers.com**, deployed on **Railway.app**. Built by Justin Skelly (jskellly@gmail.com). Flask app that runs a full data pipeline every morning at 6am ET, scores today's MLB games across moneyline, run line, totals, and player props, and serves an HTML dashboard with picks tiered by confidence (LOCK / STRONG / LEAN).

---

## Deployment
- **Platform:** Railway.app (service name: Jboom2u Picks)
- **Domain:** statalizers.com (DNS through Cloudflare)
- **Deploy command:** `railway up` (from PowerShell in repo dir, NOT from sandbox bash)
- **Odds API usage:** Resets June 1st. ~1 pull/day via adaptive refresh (~30/month). Track carefully — wasted deploys burn quota.
- **Environment variables:** `ODDS_API_KEY`, `KALSHI_API_KEY`, `DATABASE_URL`, `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY_ID`, `STORAGE_SECRET_ACCESS_KEY`, `STORAGE_BUCKET`

### PowerShell commit pattern (always include cd):
```powershell
cd C:\Users\Jskel\Documents\GitHub\sports-betting-pipeline-clean
git add <files>
git commit -m "..."
git push
railway up
```

### Data Persistence
- **Railway PostgreSQL** — picks, scored_games, pipeline_runs, player_prop_history
- **Cloudflare R2** — bucket `statalizers-data` — all CSVs + picks HTML
- On startup: downloads CSVs from R2 → checks DB if pipeline ran today → runs if needed
- `railway up` deploys code only — data survives deploys

---

## Repo Structure
```
sports-betting-pipeline/
├── app.py                        # Flask server
├── run_pipeline.py               # Full pipeline entry point
├── run_picks.py                  # Generate picks from scored games
├── run_picks_html.py             # Dashboard HTML generator
├── run_afternoon.py              # Adaptive afternoon refresh
├── run_analysis.py               # Grades yesterday's picks
├── run_historical.py             # Backfill historical data
│
├── scrapers/                     # One scraper per data source
├── normalize/                    # Raw → clean CSV normalization
├── model/
│   ├── mlb_model.py              # Core Pythagorean scoring model
│   ├── mlb_picks.py              # Pick generation + parlay builder
│   ├── mlb_props_model.py        # Player prop probability engine
│   └── mlb_analysis_sections.py # HR Watch + Monte Carlo tab builders
│
├── db/
│   ├── connection.py             # PostgreSQL connection pool
│   ├── schema.py                 # Table creation + migrations
│   ├── pipeline_log.py           # Mark pipeline started/complete/failed
│   ├── picks_store.py            # Save/grade picks, accuracy summaries
│   └── csv_sync.py               # Upload/download CSVs to/from R2
│
├── scripts/
│   └── predeploy_check.py        # Run before every railway up
│
└── data/
    ├── park_factors.csv
    ├── raw/                      # Daily raw scrape outputs
    └── clean/                    # Normalized master CSVs
```

---

## Pipeline Flow (run_pipeline.py)
Runs daily at 6am ET via app.py scheduler. Key steps:
1. mlb_scraper → yesterday's results + today's schedule
2. mlb_normalize → raw → clean CSVs
3. mlb_odds_scraper → live odds + sharp action (Odds API)
4. mlb_weather_scraper, mlb_umpire_scraper, mlb_bullpen_fatigue_scraper
5. mlb_pitcher_scraper + statcast scrapers (BOM strip required on Savant CSVs)
6. mlb_polymarket_scraper (Polymarket paginates to ~10100 markets; 422 at offset 10100 is expected — stop silently)
7. mlb_bullpen_scraper, mlb_lineup_scraper, mlb_hitter_scraper
8. mlb_kalshi_scraper (fetches KXMLB series; matching still inconsistent — 0 parseable markets is common)
9. run_analysis → grade yesterday's picks → push to DB
10. mlb_model + save_picks → score games → save to DB
11. mark_pipeline_complete + csv_upload_all

**Adaptive afternoon refresh** — fires 2h before first pitch (computed from schedule CSV). Scheduled time stored in `_schedule_state` global dict in app.py and exposed at `/schedule-status` JSON endpoint.

---

## Routes
- `/` — main dashboard (served from cache, 10 min TTL)
- `/status` — pipeline health page
- `/schedule-status` — JSON: next pipeline, next refresh, first pitch times
- `/performance` — JSON W/L/ROI by tier
- `/performance-html` — dark-themed performance dashboard (7/14/30/60/90d toggles)
  - Includes: tier breakdown, market signal (CONFIRM/DIVERGE/NEUTRAL), monthly summary, **Sharp Action vs Model head-to-head**
- `/force-statcast` — manually re-pull Statcast data + rebuild dashboard
- `/force-refresh` — manually trigger afternoon refresh

---

## Core Model (model/mlb_model.py)
**Architecture:** Pythagorean win expectation (exponent 1.83)

**Data signals:** pitcher ERA/FIP/WHIP, home/away splits (30/70), platoon splits, recent form (3 starts, 30/70), Statcast stuff (xwOBA 25% + whiff% 10%), team RPG/OPS, bullpen ERA/WHIP, bullpen fatigue (FRESH/NORMAL/TIRED/SPENT → -5%/0%/+12%/+20% bp_era), park factors, weather, HP umpire RPG tendency, Kalshi implied prob, Polymarket implied prob (CONFIRM +1.5% / DIVERGE -1.5%), Statcast barrel/EV/xwOBA (batters), confirmed lineups.

**Global state:** `_schedule_state` dict stores `next_pipeline_et`, `next_refresh_et`, `first_pitch_et` — populated by scheduler and `_schedule_adaptive_refresh()`.

---

## Pick Generation (model/mlb_picks.py)
**Confidence tiers:**
- LOCK: 75%+
- STRONG: 68-75%
- LEAN: 60-68%
- TOSSUP: 48-60% (shown, no Kelly)
- PASS: <60% (not shown)

**RL minimum:** 60% edge required (raised from 48% to filter thin-edge noise)
**TBD suppression:** TOTAL suppressed when either SP TBD; RL suppressed when either SP TBD; ML downgraded one tier when both TBD

**Parlay rules:** min 57% per leg, no two picks from same game
**Thematic parlays:** tagged by thesis (Pitching Mismatch, Home Dog Value, Sharp Action, Bullpen Edge, Market Confirm, Hot Team)
**Pick narrative:** `_build_narrative()` generates 2-3 sentence plain-English explanation per card

---

## Performance Dashboard (/performance-html)
- Rolling W/L/ROI by confidence tier and pick type
- Market signal breakdown (CONFIRM / DIVERGE / NEUTRAL vs Kalshi/Polymarket)
- Monthly W/L/ROI summary table
- **Sharp Action vs Model** — every game where STEAM contradicted model ML pick, result, and head-to-head win rate. Query joins `picks` and `scored_games` on `score_date + game_id`.

---

## Dashboard Features (run_picks_html.py)
- Schedule status bar (below header) — shows next 6am pipeline, lineup refresh time, first pitch — auto-refreshes every 60s via `/schedule-status`
- HR Watch tab — top HR candidates by barrel rate + park factor + pitcher xwOBA
- Monte Carlo tab — 1000 sim hit-rate bar per ML pick with EV tag
- TOMORROW badge (amber pill) when picks are next-day
- Auto-switches to tomorrow's slate when all today's games started
- Date toggle: today/tomorrow tabs
- gzip compression via flask-compress
- Startup: seeds cache from R2 HTML immediately (site live before pipeline reruns)

---

## Known Issues / Watch Points

### Kalshi matching broken
Fetches 30 KXMLB markets but 0 match today's games. Team/game name format mismatch between Kalshi and schedule CSV. Needs debug session to inspect raw market titles.

### Batter Statcast loads 1 row on restart
`force-statcast` correctly writes 532 batters, but on next startup only 1 loads. Likely a BOM issue in the load path (not just the scraper). Not yet fixed.

### Polymarket 422 at offset 10100
Expected behavior — Polymarket hard caps at 10,000 markets. The 422 is caught and logged as WARNING (non-fatal). Not a real error.

### Odds API quota
500 req/month free tier. Adaptive refresh = ~1 pull/day (~30/month). Every wasted `railway up` that triggers an odds call burns quota. As of June 2026: 42 used after reset June 1st.

### predeploy_check.py stash behavior
`git stash --include-untracked` is used so the simulation tests committed code only. If git stash fails (e.g. nothing to stash), the sim falls through and tests local files — still catches most issues but not the "deployed vs local" gap.

---

## Completed Features (chronological)
- Railway PostgreSQL + R2 CSV persistence
- Backtesting loop (nightly grading + /performance-html)
- Umpire HP signal, pitcher Statcast stuff, bullpen fatigue, Polymarket, adaptive refresh
- Probable pitcher upsert in afternoon refresh (catches scratches)
- Tomorrow badge + date toggle + auto-slate switch
- Odds API quota warnings at 150/75/25
- Half-Kelly bet sizing on every pick card
- market_signal column + backfill + market signal breakdown in /performance-html
- Monthly performance summary
- R2 HTML cache seeding on startup
- Hitter stats JSON uploaded to R2 after afternoon scraper
- TBD starter suppression
- Run line minimum 60% edge threshold
- Data consistency fix (ON CONFLICT DO UPDATE, grade preserved)
- Pick card narrative (`_build_narrative()`)
- Thematic parlays (`build_thematic_parlays()`, `_tag_pick_thesis()`)
- Kalshi scraper expanded (paginated search, 7 tickers, events fallback)
- Yesterday count discrepancy fix (TOSSUP picks excluded from graded_display)
- JS IIFE bug fix in run_picks_html.py
- HR Watch tab + Monte Carlo tab (model/mlb_analysis_sections.py)
- /force-statcast route
- BOM strip on Savant pitcher CSV (was silently dropping 656/657 pitchers)
- HITS prop split floor (RBI/Hit internal consistency fix)
- Polymarket 422 silent stop (stops paginating instead of logging error)
- predeploy_check.py with stash-based route simulation + SQL column validation
- Schedule status bar on dashboard (/schedule-status endpoint + JS countdown)
- Sharp Action vs Model retrospective in /performance-html

## Active Work Queue
1. **Kalshi market matching** — debug raw KXMLB market titles vs schedule team names
2. **Batter Statcast 1-row load bug** — trace why 532-row CSV produces 1 row on model load
3. **Game picks independent of lineups** — ML/RL/total picks at 6am, props only need lineup confirmation
4. **Projected props** — publish props early for everyday regulars (PROJECTED), update on lineup confirm

## Key Coding Conventions
- All scrapers have a `run()` function as entry point
- Data flows: raw scrape → `data/raw/` → normalize → `data/clean/` master CSVs
- Model loads all clean CSVs once via `MLBModel.load()`, then scores on demand
- Non-fatal steps (odds, weather, Kalshi, Polymarket) use try/except + log.warning
- All times in ET (America/New_York) via zoneinfo
- Flask never blocks on pipeline — everything runs in background threads
- Large file edits: use `cat > file << 'EOF'` via bash (Edit tool truncates files >~200 lines on Windows FS)
- SQL queries: always verify column names against db/schema.py before writing

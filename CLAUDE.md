# Statalizers — Project Context for Claude

## 📍 Which System Am I In? — Session Scope Map

This folder holds **three related but separate systems**. Confirm which one the session is
about before doing anything. Justin opens chats with a scope prefix; honor it.

| Prefix | System | Lives in | Deploys with |
|---|---|---|---|
| `PIPELINE` | MLB data pipeline + model | `scrapers/`, `normalize/`, `model/`, `run_*.py` | `railway up` |
| `DASHBOARD` | statalizers.com Flask app | `app.py`, `routes/`, `run_picks_html.py` | `railway up` |
| `CONSENSUS` | Multi-AI pick review worker | `consensus-worker/` | `npx wrangler deploy` |
| `PICKS-SITE` | picks.statalizers.com (capper picks, D1) | separate repo | Cloudflare |
| `DAILY` | Routine daily pick runs, no code changes | n/a | n/a |

**CONSENSUS is not the same as DASHBOARD.** The consensus worker is a Cloudflare Worker that
reads pipeline output from R2 and sends it to Gemini/OpenRouter/OpenAI for critical review.
It has its own D1 database (`consensus-db`), its own login, and its own cron (6:45am ET).
It deploys with wrangler, NOT `railway up`, so it costs zero Odds API quota.

**If the scope is ambiguous, ask before editing.** A change to `model/` can silently alter
what the consensus worker reviews the next morning.

### Session history note
Cowork keeps roughly the last 50 sessions. Older ones age out and cannot be recovered.
Anything worth keeping belongs in this file, not in a chat transcript.

---

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

The script does 4 things:
1. Syntax / null bytes / truncation check on all 18 critical files
2. Field-name bug checks (BOM strip, Monte Carlo conf fallback)
3. SQL column validation — cross-checks queries against schema.py column names
4. **Live route simulation against committed code** — does `git stash`, tests /, /performance-html, /schedule-status, /status, then `git stash pop`. This catches NameErrors and route crashes that syntax checks miss.

**Why the stash matters:** Without stashing, the sim tests your local patched files, not what Railway will actually run. Always stash first.

**File truncation risk on Windows-mounted filesystem:** Use Python-via-bash splice pattern for any file over ~200 lines. Always verify with `python3 scripts/predeploy_check.py` after edits.

---

## ⚠️ CRITICAL: SQL Queries — Cross-Check Column Names Against schema.py
Before writing any SQL query, verify the column names exist in `db/schema.py`.
Key table column names:
- `scored_games`: uses `score_date` (NOT `game_date`), `game_id`, `ml_signal`, `sharp_side`
- `picks`: uses `pick_date`, `game_id`, `game`, `pick_type`, `label`, `team`, `conf`, `tier`, `actual_result`, `market_signal`, `away_final`, `home_final`
- Join picks ↔ scored_games on: `sg.score_date = p.pick_date AND sg.game_id = p.game_id`

---

## What This Is
MLB betting dashboard at **statalizers.com**, deployed on **Railway.app**. Built by Justin Skelly (jskellly@gmail.com). Flask app that runs a full data pipeline every morning at 6am ET, scores today's MLB games across moneyline, run line, totals, and player props, and serves an HTML dashboard with picks tiered by confidence (LOCK / STRONG / LEAN / TOSSUP).

---

## Deployment
- **Platform:** Railway.app (service name: Jboom2u Picks)
- **Domain:** statalizers.com (DNS through Cloudflare)
- **Git repo:** `C:\Users\Jskel\GitHub\sports-betting-pipeline` — single source of truth
- **Deploy command:** `railway up` (from PowerShell in repo dir, NOT from sandbox bash)
- **Odds API usage:** Resets 1st of month. ~1 pull/day via adaptive refresh (~30/month). Track carefully.
- **Environment variables:** `ODDS_API_KEY`, `KALSHI_API_KEY`, `DATABASE_URL`, `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY_ID`, `STORAGE_SECRET_ACCESS_KEY`, `STORAGE_BUCKET`, `ADMIN_PASSWORD`, `ADMIN_SECRET`

### PowerShell commit pattern:
```powershell
cd C:\Users\Jskel\GitHub\sports-betting-pipeline
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
├── routes/
│   ├── admin.py                  # /admin routes (password protected)
│   └── analytics.py             # /analytics NL query interface
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
│   ├── model_config.py           # Load/save model weights from DB
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
6. mlb_polymarket_scraper (FIXED 2026-07-21: builds per-game slugs mlb-<away>-<home>-<date> from schedule; /markets ignores tag_slug so old catalog-walk found 0 games. The '422 at offset 10100 is expected' note described the BUG, not normal behavior.)
7. mlb_bullpen_scraper, mlb_lineup_scraper, mlb_hitter_scraper
8. mlb_kalshi_scraper (FIXED 2026-07-20: parses the ticker KXMLBGAME-... not the ambiguous title. Verified 17 game markets parsed on 2026-07-21.)
9. run_analysis → grade yesterday's picks → push to DB
10. mlb_model + save_picks → score games → save to DB
11. mark_pipeline_complete + csv_upload_all

**Adaptive afternoon refresh** — fires 2h before first pitch (computed from schedule CSV). Scheduled time stored in `_schedule_state` global dict in app.py and exposed at `/schedule-status` JSON endpoint.

---

## Routes
- `/` — main dashboard (served from cache, 10 min TTL)
- `/admin` — admin hub (password protected) — links to all internal routes
- `/admin/login` — login page; password set via `ADMIN_PASSWORD` env var
- `/admin/logout` — clears session
- `/admin/model-config` — model control panel (adjust signal weights, preview picks, save config to DB)
- `/analytics` — analytics dashboard + natural language DB query interface
- `/status` — pipeline health page
- `/schedule-status` — JSON: next pipeline, next refresh, first pitch times
- `/performance` — JSON W/L/ROI by tier
- `/performance-html` — dark-themed performance dashboard (7d/14d/30d/60d/Season/All time dropdown)
  - Includes: tier breakdown, market signal (CONFIRM/DIVERGE/NEUTRAL), monthly summary, Sharp Action vs Model head-to-head
- `/force-statcast` — manually re-pull Statcast data + rebuild dashboard
- `/force-refresh` — manually trigger afternoon refresh
- `/force-pipeline` — manually trigger full pipeline
- `/unstick` — clear stuck pipeline state

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
- PASS: <48% (not shown)

**RL minimum:** 60% edge required
**TBD suppression:** TOTAL suppressed when either SP TBD; RL suppressed when either SP TBD; ML downgraded one tier when both TBD

**Parlay rules:** min 57% per leg, no two picks from same game
**Thematic parlays:** tagged by thesis (Pitching Mismatch, Home Dog Value, Sharp Action, Bullpen Edge, Market Confirm, Hot Team)
**Pick narrative:** `_build_narrative()` generates 2-3 sentence plain-English explanation per card

---

## Performance Dashboard (/performance-html)
- Rolling W/L/ROI by confidence tier and pick type — dropdown filter: 7d/14d/30d/60d/Season/All time
- Market signal breakdown (CONFIRM / DIVERGE / NEUTRAL vs Kalshi/Polymarket)
- Monthly W/L/ROI summary table
- Sharp Action vs Model — every game where STEAM contradicted model ML pick, result, head-to-head win rate
- Query joins `picks` and `scored_games` on `score_date + game_id`

---

## Dashboard Features (run_picks_html.py)
- Schedule status bar — shows next 6am pipeline, lineup refresh time, first pitch — auto-refreshes every 60s via `/schedule-status`
- HR Watch tab — top HR candidates by barrel rate + park factor + pitcher xwOBA
- Monte Carlo tab — 1000 sim hit-rate bar per ML pick with EV tag
- Daily Summary tab — completed (Final) games only, no in-progress cards
- TOMORROW badge (amber pill) when picks are next-day
- Auto-switches to tomorrow's slate when all today's games started
- gzip compression via flask-compress
- Startup: seeds cache from R2 HTML immediately (site live before pipeline reruns)

---

## Known Issues / Watch Points

### Kalshi matching broken
Fetches 30 KXMLB markets but 0 match today's games. Team/game name format mismatch between Kalshi and schedule CSV. Needs debug session to inspect raw market titles.

### Batter Statcast loads 1 row on restart
`force-statcast` correctly writes 532 batters, but on next startup only 1 loads. Likely a BOM issue in the load path (not just the scraper). Not yet fixed.

### Polymarket 422 at offset 10100
Expected behavior — Polymarket hard caps at 10,000 markets. The 422 is caught and logged as WARNING (non-fatal).

### Odds API quota
500 req/month free tier. Adaptive refresh = ~1 pull/day (~30/month). Every wasted `railway up` that triggers an odds call burns quota.

### predeploy_check.py stash behavior
`git stash --include-untracked` is used so the simulation tests committed code only. If git stash fails (nothing to stash), the sim falls through and tests local files — still catches most issues but not the deployed vs local gap.

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
- JS IIFE bug fix in run_picks_html.py
- HR Watch tab + Monte Carlo tab (model/mlb_analysis_sections.py)
- /force-statcast route
- BOM strip on Savant pitcher CSV (was silently dropping 656/657 pitchers)
- HITS prop split floor (RBI/Hit internal consistency fix)
- Polymarket 422 silent stop
- predeploy_check.py with stash-based route simulation + SQL column validation
- Schedule status bar on dashboard (/schedule-status endpoint + JS countdown)
- Sharp Action vs Model retrospective in /performance-html
- All INTERVAL '%s days' SQL queries in picks_store.py replaced with Python date cutoff (was silently returning empty)
- push_grades_to_db() game+type fallback (prevents grade push failure when afternoon refresh updates pick label)
- Yesterday panel DB fallback (builds metrics from graded picks when JSON file is missing)
- Daily Summary tab UI redesigned — completed games only, no in-progress cards
- Performance page filter: dropdown replacing pill links, 7d/14d/30d/60d/Season/All time
- Market signal breakdown + monthly summary sections in /performance-html
- player_prop_accuracy min_picks lowered from 5 to 3
- Admin hub at /admin with password login (ADMIN_PASSWORD env var)
- Model control panel at /admin/model-config — tune signal weights, preview pick impact, save config to DB
- Analytics dashboard at /analytics — NL query interface over PostgreSQL
- db/model_config.py — load/save model weights from DB with preset support
- Probable pitcher refresh in afternoon run — fixes stale K prop starters when rotation changes post-6am
- TOSSUP picks now graded in DB (push_grades_to_db uses full graded list) — sharp table shows results for all tiers
- TOSSUP picks included in graded_display — yesterday panel shows all graded games (15+ picks per day)

## ⚠️ DATA BOUNDARY: 2026-07-21 — pre-fix vs post-fix picks

**Every pick generated before 2026-07-21 was produced by a partially blind model.**
Umpire blank, bullpen fatigue never firing, platoon splits dead, Kalshi dead, and
eight master CSVs vanishing from `data/clean/` on every container restart.

The 1,459 graded picks in the calibration findings below are ALL pre-fix. They
measure how the model performed while starved, not how it performs now.

**Do not pool pre- and post-2026-07-21 picks in one calibration run.** Doing so
averages a crippled model with a repaired one and will produce a conclusion that
is wrong about both. When there are enough post-fix graded picks (target ~300, so
roughly 3-4 weeks), run calibration filtered to `pick_date >= '2026-07-21'` and
compare against the pre-fix table below.

Specifically unresolved by the pre-fix data: whether TOTAL picks have edge. Their
pre-fix win rate is flat noise (44.8%-55.4%, no trend across bands), but bullpen
fatigue and umpire RPG both feed run expectancy and neither was firing. Totals may
genuinely improve. Do not permanently suppress totals based on pre-fix data alone.

What the pre-fix data DOES establish, because it held even while the model was
blind: **ML at 75%+ won 67.5% (n=40) and 63.8% (n=47).** That edge existed under
degraded inputs, so it is the safest thing to lean on while post-fix data
accumulates.

---

## 📊 CALIBRATION FINDINGS — 2026-07-20 (read before tuning anything)
**⚠️ ALL PRE-FIX. See the data boundary section above before acting on these.**

First real calibration run, on 1,459 graded picks covering ~May 15 to Jul 19,
after backfilling the picks table from R2. See `/admin/calibration`.

**Overall: 714-745, 48.9%. Break-even at -110 is 52.38%. The model is 3.4 points
underwater over two months.**

### The core problem: confidence does not discriminate

| band | n | predicted | actual |
|---|---|---|---|
| 50-55% | 286 | 52.5% | 44.8% |
| 55-60% | 298 | 57.6% | 45.6% |
| 60-65% | 279 | 62.5% | 49.5% |
| 65-70% | 214 | 67.3% | 50.5% |
| 70-75% | 266 | 72.7% | 49.6% |

Predicted rises 20 points; actual rises ~5 and is not monotonic. All five bands
clear n=200 (CI ≈ ±6) and are statistically indistinguishable from each other.
**Between 50% and 75% confidence the model's number carries almost no
information.** That covers 1,343 of 1,459 graded picks.

Real edge exists only at the top: 75-80% wins 67.5% (n=40), 80%+ wins 63.8%
(n=47). Small samples but clearly separated from everything below.

### Three specific findings

1. **Run line is a systematic loser.** 222 graded. Every band deeply negative:
   37.9% at 50-55 (n=103), 38.2% at 55-60, 36.8% at 65-70. The 60% RL threshold
   in `model/mlb_picks.py` is not protecting anything. Consider suppressing RL.
2. **Totals cap confirmed.** `total_conf_base = min(0.74, ...)` at
   `model/mlb_model.py:972` bunches distinct games onto one number. TOTAL at
   70-75 is n=204, actual 49.0% vs 72.9% predicted (gap -23.9, CI ±6.9).
3. **Tiers are inverted.** Approx win rates: STRONG 53.1%, LOCK 48.8%,
   TOSSUP 47.7%, LEAN 45.5%. LOCK at 65-70 wins 35.8% (n=53). STRONG is the only
   tier above break-even; LOCK is the worst-performing high tier.

### Tier/conf mismatch (unresolved)

`tier` and `conf` are assigned from different numbers. CLAUDE.md documents
LOCK as 75%+, but LOCK contains picks at 65-70% conf; STRONG (doc: 68-75) holds
picks at 60-65. Probably pre- vs post-adjustment confidence. Any tier-based
analysis is untrustworthy until this is traced.

### Open data-integrity question (resolve before acting on the above)

`/admin/calibration` market signal table splits by source: NEUTRAL = 737
live-saved picks at 51.8%; NONE = 722 backfilled picks at 46.0%. Same date
range, same model, 5.8 points apart — larger than chance comfortably explains.
Either the live grading path (`push_grades_to_db`) and the JSON grading path
disagree about outcomes, or the deploy-day sample is skewed. **Resolve this
first; it affects confidence in every number above.**

### What NOT to do

Do not tune signal weights in `/admin/model-config`. Weight tuning assumes the
model ranks correctly and needs rebalancing. The data says the confidence output
is near-uninformative across its main range — a structural problem in how
signals combine into a probability, not a weighting problem.

---

## 🎯 SPEC: "High Confidence" tab (requested 2026-07-21, not yet built)

Justin wants a tab surfacing only picks in the band with demonstrated edge, while
keeping every other pick visible for optional riskier plays. Nothing is hidden or
suppressed — this is prioritization, not filtering.

**Inclusion rule (pre-fix data, revisit after the 2026-07-21 boundary):**
`pick_type == "ML" AND conf >= 0.75`. That band won 67.5% (n=40) and 63.8% (n=47)
even while the model was starved, which is why it is the safe default. Do NOT
extend it to TOTAL or RL on pre-fix evidence; both were flat-to-negative, but they
were also measured blind. Re-derive the threshold from post-fix calibration once
~300 post-2026-07-21 graded picks exist.

**Implementation notes:**
- Nav buttons live at `run_picks_html.py:1656` (`section-nav-btn`, `data-panel`).
  Panels are `<div class="section-panel" id="panel-...">`.
- Blocker: pick cards are built as one large inline template literal inside
  `renderPicks()` (starts ~line 2055, grid `#picksGrid`). There is no reusable
  card function. **Step 1 is extracting that markup into `buildPickCard(p, idx)`**
  so both `#picksGrid` and the new `#highConfGrid` can call it. Do this as its own
  commit and verify the existing Game Picks tab still renders before adding the tab.
- run_picks_html.py is 221 KB — Edit truncates it. Use the Python-via-bash splice
  pattern and `python3 -m py_compile` after every change.
- Show the band's historical record on the panel (e.g. "ML 75%+: 87 picks, 65.5%
  since May 15") so the tab carries its own justification.

---

## 🔴 POLYMARKET ROOT CAUSE FOUND 2026-07-21 (fix not yet written)

`fetch_mlb_markets()` in `scrapers/mlb_polymarket_scraper.py:115` queries
`GET /markets?tag_slug=mlb`. **The `/markets` endpoint silently ignores
`tag_slug`.** Verified live 2026-07-21: that exact call returns
"New Rihanna Album before GTA VI?", "Will Jesus Christ return before GTA VI?",
"Trump out as President before GTA VI?" — zero baseball.

`GET /events?tag_slug=mlb` DOES filter correctly (returns "MLB World Series
Champion 2026", "New MLB CBA by Dec. 1?").

Consequence: the scraper paginates Polymarket's entire ~10k market catalog every
run, matches nothing, and returns early at the `if not games:` guard on line 514
WITHOUT writing `data/clean/mlb_polymarket_master.csv`. `load_polymarket_for_date()`
then finds no file and returns `{}` with no log line — a fully silent failure.
This is why `/admin/signal-audit` shows poly_away_prob / poly_home_prob /
poly_market_gap MISSING and poly_market_signal NO_DATA.

**The documented "422 at offset 10100 is expected — stop silently" note was
describing this bug as normal behavior.** It is not normal; it is the scraper
walking the whole catalog.

**Fix:** rewrite `fetch_mlb_markets()` to hit `/events?tag_slug=mlb&closed=false`
and extract per-game markets from each event's nested `markets` array. Per-game
event slugs look like `mlb-<away>-<home>-<date>`. Then re-verify
`extract_game_probabilities()` against the real shape before trusting it.

**Knock-on:** Kalshi's `combined_away_prob`/`combined_home_prob` and the entire
CONFIRM/DIVERGE market signal need BOTH Kalshi and Polymarket probabilities
(`get_market_divergence(poly_away_prob, kalshi_away_prob)` at
`mlb_polymarket_scraper.py:476`). So Polymarket must be fixed before the Kalshi
ticker fix from 2026-07-20 can produce any market signal at all.

---

## 🌅 BUILD FIRST TOMORROW — 2026 team hitting data (confirm before games)

**Must be done and verified before tomorrow's slate.** The team K-rate fix
(2026-07-21) now reads `k_rate` correctly and uses the latest season per team,
but `mlb_team_hitting_master.csv` only holds 2023-2025 — there is NO 2026 row —
so K props currently score 2026 games on 2025 opponent K-rates. Better than the
2023 rates it used before the fix, but still a season stale.

Fix: the team hitting scraper must pull 2026. Find it (likely
`mlb_team_scraper.py` or `mlb_hitter_scraper.py`'s team path), confirm its season
list / `SEASON` includes 2026, run it, and verify `mlb_team_hitting_master.csv`
gains 30 rows of 2026 data. Then `/admin/refresh-signals`-equivalent for team
stats, and confirm K prop opponent factors vary by team on the board.

Verify working before first pitch so all prop grading starts on clean data.

### Also queued (props, lower urgency — NOT garbage, just unrefined)
- **batting_order defaults to 5 for every hitter — CONFIRMED wrong on the board
  2026-07-21.** RBI/runs props (`mlb_props_model.py:229,437,505`) read
  `player.get("batting_order", 5)` and the hitter dict never carries it, so every
  hitter is treated as a #5 bat and the lineup-slot adjustment is dead. BUT the
  HR Watch tab shows CORRECT order numbers (#1,#2,#3...), so the order data DOES
  exist in the confirmed-lineup JSON — it's just not attached to the player dict
  in the props-scoring path (score_all_props, ~line 980 where `for player in
  game.get("away_lineup")`). Fix: carry each hitter's lineup slot onto the player
  dict there, same place HR Watch gets it. Data is reachable; this is wiring, not
  a scrape. Props still compute correctly meanwhile (PA-based).
- HR/hits/TB/RBI/runs/SB props otherwise verified sound 2026-07-21: matching
  column names, sane fallbacks, current-season rates (SEASON=datetime.now().year).

---

## 🎲 HR WATCH + MONTE CARLO (flagged 2026-07-21, diagnosed not fixed)

**Monte Carlo "Market Implied" column is a REAL bug — dead since built.**
`build_monte_carlo()` in `model/mlb_analysis_sections.py:175` reads
`p.get("ml_away_odds")` / `p.get("ml_home_odds")` off the PICK dict. But
`generate_picks()` (`model/mlb_picks.py:92-100`) never writes odds onto the pick
dict — it writes type/label/team/conf/tier/game. Odds live on the scored-game
dict, carried as `p["game_data"]`. So pick_odds is always None -> market implied
"N/A" -> EV tag "No Market" on every row. The hit-rate bars work (they just
simulate model_p), but the model-vs-market EV comparison — the whole point of the
tab — has never functioned.
Fix: read `p.get("game_data",{}).get("ml_away_odds")`, or copy ml_away_odds/
ml_home_odds/away_team onto the pick dict in generate_picks. Verify prep_picks
doesn't strip game_data before the analysis sections run.

**HR Watch is probably NOT broken — it's gated on confirmed lineups.**
`build_hr_watch()` filters `all_props` for prop_type=="HR" AND not projected AND
confidence>=0.08. HR props are only generated for confirmed lineups
(`score_all_props` skips unconfirmed games). "No HR candidates — lineups not yet
confirmed" is the correct message pre-lineup. TEST: re-check after lineups lock
near game time. If candidates appear -> working as designed. If STILL empty after
confirmation -> real bug, check whether HR props clear the 0.08 threshold at all
(and note the K-rate-style team-data path is separate; HR uses hr_per_pa which
the hitter scraper writes correctly).

---

## 💡 FEATURE IDEA: sportsbook lines card + interactive player profiles (scoped 2026-07-21)

Justin wants RotoBot-AI-style interactive player pages (per-game trend bars over
L5/L10/L20, splits, prop line vs actual) and a visible sportsbook lines display
per matchup. Talked through, not started. Foundation is mostly already in place.

### Step 1 — Sportsbook lines matchup card (SMALL, do first)
The odds scraper ALREADY captures everything needed, from 5 books (DraftKings,
FanDuel, BetMGM, Caesars, PointsBet):
  - ml_away/ml_home (+ open/now/move), rl_away_line/price, rl_home_line/price,
    total_line, total_over_price, total_under_price
All stored on scored_games. This is a DISPLAY gap, not a data gap. Build a
matchup card in run_picks_html.py showing RUN LINE / MONEYLINE / TOTAL for both
teams (like RotoBot's matchup grid). No new scraping.
Note on Hard Rock: the Odds API supports a fixed book list; Hard Rock is not
reliably in it. Show DraftKings or the consensus line instead — that is what
sharp tools display anyway.

### Step 2 — Basic player trend page (MEDIUM)
`player_game_logs` already stores per-game ab/h/hr/rbi/k/tb/sb + opponent/venue/
pitcher_hand, and `player_prop_history` stores line-vs-actual. That is the exact
data behind RotoBot's per-game bars. Build a /player/<name> route rendering the
last N games as hit/miss bars against the prop line, plus L5/L10/L20 toggle.
DEPENDENCY: verify player_game_logs is actually accumulating rows — its scraper
had the transaction-abort bug fixed 2026-07-21; confirm it fills cleanly before
building charts on top of it.

### Step 3 — Full RotoBot parity (LARGE, decide before committing)
Percentile rankings (89th-pct SLG etc.) need league-wide distributions computed
across all players. The polished interactive feel is a React SPA; the current
site is server-rendered HTML. This tier is closer to a frontend rewrite than a
feature — weeks, and an architecture decision. Do only if the interactive
frontend is worth committing to.

---

## ✅ K PROP REBUILT on real Pinnacle lines 2026-07-22 (over-only; UNDER pending)

**SHIPPED:** K props now score against Pinnacle's REAL strikeout line + prices,
not the old fictional 0.8x-projection line. Free (Pinnacle guest API), sharp book.
- `scrapers/mlb_pinnacle_scraper.py`: `fetch_strikeout_lines()` parses the
  `units=="Strikeouts"` specials (pitcher from `special.description`, line+prices
  from `/markets/straight`); `save_strikeout_lines(date)` ->
  `raw/mlb_pinnacle_k_lines_<date>.json`; `load_strikeout_lines(date)`.
  VERIFIED live 2026-07-22 via `/admin/pinnacle-k-test`: 8 pitchers, real lines
  (Ober 4.5, Singer 5.5, Cecconi 3.5...) with both prices.
- `model/mlb_props_model.py`: loads `pinnacle_k` by normalized pitcher name; both
  K call sites use the real line + over/under prices; NO line => NO bet (skip, do
  not invent). `score_k_prop` computes BOTH directions + EV vs the real price.
- Pulled in the afternoon refresh AND the Refresh Lineups button (free). File
  synced to R2 (`mlb_pinnacle_k_lines_*.json` in SYNC_PATTERNS).
- Verify routes: `/admin/pinnacle-test` (feed live?), `/admin/pinnacle-k-test`
  (parser + prices).

### ✅ UNDER direction ENABLED 2026-07-22 (side-aware grading landed)
`ALLOW_UNDER_K = True`. Full side-aware prop grading is in:
- `player_prop_history.pick_side` column (OVER/UNDER, default 'OVER') — added in
  both the CREATE TABLE and an idempotent `ADD COLUMN IF NOT EXISTS` migration in
  `schema.py create_all()`. Existing rows default 'OVER' (all were over-picks).
- `save_prop_pick(pick_side=...)` stores it; caller `run_pipeline.py` passes
  `pp.get("pick_side","OVER")`. Batter props have no pick_side => default OVER.
- `grade_prop_pick` still stores the OUTCOME direction in `result`
  (OVER/UNDER/PUSH) — correct, unchanged. WIN = (result == pick_side).
- All hit-rate queries in `db/picks_store.py` and the yesterday-props panel in
  `run_picks_html.py` now count `result = COALESCE(pick_side,'OVER')` as a hit.
- NAMING: the prop dict's `side` means away/home/pitcher (participant); the bet
  direction is `pick_side` (OVER/UNDER). Do NOT conflate them — score_k_prop
  returns `pick_side`, the wrapper keeps `side`="pitcher".
- Fixed along the way: the yesterday-props panel counted result=='WIN' which
  NEVER matched (result is OVER/UNDER/PUSH) — was always 0-0; now side-aware.

### Coverage note
Only pitchers Pinnacle lists (~8/slate) get a K prop now, vs ~13 fictional ones
before. Correct tradeoff. The PROJECTIONS VIEW (every starter's model number +
real line where it exists + edge both ways, unfiltered, missing flagged) is the
follow-up that surfaces the rest and turns "gems Justin finds manually" into
model output. Ties to the player-stats/RotoBot track.

---

## 🗑️ (superseded) original finding: K prop fictional line

`score_all_props` sets the strikeout line to `_line = round(_exp * 0.80 * 2)/2`
(`mlb_props_model.py:1041,1290`) — i.e. 80% of the model's OWN projection — and
`score_k_prop` only computes P(over). Consequences:

1. It is NOT comparing to a real sportsbook line. It invents an easy line below
   its projection and bets over, so almost every pitcher WITH data looks like a
   favorable OVER. The graded "record" measures beating 0.8x the model's own
   projection, NOT beating a book.
2. UNDER plays are invisible. A soft-tosser vs a low-K team is a strong UNDER and
   the model never surfaces it (low over-prob -> SKIP -> None).
3. "No pitcher prop for some games" has two indistinguishable causes:
   - `k9 < 1.0` (line 647): pitcher not in `mlb_pitcher_stats_master.csv` — a
     rookie/callup or a name-match miss between schedule and stats master.
   - `tier == SKIP` (line 671): below display threshold (rare given the self-set
     line). Both render as silence, so the user cannot tell "looked and passed"
     from "never looked."

### The fix (dedicated props-model session, not a quick patch)
- **USE PINNACLE, NOT THE ODDS API (discovered 2026-07-22).** The existing
  `scrapers/mlb_pinnacle_scraper.py` hits Pinnacle's FREE unauthenticated guest
  API (`guest.api.arcadia.pinnacle.com/0.1/leagues/246/matchups?brandId=0`) and
  that feed ALREADY contains pitcher strikeout props as `type=="special"`,
  `units=="Strikeouts"` matchups, e.g.
  `special.description = "Bryce Elder (Total Strikeouts)(must start)"` with
  Over/Under participants. The line + prices are in the `/markets/straight`
  endpoint the scraper already fetches, keyed by matchupId. So real K lines cost
  ZERO Odds API quota, and Pinnacle is the SHARPEST book = best CLV reference.
  Extend the scraper to parse the strikeout specials; match "Bryce Elder" to the
  schedule's probable pitcher (name-match, same class as Kalshi/platoon).
  VERIFY the specials are current for the day's starters before wiring in — the
  sample pulled had a stale May start time.
  (Odds API cost was verified as a fallback only: pitcher_strikeouts = 1 credit
  PER GAME on the per-event endpoint, ~15/day ~450/month — fits the 500 cap only
  if pulled once/day + cached. Batter props ~1350/month, do NOT fit. Prefer
  Pinnacle; keep Odds API as the documented fallback.)
- Score BOTH over and under against the real line so UNDER gems surface.
- Match starters by player_id, not name, so callups stop falling through.
- Build a PROJECTIONS VIEW: every starter, real line, model projection, edge in
  both directions, unfiltered, with missing-data flagged (not hidden). This is
  the surface that turns "hidden gems Justin finds manually" into model output
  and exposes coverage gaps at a glance. Ties into the player-stats/RotoBot track.

---

## Active Work Queue
1. **Resolve the 51.8% vs 46.0% split** between live-saved and backfilled picks
   (see Calibration Findings). Gates all model work.
2. **Trace the tier/conf mismatch** — tier labels do not match their documented
   confidence ranges.
3. **Run line** — decide whether to suppress RL entirely (39-44% over 222 picks).
4. **Totals cap** — replace `min(0.74, ...)` at `mlb_model.py:972` with something
   that keeps discriminating past ~1.7 runs of edge.
5. **K prop inflation (props, lower priority)** — `mlb_props_model.py:775` reads
   `strikeout_rate`; the CSV column is `k_rate`. Falls through to a fallback whose
   denominator defaults to 1, so `kr` = raw season strikeout total (~1420). That
   blows past the 1.4 clamp at line 641, so EVERY team maxes the multiplier and
   every K projection is inflated ~40%. Also: `mlb_team_hitting_master.csv` only
   holds 2023-2025, and line 772 keeps the first row per team, so even fixed it
   would score 2026 games on 2023 rates.
6. **Batter Statcast 1-row load bug** — 532-row CSV produces 1 row on model load.
7. **Game picks independent of lineups** — ML/RL/total at 6am, props wait for
   lineup confirmation.
8. **Projected props** — publish early for everyday regulars (PROJECTED).

## Housekeeping / latent issues
- `market_signal` is used by `save_picks` but is **not in `db/schema.py`** — no
  column definition, no migration. It exists in production only. A DB rebuilt via
  `create_all()` would make every pick write fail silently.
- `routes/routes/analytics.py` is a stray duplicate of `routes/analytics.py`.
- `kalshi_private.pem` was untracked on 2026-07-20 but remains in git history —
  **rotate the Kalshi key.**
- Kalshi markets span multiple dates; `extract_game_probabilities` does not filter
  by date, so a future game can collide with today's. Ticker carries the date.
- CLAUDE.md route list and env vars drifted ~27 commits behind before 2026-07-20;
  `/force-refresh` documented below does not exist (actual route is `/refresh`).

## ✅ SIGNAL AUDIT: 5/79 dead as of 2026-07-21 4:40pm ET (was 25)

The 5 remaining are all CONSTANT-but-not-broken: `away_bp_found`/`home_bp_found`
(data found = healthy) and the rest-days trio `away_rest`/`home_rest`/`rest_ml_adj`
(0 = "played yesterday", the normal state in daily-play baseball, not a failure).
Functionally there are no dead inputs left.

**Verified live on the 4:40pm adaptive refresh (first clean run after all fixes):**
- Polymarket: **15/15** game markets fetched, zero abbreviation misses. The 6am
  run (pre-deploy) logged `parsed 0`; the 4:40 run logged `parsed 15` — proves the
  slug-builder rewrite is what fixed it.
- Kalshi: `Fetched 70 markets` → `Parsed 17 unique game markets`.
- **`poly_market_signal` = DIVERGE/NEUTRAL for the first time ever.** CONFIRM/DIVERGE
  needs BOTH feeds; it was NO_DATA since inception. Now producing real output.
- Umpire (11 distinct), bullpen fatigue, lineups, platoon, combined_away/home_prob
  all OK.
- total_signal fix (run-unit thresholds) deployed; will show DRIFT/STEAM on the
  next refresh where a total moved 0.5+ runs.

Log-viewer note: Railway's filtered log window doesn't reliably tail to "now".
`filter=Polymarket` topped out at Jul 20; `filter=game markets` surfaced today's
20:40 UTC lines. Use a content filter that matches the exact log string.

---

## 🔬 SIGNAL AUDIT — /admin/signal-audit (built 2026-07-20)

Scores today's slate and reports, per model input, whether it actually VARIES
across games. CONSTANT means every game got the same value, so the signal cannot
differentiate picks no matter its weight. This is the tool that found the dead
subsystems below — use it before ever touching model weights.

**Tonight's progression: 25 → 31 → 20 → 18 of 79 dead.**
(The rise to 31 was deploys wiping `data/raw/` before the R2 sync fix landed.)

### Now live for the first time
- **Bullpen fatigue** — tiers now TIRED/SPENT/NORMAL, `bp_pitches_1d` 24-203.
  Documented as a -5% to +20% swing on bullpen ERA; it had never once fired.
- **Umpire** — 15 distinct names, `ump_factor` -0.212 to 0.008, `ump_rpg` 8.47-9.02.

### Still dead (18)
- **Platoon splits (4) — UNRESOLVED, highest-value remaining.** The master CSV is
  now populated (374 kB in R2, 3,492 rows) and the "vs Left" vs "vs. Left" label
  mismatch is fixed via `_canon_split()`, yet all four fields still return None.
  Both sides run through `normalize_player()` (`" ".join(...).title()`) so names
  *should* match, and `get_pitcher()` on the same name works. Next step: dump
  `self.pitcher_platoon` keys next to `away_sp_name` for one game and compare.
- **Polymarket (4)** — all probs None, signal NO_DATA. Separate from Kalshi.
- **Kalshi combined (2)** — needs both Kalshi and Polymarket; blocked on the above.
- **Lineups (3)** — recoverable any time via `/force-lineups` (free, no Odds API).
- **Rest days (2)** — `away_rest`/`home_rest` always 0, forcing `rest_ml_adj` to 0.
- **total_signal / total_adj** — `total_adj` is a flat -0.01 on every game, so
  totals get zero differentiation from adjustments. Combined with the 0.74 cap at
  `mlb_model.py:972`, this is why TOTAL at 70-75 wins 49% on n=204.
- `bp_found`, `lineup_confirmed` — CONSTANT-but-healthy, ignore.

---

## Fixed 2026-07-24 (value/EV + RL rebuild + props grading + email)

**⚠️ This bundle CHANGES pick generation (RL + ML confidence). It feeds the
consensus worker — verify CONSENSUS still parses picks after deploy.**

- **Props were never graded (Props 0-0).** Props only got a save attempt at 6am
  in `run_pipeline.py` step 8b, but lineups don't post until ~2-3h before first
  pitch, so every prop was `projected` and skipped → `player_prop_history` empty
  → nothing to grade. FIX: added a prop-save step to `run_afternoon.py` (Step 3c)
  which runs AFTER lineups lock, so confirmed props persist and grade next 6am.
  `save_prop_pick` is ON CONFLICT DO NOTHING so it's safe to re-run.

- **Daily Summary never populated (real root cause).** The live ticker
  (`fetchLiveScores`/`refreshTicker` in run_picks_html.py) fetched finished games
  from statsapi but dumped them into `track.innerHTML` and NEVER set
  `window._liveGames`. So `_liveScores()` always fell back to `DATA_SCORES` (the
  score-less 6am snapshot). Prior "fixes" only touched the picks side. FIX: ticker
  now sets `window._liveGames` (with full team names for matching) + re-renders the
  summary every 2 min. Also fixed TOTAL grading in both JS graders (was using the
  model projection `exp_total` and couldn't parse "OVER 8.5"; now parses the line
  from the label) and RL grading (both JS graders treated RL like ML — now parse
  the ±1.5 spread from the label; the DB grader `grade_pick` was already correct).

- **market_signal added to `db/schema.py`** (CREATE TABLE + idempotent migration).
  Was prod-only; the analysis query needs it.

- **NEW: market VALUE / EV engine (`model/value.py`).** De-vigs the two-way price,
  compares to the model prob, computes EV per $1. Confidence ≠ value: a 70% team
  at -240 is NO VALUE (-0.8% EV) + CHALK; a +150 dog the model likes is +15% EV.
  `value_for_pick(pick)` attached to every pick in `generate_picks`; serialized in
  `prep_picks`; shown on cards as a VALUE/FAIR/NO VALUE row + EV + "Model x% vs
  Market y%", with a ⚠ NO VALUE / CHALK badge in the card header. DISPLAY ONLY —
  does not filter picks. (Report-level value needs price stored per graded pick in
  the DB — deferred.)

- **RL prices surfaced onto the scored game** (`mlb_model.py` score_game dict):
  `rl_away_line/price`, `rl_home_line/price`, `total_over_price`,
  `total_under_price`. The odds scraper already captured them; they just weren't
  on the scored-game dict. Needed for RL value + the RL rebuild.

- **RL REBUILT (the big one).** OLD logic: always bet the favorite -1.5 with
  `rl_conf = 0.50 + (wp-0.60)*0.80` — i.e. cover% scaled straight off ML win%.
  That conflates "wins" with "wins by 2+" and is why RL ran ~40%. NEW: `run_margin_probs(exp_home, exp_away)`
  in mlb_model.py computes P(win by 2+) from a Poisson run-margin (Skellam) on the
  model's per-team expected runs. The RL block now prices BOTH favorite -1.5 and
  dog +1.5 (=1 - fav cover) and publishes only the side with POSITIVE EV vs the
  real RL price (falls back to a cover-prob edge when prices missing). Net effect:
  favorite -1.5 (~34-40% cover) mostly stops publishing; +EV dog +1.5 surfaces.
  Sanity: avg favorite covers -1.5 ~34%, big favorite ~51% — realistic. RL labels
  can now be "+1.5"; all graders handle it.

- **Confidence: stopped boosting chalk the model over-loves.** `market_agreement_adj`
  used to return +0.01 when `model_gap > 0.10` (model way above a short market) and
  +0.02 in the 0.04-0.10 band. Now: `>0.10` FADES (-0.02), 0.04-0.10 trimmed to
  +0.01. This targets the 77%+ overconfidence WITHOUT a blunt cap — deliberately
  NOT capping ML top-end, because ML 75%+ is the model's best slice (67.5%/63.8%);
  the top-end rot was short-priced RL/TOTAL, now handled by the RL rebuild + value
  flag. Do not add a hard 75% ceiling; it would fade the one proven edge.

- **NEW: nightly analysis report + email.** `analysis_report.py` (day record +
  21-day trends → Claude narrative), route `/admin/analysis` (?date, ?download,
  ?email), emailed after 6am grading via `run_pipeline.py` step 7b using a new
  `send_html_email()` in `alerts.py` (reuses ALERT_EMAIL_* Gmail creds). Needs
  ANTHROPIC_API_KEY (set) for narrative, else raw numbers.

- **Parlay cards show real combined American odds** (from each ML leg's book price,
  `build_parlays` → `book_odds`), not the old fixed +260. Prop cards show the real
  Pinnacle line + price, side-aware (K unders read "Under"). Top Players lowered to
  3+ picks.

### Follow-up bundle (same day)

- **CONFIRMED DATA BUG — mixed confidence scales in `picks`.** Backfilled picks
  (`/admin/grade-backfill`) stored conf on a 0-100 scale (the analysis JSON saves
  `round(conf*100)`) while live `save_picks` stores 0-1. So `width_bucket(conf,...)`
  in calibration dumped every backfilled row into the overflow bucket — the report's
  "77-9000%" / "77%+ collapsing, n=158" band is an ARTIFACT (it's just all the
  backfilled ~46% picks), NOT genuine top-end overconfidence. FIX: backfill insert
  now normalizes conf/100 when >1.5; one-time route `/admin/fix-conf-scale` divides
  existing 0-100 rows by 100 (idempotent, conf-only). **RUN /admin/fix-conf-scale
  ONCE after deploy**, then calibration/report confidence bands become trustworthy.

- **HR/SB props suppressed from the BETTABLE surface** (`SUPPRESS_BETTABLE_PROPS =
  {"HR","SB"}` in run_picks_html `prep_props`). They were OVER-only, fixed-0.5-line,
  no-real-price bets losing structurally (HR 4/26≈15%, SB 6/36≈17%). Suppression is
  DISPLAY-ONLY: HR Watch (built from raw `props`, not prep_props) is unaffected, and
  grading continues (save loops read raw props), so the record keeps accruing. The
  real fix (real book lines + both directions + EV, like K props got) is deferred —
  needs a free/affordable batter-prop line source; Odds API batter props (~1350/mo)
  don't fit the 500 quota.

- **Reminder: DO NOT pool pre/post-2026-07-21 picks for calibration.** The 21-day
  report window is mostly the pre-fix blind model. Re-run calibration filtered to
  `pick_date >= '2026-07-21'` once ~300 post-fix graded picks exist. TOSSUP-ML edge
  (61%, n=18) and 71-74% sweet spot (n=17) are pre-fix + tiny — do not tune on them.

### Third bundle (same day) — Best Bets, per-card Analysis, K bug (2nd path)

- **K opponent-rate bug — SECOND loader fixed.** `score_all_props` team-K loader
  was fixed 2026-07-21, but `score_projected_props` had its OWN duplicate loader
  (mlb_props_model.py ~1204) still reading `strikeout_rate` (wrong col) with a
  denominator defaulting to 1 → kr = raw season K total (~1241) → "124100%" on the
  board + maxed K-OVER multiplier. Since lineups are unconfirmed most of the day,
  the PROJECTED path drives the visible props, so the bug was live. Replaced with
  the same validated loader (reads `k_rate`, clamps 0.10-0.35, latest season).

- **NEW: per-card Analysis dropdown** (`buildAnalysis(p)` in run_picks_html.py).
  A second dropdown on every pick card (next to Player Props) that generates a
  plain-English honest read from the pick's own numbers: market breakeven vs
  price, model-vs-market gap credibility (flags >15-pt gaps as mirages), the
  calibration-band warning (75-84% overconfident, 85-90% trusted), chalk warning,
  and a bottom-line verdict. Pure JS, no API. Bakes the whole framework into the
  card so the user doesn't have to remember it.

- **NEW: Best Bets surface** (`bestBetEval`/`renderBestBets`, container `#bestBets`
  above `#picksGrid`). The model filters FOR the user. A pick qualifies only if:
  type ML (`BEST_BET_TYPES`), price >= -180 (`BEST_BET_PRICE_FLOOR`, kills deep
  chalk), model-vs-market gap in [0,15] (`BEST_BET_MAX_GAP`, kills mirages), and
  EV still positive when the model prob is blended 50/50 with the market (strips
  overconfidence). Ranked by that honest blended EV. On 2026-07-24's board it
  surfaced Giants/Phillies/CWS/Twins/Cards/Rays and excluded Tigers -294 +
  Brewers -248 (chalk) and Arizona/Mariners ML (20-pt mirages). Tune the 3 consts
  to change price ceiling / add bet types.

### RL guards + unproven-value muting (same day)

- **Impossible-price guard.** `american_to_decimal` (model/value.py) now returns
  None for `|price| < 100` (e.g. -57). Cause: the odds scraper averages every
  book's run-line price into one bucket regardless of line, so a near-even game
  where books split (Braves -1.5 +180 vs +1.5 -220) averages to garbage like -57.
  Also guarded in `mlb_model.py` RL block and `prep_picks` `_pick_price`. **Real
  fix queued: pair line+price per book in the scraper (task).**

- **RL cover-prob reality caps** (`mlb_model.py`): `RL_FAV_COVER_CAP=0.55`,
  `RL_DOG_COVER_CAP=0.68`. Poisson cover can run past these when per-team expected
  runs are hot (3.9-run projected margins), producing absurd cover%/EV. Clamp
  until the run-environment projection itself is reined in.

- **RL value display muted.** `valueHtml`/`buildAnalysis` give RL an honest
  "RUN LINE · UNPROVEN — Model cover X%, tracking results, no verdict yet" note
  instead of a loud VALUE/EV (which read as fake +70%/+96% edges on hot
  projections + bad prices). RL already excluded from Best Bets. Un-mute once the
  scraper price fix lands and ~2 weeks of graded RL results exist.

## Fixed 2026-07-30 — TOTALS overprojection (suppression cap)

- **Root cause of 13-15 run totals + OVER bias:** `exp_runs` used
  `suppression = blended_era / LEAGUE_era` UNBOUNDED. A spot starter's 8.00 ERA (or
  a recent blow-up start getting 30% weight) pushed suppression past 1.5x -> 7+
  projected runs/team -> every total an OVER. FIX (`mlb_model.py` exp_runs): clamp
  suppression to [0.68, 1.30] before AND after the Statcast nudge. Verified: avg
  game unchanged (9.2), ace duel unchanged (7.2), two-bad-SP 13.8->11.8, hitter-
  park blowup 14.1->13.0. Only the extremes move.
- The 0.74 total_conf cap was ALREADY trimmed to 0.68 (mlb_model.py:1118) earlier.
- Validate on post-fix data before further totals changes — the overprojection was
  structural (a projection bug), but whether TOTAL has edge still needs clean grades.

## Changed 2026-07-30 — analysis/trends floored at post-fix boundary

- `analysis_report.build_data_pack` trend cutoff = `max(date-21d, "2026-07-25")` so
  trends/calibration NEVER pool pre-fix (blind-model) picks with the current model.
  System prompt updated: window is post-fix, small-sample, first clean read on the
  rebuilt RL/totals/confidence — don't over-conclude, and don't recommend RL/HR/SB/
  conf changes that are already done. Stats & Trends tab inherits this via build_data_pack.
- **Model-tweak brief triage (2026-07-30):** most points in the user's brief + the
  model's own "ideas" were reading the CONTAMINATED 21-day pool. Already done: RL
  rebuilt, HR/SB suppressed, conf-scale fixed, chalk faded, Best Bets = prune-the-
  middle. Needs clean data (~300 post-fix picks, ~2-3 wks): isotonic recalibration,
  K-model. Genuinely actionable NOW (structural, data-independent): TOTALS
  overprojection (exp_total runs hot + the min(0.74) cap at mlb_model.py:972).

## Fixed 2026-07-29 — CRITICAL: schedule column-shift corrupted team names

- **Symptom:** every pick's `home_team` was blank or a player id ("656550"), so
  Best Bets showed "Team @ " and RL cards "Team @ 666200". away_team was fine.
- **Cause:** `append_to_master` (normalize/mlb_normalize.py) opened the master in
  APPEND mode and only wrote a header if the file was absent. When the probable-
  pitcher-ID columns were added (2026-07-27), it began appending 15-col rows under
  the old 13-col header, so DictReader read `home_team` from the away pitcher's id
  column (shift-by-one). CONFIRMED via live DATA_PICKS (43/43 picks bad).
- **FIX:** `append_to_master` now detects a header change and REWRITES the whole
  file with the union schema (old rows get "" for new cols), so columns can never
  shift again on a schema change. Verified with a unit test.
- **Repair route `/admin/rebuild-schedule`:** deletes the corrupt master + fresh
  re-scrape + normalize + dashboard rebuild (no Odds API). **Run it once after
  deploy** to fix the current board; the append fix prevents recurrence.

## Improved 2026-07-27 — Players coverage (40-man + props, higher limit)

- Prop-card players (Sandy Alcantara etc.) came up empty because `seed_all_players`
  used rosterType="active" (excludes IL/rehab/some probables). FIX: `roster_players()`
  now uses "40Man" (broadest standard roster), and `/admin/refresh-gamelogs` seeds
  today's PROP players FIRST (exact IDs on cards) THEN all 40-man rosters, deduped.
  So every prop-card player is guaranteed covered and searchable.
- `search_players` LIMIT 60 -> 300 so the directory shows more.
- **RE-RUN /admin/refresh-gamelogs after deploy** to pick up the broader set.

## Fixed 2026-07-27 — Players data ACTUAL root cause (empty game_date)

- **THE bug (found via `/admin/gamelog-diag`):** `fetch_game_log` read the date
  from the nested `game` object (`game.officialDate`) which is EMPTY in gameLog
  splits — the date is at `split["date"]`. So every row had game_date="" and
  `upsert_game_logs` SKIPPED all of them (its no-game_date guard). Result: MLB API
  returned 84 rows for a player, 0 written, table always empty. FIX: read
  `split.get("date")` first. Diagnostic confirmed fetch=84 rows, upsert=0 before,
  which pinpointed it exactly.
- `seed_all_players()` (all 30 active rosters) now populates everyone via
  `/admin/refresh-gamelogs`; `gamelog_diag` self-heals the unique index too.
- **After deploy: hit /admin/refresh-gamelogs once** to backfill the whole league.

## Fixed 2026-07-27 — game logs seed from props (not confirmed lineups) + pitcher logs

- **Root cause of empty Players data:** gamelog scraper only pulled CONFIRMED
  lineup players, and lineups often aren't confirmed when it runs -> 0 rows forever.
  FIX: new `run_for_players(players)` seeds from an explicit list; the admin route
  `/admin/refresh-gamelogs` and run_afternoon step 3d now build that list from
  today's PROPS (score_all_props -> player_id + side), which ALWAYS exist. So the
  table fills regardless of lineup state — hit /admin/refresh-gamelogs anytime.
- **Pitchers now get real logs.** `fetch_game_log(pid, group="pitching")` pulls the
  pitching game log; `k`=strikeouts THROWN (the useful chart for K props), h/hr/bb
  = allowed. Player page auto-detects pitchers (all ab==0) and shows pitching
  charts (Strikeouts thrown / Hits allowed / Walks / HR allowed) with correct labels.
- **Prop-card links fixed on the REAL card** (`renderProps` #propsGrid `.prop-player`),
  not just the top-props card. Face + name link to /player/<id>.

## Fixed 2026-07-27 — Players section wiring (clicks + empty search)

- **Prop-card links were on the WRONG card.** The face/link were added to the
  Top-Props/inline card, but the main Player Props tab uses `renderProps()`
  (`#propsGrid`, prop-card with Line/Projection/Sportsbook rows). Added the face +
  `/player/<id>` link to the `.prop-player` name there. All 259 props DO carry
  player_id (verified live) — it was purely a render-location miss.
- **`player_game_logs` was EMPTY → Players search/pages blank.** The gamelog
  scraper's `get_lineup_players()` reads today's CONFIRMED lineup JSON, but it only
  ran in the 6am pipeline (step 9b) before lineups post, so it found nobody every
  day and the table never filled. FIX: run `mlb_player_gamelog_scraper` in
  `run_afternoon` (step 3d) after lineups lock, + on-demand `/admin/refresh-gamelogs`
  to populate immediately once lineups are up. Fetches each player's FULL season
  log, so the table + trend charts fill fast once it runs with real lineups.

## Added 2026-07-27 — Players section (search + per-game trend pages)

- **NEW `player_data.py`**: `search_players(q,team)` + `get_player(id)` over
  `player_game_logs` (per-game h/tb/hr/rbi/k/sb + opponent/date).
- **`/players`** — searchable directory (name/team) with headshots, links to pages.
- **`/player/<id>`** — RotoBot-style per-game vertical bar charts for Hits/TB/HR/
  RBI/K/SB with L5/L10/L20 toggle. Both routes site-auth gated.
- **Clickable players**: prop card face + name now link to `/player/<id>`. Header
  has a 👤 Players link.
- **DEPENDENCY**: pages are empty if `player_game_logs` isn't accumulating. The
  gamelog scraper runs in run_pipeline step 9b (`mlb_player_gamelog_scraper`); its
  transaction-abort bug was fixed 2026-07-21. If a player page shows "No game logs
  yet," verify that table is filling (check /analytics or db). Runs column not in
  logs, so no Runs/H+R+RBI chart yet — add to the scraper + STAT_COLS later.

## Added 2026-07-27 — game cancel/delay status on cards

- **Weather/disruption status surfaced.** `fetchLiveScores` now captures each game's
  `detailedState` and an `abnormal` flag (postponed/cancelled/delayed/suspended) and
  no longer drops non-Final/Live games. `statusBadge()` + `_statusFor()` render a
  badge (⛔ POSTPONED / 🚫 CANCELLED / ⏳ DELAYED / ⏸ SUSPENDED) on Today's Games
  cards (next to lineup badge) and a red banner on Game Picks cards ("bet may not
  stand"). refreshTicker re-renders Today's Games each cycle + Game Picks once on
  first live fetch (window._picksStatusPainted guard) so banners appear.
- **Postponed games no longer mis-grade.** Both `_pickResult` graders skip
  `score.abnormal`, so a 0-0 postponed game is no-action, not a PUSH (was wrongly
  showing Cincinnati games as PUSH in Daily Summary).

## Added 2026-07-27 — frequent odds pulls + freeze at first pitch

- **Frequent Pinnacle pulls** (`_start_frequent_odds` in app.py): recurring thread
  re-pulls Pinnacle ML/RL every 40 min up to the day's LAST first pitch, then
  stops. Free, keeps Sharp Action / line movement current. Guarded (`_frequent_odds_started`)
  so restarts don't stack threads. Started at app startup.
- **Freeze at first pitch** (`MLBModel.load` odds loader): only uses snapshots
  whose `snapshot_time <= game_time_utc`, so a started game locks at its last
  pre-game line and frequent evening pulls never overwrite it with a live number.
- Badge tracker relabeled to read as a W-L record ("🔥 tagged High Confidence
  cards: 33-18 · 64.7%"). NOTE: 📈 record still counts ALL ML 70%+ (superset of the
  80%+ 🔥 cards); split to 70-80% exclusive if the user wants it to match badges 1:1.
- Lineup-confirmed badge on every Today's Games card (green confirmed / amber not set).

## Added 2026-07-26 — Stats & Trends tab (visual bar charts)

- **New "📊 Stats & Trends" tab** (`panel-trends`, `renderTrends()` in
  run_picks_html). Server computes 21-day graded trends via
  `analysis_report.build_data_pack(today)` -> embedded as `DATA_TRENDS`
  (`__TRENDS__`). Four CSS bar charts (no external lib): win% by confidence band,
  by bet type, by tier, and prop hit% by type. Bars colored green/red vs the
  52.4% break-even line (white marker). To add more charts, extend build_data_pack
  and add a `chart(...)` call in renderTrends.
- **DONE: player headshots on prop cards.** MLBAM `player_id` now flows onto every
  prop: batters from the lineup dict, pitchers from a NEW `away/home_probable_pitcher_id`
  column added to mlb_scraper + mlb_normalize + game_pitchers. prep_props serializes
  `player_id`; the prop card renders `img.mlbstatic.com/.../people/<id>/headshot/67/current`
  with onerror hide. NOTE: pitcher (K-prop) faces only appear AFTER the next 6am
  schedule re-scrape populates the new id column; batter faces work immediately.

## Added 2026-07-26 — Badge tracker + full admin index

- **HIGH CONFIDENCE / PROFITABLE tracker** (`renderTracker` in run_picks_html,
  `#trackerWidget` on the LEFT of the yesterday/bankroll row). Shows the running
  graded record of the two badge categories (elite = 🔥 High Confidence, wide =
  📈 Profitable) from `compute_high_conf_rule()` (live DB), green if >52.4%. Same
  source the board badges use, so it reconciles with the Yesterday tab.
- **Admin hub rebuilt** (`/admin`) into a full organized index: Daily views,
  Analysis & performance (analysis report w/ date picker, calibration, signal
  audit, analytics, model-config), Diagnostics (pinnacle tests, status), Actions
  (force-pipeline/odds, refresh-signals, grade-backfill, unstick, pw). Header now
  has 📋 Analysis + 🗂 Menu links so past-day analysis is one click.
- Records mismatch (Yesterday tab 24-19 vs analysis report 20-16) = point-in-time
  report (generated 7:52am) vs live tab. Not a math bug. TODO: make the report
  read live at view time / show its generation time prominently.

## Fixed 2026-07-25 — Daily Summary REAL root cause (finished games dropped)

**The long-running "Daily Summary not working" bug — actual cause found via live
console inspection.** `window._liveGames` + the live-score matching were FINE all
along. The problem: `today_picks_all` (→ `DATA_TODAY_PICKS`) was set to `picks`
whenever `actual_date == today`, and `picks` comes from `score_today()`, which
**deliberately excludes completed/in-progress games** (`mlb_model.py:1483`,
`_game_is_over`). So the instant a game went Final it was removed from the picks
dataset — the exact games the summary needs to show. Confirmed live: 34 picks
across 14 games, Kansas City @ Detroit (Final 3-2) had ZERO picks in the data.
FIX (`run_picks_html.py` ~4794): ALWAYS build `today_picks_all` by scoring
`all_schedule` (the full slate incl. started/finished), never from `picks`. The
main grid still uses `picks` (correctly hides finished games from the bettable
board); only the summary dataset includes them.

## Fixed 2026-07-25 — props grading root cause + Pinnacle as PRIMARY odds source

- **PROPS 0-0 ROOT CAUSE (real one).** Neither `run_pipeline` step 8b nor
  `run_afternoon` step 3c pulled the Pinnacle K-line file before calling
  `score_all_props`, so K props generated 0 rows (no line = no bet) and nothing
  was saved to `player_prop_history` to grade — 23rd AND 24th both blank. FIX:
  both now call `save_strikeout_lines()` before scoring props. (Note: the K-prop
  loop in score_all_props ~line 1091 is OUTSIDE the `lineup_confirmed` gate, so K
  props do NOT need lineups — they only need the Pinnacle file present.)

- **PINNACLE IS NOW THE PRIMARY ODDS SOURCE (was Odds-API-first).** The Brewers
  card showed -259 when the real line was -115 because `mlb_odds_scraper`
  arithmetic-averages American odds across books (mathematically wrong) and the
  running model only reloads odds at startup. `mlb_pinnacle_scraper.run()` ALREADY
  existed (built as a quota fallback) and pulls ML (s;0;ml), total (s;0;ou), and
  run line (s;0;s) with PAIRED line+price per matchup, writing the same
  `mlb_odds_master.csv`. Flipped to Pinnacle-first in 3 places: `run_pipeline`
  step 3, `run_afternoon` step 1, and `_run_odds_snapshot()` in app.py. Odds API
  is now the fallback only (saves ~30 quota/mo AND fixes accuracy + the RL -57
  garbage, since Pinnacle needs no cross-book averaging).
  - **VERIFY AFTER DEPLOY:** hit `/admin/pinnacle-odds-test` (dry run, no writes,
    no quota) — confirms all ~15 games parse with correct team names. The A's are
    the one to watch: schedule uses "Athletics", Pinnacle map outputs "Oakland
    Athletics" (TEAM_NAME_MAP) — if the A's game shows no odds on the board after
    deploy, align that mapping.
  - STILL TODO (task): frequent Pinnacle pulls up to first pitch + freeze per game
    at start ("ML should update to gametime then lock"). Right now it's 6am +
    afternoon + force-odds (all now accurate via Pinnacle).
  - **PARSER BUG FOUND + FIXED 2026-07-25 (dry-run showed 0 games parsed).** The
    real Pinnacle market key formats (verified live): moneyline = `s;0;m` (NOT
    `s;0;ml` — the old parser looked for "ml" so it matched nothing → 0 games),
    total = `s;0;ou`, run line = `s;0;s;1.5`/`s;0;s;-1.5` (handicap in the key,
    not a bare `s;0;s`). Also: matchup participants carry NO id in this feed (only
    `alignment` + `order`), so `_parse_markets` now maps prices by LIST ORDER —
    prices[0]=away (participant order 0), prices[1]=home. Totals: [0]=over,
    [1]=under. **VERIFY the away/home mapping isn't flipped** via the DEEP DUMP in
    `/admin/pinnacle-odds-test` (favorite should carry the negative price on the
    correct team) before fully trusting Pinnacle lines.
  - **RESOLVED 2026-07-25: away/home flip fixed** (prices carry `designation`
    home/away/over/under — map by that, NOT list order; home is listed first).
    A's mapped to bare "Athletics" (schedule name). Leaguewide-prop matchups
    (e.g. "Away Runs (15 Games)") filtered via `REAL_TEAMS`.
  - **TOTALS: Pinnacle's free feed has NO clean full-game total** — its s;0;ou
    children are team totals + inning props (lines 0.5-5.5), never the ~8.5 game
    total. Decision: keep ONE Odds-API pull at 6am purely for the total (total
    LINE averaging is safe, unlike ML price averaging), Pinnacle drives ML/RL all
    day, and `MLBModel.load()` BACKFILLS the total onto the latest (Pinnacle)
    snapshot from the most recent snapshot that has one. So run_pipeline step 3
    now runs BOTH (Odds API for total + Pinnacle for ML/RL); afternoon/mid-day
    stay Pinnacle-only and the 6am total carries forward.
  - **EXHAUSTIVE SEARCH CONFIRMED: Pinnacle's guest feed has NO clean full-game
    Total Runs line.** Main matchup = ML + spread only. Every Over/Under CHILD
    matchup is a player prop or inning/derivative total, tagged by `units`
    (TotalBases, HomeRuns, EarnedRuns, HitsAllowed, PitchingOuts, Strikeouts) or a
    low inning line (0.5-4.5). No units=Total / ~8.5 game-total child exists.
    FINAL DECISION: pull the total from the Odds API TWICE/day — 6am (run_pipeline)
    AND the afternoon refresh (run_afternoon step 1, ~2h pre-game ≈ closing) — and
    the model backfills it onto the live Pinnacle snapshots. ~2 Odds pulls/day
    (~60/mo, fine). Do NOT keep hunting the Pinnacle game total; it isn't there.
  - **PROP ROADMAP UNLOCKED (free).** Those units-tagged Over/Under children ARE
    the full prop menu on the same plumbing as K props: batter TotalBases/HomeRuns/
    Hits/etc. and pitcher EarnedRuns/HitsAllowed/PitchingOuts/Walks. Parse a child
    by `units` + its s;0;ou line/prices (line often in the key or points). This is
    the path to expanding props (task 15/props work).

- **NEW: Ask-the-Model (`ask_model.py` + `/ask`, `/ask/answer`).** Natural-language
  Q&A over today's board. `build_board_pack()` scores today's games + picks (with
  value/EV, market %, pitching, signals) + real-line K props + 21-day trends into a
  text pack (cached 10 min), then `answer_question()` sends it to Claude (haiku)
  with a system prompt that bakes in the calibration truths (85%+ trusted, 75-84%
  overconfident, confidence != value, RL unproven, never invent data). `/ask` is a
  standalone page (textarea + async fetch) linked from the dashboard header; both
  routes are site-auth gated by the global before_request. Needs ANTHROPIC_API_KEY
  (set); without it returns the raw board pack. Answers today's board only — extend
  build_board_pack for historical/player-deep questions later.
  - **UPGRADED 2026-07-25: independent analyst.** build_board_pack now scores the
    FULL slate from m.schedule (not score_today, which drops finished/unpicked
    games - why the bot went silent on no-pick games), with richer per-game data
    (starters+ERA, bullpen ERA, offense RPG/OPS, park, weather, market). System
    prompt now tells it to give its OWN read from the raw data AND compare to the
    model, flagging agree/disagree, and never go silent when the model has no pick.

---

## Fixed 2026-07-22 (single bundle)
- **2026 team hitting data.** `SEASONS` in `mlb_team_scraper.py` and
  `mlb_historical_normalize.py` now include 2026. Wired the team scrape + team
  normalize into `/admin/refresh-signals` (the scraper was never in the daily
  pipeline, which is why the master was frozen at 2023-2025). Dedup is by
  (team_id, season) so re-running is safe. RUN /admin/refresh-signals after
  deploy to populate 2026, then confirm K prop opponent factors vary.
- **TBD starter suppression (was documented but NOT implemented).** In
  `mlb_picks.py`: either SP TBD -> suppress TOTAL and RL entirely; both SP TBD ->
  ML capped below LOCK and dropped a tier. Note: "Lineup Not Set" (batting order)
  is a DIFFERENT flag from "Starter Unknown" (pitcher TBD) — a game can have
  confirmed lineups but a TBD starter.
- **Monte Carlo market column.** `build_monte_carlo` now reads odds + away team
  from `p["game_data"]` (it was reading `p.get("ml_away_odds")` off the top-level
  pick, always None -> "No Market" on every row since built).
- **Daily Summary.** `renderDailySummary` now matches picks against the LIVE
  score feed (`_liveScores()`) instead of static baked-in DATA_SCORES, so games
  flow in as they finish. DATA_PICKS always holds today (score_today pivot=False);
  the tomorrow-switch is only a client-side grid toggle.
- **Fire/Profitable track-record banner.** Shows the badge tiers' derived graded
  record (e.g. "🔥 ML 75%+: 87 picks, 65.5%") above the picks grid. Records were
  already computed in compute_high_conf_rule(); this just surfaces them. NOTE: the
  🔥/📈 picks ARE graded — they are plain ML picks in the picks table; the badge
  is a display flag, not a separate pick type.

### Deferred decision (not a bug): true closing line
Odds pull only at 6am + once ~2h before earliest first pitch. There are NO
in-game odds pulls, so sharp action / line movement is ALREADY frozen pre-game —
nothing to "freeze." The only refinement: the afternoon snapshot is 2h out, not
at first pitch, so it is a "2-hours-out" line, not the true close. Capturing the
actual close = one extra Odds API pull near game time (quota cost). Justin's call.

---

## Fixed 2026-07-20
- **DB connection pool leak** (root cause of ~2 months of silent data loss):
  `run_picks_html.py` called `get_conn()` then `conn.close()`, never returning the
  slot to the pool. maxconn=5 drained ~50 min after each deploy, so DB writes only
  succeeded on deploy days. Now uses the `db_conn()` context manager. maxconn 5→10.
  Missing-DATABASE_URL log lifted DEBUG→WARNING (Railway logs at INFO, so it was
  invisible).
- **`/admin/grade-backfill` rewritten** — was a hardcoded 5-date list; now
  enumerates `picks/mlb_analysis_*.json` from R2 and inserts+grades, committing
  per file. Idempotent on (pick_date, game, pick_type, label).
- **`/admin/calibration` added** — predicted vs actual by conf band, pick type,
  and tier, with 95% CIs and thin-sample flags.
- **Kalshi parsing fixed** — titles use ambiguous city names ("Chicago WS",
  "Los Angeles D"); TEAM_ALIASES is keyed on nicknames the titles never contain;
  and the old regex captured "Toronto Winner". Now parses the ticker
  (`KXMLBGAME-26JUL221335PITNYY-NYY`) via `KALSHI_ABBR`, and inverts yes_prob when
  the contract covers the home side. Note: market signal stays NEUTRAL for all
  historical picks — CONFIRM/DIVERGE needs both Kalshi and Polymarket probs, and
  Kalshi has been returning 0 matches, so that dimension was dead the whole time.
- **UTC vs ET date resolution (major).** Railway runs Python in UTC, so a bare
  `datetime.now()` rolls to tomorrow at 8pm ET. From 8pm ET to midnight the model
  looked for *tomorrow's* dated files and silently lost umpires, lineups and
  bullpen fatigue every single evening. Caught live: an audit at 7:5x showed
  `lineup_confirmed=True`, at 8:0x it showed False. Fixed with `_today_et()` in
  `mlb_model.py`, `run_picks_html.py`, `mlb_props_model.py` (6+2+2 call sites).
  `run_pipeline.py` still has naive calls but runs at 6am ET, same UTC date.
- **raw/ JSONs never synced to R2.** `SYNC_PATTERNS` omitted `mlb_umpires_*`,
  `mlb_bullpen_fatigue_*` and `mlb_lineups_*`, so Railway's ephemeral FS destroyed
  them on every restart. Each deploy made the model progressively blinder.
- **CSV sync race.** Railway's healthcheck hits `/` the instant Flask binds,
  triggering a dashboard regen while `download_all()` was still writing. The model
  loaded a half-empty `data/clean/` and scored on defaults, logging only
  "File not found". Now gated on a `_csv_ready` Event.
- **`json` imported inside a conditional block** in `MLBModel.load()` — the umpire
  block used it but only the lineup block imported it, so fixing the ET date
  surfaced an UnboundLocalError. Hoisted to module scope.
- **`/admin/refresh-signals` added** — regenerates umpire, bullpen-fatigue and
  pitcher/platoon data and uploads to R2. Zero Odds API usage (verified: no
  reference to ODDS_API_KEY in any of the three scrapers).
- **Gamelog transaction abort** — an empty `game_date` raised a date syntax error
  that poisoned the transaction, so every subsequent row failed with "current
  transaction is aborted" and logged hundreds of duplicate lines. Now skips empty
  dates, wraps each row in a SAVEPOINT, and caps the log noise.

## Key Coding Conventions
- All scrapers have a `run()` function as entry point
- Data flows: raw scrape → `data/raw/` → normalize → `data/clean/` master CSVs
- Model loads all clean CSVs once via `MLBModel.load()`, then scores on demand
- Non-fatal steps (odds, weather, Kalshi, Polymarket) use try/except + log.warning
- All times in ET (America/New_York) via zoneinfo
- Flask never blocks on pipeline — everything runs in background threads
- Large file edits: use Python-via-bash splice pattern (Edit tool truncates files >~200 lines on Windows FS)
- SQL queries: always verify column names against db/schema.py before writing
- Never call the Odds API during development or testing
- Never delete existing data — additive changes only

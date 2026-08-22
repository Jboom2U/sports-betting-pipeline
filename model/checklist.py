"""
model/checklist.py: the open work list, verified against the repo at request time.

WHY THIS EXISTS (created 2026-08-18)

Justin's words: "Things that I thought we already have apparently have just been
sitting there and I was never prompted to add those."

That is the failure this module is built against. A hand maintained checklist
has exactly the same failure mode as the document that produced the complaint.
CLAUDE.md confidently described tier thresholds of 75/68/60/48 for weeks while
the code used 68/62/52/48, and that phantom got chased more than once. A list
of ticked boxes maintained by hand would have reported platoon splits as wired.

So almost every item here carries a PROBE: a small function that reads the
actual source or data file and reports what it finds. "Done" means the evidence
says done. Nothing is marked complete because someone remembered to mark it.

Items that genuinely cannot be probed (a judgement call, an external action like
rotating a key) are tagged MANUAL and say so on the page, so it is always visible
which statuses are measured and which are asserted.

ADDING AN ITEM
    Append to ITEMS. Give it a probe if you can possibly write one. If the probe
    would be more fragile than useful, set probe=None and it renders as MANUAL.

A PROBE MUST NEVER RAISE. run_probe() catches everything and degrades to
"unknown" with the exception text as evidence, because a checklist that takes
the admin page down is worse than no checklist.
"""
from __future__ import annotations

import os
import re
import time

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")

# Statuses
DONE    = "done"
OPEN    = "open"
PARTIAL = "partial"
UNKNOWN = "unknown"
MANUAL  = "manual"

_CACHE: dict = {"at": 0.0, "rows": None}
_CACHE_TTL = 120          # seconds. Source cannot change between deploys; data can.


# ----------------------------------------------------------------- helpers

def _read(rel: str) -> str:
    """File text, or empty string when absent. Never raises."""
    try:
        with open(os.path.join(BASE_DIR, rel), "r", encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except Exception:
        return ""


def _exists(rel: str) -> bool:
    return os.path.exists(os.path.join(BASE_DIR, rel))


def _count(rel: str, pattern: str, flags=re.I) -> int:
    txt = _read(rel)
    if not txt:
        return 0
    return len(re.findall(pattern, txt, flags))


def _csv_rows(rel: str) -> int:
    """Data rows in a CSV, excluding the header. -1 when the file is missing."""
    path = os.path.join(BASE_DIR, rel)
    if not os.path.exists(path):
        return -1
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            return max(0, sum(1 for _ in fh) - 1)
    except Exception:
        return -1


def _py_files(subdir: str):
    """Python files in a subdir, EXCLUDING this module.

    Learned the hard way on 2026-08-18: the injuries probe counted the word
    "injuries" inside checklist.py itself and reported the item as done. A
    checklist that can see itself will always grade itself complete.
    """
    d = os.path.join(BASE_DIR, subdir)
    if not os.path.isdir(d):
        return []
    me = os.path.basename(__file__).replace(".pyc", ".py")
    return [os.path.join(subdir, f) for f in sorted(os.listdir(d))
            if f.endswith(".py") and f != me]


# ------------------------------------------------------------------ probes
# Each returns (status, evidence). Evidence is shown verbatim on the page, so
# it should be a fact, not a summary. "0 references in model/mlb_model.py" is
# useful. "Not done" is not.

def p_injuries():
    n = sum(_count(f, r"injur") for f in _py_files("model"))
    rows = _csv_rows("data/clean/mlb_injuries_master.csv")
    have = f"{rows} rows collected and sitting unused" if rows > 0 else "no local file"
    if n == 0:
        return OPEN, f"0 references to injuries anywhere in model/, while {have}"
    scored = any(re.search(r"injur[^\n]{0,80}(exp_runs|ml_conf|_adj|weight)", _read(f), re.I)
                 for f in _py_files("model"))
    if not scored:
        return PARTIAL, f"{n} mentions in model/ but none reach exp_runs or ml_conf. {have}"
    return DONE, f"{n} references in model/ and the value reaches scoring"


def p_platoon_writer():
    """A writer that exists is not the same as a writer that runs.

    normalize_platoon_splits() lives in normalize/mlb_pitcher_normalize.py, but
    run_pipeline.py imports only normalize_recent_starts from that module. So
    the platoon master is refreshed solely by a manual /admin/refresh-signals,
    never by the 6am run. That is a far better explanation of an empty file than
    the name matching theory this was chased on three times.
    """
    writers = []
    for f in _py_files("scrapers") + _py_files("normalize"):
        if "mlb_pitcher_platoon_master" in _read(f):
            writers.append(os.path.basename(f))
    rows = _csv_rows("data/clean/mlb_pitcher_platoon_master.csv")
    state = "missing" if rows < 0 else (f"{rows} data rows" if rows else "header only, 0 data rows")

    pipe = _read("run_pipeline.py")
    in_pipeline = bool(re.search(r"normalize_platoon_splits|mlb_pitcher_normalize\s+import\s+run\b", pipe))

    if not writers:
        return OPEN, (f"nothing writes mlb_pitcher_platoon_master.csv; local copy is {state}")
    if not in_pipeline:
        return OPEN, (f"written by {', '.join(writers)} but run_pipeline.py never calls "
                      f"normalize_platoon_splits, only normalize_recent_starts. Refreshes only on a "
                      f"manual /admin/refresh-signals. Local copy is {state}")
    if rows <= 0:
        return PARTIAL, f"in the pipeline via {', '.join(writers)}, but the file is {state}"
    return DONE, f"written by {', '.join(writers)} in the daily pipeline; {state}"


def p_platoon_wired():
    txt = _read("model/mlb_model.py")
    uses = len(re.findall(r"get_platoon\s*\(", txt))
    # Wired means the value reaches a score, not just a display dict.
    scored = bool(re.search(r"platoon[^\n]{0,80}(exp_runs|ml_conf|_adj\s*\+?=)", txt, re.I))
    if uses == 0:
        return OPEN, "get_platoon() is never called in mlb_model.py"
    if not scored:
        return OPEN, f"get_platoon() called {uses}x but the result never reaches exp_runs or ml_conf"
    return DONE, f"get_platoon() called {uses}x and feeds scoring"


def p_batter_statcast():
    n = _count("model/mlb_model.py", r"mlb_statcast_master")
    if n == 0:
        return OPEN, "mlb_model.py never reads mlb_statcast_master.csv; batter quality enters only as team RPG and OPS"
    return DONE, f"{n} references in mlb_model.py"


def p_pitcher_statcast_fields():
    txt = _read("model/mlb_model.py")
    fields = {"xwoba": 0, "whiff": 0, "velo": 0, "barrel": 0, "exit_velo": 0, "hard_hit": 0}
    for k in fields:
        fields[k] = len(re.findall(k, txt, re.I))
    used = [k for k, v in fields.items() if v > 0]
    dead = [k for k, v in fields.items() if v == 0]
    if not dead:
        return DONE, "all scraped pitcher Statcast fields are referenced"
    return PARTIAL, f"using {', '.join(used)}; scraped and ignored: {', '.join(dead)}"


def p_scraper_timeouts():
    """Read the whole CALL, not one line.

    The first version tested `"requests.get(" in line and "timeout" not in line`,
    which reported 7 scrapers as unprotected when every one of them was fine:
    they are multi line calls with `timeout=` on a continuation line. A probe
    that reports a fixed thing as broken is the same failure as one that reports
    a broken thing as fixed, and it wastes exactly as much time.
    """
    bad = []
    for f in _py_files("scrapers"):
        txt = _read(f)
        for m in re.finditer(r"requests\.(get|post)\s*\(", txt):
            i = m.end() - 1
            depth, j = 0, i
            while j < len(txt):
                if txt[j] == "(":
                    depth += 1
                elif txt[j] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            if "timeout" not in txt[i:j]:
                bad.append(f"{os.path.basename(f)}:{txt[:m.start()].count(chr(10)) + 1}")
    if not bad:
        return DONE, "every requests.get/post in scrapers/ passes a timeout"
    return OPEN, f"{len(bad)} call(s) with no timeout: {', '.join(bad[:6])}"


def p_best_price_ev():
    hit = _count("model/value.py", r"books_json") + _count("model/mlb_picks.py", r"books_json")
    if hit == 0:
        return OPEN, "value.py and mlb_picks.py never read books_json; EV still uses the consensus price nobody offers"
    return DONE, f"{hit} references to books_json in the EV path"


def p_devig_method():
    txt = _read("model/value.py")
    if re.search(r"shin|power_devig|\*\*\s*k\b|pow\s*\(", txt, re.I):
        return DONE, "value.py uses a favourite/longshot aware devig"
    if "ip / (ip + io)" in txt or re.search(r"ip\s*/\s*\(\s*ip\s*\+\s*io\s*\)", txt):
        return OPEN, "devig_two_way uses proportional devig, which understates favourites and inflates apparent edge on them"
    return UNKNOWN, "could not identify the devig method in value.py"


def p_devig_fallback():
    """Look for a BARE `return ip`, not any line containing it.

    The first version matched `return ip / (ip + io)` too, so it reported the
    item as open after it had been fixed. A probe that cannot tell those apart is
    worse than no probe, because it trains you to ignore the page.
    """
    txt = _read("model/value.py")
    bare = re.search(r"^\s*return ip\s*$", txt, re.M)
    if bare and "io is None" in txt:
        return OPEN, "devig_two_way returns the vig inclusive probability in the no-vig field when the other side is missing"
    return DONE, "returns None when the other side has no price, no vig inclusive fallback"


def p_model_version():
    n = _count("db/schema.py", r"model_version")
    if n == 0:
        return OPEN, "no model_version column in db/schema.py; records cannot be segmented per config version"
    return DONE, f"{n} references in db/schema.py"


def p_high_conf_tab():
    n = _count("run_picks_html.py", r"highConfGrid")
    c = _count("run_picks_html.py", r"buildPickCard")
    if n == 0 and c == 0:
        return OPEN, "neither highConfGrid nor buildPickCard exists; the card markup is still one inline template literal"
    if c > 0 and n == 0:
        return PARTIAL, "buildPickCard extracted, tab not built"
    return DONE, "High Confidence tab present"


def p_waterfall():
    n = _count("run_picks_html.py", r"waterfall|factorBreakdown|contribHtml")
    if n == 0:
        return OPEN, "no factor breakdown on the cards; adjustments are invisible so a dead signal looks like a live one"
    return DONE, f"{n} references to the factor breakdown in run_picks_html.py"


def p_stray_dupe():
    if _exists("routes/routes/analytics.py"):
        return OPEN, "routes/routes/analytics.py still exists as a stray duplicate of routes/analytics.py"
    return DONE, "stray duplicate removed"


def p_sklearn_dep():
    req = _read("requirements.txt")
    imports = _count("fit_calibration.py", r"from sklearn|import sklearn")
    if imports and "scikit-learn" not in req and "sklearn" not in req:
        return OPEN, "fit_calibration.py imports sklearn but it is absent from requirements.txt, so it fails anywhere it is not already installed"
    if not imports:
        return DONE, "no sklearn import outstanding"
    return DONE, "sklearn present in requirements.txt"


def p_pybaseball():
    req = _read("requirements.txt")
    if "pybaseball" in req:
        return DONE, "pybaseball in requirements.txt"
    return OPEN, "not installed. Decision pending: it pulls in pandas and numpy, which this pipeline currently does without"


def p_ev_gate():
    txt = _read("model/mlb_picks.py")
    m = re.search(r'EV_GATE"?\s*,\s*"([^"]*)"', txt)
    if not m:
        return UNKNOWN, "could not read the EV_GATE default from mlb_picks.py"
    if m.group(1).strip() in ("1", "true", "yes", "on"):
        return DONE, "EV gate defaults on"
    return OPEN, f'EV gate built but defaults to "{m.group(1)}"; never enabled or measured'


def p_batting_order():
    n = _count("model/mlb_props_model.py", r'batting_order["\']?\s*,\s*5')
    if n:
        return OPEN, f"batting_order defaults to 5 in {n} places, so every hitter is scored as a number five bat"
    return DONE, "batting order carried onto the player dict"


def p_bet_log():
    has_tbl = _count("db/schema.py", r"_BETS|CREATE TABLE IF NOT EXISTS bets") > 0
    has_api = _count("app.py", r"/api/bet") > 0
    has_ui  = _count("run_picks_html.py", r"betLogHtml|logBet") > 0
    parts = [("bets table", has_tbl), ("/api/bet", has_api), ("card control", has_ui)]
    missing = [n for n, ok in parts if not ok]
    if not missing:
        return DONE, "bets table, /api/bet and the card control are all present"
    return PARTIAL, f"built but missing: {', '.join(missing)}"


def p_js_rule_dupe():
    js = _count("run_picks_html.py", r"function\s+bestBetEval|bestBetEval\s*=")
    py = _exists("model/best_bets.py")
    if js and py:
        return OPEN, "the Best Bets rule exists in both run_picks_html.py and model/best_bets.py and must be kept in lockstep by hand"
    if py and not js:
        return DONE, "rule lives only in model/best_bets.py"
    return UNKNOWN, "could not locate the rule in one or both places"


def p_analysis_cache():
    txt = _read("analysis_report.py")
    if re.search(r"_cache|lru_cache|CACHE_TTL", txt):
        return DONE, "analysis_report.py has a cache"
    return OPEN, "no cache in analysis_report.py; every view of /admin/analysis re-runs the LLM round trips"


def p_recent_form_leak():
    txt = _read("model/mlb_model.py")
    if re.search(r"game_date\s*<\s*(target|as_of|_date)", txt):
        return DONE, "recent form is bounded by an explicit as-of date"
    return OPEN, "no explicit game_date < target_date guard on recent_form; leakage is possible on any backfill or replay"


def p_lineup_quality():
    txt = _read("model/mlb_model.py")
    if re.search(r"lineup_woba|lineup_quality|slot_weight", txt, re.I):
        return DONE, "offense is scored from the confirmed lineup"
    return OPEN, "offense still enters as team season RPG and OPS, which includes park and every game the opposing starter did not pitch"


def p_kalshi_date_filter():
    txt = _read("scrapers/mlb_kalshi_scraper.py")
    if re.search(r"date", txt, re.I) and re.search(r"ticker.*date|date.*ticker", txt, re.I):
        return PARTIAL, "ticker carries the date; confirm extract_game_probabilities filters on it"
    return OPEN, "extract_game_probabilities does not filter by date, so a future game can collide with today"


def p_pull_source():
    from_schema = "source" in _read("scrapers/odds_schema.py").split("MOVEMENT_FIELDNAMES")[0]
    if not from_schema:
        return OPEN, "SNAPSHOT_FIELDNAMES has no source column, so no snapshot records which scraper wrote it"
    stamped = "_r[\"source\"] = source" in _read("scrapers/odds_schema.py")
    if not stamped:
        return PARTIAL, "source column exists but write_snapshot_rows does not stamp it"
    return DONE, "source is in the schema and stamped centrally in write_snapshot_rows"


def p_multiuser():
    sch = _read("db/schema.py")
    if re.search(r"CREATE TABLE IF NOT EXISTS (users|accounts|subscribers)", sch, re.I):
        return DONE, "a user table exists"
    return OPEN, ("access is one shared site password in site_config. A paid service needs "
                  "per user accounts, which changes the schema and every auth check")


def p_immutable_record():
    st = _read("db/picks_store.py")
    frozen = "tier_locked" in st and "was_best_bet" in st
    if frozen:
        return PARTIAL, ("tier and Best Bets status latch, but picks are still re-scored intraday "
                         "and conf can move before lineups confirm. Nothing publishes an immutable "
                         "pre game record")
    return OPEN, "nothing freezes a published pick"


def p_props_pull_async():
    txt = _read("app.py")
    if "_props_pull_state" in txt and "threading.Thread(target=_pull_worker" in txt:
        return DONE, "props pull runs off the request thread and reports credits spent"
    return OPEN, ("props pull runs inline; one HTTP call per event outruns the gunicorn "
                  "worker timeout, killing it mid-loop after spending credits")


def p_parlay_log():
    txt = _read("run_picks_html.py")
    if re.search(r"parlay[^\n]{0,40}(stake|logBet|logParlay)", txt, re.I):
        return DONE, "the parlay drawer can record a stake"
    return OPEN, ("the parlay drawer has no stake field and no logging path; only "
                  "single picks can be recorded")


def p_shadow_model():
    app = _read("app.py")
    picks = _read("model/mlb_picks.py")
    has_route = "/admin/shadow" in app
    has_run   = bool(re.search(r"SHADOW_(CONFIG|VERSION|ENABLED)", picks + app))
    if has_route and has_run:
        return DONE, "shadow scoring runs and /admin/shadow renders the comparison"
    if has_run and not has_route:
        return PARTIAL, "shadow scoring exists but there is no side by side page"
    if has_route and not has_run:
        return PARTIAL, "route exists but nothing produces shadow picks"
    return OPEN, ("no shadow model. model_version stamping (live 2026-08-19) is the "
                  "primitive it needs and is already in place")


def p_parlay_calibration():
    """The parlay panel is the last place on the board using RAW confidence."""
    txt = _read("run_picks_html.py")
    m = re.search(r"(thematic|combined confidence|buildParlay|parlayCard)[\s\S]{0,2500}", txt, re.I)
    if not m:
        return UNKNOWN, "could not locate the parlay builder in run_picks_html.py"
    blk = m.group(0)
    if re.search(r"calibrat|CAL_COEFFS|calProb", blk, re.I):
        return DONE, "parlay maths reads a calibrated probability"
    return OPEN, ("parlay combined confidence multiplies RAW conf; the pick cards for the same "
                  "legs apply calibration and can read NO EDGE on picks the parlay panel counts "
                  "as a +26% edge")


def p_line_sanity():
    """Is anything checking the published line against what books actually quoted?"""
    picks = _read("model/mlb_picks.py")
    model = _read("model/mlb_model.py")
    if re.search(r"total_line_(min|max)[\s\S]{0,200}(reject|skip|suppress|continue)", picks + model, re.I):
        return DONE, "the published total is checked against the range books quoted"
    return OPEN, ("nothing validates the published total against total_line_min/max or books_json, "
                  "so a line no book offered reaches the label and then the graded record intact")


def p_fill_monitor():
    """Empty columns feeding live inputs went unnoticed for weeks."""
    hit = _count("app.py", r"/admin/data-health") + _count("admin_hub.py", r"data-health")
    if hit:
        return DONE, "a column fill monitor exists"
    return OPEN, ("nothing reports column fill rates. xwoba, whiff_percent and avg_velocity were "
                  "empty in 787 of 787 rows and the platoon master held 0 data rows, all feeding "
                  "live model inputs, none of it flagged")


def p_cross_source():
    txt = _read("scrapers/odds_schema.py") + _read("model/mlb_model.py")
    if re.search(r"(median|abs)[\s\S]{0,120}(disagree|mismatch|untrusted|reject_source)", txt, re.I):
        return DONE, "sources are compared against each other before being trusted"
    return OPEN, "no cross source agreement check; a second feed would be trusted on arrival"


def p_move_plausibility():
    txt = _read("scrapers/odds_schema.py") + _read("scrapers/mlb_odds_scraper.py")
    if re.search(r"MAX_(TOTAL_)?MOVE|implausible|move_cap", txt, re.I):
        return OPEN if False else DONE, "line movement is bounded at intake"
    return OPEN, ("no movement plausibility bound. An MLB total does not move 4 points in a day, "
                  "and 'total moved DOWN 4.0 pts' reached a card")


def p_coverage_floor():
    txt = _read("scrapers/odds_schema.py") + _read("scrapers/mlb_odds_scraper.py")
    if re.search(r"MIN_(GAMES|COVERAGE)|coverage_floor", txt, re.I):
        return DONE, "a pull below the coverage floor is treated as failed"
    return OPEN, ("no coverage floor. A pull returning 3 of 15 games writes 3 rows and reports "
                  "success, which is how a failing feed looks healthy")


def p_staleness_visible():
    if _count("run_picks_html.py", r"booksAge|books_at"):
        return PARTIAL, "per book prices show their age; other prices do not"
    return OPEN, "no price on the board states how old it is"


def p_clamp_saturation():
    """Does anything detect or report a saturated clamp?"""
    txt = _read("model/mlb_model.py")
    caps = len(re.findall(r"max\(0\.68,\s*min\(1\.30", txt))
    if re.search(r"saturat|_clamped_at|at_cap", txt, re.I):
        return PARTIAL, f"{caps} suppression clamps, and saturation is at least detected"
    return OPEN, (f"{caps} hard clamps on suppression alone, and nothing reports when one "
                  f"saturates. Observed live 2026-08-20: opposing bullpen and pitcher Statcast "
                  f"both contributed 0.00 on the same card because the starter alone hit the cap")


# -------------------------------------------------------------------- items
# group order is the order they render. Keep the highest leverage groups first.

GROUPS = [
    ("wiring",      "Model wiring",        "Signals already collected that reach no pick"),
    ("inputs",      "Model inputs",        "The big terms, currently estimated crudely"),
    ("math",        "Pricing and math",    "Where a wrong number looks like an opportunity"),
    ("guardrails",  "Data guardrails",
     "Catch a wrong value at intake, before anything consumes it"),
    ("surface",     "Board and UI",        "What the dashboard shows and does not"),
    ("data",        "Data sources",        "Things not collected yet"),
    ("reliability", "Reliability",         "Failures that would be silent"),
    ("keeping",     "Housekeeping",        "Small, and one security item"),
    ("commercial",  "If this becomes a paid service",
     "Cheap to decide now, expensive to retrofit later"),
]

ITEMS = [
    # --- wiring -----------------------------------------------------------
    dict(id="platoon-writer", group="wiring", effort="S",
         title="Confirm the platoon master actually fills",
         why="Chased three times as a name matching bug. If nothing writes the file, "
             "no amount of name matching will ever fix it.",
         where="scrapers/, normalize/, read at model/mlb_model.py:257", probe=p_platoon_writer),
    dict(id="platoon-pipeline", group="wiring", effort="S",
         title="Call normalize_platoon_splits from the daily pipeline",
         why="run_pipeline.py imports only normalize_recent_starts from mlb_pitcher_normalize, so "
             "the platoon master refreshes only on a manual refresh-signals. Wiring the model to a "
             "file nothing refreshes would fix nothing.",
         where="run_pipeline.py:170", probe=p_platoon_writer),
    dict(id="platoon-wire", group="wiring", effort="M",
         title="Wire platoon splits into scoring",
         why="Computed at mlb_model.py:1064 and written to the output dict for display only. "
             "3,492 scraped rows feeding a text label. Needs pitchHand on the schedule scrape.",
         where="model/mlb_model.py", probe=p_platoon_wired),
    dict(id="injuries", group="wiring", effort="M",
         title="Wire injuries into the model",
         why="A lineup missing its two best bats is a different offense and the model does not know. "
             "The data is already scraped and sitting there.",
         where="data/clean/mlb_injuries_master.csv", probe=p_injuries),
    dict(id="batter-statcast", group="wiring", effort="M",
         title="Feed batter Statcast to the game model",
         why="Every ML, RL and TOTAL is scored with no batter quality input beyond team season RPG and OPS.",
         where="model/mlb_model.py", probe=p_batter_statcast),
    dict(id="pitcher-statcast", group="wiring", effort="S",
         title="Use the pitcher Statcast fields already scraped",
         why="Velocity, barrel rate, exit velocity and hard hit rate are pulled every day and dropped.",
         where="model/mlb_model.py", probe=p_pitcher_statcast_fields),
    dict(id="batting-order", group="wiring", effort="S",
         title="Carry lineup slot onto the player dict for props",
         why="Every hitter is scored as a number five bat, so the lineup slot adjustment is dead. "
             "HR Watch already shows the correct order, so the data is reachable.",
         where="model/mlb_props_model.py", probe=p_batting_order),

    # --- inputs -----------------------------------------------------------
    dict(id="lineup-quality", group="inputs", effort="L",
         title="Score offense from the nine hitters actually playing",
         why="Team season RPG contains that team's park and averages in every game the opposing "
             "starter did not pitch. It is the crudest input feeding the second largest term.",
         where="model/mlb_model.py exp_runs", probe=p_lineup_quality),
    dict(id="projections", group="inputs", effort="L",
         title="Use a projection system for pitcher talent",
         why="Talent is currently raw season ERA blended with splits and last three starts, which "
             "double counts hot and cold runs. Steamer or ZiPS regress properly.",
         where="new scraper, needs pybaseball", probe=None),
    dict(id="recent-form", group="inputs", effort="M",
         title="Adjust recent form for opponent and park",
         why="Raw last ten runs at 35 percent of the offensive weight. A hot streak against bad "
             "pitching in Coors counts the same as one in Petco.",
         where="model/mlb_model.py", probe=None),
    dict(id="park-double", group="inputs", effort="S",
         title="Stop double counting park factor",
         why="Season RPG already contains that team's home games and is then multiplied by park again.",
         where="model/mlb_model.py exp_runs", probe=None),
    dict(id="clamp-saturation", group="inputs", effort="M",
         title="Stop the suppression clamp from swallowing every signal behind it",
         why="OBSERVED LIVE 2026-08-20, Angels at Astros. The Astros card read:\n"
             "    Opposing starter   +1.46\n"
             "    Opposing bullpen    0.00   'bullpen ERA, fatigue adjusted'\n"
             "    Pitcher Statcast    0.00   'xwOBA 0.379, whiff 24.18 (matched by id)'\n\n"
             "Both zeros have details proving the code ran. They are zero because suppression hit "
             "its 1.30 cap on the starter alone, leaving nothing for anything after it. Traced: "
             "the Statcast multiplier was 1.052, a real 5% adjustment, and the clamp absorbed all "
             "of it.\n\n"
             "This is the external audit's Finding 2B (double shrinkage) made concrete. Order of "
             "operations decides which signals survive, and whatever is applied last is discarded "
             "first. It also means the 2026-08-19 Statcast repair buys nothing on saturated games.\n\n"
             "OPTIONS: apply the clamp ONCE to the combined multiplier rather than at each stage; "
             "or use a soft compression (tanh/logistic) that never fully saturates; or at minimum "
             "flag on the card when a row was zeroed by a cap rather than by having no effect, "
             "since those are different facts and currently look identical.",
         where="model/mlb_model.py exp_runs", probe=p_clamp_saturation),
    dict(id="totals-rebuild", group="inputs", effort="L",
         title="Rebuild the run projection behind totals",
         why="total_conf_base is capped and the fitted slope is nearly flat: 30 points of stated "
             "confidence map to under 7 points of real probability. Raising the cap would only "
             "relabel the same non information.",
         where="model/mlb_model.py", probe=None),

    # --- math -------------------------------------------------------------
    dict(id="best-price-ev", group="math", effort="M",
         title="Make the best available price drive EV",
         why="EV is computed off a consensus that is not bettable anywhere, while the card "
             "separately shows a book six cents better. The shopping spread is often the whole edge.",
         where="model/value.py, model/mlb_picks.py", probe=p_best_price_ev),
    dict(id="devig", group="math", effort="S",
         title="Replace proportional devig with a favourite aware method",
         why="Proportional devig understates favourites, which inflates apparent edge on exactly "
             "the -130 to -160 favourites that lose 13.5 percent at real prices.",
         where="model/value.py devig_two_way", probe=p_devig_method),
    dict(id="devig-fallback", group="math", effort="S",
         title="Stop returning a vig inclusive probability in the no-vig field",
         why="Two different quantities in one variable is the pattern that produced the -109 price "
             "and the column shift.",
         where="model/value.py devig_two_way", probe=p_devig_fallback),
    dict(id="ev-gate", group="math", effort="S",
         title="Preview and decide on the EV gate",
         why="Built, defaults off, never measured. It is either an improvement or it is dead code.",
         where="model/mlb_picks.py, scripts/ev_gate_preview.py", probe=p_ev_gate),
    dict(id="outlier-books", group="math", effort="M",
         title="Detect stale and outlier book prices",
         why="One book twenty cents off the field is usually stale, not an opportunity, and it is "
             "the one that will look like the best price.",
         where="scrapers/odds_schema.py, model/value.py", probe=None),
    dict(id="model-version", group="math", effort="M",
         title="Stamp picks with a model version",
         why="Asked for on 08-04 and never built. Without it, wiring four dead signals at once "
             "changes every number on the board and nothing can be attributed.",
         where="db/schema.py, db/picks_store.py, db/model_config.py", probe=p_model_version),
    dict(id="split-51-46", group="math", effort="M",
         title="Resolve the 51.8 vs 46.0 percent split",
         why="Live saved picks and backfilled picks disagree by 5.8 points over the same range. "
             "Either the two grading paths disagree or the sample is skewed. It affects every "
             "calibration number.",
         where="/admin/calibration", probe=None),

    # --- surface ----------------------------------------------------------
    dict(id="waterfall", group="surface", effort="M",
         title="Factor waterfall on every pick card",
         why="Baseline, then each adjustment sized, then the final number. Turns a dead signal "
             "into a blank bar you cannot miss instead of an archaeology project.",
         where="run_picks_html.py, model/mlb_model.py", probe=p_waterfall),
    dict(id="calib-strip", group="surface", effort="S",
         title="Trust strip at the top of the board",
         why="Which bet types are earning at real prices, with sample size, where you look every "
             "day rather than on a page you have to remember to open.",
         where="run_picks_html.py, /admin/real-roi", probe=None),
    dict(id="high-conf", group="surface", effort="M",
         title="High Confidence tab",
         why="Blocked on extracting the card markup into buildPickCard first. That extraction is "
             "its own commit and unblocks several other things.",
         where="run_picks_html.py", probe=p_high_conf_tab),
    dict(id="js-dupe", group="surface", effort="S",
         title="Delete the duplicated Best Bets rule from the JS",
         why="The same rule in two languages already produced two different answers on 08-17.",
         where="run_picks_html.py, model/best_bets.py", probe=p_js_rule_dupe),
    dict(id="analysis-slow", group="surface", effort="S",
         title="Cache the nightly analysis page",
         why="Three LLM round trips on every view, so it takes close to a minute and costs on "
             "each refresh.",
         where="analysis_report.py", probe=p_analysis_cache),
    dict(id="props-parlay", group="surface", effort="M",
         title="Player props in thematic parlays",
         why="Open since the 08-01 nine item list.",
         where="model/mlb_picks.py build_thematic_parlays", probe=None),

    # --- data -------------------------------------------------------------
    dict(id="pybaseball", group="data", effort="M",
         title="Decide on pybaseball for FanGraphs projections and wRC+",
         why="The only way to reach projections and lineup quality metrics. Costs pandas and numpy "
             "in an image that is currently stdlib only, and introduces a second way to read a CSV.",
         where="requirements.txt", probe=p_pybaseball),
    dict(id="prop-lines", group="data", effort="M",
         title="Real book lines for batter Hits, RBI, Runs and SB",
         why="These are graded against a fictional 0.5 line, so the record measures beating a made "
             "up number. Pinnacle has no batter Hits market at all.",
         where="scrapers/mlb_oddsapi_props.py", probe=None),
    dict(id="bat-tracking", group="data", effort="M",
         title="Scrape pitch movement and bat tracking",
         why="IVB, horizontal break, spin, extension, swing speed and squared up rate are not "
             "collected. The Advanced Statcast block is empty for many players.",
         where="scrapers/mlb_statcast_scraper.py", probe=None),
    dict(id="hits-allowed-model", group="data", effort="M",
         title="Probability model for Hits Allowed and Pitching Outs",
         why="Both have real Pinnacle lines already parsing, and no model behind them.",
         where="model/mlb_props_model.py", probe=None),
    dict(id="k-projections-view", group="data", effort="M",
         title="All starters K projections view",
         why="Only the pitchers Pinnacle lists get a K prop, roughly eight a slate. The rest are "
             "invisible rather than shown with the model number and no line.",
         where="run_picks_html.py", probe=None),

    # --- reliability ------------------------------------------------------
    dict(id="timeouts", group="reliability", effort="S",
         title="Add a timeout to every scraper request",
         why="A hung request with no timeout blocks a pipeline thread indefinitely and looks "
             "identical to a slow morning.",
         where="scrapers/", probe=p_scraper_timeouts),
    dict(id="sklearn-dep", group="reliability", effort="S",
         title="Add sklearn to requirements or vendor the fit",
         why="fit_calibration.py imports it and it is not declared, so it works locally and fails "
             "anywhere clean.",
         where="requirements.txt, fit_calibration.py", probe=p_sklearn_dep),
    dict(id="leakage-guard", group="reliability", effort="S",
         title="Bound recent form by an explicit as-of date",
         why="Without it, any backfill or replay can score a game using data from after it "
             "finished, which makes a backtest look better than reality.",
         where="model/mlb_model.py", probe=p_recent_form_leak),
    dict(id="gemini-predeploy", group="reliability", effort="M",
         title="Gemini proofread inside predeploy_check.py",
         why="Four self inflicted bugs on 08-11 were caught only because they happened to be "
             "re-read. Advisory only, never blocking.",
         where="scripts/predeploy_check.py, gemini_client.py", probe=None),
    dict(id="fill-monitor", group="guardrails", effort="S",
         title="Report column fill rates on every master file",
         why="THE GENERAL CURE FOR 2026-08-19. Four columns feeding live model inputs were "
             "completely empty and none of it was noticed for weeks: xwoba 0/787, whiff_percent "
             "0/787, avg_velocity 0/787, and the platoon master at 0 data rows.\n\n"
             "A page showing fill rate per column per master CSV would have flagged every one on "
             "the morning it started. /admin/signal-audit asks 'does this input VARY across "
             "games'. This asks the question one layer down: 'is there anything here at all'. "
             "Today proved both are needed, because a column of empty strings varies exactly as "
             "little as a column of identical values and neither raises an error.\n\n"
             "Flag any column under a threshold (say 5% populated) that the model actually reads. "
             "Cheap, and it closes the class rather than the instance.",
         where="new route, reads data/clean/*.csv", probe=p_fill_monitor),
    dict(id="line-sanity-guard", group="guardrails", effort="S",
         title="Refuse to publish a line no book is quoting",
         why="Pricing, labelling and grading are each FAITHFUL to the step before them, which is "
             "why the fabricated total went undetected. The main total picked the lowest alternate "
             "line on the board, the label recorded 'OVER 6.5', and run_analysis graded it against "
             "6.5 exactly as published. Every stage did its job and the result was a recorded win "
             "on a bet no book would have taken.\n\n"
             "GUARD: at pick time, compare the chosen total_line against total_line_min/max (and "
             "books_json where present). If it sits outside the range books actually quoted, "
             "suppress the pick rather than publish it. Same idea as rejecting |price| < 100.\n\n"
             "Cheap, and it closes the whole class rather than the one instance.",
         where="model/mlb_picks.py, scrapers/odds_schema.py", probe=p_line_sanity),
    dict(id="cross-source-check", group="guardrails", effort="M",
         title="Gate a new odds source on agreement, not on the request succeeding",
         why="Prerequisite for adding any scraped feed. Where two sources quote the same market, "
             "compare them. If the median gap across a slate exceeds a threshold, mark that source "
             "untrusted for the pull and fall through to the next one.\n\n"
             "A broken scraper is harmless because it fails loudly. The dangerous one half works "
             "and returns a plausible wrong price, which is this codebase's signature failure: the "
             "-109 run line, the column shift, three Savant column names silently returning empty.",
         where="scrapers/odds_schema.py", probe=p_cross_source),
    dict(id="move-plausibility", group="guardrails", effort="S",
         title="Bound line movement at intake",
         why="An MLB total does not move 4 points in a day. 'DRIFT: Total moved DOWN 4.0 pts' "
             "reached a live card because nothing checks whether a move is physically possible. "
             "Reject or quarantine implausible moves instead of publishing them.",
         where="scrapers/odds_schema.py", probe=p_move_plausibility),
    dict(id="coverage-floor", group="guardrails", effort="S",
         title="Treat a thin pull as a failure, not a partial success",
         why="A pull returning 3 of 15 games writes 3 rows and reports success. That is how a "
             "failing feed looks healthy, and it is the same shape as Polymarket walking the whole "
             "catalogue and matching zero games for weeks while logging fine.",
         where="scrapers/, odds_schema.py", probe=p_coverage_floor),
    dict(id="staleness-visible", group="guardrails", effort="S",
         title="Every price on the board states its age",
         why="Half built. Per book prices now say 'prices from 6:00 AM, over 6h old' after the "
             "NO BOOK ON 6.5 confusion. The board price, the total and the run line do not. A "
             "price with no timestamp cannot be judged.",
         where="run_picks_html.py", probe=p_staleness_visible),
    dict(id="totals-record-audit", group="math", effort="M",
         title="Audit graded totals against the lines books were really on",
         why="Totals published between roughly 2026-08-17 and 2026-08-20 may have been graded "
             "against a fabricated line. Going over 6.5 when the market was 8.5 records a win that "
             "could never have been placed. This inflates LOCK TOTAL (6-2) and the TOTAL 55-60% "
             "band (+10.9%), and both currently feed decisions.\n\n"
             "TEST: for every graded TOTAL pick, parse the line out of `label` and compare it to "
             "total_line_min/total_line_max on the snapshot for that game. Flag any pick whose "
             "line sits outside the range. Then re-run the totals record excluding them.\n\n"
             "Decide afterwards whether to void those rows or keep them flagged. Do NOT silently "
             "re-grade: what was published is what was published.",
         where="picks table, mlb_odds_master.csv", probe=None),
    dict(id="wrong-market-price", group="surface", effort="S",
         title="Stop showing the moneyline as the headline price on a spread card",
         why="A SPREAD card for Braves +1.5 displays 'SPORTSBOOK ML - Braves -110 - Sox -104' as "
             "its most prominent price box, while the actual run line had no feed price and the "
             "real book price was -250. Justin read it as the model pricing the spread off the "
             "moneyline.\n\n"
             "THE MODEL IS CORRECT. _price_for keys RL strictly to rl_{side}_{hcap}_price with no "
             "fallback (fixed 2026-08-14), and the card refuses EV and refuses a stake. The BUG "
             "IS THE DISPLAY: the most visible number on the card is not the price of the bet.\n\n"
             "FIX: on a spread or total card, either drop the ML box or demote it and label it as "
             "context. Where the price belongs, say plainly that the feed has none, and if useful "
             "state what the ML implies the run line will cost.\n\n"
             "RELATED GAP: Pinnacle's free feed quotes only the dog's +1.5 and the favourite's "
             "-1.5. When the model picks the FAVOURITE at +1.5 there is no price at all, which is "
             "why this card is blank. Worth counting how often that happens.",
         where="run_picks_html.py card template", probe=None),
    dict(id="parlay-logic", group="surface", effort="M",
         title="Fix or remove the suggested parlays",
         why="THE BOARD CONTRADICTS ITSELF. The parlay panel multiplies RAW stated confidence "
             "(81.1% x 78.5% = 63.7%, shown as '+26.4% edge vs break-even'), while the pick card "
             "for the same leg applies calibration and reads 'HONEST READ - NO EDGE, EV -0.8%'. "
             "Calibrated, that parlay is roughly +1.4 points over break-even, not +26. It is the "
             "only surface left using uncalibrated confidence.\n\n"
             "NO THESIS. 'Thematic parlays, grouped by shared thesis' but the thesis is 'these "
             "two numbers are the largest'. Astros ML appears in 4 of 5 cards, Guardians in 3. "
             "It is combinatorics over the top four picks, which the parlay builder already lets "
             "Justin do by hand.\n\n"
             "AND THE MATH ARGUES AGAINST THE FEATURE. If two legs are genuinely +EV singles, "
             "parlaying them is STRICTLY WORSE than betting them separately, because the hold "
             "compounds. A parlay is only justified when legs are correlated and the book has "
             "mispriced the correlation, or as a deliberate variance trade. Nothing on the panel "
             "says either.\n\n"
             "THREE OPTIONS, Justin to choose:\n"
             "  1. Remove it. Cheapest, and loses nothing he cannot do himself.\n"
             "  2. Rebuild honestly: calibrated probabilities, EV computed against the real "
             "combined price, and a line stating the singles are better EV unless he wants "
             "variance.\n"
             "  3. Make the thesis real: group legs that share a CAUSAL driver (overs in the same "
             "wind conditions, dogs against spent bullpens), which is the only case where a "
             "parlay has a defensible reason to exist.",
         where="run_picks_html.py, model/mlb_picks.py build_thematic_parlays",
         probe=p_parlay_calibration),
    dict(id="shadow-model", group="surface", effort="L",
         title="Shadow model with a live side by side view",
         why="Every real bug this week was caught by EYE, not by a metric: blank weather across "
             "four cards, NO BOOK ON 6.5, the Statcast row reading zero. A summary statistic "
             "surfaced none of them. So the shadow must be WATCHABLE, not just gradeable.\n\n"
             "DESIGN (Justin, 2026-08-20):\n"
             "  - A second scoring pass on the same slate with an experimental config, writing "
             "picks under a distinct model_version (e.g. 2026.08.21-shadow-xera). Same odds "
             "snapshots, so NO extra Odds API quota.\n"
             "  - /admin/shadow renders one row per GAME: live pick on the left, shadow pick on "
             "the right, and a clear marker where they DISAGREE. Agreement rows teach nothing; "
             "the disagreements are the whole point and should sort to the top.\n"
             "  - Show the factor waterfall DELTA on disagreement rows, so the reason for the "
             "split is visible rather than inferred.\n"
             "  - Updates through the day like the main board, and fills in who was right as "
             "games finish. Running tally at the top: live W-L vs shadow W-L.\n"
             "  - Shadow picks are NEVER shown on the public board and never enter Best Bets.\n\n"
             "This is how the xera question gets settled: live on xwOBA, shadow on xera, two "
             "weeks, compare. No guessing and no risk to the board.\n\n"
             "WATCH OUT: anything summarising the record must filter by model_version or it "
             "will silently blend the two, which is the exact failure model_version was built "
             "to prevent. Also the shadow shares the odds feed, so it can say nothing about "
             "whether a change would have gotten a better PRICE.",
         where="model/mlb_picks.py, run_pipeline.py, app.py /admin/shadow", probe=p_shadow_model),
    dict(id="parlay-log", group="surface", effort="M",
         title="Let the parlay builder record a stake",
         why="Single picks can be logged, parlays cannot. Justin builds them and has nowhere "
             "to put the amount. Preferred shape is one bets row PER LEG with a shared parlay "
             "group id, so each leg keeps its real game_id and CLV and grading still work, and "
             "the parlay result is derived from whether every leg won. That answers which leg "
             "keeps killing them, which is the only question worth asking about parlays.",
         where="run_picks_html.py, db/schema.py, app.py", probe=p_parlay_log),
    dict(id="props-pull-async", group="reliability", effort="S",
         title="Run the props pull off the request thread",
         why="One HTTP call per event at timeout=30. Twelve games outran the gunicorn worker "
             "timeout, the worker was killed mid-loop, the browser got a blank 500, and the "
             "except block that reports spent credits never ran. Credits were spent with no "
             "record of it.",
         where="app.py /admin/props-pull", probe=p_props_pull_async),

    dict(id="bet-log-deploy", group="reliability", effort="S",
         title="Deploy the bet log",
         why="Built and verified, not yet shipped. Until it is, there is no record of what was "
             "actually staked or at what price.",
         where="app.py, db/schema.py, run_picks_html.py", probe=p_bet_log),

    # --- housekeeping -----------------------------------------------------
    dict(id="kalshi-key", group="keeping", effort="S",
         title="Rotate the Kalshi key",
         why="kalshi_private.pem is in git history. Removing the file did not remove it from the "
             "history, so the key must be rotated at Kalshi.",
         where="external action", probe=None),
    dict(id="stray-dupe", group="keeping", effort="S",
         title="Delete routes/routes/analytics.py",
         why="A stray duplicate. Someone will eventually edit the wrong one.",
         where="routes/routes/analytics.py", probe=p_stray_dupe),
    dict(id="kalshi-date", group="keeping", effort="S",
         title="Filter Kalshi markets by date",
         why="Markets span multiple dates and are not filtered, so a future game can collide with "
             "today's. The ticker carries the date.",
         where="scrapers/mlb_kalshi_scraper.py", probe=p_kalshi_date_filter),
    dict(id="pull-visibility", group="surface", effort="S",
         title="Show which odds pulls have landed today",
         why="The schedule bar showed only what was coming next, so there was no way to tell "
             "from the board whether its prices were from 6am or twenty minutes ago.",
         where="pull_log.py, admin_hub.py, run_picks_html.py", probe=p_pull_source),

    dict(id="licensing", group="commercial", effort="M",
         title="Check what the data licences allow before charging for this",
         why="This is the one that can invalidate work already done. Free and personal use tiers "
             "commonly forbid commercial redistribution of odds, and that applies to the Odds API "
             "and to anything scraped from a book. Worth reading the terms BEFORE building the "
             "scraper layer on top of them, not after.",
         where="external, terms review", probe=None),
    dict(id="immutable-record", group="commercial", effort="L",
         title="Publish an immutable pre game record",
         why="A paid service is bought on a verifiable track record. Picks are currently re-scored "
             "all day, so what a subscriber saw at noon is not necessarily what gets graded. Needs "
             "a published snapshot that is written once, timestamped, and never edited.",
         where="db/picks_store.py, run_pipeline.py", probe=p_immutable_record),
    dict(id="multiuser", group="commercial", effort="L",
         title="Per user accounts",
         why="Access is one shared site password today. Subscriptions need real accounts, which "
             "touches the schema, every auth check and the whole session model.",
         where="db/schema.py, app.py", probe=p_multiuser),
    dict(id="record-segmentation", group="commercial", effort="M",
         title="Model version stamping is mandatory, not optional, once money is involved",
         why="Already on the list under Pricing and math. Flagged again here because a published "
             "record that silently spans several model versions is not a track record, and that "
             "becomes a claim you are selling rather than a note to self.",
         where="db/schema.py", probe=p_model_version),

    dict(id="market-signal-backfill", group="keeping", effort="S",
         title="Decide whether to backfill market_signal on historical picks",
         why="956 historical picks are stuck on NEUTRAL because the old inference read prose. "
             "They will never produce a CONFIRM or DIVERGE breakdown.",
         where="db/picks_store.py", probe=None),
]


# ----------------------------------------------------------------- assembly

def run_probe(item) -> tuple:
    probe = item.get("probe")
    if probe is None:
        return MANUAL, "no automatic check for this one"
    try:
        status, evidence = probe()
        return status, evidence
    except Exception as exc:                      # never take the page down
        return UNKNOWN, f"probe failed: {type(exc).__name__}: {exc}"


def rows(force: bool = False, state: dict | None = None) -> list:
    """Every item with a live status. Cached briefly so the page stays fast.

    `state` is the asserted-status overlay from db.checklist_store.get_state().
    IT CAN ONLY TOUCH ITEMS WITH NO PROBE. For anything probeable the evidence
    wins, always, and an override row is ignored outright. That asymmetry is the
    whole integrity guarantee: no stored flag can ever paint a red item green
    while the code still says otherwise.
    """
    now = time.time()
    if force or _CACHE["rows"] is None or (now - _CACHE["at"]) >= _CACHE_TTL:
        out = []
        for it in ITEMS:
            status, evidence = run_probe(it)
            out.append({**{k: v for k, v in it.items() if k != "probe"},
                        "status": status, "evidence": evidence,
                        "probed": it.get("probe") is not None,
                        "asserted": False, "note": ""})
        _CACHE["rows"] = out
        _CACHE["at"] = now

    base = _CACHE["rows"]
    if not state:
        return base

    merged = []
    for it in base:
        if it["probed"]:
            merged.append(it)          # measured beats asserted, no exceptions
            continue
        ov = state.get(it["id"])
        if not ov:
            merged.append(it)
            continue
        when = ov.get("updated_at")
        stamp = when.strftime("%b %d") if hasattr(when, "strftime") else ""
        merged.append({**it,
                       "status": ov.get("status") or it["status"],
                       "asserted": True,
                       "note": ov.get("note") or "",
                       "evidence": f"marked {ov.get('status','?')}"
                                   + (f" on {stamp}" if stamp else "")
                                   + ". No automatic check exists for this one."})
    return merged


def summary(all_rows=None) -> dict:
    r = all_rows if all_rows is not None else rows()
    done = sum(1 for x in r if x["status"] == DONE)
    return {
        "total":   len(r),
        "done":    done,
        "open":    sum(1 for x in r if x["status"] == OPEN),
        "partial": sum(1 for x in r if x["status"] == PARTIAL),
        "manual":  sum(1 for x in r if x["status"] == MANUAL),
        "unknown": sum(1 for x in r if x["status"] == UNKNOWN),
        "probed":  sum(1 for x in r if x["probed"]),
        "pct":     round(100.0 * done / len(r)) if r else 0,
    }


def by_group(all_rows=None) -> list:
    r = all_rows if all_rows is not None else rows()
    out = []
    for key, label, blurb in GROUPS:
        items = [x for x in r if x["group"] == key]
        if not items:
            continue
        out.append({
            "key": key, "label": label, "blurb": blurb, "items": items,
            "done": sum(1 for x in items if x["status"] == DONE),
            "total": len(items),
        })
    return out

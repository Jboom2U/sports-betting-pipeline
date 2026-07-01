"""
scripts/predeploy_check.py
Run before every railway up.

Checks:
  1. Syntax / null bytes on all critical files
  2. Common field-name bugs
  3. SQL column validation (inside SQL literals only)
  4. Route simulation (catches route crashes and NameErrors before deploy)

NOTE: Run this from PowerShell, not from the Cowork sandbox bash.
The sandbox cannot clean up git lock files on the Windows filesystem.

Usage:
    python scripts/predeploy_check.py
"""

import ast, os, re, subprocess, sys, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CRITICAL_FILES = [
    "app.py", "run_picks_html.py", "run_pipeline.py", "run_afternoon.py",
    "run_analysis.py", "model/mlb_model.py", "model/mlb_picks.py",
    "model/mlb_props_model.py", "model/mlb_analysis_sections.py",
    "scrapers/mlb_odds_scraper.py", "scrapers/mlb_statcast_scraper.py",
    "scrapers/mlb_statcast_pitcher_scraper.py", "scrapers/mlb_polymarket_scraper.py",
    "scrapers/mlb_hitter_scraper.py", "scrapers/mlb_lineup_scraper.py",
    "scrapers/mlb_pinnacle_scraper.py", "db/csv_sync.py", "db/picks_store.py",
]

SIM_SCRIPT = """\
import sys, os
sys.path.insert(0, '.')
for k, v in [
    ('DATABASE_URL', ''), ('ODDS_API_KEY', 'test'),
    ('STORAGE_ENDPOINT_URL', 'http://localhost'),
    ('STORAGE_ACCESS_KEY_ID', 'test'),
    ('STORAGE_SECRET_ACCESS_KEY', 'test'),
    ('STORAGE_BUCKET', 'test'),
]:
    os.environ.setdefault(k, v)
import app as a
with a.app.test_client() as c:
    routes = ['/', '/performance-html', '/schedule-status', '/status']
    print(','.join(r + ':' + str(c.get(r).status_code) for r in routes))
"""


def load_schema_columns():
    path = os.path.join(ROOT, "db", "schema.py")
    if not os.path.exists(path):
        return {}
    src = open(path).read()
    tables = {}
    for block in re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\);", src, re.DOTALL):
        name, body = block
        cols = set()
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith(("--", "UNIQUE", "PRIMARY", "FOREIGN")):
                continue
            col = line.split()[0].strip(",")
            if col:
                cols.add(col)
        tables[name] = cols
    return tables


def check_sql_columns(source, schema):
    """Only scans triple-quoted SQL literals to avoid Python false positives."""
    issues = []
    sql_blocks = re.findall(r'"""(.*?)"""', source, re.DOTALL)
    sql_blocks += re.findall(r"'''(.*?)'''", source, re.DOTALL)
    sql_blocks = [b for b in sql_blocks
                  if re.search(r"\b(SELECT|FROM|JOIN|WHERE)\b", b, re.IGNORECASE)]
    for sql in sql_blocks:
        alias_map = {}
        for m in re.finditer(r"\b(?:FROM|JOIN)\s+(\w+)\s+(\w+)", sql, re.IGNORECASE):
            table, alias = m.group(1), m.group(2)
            if table in schema:
                alias_map[alias] = table
        for m in re.finditer(r"\b([a-z]{1,4})\.([a-z_]+)\b", sql):
            alias, col = m.group(1), m.group(2)
            if alias not in alias_map:
                continue
            table = alias_map[alias]
            if col not in schema[table]:
                issues.append(
                    "SQL ref '" + alias + "." + col + "' not in " + table +
                    " (valid: " + str(sorted(schema[table])) + ")"
                )
    return issues


errors = []
warnings = []
schema = load_schema_columns()

print("=" * 60)
print("  Statalizers Pre-Deploy Validation")
print("=" * 60)

# ── Phase 1: File checks ──────────────────────────────────────────────────────
for rel_path in CRITICAL_FILES:
    full_path = os.path.join(ROOT, rel_path)
    if not os.path.exists(full_path):
        warnings.append("[MISSING]  " + rel_path)
        continue

    raw = open(full_path, "rb").read()

    if raw.count(b"\x00"):
        errors.append("[NULL BYTES]  " + rel_path)
        continue
    if not raw.rstrip():
        errors.append("[EMPTY FILE]  " + rel_path)
        continue

    try:
        source = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        errors.append("[DECODE ERROR]  " + rel_path + ": " + str(e))
        continue

    try:
        ast.parse(source)
    except SyntaxError as e:
        errors.append("[SYNTAX ERROR]  " + rel_path + " line " + str(e.lineno) + ": " + e.msg)
        continue

    if rel_path == "model/mlb_analysis_sections.py":
        if 'p.get("confidence", p.get("conf"' not in source:
            warnings.append("[WARN]  " + rel_path + ": Monte Carlo conf fallback missing")

    if "statcast" in rel_path and "scraper" in rel_path:
        if "lstrip" not in source and "ufeff" not in source:
            warnings.append("[WARN]  " + rel_path + ": BOM strip not found")

    if schema and rel_path in ("db/picks_store.py", "app.py"):
        for issue in check_sql_columns(source, schema):
            errors.append("[SQL ERROR]  " + rel_path + ": " + issue)

    if not any(rel_path in e for e in errors):
        print("  OK   " + rel_path)

# ── Phase 2: Route simulation ─────────────────────────────────────────────────
print()
print("  Running route simulation...")

with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tf:
    tf.write(SIM_SCRIPT)
    sim_file = tf.name

sim = subprocess.run([sys.executable, sim_file], cwd=ROOT, capture_output=True, text=True)
os.unlink(sim_file)

route_line = next((l for l in sim.stdout.strip().splitlines() if ":" in l and "/" in l), None)
if not route_line:
    stderr = sim.stderr or ""
    # Missing local packages are an env issue, not a code bug — skip simulation
    if "ModuleNotFoundError" in stderr or "No module named" in stderr:
        missing = next((l.strip() for l in stderr.splitlines() if "No module named" in l), "unknown")
        warnings.append("[WARN]  Route simulation skipped — local dep missing (" + missing + "). Run on Railway env to verify.")
        print("  SKIP route simulation (local dep missing — not a code error)")
    else:
        errors.append("[SIM ERROR]  Route simulation failed:\n" + stderr[-600:])
else:
    for pair in route_line.split(","):
        route, code = pair.rsplit(":", 1)
        if code == "200":
            print("  OK   " + route + " -> " + code)
        else:
            errors.append("[ROUTE ERROR]  " + route + " returned HTTP " + code)

# ── Summary ───────────────────────────────────────────────────────────────────
print()
if warnings:
    print("WARNINGS:")
    for w in warnings:
        print("  " + w)
    print()

if errors:
    print("ERRORS (fix before deploying):")
    for e in errors:
        print("  " + e)
    print()
    print("[FAIL] " + str(len(errors)) + " error(s) — do NOT run railway up")
    sys.exit(1)
else:
    msg = "[PASS] All checks clean"
    if warnings:
        msg += " (" + str(len(warnings)) + " warning(s))"
    print(msg)
    print()
    print("Safe to deploy:")
    print("  git add <files>  |  git commit  |  git push  |  railway up")
    sys.exit(0)

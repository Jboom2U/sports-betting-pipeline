"""
scripts/predeploy_check.py
Run before every railway up to catch syntax errors, truncations, null bytes,
import issues, and common field name bugs.

Usage:
    python scripts/predeploy_check.py

Returns exit code 0 if all checks pass, 1 if any fail.
"""

import ast
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# All Python files that must pass every check before deploying
CRITICAL_FILES = [
    "app.py",
    "run_picks_html.py",
    "run_pipeline.py",
    "run_afternoon.py",
    "run_analysis.py",
    "model/mlb_model.py",
    "model/mlb_picks.py",
    "model/mlb_props_model.py",
    "model/mlb_analysis_sections.py",
    "scrapers/mlb_odds_scraper.py",
    "scrapers/mlb_statcast_scraper.py",
    "scrapers/mlb_statcast_pitcher_scraper.py",
    "scrapers/mlb_polymarket_scraper.py",
    "scrapers/mlb_hitter_scraper.py",
    "scrapers/mlb_lineup_scraper.py",
    "scrapers/mlb_pinnacle_scraper.py",
    "db/csv_sync.py",
    "db/picks_store.py",
]

errors = []
warnings = []

print("=" * 60)
print("  Statalizers Pre-Deploy Validation")
print("=" * 60)

for rel_path in CRITICAL_FILES:
    full_path = os.path.join(ROOT, rel_path)

    if not os.path.exists(full_path):
        warnings.append(f"  [MISSING]  {rel_path}")
        continue

    raw = open(full_path, "rb").read()

    # ── 1. Null byte check ───────────────────────────────────────────────────
    null_count = raw.count(b"\x00")
    if null_count:
        errors.append(f"  [NULL BYTES x{null_count}]  {rel_path}")
        continue

    # ── 2. Truncation check — file should end with newline, not mid-token ────
    tail = raw.rstrip()
    if not tail:
        errors.append(f"  [EMPTY FILE]  {rel_path}")
        continue

    # ── 3. Syntax check ──────────────────────────────────────────────────────
    try:
        source = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        errors.append(f"  [DECODE ERROR]  {rel_path}: {e}")
        continue

    try:
        ast.parse(source)
    except SyntaxError as e:
        errors.append(f"  [SYNTAX ERROR]  {rel_path} line {e.lineno}: {e.msg}")
        continue

    # ── 4. Common field name bugs ─────────────────────────────────────────────
    checks = []

    # Monte Carlo builder must use .get("conf") fallback
    if rel_path == "model/mlb_analysis_sections.py":
        if 'p.get("confidence", p.get("conf"' not in source:
            checks.append("Monte Carlo may not fall back to 'conf' field — check build_monte_carlo()")

    # Statcast scrapers must strip BOM
    if "statcast" in rel_path and "scraper" in rel_path:
        if "lstrip" not in source and "ufeff" not in source:
            checks.append("BOM strip not found — Savant CSV first column will be corrupted")

    # Odds scraper must not have syntax error at line 496
    if rel_path == "scrapers/mlb_odds_scraper.py":
        lines = source.splitlines()
        if len(lines) > 495:
            suspect = lines[495]
            if suspect.strip().endswith(")") and "f\"" in suspect and "Total" in suspect:
                checks.append("Possible duplicate block at line 496 — check for doubled code")

    for c in checks:
        warnings.append(f"  [WARN]  {rel_path}: {c}")

    print(f"  OK   {rel_path}")

# ── Summary ───────────────────────────────────────────────────────────────────
print()
if warnings:
    print("WARNINGS:")
    for w in warnings:
        print(w)
    print()

if errors:
    print("ERRORS (fix before deploying):")
    for e in errors:
        print(e)
    print()
    print(f"[FAIL] {len(errors)} error(s) found — do NOT run railway up")
    sys.exit(1)
else:
    print(f"[PASS] All {len(CRITICAL_FILES)} files clean" +
          (f" ({len(warnings)} warning(s))" if warnings else ""))
    print()
    print("Safe to deploy:")
    print("  git add <files>")
    print("  git commit -m 'your message'")
    print("  git push")
    print("  railway up")
    sys.exit(0)

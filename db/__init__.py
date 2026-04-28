"""
db/ — Statalizers persistence layer.

Provides PostgreSQL-backed storage for:
  - Pipeline run tracking (replaces pipeline_run_date.txt)
  - Picks storage (foundation for backtesting)
  - Scored game snapshots

Also provides S3/R2 CSV sync so data survives Railway deploys.

All modules are non-fatal: if DATABASE_URL or storage env vars are absent,
everything falls back to file-based behavior silently.
"""

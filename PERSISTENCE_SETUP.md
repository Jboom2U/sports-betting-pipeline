# Statalizers — Persistence Setup

Fix the Railway data wipe problem. After completing these steps, `railway up`
will no longer destroy your picks history or CSV data.

---

## Part 1 — Railway PostgreSQL

### 1. Add the Postgres plugin to your Railway project

```bash
# From the repo root
railway add --plugin postgresql
```

Or in the Railway dashboard: open your project → **+ New** → **Database** → **PostgreSQL**.

Railway automatically sets `DATABASE_URL` as an environment variable in your
service. No manual config needed — the app reads it on startup.

### 2. Verify the variable is visible to your service

In the Railway dashboard: your service → **Variables** tab.
You should see `DATABASE_URL` starting with `postgresql://...`

If it's not there, link it manually:
Railway dashboard → Variables → **Add Variable Reference** → select the
Postgres plugin's `DATABASE_URL`.

### 3. Deploy

```bash
railway up
```

On first boot, the app will automatically create the `pipeline_runs`, `picks`,
and `scored_games` tables. Check Railway logs for:

```
DB schema verified / created.
PostgreSQL connection pool established.
```

---

## Part 2 — Cloudflare R2 (CSV object storage)

Cloudflare R2 stores your CSV snapshots so they survive Railway deploys.
You already have a Cloudflare account at jskellly@gmail.com.

### 1. Create an R2 bucket

1. Go to [dash.cloudflare.com](https://dash.cloudflare.com) → **R2 Object Storage**
2. Click **Create bucket**
3. Name it `statalizers-data`
4. Location: leave as default (automatic)
5. Click **Create bucket**

### 2. Create an R2 API token

1. In the R2 section → **Manage R2 API Tokens**
2. Click **Create API Token**
3. Name: `statalizers-railway`
4. Permissions: **Object Read & Write**
5. Bucket scope: **statalizers-data** (specific bucket only)
6. Click **Create API Token**
7. **Copy the Access Key ID and Secret Access Key now** — you won't see the secret again

### 3. Get your R2 endpoint URL

In the R2 dashboard → your bucket → **Settings** tab.
The S3 API endpoint looks like:

```
https://<your_account_id>.r2.cloudflarestorage.com
```

Copy that full URL.

### 4. Set environment variables in Railway

```bash
railway variables set STORAGE_ENDPOINT_URL="https://<account_id>.r2.cloudflarestorage.com"
railway variables set STORAGE_ACCESS_KEY_ID="<your_access_key_id>"
railway variables set STORAGE_SECRET_ACCESS_KEY="<your_secret_access_key>"
railway variables set STORAGE_BUCKET="statalizers-data"
```

Or add them in the Railway dashboard → your service → **Variables** tab.

### 5. Deploy

```bash
railway up
```

After the pipeline runs (6am ET), check Railway logs for:

```
Uploading CSV snapshots to object storage...
CSV sync upload complete: N file(s).
```

On the next deploy after that, startup logs will show:

```
Object storage detected — downloading CSV snapshots...
Startup CSV sync: N file(s) downloaded from storage.
```

---

## How it works after setup

| Event | What happens |
|-------|-------------|
| `railway up` | App starts → creates DB schema → downloads CSVs from R2 → checks if pipeline ran today (via DB) |
| 6am ET pipeline | Runs all scrapers → saves picks + scored games to Postgres → marks pipeline complete in DB → uploads all CSVs to R2 |
| Next `railway up` | Skips pipeline (DB says it ran today) → downloads CSVs from R2 → serves dashboard with real data |
| Pipeline hasn't run today | Downloads CSVs → runs pipeline → everything proceeds normally |

---

## Verification

Visit `/status` on your deployed app. When persistence is working you'll see:
- Pipeline status showing the correct last run date (pulled from DB, not a local file)

Check R2 bucket contents:
1. Cloudflare dashboard → R2 → **statalizers-data** → browse files
2. You should see `clean/mlb_scores_master.csv`, `clean/mlb_schedule_master.csv`, etc.

---

## Local development (no setup needed)

When running locally without `DATABASE_URL` set, the app falls back to the
original file-based behavior (`data/pipeline_run_date.txt`). No changes needed
for local runs.

---

## Environment variable summary

| Variable | Where | Description |
|----------|-------|-------------|
| `DATABASE_URL` | Set by Railway Postgres plugin automatically | PostgreSQL connection string |
| `STORAGE_ENDPOINT_URL` | Set manually | R2 endpoint: `https://<account_id>.r2.cloudflarestorage.com` |
| `STORAGE_ACCESS_KEY_ID` | Set manually | R2 API token access key |
| `STORAGE_SECRET_ACCESS_KEY` | Set manually | R2 API token secret |
| `STORAGE_BUCKET` | Set manually | R2 bucket name (default: `statalizers-data`) |
| `ODDS_API_KEY` | Already set | Unchanged |
| `KALSHI_API_KEY` | Already set | Unchanged |

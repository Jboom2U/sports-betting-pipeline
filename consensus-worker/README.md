# picks-consensus — multi-AI review of Statalizers daily picks

Cloudflare Worker. Reads the 6am pipeline output from R2 (`statalizers-data`),
sends the slate + Statalizers picks to Gemini and OpenRouter free models for
critical review, stores the consensus in D1 (`consensus-db`), and serves a
report page. Runs automatically at 6:45am ET (cron `45 10 * * *` UTC) and on
demand from the report page.

## Deploy (PowerShell, from repo root)
```powershell
cd consensus-worker
npx wrangler deploy
```
D1 database `consensus-db` and tables already exist — no migration needed.

## First-time setup (in browser, after deploy)
1. Open the worker URL (wrangler prints it, e.g. https://picks-consensus.<account>.workers.dev)
2. Create an admin password (first visit only)
3. Go to Settings → paste keys → hit Test on each:
   - Gemini key: https://aistudio.google.com/apikey  (free, no card)
   - OpenRouter key: https://openrouter.ai/settings/keys  (free, no card)
4. Back on the report page, hit "Run consensus now"

## Notes
- Zero Odds API usage — reads only what the pipeline already cached in R2.
- OpenRouter models are configurable in Settings (comma-separated), default:
  `deepseek/deepseek-r1:free,openai/gpt-oss-120b:free`
- Optional: paste Claude's analysis into the box on the report page before a
  run and it is included in the prompt for the other models to review.
- Keys live in D1 `settings` table, entered via UI only — never in code.

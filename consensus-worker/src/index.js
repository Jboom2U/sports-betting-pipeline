// picks-consensus — multi-AI consensus review of Statalizers daily picks
// Reads dossier data from R2 (statalizers-data), relays to Gemini + OpenRouter
// models, stores consensus in D1, serves report + settings UI.
// API keys are entered on /settings — never stored in code.

const ET_OFFSET_GUESS = -4; // EDT; only used for date labeling

function etToday() {
  const now = new Date(Date.now() + ET_OFFSET_GUESS * 3600 * 1000);
  return now.toISOString().slice(0, 10);
}

// ── settings helpers ─────────────────────────────────────────────────────────
async function getSetting(env, key) {
  const row = await env.DB.prepare("SELECT value FROM settings WHERE key=?").bind(key).first();
  return row ? row.value : null;
}
async function setSetting(env, key, value) {
  await env.DB.prepare(
    "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now')) " +
    "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')"
  ).bind(key, value).run();
}

// ── auth (password set on first visit, session = HMAC cookie) ───────────────
async function sha256hex(text) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
}
async function sessionToken(env) {
  const hash = await getSetting(env, "admin_hash");
  return hash ? await sha256hex("session:" + hash) : null;
}
async function isAuthed(request, env) {
  const cookie = request.headers.get("Cookie") || "";
  const m = cookie.match(/sid=([a-f0-9]{64})/);
  if (!m) return false;
  const tok = await sessionToken(env);
  return tok && m[1] === tok;
}

// ── model calls ──────────────────────────────────────────────────────────────
async function callGemini(env, prompt) {
  const key = await getSetting(env, "gemini_key");
  if (!key) return { model: "gemini", error: "no key configured" };
  const model = (await getSetting(env, "gemini_model")) || "gemini-2.5-flash";
  const t0 = Date.now();
  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
    { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] }) });
  const latency = Date.now() - t0;
  if (!res.ok) return { model: `gemini/${model}`, error: `HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`, latency };
  const data = await res.json();
  const text = data?.candidates?.[0]?.content?.parts?.map(p => p.text).join("") || "";
  return { model: `gemini/${model}`, text, latency };
}

async function callOpenRouter(env, modelId, prompt) {
  const key = await getSetting(env, "openrouter_key");
  if (!key) return { model: modelId, error: "no key configured" };
  const t0 = Date.now();
  let res;
  for (let attempt = 0; attempt < 2; attempt++) {
    res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: { "Authorization": `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ model: modelId, messages: [{ role: "user", content: prompt }] }) });
    if (res.status !== 429 || attempt === 1) break;
    await new Promise(r2 => setTimeout(r2, 5000));
  }
  const latency = Date.now() - t0;
  if (!res.ok) return { model: modelId, error: `HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`, latency };
  const data = await res.json();
  return { model: modelId, text: data?.choices?.[0]?.message?.content || "", latency };
}

function extractJson(text) {
  if (!text) return null;
  const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  const candidates = [fence ? fence[1] : null, text].filter(Boolean);
  for (const c of candidates) {
    const start = c.indexOf("{");
    const end = c.lastIndexOf("}");
    if (start === -1 || end <= start) continue;
    try { return JSON.parse(c.slice(start, end + 1)); } catch {}
  }
  return null;
}

// ── dossier from R2 ──────────────────────────────────────────────────────────
function parseCsv(text) {
  const rows = [];
  let row = [], field = "", inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; }
      else field += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n" || c === "\r") {
      if (field !== "" || row.length) { row.push(field); rows.push(row); row = []; field = ""; }
      if (c === "\r" && text[i + 1] === "\n") i++;
    } else field += c;
  }
  if (field !== "" || row.length) { row.push(field); rows.push(row); }
  return rows;
}

async function loadDossier(env, date) {
  const out = { date, picks: [], schedule: "" };
  const obj = await env.DATA.get(`picks/mlb_picks_${date}.csv`);
  if (obj) {
    const rows = parseCsv(await obj.text());
    if (rows.length > 1) {
      const header = rows[0].map(h => h.trim().toLowerCase());
      const idx = k => header.indexOf(k);
      for (const r of rows.slice(1)) {
        let conf = parseFloat(r[idx("conf")] || "0") || 0;
        if (conf <= 1) conf = conf * 100;
        const label = (r[idx("label")] || "").trim();
        if (!label) continue;
        const game = (r[idx("game")] || "").trim();
        out.picks.push({
          pick: game ? `${game}: ${label}` : label,
          game: (r[idx("game")] || "").trim(),
          type: (r[idx("type")] || "").trim(),
          odds: "",
          conf: Math.round(conf * 10) / 10,
          tier: (r[idx("tier")] || "").trim(),
          narrative: (r[idx("reasoning")] || "").trim()
        });
      }
    }
  } else {
    out.error = `picks/mlb_picks_${date}.csv not found in R2 — has today's pipeline run?`;
  }
  const sched = await env.DATA.get("data/clean/mlb_schedule_master.csv");
  if (sched) {
    const txt = await sched.text();
    out.schedule = txt.split("\n").filter(l => l.includes(date)).slice(0, 40).join("\n");
  }
  return out;
}

function buildPrompt(dossier, claudeAnalysis) {
  const picksBlock = dossier.picks.map(p =>
    `- ${p.pick} [${p.type}] ${p.odds ? "odds " + p.odds : ""} | model conf ${p.conf}% (${p.tier})${p.narrative ? " | " + p.narrative : ""}`
  ).join("\n");
  return `You are a professional sports betting analyst reviewing another model's MLB picks for ${dossier.date}.

TODAY'S SCHEDULE (from official data):
${dossier.schedule || "(unavailable)"}

STATALIZERS MODEL PICKS (Pythagorean model using pitcher ERA/FIP/WHIP, Statcast, bullpen fatigue, park factors, weather, umpire, sharp line movement):
${picksBlock || "(no picks found)"}
${claudeAnalysis ? "\nCLAUDE'S INDEPENDENT ANALYSIS:\n" + claudeAnalysis + "\n" : ""}
YOUR TASK: Critically review each pick. Push back where the reasoning looks weak. Use ONLY the data provided plus general baseball knowledge — do not invent injuries or recent results you cannot know.

Respond with STRICT JSON only, no prose outside the JSON:
{
  "reviews": [{"pick": "<exact pick text>", "verdict": "agree"|"fade", "conf": <0-100 your win probability>, "reason": "<1 sentence>"}],
  "best_bet": "<pick text you rate highest>",
  "own_plays": [{"pick": "<any play you like that is not listed>", "conf": <0-100>, "reason": "<1 sentence>"}]
}`;
}

// ── consensus run ────────────────────────────────────────────────────────────
async function runConsensus(env, claudeAnalysis) {
  const date = etToday();
  const dossier = await loadDossier(env, date);
  const ins = await env.DB.prepare("INSERT INTO runs (run_date, slate_summary) VALUES (?, ?)")
    .bind(date, `${dossier.picks.length} picks`).run();
  const runId = ins.meta.last_row_id;
  if (!dossier.picks.length) {
    await env.DB.prepare("UPDATE runs SET status='failed', error=? WHERE id=?")
      .bind(dossier.error || ("no picks found in R2 for " + date), runId).run();
    return { runId, error: dossier.error || ("No picks in R2 for " + date) };
  }
  const prompt = buildPrompt(dossier, claudeAnalysis);
  const orModels = ((await getSetting(env, "openrouter_models")) ||
    "deepseek/deepseek-v4-flash:free,openai/gpt-oss-120b:free,nvidia/nemotron-3-super-120b-a12b:free").split(",").map(s => s.trim()).filter(Boolean);

  const results = await Promise.all([
    callGemini(env, prompt),
    ...orModels.map(m => callOpenRouter(env, m, prompt))
  ]);

  for (const r of results) {
    const parsed = r.text ? extractJson(r.text) : null;
    r.parsed = parsed;
    await env.DB.prepare(
      "INSERT INTO model_responses (run_id, model, raw_response, parsed_json, latency_ms, error) VALUES (?,?,?,?,?,?)"
    ).bind(runId, r.model, r.text || null, parsed ? JSON.stringify(parsed) : null, r.latency || null, r.error || null).run();
  }

  // blend per pick
  const bestVotes = {};
  for (const p of dossier.picks) {
    const confs = { statalizers: p.conf };
    const fades = [];
    for (const r of results) {
      if (!r.parsed?.reviews) continue;
      const rev = r.parsed.reviews.find(v => v.pick && (v.pick === p.pick || p.pick.includes(v.pick) || v.pick.includes(p.pick)));
      if (!rev) continue;
      const short = r.model.split("/").pop().replace(":free", "");
      if (rev.verdict === "fade") fades.push(`${short}: ${rev.reason || "fade"}`);
      else if (Number(rev.conf) > 0) confs[short] = Number(rev.conf);
    }
    const vals = Object.values(confs);
    const blended = Math.round(vals.reduce((a, b) => a + b, 0) / vals.length);
    p.blended = blended; p.confs = confs; p.fades = fades;
    await env.DB.prepare(
      "INSERT INTO consensus_picks (run_id, run_date, pick, pick_type, odds, statalizers_conf, model_confs, blended_conf, fades) VALUES (?,?,?,?,?,?,?,?,?)"
    ).bind(runId, date, p.pick, p.type, String(p.odds), p.conf, JSON.stringify(confs), blended, JSON.stringify(fades)).run();
  }
  for (const r of results) {
    const bb = r.parsed?.best_bet;
    if (bb) bestVotes[bb] = (bestVotes[bb] || 0) + 1;
  }
  const noFade = dossier.picks.filter(p => !p.fades.length);
  const pool = noFade.length ? noFade : dossier.picks;
  const best = pool.sort((a, b) => b.blended - a.blended)[0];
  if (best) {
    await env.DB.prepare("UPDATE consensus_picks SET is_best_bet=1 WHERE run_id=? AND pick=?").bind(runId, best.pick).run();
  }
  await env.DB.prepare("UPDATE runs SET status='complete' WHERE id=?").bind(runId).run();
  return { runId, date, picks: dossier.picks.length, models: results.map(r => ({ model: r.model, ok: !!r.parsed, error: r.error })) };
}

// ── HTML ─────────────────────────────────────────────────────────────────────
const CSS = `<style>body{font-family:system-ui,sans-serif;background:#0f1420;color:#e6e9f0;max-width:920px;margin:0 auto;padding:24px}
a{color:#7fb3ff}input,button,textarea{font:inherit;padding:8px 12px;border-radius:8px;border:1px solid #33405c;background:#1a2233;color:#e6e9f0}
button{cursor:pointer;background:#2b3a55}table{width:100%;border-collapse:collapse;font-size:14px}td,th{padding:8px;border-bottom:1px solid #26304a;text-align:left}
.badge{padding:2px 8px;border-radius:99px;font-size:12px}.ok{background:#123524;color:#7ee2a8}.bad{background:#3a1620;color:#ff9aa8}
.card{background:#161d2e;border:1px solid #26304a;border-radius:12px;padding:16px;margin:12px 0}</style>`;

function page(title, body) {
  return new Response(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${title}</title>${CSS}</head><body><h2>${title}</h2>${body}</body></html>`,
    { headers: { "Content-Type": "text/html;charset=utf-8" } });
}

function loginPage(setup) {
  return page(setup ? "Create admin password" : "Log in",
    `<form method="POST" action="${setup ? "/setup" : "/login"}" class="card">
     <input type="password" name="password" placeholder="password" required>
     <button type="submit">${setup ? "Create" : "Log in"}</button></form>`);
}

async function settingsPage(env) {
  const g = await getSetting(env, "gemini_key");
  const o = await getSetting(env, "openrouter_key");
  const models = (await getSetting(env, "openrouter_models")) || "deepseek/deepseek-v4-flash:free,openai/gpt-oss-120b:free,nvidia/nemotron-3-super-120b-a12b:free";
  return page("Consensus settings", `
  <div class="card"><h3>API keys</h3>
    <p>Gemini: ${g ? '<span class="badge ok">saved</span>' : '<span class="badge bad">not set</span>'}
       OpenRouter: ${o ? '<span class="badge ok">saved</span>' : '<span class="badge bad">not set</span>'}</p>
    <form id="kf">
      <p><input style="width:100%" type="password" name="gemini_key" placeholder="Gemini API key (aistudio.google.com)"></p>
      <p><input style="width:100%" type="password" name="openrouter_key" placeholder="OpenRouter API key (openrouter.ai)"></p>
      <p><input style="width:100%" name="openrouter_models" value="${models}"></p>
      <button type="submit">Save</button>
      <button type="button" onclick="test('gemini')">Test Gemini</button>
      <button type="button" onclick="test('openrouter')">Test OpenRouter</button>
      <span id="msg"></span>
    </form></div>
  <p><a href="/">← report</a></p>
  <script>
  const f=document.getElementById('kf'),msg=document.getElementById('msg');
  f.onsubmit=async e=>{e.preventDefault();const d=Object.fromEntries(new FormData(f).entries());
    const r=await fetch('/api/keys',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});
    msg.textContent=r.ok?'Saved':'Save failed';setTimeout(()=>location.reload(),600)};
  async function test(p){msg.textContent='Testing '+p+'…';
    const r=await fetch('/api/test-key',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({provider:p})});
    const j=await r.json();msg.innerHTML=j.ok?'<span class="badge ok">'+p+' connected</span>':'<span class="badge bad">'+ (j.error||'failed')+'</span>'}
  </script>`);
}

async function reportPage(env) {
  const run = await env.DB.prepare("SELECT * FROM runs ORDER BY id DESC LIMIT 1").first();
  if (!run) return page("MLB consensus report", `<p>No runs yet.</p><div class="card"><form method="POST" action="/api/run"><textarea name="claude_analysis" rows="6" style="width:100%" placeholder="Optional: paste Claude's analysis here"></textarea><p><button>Run consensus now</button></p></form></div><p><a href="/settings">settings</a></p>`);
  const picks = (await env.DB.prepare("SELECT * FROM consensus_picks WHERE run_id=? ORDER BY blended_conf DESC").bind(run.id).all()).results || [];
  const models = (await env.DB.prepare("SELECT model, error, latency_ms FROM model_responses WHERE run_id=?").bind(run.id).all()).results || [];
  const rows = picks.map(p => {
    const confs = JSON.parse(p.model_confs || "{}");
    const fades = JSON.parse(p.fades || "[]");
    const confStr = Object.entries(confs).map(([k, v]) => `${k} ${v}%`).join(" · ");
    return `<tr><td>${p.is_best_bet ? "⭐ " : ""}${p.pick}</td><td>${p.pick_type || ""}</td><td>${p.odds || ""}</td><td><b>${p.blended_conf}%</b></td><td style="font-size:12px;color:#9fb0d0">${confStr}${fades.length ? '<br><span style="color:#ff9aa8">' + fades.join("; ") + "</span>" : ""}</td></tr>`;
  }).join("");
  const modelBadges = models.map(m => `<span class="badge ${m.error ? "bad" : "ok"}">${m.model}${m.error ? " ✗" : " ✓"}</span>`).join(" ");
  return page(`MLB consensus report — ${run.run_date}`, `
    <p>${modelBadges} <span style="color:#9fb0d0">status: ${run.status}</span></p>
    <table><tr><th>Pick</th><th>Type</th><th>Odds</th><th>Blend</th><th>Model confs / pushback</th></tr>${rows}</table>
    <div class="card"><form method="POST" action="/api/run"><textarea name="claude_analysis" rows="5" style="width:100%" placeholder="Optional: paste Claude's analysis to include next run"></textarea><p><button>Rerun consensus</button></p></form></div>
    <p><a href="/settings">settings</a></p>`);
}

// ── router ───────────────────────────────────────────────────────────────────
export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runConsensus(env, null));
  },
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const adminHash = await getSetting(env, "admin_hash");

    if (path === "/setup" && request.method === "POST") {
      if (adminHash) return new Response("already set", { status: 400 });
      const form = await request.formData();
      const pw = form.get("password") || "";
      if (pw.length < 8) return page("Error", "<p>Password must be 8+ chars. <a href='/'>back</a></p>");
      await setSetting(env, "admin_hash", await sha256hex(pw));
      const tok = await sessionToken(env);
      return new Response(null, { status: 302, headers: { "Location": "/settings", "Set-Cookie": `sid=${tok}; HttpOnly; Secure; Path=/; Max-Age=31536000` } });
    }
    if (path === "/login" && request.method === "POST") {
      const form = await request.formData();
      const pw = form.get("password") || "";
      if (await sha256hex(pw) !== adminHash) return loginPage(false);
      const tok = await sessionToken(env);
      return new Response(null, { status: 302, headers: { "Location": "/", "Set-Cookie": `sid=${tok}; HttpOnly; Secure; Path=/; Max-Age=31536000` } });
    }

    if (!adminHash) return loginPage(true);
    if (!(await isAuthed(request, env))) return loginPage(false);

    if (path === "/settings") return settingsPage(env);
    if (path === "/api/keys" && request.method === "POST") {
      const d = await request.json();
      for (const k of ["gemini_key", "openrouter_key", "openrouter_models"]) {
        if (d[k]) await setSetting(env, k, d[k].trim());
      }
      return Response.json({ ok: true });
    }
    if (path === "/api/test-key" && request.method === "POST") {
      const { provider } = await request.json();
      try {
        if (provider === "gemini") {
          const key = await getSetting(env, "gemini_key");
          if (!key) return Response.json({ ok: false, error: "no key saved" });
          const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${key}`);
          return Response.json(r.ok ? { ok: true } : { ok: false, error: `HTTP ${r.status}` });
        }
        if (provider === "openrouter") {
          const key = await getSetting(env, "openrouter_key");
          if (!key) return Response.json({ ok: false, error: "no key saved" });
          const r = await fetch("https://openrouter.ai/api/v1/key", { headers: { "Authorization": `Bearer ${key}` } });
          return Response.json(r.ok ? { ok: true } : { ok: false, error: `HTTP ${r.status}` });
        }
      } catch (e) { return Response.json({ ok: false, error: e.message }); }
      return Response.json({ ok: false, error: "unknown provider" });
    }
    if (path === "/api/run" && request.method === "POST") {
      let claude = null;
      const ct = request.headers.get("Content-Type") || "";
      if (ct.includes("form")) { const f = await request.formData(); claude = f.get("claude_analysis") || null; }
      else if (ct.includes("json")) { claude = (await request.json()).claude_analysis || null; }
      const result = await runConsensus(env, claude);
      if (ct.includes("form")) return new Response(null, { status: 302, headers: { "Location": "/" } });
      return Response.json(result);
    }
    return reportPage(env);
  }
};

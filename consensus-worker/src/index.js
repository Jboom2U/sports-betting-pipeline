// picks-consensus v2 — multi-AI consensus review of Statalizers daily picks
// Dossier (picks + props + schedule) from R2 → Gemini + OpenRouter models →
// consensus computed at render time from model_responses, so external rows
// (e.g. model='claude' inserted via D1) appear automatically.

function etToday() {
  const now = new Date(Date.now() - 4 * 3600 * 1000);
  return now.toISOString().slice(0, 10);
}

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

async function callOpenAI(env, prompt) {
  const key = await getSetting(env, "openai_key");
  if (!key) return null;
  const model = (await getSetting(env, "openai_model")) || "gpt-5.4-mini";
  const t0 = Date.now();
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Authorization": `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify({ model, messages: [{ role: "user", content: prompt }] }) });
  const latency = Date.now() - t0;
  if (!res.ok) return { model: `openai-api/${model}`, error: `HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`, latency };
  const data = await res.json();
  return { model: `openai-api/${model}`, text: data?.choices?.[0]?.message?.content || "", latency };
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

function csvObjects(text) {
  const rows = parseCsv(text);
  if (rows.length < 2) return [];
  const header = rows[0].map(h => h.trim().toLowerCase());
  return rows.slice(1).map(r => {
    const o = {};
    header.forEach((h, i) => { o[h] = (r[i] || "").trim(); });
    return o;
  });
}

async function loadDossier(env, date) {
  const out = { date, picks: [], props: [], schedule: "" };
  const obj = await env.DATA.get(`picks/mlb_picks_${date}.csv`);
  if (obj) {
    for (const r of csvObjects(await obj.text())) {
      let conf = parseFloat(r.conf || "0") || 0;
      if (conf <= 1) conf = conf * 100;
      if (!r.label) continue;
      out.picks.push({
        pick: r.game ? `${r.game}: ${r.label}` : r.label,
        game: r.game || "", type: r.type || "", odds: "",
        conf: Math.round(conf * 10) / 10,
        tier: r.tier || "", narrative: r.reasoning || ""
      });
    }
  } else {
    out.error = `picks/mlb_picks_${date}.csv not found in R2 — has today's pipeline run?`;
  }
  const pobj = await env.DATA.get(`picks/mlb_props_${date}.csv`);
  if (pobj) {
    for (const r of csvObjects(await pobj.text())) {
      let conf = parseFloat(r.confidence || "0") || 0;
      if (conf <= 1) conf = conf * 100;
      if (!r.player_name) continue;
      out.props.push({
        player: r.player_name, game: r.game || "",
        prop_type: r.prop_type || "", line: r.line || "", proj: r.proj || "",
        conf: Math.round(conf * 10) / 10, tier: r.tier || "", reasoning: r.reasoning || ""
      });
    }
    out.props.sort((a, b) => b.conf - a.conf);
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
    `- ${p.pick} [${p.type}] | model conf ${p.conf}% (${p.tier})${p.narrative ? " | " + p.narrative : ""}`
  ).join("\n");
  const hrProps = dossier.props.filter(p => p.prop_type === "HR").slice(0, 10);
  const otherProps = dossier.props.filter(p => p.prop_type !== "HR").slice(0, 15);
  const propLine = p => `- ${p.player} (${p.game}) ${p.prop_type} ${p.line ? "line " + p.line : ""} | proj ${p.proj} | conf ${p.conf}%${p.reasoning ? " | " + p.reasoning : ""}`;
  return `You are a professional sports betting analyst reviewing another model's MLB picks for ${dossier.date}.

TODAY'S SCHEDULE (from official data):
${dossier.schedule || "(unavailable)"}

STATALIZERS MODEL GAME PICKS (Pythagorean model: pitcher ERA/FIP/WHIP, Statcast, bullpen fatigue, park factors, weather, umpire, sharp line movement):
${picksBlock || "(no picks found)"}

STATALIZERS HR PROP CANDIDATES:
${hrProps.map(propLine).join("\n") || "(none)"}

STATALIZERS OTHER PLAYER PROP CANDIDATES:
${otherProps.map(propLine).join("\n") || "(none)"}
${claudeAnalysis ? "\nCLAUDE'S INDEPENDENT ANALYSIS:\n" + claudeAnalysis + "\n" : ""}
YOUR TASK: Review EVERY pick listed above and give each a verdict — agree (with your own win probability) or fade. Do not skip picks you agree with. Push back where reasoning looks weak. Build parlays from the strongest plays (never two legs from the same game). Pick the best HR props and player props. Use ONLY the data provided plus general baseball knowledge — do not invent injuries or results you cannot know.

Respond with STRICT JSON only, no prose outside the JSON:
{
  "reviews": [{"pick": "<exact pick text, copied verbatim from the list above with NOTHING appended>", "verdict": "agree"|"fade", "conf": <0-100>, "reason": "<1 sentence>"}],
  "best_bet": "<pick text you rate highest>",
  "parlays": [
    {"legs": ["<pick text>", "<pick text>"], "conf": <0-100 combined>, "reason": "<1 sentence>"},
    {"legs": ["<pick>", "<pick>", "<pick>"], "conf": <0-100>, "reason": "<1 sentence>"}
  ],
  "hr_props": [{"player": "<name>", "conf": <0-100>, "reason": "<1 sentence>"}],
  "player_props": [{"player": "<name>", "prop": "<e.g. Over 1.5 TB>", "conf": <0-100>, "reason": "<1 sentence>"}]
}
Limit hr_props and player_props to your top 3 each, parlays to one 2-leg, one 3-leg, one 4-leg.`;
}

// ── consensus math (shared by run + render) ─────────────────────────────────
function shortName(model) {
  return model.split("/").pop().replace(":free", "");
}

const DISPLAY_NAMES = { statalizers: "Statalizers", claude: "Claude" };
function dispName(model) {
  const sn = shortName(model);
  if (DISPLAY_NAMES[sn]) return DISPLAY_NAMES[sn];
  if (sn.startsWith("gemini")) return "Gemini";
  if (sn.startsWith("nemotron")) return "NemoTron";
  if (sn.startsWith("gpt-oss")) return "GPT-OSS";
  if (sn.startsWith("gpt-")) return "OpenAI";
  if (sn.startsWith("trinity")) return "Trinity";
  if (sn.startsWith("gemma")) return "Gemma";
  if (sn.startsWith("deepseek")) return "DeepSeek";
  return sn.charAt(0).toUpperCase() + sn.slice(1);
}

function cleanPick(str) {
  return String(str || "").split("|")[0].replace(/\[(ML|RL|TOTAL|PROP)\]/gi, "").replace(/\s+/g, " ").trim();
}
function pickMatch(a, b) {
  const x = cleanPick(a).toLowerCase(), y = cleanPick(b).toLowerCase();
  return !!(x && y && (x === y || x.includes(y) || y.includes(x)));
}

function computeConsensus(picks, responses) {
  const out = picks.map(p => ({ ...p, confs: { Statalizers: p.conf }, fades: [] }));
  for (const r of responses) {
    if (!r.parsed || !Array.isArray(r.parsed.reviews)) continue;
    const sn = dispName(r.model);
    for (const p of out) {
      const rev = r.parsed.reviews.find(v => pickMatch(v.pick, p.pick));
      if (!rev) continue;
      if (rev.verdict === "fade") p.fades.push({ model: sn, reason: rev.reason || "fade" });
      else if (Number(rev.conf) > 0) p.confs[sn] = Math.round(Number(rev.conf));
    }
  }
  for (const p of out) {
    const vals = Object.values(p.confs);
    p.blended = Math.round(vals.reduce((a, b) => a + b, 0) / vals.length);
  }
  out.sort((a, b) => b.blended - a.blended);
  const noFade = out.filter(p => !p.fades.length);
  const best = (noFade.length ? noFade : out)[0];
  if (best) best.isBest = true;
  return out;
}

function fairOdds(prob) {
  if (prob <= 0 || prob >= 100) return "";
  const p = prob / 100;
  return p >= 0.5 ? `-${Math.round(100 * p / (1 - p))}` : `+${Math.round(100 * (1 - p) / p)}`;
}

function statalizersParlays(consensus) {
  const legs = [];
  const usedGames = new Set();
  for (const p of consensus) {
    if (p.conf < 57 || !p.game || usedGames.has(p.game)) continue;
    usedGames.add(p.game);
    legs.push(p);
    if (legs.length >= 4) break;
  }
  const mk = n => {
    if (legs.length < n) return null;
    const sel = legs.slice(0, n);
    const prob = Math.round(sel.reduce((a, l) => a * (l.conf / 100), 1) * 100);
    return { legs: sel.map(l => l.pick), conf: prob, reason: "top non-overlapping plays by model confidence" };
  };
  return [mk(2), mk(3), mk(4)].filter(Boolean);
}


// ── source pick tracking + grading ──────────────────────────────────────────
async function recordSourcePicks(env, runId, date, consensus, dossier, responses) {
  const add = (source, category, pick, conf) =>
    env.DB.prepare("INSERT INTO source_picks (run_id, run_date, source, category, pick, conf) VALUES (?,?,?,?,?,?)")
      .bind(runId, date, source, category, pick, Math.round(conf || 0)).run();
  for (const p of consensus.filter(x => x.type === "ML").sort((a, b) => b.conf - a.conf).slice(0, 5))
    await add("Statalizers", "ML_TOP5", p.pick, p.conf);
  for (const p of (dossier.props || []).filter(x => x.prop_type === "HR").slice(0, 3))
    await add("Statalizers", "HR_PROP", `${p.player} HR (${p.game})`, p.conf);
  for (const p of (dossier.props || []).filter(x => x.prop_type !== "HR").slice(0, 3))
    await add("Statalizers", "PLAYER_PROP", `${p.player} ${p.prop_type} ${p.line} (${p.game})`, p.conf);
  for (const r of responses) {
    if (!r.parsed) continue;
    const sn = dispName(r.model);
    const agrees = (r.parsed.reviews || []).filter(v => v.verdict !== "fade" && Number(v.conf) > 0);
    const mls = agrees.filter(v => {
      const cp = consensus.find(x => pickMatch(x.pick, v.pick));
      return cp && cp.type === "ML";
    }).sort((a, b) => Number(b.conf) - Number(a.conf)).slice(0, 5);
    for (const v of mls) await add(sn, "ML_TOP5", cleanPick(v.pick), v.conf);
    for (const x of (r.parsed.hr_props || []).slice(0, 3)) await add(sn, "HR_PROP", `${x.player} HR`, x.conf);
    for (const x of (r.parsed.player_props || []).slice(0, 3)) await add(sn, "PLAYER_PROP", `${x.player} ${x.prop || ""}`.trim(), x.conf);
    if (r.parsed.best_bet) await add(sn, "BEST_BET", cleanPick(r.parsed.best_bet), 0);
  }
}

async function gradeSourcePicks(env) {
  const days = (await env.DB.prepare("SELECT DISTINCT run_date FROM source_picks WHERE result IS NULL AND run_date < ?").bind(etToday()).all()).results || [];
  for (const { run_date } of days) {
    const obj = await env.DATA.get(`picks/mlb_analysis_${run_date}.json`);
    if (!obj) continue;
    let graded;
    try { graded = JSON.parse(await obj.text()).graded_picks || []; } catch { continue; }
    const pending = (await env.DB.prepare("SELECT id, pick FROM source_picks WHERE result IS NULL AND run_date=?").bind(run_date).all()).results || [];
    for (const sp of pending) {
      const g = graded.find(gp => gp.label && (sp.pick.includes(gp.label) || gp.label.includes(sp.pick)));
      if (g && ["WIN", "LOSS", "PUSH"].includes(g.result))
        await env.DB.prepare("UPDATE source_picks SET result=? WHERE id=?").bind(g.result, sp.id).run();
    }
  }
}

// ── consensus run ────────────────────────────────────────────────────────────
async function runConsensus(env, claudeAnalysis) {
  const date = etToday();
  const dossier = await loadDossier(env, date);
  const prompt = dossier.picks.length ? buildPrompt(dossier, claudeAnalysis) : null;
  const ins = await env.DB.prepare("INSERT INTO runs (run_date, slate_summary, dossier, prompt) VALUES (?, ?, ?, ?)")
    .bind(date, `${dossier.picks.length} picks, ${dossier.props.length} props`, JSON.stringify(dossier), prompt).run();
  const runId = ins.meta.last_row_id;
  if (!dossier.picks.length) {
    await env.DB.prepare("UPDATE runs SET status='failed', error=? WHERE id=?")
      .bind(dossier.error || ("no picks found in R2 for " + date), runId).run();
    return { runId, error: dossier.error || ("No picks in R2 for " + date) };
  }
  const orModels = ((await getSetting(env, "openrouter_models")) ||
    "deepseek/deepseek-v4-flash:free,openai/gpt-oss-120b:free,nvidia/nemotron-3-super-120b-a12b:free")
    .split(",").map(s => s.trim()).filter(Boolean);

  const calls = [callGemini(env, prompt), ...orModels.map(m => callOpenRouter(env, m, prompt))];
  if (await getSetting(env, "openai_key")) calls.push(callOpenAI(env, prompt));
  const results = (await Promise.all(calls)).filter(Boolean);

  for (const r of results) {
    r.parsed = r.text ? extractJson(r.text) : null;
    await env.DB.prepare(
      "INSERT INTO model_responses (run_id, model, raw_response, parsed_json, latency_ms, error) VALUES (?,?,?,?,?,?)"
    ).bind(runId, r.model, r.text || null, r.parsed ? JSON.stringify(r.parsed) : null, r.latency || null, r.error || null).run();
  }

  const consensus = computeConsensus(dossier.picks, results);
  for (const p of consensus) {
    await env.DB.prepare(
      "INSERT INTO consensus_picks (run_id, run_date, pick, pick_type, odds, statalizers_conf, model_confs, blended_conf, fades, is_best_bet) VALUES (?,?,?,?,?,?,?,?,?,?)"
    ).bind(runId, date, p.pick, p.type, "", p.conf, JSON.stringify(p.confs), p.blended,
           JSON.stringify(p.fades.map(f => `${f.model}: ${f.reason}`)), p.isBest ? 1 : 0).run();
  }
  await recordSourcePicks(env, runId, date, consensus, dossier, results);
  await env.DB.prepare("UPDATE runs SET status='complete' WHERE id=?").bind(runId).run();
  return { runId, date, picks: dossier.picks.length, models: results.map(r => ({ model: r.model, ok: !!r.parsed, error: r.error })) };
}

// ── HTML ─────────────────────────────────────────────────────────────────────
const CSS = `<style>
body{font-family:system-ui,sans-serif;background:#0f1420;color:#e6e9f0;max-width:1200px;margin:0 auto;padding:24px}
a{color:#7fb3ff}input,button,textarea{font:inherit;padding:8px 12px;border-radius:8px;border:1px solid #33405c;background:#1a2233;color:#e6e9f0}
button{cursor:pointer;background:#2b3a55}
table{width:100%;border-collapse:collapse;font-size:14px}td,th{padding:8px;border-bottom:1px solid #26304a;text-align:left;vertical-align:top}
.badge{padding:2px 8px;border-radius:99px;font-size:12px}.ok{background:#123524;color:#7ee2a8}.bad{background:#3a1620;color:#ff9aa8}
.card{background:#161d2e;border:1px solid #26304a;border-radius:12px;padding:14px 16px;margin:0 0 14px}
.card h3{margin:0 0 10px;font-size:15px;color:#c7d2ea}
.layout{display:grid;grid-template-columns:1.7fr 1fr;gap:18px;align-items:start}
@media(max-width:900px){.layout{grid-template-columns:1fr}}
.src{font-size:11px;color:#8fa3c8;text-transform:uppercase;letter-spacing:.05em;margin:10px 0 4px}
.item{font-size:13px;margin:0 0 6px;line-height:1.45}
.muted{color:#9fb0d0;font-size:12px}.fade{color:#ff9aa8;font-size:12px}
.blend{font-weight:600;font-size:15px}
.tabs{display:flex;flex-wrap:wrap;gap:6px;margin:0 0 10px}
.tab{padding:5px 14px;font-size:14px;border-radius:99px;background:#1a2233;border:1px solid #33405c;color:#9fb0d0;cursor:pointer}
.tab.on{background:#3b4f7a;color:#fff;border-color:#7fa0e0;font-weight:600}
.pill{display:inline-block;background:#1e2840;border:1px solid #33405c;border-radius:99px;padding:1px 8px;font-size:11px;color:#aebfe0;margin-left:6px}
</style>`;

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
  const oa = await getSetting(env, "openai_key");
  const oaModel = (await getSetting(env, "openai_model")) || "gpt-5.4-mini";
  const models = (await getSetting(env, "openrouter_models")) || "deepseek/deepseek-v4-flash:free,openai/gpt-oss-120b:free,nvidia/nemotron-3-super-120b-a12b:free";
  return page("Consensus settings", `
  <div class="card"><h3>API keys</h3>
    <p>Gemini: ${g ? '<span class="badge ok">saved</span>' : '<span class="badge bad">not set</span>'}
       OpenRouter: ${o ? '<span class="badge ok">saved</span>' : '<span class="badge bad">not set</span>'}
       OpenAI: ${oa ? '<span class="badge ok">saved</span>' : '<span class="badge bad">not set</span>'}</p>
    <form id="kf">
      <p><input style="width:100%" type="password" name="gemini_key" placeholder="Gemini API key (leave blank to keep)"></p>
      <p><input style="width:100%" type="password" name="openrouter_key" placeholder="OpenRouter API key (leave blank to keep)"></p>
      <p><input style="width:100%" type="password" name="openai_key" placeholder="OpenAI API key (leave blank to keep)"></p>
      <p><input style="width:100%" name="openai_model" value="${oaModel}" title="OpenAI model"></p>
      <p><input style="width:100%" name="openrouter_models" value="${models}"></p>
      <button type="submit">Save</button>
      <button type="button" onclick="test('gemini')">Test Gemini</button>
      <button type="button" onclick="test('openrouter')">Test OpenRouter</button>
      <button type="button" onclick="test('openai')">Test OpenAI</button>
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
    const j=await r.json();msg.innerHTML=j.ok?'<span class="badge ok">'+p+' connected</span>':'<span class="badge bad">'+(j.error||'failed')+'</span>'}
  </script>`);
}

function esc(s) { return String(s || "").replace(/</g, "&lt;"); }

async function reportPage(env) {
  const run = await env.DB.prepare("SELECT * FROM runs WHERE status='complete' ORDER BY id DESC LIMIT 1").first();
  const rerunForm = `<div class="card"><form method="POST" action="/api/run"><textarea name="claude_analysis" rows="4" style="width:100%" placeholder="Optional: paste Claude's analysis to include next run"></textarea><p><button>Rerun consensus</button></p></form></div><p><a href="/settings">settings</a></p>`;
  if (!run) return page("MLB consensus report", `<p>No completed runs yet.</p>${rerunForm}`);

  const dossier = JSON.parse(run.dossier || "{}");
  const picks = dossier.picks || [];
  const rows = (await env.DB.prepare("SELECT model, parsed_json, error FROM model_responses WHERE run_id=?").bind(run.id).all()).results || [];
  const responses = rows.map(r => ({ model: r.model, parsed: r.parsed_json ? JSON.parse(r.parsed_json) : null, error: r.error }));
  const consensus = computeConsensus(picks, responses);

  const tableRows = consensus.map(p => {
    const confStr = Object.entries(p.confs).map(([k, v]) => `${esc(k)} ${v}%`).join(" · ");
    const fadeStr = p.fades.map(f => `${esc(f.model)}: ${esc(f.reason)}`).join("<br>");
    return `<tr><td>${p.isBest ? "⭐ " : ""}${esc(p.pick)}</td><td>${esc(p.type)}</td><td class="blend">${p.blended}%</td><td><span class="muted">${confStr}</span>${fadeStr ? '<br><span class="fade">' + fadeStr + "</span>" : ""}</td></tr>`;
  }).join("");

  const modelBadges = responses.map(r => `<span class="badge ${r.error ? "bad" : "ok"}">${esc(dispName(r.model))}${r.error ? " ✗" : " ✓"}</span>`).join(" ");

  const sources = [["Statalizers", null]];
  for (const r of responses) if (r.parsed) sources.push([dispName(r.model), r.parsed]);

  let tabSeq = 0;
  const tabbedCard = (title, renderSrc) => {
    const id = "tc" + (++tabSeq);
    const btns = sources.map(([name], i) =>
      `<button type="button" class="tab${i === 0 ? " on" : ""}" data-card="${id}" data-i="${i}">${esc(name)}</button>`).join("");
    const panes = sources.map(([name, parsed], i) =>
      `<div class="pane" data-card="${id}" data-i="${i}" style="display:${i === 0 ? "block" : "none"}">${renderSrc(name, parsed) || '<p class="item muted">none</p>'}</div>`).join("");
    return `<div class="card"><h3>${title}</h3><div class="tabs">${btns}</div>${panes}</div>`;
  };

  const mlCard = tabbedCard("Top 5 ML picks", (src, parsed) => {
    if (src === "Statalizers") {
      const top = consensus.filter(x => x.type === "ML").sort((a, b) => b.conf - a.conf).slice(0, 5);
      return top.map(x => `<p class="item">${esc(x.pick)} <span class="muted">${x.conf}%</span></p>`).join("");
    }
    const agrees = (parsed?.reviews || []).filter(v => v.verdict !== "fade" && Number(v.conf) > 0);
    const top = agrees.filter(v => {
      const cp = consensus.find(x => pickMatch(x.pick, v.pick));
      return cp && cp.type === "ML";
    }).sort((a, b) => Number(b.conf) - Number(a.conf)).slice(0, 5);
    if (top.length) return top.map(v => `<p class="item">${esc(cleanPick(v.pick))} <span class="muted">${Math.round(v.conf)}%</span></p>`).join("");
    const legs = new Set();
    if (parsed?.best_bet) legs.add(cleanPick(parsed.best_bet));
    for (const pl of (parsed?.parlays || [])) for (const l of (pl.legs || [])) legs.add(cleanPick(l));
    const mlLegs = [...legs].filter(l => {
      const cp = consensus.find(x => pickMatch(x.pick, l));
      return cp && cp.type === "ML";
    }).slice(0, 5);
    if (!mlLegs.length) return "";
    return mlLegs.map(l => `<p class="item">${esc(l)}</p>`).join("") + `<p class="muted">derived from its parlays/best bet</p>`;
  });

  const parlayCard = tabbedCard("Parlay recommendations", (src, parsed) => {
    const list = src === "Statalizers" ? statalizersParlays(consensus) : (parsed?.parlays || []);
    return list.slice(0, 3).map(pl =>
      `<p class="item">${(pl.legs || []).map(l => esc(cleanPick(l))).join(" + ")}<br><span class="muted">${Math.round(pl.conf || 0)}% combined${fairOdds(pl.conf) ? " · fair " + fairOdds(pl.conf) : ""}${pl.reason ? " — " + esc(pl.reason) : ""}</span></p>`
    ).join("");
  });

  const hrCard = tabbedCard("Top 3 HR props", (src, parsed) => {
    if (src === "Statalizers") {
      const hr = (dossier.props || []).filter(x => x.prop_type === "HR").slice(0, 3);
      return hr.map(x => `<p class="item">${esc(x.player)} <span class="muted">(${esc(x.game)}) ${x.conf}%${x.reasoning ? " — " + esc(x.reasoning) : ""}</span></p>`).join("");
    }
    return (parsed?.hr_props || []).slice(0, 3).map(x => `<p class="item">${esc(x.player)} <span class="muted">${Math.round(x.conf || 0)}%${x.reason ? " — " + esc(x.reason) : ""}</span></p>`).join("");
  });

  const propsCard = tabbedCard("Top 3 player props", (src, parsed) => {
    if (src === "Statalizers") {
      const pr = (dossier.props || []).filter(x => x.prop_type !== "HR").slice(0, 3);
      return pr.map(x => `<p class="item">${esc(x.player)} ${esc(x.prop_type)} ${esc(x.line)} <span class="muted">(${esc(x.game)}) ${x.conf}%${x.reasoning ? " — " + esc(x.reasoning) : ""}</span></p>`).join("");
    }
    return (parsed?.player_props || []).slice(0, 3).map(x => `<p class="item">${esc(x.player)} ${esc(x.prop || "")} <span class="muted">${Math.round(x.conf || 0)}%${x.reason ? " — " + esc(x.reason) : ""}</span></p>`).join("");
  });

  const best = consensus.find(x => x.isBest);
  const bestVotes = responses.filter(r => r.parsed?.best_bet).map(r => `<b>${esc(dispName(r.model))}</b>: ${esc(cleanPick(r.parsed.best_bet))}`).join("<br>");
  const bestHtml = best ? `<div class="card"><h3>Best bet</h3><p class="item">⭐ ${esc(best.pick)} — <span class="blend">${best.blended}%</span> blended</p>${bestVotes ? `<div class="src">Model best-bet votes</div><p class="item muted">${bestVotes}</p>` : ""}</div>` : "";

  const track = (await env.DB.prepare("SELECT source, SUM(result='WIN') w, SUM(result='LOSS') l, SUM(result='PUSH') pu FROM source_picks WHERE result IS NOT NULL GROUP BY source ORDER BY w DESC").all()).results || [];
  const trackHtml = track.length ? '<div class="card"><h3>Track record (graded)</h3>' + track.map(t => `<p class="item">${esc(t.source)} <span class="muted">${t.w}-${t.l}${t.pu ? "-" + t.pu : ""}</span></p>`).join("") + "</div>" : "";

  const tabScript = `<script>document.addEventListener('click',function(e){var t=e.target.closest('.tab');if(!t)return;var c=t.dataset.card;document.querySelectorAll('.tab[data-card="'+c+'"]').forEach(function(b){b.classList.toggle('on',b===t)});document.querySelectorAll('.pane[data-card="'+c+'"]').forEach(function(p){p.style.display=p.dataset.i===t.dataset.i?'block':'none'})});</script>`;

  return page(`MLB consensus report — ${run.run_date}`, `
    <p>${modelBadges} <span class="muted">run #${run.id} · ${esc(run.slate_summary || "")}</span></p>
    <div class="layout">
      <div>
        <table><tr><th>Pick</th><th>Type</th><th>Blend</th><th>Model confs / pushback</th></tr>${tableRows}</table>
        ${rerunForm}
      </div>
      <div>
        ${trackHtml}
        ${bestHtml}
        ${mlCard}
        ${parlayCard}
        ${hrCard}
        ${propsCard}
      </div>
    </div>${tabScript}`);
}

// ── router ───────────────────────────────────────────────────────────────────
export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(gradeSourcePicks(env).then(() => runConsensus(env, null)));
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
      for (const k of ["gemini_key", "openrouter_key", "openrouter_models", "openai_key", "openai_model"]) {
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
        if (provider === "openai") {
          const key = await getSetting(env, "openai_key");
          if (!key) return Response.json({ ok: false, error: "no key saved" });
          const r = await fetch("https://api.openai.com/v1/models", { headers: { "Authorization": `Bearer ${key}` } });
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

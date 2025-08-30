// ============================================================
// Cloudflare Worker (Modules syntax) — CORS-safe Router (FINAL)
// - Semua response selalu menyertakan CORS, bahkan saat 500
// - /api/reko/dates kini tanpa KV.list() → hemat kuota & stabil
// - /api/reko/daily mengembalikan 4 slot sekaligus (hemat KV)
// - KV.get pakai { type: 'json', cacheTtl: 60 } untuk edge cache
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsOrigin = resolveCorsOrigin(request, env);

    // ---------- 0) CORS Preflight ----------
    if (request.method === "OPTIONS") {
      const allow = corsOrigin || "*";
      const reqHdr = request.headers.get("Access-Control-Request-Headers") || "Content-Type, Authorization";
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": allow,
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": reqHdr,
          "Access-Control-Max-Age": "86400",
          "Vary": "Origin, Access-Control-Request-Headers",
        },
      });
    }

    // ---------- 1) Router dibungkus try/catch ----------
    try {
      // --- Routes utama ---
      if (request.method === "POST" && url.pathname === "/api/reko/ingest") {
        return ingestCSV(request, env, corsOrigin);
      }
      if (request.method === "GET" && url.pathname === "/api/reko/latest") {
        return getLatest(request, env, corsOrigin);
      }
      if (request.method === "GET" && url.pathname === "/api/reko/latest-any") {
        return getLatestAny(env, corsOrigin);
      }
      if (request.method === "GET" && url.pathname === "/api/reko/by-date") {
        return getByDate(request, env, corsOrigin);
      }
      if (request.method === "GET" && url.pathname === "/api/reko/daily") {
        return getDailyEndpoint(request, env, corsOrigin);
      }
      if (request.method === "GET" && url.pathname === "/api/reko/dates") {
        return listDates(env, corsOrigin);
      }
      if (request.method === "GET" && url.pathname === "/api/candidates") {
        return getCandidates(env, corsOrigin);
      }

      // Summary (tanpa jam)
      if (request.method === "POST" && url.pathname === "/api/reko/ingest-summary") {
        return ingestSummary(request, env, corsOrigin);
      }
      if (request.method === "GET" && url.pathname === "/api/reko/latest-summary") {
        return getLatestSummary(env, corsOrigin);
      }

      // Not found
      return json({ ok: false, error: "not_found" }, 404, corsOrigin || "*");
    } catch (err) {
      // ---------- 2) Error tak ter-handle → tetap JSON + CORS ----------
      return json({ ok: false, error: String(err?.message || err) }, 500, corsOrigin || "*");
    }
  },
};

/* =========================
 * CORS helpers
 * =======================*/

/**
 * Menentukan origin yang diizinkan berdasarkan env.ALLOWED_ORIGIN (coma-separated)
 * Contoh: ALLOWED_ORIGIN="http://127.0.0.1:5500,http://localhost:5500,https://app.domainmu.com"
 */
function resolveCorsOrigin(request, env) {
  const reqOrigin = request.headers.get("Origin") || "";
  const raw = (env.ALLOWED_ORIGIN || "*")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (raw.includes("*")) return "*";
  if (!reqOrigin) return ""; // non-browser / server-to-server
  return raw.includes(reqOrigin) ? reqOrigin : "";
}

/** Balas JSON + CORS (default * jika origin kosong) */
function json(body, status = 200, origin = "*") {
  const allow = origin && origin.length ? origin : "*";
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": allow,
      "Vary": "Origin",
    },
  });
}

/** 401 JSON + CORS */
function unauthorized(origin) {
  return json({ ok: false, error: "unauthorized" }, 401, origin);
}

/* =========================
 * Index tanggal (hindari KV.list di jalur read)
 * =======================*/
const DATES_INDEX_KEY = "index:dates"; // satu key JSON: ["2025-08-20", "2025-08-21", ...]

async function readDateIndex(env) {
  const arr = await env.REKO_KV.get(DATES_INDEX_KEY, { type: "json", cacheTtl: 60 }).catch(() => null);
  return Array.isArray(arr) ? arr : [];
}

async function upsertDateIndex(env, date, limit = 90) {
  const d = String(date || "");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return; // guard

  const current = await readDateIndex(env);
  if (current.includes(d)) return; // sudah ada

  const next = [...current, d].sort();    // asc
  const trimmed = next.slice(-limit);     // keep last N
  await env.REKO_KV.put(DATES_INDEX_KEY, JSON.stringify(trimmed));
}

/* =========================
 * latest-any helpers
 * =======================*/
function slotMinutes(slot) {
  const m = String(slot).match(/^(\d{2})(\d{2})$/);
  if (!m) return -1;
  return parseInt(m[1], 10) * 60 + parseInt(m[2], 10);
}
function cmpDateSlot(a, b) {
  if (a.date !== b.date) return a.date > b.date ? 1 : -1;
  const d = slotMinutes(a.slot) - slotMinutes(b.slot);
  return d === 0 ? 0 : d;
}

/* =========================
 * Main endpoints (per-slot)
 * =======================*/
async function getLatestAny(env, origin) {
  const SLOTS = ["0930", "1130", "1415", "1550"];
  const candidates = [];

  for (const s of SLOTS) {
    const ptr = await env.REKO_KV.get(`latest:${s}`, { type: "json", cacheTtl: 60 });
    if (ptr?.date && ptr?.key) candidates.push({ slot: s, date: ptr.date, key: ptr.key });
  }
  if (!candidates.length) {
    const res404 = json({ ok: false, error: "not_found" }, 404, origin);
    return res404;
  }

  candidates.sort(cmpDateSlot);
  const best = candidates[candidates.length - 1];
  const data = await env.REKO_KV.get(best.key, { type: "json", cacheTtl: 60 });
  const res = json(data || { ok: false, error: "not_found" }, data ? 200 : 404, origin);
  if (data) res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
  return res;
}

async function ingestCSV(request, env, origin) {
  // Auth
  const auth = request.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
  const workerToken = env.INGEST_TOKEN || env.CF_TOKEN;
  if (!token || token !== workerToken) return unauthorized(origin);

  // Query
  const url = new URL(request.url);
  const date = url.searchParams.get("date");
  const slot = url.searchParams.get("slot");
  const top = parseInt(url.searchParams.get("top") || "10", 10);
  if (!date || !slot) return json({ ok: false, error: "missing date/slot" }, 400, origin);

  // Read CSV
  const csvText = await request.text();
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return json({ ok: false, error: "empty_csv" }, 400, origin);
  const headers = lines[0].split(",");

  const rows = lines.slice(1).map((line) => {
    const cols = splitCsvLine(line, headers.length);
    const obj = {};
    headers.forEach((h, i) => { obj[h?.trim?.() ? h.trim() : h] = parseMaybeNumber(cols[i]); });
    return obj;
  });

  const payload = {
    ok: true,
    date,
    slot,
    top,
    cutoff: slotToCutoff(slot),
    generated_at: new Date().toISOString(),
    rows,
  };

  const key = `reko:${date}:${slot}`;
  const latestKey = `latest:${slot}`;
  await env.REKO_KV.put(key, JSON.stringify(payload));
  await env.REKO_KV.put(latestKey, JSON.stringify({ date, key }), { expirationTtl: 7 * 24 * 3600 });

  // ---- update index tanggal (hindari KV.list di jalur read) ----
  await upsertDateIndex(env, date);

  return json({ ok: true, key }, 200, origin);
}

async function getLatest(request, env, origin) {
  const url = new URL(request.url);
  const slot = url.searchParams.get("slot") || "0930";
  const latest = await env.REKO_KV.get(`latest:${slot}`, { type: "json", cacheTtl: 60 });
  if (!latest) return json({ ok: false, error: "not_found" }, 404, origin);
  const data = await env.REKO_KV.get(latest.key, { type: "json", cacheTtl: 60 });
  const res = json(data || { ok: false, error: "not_found" }, data ? 200 : 404, origin);
  if (data) res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
  return res;
}

async function getByDate(request, env, origin) {
  const url = new URL(request.url);
  const date = url.searchParams.get("date");
  const slot = url.searchParams.get("slot");
  if (!date || !slot) return json({ ok: false, error: "missing date/slot" }, 400, origin);
  const key = `reko:${date}:${slot}`;
  const data = await env.REKO_KV.get(key, { type: "json", cacheTtl: 60 });
  const res = json(data || { ok: false, error: "not_found" }, data ? 200 : 404, origin);
  if (data) res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
  return res;
}

/* =========================
 * /api/reko/daily — ambil 4 slot sekaligus
 * =======================*/
const DAILY_SLOTS = ["0930", "1130", "1415", "1550"];

async function getDaily(date, env) {
  const out = {};
  await Promise.all(DAILY_SLOTS.map(async (s) => {
    let v = await env.REKO_KV.get(`reko:${date}:${s}`, { type: "json", cacheTtl: 60 });
    if (v == null) {
      // Retry kecil untuk mitigasi propagasi KV
      await new Promise(r => setTimeout(r, 250));
      v = await env.REKO_KV.get(`reko:${date}:${s}`, { type: "json", cacheTtl: 60 });
    }
    out[s] = v;
  }));
  return { date, slots: DAILY_SLOTS, data: out };
}

async function getDailyEndpoint(request, env, origin) {
  const url = new URL(request.url);
  const date = url.searchParams.get("date");
  if (!date) return json({ ok: false, error: "missing date" }, 400, origin);
  const payload = await getDaily(date, env);
  const res = json({ ok: true, ...payload }, 200, origin);
  res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
  return res;
}

/* =========================
 * /api/reko/dates — tanpa KV.list()
 * =======================*/
async function listDates(env, origin) {
  try {
    const dates = await readDateIndex(env); // 1x KV.get ringan
    const res = json({ ok: true, dates }, 200, origin);
    res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
    return res;
  } catch (err) {
    return json({ ok: false, error: "dates_index_error", detail: String(err?.message || err) }, 500, origin);
  }
}

/* =========================
 * Summary (tanpa jam)
 * =======================*/
function normKey(k) {
  return String(k || "")
    .replace(/^\uFEFF/, "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/≥|>=/g, "ge")
    .replace(/\s+/g, " ")
    .trim();
}
function parseNumLoose(v) {
  if (v == null) return null;
  const s = String(v).trim().replace(/\u00A0/g, " ").replace(/,/g, "").replace(/\s*%$/, "");
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}
function toProb01(v) {
  const n = parseNumLoose(v);
  if (n == null) return null;
  return n > 1 ? n / 100 : n; // 45 -> 0.45, 0.45 -> 0.45
}
function normalizeSummaryPayload(data) {
  if (!data || !Array.isArray(data.rows)) return data;
  const r0 = data.rows[0] || {};
  const already = "ticker" in r0 || "symbol" in r0;
  if (already) return data;

  const normRows = data.rows
    .map((m) => {
      const ticker = String(m["Kode Saham"] ?? m["ticker"] ?? m["Symbol"] ?? m["symbol"] ?? "").toUpperCase();
      const score = parseNumLoose(m["Skor Sistem"] ?? m["score"]);
      const rekom = m["Rekomendasi Singkat"] ?? m["rekomendasi"] ?? m["rekom"] ?? "";
      const p_close = toProb01(m["Peluang Bertahan sampai Tutup"]);
      const p_am = toProb01(m["Peluang Naik ge3% Besok Pagi"] ?? m["Peluang Naik ≥3% Besok Pagi"]);
      const p_next = toProb01(m["Peluang Lanjut Naik Lusa"]);
      const p_chain = toProb01(m["Peluang Total Berantai"]);
      return { ticker, score, rekom, p_close, p_am, p_next, p_chain };
    })
    .filter((r) => r.ticker);
  return { ...data, type: "summary", rows: normRows };
}
function splitCsvRow(line) { return line.split(","); }

async function ingestSummary(request, env, origin) {
  // Auth
  const auth = request.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
  const workerToken = env.INGEST_TOKEN || env.CF_TOKEN;
  if (!token || token !== workerToken) return unauthorized(origin);

  const url = new URL(request.url);
  const date = url.searchParams.get("date");
  if (!date) return json({ ok: false, error: "missing date" }, 400, origin);

  const csvText = await request.text();
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return json({ ok: false, error: "empty_csv" }, 400, origin);

  const head = splitCsvRow(lines[0]).map(normKey);
  const rows = lines.slice(1).map((ln) => {
    const cells = splitCsvRow(ln);
    const m = {};
    head.forEach((h, i) => { m[h] = (cells[i] ?? "").trim(); });

    const ticker = (m["kode saham"] || m["ticker"] || m["symbol"] || "").toUpperCase();
    const score  = parseNumLoose(m["skor sistem"] ?? m["score"]);
    const rekom  = m["rekomendasi singkat"] || m["rekomendasi"] || m["rekom"] || "";

    return {
      ticker,
      score,
      rekom,
      p_close: toProb01(m["peluang bertahan sampai tutup"]),
      p_am:    toProb01(m["peluang naik ge3% besok pagi"]),
      p_next:  toProb01(m["peluang lanjut naik lusa"]),
      p_chain: toProb01(m["peluang total berantai"]),
    };
  }).filter(r => r.ticker);

  const payload = { ok: true, type: "summary", date, generated_at: new Date().toISOString(), rows };

  // Nama baru & legacy
  const keyNew = `summary:${date}`;
  await env.REKO_KV.put(keyNew, JSON.stringify(payload));
  await env.REKO_KV.put("latest:summary", JSON.stringify({ date, key: keyNew }), { expirationTtl: 14 * 24 * 3600 });

  const keyLegacy = `reko:${date}:sum`;
  await env.REKO_KV.put(keyLegacy, JSON.stringify(payload));
  await env.REKO_KV.put("latest:sum", JSON.stringify({ date, key: keyLegacy }), { expirationTtl: 14 * 24 * 3600 });

  // ---- update index tanggal ----
  await upsertDateIndex(env, date);

  return json({ ok: true, key: keyNew, legacy: keyLegacy }, 200, origin);
}

async function getLatestSummary(env, origin) {
  // pointer baru → fallback legacy
  let ptr = await env.REKO_KV.get("latest:summary", { type: "json", cacheTtl: 60 });
  if (!ptr?.key) ptr = await env.REKO_KV.get("latest:sum", { type: "json", cacheTtl: 60 });
  if (!ptr?.key) return json({ ok: false, error: "not_found" }, 404, origin);

  // ambil data
  let data = await env.REKO_KV.get(ptr.key, { type: "json", cacheTtl: 60 });

  // jika legacy pointer tapi value kosong → coba nama baru
  if (!data && /^reko:\d{4}-\d{2}-\d{2}:sum$/.test(ptr.key)) {
    const m = ptr.key.match(/^reko:(\d{4}-\d{2}-\d{2}):sum$/);
    if (m) data = await env.REKO_KV.get(`summary:${m[1]}`, { type: "json", cacheTtl: 60 });
  }
  if (!data) return json({ ok: false, error: "not_found" }, 404, origin);

  const normalized = normalizeSummaryPayload(data);
  const res = json(normalized, 200, origin);
  res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
  return res;
}

/* =========================
 * Adapter: /api/candidates
 * =======================*/
async function getCandidates(env, origin) {
  const SLOTS = ["0930", "1130", "1415", "1550"];
  const candidates = [];
  for (const s of SLOTS) {
    const ptr = await env.REKO_KV.get(`latest:${s}`, { type: "json", cacheTtl: 60 });
    if (ptr?.date && ptr?.key) candidates.push({ slot: s, date: ptr.date, key: ptr.key });
  }
  if (!candidates.length) {
    const res = json({ tickers: [], announce_at: null, detail: [] }, 200, origin);
    res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
    return res;
  }

  candidates.sort(cmpDateSlot);
  const best = candidates[candidates.length - 1];
  const latest = await env.REKO_KV.get(best.key, { type: "json", cacheTtl: 60 });
  if (!latest?.rows?.length) {
    const res = json({ tickers: [], announce_at: null, detail: [] }, 200, origin);
    res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
    return res;
  }

  const topN = latest.rows.slice(0, 3);
  const detail = topN.map((r) => {
    const tkr = String(r.ticker || "").toUpperCase().replace(/\.JK$/, "");
    const score = isNum(r.score) ? Number(r.score) : null;

    const reasons = [];
    if (isNum(r.daily_return))      reasons.push(`Return Hari Ini ${fmtPct(r.daily_return)}`);
    if (isNum(r.vol_pace))          reasons.push(`Volume pace ${fmtTimes(r.vol_pace)} rata-rata`);
    if (isNum(r.closing_strength))  reasons.push(`Closing strength ${fmtPct(r.closing_strength)}`);
    if (isNum(r.afternoon_power))   reasons.push(`Afternoon power ${fmtPct(r.afternoon_power)}`);
    if (!reasons.length && isNum(r.last)) {
      reasons.push(`Harga terakhir ${Number(r.last).toLocaleString("id-ID")}`);
    }

    return { ticker: tkr, score, reasons };
  });

  const out = {
    tickers: detail.map((d) => d.ticker),
    announce_at: `${latest.date} ${latest.cutoff || "09:05"}:00`,
    detail,
  };
  const res = json(out, 200, origin || "*");
  res.headers.set("Cache-Control", "public, max-age=60, stale-while-revalidate=300");
  return res;
}

/* =========================
 * Misc utils
 * =======================*/
function isNum(v) {
  const n = Number(v);
  return Number.isFinite(n);
}
function fmtPct(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  const pct = Math.abs(n) <= 1 ? n * 100 : n;
  return (pct >= 0 ? "+" : "") + pct.toFixed(1) + "%";
}
function fmtTimes(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  return (n >= 10 ? n.toFixed(0) : n.toFixed(1)) + "x";
}
function slotToCutoff(slot) {
  return ({ "0930": "09:30", "1130": "11:30", "1415": "14:15", "1550": "15:50" })[slot] || slot;
}
function parseMaybeNumber(v) {
  if (v == null) return null;
  const t = ("" + v).trim();
  if (t === "") return "";
  const n = Number(t.replace(/,/g, ""));
  return Number.isFinite(n) ? n : t;
}
function splitCsvLine(line, expectedCols) {
  const parts = line.split(",");
  while (parts.length < expectedCols) parts.push("");
  return parts;
}

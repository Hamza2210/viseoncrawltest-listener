/**
 * Schema-change detector — PROXY model (sits in front of your existing site).
 * ----------------------------------------------------------------------------
 * This Worker does NOT serve your pages. Your existing static worker keeps serving
 * them on its Custom Domain. This Worker is attached to a ROUTE on the same hostname;
 * per Cloudflare routing, a route takes precedence over a custom domain, and your
 * OTHER workers' more-specific routes (e.g. /api/*) take precedence over this one — so
 * they are untouched. This Worker only handles page paths no other route claims.
 *
 *   fetch()            — proxies each request to origin via fetch(request) (which, on
 *                        this same zone, invokes your static worker / origin), returns
 *                        it UNTOUCHED, and inspects a clone in the background. On a real
 *                        JSON-LD change it hands {url, hash} to the coordinator DO.
 *                        Also exposes the seeder's control route at "/__schema-seed".
 *
 *   SchemaCoordinator  — Durable Object. One strongly-consistent instance owns all
 *                        state: per-URL hashes, the pending list, the seed cursor.
 *
 *   scheduled()        — Cron Trigger. Each tick: one SEED batch (until the baseline
 *                        pass is complete), then flush the pending list.
 *
 * SEEDING reads the master URL list (all_urls.json at S3_URLS_KEY) and baselines each
 * page through the SAME proxied fetch path live traffic uses. Run with
 * ENQUEUE_NEW_PAGES="false"; once it reports done, flip to "true".
 *
 * Set RESERVED CONCURRENCY = 1 on the singlepage Lambda so two parquet merges never
 * overlap (the endpoint invokes the Lambda asynchronously).
 */

import { DurableObject } from "cloudflare:workers";
import { AwsClient } from "aws4fetch";

// --- Tunables ---------------------------------------------------------------
const THROTTLE_SECONDS = 300; // re-inspect any one URL at most once / 5 min (per colo)
const MAX_BATCH = 10;         // URLs flushed to the endpoint per cron tick (one Lambda run)

const SEED_BATCH = 100;       // URLs scanned per cron tick during the baseline pass
const SEED_CONCURRENCY = 10;  // parallel page fetches within a seed batch

const HASH_PREFIX = "hash:";       // hash:<sha(url)>     -> last-seen schema hash
const PENDING_PREFIX = "pending:"; // pending:<sha(url)>  -> changed URL awaiting flush
const SEED_CURSOR_KEY = "seed:cursor";
const SEED_TOTAL_KEY = "seed:total";
const SEED_DONE_KEY = "seed:done";

const DO_NAME = "coordinator"; // one coordinator instance per deployment

// --- Request identity (matches the crawler / Lambda for origin allowlisting) ----
function crawlerHeaders(env) {
  return {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Fuseon/1.0",
    "x-worker-auth": env.WORKER_AUTH || "",
    "Cache-Control": "no-cache",
  };
}

// --- URL normalisation: identical rule to the Lambda's ----------------------
function normalizeUrl(raw) {
  let s = (raw || "").trim();
  if (!s) return "";
  if (!s.includes("://")) s = "https://" + s;
  let u;
  try { u = new URL(s); } catch { return ""; }
  const scheme = u.protocol.replace(":", "").toLowerCase();
  const host = u.hostname.toLowerCase().replace(/\.+$/, "");
  const port = u.port ? ":" + u.port : "";
  const path = u.pathname.replace(/\/+$/, "");
  return `${scheme}://${host}${port}${path}${u.search || ""}`;
}

// --- JSON-LD extraction via streaming HTMLRewriter --------------------------
async function extractJsonLd(response) {
  const blocks = [];
  let current = "";
  const rewriter = new HTMLRewriter().on('script[type="application/ld+json"]', {
    element(el) {
      current = "";
      el.onEndTag(() => { blocks.push(current); current = ""; });
    },
    text(chunk) { current += chunk.text; },
  });
  await rewriter.transform(response).text(); // drive the stream so handlers run
  return blocks;
}

// --- Canonicalisation: only MEANINGFUL changes move the hash ----------------
function stableStringify(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return "[" + value.map(stableStringify).join(",") + "]";
  const keys = Object.keys(value).sort();
  return "{" + keys.map((k) => JSON.stringify(k) + ":" + stableStringify(value[k])).join(",") + "}";
}
function canonicalizeBlocks(blocks) {
  const canon = blocks
    .map((b) => {
      const t = (b || "").trim();
      if (!t) return "";
      try { return stableStringify(JSON.parse(t)); } catch { return t; }
    })
    .filter(Boolean);
  canon.sort(); // <script> block order shouldn't matter
  return canon.join("\n");
}
async function sha256Hex(str) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(str));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

// --- Fetch a page (proxied to origin) and compute its schema hash -----------
async function hashPage(rawUrl, env) {
  const url = normalizeUrl(rawUrl);
  if (!url) return null;
  // fetch() to this same zone invokes the origin (your static worker), not this Worker.
  const res = await fetch(url, { headers: crawlerHeaders(env) });
  if (!res.ok) throw new Error(`fetch ${res.status}`);
  const hash = await sha256Hex(canonicalizeBlocks(await extractJsonLd(res)));
  return { url, hash };
}

// --- Read the master URL list (all_urls.json) from S3 via SigV4 -------------
async function readAllUrls(env) {
  const aws = new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY,
    secretAccessKey: env.AWS_SECRET_KEY,
    region: env.AWS_REGION,
    service: "s3",
  });
  // Virtual-hosted-style. Dotted bucket name? Use path-style:
  //   `https://s3.${env.AWS_REGION}.amazonaws.com/${env.S3_BUCKET}/${env.S3_URLS_KEY}`
  const endpoint =
    `https://${env.S3_BUCKET}.s3.${env.AWS_REGION}.amazonaws.com/${env.S3_URLS_KEY}`;
  const res = await aws.fetch(endpoint, { method: "GET" });
  if (!res.ok) throw new Error(`S3 GET ${res.status} for ${env.S3_URLS_KEY}`);
  const data = await res.json();
  if (!Array.isArray(data)) throw new Error("all_urls.json is not a JSON array of URLs");
  return data.filter((u) => typeof u === "string" && u.trim());
}

// --- Bounded-concurrency map ------------------------------------------------
async function mapLimit(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  const lanes = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx]);
    }
  });
  await Promise.all(lanes);
  return out;
}

// --- Flush the pending list to the endpoint ---------------------------------
async function postBatch(urls, env) {
  try {
    const resp = await fetch(env.ENDPOINT_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": env.API_KEY,
        "x-aws-access-key": env.AWS_ACCESS_KEY,
        "x-aws-secret-key": env.AWS_SECRET_KEY,
        "x-aws-region": env.AWS_REGION,
        "x-s3-bucket": env.S3_BUCKET,
        "x-s3-key": env.S3_KEY,
        "x-s3-pages-key": env.S3_PAGES_KEY,
        "x-s3-domain-key": env.S3_DOMAIN_KEY,
        "x-s3-urls-key": env.S3_URLS_KEY,
        qlikAutomation: env.QLIK_AUTOMATION,
        qlikTenant: env.QLIK_TENANT,
        qlikBearer: env.QLIK_BEARER,
      },
      body: JSON.stringify({ urls, report: "false", email: env.NOTIFY_EMAIL || "" }),
    });
    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      console.error(`[flush] endpoint ${resp.status}: ${t.slice(0, 300)}`);
      return false;
    }
    console.log(`[flush] posted ${urls.length} url(s)`);
    return true;
  } catch (e) {
    console.error("[flush] endpoint POST failed:", e);
    return false;
  }
}

// ----------------------------------------------------------------------------
// Durable Object: the single source of truth. Pure storage, no network.
// ----------------------------------------------------------------------------
export class SchemaCoordinator extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.storage = ctx.storage;
    this.env = env;
  }

  async record(url, hash) {
    const urlKey = await sha256Hex(url);
    const stored = await this.storage.get(HASH_PREFIX + urlKey);

    if (stored === hash) return;                          // unchanged
    await this.storage.put(HASH_PREFIX + urlKey, hash);   // persist baseline

    if (stored === undefined && this.env.ENQUEUE_NEW_PAGES === "false") return;

    await this.storage.put(PENDING_PREFIX + urlKey, url); // dedup by key
  }

  async seedState() {
    const cursor = (await this.storage.get(SEED_CURSOR_KEY)) || 0;
    const total = (await this.storage.get(SEED_TOTAL_KEY)) || 0;
    const done = (await this.storage.get(SEED_DONE_KEY)) === true;
    return { cursor, total, done };
  }

  async unbaselined(urls) {
    const out = [];
    for (const url of urls) {
      const has = await this.storage.get(HASH_PREFIX + (await sha256Hex(url)));
      if (has === undefined) out.push(url);
    }
    return out;
  }

  async commitSeed(pairs, newCursor, total) {
    for (const { url, hash } of pairs) {
      await this.storage.put(HASH_PREFIX + (await sha256Hex(url)), hash);
    }
    const done = newCursor >= total;
    await this.storage.put(SEED_CURSOR_KEY, newCursor);
    await this.storage.put(SEED_TOTAL_KEY, total);
    await this.storage.put(SEED_DONE_KEY, done);
    return done;
  }

  async resetSeed() {
    await this.storage.delete([SEED_CURSOR_KEY, SEED_TOTAL_KEY, SEED_DONE_KEY]);
    return true;
  }

  async flush() {
    const pending = await this.storage.list({ prefix: PENDING_PREFIX, limit: MAX_BATCH });
    if (pending.size === 0) return;

    const keys = [...pending.keys()];
    const urls = [...new Set([...pending.values()].filter(Boolean))];

    if (urls.length === 0) {
      await this.storage.delete(keys);
      return;
    }
    const ok = await postBatch(urls, this.env);
    if (ok) await this.storage.delete(keys);
  }
}

function coordinator(env) {
  return env.SCHEMA_DO.get(env.SCHEMA_DO.idFromName(DO_NAME));
}

// --- One seed batch ---------------------------------------------------------
async function runSeedBatch(env, stub) {
  const state = await stub.seedState();
  if (state.done) return;

  const urls = await readAllUrls(env);
  const total = urls.length;
  const start = Math.min(state.cursor, total);
  const window = urls.slice(start, start + SEED_BATCH).map(normalizeUrl).filter(Boolean);
  const newCursor = start + Math.min(SEED_BATCH, Math.max(0, total - start));

  const todo = window.length ? await stub.unbaselined(window) : [];
  let pairs = [];
  if (todo.length) {
    const results = await mapLimit(todo, SEED_CONCURRENCY, async (u) => {
      try { return await hashPage(u, env); }
      catch (e) { console.error("[seed] skip", u, e.message); return null; }
    });
    pairs = results.filter(Boolean);
  }

  const done = await stub.commitSeed(pairs, newCursor, total);
  console.log(`[seed] ${start}->${newCursor}/${total}, baselined ${pairs.length}, done=${done}`);
}

// --- Background inspection (runs in waitUntil; never blocks the response) ----
async function inspect(rawUrl, clonedResponse, env, ctx) {
  try {
    const url = normalizeUrl(rawUrl);
    if (!url) return;

    const cache = caches.default;
    const throttleKey = new Request("https://throttle.local/" + encodeURIComponent(url));
    if (await cache.match(throttleKey)) return;
    ctx.waitUntil(
      cache.put(
        throttleKey,
        new Response("1", { headers: { "Cache-Control": "max-age=" + THROTTLE_SECONDS } })
      )
    );

    const hash = await sha256Hex(canonicalizeBlocks(await extractJsonLd(clonedResponse)));
    await coordinator(env).record(url, hash);
  } catch (e) {
    console.error("[inspect] error:", e);
  }
}

export default {
  async fetch(request, env, ctx) {
    const u = new URL(request.url);

    // Authenticated seeder control route (guarded by the WORKER_AUTH secret).
    if (u.pathname === "/__schema-seed") {
      const provided = request.headers.get("x-worker-auth") || "";
      if (!env.WORKER_AUTH || provided !== env.WORKER_AUTH) {
        return new Response("forbidden", { status: 403 });
      }
      const stub = coordinator(env);
      if (u.searchParams.get("action") === "reset") {
        await stub.resetSeed();
        return Response.json({ ok: true, action: "reset" });
      }
      return Response.json(await stub.seedState());
    }

    // Proxy to origin (your static worker), return untouched, inspect a clone.
    const response = await fetch(request);
    if (request.method === "GET" && response.ok) {
      const ct = (response.headers.get("content-type") || "").toLowerCase();
      if (ct.includes("text/html")) {
        ctx.waitUntil(inspect(request.url, response.clone(), env, ctx));
      }
    }
    return response;
  },

  async scheduled(event, env, ctx) {
    const stub = coordinator(env);
    ctx.waitUntil(
      (async () => {
        try { await runSeedBatch(env, stub); }
        catch (e) { console.error("[seed] batch error:", e); }
        await stub.flush();
      })()
    );
  },
};

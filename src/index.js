/**
 * Schema-change detector (in-path) + a Durable Object that accumulates the changed
 * URLs and flushes them to /enqueue-singlepage on a Cron Trigger.
 * ----------------------------------------------------------------------------
 * THREE pieces in one Worker:
 *
 *   fetch()            — in-path detector. Proxies each request to origin and returns
 *                        it UNTOUCHED, then inspects a clone in the background
 *                        (ctx.waitUntil) so visitors get zero added latency. Extracts
 *                        the page's JSON-LD, canonicalises + hashes it, and hands the
 *                        {url, hash} pair to the coordinator Durable Object.
 *
 *   SchemaCoordinator  — the Durable Object. ONE strongly-consistent, single-threaded
 *                        instance owns ALL state: the per-URL last-seen hashes AND the
 *                        pending list. Because it is single-threaded and transactional,
 *                        the compare-and-set and the dedup are race-free (no KV eventual
 *                        consistency, no cross-colo duplicates). It also performs the
 *                        flush, so reading the pending list and POSTing it is atomic.
 *
 *   scheduled()        — runs on the Cron Trigger and just pokes the DO's flush().
 *                        The cron interval is your "every x" flush window.
 *
 * Set RESERVED CONCURRENCY = 1 on the singlepage Lambda so two parquet merges never
 * overlap (the endpoint invokes the Lambda asynchronously).
 *
 * NOTE: adding a Durable Object means this Worker needs a migration (see wrangler.toml)
 * and is deployed via Wrangler — which is exactly what the GitHub -> Cloudflare build
 * integration runs for you on every push.
 */

import { DurableObject } from "cloudflare:workers";

// --- Tunables ---------------------------------------------------------------
const THROTTLE_SECONDS = 300; // re-inspect any one URL at most once / 5 min (per colo)
const MAX_BATCH = 10;         // URLs per endpoint call (one Lambda run). Overflow waits
                              //   for the next cron run. NOTE: with an hourly cron this
                              //   caps you at MAX_BATCH changes flushed per hour — raise
                              //   it if a single site can change more than that hourly.

const HASH_PREFIX = "hash:";       // hash:<sha(url)>     -> last-seen schema hash
const PENDING_PREFIX = "pending:"; // pending:<sha(url)>  -> changed URL awaiting flush

// One coordinator instance per deployment. fetch() and scheduled() both resolve to the
// same DO via this name, so all inspection traffic and the flush hit the same state.
const DO_NAME = "coordinator";

// --- Request identity (must match the crawler / Lambda for allowlisting) ----
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

// --- Flush the pending list to the endpoint ---------------------------------
// Module-level so the Durable Object can call it (it has env). Same wire format as before.
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
// Durable Object: the single source of truth.
// ----------------------------------------------------------------------------
export class SchemaCoordinator extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.storage = ctx.storage; // transactional, strongly-consistent per-object storage
    this.env = env;
  }

  /**
   * Authoritative compare-and-set for ONE URL's schema hash. Called via RPC from the
   * in-path fetch handler (which has already throttled per-colo). Because the DO is
   * single-threaded, this whole method runs without interleaving from other requests,
   * so the read/compare/write is atomic — no KV race, no duplicate enqueues.
   */
  async record(url, hash) {
    const urlKey = await sha256Hex(url); // stable, fixed-length storage key suffix
    const stored = await this.storage.get(HASH_PREFIX + urlKey);

    if (stored === hash) return;                          // unchanged -> nothing to do
    await this.storage.put(HASH_PREFIX + urlKey, hash);   // persist new baseline

    // First time we've ever seen this URL. DO storage returns `undefined` (not null)
    // for a missing key. ENQUEUE_NEW_PAGES (default "true") decides whether a brand-new
    // page counts as content worth enqueuing.
    if (stored === undefined && this.env.ENQUEUE_NEW_PAGES === "false") return;

    // Add to the pending list. Same URL -> same key -> deduped automatically.
    await this.storage.put(PENDING_PREFIX + urlKey, url);
  }

  /**
   * Drain up to MAX_BATCH pending URLs and POST them. Reading the list, POSTing, and
   * deleting all happen inside this one single-threaded turn, so a concurrent record()
   * can't slip a URL in between the read and the delete and lose it.
   */
  async flush() {
    const pending = await this.storage.list({ prefix: PENDING_PREFIX, limit: MAX_BATCH });
    if (pending.size === 0) return; // nothing pending -> don't trigger the endpoint

    const keys = [...pending.keys()];
    const urls = [...new Set([...pending.values()].filter(Boolean))];

    if (urls.length === 0) {
      await this.storage.delete(keys); // stray keys with no value -> clean up and stop
      return;
    }

    const ok = await postBatch(urls, this.env);
    if (ok) {
      // Clear only what we listed. Anything that arrived since stays for the next run.
      await this.storage.delete(keys);
    }
    // On failure we keep the list; the next cron run retries. Nothing is lost.
  }
}

// Resolve the one coordinator instance.
function coordinator(env) {
  const id = env.SCHEMA_DO.idFromName(DO_NAME);
  return env.SCHEMA_DO.get(id);
}

// --- Background inspection (runs in waitUntil; never blocks the response) ----
async function inspect(rawUrl, clonedResponse, env, ctx) {
  try {
    const url = normalizeUrl(rawUrl);
    if (!url) return;

    // Per-colo throttle (Cache API). This is now purely a load-reducer in front of the
    // DO: it caps how often a hot page is re-hashed and how often we touch the DO. The
    // DO stays the single source of truth for whether the schema actually changed, so
    // it's fine that this marker is per-colo and only eventually consistent.
    const cache = caches.default;
    const throttleKey = new Request("https://throttle.local/" + encodeURIComponent(url));
    if (await cache.match(throttleKey)) return;
    ctx.waitUntil(
      cache.put(
        throttleKey,
        new Response("1", { headers: { "Cache-Control": "max-age=" + THROTTLE_SECONDS } })
      )
    );

    // Hash here (work is spread across colos, not concentrated in the single DO), then
    // hand the small {url, hash} pair to the coordinator for the consistent compare.
    const hash = await sha256Hex(canonicalizeBlocks(await extractJsonLd(clonedResponse)));
    await coordinator(env).record(url, hash);
  } catch (e) {
    console.error("[inspect] error:", e);
  }
}

export default {
  // In-path detector.
  async fetch(request, env, ctx) {
    // fetch(request) on the incoming request goes to origin, NOT back through this
    // Worker, so the pass-through proxy doesn't loop.
    const response = await fetch(request);

    if (request.method === "GET" && response.ok) {
      const ct = (response.headers.get("content-type") || "").toLowerCase();
      if (ct.includes("text/html")) {
        ctx.waitUntil(inspect(request.url, response.clone(), env, ctx));
      }
    }
    return response;
  },

  // Cron Trigger: the DO owns the pending list and does the flush atomically, so we
  // just poke it. If there's nothing pending, flush() returns immediately.
  async scheduled(event, env, ctx) {
    ctx.waitUntil(coordinator(env).flush());
  },
};

# schema-change-detector (Durable Object build)

A single Cloudflare Worker that watches an entire (Cloudflare-proxied) domain for
JSON-LD schema changes, accumulates the changed URLs in a **Durable Object**, and
flushes that list to your `/enqueue-singlepage` endpoint on a Cron Trigger.

The Durable Object is one strongly-consistent, single-threaded coordinator that owns
all state (the per-URL hashes **and** the pending list). That removes the
eventual-consistency races the old KV version had: the compare-and-set per URL is
atomic, dedup is exact, and the flush can read + POST + delete without another request
slipping a URL in between. There is **no KV namespace anymore** — the DO replaces it.

## Deploy from GitHub

A Durable Object needs a migration, so this is deployed with Wrangler (the git build
integration runs `wrangler deploy` for you), not by pasting code into the dashboard.

1. Push these files to your repo (`src/index.js`, `wrangler.toml`, `README.md`).
2. Cloudflare dashboard -> **Workers & Pages** -> **Create** -> **Connect to Git**,
   pick the repo. Cloudflare builds and deploys on every push and applies the
   `[[migrations]]` block automatically (this is what creates the `SchemaCoordinator`
   Durable Object).
3. Edit `wrangler.toml`: set your real `routes` zone/pattern and the `[vars]`.
4. Add the **secrets** (see below) — these are never committed.
5. Add the **Cron Trigger** if it isn't picked up from `wrangler.toml` (it should be).

## What you configure

**Plaintext variables** — in `wrangler.toml` `[vars]` (committed) or the dashboard
(type *Text*):

| Name | Example |
|---|---|
| `ENDPOINT_URL` | `https://your-api.example.com/enqueue-singlepage` |
| `AWS_REGION` | `eu-west-1` |
| `S3_BUCKET` | `your-bucket` |
| `S3_KEY` | `clients/acme` |
| `S3_PAGES_KEY` | `clients/acme/pages` |
| `S3_DOMAIN_KEY` | `clients/acme/domain` |
| `S3_URLS_KEY` | `clients/acme/urls` |
| `NOTIFY_EMAIL` | (blank, or an address) |
| `ENQUEUE_NEW_PAGES` | `true` |

**Secrets** — NOT in the repo. Add as encrypted secrets in the dashboard
(Worker -> Settings -> Variables and Secrets) or via `wrangler secret put <NAME>`:
- `API_KEY`, `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`,
  `QLIK_AUTOMATION`, `QLIK_TENANT`, `QLIK_BEARER`, `WORKER_AUTH`

**Route** — in `wrangler.toml` `routes` (or dashboard). Zone = your domain,
pattern = `example.com/*` (use your real domain, orange-clouded/proxied).

**Cron Trigger** — `0 * * * *` (once an hour, on the hour), set in
`wrangler.toml` `[triggers]`. This is your flush window.

**Durable Object binding & migration** — already in `wrangler.toml`:
binding `SCHEMA_DO` -> class `SchemaCoordinator`, created by the `tag = "v1"`
migration with `new_sqlite_classes`. Nothing to click; the deploy handles it.

## Important: serialise the Lambda

The endpoint invokes `singlepage` asynchronously, so the flush POST returns before the
merge finishes. Guarantee two merges never overwrite `s1.parquet` at once:

```bash
aws lambda put-function-concurrency --function-name singlepage --reserved-concurrent-executions 1
```

## Tuning (top of `src/index.js`)

| Setting | Where | Effect |
|---|---|---|
| Cron expression | `wrangler.toml` | Flush frequency. `0 * * * *` = hourly. |
| `MAX_BATCH` | code (10) | URLs flushed per cron run (one Lambda call). **With an hourly cron this caps you at 10 changed URLs per hour** — raise it if a site can change more than that in an hour, or the backlog will take several hours to drain. |
| `THROTTLE_SECONDS` | code (300) | How often any one URL is re-inspected (per colo). Raise to cut cost; lower for faster detection. Now only a load-reducer in front of the DO, not part of correctness. |
| `ENQUEUE_NEW_PAGES` | var (`true`) | First-seen pages trigger a scan (catches new articles). See below. |

## New pages & the first-run seed

With `ENQUEUE_NEW_PAGES = "true"`, a first-seen page is treated as new content and
enqueued — so new articles fire. The catch: on first deploy every existing page is
"first-seen", so traffic would enqueue the whole site once. To avoid that, seed the DO
with the current hashes of your existing URLs (from `S3_URLS_KEY`) before going live,
or set the var to `"false"` for the first day to baseline via traffic, then flip it.

## Notes

- The Worker proxies every zone request; the visitor response is returned untouched and
  inspection runs on a clone in `waitUntil`, so no added latency. You pay for the
  in-path requests + background work, so Workers Paid is recommended for a live domain.
- One DO coordinates the whole deployment. All inspection traffic funnels to that single
  single-threaded instance, but the per-colo throttle keeps the volume low (each URL is
  hashed at most once per `THROTTLE_SECONDS` per colo). For a very high-traffic site with
  thousands of distinct hot URLs you'd want to watch the DO's load; the work per call is
  tiny (a couple of storage ops).
- The DO is strongly consistent, so a change recorded just before a cron run is reliably
  visible to the flush. Nothing is lost; a failed flush is retried on the next run.
- First time a page is seen it's baselined (unless `ENQUEUE_NEW_PAGES` catches it as new).
- The DO uses SQLite-backed storage. You can't switch an already-deployed class to a
  different storage backend, so leave the `v1` migration as-is.

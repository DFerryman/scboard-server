# hnreader server (sync-only)

A real data pipeline for Hacker News fetch + AI enrichment + daily digest +
cloud sync, persisting to SQLite and pushing to the cloud-dev database.

**Architecture overview (sync-only)**:

- Production deployment only runs `python -m server.ingest --loop`. On each
  finalize, `ingest._trigger_and_record_cloud_sync` chains the two cloud sync
  phases: first `cloud_sync_runner.run_business_once` (business `writeBatch` +
  `switchMeta` version switch); after this round's `cloud_sync_runs` reaches a
  terminal state, it then runs `cloud_sync_runner.run_dashboard_once` (a
  separate `writeDashboard` action that pushes the dashboard projection). Both
  phases are HMAC-signed and pushed to `cloudfunctions/pushSync`.
- Mini-program business reads go 100% through cloud functions (reading the
  `stories` / `topics` / `digests` / `meta` collections in the cloud-dev
  database).
- The ops dashboard also reads the cloud-dev database; its collections are
  `hn_dashboard_summary` / `hn_dashboard_ingest_runs` /
  `hn_dashboard_cloud_sync_runs`, whose content is built separately by
  `server/dashboard_projection.py` during the dashboard phase and pushed via
  `writeDashboard`.
- **The server no longer listens on any HTTP port**: the FastAPI /
  `/dashboard` / `/api/admin/*` endpoints previously kept for "local dashboard
  debugging" have all been removed; `server/main.py`, `server/auth.py`, and
  `server/web/` no longer exist; `requirements.txt` no longer depends on
  fastapi / uvicorn.

`docs/cloud-migration-followup.md` records the P0/P1/P2/P4 migration process.
This README plus the code is the latest source of truth.

## Module overview

```
server/
├── __init__.py             # package entry point
├── schemas.py              # Pydantic data contracts
├── settings.py             # runtime configuration (HNREADER_* env vars)
├── db.py                   # SQLite connection + DDL + column migration (WAL, foreign keys)
├── repository.py           # row <-> Story conversion, all SQL reads/writes
├── topics.py               # dynamic topic normalization
├── hn_client.py            # Hacker News HTTP client
├── http_client.py          # generic HTTP client with retry/timeout
├── normalizer.py           # HN item -> stories row dict
├── ai_agent.py             # Fallback/RealAiAgent + batch protocol
├── ai_config_status.py     # AI provider status probe + cache (called by cloud_sync on the sync cadence)
├── ingest.py               # Fetcher + Enricher + supervisor; finalize triggers cloud sync
├── digest.py               # daily digest prepare/commit
├── cleanup.py              # expired data cleanup
├── cloud_sync.py           # SQLite -> read model + dashboard projection
├── cloud_push.py           # read model -> pushSync (HMAC signing + URL safety validation)
├── cloud_sync_runner.py    # orchestration of build + push
├── cloud_sync_diff.py      # read model vs SQLite consistency self-check
├── dashboard_projection.py # generates the projection files for the hn_dashboard_* collections
├── notifications.py        # SMTP alerts + outbox fallback + atomic cooldown CAS
├── test_api.py             # contract + behavior unit tests
├── launcher.sh             # Linux/systemd startup script (only runs db init + ingest; on Windows use `python -m server.ingest` directly)
├── data/                   # SQLite + state cache + .cloud-sync-output/
└── README.md
```

## Running

```bash
# Production uses constraints for reproducibility; for local debugging, installing with -r alone is fine
pip install -r server/requirements.txt -c server/constraints.txt

# 1. Initialize the SQLite schema
python -c "from server import db; db.init_db()"

# 2. Long-running ingest supervisor; every finalize calls cloud sync to push to the cloud DB
python -m server.ingest --loop --interval-seconds 1800
```

After startup:

- The VPS has no HTTP listener: the read model is pushed via HMAC POST to
  `HNREADER_CLOUD_PUSH_URL` (HTTPS only; loopback / private-network /
  169.254.169.254 and similar internal addresses are rejected).
- Business reads: mini-program -> cloud function -> cloud-dev database
  `stories` / `topics` / `digests`.
- Ops dashboard: reads `hn_dashboard_summary` / `hn_dashboard_ingest_runs` /
  `hn_dashboard_cloud_sync_runs` from the cloud-dev database. How exactly it is
  displayed (cloud-dev console / cloud function / custom frontend) is left to
  the ops side to decide; the server is not involved.

### SQLite deployment notes

At the current scale SQLite is sufficient, but deployment must avoid
artificially creating multiple writers:

- Start only one `python -m server.ingest --loop` data pipeline.
- Do not put `HNREADER_DB_PATH` on a network drive, SMB/NFS, or an
  OneDrive/Dropbox sync directory; use a local disk directory.
- Do not let multiple machines share the same SQLite file; for multi-machine
  deployments, migrate to Postgres, or at least centralize writes into a
  single-machine ingest.

`server.ingest --loop` and a manual non-child `--once` both hold a `*.lock`
file lock in the DB's directory; if a second ingest supervisor is started by
mistake, it exits immediately, preventing two pipelines from writing the same
SQLite at once.

You can also use the launcher script in the repo directly (Linux = systemd,
Windows = background process). First edit the variables in the config section
at the top of the script (`PROJECT_DIR`, `APP_USER`, AI key, SMTP, etc.), or
write local overrides to `<project>/.env.local` and `server/.env.local` (the
latter loads later and can override the former), then run:

```bash
# Linux / macOS: writes /etc/hnreader/server.env + the db-init/ingest systemd units, and starts all services
bash server/launcher.sh start
bash server/launcher.sh restart    # restart; rewrites env/unit
bash server/launcher.sh stop
bash server/launcher.sh status
bash server/launcher.sh logs
bash server/launcher.sh doctor
bash server/launcher.sh repair    # repairs unmigrated schema / crash-leftover running records
bash server/launcher.sh ai-check --no-probe
bash server/launcher.sh metrics
bash server/launcher.sh reset-failed
```

For Linux production deployment, prefer placing the repo at `/opt/hnreader`
or `/srv/hnreader`. If it must live under `/home/*`, beyond the
`ProtectHome=read-only` generated by the launcher, you must also ensure the
`hnreader` system user can traverse/read `/home/<user>` (e.g. via ACL, group
permissions, or `chmod o+rx`); the DB directory is chowned to
`hnreader:hnreader` by the launcher.

In sync-only mode, `launcher.sh` only installs the `db-init` + `ingest`
systemd units, with no API/dashboard service; if an older version installed
`hnreader-api.service`, the launcher automatically disables and removes it
during install, preventing a leftover HTTP listener.

`launcher.sh` enables RealAiAgent by default and writes a DeepSeek placeholder
config containing `REPLACE_WITH`; before a production install you must replace
`HNREADER_AI_CONFIGS`, or explicitly set `HNREADER_AI_PROVIDER=none` to use the
fallback.

Windows is for development/debugging only, with no official launcher.bat. Just
run ingest in the foreground:

```cmd
set HNREADER_DB_PATH=server\data\hnreader.db
:: Other HNREADER_* configs are the same as .env.example; you can write them to .env.local and set / source them yourself
python -m server.ingest --loop --interval-seconds 1800
```

## Data pipeline CLI

```bash
# Single round: fetch -> enrich -> digest -> publish -> cleanup
python -m server.ingest --once

# Fetch only: writes only staging candidates, not published to clients
python -m server.ingest --fetch

# Enrich only (consume the pending queue)
python -m server.ingest --enrich

# Generate today's digest only
python -m server.ingest --digest

# Cleanup only
python -m server.ingest --cleanup

# Long-running (default one round every 30 minutes)
python -m server.ingest --loop --interval-seconds 1800

# Maintenance: reset failed back to pending (use after a prompt or model upgrade)
python -m server.ingest --reset-failed
```

The actual per-round order of `--once` / `--loop`:

```text
fetch    -> write stories raw fields + ranking_candidates (staging, not visible to clients)
enrich   -> process this round's candidates; after each worker chunk completes,
            incrementally publish the done candidates into rankings (incrementally
            visible, catalog_version +1 accordingly)
publish  -> after enrich ends, do one more full publish; in partial mode
            preserve_existing=True, keeping leftovers from the previous round; in
            success mode, fully replace rankings with this round's candidates
digest   -> generate/update today's digest based on the published done stories
            (auto mode has its own first/incremental/timer thresholds)
cleanup  -> clean up expired stories / comments / old digest / ingest_runs
finalize -> two-phase cloud sync; failure does not block the main flow:
            (A) cloud_sync_runner.run_business_once
                = cloud_sync.build_read_model(include_dashboard=False)
                + cloud_push.push_read_model(ping -> writeBatch -> switchMeta)
                the business terminal state lands in the cloud_sync_runs table
                first, then proceeds to (B)
            (B) cloud_sync_runner.run_dashboard_once (only runs when business is ok)
                = dashboard_projection.build_dashboard_projection
                + cloud_push.push_dashboard(writeDashboard)
                failure/timeout does not roll back the business publish; it only
                downgrades the local cloud_sync_runs from ok to warning, and the
                next round's dashboard projection catches up
            local progress is written to cloud_sync_runs first; after the
            dashboard push succeeds it is synced into the
            hn_dashboard_cloud_sync_runs collection
```

`ranking_candidates` is always invisible to clients; clients only see done
stories already published into `rankings`. `ingest_runs.status` has five
terminal states:

- `completed`: all candidates in this round finished enrich, and publish
  succeeded with visible results.
- `partial`: enrich timed out or there are still unfinished candidates, but
  the done portion has ready_count > 0 -- the finished portion is published
  into rankings as usual, unfinished candidates are discarded, and on the
  next fetch they are re-ingested if they still appear in the HN feed.
- `timeout`: enrich timed out and ready_count == 0.
- `failed`: fetch produced no publishable candidates, or visible stories are
  still 0 after publish.
- `discarded`: when the supervisor stops or repair starts, a non-expired
  `running` round left over from a previous process is found; the system
  releases/cleans up this round's staging state and keeps the history
  records already written to a terminal state.

In partial mode `delete_pending_orphans=False`, so the stories rows of
unfinished candidates are kept for reuse in the next round; once they
continue to appear in the feed in the future, the fetch phase goes straight
down the "already exists" path and does not re-fetch the item.

The Enricher processes in batches with a per-wave cap of
`HNREADER_ENRICH_WORKER_COUNT * HNREADER_ENRICH_SESSION_STORY_LIMIT`. The
default is 8 workers, each claiming at most 16 per wave. `RealAiAgent` sends
the stories claimed by one worker as a single batch to the AI and requires a
structured JSON array aligned by id in return; Agents that do not support
batch continue along the per-story `process_story()` compatibility path.

The daily digest's `story_ids` are selected by the AI Agent from the
candidate done stories. The program only validates that the ids returned by
the AI come from the candidates, are deduplicated, and do not exceed
`HNREADER_DIGEST_MAX_STORIES`; a selection failure makes this round's digest
fail and triggers an alert, and no longer silently falls back to rule-based
selection.

The timeout budget cannot be estimated by `ceil(M / (worker_count *
session_story_limit))` alone; that only covers the number of AI story
processing waves. A full round's elapsed time also includes HN feed fetch,
item/comment fetch, per-batch AI call time, digest generation, publish, and
cleanup. Conservative estimate:

```text
ceil(M / (HNREADER_ENRICH_WORKER_COUNT * HNREADER_ENRICH_SESSION_STORY_LIMIT))
  * avg_ai_seconds_per_batch
+ fetch_seconds
+ digest_publish_cleanup_seconds
+ cloud_sync_seconds (HMAC signing + a few HTTP calls, usually a few seconds)
```

Under the default feed window, M can theoretically approach 600; if the
average batch time is very high, `HNREADER_INGEST_ROUND_TIMEOUT_SECONDS=1680`
is still tight, and you need to reduce the window or increase the timeout.

`--loop` is the supervisor: each round it starts a child process to run
`--once`. After exceeding `HNREADER_INGEST_ROUND_TIMEOUT_SECONDS` it
terminates/kills the child, releases in-flight enrich tasks in the DB,
discards this round's candidates, and notifies the admin by email. The
supervisor holds a `*.lock` file lock in the DB's directory, preventing
multiple ingest loops from being started repeatedly.

## Cloud sync details

`server/cloud_sync.py` + `server/cloud_push.py` + `server/cloud_sync_runner.py` +
`server/dashboard_projection.py` are responsible for incrementally syncing
the latest content (after ingest writes to SQLite) to the WeChat cloud
database; the mini-program reads it through cloud functions, and the ops
dashboard reads the `hn_dashboard_*` collections directly. After each ingest
round's finalize completes, `ingest._trigger_and_record_cloud_sync`
orchestrates the two-stage push (failure does not block the main flow):

- Phase A -- `cloud_sync_runner.run_business_once`:
  `cloud_sync.build_read_model(include_dashboard=False)` writes
  `stories.jsonl` / `topics.jsonl` / `digests.jsonl` / `meta.json`, then
  `cloud_push.push_read_model` publishes the business collections in the
  order `ping -> writeBatch -> switchMeta -> cleanupOld`. After this stage
  succeeds, this round's `cloud_sync_runs` immediately lands a terminal
  state (`ok`/`failed`) first, ensuring the dashboard projection does not
  read this round as `running`.
- Phase B -- `cloud_sync_runner.run_dashboard_once` (only when Phase A is `ok`):
  `dashboard_projection.build_dashboard_projection` reads this round's ok
  row + actively `refresh_ai_config_status`, then writes the dashboard trio,
  and `cloud_push.push_dashboard` pushes to the dashboard collections via a
  separate `writeDashboard` action. On failure it does not roll back the
  business publish; the local `cloud_sync_runs` is downgraded from `ok` to
  `warning`; the cloud dashboard still retains the last successfully written
  state, and the next successful dashboard projection automatically catches up.

Enablement conditions:

```bash
HNREADER_CLOUD_SYNC_ENABLED=1
HNREADER_CLOUD_PUSH_URL=https://<env>.tcloudbase.com/push-sync
HNREADER_CLOUD_PUSH_SECRET=<64-char hex>
# Optional:
# HNREADER_CLOUD_PUSH_BATCH_SIZE=50
# HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS=120
```

`HNREADER_CLOUD_PUSH_SECRET` must match the `pushSync` cloud function's
environment variable `PUSH_SECRET`. Generate it with:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

`HNREADER_CLOUD_PUSH_URL` is strictly validated in
`cloud_push.validate_cloud_push_url`: it must be `https://`, must not
contain userinfo / fragment, and must not resolve to a loopback / RFC1918 /
link-local / cloud metadata address. `urlopen_no_redirect` raises any 3xx
directly as an `HTTPError`, preventing the push from being redirected to an
attacker-controlled internal/metadata endpoint.

Each actually started sync lands in the local `cloud_sync_runs` table first
(`run_id` is associated with ingest_runs); only after the dashboard
projection is successfully pushed will the cloud
`hn_dashboard_cloud_sync_runs` see summaries of these local records. A Phase
A failure writes a local `failed` and retries after the next ingest round
completes; a dashboard failure downgrades the local record to `warning`, and
the cloud catches up with the next successful projection.

If `HNREADER_CLOUD_SYNC_ENABLED=false` or this round's remaining budget is
insufficient, `_trigger_and_record_cloud_sync` returns `skipped` /
`deferred` directly and does not write a `cloud_sync_runs` row. When a push
actually starts, ingest first writes a `status='running'` record
(pre-written by `_trigger_and_record_cloud_sync`), then later UPDATEs it to
one of the following in-table states:

- `ok` -- Phase A + Phase B both succeeded. Phase A lands `ok` first, and
  when Phase B completes it UPDATEs once more so that
  `elapsed_seconds`/`finished_at` cover the total time of both stages.
- `failed` -- Phase A failed (missing config, build threw an exception, push
  4xx/5xx) or a runner-level exception was caught by the fallback handler.
- `warning` -- Phase A `ok` but the local `cloud_sync_runs` UPDATE failed
  (`record_ok=False`), or Phase B failed / was skipped over budget / threw
  an exception. Downgrading to warning triggers an alert, avoiding a "pushed
  but not recorded locally" situation that would skew `previousVersion`
  inference; the next round's dashboard projection still reads this round's
  terminal state, so the cloud dashboard does not lag forever.

`running` only means a push has started but not yet wrapped up; if the
process crashes, it may remain as an abnormal leftover row in the local
table. `skipped` / `deferred` are in-memory states of this round's ingest
summary / alert, are not written to `cloud_sync_runs`, and the cloud
dashboard does not see these two transient states either.

`meta.previousVersion` comes from the `sync_version` of the last
`status='ok'` in `cloud_sync_runs`, not `currentVersion - 1`. This way,
even if there were multiple push failures / skips in between, the cutoff
used by cloud cleanup still points to a version that actually exists.

Manual troubleshooting:

```bash
python -m server.cloud_sync       # build read model + dashboard projection into data/.cloud-sync-output/
python -m server.cloud_push       # push data/.cloud-sync-output/ to the cloud function
python -m server.cloud_sync_diff  # compare the local read model with SQLite for consistency
```

> The output directory defaults to the same directory as the SQLite DB
> (the systemd unit's `ReadWritePaths`), and can be overridden via
> `HNREADER_CLOUD_SYNC_OUTPUT_DIR`.

## Configuration

Override the defaults in `server/settings.py` via environment variables:

| Variable | Default | Description |
|---|---|---|
| `HNREADER_DB_PATH` | `server/data/hnreader.db` | SQLite file location |
| `HNREADER_LOG_DIR` | `server/logs` | local daily file-log directory |
| `HNREADER_CLOUD_SYNC_ENABLED` | _empty_ | when enabled, ingest finalize triggers the two-stage cloud sync (`run_business_once` + `run_dashboard_once`), pushing the read model and dashboard projection separately to the cloud database |
| `HNREADER_CLOUD_PUSH_URL` | _empty_ | HTTP trigger URL of the pushSync cloud function (must be https; internal/metadata addresses are rejected) |
| `HNREADER_CLOUD_PUSH_SECRET` | _empty_ | HMAC shared secret (matches the cloud function's `PUSH_SECRET`) |
| `HNREADER_CLOUD_PUSH_BATCH_SIZE` | `50` | stories per batch |
| `HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS` | `120` | timeout (seconds) for a single push HTTP call |
| `HNREADER_CLOUD_SYNC_OUTPUT_DIR` | `.cloud-sync-output` in the DB directory | output directory for the read model + dashboard projection JSON generated by cloud_sync |
| `HNREADER_FEED_WINDOW_SIZE` | `100` | visible window size per feed |
| `HNREADER_STORY_STORE_MAX_ROWS` | `1000` | stories backing store hard cap; visible/digest/in-flight rows are protected |
| `HNREADER_COMMENT_FETCH_LIMIT` | `60` | max comments fetched per story |
| `HNREADER_COMMENT_MAX_DEPTH` | `2` | max comment-tree depth |
| `HNREADER_COMMENT_MIN_DESCENDANTS` | `5` | below this, comments are not fetched |
| `HNREADER_ENRICH_STALE_SECONDS` | `600` | enriching stuck timeout |
| `HNREADER_ENRICH_MAX_ATTEMPTS` | `3` | AI retry cap |
| `HNREADER_ENRICH_WORKER_COUNT` | `8` | number of concurrent AI workers |
| `HNREADER_ENRICH_SESSION_STORY_LIMIT` | `16` | max stories processed per worker per wave |
| `HNREADER_ENRICH_BATCH_SIZE` | `3` | stories batched per AI request; not recommended above 3 under DeepSeek's 8192 output cap |
| `HNREADER_INGEST_INTERVAL_SECONDS` | `1800` | supervisor default polling interval |
| `HNREADER_INGEST_ROUND_TIMEOUT_SECONDS` | `1680` | max execution time per round |
| `HNREADER_INGEST_DIGEST_RESERVED_SECONDS` | `90` | time reserved after enrich for digest/publish/cleanup/cloud sync |
| `HNREADER_INGEST_CHILD_KILL_GRACE_SECONDS` | `10` | seconds to wait for kill after child terminate |
| `HNREADER_DIGEST_TIMEZONE` | `Asia/Shanghai` | digest date timezone |
| `HNREADER_DIGEST_UPDATE_INTERVAL_SECONDS` | `1800` | minimum interval for digest updates |
| `HNREADER_DIGEST_MIN_NEW_DONE_STORIES` | `3` | minimum new done count that triggers a digest refresh |
| `HNREADER_DIGEST_MAX_STORIES` | `7` | max stories included in the digest |
| `HNREADER_INGEST_RUN_RETENTION_DAYS` | `30` | retention for old `ingest_runs` history |
| `HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS` | `30` | retention for finished `cloud_sync_runs` history; `running` rows are preserved |
| `HNREADER_TOPIC_RETENTION_DAYS` | `30` | retention for old unused dynamic topics |
| `HNREADER_RANKING_GRACE_SECONDS` | `86400` | retention grace for out-of-window stories |
| `HNREADER_CLEANUP_STALE_GUARD_SECONDS` | `43200` | fetch-heartbeat aging threshold |
| `HNREADER_HN_API_BASE` | `https://hacker-news.firebaseio.com/v0` | HN API root |
| `HNREADER_HN_REQUEST_TIMEOUT_SECONDS` | `10` | HN request timeout |
| `HNREADER_HN_RETRY_ATTEMPTS` | `3` | retries per request |
| `HNREADER_AI_PROVIDER` | `none` | `none`=Fallback; anything else=RealAiAgent |
| `HNREADER_AI_CONFIGS` | _empty_ | JSON array; when set, replaces the single `AI_API_KEY/MODEL/BASE_URL` group, fails over in order, and can configure a concurrency cap and dashboard balance-probe URL per provider |
| `HNREADER_AI_API_KEY` | _empty_ | OpenAI-compatible API key |
| `HNREADER_AI_MODEL` | _empty_ | model name (e.g. `gpt-4o-mini`) |
| `HNREADER_AI_BASE_URL` | `https://api.openai.com/v1` | any OpenAI-compatible endpoint; non-loopback must use https, and pointing at RFC1918 / link-local / cloud metadata and similar internal addresses is forbidden |
| `HNREADER_AI_INTERNAL_HOST_ALLOWLIST` | _empty_ | comma-separated hostname list; when matched, bypasses the internal-network/cleartext-HTTP check; use only when the provider is genuinely behind an internal-network proxy |
| `HNREADER_AI_REQUEST_TIMEOUT_SECONDS` | `60` | LLM request timeout |
| `HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS` | `60` | dashboard AI provider status cache TTL |
| `HNREADER_AI_CONFIG_STATUS_CACHE_PATH` | `ai-config-status-cache.json` in the DB directory | last-known on-disk cache of the dashboard AI status, used as a fallback when refresh fails |
| `HNREADER_AI_CONFIG_FILE` | env file generated by `launcher.sh` | hot-loads AI-related config at runtime; can point at an env file or a standalone `ai-config.json`, taking effect on the next AI Agent build |
| `HNREADER_ADMIN_EMAIL_ENABLED` | `false` | whether to send admin alert emails |
| `HNREADER_ADMIN_EMAIL_TO` | _empty_ | admin email address |
| `HNREADER_SMTP_HOST` | _empty_ | SMTP host |
| `HNREADER_SMTP_PORT` | `587` | SMTP port |
| `HNREADER_SMTP_USERNAME` | _empty_ | SMTP username |
| `HNREADER_SMTP_PASSWORD` | _empty_ | SMTP password |
| `HNREADER_SMTP_FROM` | _empty_ | sender; if blank, the SMTP username is used |
| `HNREADER_SMTP_STARTTLS` | `true` | whether to enable STARTTLS |
| `HNREADER_SMTP_SSL` | `false` | whether to use SMTP_SSL (commonly for port 465); when enabled, STARTTLS is not called |
| `HNREADER_ALERT_COOLDOWN_SECONDS` | `1800` | rate-limit window for same-type alerts (atomic CAS claim) |
| `HNREADER_ALERT_OUTBOX_PATH` | `alerts.jsonl` in the DB directory | local alert fallback queue when SMTP is unavailable |
| `HNREADER_ALERT_OUTBOX_MAX_RECORDS` | `1000` | max pending local alert records retained |

> It runs end-to-end without any LLM config -- `FallbackAiAgent` writes
> fallback fields, marks the story `done`, and the frontend still gets the
> full contract (`titleZh=titleEn`, `aiSummary=""`, `discussionThemes=[]`,
> `insights=[]`, `terms=[]`, `topic="web"`).

Use `HNREADER_AI_CONFIGS` for multiple OpenAI-compatible configs:

```bash
HNREADER_AI_PROVIDER=openai
HNREADER_AI_CONFIGS='[
  {"name":"OpenAI","api_key":"sk-...","model":"gpt-4o-mini","base_url":"https://api.openai.com/v1","timeout_seconds":60,"input_token_price_per_million":0.15,"output_token_price_per_million":0.60},
  {"name":"DeepSeek","api_key":"sk-...","model":"deepseek-v4-flash","base_url":"https://api.deepseek.com","balance_url":"https://api.deepseek.com/user/balance","timeout_seconds":60,"max_concurrent_requests":1,"token_price_per_million":0.30}
]'
```

Once `HNREADER_AI_CONFIGS` is set, it replaces the single `HNREADER_AI_API_KEY/MODEL/BASE_URL` group. At execution time it tries the array in order; connection/HTTP and other provider errors automatically switch to the next group; a model processing timeout or invalid returned content does not switch the key, but is redacted and enters the existing `enrich_error` / retry flow. The API key is never written to logs, exceptions, or alert fields; `base_url` may not carry a username, password, query, or fragment; non-loopback hosts must be `https://`, and RFC1918 / link-local / cloud metadata and similar internal addresses are rejected by default (if genuinely needed, add `HNREADER_AI_INTERNAL_HOST_ALLOWLIST`).

AI config supports hot-reload at runtime: `build_ai_agent()` / the dashboard AI config probe re-read `HNREADER_AI_CONFIG_FILE` before use. When deploying with `launcher.sh`, after editing `.env.local` or the script config just run `bash server/launcher.sh env` to rewrite the env file; you do not need to redeploy the code, and the next enrichment round uses the new model. Before writing, `launcher.sh env` runs a Python-level AI config parse validation; you can also manually run `bash server/launcher.sh ai-check --no-probe` to only parse the config, or `bash server/launcher.sh ai-check` to connectivity-probe the provider. When deleting a value, keep an empty assignment in the env file (e.g. `HNREADER_AI_CONFIGS=`); do not delete the whole line.

If you do not want to maintain a one-line JSON env, you can change `.env.local` to point only at a standalone file:

```bash
HNREADER_AI_CONFIG_FILE=/etc/hnreader/ai-config.json
```

`ai-config.json` can directly contain a provider array, or an object. The object form is convenient for also holding global options:

```json
{
  "provider": "enabled",
  "request_timeout_seconds": 60,
  "configs": [
    {
      "name": "DeepSeek",
      "api_key": "sk-...",
      "model": "deepseek-v4-flash",
      "base_url": "https://api.deepseek.com",
      "balance_url": "https://api.deepseek.com/user/balance",
      "timeout_seconds": 120,
      "max_concurrent_requests": 1,
      "max_output_tokens": 8000
    }
  ]
}
```

After migrating, first run `bash server/launcher.sh ai-check --no-probe` and confirm that the `AI source` in the output is `json:/etc/hnreader/ai-config.json`, then run `bash server/launcher.sh env` or `restart`.

The optional field `name` is used for dashboard display; `balance_url` is used by the dashboard for a real-time GET probe of balance and connectivity, and when unset it falls back to `base_url + "/models"` for a lightweight connectivity check. DeepSeek's balance endpoint is `https://api.deepseek.com/user/balance`.

The optional field `max_concurrent_requests` limits the number of concurrent requests to the same `base_url` provider. DeepSeek's official rate limiting is primarily dynamic concurrency control and does not publish a fixed RPM/TPM; if you see HTTP 429, it is recommended to first configure `"max_concurrent_requests":1` for `https://api.deepseek.com`, then gradually raise it to 2 or higher once stable.

## Dashboard data contract (cloud)

On each round cloud_sync writes the following documents to
`.cloud-sync-output/`, which `cloud_push` HMAC POSTs to the pushSync cloud
function; the cloud function upserts them into the corresponding
collection by `_id`:

- `dashboard_summary.json` -> collection `hn_dashboard_summary`, a single document `_id="summary"`
  - `syncVersion` / `publishedAt` / `serverTime` / `appVersion`
  - `metrics` (pipeline counts, from `repository.get_pipeline_metrics`)
  - `latestRun` / `latestCloudSync`
  - `ai` (AI provider status, after redaction by `_safe_ai_status_for_dashboard`)
  - `recentIngestRunsCount` / `recentCloudSyncRunsCount`
- `dashboard_ingest_runs.jsonl` -> collection `hn_dashboard_ingest_runs`,
  `_id="{syncVersion}:{run_id}"`, most recent 100
- `dashboard_cloud_sync_runs.jsonl` -> collection `hn_dashboard_cloud_sync_runs`,
  `_id="{syncVersion}:{run_id}:{started_at}"`, most recent 100

The cloud only holds display-only summaries -- `dashboard_projection.py`
does not write full error stack traces, API keys, article bodies, etc.
The `error` field is truncated to 200 characters, and `has_error` is a
boolean.

For the full field conventions, see `row_to_run_summary` /
`_cloud_sync_row_summary` and `build_dashboard_summary` in
`dashboard_projection.py`.

## Field constraints

In `Story`, **all fields must exist**; on missing / failure, use:

- strings: `""`
- arrays: `[]` (`discussionThemes` / `insights` / `terms`)

Keys cannot be omitted -- frontend templates (mini-program wxml, etc.) do
not tolerate `undefined`. The `topic` field is always non-empty,
defaulting to `general` (display name "Comprehensive Tech"); clients can
trust that `topic` appears in the list returned by the `topics`
collection -- the topic table is auto-upserted by the `topicName`
returned by the AI when enrich writes, and the cloud list only lists
active topics that have done stories, up to at most
`HNREADER_TOPIC_MAX_ACTIVE_TOPICS`.

## catalogVersion semantics

`catalogVersion` is a monotonically increasing integer (stored as a string in `meta.catalog_version`):

- +1 when enrich-time incremental publish writes a new done story into
  `rankings` (every worker chunk completion may trigger one)
- +1 when the final publish after enrich changes the visible_ranking_hash
  of any feed
- +1 when enrich fails into the `failed` terminal state and the written
  fallback fields differ from the current state (only when visible fields
  actually change; a fallback write-back with no change does not bump;
  see `repository.mark_enrich_failed`)
- +1 when the day's digest (intro / story_ids) changes
- **unchanged** for staging fetch / cleanup / fetch with no change /
  single-story score / descendants jitter / when `mark_enrich_failed`
  fallback fields are unchanged

The frontend can use this to precisely invalidate its local cache, and will not refresh frequently due to HN score jitter.

## Testing

```bash
# project root directory
python -m unittest server.test_api
```

Coverage: empty-database contract, kind inference, normalizer skipping
poll/comment/deleted, two-phase publish, enrich success/failure/stuck
recovery, digester incremental update, cleanup grace and grace period,
pipeline metrics, error response shape, cloud push URL safety, dashboard
projection file contract, cloud push payload containing dashboard, atomic
alert cooldown CAS, absence of `server.main`/`server.auth`.

## Local end-to-end verification

```bash
# 1. Run one ingest round (local SQLite; triggers a cloud sync push)
python -m server.ingest --once -v

# 2. View the most recent 5 cloud_sync_runs push results
sqlite3 server/data/hnreader.db \
  "SELECT run_id, status, sync_version, stories FROM cloud_sync_runs ORDER BY started_at DESC LIMIT 5;"

# 3. View the dashboard projection files (will be pushed to the cloud DB)
ls server/data/.cloud-sync-output/
cat server/data/.cloud-sync-output/dashboard_summary.json | jq .
```

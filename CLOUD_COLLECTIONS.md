# Cloud-Dev Database Collection Configuration

Under the sync-only architecture, all persisted data either lives in the server's SQLite
(the local source of truth) or is pushed to the cloud-dev database through
`cloudfunctions/pushSync`. This document lists **all** cloud-dev collections: their names,
permissions, indexes, who reads/writes each, and how to create them manually on the first
deployment.

> Actual behavior is governed by the `cloudfunctions/*` and `server/cloud_push.py` /
> `server/dashboard_projection.py` code; this document is kept in sync with the code.

---

## 1. Collection Overview

| # | Collection | Category | Permission | Writer | Reader |
|---|---|---|---|---|---|
| 1 | `stories`                       | Business read | Creator read/write only | `pushSync.writeBatch`                     | `stories` / `story` / `topicStories` cloud functions |
| 2 | `topics`                        | Business read | Creator read/write only | `pushSync.writeBatch`                     | `topics` / `topicStories` cloud functions |
| 3 | `digests`                       | Business read | Creator read/write only | `pushSync.writeBatch`                     | `digest` cloud function |
| 4 | `meta`                          | Metadata      | Creator read/write only | `pushSync.switchMeta`                     | `health` / `stories` / `story` / `topics` / `topicStories` / `digest` cloud functions |
| 5 | `push_log`                      | Ops audit     | Creator read/write only | `pushSync` (one row per call); `cleanupPushLog` periodically deletes old logs | Ops (viewed directly in the cloud-dev console) |
| 6 | `hn_dashboard_summary`          | Ops dashboard | Creator read/write only | `pushSync.writeDashboard`                 | Ops / a future `readDashboard` cloud function |
| 7 | `hn_dashboard_ingest_runs`      | Ops dashboard | Creator read/write only | `pushSync.writeDashboard`                 | Same as above |
| 8 | `hn_dashboard_cloud_sync_runs`  | Ops dashboard | Creator read/write only | `pushSync.writeDashboard`                 | Same as above |

All eight collections use **"Creator read/write only"** (a WeChat cloud-dev console
permission). The rationale is given in §4.

---

## 2. Business-Read Collections (Final Consumption by the Mini Program Client)

### 2.1 `stories`

Each document corresponds to one enriched HN story. The `_id` carries a version prefix;
each cloud sync round writes a new version, and `cleanupOld` removes old versions where
`syncVersion < cutoff`.

- `_id`: `"${syncVersion}:${storyId}"`, e.g. `"12:38712345"`
- Key fields: `id` / `syncVersion` / `titleZh` / `titleEn` / `aiSummary` /
  `discussionThemes` / `insights` / `terms` / `feedRanks` / `topic` /
  `defaultType` / `inAnyRanking` (the full schema is in `_story_to_doc`
  inside `server/cloud_sync.py`)
- Write constraints (validated by `pushSync.writeBatch`):
  - `_id` must start with `${syncVersion}:`
  - `titleZh` must be non-empty, contain CJK, and be `!=` `titleEn`
  - `aiSummary` must be non-empty and contain CJK

> Stories that did not pass AI enrichment never enter this collection -- the client
> only ever sees the Chinese version.

### 2.2 `topics`

Versioned mapping of the dynamic topic categories.

- `_id`: `"${syncVersion}:${topicId}"`, e.g. `"12:ai"`
- Fields: `id` / `name` (Chinese display name) / `count` / `syncVersion`

### 2.3 `digests`

The daily picks. The `_id` carries a version prefix and, just like stories/topics, its
visibility is controlled by `meta.currentVersion`. This way, if `writeBatch` has already
written but `switchMeta` fails, the mini program will not prematurely read a
half-published digest.

- `_id`: `"${syncVersion}:YYYY-MM-DD"`, e.g. `"12:2026-05-12"`
- Fields: `syncVersion` / `date` / `intro` / `stories[]` (each element is an inline
  summary produced by `_client_story_dict_from_story`)

### 2.4 `meta`

A single document with `_id="catalog"` that records the currently published version.
`switchMeta` is the atomic commit point of a sync-only release -- until this document is
overwritten, the new version that the preceding `writeBatch` wrote is invisible to the
client (the client always reads `meta.catalog.currentVersion` first).

- `_id`: `"catalog"` (there is always exactly this one row)
- Fields:
  - `currentVersion`: the currently published version number (int)
  - `previousVersion`: the version number of the last successful push; derived on the
    server side by `cloud_sync.build_read_model` reading the most recent `ok`
    `sync_version` from `cloud_sync_runs`, **not** `currentVersion - 1` (to avoid
    referencing a version that does not exist in the cloud)
  - `feedCounts`: `{top, new, best, ask, show, job}` -- the visible story count for each feed
  - `publishedAt`: the time `switchMeta` wrote (epoch seconds)

---

## 3. Ops/Audit and Dashboard Collections

### 3.1 `push_log`

`cloudfunctions/pushSync` writes one row to `push_log` at the start and end of **every**
call (ping / writeBatch / switchMeta / cleanupOld), whether it succeeds or fails.

- No fixed `_id` (auto-generated)
- Fields: `action` / `ts` / `statusCode` / `ok` / `syncVersion` / `counts` /
  `durationMs` / `signatureOk` / `ip` / `error`
- Capacity: the `cleanupPushLog` scheduled cloud function cleans up rows where
  `ts < now - PUSH_LOG_RETENTION_DAYS` every day; the default retention is 30 days,
  with a minimum of 1 day.

Signature-verification failures, out-of-window `ts`, nonce replays, and unknown actions
all land in `push_log`; this is the first collection ops checks when troubleshooting a
push anomaly.

### 3.2 `hn_dashboard_summary`

The "home page" data of the entire sync-only dashboard. A **single document** that is
overwritten in place on every cloud sync.

- `_id`: `"summary"` (there is always exactly this one row)
- Source: `server/dashboard_projection.py::build_dashboard_summary`
- Fields:
  - `syncVersion` / `publishedAt` / `serverTime` / `appVersion`
  - `metrics`: the output of `repository.get_pipeline_metrics(conn)`
    (`enrich_status_counts` / `failure_rate` / `last_full_fetch_at` /
    `last_refresh_at` / `latest_digest` / `latest_run` / `catalog_version`
    / `total_stories`)
  - `latestRun`: a dashboard-friendly summary of the most recent `ingest_runs`; a
    `running` run that has exceeded its deadline is marked `stale`
  - `latestCloudSync`: a redacted summary of the most recent `cloud_sync_runs`
  - `ai`: AI provider configuration + probe status (`status` / `configs[]` /
    `balance`, etc.; does not include the API key)
  - `recentIngestRunsCount` / `recentCloudSyncRunsCount`: counts that align with
    the number of documents in the current batch of the two collections below

### 3.3 `hn_dashboard_ingest_runs`

The most recent N `ingest_runs` history entries (default 100, controlled by
`dashboard_projection.DEFAULT_RECENT_INGEST_RUNS`).

- `_id`: `"${syncVersion}:${run_id}"`, e.g. `"12:run-abc-1700000000"`
- Fields (redacted ingest_runs rows):
  - `run_id` / `started_at` / `deadline_at` / `finished_at`
  - `status` (an expired `running` will be marked `stale`)
  - `raw_status` (the underlying real status, which the cloud can display alongside it)
  - `stale` / `overdue_seconds`
  - `phase` / `raw_count` / `candidate_count` / `claimed` / `done`
    / `failed` / `retried`
  - `has_error` (boolean; **the full stack trace never enters the collection** -- it
    stays in the server logs)
  - `ai_usage` (`{total_tokens, cost, by_model[]}`)
- Old versions are cleaned up by `pushSync.cleanupOld` where `syncVersion < cutoff`

### 3.4 `hn_dashboard_cloud_sync_runs`

The most recent N `cloud_sync_runs`, i.e. the push's own execution history.

- `_id`: `"${syncVersion}:${run_id}:${started_at}"`, e.g.
  `"12:run-abc:1700000123"` (the same run may retry multiple times, so `started_at`
  is added)
- Fields:
  - `run_id` / `started_at` / `finished_at` / `status`
  - `sync_version` / `stories` / `topics` / `digests` (counts for this push)
  - `elapsed_seconds`
  - `has_error` / `error` (the first 200 characters)
- State machine:
  - `running` -> written as soon as the push starts (the push writes a `running`
    record before the push so the dashboard projection can see an in-flight push)
  - `ok`     -> all steps succeeded
  - `failed` -> any step failed
  - `deferred` -> insufficient ingest deadline remaining, so this round is skipped
  - `warning` -> push succeeded but the local table write failed (rare)
  - `skipped` -> `HNREADER_CLOUD_SYNC_ENABLED=false`

---

## 4. Why All Collections Use "Creator read/write only"

Cloud-dev database permissions come in four tiers. Their semantics target the **mini
program front-end users** (different OPENIDs) and do not affect cloud functions -- a
cloud function always runs with root permission and bypasses permission checks.

| Tier | Who can read | Who can write |
|---|---|---|
| Creator read/write only | The document creator's OPENID | Same as left |
| Creator write only, anyone can read | Any mini program user | The creator |
| Admin write only, anyone can read | Any mini program user | Only the cloud-dev console / cloud functions |
| All users can read/write | Any mini program user | Any mini program user |

In the current deployment, **no module needs the mini program front end to read the
cloud database directly**:

- `stories` / `topics` / `digests` / `meta` -> read through the `stories` / `story` /
  `topics` / `topicStories` / `digest` cloud functions
- dashboard -> no front end is implemented yet; ops views it directly through the
  cloud-dev console. Even when it is built later, it will first stand up a
  `readDashboard` cloud function rather than letting the front end hit the DB directly

So the strictest tier, "Creator read/write only", is chosen across the board: any mini
program user (including one crafting malicious requests) gets a direct 403, and only
cloud functions + console ops can access the data, minimizing the attack surface.

### When a Mini Program Admin Dashboard Is Built Later

Two options:

1. **Recommended**: stand up another `readDashboard` cloud function and have the front
   end call it via `wx.cloud.callFunction`, leaving the collection permissions
   untouched -- keep "Creator read/write only", let the cloud function read with root
   permission, and do authorization inside the cloud function (e.g. an OPENID
   allowlist).
2. **Secondary option**: change the permission of the three `hn_dashboard_*` collections
   to "Admin write only, anyone can read" and have the front end read directly with
   `wx.cloud.database().collection(...).get()`. This saves a cloud function but is
   equivalent to exposing the dashboard data to all mini program users. Do not choose
   this unless you accept that.

---

## 5. Recommended Indexes

| Collection | Field | Type | Purpose |
|---|---|---|---|
| `stories` | `syncVersion` | Normal, ascending | `cleanupOld` cleans up by version |
| `stories` | `id` | Normal, ascending | The `story` cloud function queries by storyId |
| `topics` | `syncVersion` | Normal, ascending | `cleanupOld` |
| `digests` | `syncVersion` | Normal, ascending | The `digest` cloud function queries by the current version; `cleanupOld` cleans up by version |
| `digests` | `date` | Normal, descending | Ops troubleshoots by date; the main query uses the versioned `_id` |
| `meta` | (not needed) | — | Only one row |
| `push_log` | `ts` | Normal, descending | Reverse-chronological troubleshooting |
| `push_log` | `action` | Normal, ascending | Filter by action (optional) |
| `hn_dashboard_summary` | (not needed) | — | Only one row |
| `hn_dashboard_ingest_runs` | `syncVersion` | Normal, ascending | `cleanupOld` |
| `hn_dashboard_ingest_runs` | `started_at` | Normal, descending | Dashboard reverse-chronological order |
| `hn_dashboard_cloud_sync_runs` | `syncVersion` | Normal, ascending | `cleanupOld` |
| `hn_dashboard_cloud_sync_runs` | `started_at` | Normal, descending | Dashboard reverse-chronological order |

> Indexes can be added later; each `hn_dashboard_*` collection holds at most 100
> documents, so even a full table scan is fast. The `syncVersion` index helps
> `cleanupOld` the most when cleaning up large batches of old-version data after
> long-running operation.

---

## 6. First Deployment: Cloud-Dev Console Steps

1. **Create the collections**. Open the WeChat Developer Tools -> Cloud Dev ->
   Database -> "+" New Collection, and fill them in one by one:
   - `stories`
   - `topics`
   - `digests`
   - `meta`
   - `push_log`
   - `hn_dashboard_summary`
   - `hn_dashboard_ingest_runs`
   - `hn_dashboard_cloud_sync_runs`

   Set the permission of every collection to **"Creator read/write only"** (this is the
   default tier).

2. **Create indexes** (optional, can be deferred). Click into each collection ->
   "Index Management" -> add them per the §5 table above.

3. **Initialize `meta.catalog`** (optional). `pushSync.switchMeta` has a built-in
   fallback `add({_id: "catalog", ...})`, so the first push creates it automatically;
   you do not need to add it manually. Adding it manually is also harmless (`set`
   overwrites it).

4. **Deploy the `pushSync` cloud function** and set `PUSH_SECRET=<HMAC key>` in the
   cloud function's environment variables, with a value matching the server-side
   `HNREADER_CLOUD_PUSH_SECRET`.

5. **Deploy the `cleanupPushLog` scheduled cloud function**. It runs at 04:00 every day
   by default and deletes logs where `push_log.ts < now - PUSH_LOG_RETENTION_DAYS`.
   Optional environment variables: `PUSH_LOG_RETENTION_DAYS=30`,
   `PUSH_LOG_MAX_DELETE=1000`. After deploying the function in the WeChat Developer
   Tools, you also need to right-click the cloud function and upload the trigger.

6. **Obtain the pushSync HTTP trigger URL** and set the server-side `.env.local`:

   ```
   HNREADER_CLOUD_SYNC_ENABLED=1
   HNREADER_CLOUD_PUSH_URL=https://<env-id>.service.tcloudbasegateway.com/pushSync
   HNREADER_CLOUD_PUSH_SECRET=<must match the cloud function's PUSH_SECRET>
   ```

   The URL must be `https://` and must not point to loopback / private networks /
   169.254.169.254, etc.; `cloud_push.validate_cloud_push_url` blocks any dangerous
   target.

7. **Trigger the first push**: run `python -m server.ingest --once` on the server
   machine; cloud sync is called automatically when ingest finalizes. Watch
   `journalctl -u hnreader-ingest` or the local terminal -- the appearance of
   `[cloud_sync] OK ...` indicates success.

---

## 7. Verifying the Collections Are Populated Correctly

After the first ingest round finishes, the cloud-dev console database should contain:

| Collection | What you should see |
|---|---|
| `stories` | At least a few dozen documents, with `_id` like `"1:38712345"` |
| `topics` | A few dynamic categories, with `_id` like `"1:ai"` / `"1:web"`, etc. |
| `digests` | One document for the current date, with `_id` like `"1:2026-05-12"` |
| `meta` | One `_id="catalog"`, with fields including `currentVersion=1` / `previousVersion=null` / `feedCounts` / `publishedAt` |
| `push_log` | 4 rows: one each for `ping`/`writeBatch`/`switchMeta`/`cleanupOld` (or more, if stories are split into batches) |
| `hn_dashboard_summary` | One `_id="summary"`, with fields including `metrics` / `latestRun` / `latestCloudSync` / `ai` |
| `hn_dashboard_ingest_runs` | One or more, with `_id="1:run-xxx"` |
| `hn_dashboard_cloud_sync_runs` | One or more, with `_id="1:run-xxx:1700000000"` (written after the server finishes pushing; *this* push's own record is in the `running` state) |

If a collection has no data, first check the most recent row's `statusCode` / `error`
fields in `push_log`; when `ok=false`, `error` contains the reason (signature error,
URL validation failure, invalid field, etc.).

---

## 8. Collection Data Volume and Cleanup Strategy

| Collection | Volume | Cleanup |
|---|---|---|
| `stories` | <= `FEED_WINDOW_SIZE x 6 = 600` rows (single version); with multiple versions = single version x retained version count, leaving only current + previous after cleanup | `pushSync.cleanupOld` deletes where `syncVersion < previousVersion` |
| `topics` | < 50 rows/version | Same as above |
| `digests` | <= retained date count x retained version count | `pushSync.cleanupOld` cleans old versions by `syncVersion`; the server-side `cleanup.py` decides which dates remain in this round's manifest by `DIGEST_RETENTION_DAYS` |
| `meta` | 1 row | Always 1 row |
| `push_log` | +4~10 rows per ingest round; in steady state retains the most recent `PUSH_LOG_RETENTION_DAYS` days | `cleanupPushLog` cleans up on a schedule at 04:00 daily; default 30 days, default at most 1000 rows deleted per round |
| `hn_dashboard_summary` | 1 row | Always 1 row |
| `hn_dashboard_ingest_runs` | <= 100 rows (`DEFAULT_RECENT_INGEST_RUNS`) / version | `pushSync.cleanupOld` deletes where `syncVersion < previousVersion` + `cleanupCurrentVersion` deletes by manifest |
| `hn_dashboard_cloud_sync_runs` | Same as above | Same as above |

`push_log` is the only collection that grows naturally; it is now maintained on a
schedule by `cleanupPushLog`. The other collections are still maintained by the
version cleanup built into the push flow.

---

## 9. Related Code Locations

- `server/cloud_sync.py::build_read_model`: produces `stories.jsonl` / `topics.jsonl`
  / `digests.jsonl` / `meta.json` + the three dashboard files
- `server/dashboard_projection.py`: produces `dashboard_summary.json` /
  `dashboard_ingest_runs.jsonl` / `dashboard_cloud_sync_runs.jsonl`
- `server/cloud_push.py::push_read_model`: signing + push + manifest orchestration
- `server/cloud_push.py::validate_cloud_push_url`: URL SSRF / cleartext HTTP validation
- `cloudfunctions/pushSync/index.js`: the receiving end, responsible for writing to the
  database by the `_id` version prefix + collection-wise cleanup
- `cloudfunctions/cleanupPushLog/index.js`: scheduled cleanup of old `push_log`
- `cloudfunctions/{stories,story,topics,topicStories,digest,health}/index.js`:
  the mini program read path, reading the business collections with root permission

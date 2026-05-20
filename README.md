# hnreader server

Ubuntu 22.04 deployment and daily operations are handled by `launcher.sh`.

## First Run

After cloning this server on Ubuntu 22.04:

```bash
cd /path/to/hackernews-server
bash launcher.sh
```

With no command, `launcher.sh` runs `bootstrap`. It installs Ubuntu packages,
creates `.venv`, installs Python dependencies, guides you through `.env.local`
configuration, writes systemd units, and starts the sync-only service.

Ingest and Insights try the local `codex` CLI first. That path uses the
systemd service user's existing Codex login/subscription and runs with
`codex --ask-for-approval never exec --sandbox read-only`, so it must not edit
local files. The launcher gives Codex a writable service-user home via
`HNREADER_CODEX_HOME` and checks `codex --version` as that user before writing
the units. If Codex fails at request time, the server falls back to the existing
OpenAI-compatible AI config path unchanged.

During the guided setup you will be asked for:

- One or more OpenAI-compatible fallback AI configs, or `none` to use offline
  fallback output. If Codex is enabled, setup still verifies that the service
  user can start the local CLI.
- Optional Codex overrides in `.env.local`: `HNREADER_CODEX_CLI_PATH`,
  `HNREADER_CODEX_HOME`, `HNREADER_CODEX_EXTRA_PATH`, `HNREADER_CODEX_MODEL`,
  and `HNREADER_CODEX_ENABLED=false` when you intentionally want fallback-only
  AI.
- WeChat `pushSync` HTTP trigger URL and `PUSH_SECRET`.
- Optional admin alert email settings.
- Optional UFW setup. It allows only OpenSSH; this server exposes no HTTP port.
- Optional daily SQLite backup timer.

AI config entries can include optional
`input_token_price_per_million` and `output_token_price_per_million` fields.
When those are omitted, ingest records `unpriced_tokens` instead of guessing
cost from provider pricing that may have changed.

## Commands

```bash
bash launcher.sh bootstrap
```

Guided Ubuntu 22.04 first-run setup. This is the default when no command is
given.

```bash
bash launcher.sh start
bash launcher.sh install
```

Validate config, write `/etc/hnreader/server.env`, install/update the systemd
units, enable them, and start the service. `start` and `install` are aliases.

```bash
bash launcher.sh restart
```

Rewrite env/unit files and restart the service after code or config changes.

```bash
bash launcher.sh env
```

Rewrite `/etc/hnreader/server.env` only. AI config hot-loads on the next round;
non-AI runtime changes should use `restart`.

```bash
bash launcher.sh status
```

Show systemd status for `hnreader-db-init.service` and
`hnreader-ingest.service`.

```bash
bash launcher.sh logs
```

Follow systemd logs for both service units.

```bash
bash launcher.sh doctor
```

Run local health checks: SQLite integrity, latest ingest state, cloud sync, AI
config, disk/config basics.

```bash
bash launcher.sh repair
```

Initialize or migrate schema and recover abandoned running ingest records.

```bash
bash launcher.sh ai-check --no-probe
bash launcher.sh ai-check
```

Validate AI configuration. Without `--no-probe`, it also contacts the provider.

```bash
bash launcher.sh metrics
```

Print pipeline metrics as JSON.

```bash
bash launcher.sh reset-failed
```

Reset failed enrich jobs back to pending after fixing an AI prompt/model/config
problem.

```bash
bash launcher.sh backup
bash launcher.sh backup /path/to/snapshot.db
```

Create a SQLite online backup. The default destination is
`data/backups/hnreader-<timestamp>.db` under the server data directory.

```bash
bash launcher.sh verify
bash launcher.sh verify-file /path/to/snapshot.db
```

Run SQLite integrity checks on the live DB or a backup file.

```bash
bash launcher.sh restore /path/to/snapshot.db
```

Stop ingest, verify the backup, replace the live DB, and keep the previous DB as
a rollback file.

## Migration Archives

`migration.py` exports and imports the runtime state needed to move an Ubuntu
22.04 deployment to another checkout of this server. It does not package source
code or `.venv`; clone/update the repo first, then import the archive. The
archive contains secrets from env files, so `export` writes it with mode `0600`
on POSIX systems and you should store/transfer it as a secret.

Create an archive:

```bash
python3 migration.py export /path/to/hnreader-migration.tar.gz
```

Inspect what an archive contains:

```bash
python3 migration.py inspect /path/to/hnreader-migration.tar.gz
```

Import on the target server:

```bash
python3 migration.py import /path/to/hnreader-migration.tar.gz --yes
bash launcher.sh restart
bash launcher.sh doctor
```

Import is a full replacement. It stops the hnreader systemd services when
available (and aborts if a loaded unit cannot be stopped), removes the target
checkout's managed env/data/log state, then copies the archived state in.
Imported `.env*` and external AI config files are set to owner-only mode on
POSIX systems. After that, `launcher.sh restart` regenerates
`/etc/hnreader/server.env`, rewrites systemd units, fixes runtime directory
ownership, and starts from the imported database. Keep the archive until
`launcher.sh doctor` passes; import is destructive and does not create a local
rollback copy of the replaced runtime directories.

Archive coverage:

- Included: `.env`, `.env.local`, `.env.*.local` from the project/server dirs.
- Included: the directory containing `HNREADER_DB_PATH` (default `data/`),
  including cloud-sync JSON output, alert outbox/cache files, and DB backups.
- Included: `HNREADER_LOG_DIR` (default `logs/`).
- Included when they are configured outside the DB directory:
  `HNREADER_CLOUD_SYNC_OUTPUT_DIR`, `HNREADER_ALERT_OUTBOX_PATH`,
  `HNREADER_AI_CONFIG_STATUS_CACHE_PATH`, and external files named by
  `HNREADER_AI_CONFIG_FILE`.
- Included only with `--include-service-env`: the generated service env file
  (default `/etc/hnreader/server.env`). Normal migrations do not need this
  because `launcher.sh restart` recreates it from `.env.local`.
- Excluded: SQLite `-wal`/`-shm`/`-journal` sidecars. The live DB is copied with
  SQLite's online backup API so `hnreader.db` in the archive is self-contained.
- Excluded: transient `*.pid`, `*.lock`, and `supervisor.stop` files.

If the deployment uses non-default absolute paths in `.env.local`
(`HNREADER_DB_PATH`, `HNREADER_LOG_DIR`, or related paths), the import restores
state to those paths from the archive. Make sure the target host should use the
same paths before running `launcher.sh restart`.

```bash
bash launcher.sh stop
```

Stop the systemd services without disabling them.

```bash
bash launcher.sh disable
```

Stop and disable the systemd services. If the optional daily backup timer was
installed during bootstrap, it is disabled too.

```bash
bash launcher.sh help
```

Print the command list.

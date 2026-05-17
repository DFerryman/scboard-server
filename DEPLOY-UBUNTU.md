# Ubuntu 22.04 Deployment Guide (sync-only)

Complete steps to bring the hnreader backend up on a clean Ubuntu 22.04 server.

Applies to: the sync-only production architecture, single-host deployment, SQLite storage. On every ingest finalize, the business read model is pushed first via HMAC (`writeBatch` + `switchMeta`); once the business state has reached its final form, the dashboard projection is pushed (a separate `writeDashboard` action). Both stages are sent to the cloud function `pushSync`.

Does not apply to: multi-host clusters, Postgres migration, Dockerized deployment. **The server no longer has any HTTP listener**, so there is no "expose the dashboard externally" deployment shape either -- the dashboard data lives entirely in the cloud development database.

---

## 1. Prerequisites

- An Ubuntu 22.04 server, with a login user that has sudo privileges
- At least 1 GB RAM and 5 GB disk
- **No need** to open 80/443 to the public internet -- in sync-only mode the server only initiates outbound HTTPS pushes to the cloud function; it does not listen
- An OpenAI-compatible API key (the default config uses DeepSeek; if you do not want AI, set `HNREADER_AI_PROVIDER=none` to use the fallback)
- A WeChat cloud development environment plus a deployed `pushSync` cloud function; note its HTTP trigger URL and `PUSH_SECRET`

---

## 2. Install system dependencies

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git curl ufw
```

Ubuntu 22.04 ships with Python 3.10, which works directly; you do not need to install deadsnakes.

---

## 3. Pull the code into `/opt/hnreader`

`launcher.sh` recommends `/opt/hnreader` or `/srv/hnreader`, to avoid systemd's `ProtectHome=read-only` locking the project out.

```bash
sudo mkdir -p /opt/hnreader
sudo chown "$USER":"$USER" /opt/hnreader
git clone <your repository URL> /opt/hnreader
cd /opt/hnreader
```

If the code is already on your local machine, uploading it to `/opt/hnreader` with `scp` / `rsync` works too.

---

## 4. Create a Python virtual environment and install dependencies

```bash
cd /opt/hnreader
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r server/requirements.txt -c server/constraints.txt
```

By default `launcher.sh` looks for the interpreter at `/opt/hnreader/.venv/bin/python` (the `PYTHON_BIN` variable). If the venv path differs, you will need to export `PYTHON_BIN` later.

The only dependency at this point is `pydantic` -- sync-only removed fastapi/uvicorn. `server/constraints.txt` pins transitive dependencies such as pydantic / pydantic_core / annotated-types to a validated set of versions, ensuring that venvs built at different times behave consistently. To upgrade to a new pydantic, on a test machine run `pip install -U`, run `python -m unittest -v server.test_api`, and once it passes, rewrite `server/constraints.txt` using `pip freeze | grep -E '^(pydantic|annotated|typing)'`.

---

## 5. Write per-host secrets into `.env.local`

`launcher.sh` loads environment variables in this order:
shell env -> project root `.env.local` -> `server/.env.local` (a file read later overrides same-named variables from earlier files).

Create `/opt/hnreader/.env.local`:

```bash
sudo tee /opt/hnreader/.env.local > /dev/null <<'EOF'
# ---- AI provider (defaults to DeepSeek, replace the placeholder key) ----
HNREADER_AI_PROVIDER=enabled
HNREADER_AI_CONFIGS=[{"name":"DeepSeek","api_key":"REPLACE_WITH_DEEPSEEK_API_KEY","model":"deepseek-v4-flash","base_url":"https://api.deepseek.com","balance_url":"https://api.deepseek.com/user/balance","timeout_seconds":120,"max_concurrent_requests":1,"max_output_tokens":8000}]

# ---- Cloud sync (the core of sync-only: read model + dashboard pushed to the cloud function) ----
# When disabled (empty / 0), no push is done after ingest finalize.
# CLOUD_PUSH_URL must be https, and must not point at loopback / private network / metadata (169.254.169.254).
HNREADER_CLOUD_SYNC_ENABLED=1
HNREADER_CLOUD_PUSH_URL=
HNREADER_CLOUD_PUSH_SECRET=

# ---- Alert email (optional) ----
HNREADER_ADMIN_EMAIL_ENABLED=false
HNREADER_ADMIN_EMAIL_TO=
HNREADER_SMTP_HOST=smtp.163.com
HNREADER_SMTP_PORT=465
HNREADER_SMTP_USERNAME=
HNREADER_SMTP_FROM=
HNREADER_SMTP_PASSWORD=
HNREADER_SMTP_STARTTLS=false
HNREADER_SMTP_SSL=true
EOF

sudo chmod 600 /opt/hnreader/.env.local
```

Before starting, deal with at least these groups of values:

- AI: replace `REPLACE_WITH_DEEPSEEK_API_KEY` with a real key; if you do not want AI, set `HNREADER_AI_PROVIDER=none`
- Cloud sync: when `HNREADER_CLOUD_SYNC_ENABLED=1` is on, both `HNREADER_CLOUD_PUSH_URL` and `HNREADER_CLOUD_PUSH_SECRET` must be filled in, or the launcher startup check fails and exits immediately

> After sync-only, `HNREADER_ADMIN_TOKEN` / `HNREADER_LOCAL_DASHBOARD_*` /
> `API_HOST` / `API_PORT` / `MINIPROGRAM_BACKGROUND_INGEST` are no longer
> needed -- these fields, along with `server.main` / `server.auth` /
> `server/web/`, were removed together during this sync-only housekeeping;
> if you see old examples, just ignore them.

`launcher.sh` will reject: a placeholder AI key (`REPLACE_WITH`), AI enabled but `HNREADER_AI_CONFIGS` missing, and `HNREADER_CLOUD_SYNC_ENABLED=1` but URL/SECRET incomplete.

The JSON in `HNREADER_AI_CONFIGS` must be a single line (the launcher's env parser splits by line); the `"` characters inside must not be eaten by the shell, which is why a `<<'EOF'` (single-quoted heredoc, no variable expansion) is used above.

**Commands to generate the secret**:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"        # CLOUD_PUSH_SECRET (64 char hex)
```

`HNREADER_CLOUD_PUSH_SECRET` must match the `pushSync` cloud function's `PUSH_SECRET` environment variable.

---

## 6. Start the service

```bash
cd /opt/hnreader
sudo bash server/launcher.sh start
```

In this step the launcher automatically does the following:

1. Validates AI and cloud sync config integrity (an unreplaced placeholder key, or cloud sync enabled but URL/SECRET incomplete, both fail immediately)
2. Creates the `hnreader` system user and group (`useradd --system`, no shell)
3. Creates `/opt/hnreader/server/data` and `/opt/hnreader/server/logs` and `chown hnreader:hnreader`
4. Writes `/etc/hnreader/server.env` (0640, owner=root, group=hnreader)
5. Writes the systemd units: `hnreader-db-init.service` + `hnreader-ingest.service`,
   two of them, **no api unit**; if an old `hnreader-api.service` remains it is disabled and removed
6. `systemctl enable --now` on these services

On success it prints:

```
Sync-only mode active. Cloud dashboard reads from cloud DB collections:
  hn_dashboard_summary
  hn_dashboard_ingest_runs
  hn_dashboard_cloud_sync_runs

No HTTP surface is exposed by this host. All operator visibility goes
through the cloud DB.
```

**Verify**:

```bash
sudo systemctl status hnreader-ingest hnreader-db-init

# Look at the latest cloud_sync_runs row to confirm the push is running
sudo -u hnreader sqlite3 /opt/hnreader/server/data/hnreader.db \
  "SELECT run_id, status, sync_version, stories FROM cloud_sync_runs ORDER BY started_at DESC LIMIT 3;"

# In the cloud development database console, view the _id="summary" document in the hn_dashboard_summary collection
```

The first ingest takes a few minutes to fetch data. When a cloud push actually starts, `cloud_sync_runs` first writes a `running` row, and after the push wraps up it is updated to one of the statuses in the table below; for full semantics see the "Cloud sync details" section of `server/README.md`:

- `ok`      -- both the business and dashboard stages succeeded
- `failed`  -- business publish failed (config/network/4xx)
- `warning` -- business ok but dashboard failed / skipped / local table write failed; degraded, triggers an alert

A long-lived `running` indicates the previous push started but did not wrap up properly (commonly from an abnormal crash); `skipped` / `deferred` are in-memory states of this ingest's summary / alert and are normally not written to `cloud_sync_runs`.

---

## 7. Firewall (ufw)

In sync-only mode the server only initiates outbound connections; there is no inbound listener at all. There is **no need** to open 80/443/any port. The minimal policy:

```bash
sudo ufw allow OpenSSH
sudo ufw enable
sudo ufw status
```

If you want to inspect the data with sqlite3 on the host, just SSH in and use the sqlite3 command; there is no HTTP to proxy.

WARNING: do not expose the hnreader server to the public internet (no 80/443/3000 whatsoever). The production data source for the dashboard already lives in the cloud development database `hn_dashboard_*` collections, read via the cloud development console / cloud functions / your own frontend.

---

## 8. Day-to-day operations commands

```bash
# Check service status
sudo bash /opt/hnreader/server/launcher.sh status

# Follow the logs (Ctrl+C to exit)
sudo bash /opt/hnreader/server/launcher.sh logs

# Local inspection: DB integrity/latest ingest/latest cloud sync/AI/disk/config
sudo bash /opt/hnreader/server/launcher.sh doctor

# Validate AI config parsing; without --no-probe it contacts the provider to probe balance or /models
sudo bash /opt/hnreader/server/launcher.sh ai-check --no-probe

# Pipeline metrics as JSON
sudo bash /opt/hnreader/server/launcher.sh metrics

# Apply config after editing .env.local (rewrites /etc/hnreader/server.env and restarts)
sudo bash /opt/hnreader/server/launcher.sh restart

# Stop the service (without uninstalling the units)
sudo bash /opt/hnreader/server/launcher.sh stop

# Uninstall the service (disable + stop, unit files retained)
sudo bash /opt/hnreader/server/launcher.sh disable

# After an AI prompt upgrade, reset failed enrich jobs back to pending
sudo bash /opt/hnreader/server/launcher.sh reset-failed
```

You can also use `systemctl` directly:

```bash
sudo systemctl restart hnreader-ingest
sudo systemctl restart hnreader-db-init
sudo journalctl -u hnreader-ingest -f --since "10 min ago"
```

---

## 9. Back up / restore SQLite

**Back up before an upgrade or major change.** `launcher.sh` calls the SQLite Online Backup API, which is safe even while ingest is still running (it does not block writes, and produces a consistent snapshot as of the moment the backup started).

```bash
# Back up to ${DB_DIR}/backups/hnreader-<timestamp>.db
sudo bash /opt/hnreader/server/launcher.sh backup

# Or a custom path (a mount point / external disk is fine, as long as the hnreader user can write)
sudo bash /opt/hnreader/server/launcher.sh backup /mnt/snapshots/hnreader-pre-upgrade.db

# Verify the current live DB
sudo bash /opt/hnreader/server/launcher.sh verify

# Verify a particular backup file
sudo bash /opt/hnreader/server/launcher.sh verify-file /mnt/snapshots/hnreader-pre-upgrade.db
```

The backup artifact is a single `.db` file (DELETE journal mode, no `-wal` / `-shm` side files), and carries its own `PRAGMA integrity_check`; on failure the partial file is deleted outright, so no "looks usable" corrupt snapshot is left behind.

**Restore** (`hnreader-ingest` is automatically stopped first):

```bash
sudo bash /opt/hnreader/server/launcher.sh restore /mnt/snapshots/hnreader-pre-upgrade.db
# The original DB is renamed to hnreader.db.pre-restore-<timestamp>.bak, so you can roll back
sudo bash /opt/hnreader/server/launcher.sh start
```

**Automatic periodic backup** (one copy daily at 03:00):

```bash
sudo tee /etc/systemd/system/hnreader-backup.service > /dev/null <<'EOF'
[Unit]
Description=HN Reader daily SQLite backup
After=hnreader-db-init.service

[Service]
Type=oneshot
ExecStart=/usr/bin/bash /opt/hnreader/server/launcher.sh backup
EOF

sudo tee /etc/systemd/system/hnreader-backup.timer > /dev/null <<'EOF'
[Unit]
Description=Run hnreader-backup daily

[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now hnreader-backup.timer
sudo systemctl list-timers hnreader-backup.timer
```

`${DB_DIR}/backups` is brought under the hnreader user by the launcher during its `chown`. **Remember to do the cleanup yourself** (for example a cron `find` that keeps the last 14 days) -- the launcher does not delete old backups automatically, to avoid accidental deletion.

---

## 10. Upgrade the code

```bash
cd /opt/hnreader

# Back up first (mandatory)
sudo bash server/launcher.sh backup

sudo -u $USER git pull
# If dependencies changed
.venv/bin/pip install -r server/requirements.txt -c server/constraints.txt
sudo bash server/launcher.sh restart

# Verify the new version comes up
sudo bash server/launcher.sh status
```

`db.init_db()` carries its own column migration; a new version that adds a field will ALTER TABLE automatically, no manual run needed.

If something goes wrong and you need to roll back: `sudo bash server/launcher.sh restore <the backup you just made>` + `git checkout <previous version>` + `restart`.

---

## 11. Common issues

**`ERROR: AI config still contains a placeholder key`**
-> `REPLACE_WITH...` in `HNREADER_AI_CONFIGS` in `.env.local` was not replaced with a real key. Either replace it, or add a line `HNREADER_AI_PROVIDER=none` to turn off real AI and use the fallback.

**`ERROR: cloud sync is enabled but config is incomplete`**
-> `HNREADER_CLOUD_SYNC_ENABLED=1` but URL/SECRET is missing one of them. Fill in both `HNREADER_CLOUD_PUSH_URL` and `HNREADER_CLOUD_PUSH_SECRET`, or set `HNREADER_CLOUD_SYNC_ENABLED` to empty / 0 to turn it off.

**`cloud_sync_runs` keeps getting no new rows**
-> Look at `journalctl -u hnreader-ingest -f` and confirm whether each ingest round triggers `[cloud_sync] OK ...`; if the URL validation fails (non-https / private network) it raises `CloudPushUrlError`.

**The latest `cloud_sync_runs` row is `warning`**
-> The business publish (Phase A) succeeded, but some later step degraded:
  - the dashboard publish (Phase B) failed / was skipped due to exceeding the remaining budget / the runner raised an exception;
  - or business ok but the local `cloud_sync_runs` UPDATE failed (`record_ok=False`, e.g. the DB was briefly locked).
If the local `cloud_sync_runs` row was written successfully, the `error` column indicates which case it is, prefixed with `business ok; dashboard ...`. If the warning was caused specifically by a dashboard publish failure, the cloud-side `hn_dashboard_cloud_sync_runs` stays at the state successfully written in the previous round; once the next dashboard publish succeeds it naturally backfills the cloud side, and the business collections are unaffected.

**SQLite `database is locked` error**
-> Check whether a second ingest instance was started by mistake (for example a manual `python -m server.ingest --loop`). The `hnreader-ingest.service` installed by the launcher is the only allowed writer.

---

## 12. Things not to do

- Do NOT put `/opt/hnreader/server/data` on an NFS / SMB / OneDrive sync drive
- Do NOT share the same SQLite file across multiple servers -- to scale out to multiple hosts, migrate to Postgres
- Do NOT manually run `python -m server.ingest --loop` outside the systemd unit
- Do NOT expose the hnreader server externally (there is no HTTP listener, so a reverse proxy is pointless too)
- Do NOT try to restore `server/main.py` / `server/auth.py` / `server/web/`; in the sync-only architecture they were deliberately deleted; the production data for the dashboard is already in the cloud development database
- Do NOT commit `.env.local` to git (the project `.gitignore` already excludes it, but double-check)
- Do NOT use methods like `--no-verify` / `--force` to bypass the systemd restrictions

---

## 13. Further reading

- Full list of environment variables and their defaults: `server/README.md`
- Data pipeline behavior details (catalogVersion / partial / timeout): the "Data pipeline CLI" section of `server/README.md`
- Cloud collection contract: `server/CLOUD_COLLECTIONS.md`

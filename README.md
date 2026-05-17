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

During the guided setup you will be asked for:

- AI config, or `none` to use fallback output.
- WeChat `pushSync` HTTP trigger URL and `PUSH_SECRET`.
- Optional admin alert email settings.
- Optional UFW setup. It allows only OpenSSH; this server exposes no HTTP port.
- Optional daily SQLite backup timer.

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

```bash
bash launcher.sh stop
```

Stop the systemd services without disabling them.

```bash
bash launcher.sh disable
```

Stop and disable the systemd services.

```bash
bash launcher.sh help
```

Print the command list.

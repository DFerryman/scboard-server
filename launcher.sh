#!/usr/bin/env bash
set -euo pipefail

# HN Reader production service manager (sync-only).
#
# Run `bash server/launcher.sh help` for the full command list.
# Edit the configuration block below before first production use.
#
# Sync-only mode:
#   Production runs only db init + the ingest supervisor. Each ingest finalize
#   is orchestrated by `ingest._trigger_and_record_cloud_sync` into two push
#   phases:
#     Phase A `cloud_sync_runner.run_business_once` -- business collections
#       (`writeBatch` + `switchMeta`) version switch; this round's
#       `cloud_sync_runs` reaches a terminal state.
#     Phase B `cloud_sync_runner.run_dashboard_once` (runs only when the
#       business phase succeeded) -- a standalone `writeDashboard` action
#       pushes the dashboard projection.
#   A failure in either phase does not block the ingest main flow. Local state
#   is written to the `cloud_sync_runs` table first; only after a successful
#   dashboard push does it enter the `hn_dashboard_cloud_sync_runs` collection.
#   The operator dashboard reads from the cloud-development database
#   `hn_dashboard_*` collections; the server does not listen on any HTTP port.
#
# Production path note:
# - Prefer /opt/hnreader or /srv/hnreader. This avoids both systemd home
#   namespace restrictions and regular Unix home directory permissions.
# - If PROJECT_DIR must be under /home/*, keep ProtectHome=read-only and make
#   sure the service user can traverse/read /home/<user> (for example via ACL,
#   group membership, or o+rx). The DB directory is still chowned below.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

load_env_file() {
  local f
  f="$1"
  [[ -f "$f" ]] || return 0

  local line k v
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
      k="${BASH_REMATCH[1]}"
      v="${BASH_REMATCH[2]}"
      v="${v%$'\r'}"
      export "$k=$v"
    fi
  done <"$f"
}

PROJECT_DIR="${PROJECT_DIR:-${DEFAULT_PROJECT_DIR}}"
load_env_file "${PROJECT_DIR}/.env.local"
load_env_file "${SCRIPT_DIR}/.env.local"

# ---------- Deployment paths / service identity ----------

PROJECT_DIR="${PROJECT_DIR:-${DEFAULT_PROJECT_DIR}}"
APP_USER="${APP_USER:-hnreader}"
APP_GROUP="${APP_GROUP:-${APP_USER}}"
SERVICE_PREFIX="${SERVICE_PREFIX:-hnreader}"
ENV_DIR="${ENV_DIR:-/etc/hnreader}"
ENV_FILE="${ENV_FILE:-${ENV_DIR}/server.env}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
PYTHON_BIN="${PYTHON_BIN:-${PROJECT_DIR}/.venv/bin/python}"

# ---------- Core runtime env ----------

HNREADER_DB_PATH="${HNREADER_DB_PATH:-${PROJECT_DIR}/server/data/hnreader.db}"
HNREADER_LOG_DIR="${HNREADER_LOG_DIR:-${PROJECT_DIR}/server/logs}"
HNREADER_APP_VERSION="${HNREADER_APP_VERSION:-1.0.0}"

# ---------- Cloud sync (sync-only: the only outbound channel) ----------
#
# When disabled (empty / 0 / false), no push happens after ingest finalize.
# When enabled, two push phases run:
#   Phase A `run_business_once`:
#     1. cloud_sync.build_read_model(include_dashboard=False) writes
#        stories/topics/digests/meta.json
#     2. cloud_push.push_read_model via HMAC POST (`writeBatch` + `switchMeta`)
#   Phase B `run_dashboard_once` (only when Phase A succeeded):
#     1. dashboard_projection.build_dashboard_projection writes the three
#        dashboard artifacts
#     2. cloud_push.push_dashboard via a standalone `writeDashboard` action
#        HMAC POST
# A failure does not block the ingest main flow; local state is recorded in the
# cloud_sync_runs table, and is synced to the hn_dashboard_cloud_sync_runs
# collection only after a successful dashboard push.

HNREADER_CLOUD_SYNC_ENABLED="${HNREADER_CLOUD_SYNC_ENABLED:-}"
HNREADER_CLOUD_PUSH_URL="${HNREADER_CLOUD_PUSH_URL:-}"
HNREADER_CLOUD_PUSH_SECRET="${HNREADER_CLOUD_PUSH_SECRET:-}"
HNREADER_CLOUD_PUSH_BATCH_SIZE="${HNREADER_CLOUD_PUSH_BATCH_SIZE:-50}"
HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS="${HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS:-120}"

# ---------- Feed / Hacker News HTTP ----------

HNREADER_FEED_WINDOW_SIZE="${HNREADER_FEED_WINDOW_SIZE:-100}"
HNREADER_STORY_STORE_MAX_ROWS="${HNREADER_STORY_STORE_MAX_ROWS:-1000}"
HNREADER_HN_API_BASE="${HNREADER_HN_API_BASE:-https://hacker-news.firebaseio.com/v0}"
HNREADER_HN_REQUEST_TIMEOUT_SECONDS="${HNREADER_HN_REQUEST_TIMEOUT_SECONDS:-10}"
HNREADER_HN_RETRY_ATTEMPTS="${HNREADER_HN_RETRY_ATTEMPTS:-3}"

# ---------- Comment fetching ----------

HNREADER_COMMENT_FETCH_LIMIT="${HNREADER_COMMENT_FETCH_LIMIT:-60}"
HNREADER_COMMENT_MAX_DEPTH="${HNREADER_COMMENT_MAX_DEPTH:-2}"
HNREADER_COMMENT_MIN_DESCENDANTS="${HNREADER_COMMENT_MIN_DESCENDANTS:-5}"

# ---------- Ingest scheduling / timeout ----------

HNREADER_INGEST_INTERVAL_SECONDS="${HNREADER_INGEST_INTERVAL_SECONDS:-1800}"
HNREADER_INGEST_ROUND_TIMEOUT_SECONDS="${HNREADER_INGEST_ROUND_TIMEOUT_SECONDS:-1680}"
HNREADER_INGEST_DIGEST_RESERVED_SECONDS="${HNREADER_INGEST_DIGEST_RESERVED_SECONDS:-90}"
HNREADER_INGEST_CHILD_KILL_GRACE_SECONDS="${HNREADER_INGEST_CHILD_KILL_GRACE_SECONDS:-10}"
HNREADER_INGEST_FAILURE_BACKOFF_SECONDS="${HNREADER_INGEST_FAILURE_BACKOFF_SECONDS:-60}"
INGEST_STOP_TIMEOUT_SECONDS="${INGEST_STOP_TIMEOUT_SECONDS:-30}"

# ---------- Enricher recovery / retention ----------

HNREADER_ENRICH_STALE_SECONDS="${HNREADER_ENRICH_STALE_SECONDS:-600}"
HNREADER_ENRICH_MAX_ATTEMPTS="${HNREADER_ENRICH_MAX_ATTEMPTS:-3}"
HNREADER_ENRICH_WORKER_COUNT="${HNREADER_ENRICH_WORKER_COUNT:-8}"
HNREADER_ENRICH_SESSION_STORY_LIMIT="${HNREADER_ENRICH_SESSION_STORY_LIMIT:-16}"
HNREADER_ENRICH_BATCH_SIZE="${HNREADER_ENRICH_BATCH_SIZE:-3}"
HNREADER_REENRICH_DESCENDANTS_MIN_DELTA="${HNREADER_REENRICH_DESCENDANTS_MIN_DELTA:-20}"
HNREADER_REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT="${HNREADER_REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT:-30}"
HNREADER_COMMENT_RETENTION_DAYS="${HNREADER_COMMENT_RETENTION_DAYS:-7}"

# ---------- Digest / cleanup ----------

HNREADER_DIGEST_TIMEZONE="${HNREADER_DIGEST_TIMEZONE:-Asia/Shanghai}"
HNREADER_DIGEST_UPDATE_INTERVAL_SECONDS="${HNREADER_DIGEST_UPDATE_INTERVAL_SECONDS:-1800}"
HNREADER_DIGEST_MIN_NEW_DONE_STORIES="${HNREADER_DIGEST_MIN_NEW_DONE_STORIES:-3}"
HNREADER_DIGEST_MAX_STORIES="${HNREADER_DIGEST_MAX_STORIES:-7}"
HNREADER_DIGEST_RETENTION_DAYS="${HNREADER_DIGEST_RETENTION_DAYS:-30}"
HNREADER_INGEST_RUN_RETENTION_DAYS="${HNREADER_INGEST_RUN_RETENTION_DAYS:-30}"
HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS="${HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS:-30}"
HNREADER_RANKING_CANDIDATE_RETENTION_DAYS="${HNREADER_RANKING_CANDIDATE_RETENTION_DAYS:-3}"
HNREADER_TOPIC_RETENTION_DAYS="${HNREADER_TOPIC_RETENTION_DAYS:-30}"
HNREADER_RANKING_GRACE_SECONDS="${HNREADER_RANKING_GRACE_SECONDS:-86400}"
HNREADER_CLEANUP_STALE_GUARD_SECONDS="${HNREADER_CLEANUP_STALE_GUARD_SECONDS:-43200}"

# ---------- AI provider ----------
#
# HNREADER_AI_CONFIGS is the source of truth: a JSON array of OpenAI-compatible
# providers. Failover order = array order; switches on connection errors,
# provider JSON parse errors, and HTTP 429/5xx. It does not switch on
# per-request timeout, HTTP 401/403, or malformed model output.
#
# Default below is a single DeepSeek entry with a placeholder api_key. Replace
# sk-REPLACE_WITH_YOUR_DEEPSEEK_API_KEY before running. To add a backup
# provider, append another object after a comma, e.g.:
#   [{...primary...},{"name":"DeepSeek backup","api_key":"sk-BACKUP","model":"deepseek-v4-flash","base_url":"https://api.deepseek.com","balance_url":"https://api.deepseek.com/user/balance","timeout_seconds":60,"max_concurrent_requests":1}]
#
# When HNREADER_AI_CONFIGS is non-empty it fully overrides the legacy
# HNREADER_AI_API_KEY / HNREADER_AI_MODEL / HNREADER_AI_BASE_URL trio.

# Any non-empty / non-"none" value enables RealAiAgent; the literal is not
# used for routing -- each AI_CONFIGS entry routes itself via its base_url.
HNREADER_AI_PROVIDER="${HNREADER_AI_PROVIDER:-enabled}"
# The Python ingest process re-reads this file before building AI agents, so
# AI provider/model changes can take effect without redeploying code. It can
# point at the generated env file or at a dedicated JSON ai-config file.
HNREADER_AI_CONFIG_FILE="${HNREADER_AI_CONFIG_FILE:-$ENV_FILE}"
# Single-quoted heredoc keeps JSON `"` and `}` as literals; ${VAR:-${DEFAULT}}
# would otherwise close at the first `}` inside the JSON object.
DEFAULT_AI_CONFIGS='[{"name":"DeepSeek","api_key":"sk-REPLACE_WITH_YOUR_DEEPSEEK_API_KEY","model":"deepseek-v4-flash","base_url":"https://api.deepseek.com","balance_url":"https://api.deepseek.com/user/balance","timeout_seconds":120,"max_concurrent_requests":1,"max_output_tokens":8000}]'
if [[ "$HNREADER_AI_CONFIG_FILE" == "$ENV_FILE" ]]; then
  HNREADER_AI_CONFIGS="${HNREADER_AI_CONFIGS:-$DEFAULT_AI_CONFIGS}"
else
  HNREADER_AI_CONFIGS="${HNREADER_AI_CONFIGS:-}"
fi
HNREADER_AI_API_KEY="${HNREADER_AI_API_KEY:-}"
HNREADER_AI_MODEL="${HNREADER_AI_MODEL:-}"
HNREADER_AI_BASE_URL="${HNREADER_AI_BASE_URL:-}"
HNREADER_AI_INTERNAL_HOST_ALLOWLIST="${HNREADER_AI_INTERNAL_HOST_ALLOWLIST:-}"
HNREADER_AI_REQUEST_TIMEOUT_SECONDS="${HNREADER_AI_REQUEST_TIMEOUT_SECONDS:-60}"
HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS="${HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS:-60}"
HNREADER_AI_CONFIG_STATUS_CACHE_PATH="${HNREADER_AI_CONFIG_STATUS_CACHE_PATH:-}"
HNREADER_AI_ENRICH_BODY_MAX_CHARS="${HNREADER_AI_ENRICH_BODY_MAX_CHARS:-1200}"
HNREADER_AI_ENRICH_COMMENT_LIMIT="${HNREADER_AI_ENRICH_COMMENT_LIMIT:-18}"
HNREADER_AI_ENRICH_COMMENT_MAX_CHARS="${HNREADER_AI_ENRICH_COMMENT_MAX_CHARS:-240}"

# ---------- Admin alert email ----------

HNREADER_ADMIN_EMAIL_ENABLED="${HNREADER_ADMIN_EMAIL_ENABLED:-false}"
HNREADER_ADMIN_EMAIL_TO="${HNREADER_ADMIN_EMAIL_TO:-}"
HNREADER_SMTP_HOST="${HNREADER_SMTP_HOST:-}"
HNREADER_SMTP_PORT="${HNREADER_SMTP_PORT:-587}"
HNREADER_SMTP_USERNAME="${HNREADER_SMTP_USERNAME:-}"
HNREADER_SMTP_PASSWORD="${HNREADER_SMTP_PASSWORD:-}"
HNREADER_SMTP_FROM="${HNREADER_SMTP_FROM:-}"
HNREADER_SMTP_STARTTLS="${HNREADER_SMTP_STARTTLS:-true}"
HNREADER_SMTP_SSL="${HNREADER_SMTP_SSL:-false}"
HNREADER_ALERT_COOLDOWN_SECONDS="${HNREADER_ALERT_COOLDOWN_SECONDS:-1800}"
HNREADER_ALERT_OUTBOX_MAX_RECORDS="${HNREADER_ALERT_OUTBOX_MAX_RECORDS:-1000}"

INGEST_SERVICE="${SERVICE_PREFIX}-ingest.service"
DB_INIT_SERVICE="${SERVICE_PREFIX}-db-init.service"

usage() {
  cat <<EOF
HN Reader production service manager (sync-only).

Usage: bash server/launcher.sh <command>

Commands:
  install   write env + units, enable and start services
  start     alias for install
  restart   write env + units, restart services
  stop      stop services
  status    show services' status
  logs      follow service logs (journalctl -f)
  disable   stop and disable services
  env       rewrite env file only (AI config hot-reloads; non-AI needs restart)
  ai-check  validate AI config; probes provider unless --no-probe is passed
  doctor    run DB integrity, latest run, cloud sync, AI, disk checks
  repair    initialize schema and recover abandoned running ingest rows
  metrics   print pipeline metrics JSON (read-only)
  reset-failed
            reset failed enrich jobs back to pending once
  backup [path]
            SQLite online backup of HNREADER_DB_PATH (safe while ingest runs);
            default output \${DB_DIR}/backups/hnreader-<ts>.db
  restore <path>
            stop ingest, verify the backup, replace HNREADER_DB_PATH (previous
            DB is moved to *.pre-restore-<ts>.bak so it can be rolled back)
  verify    PRAGMA integrity_check on HNREADER_DB_PATH (read-only)
  help      show this help

Sync-only mode:
  Production installs only two systemd units: ${DB_INIT_SERVICE} +
  ${INGEST_SERVICE}. At ingest finalize, cloud sync pushes the read model +
  dashboard projection to the cloud-development database. The operator reads
  dashboard data from the hn_dashboard_* collections; the server no longer
  listens on any HTTP port.

Edit the configuration block at the top of the script before first
production use. All variables can also be overridden via the environment.
EOF
}

print_endpoints() {
  cat <<EOF

Sync-only mode active. Cloud dashboard reads from cloud DB collections:
  hn_dashboard_summary
  hn_dashboard_ingest_runs
  hn_dashboard_cloud_sync_runs

No HTTP surface is exposed by this host. All operator visibility goes
through the cloud DB.
EOF
}

run_root() {
  if [[ "${EUID}" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

write_root_file() {
  local target="$1"
  local mode="$2"
  local tmp
  tmp="$(mktemp)"
  cat >"$tmp"
  run_root install -D -m "$mode" "$tmp" "$target"
  rm -f "$tmp"
}

quote_env() {
  local value="${1:-}"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "$value"
}

env_line() {
  local key="$1"
  local value="$2"
  printf '%s=%s\n' "$key" "$(quote_env "$value")"
}

resolve_python() {
  if [[ -x "$PYTHON_BIN" ]]; then
    return
  fi
  echo "ERROR: PYTHON_BIN is not executable: ${PYTHON_BIN}" >&2
  echo "       Create the project virtualenv first, e.g.:" >&2
  echo "         python3 -m venv ${PROJECT_DIR}/.venv" >&2
  echo "         ${PROJECT_DIR}/.venv/bin/pip install -r ${PROJECT_DIR}/server/requirements.txt -c ${PROJECT_DIR}/server/constraints.txt" >&2
  echo "       Or set PYTHON_BIN to a Python interpreter that has the project deps installed." >&2
  exit 1
}

python_env_args() {
  local -n _out_array="$1"
  local config_source="${2:-service}"
  local ai_config_file_for_python="$HNREADER_AI_CONFIG_FILE"
  if [[ "$config_source" == "current" && "$HNREADER_AI_CONFIG_FILE" == "$ENV_FILE" ]]; then
    ai_config_file_for_python="$PROJECT_DIR/.hnreader-ai-check-env-missing"
  fi
  _out_array=(
    "HNREADER_DB_PATH=$HNREADER_DB_PATH"
    "HNREADER_LOG_DIR=$HNREADER_LOG_DIR"
    "HNREADER_APP_VERSION=$HNREADER_APP_VERSION"
    "HNREADER_CLOUD_SYNC_ENABLED=$HNREADER_CLOUD_SYNC_ENABLED"
    "HNREADER_CLOUD_PUSH_URL=$HNREADER_CLOUD_PUSH_URL"
    "HNREADER_CLOUD_PUSH_SECRET=$HNREADER_CLOUD_PUSH_SECRET"
    "HNREADER_CLOUD_PUSH_BATCH_SIZE=$HNREADER_CLOUD_PUSH_BATCH_SIZE"
    "HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS=$HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS"
    "HNREADER_ENRICH_MAX_ATTEMPTS=$HNREADER_ENRICH_MAX_ATTEMPTS"
    "HNREADER_COMMENT_RETENTION_DAYS=$HNREADER_COMMENT_RETENTION_DAYS"
    "HNREADER_DIGEST_RETENTION_DAYS=$HNREADER_DIGEST_RETENTION_DAYS"
    "HNREADER_INGEST_RUN_RETENTION_DAYS=$HNREADER_INGEST_RUN_RETENTION_DAYS"
    "HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS=$HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS"
    "HNREADER_RANKING_CANDIDATE_RETENTION_DAYS=$HNREADER_RANKING_CANDIDATE_RETENTION_DAYS"
    "HNREADER_TOPIC_RETENTION_DAYS=$HNREADER_TOPIC_RETENTION_DAYS"
    "HNREADER_AI_PROVIDER=$HNREADER_AI_PROVIDER"
    "HNREADER_AI_CONFIGS=$HNREADER_AI_CONFIGS"
    "HNREADER_AI_API_KEY=$HNREADER_AI_API_KEY"
    "HNREADER_AI_MODEL=$HNREADER_AI_MODEL"
    "HNREADER_AI_BASE_URL=$HNREADER_AI_BASE_URL"
    "HNREADER_AI_INTERNAL_HOST_ALLOWLIST=$HNREADER_AI_INTERNAL_HOST_ALLOWLIST"
    "HNREADER_AI_REQUEST_TIMEOUT_SECONDS=$HNREADER_AI_REQUEST_TIMEOUT_SECONDS"
    "HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS=$HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS"
    "HNREADER_AI_CONFIG_STATUS_CACHE_PATH=$HNREADER_AI_CONFIG_STATUS_CACHE_PATH"
    "HNREADER_AI_ENRICH_BODY_MAX_CHARS=$HNREADER_AI_ENRICH_BODY_MAX_CHARS"
    "HNREADER_AI_ENRICH_COMMENT_LIMIT=$HNREADER_AI_ENRICH_COMMENT_LIMIT"
    "HNREADER_AI_ENRICH_COMMENT_MAX_CHARS=$HNREADER_AI_ENRICH_COMMENT_MAX_CHARS"
    "HNREADER_AI_CONFIG_FILE=$ai_config_file_for_python"
  )
}

run_python_module() {
  resolve_python
  local env_args=()
  python_env_args env_args
  env "${env_args[@]}" "$PYTHON_BIN" -m "$@"
}

run_python_module_current_config() {
  resolve_python
  local env_args=()
  # install/env validate the script's current values before writing ENV_FILE.
  # Hide the generated AI env file in that path so an older file cannot
  # override the values about to be written.
  python_env_args env_args current
  env "${env_args[@]}" "$PYTHON_BIN" -m "$@"
}

ensure_user() {
  if [[ "$APP_USER" == "root" ]]; then
    return
  fi
  if ! getent group "$APP_GROUP" >/dev/null 2>&1; then
    echo "Creating system group: ${APP_GROUP}"
    run_root groupadd --system "$APP_GROUP"
  fi
  if id "$APP_USER" >/dev/null 2>&1; then
    return
  fi
  echo "Creating system user: ${APP_USER}"
  run_root useradd --system --gid "$APP_GROUP" \
    --home-dir /var/lib/hnreader --no-create-home \
    --shell /usr/sbin/nologin "$APP_USER"
}

prepare_runtime_dirs() {
  run_root mkdir -p "$(dirname "$HNREADER_DB_PATH")" "$HNREADER_LOG_DIR" "$ENV_DIR"
  if id "$APP_USER" >/dev/null 2>&1; then
    run_root chown -R "${APP_USER}:${APP_GROUP}" "$(dirname "$HNREADER_DB_PATH")" "$HNREADER_LOG_DIR"
  fi
}

validate_cloud_sync_config() {
  local enabled
  enabled="$(printf '%s' "$HNREADER_CLOUD_SYNC_ENABLED" | tr '[:upper:]' '[:lower:]')"
  case "$enabled" in
    ""|0|false|no|off)
      return
      ;;
  esac

  local missing=()
  [[ -n "$HNREADER_CLOUD_PUSH_URL" ]] || missing+=("HNREADER_CLOUD_PUSH_URL")
  [[ -n "$HNREADER_CLOUD_PUSH_SECRET" ]] || missing+=("HNREADER_CLOUD_PUSH_SECRET")
  if (( ${#missing[@]} > 0 )); then
    echo "ERROR: cloud sync is enabled but config is incomplete: ${missing[*]}" >&2
    echo "       Fill both, or set HNREADER_CLOUD_SYNC_ENABLED=0 to disable." >&2
    exit 1
  fi
  if [[ ! "$HNREADER_CLOUD_PUSH_SECRET" =~ ^[0-9A-Fa-f]{64}$ ]]; then
    echo "ERROR: HNREADER_CLOUD_PUSH_SECRET must be 64 hexadecimal characters." >&2
    echo "       Generate one with: python3 -c 'import secrets; print(secrets.token_hex(32))'" >&2
    exit 1
  fi
}

validate_ai_config() {
  local provider
  provider="$(printf '%s' "$HNREADER_AI_PROVIDER" | tr '[:upper:]' '[:lower:]')"
  case "$provider" in
    ""|none|fallback|off|disabled)
      return
      ;;
  esac

  local external_ai_config_file=0
  if [[ "$HNREADER_AI_CONFIG_FILE" != "$ENV_FILE" ]]; then
    external_ai_config_file=1
  fi

  if (( external_ai_config_file == 0 )) \
      && [[ -z "$HNREADER_AI_CONFIGS" && ( -z "$HNREADER_AI_API_KEY" || -z "$HNREADER_AI_MODEL" ) ]]; then
    echo "ERROR: AI provider is enabled but no usable AI config is set." >&2
    echo "       Fill HNREADER_AI_CONFIGS, or set HNREADER_AI_PROVIDER=none to use fallback output." >&2
    exit 1
  fi
  if (( external_ai_config_file == 0 )) \
      && [[ "$HNREADER_AI_CONFIGS" == *REPLACE_WITH* || "$HNREADER_AI_API_KEY" == *REPLACE_WITH* ]]; then
    echo "ERROR: AI config still contains a placeholder key." >&2
    echo "       Replace sk-REPLACE_WITH_YOUR_DEEPSEEK_API_KEY or disable RealAiAgent." >&2
    exit 1
  fi
  if ! run_python_module_current_config server.ops ai-check --no-probe --quiet; then
    echo "ERROR: AI config failed Python validation." >&2
    echo "       Run 'bash server/launcher.sh ai-check --no-probe' for details." >&2
    exit 1
  fi
}

validate_admin_alert_config() {
  # Mirrors server/notifications.py::validate_admin_alert_config so install /
  # env writes fail fast — without this, an ENABLED=true + missing SMTP env
  # passes the launcher but crashes ingest on startup, leaving systemd
  # (Restart=always) in a tight crash-loop. Keep the two validators in sync.
  local enabled
  enabled="$(printf '%s' "$HNREADER_ADMIN_EMAIL_ENABLED" | tr '[:upper:]' '[:lower:]')"
  case "$enabled" in
    ""|0|false|no|off)
      return
      ;;
  esac

  local missing=()
  [[ -n "$HNREADER_ADMIN_EMAIL_TO" ]] || missing+=("HNREADER_ADMIN_EMAIL_TO")
  [[ -n "$HNREADER_SMTP_HOST" ]] || missing+=("HNREADER_SMTP_HOST")
  if [[ -n "$HNREADER_SMTP_USERNAME" && -z "$HNREADER_SMTP_PASSWORD" ]]; then
    missing+=("HNREADER_SMTP_PASSWORD")
  fi
  if (( ${#missing[@]} > 0 )); then
    echo "ERROR: admin alert email is enabled but config is incomplete: ${missing[*]}" >&2
    echo "       Fill all missing fields, or set HNREADER_ADMIN_EMAIL_ENABLED=false." >&2
    exit 1
  fi
}

write_env_file() {
  echo "Writing env file: ${ENV_FILE}"
  write_root_file "$ENV_FILE" 0640 <<EOF
# Generated by ${PROJECT_DIR}/server/launcher.sh
# Edit the script configuration block, then run:
#   bash server/launcher.sh env      # AI-only changes hot-load next round
#   bash server/launcher.sh restart  # non-AI runtime changes

$(env_line HNREADER_DB_PATH "$HNREADER_DB_PATH")
$(env_line HNREADER_LOG_DIR "$HNREADER_LOG_DIR")
$(env_line HNREADER_APP_VERSION "$HNREADER_APP_VERSION")

$(env_line HNREADER_CLOUD_SYNC_ENABLED "$HNREADER_CLOUD_SYNC_ENABLED")
$(env_line HNREADER_CLOUD_PUSH_URL "$HNREADER_CLOUD_PUSH_URL")
$(env_line HNREADER_CLOUD_PUSH_SECRET "$HNREADER_CLOUD_PUSH_SECRET")
$(env_line HNREADER_CLOUD_PUSH_BATCH_SIZE "$HNREADER_CLOUD_PUSH_BATCH_SIZE")
$(env_line HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS "$HNREADER_CLOUD_SYNC_TIMEOUT_SECONDS")

$(env_line HNREADER_FEED_WINDOW_SIZE "$HNREADER_FEED_WINDOW_SIZE")
$(env_line HNREADER_STORY_STORE_MAX_ROWS "$HNREADER_STORY_STORE_MAX_ROWS")
$(env_line HNREADER_HN_API_BASE "$HNREADER_HN_API_BASE")
$(env_line HNREADER_HN_REQUEST_TIMEOUT_SECONDS "$HNREADER_HN_REQUEST_TIMEOUT_SECONDS")
$(env_line HNREADER_HN_RETRY_ATTEMPTS "$HNREADER_HN_RETRY_ATTEMPTS")

$(env_line HNREADER_COMMENT_FETCH_LIMIT "$HNREADER_COMMENT_FETCH_LIMIT")
$(env_line HNREADER_COMMENT_MAX_DEPTH "$HNREADER_COMMENT_MAX_DEPTH")
$(env_line HNREADER_COMMENT_MIN_DESCENDANTS "$HNREADER_COMMENT_MIN_DESCENDANTS")

$(env_line HNREADER_INGEST_INTERVAL_SECONDS "$HNREADER_INGEST_INTERVAL_SECONDS")
$(env_line HNREADER_INGEST_ROUND_TIMEOUT_SECONDS "$HNREADER_INGEST_ROUND_TIMEOUT_SECONDS")
$(env_line HNREADER_INGEST_DIGEST_RESERVED_SECONDS "$HNREADER_INGEST_DIGEST_RESERVED_SECONDS")
$(env_line HNREADER_INGEST_CHILD_KILL_GRACE_SECONDS "$HNREADER_INGEST_CHILD_KILL_GRACE_SECONDS")
$(env_line HNREADER_INGEST_FAILURE_BACKOFF_SECONDS "$HNREADER_INGEST_FAILURE_BACKOFF_SECONDS")

$(env_line HNREADER_ENRICH_STALE_SECONDS "$HNREADER_ENRICH_STALE_SECONDS")
$(env_line HNREADER_ENRICH_MAX_ATTEMPTS "$HNREADER_ENRICH_MAX_ATTEMPTS")
$(env_line HNREADER_ENRICH_WORKER_COUNT "$HNREADER_ENRICH_WORKER_COUNT")
$(env_line HNREADER_ENRICH_SESSION_STORY_LIMIT "$HNREADER_ENRICH_SESSION_STORY_LIMIT")
$(env_line HNREADER_ENRICH_BATCH_SIZE "$HNREADER_ENRICH_BATCH_SIZE")
$(env_line HNREADER_REENRICH_DESCENDANTS_MIN_DELTA "$HNREADER_REENRICH_DESCENDANTS_MIN_DELTA")
$(env_line HNREADER_REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT "$HNREADER_REENRICH_DESCENDANTS_MIN_GROWTH_PERCENT")
$(env_line HNREADER_COMMENT_RETENTION_DAYS "$HNREADER_COMMENT_RETENTION_DAYS")

$(env_line HNREADER_DIGEST_TIMEZONE "$HNREADER_DIGEST_TIMEZONE")
$(env_line HNREADER_DIGEST_UPDATE_INTERVAL_SECONDS "$HNREADER_DIGEST_UPDATE_INTERVAL_SECONDS")
$(env_line HNREADER_DIGEST_MIN_NEW_DONE_STORIES "$HNREADER_DIGEST_MIN_NEW_DONE_STORIES")
$(env_line HNREADER_DIGEST_MAX_STORIES "$HNREADER_DIGEST_MAX_STORIES")
$(env_line HNREADER_DIGEST_RETENTION_DAYS "$HNREADER_DIGEST_RETENTION_DAYS")
$(env_line HNREADER_INGEST_RUN_RETENTION_DAYS "$HNREADER_INGEST_RUN_RETENTION_DAYS")
$(env_line HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS "$HNREADER_CLOUD_SYNC_RUN_RETENTION_DAYS")
$(env_line HNREADER_RANKING_CANDIDATE_RETENTION_DAYS "$HNREADER_RANKING_CANDIDATE_RETENTION_DAYS")
$(env_line HNREADER_TOPIC_RETENTION_DAYS "$HNREADER_TOPIC_RETENTION_DAYS")
$(env_line HNREADER_RANKING_GRACE_SECONDS "$HNREADER_RANKING_GRACE_SECONDS")
$(env_line HNREADER_CLEANUP_STALE_GUARD_SECONDS "$HNREADER_CLEANUP_STALE_GUARD_SECONDS")

$(env_line HNREADER_AI_PROVIDER "$HNREADER_AI_PROVIDER")
$(env_line HNREADER_AI_CONFIGS "$HNREADER_AI_CONFIGS")
$(env_line HNREADER_AI_API_KEY "$HNREADER_AI_API_KEY")
$(env_line HNREADER_AI_MODEL "$HNREADER_AI_MODEL")
$(env_line HNREADER_AI_BASE_URL "$HNREADER_AI_BASE_URL")
$(env_line HNREADER_AI_INTERNAL_HOST_ALLOWLIST "$HNREADER_AI_INTERNAL_HOST_ALLOWLIST")
$(env_line HNREADER_AI_REQUEST_TIMEOUT_SECONDS "$HNREADER_AI_REQUEST_TIMEOUT_SECONDS")
$(env_line HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS "$HNREADER_AI_CONFIG_STATUS_CACHE_TTL_SECONDS")
$(env_line HNREADER_AI_CONFIG_STATUS_CACHE_PATH "$HNREADER_AI_CONFIG_STATUS_CACHE_PATH")
$(env_line HNREADER_AI_ENRICH_BODY_MAX_CHARS "$HNREADER_AI_ENRICH_BODY_MAX_CHARS")
$(env_line HNREADER_AI_ENRICH_COMMENT_LIMIT "$HNREADER_AI_ENRICH_COMMENT_LIMIT")
$(env_line HNREADER_AI_ENRICH_COMMENT_MAX_CHARS "$HNREADER_AI_ENRICH_COMMENT_MAX_CHARS")
$(env_line HNREADER_AI_CONFIG_FILE "$HNREADER_AI_CONFIG_FILE")

$(env_line HNREADER_ADMIN_EMAIL_ENABLED "$HNREADER_ADMIN_EMAIL_ENABLED")
$(env_line HNREADER_ADMIN_EMAIL_TO "$HNREADER_ADMIN_EMAIL_TO")
$(env_line HNREADER_SMTP_HOST "$HNREADER_SMTP_HOST")
$(env_line HNREADER_SMTP_PORT "$HNREADER_SMTP_PORT")
$(env_line HNREADER_SMTP_USERNAME "$HNREADER_SMTP_USERNAME")
$(env_line HNREADER_SMTP_PASSWORD "$HNREADER_SMTP_PASSWORD")
$(env_line HNREADER_SMTP_FROM "$HNREADER_SMTP_FROM")
$(env_line HNREADER_SMTP_STARTTLS "$HNREADER_SMTP_STARTTLS")
$(env_line HNREADER_SMTP_SSL "$HNREADER_SMTP_SSL")
$(env_line HNREADER_ALERT_COOLDOWN_SECONDS "$HNREADER_ALERT_COOLDOWN_SECONDS")
$(env_line HNREADER_ALERT_OUTBOX_MAX_RECORDS "$HNREADER_ALERT_OUTBOX_MAX_RECORDS")
EOF
}

write_db_init_service() {
  echo "Writing systemd unit: ${SYSTEMD_DIR}/${DB_INIT_SERVICE}"
  local db_dir
  db_dir="$(dirname "$HNREADER_DB_PATH")"
  write_root_file "${SYSTEMD_DIR}/${DB_INIT_SERVICE}" 0644 <<EOF
[Unit]
Description=HN Reader DB Init
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${PROJECT_DIR}
EnvironmentFile=-${ENV_FILE}
ExecStart=${PYTHON_BIN} -c "from server import db; db.init_db()"
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=read-only
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
ReadWritePaths=${db_dir} ${HNREADER_LOG_DIR}

[Install]
WantedBy=multi-user.target
EOF
}

write_ingest_service() {
  echo "Writing systemd unit: ${SYSTEMD_DIR}/${INGEST_SERVICE}"
  local db_dir
  db_dir="$(dirname "$HNREADER_DB_PATH")"
  write_root_file "${SYSTEMD_DIR}/${INGEST_SERVICE}" 0644 <<EOF
[Unit]
Description=HN Reader Ingest Supervisor
Requires=${DB_INIT_SERVICE}
After=network-online.target ${DB_INIT_SERVICE}
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${PROJECT_DIR}
EnvironmentFile=-${ENV_FILE}
ExecStart=${PYTHON_BIN} -m server.ingest --loop
Restart=always
RestartSec=10
TimeoutStopSec=${INGEST_STOP_TIMEOUT_SECONDS}
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=read-only
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
ReadWritePaths=${db_dir} ${HNREADER_LOG_DIR}

[Install]
WantedBy=multi-user.target
EOF
}

active_services() {
  printf '%s\n' "$DB_INIT_SERVICE" "$INGEST_SERVICE"
}

remove_legacy_api_unit() {
  # Defensive: prior versions installed a FastAPI ${SERVICE_PREFIX}-api.service.
  # If an old install left it behind, take it down so it can never come back
  # up across a sync-only upgrade. Called from every command that touches
  # systemd (install/start/restart/stop/disable/status) so an upgrade-then-stop
  # path doesn't leave the legacy unit enabled and serving HTTP. systemctl
  # is idempotent — repeating the disable on an already-removed unit is cheap
  # and only requires one privileged call to check via the file existence test.
  local api_unit="${SERVICE_PREFIX}-api.service"
  if [[ -f "${SYSTEMD_DIR}/${api_unit}" ]]; then
    echo "Sync-only mode: removing legacy ${api_unit}"
    run_root systemctl disable --now "$api_unit" >/dev/null 2>&1 || true
    run_root rm -f "${SYSTEMD_DIR}/${api_unit}"
    run_root systemctl daemon-reload >/dev/null 2>&1 || true
  fi
}

install_files() {
  resolve_python
  validate_cloud_sync_config
  validate_ai_config
  validate_admin_alert_config
  ensure_user
  prepare_runtime_dirs
  write_env_file
  write_db_init_service
  write_ingest_service
  remove_legacy_api_unit
  run_root systemctl daemon-reload
}

start_services() {
  install_files
  local svcs=()
  while IFS= read -r svc; do svcs+=("$svc"); done < <(active_services)
  run_root systemctl enable --now "${svcs[@]}"
  run_root systemctl status --no-pager "${svcs[@]}" || true
  print_endpoints
}

restart_services() {
  install_files
  local svcs=()
  while IFS= read -r svc; do svcs+=("$svc"); done < <(active_services)
  run_root systemctl enable "${svcs[@]}"
  run_root systemctl restart "${svcs[@]}"
  run_root systemctl status --no-pager "${svcs[@]}" || true
  print_endpoints
}

stop_services() {
  # Even on plain stop, sweep the legacy api unit — an upgraded host that
  # only ever runs ``stop`` (e.g. before decommission) shouldn't leave the
  # old uvicorn enabled.
  remove_legacy_api_unit
  local svcs=()
  while IFS= read -r svc; do svcs+=("$svc"); done < <(active_services)
  # Stop in reverse dependency order: ingest → db-init.
  local i
  for ((i=${#svcs[@]}-1; i>=0; i--)); do
    run_root systemctl stop "${svcs[$i]}"
  done
}

status_services() {
  remove_legacy_api_unit
  local svcs=()
  while IFS= read -r svc; do svcs+=("$svc"); done < <(active_services)
  run_root systemctl status --no-pager "${svcs[@]}"
  print_endpoints
}

logs_services() {
  local args=()
  while IFS= read -r svc; do args+=("-u" "$svc"); done < <(active_services)
  run_root journalctl "${args[@]}" -f
}

disable_services() {
  remove_legacy_api_unit
  local svcs=()
  while IFS= read -r svc; do svcs+=("$svc"); done < <(active_services)
  run_root systemctl disable --now "${svcs[@]}"
}

write_env_only() {
  if [[ ! -f "${SYSTEMD_DIR}/${DB_INIT_SERVICE}" || ! -f "${SYSTEMD_DIR}/${INGEST_SERVICE}" ]]; then
    echo "ERROR: core systemd units not found under ${SYSTEMD_DIR}." >&2
    echo "       Run 'bash server/launcher.sh install' first." >&2
    exit 1
  fi
  validate_cloud_sync_config
  validate_ai_config
  validate_admin_alert_config
  write_env_file
  echo "Wrote ${ENV_FILE}. Run 'bash server/launcher.sh restart' to apply."
}

reset_failed_jobs() {
  if [[ ! -f "${SYSTEMD_DIR}/${DB_INIT_SERVICE}" \
        || ! -f "${SYSTEMD_DIR}/${INGEST_SERVICE}" ]]; then
    echo "ERROR: systemd units not found under ${SYSTEMD_DIR}." >&2
    echo "       Run 'bash server/launcher.sh install' first." >&2
    exit 1
  fi
  resolve_python
  local db_dir
  db_dir="$(dirname "$HNREADER_DB_PATH")"
  local rw_args
  _rw_property_args rw_args "$db_dir" "$HNREADER_LOG_DIR"
  run_root systemctl start "$DB_INIT_SERVICE"
  # Use systemd-run so the maintenance command inherits the same
  # EnvironmentFile and sandbox envelope as the production services,
  # without re-running install_files (which would rewrite env + units).
  run_root systemd-run \
    --pty --wait --collect --quiet \
    --uid="$APP_USER" --gid="$APP_GROUP" \
    --working-directory="$PROJECT_DIR" \
    --property=EnvironmentFile="-${ENV_FILE}" \
    --property=ProtectSystem=strict \
    --property=ProtectHome=read-only \
    "${rw_args[@]}" \
    --property=NoNewPrivileges=yes \
    --property=PrivateTmp=yes \
    -- "$PYTHON_BIN" -m server.ingest --reset-failed
}

# Build a deduplicated list of ``--property=ReadWritePaths=<dir>`` flags into
# the bash array named in $1. ReadWritePaths is space-separated in unit files,
# NOT colon-separated like $PATH — joining with ":" silently creates one
# invalid path and the sandbox blocks every write. Passing each directory as
# its own --property entry sidesteps the parsing rule entirely and also
# survives directory names that contain spaces.
_rw_property_args() {
  local -n _out_array="$1"
  shift
  _out_array=()
  local seen="" dir
  for dir in "$@"; do
    [[ -z "$dir" ]] && continue
    if [[ ":${seen}:" != *":${dir}:"* ]]; then
      _out_array+=( "--property=ReadWritePaths=${dir}" )
      seen+="${dir}:"
    fi
  done
}

backup_db() {
  resolve_python
  local db_dir
  db_dir="$(dirname "$HNREADER_DB_PATH")"

  local dst="${1:-}"
  local ts
  ts="$(date +%Y%m%d-%H%M%S)"
  if [[ -z "$dst" ]]; then
    dst="${db_dir}/backups/hnreader-${ts}.db"
  fi

  local dst_parent
  dst_parent="$(dirname "$dst")"
  run_root mkdir -p "$dst_parent"
  if id "$APP_USER" >/dev/null 2>&1; then
    run_root chown "${APP_USER}:${APP_GROUP}" "$dst_parent"
  fi

  local rw_args
  _rw_property_args rw_args "$db_dir" "$dst_parent"

  if [[ -f "${SYSTEMD_DIR}/${DB_INIT_SERVICE}" ]]; then
    # Online backup is safe even with ingest running, so no service stop.
    run_root systemd-run \
      --pty --wait --collect --quiet \
      --uid="$APP_USER" --gid="$APP_GROUP" \
      --working-directory="$PROJECT_DIR" \
      --property=EnvironmentFile="-${ENV_FILE}" \
      --property=ProtectSystem=strict \
      --property=ProtectHome=read-only \
      "${rw_args[@]}" \
      --property=NoNewPrivileges=yes \
      --property=PrivateTmp=yes \
      -- "$PYTHON_BIN" -m server.db_backup backup "$dst"
  else
    # No installed units yet (fresh dev box). Run inline; the DB file is
    # owned by the current user in that case.
    HNREADER_DB_PATH="$HNREADER_DB_PATH" \
      "$PYTHON_BIN" -m server.db_backup backup "$dst"
  fi

  echo "Backup written: ${dst}"
  echo "Recommended: verify the file integrity once more before relying on it:"
  echo "  bash server/launcher.sh verify-file ${dst}"
}

restore_db() {
  local src="${1:-}"
  if [[ -z "$src" ]]; then
    echo "ERROR: restore requires a backup file path." >&2
    echo "       Usage: bash server/launcher.sh restore <path>" >&2
    exit 1
  fi
  if [[ ! -f "$src" ]]; then
    echo "ERROR: backup file not found: ${src}" >&2
    exit 1
  fi

  resolve_python
  local db_dir
  db_dir="$(dirname "$HNREADER_DB_PATH")"
  local src_parent
  src_parent="$(cd "$(dirname "$src")" && pwd)"
  src="${src_parent}/$(basename "$src")"

  echo "About to restore ${HNREADER_DB_PATH} from ${src}."
  echo "Stopping ${INGEST_SERVICE} first..."
  if [[ -f "${SYSTEMD_DIR}/${INGEST_SERVICE}" ]]; then
    run_root systemctl stop "$INGEST_SERVICE" || true
  fi

  local rw_args
  _rw_property_args rw_args "$db_dir" "$src_parent"

  if [[ -f "${SYSTEMD_DIR}/${DB_INIT_SERVICE}" ]]; then
    run_root systemd-run \
      --pty --wait --collect --quiet \
      --uid="$APP_USER" --gid="$APP_GROUP" \
      --working-directory="$PROJECT_DIR" \
      --property=EnvironmentFile="-${ENV_FILE}" \
      --property=ProtectSystem=strict \
      --property=ProtectHome=read-only \
      "${rw_args[@]}" \
      --property=NoNewPrivileges=yes \
      --property=PrivateTmp=yes \
      -- "$PYTHON_BIN" -m server.db_backup restore "$src"
  else
    HNREADER_DB_PATH="$HNREADER_DB_PATH" \
      "$PYTHON_BIN" -m server.db_backup restore "$src"
  fi
}

verify_db() {
  resolve_python
  local db_dir
  db_dir="$(dirname "$HNREADER_DB_PATH")"
  local target="${1:-$HNREADER_DB_PATH}"
  local target_parent
  target_parent="$(cd "$(dirname "$target")" && pwd)"
  target="${target_parent}/$(basename "$target")"

  if [[ -f "${SYSTEMD_DIR}/${DB_INIT_SERVICE}" ]]; then
    local rw_args
    _rw_property_args rw_args "$db_dir" "$target_parent"
    run_root systemd-run \
      --pty --wait --collect --quiet \
      --uid="$APP_USER" --gid="$APP_GROUP" \
      --working-directory="$PROJECT_DIR" \
      --property=EnvironmentFile="-${ENV_FILE}" \
      --property=ProtectSystem=strict \
      --property=ProtectHome=read-only \
      "${rw_args[@]}" \
      --property=NoNewPrivileges=yes \
      --property=PrivateTmp=yes \
      -- env HNREADER_DB_PATH="$target" "$PYTHON_BIN" -m server.db_backup verify
  else
    HNREADER_DB_PATH="$target" \
      "$PYTHON_BIN" -m server.db_backup verify
  fi
}

ai_check() {
  run_python_module server.ops ai-check "${@:1}"
}

doctor() {
  run_python_module server.ops doctor "${@:1}"
}

repair() {
  run_python_module server.ops repair "${@:1}"
}

metrics() {
  run_python_module server.ingest --metrics
}

case "${1:-start}" in
  install|start)
    start_services
    ;;
  restart)
    restart_services
    ;;
  stop)
    stop_services
    ;;
  status)
    status_services
    ;;
  logs)
    logs_services
    ;;
  disable)
    disable_services
    ;;
  env)
    write_env_only
    ;;
  ai-check)
    shift
    ai_check "$@"
    ;;
  doctor)
    shift
    doctor "$@"
    ;;
  repair)
    shift
    repair "$@"
    ;;
  metrics)
    metrics
    ;;
  reset-failed)
    reset_failed_jobs
    ;;
  backup)
    backup_db "${2:-}"
    ;;
  restore)
    restore_db "${2:-}"
    ;;
  verify)
    verify_db
    ;;
  verify-file)
    verify_db "${2:-}"
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "Unknown command: ${1}" >&2
    usage
    exit 2
    ;;
esac

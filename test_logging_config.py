"""Tests for daily file logging configuration."""

from __future__ import annotations

import logging
import os
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch


class DailyLoggingConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = logging.getLogger()
        self.original_level = self.root.level
        self.original_handlers = list(self.root.handlers)
        self._clear_root_handlers()

    def _clear_root_handlers(self) -> None:
        for handler in list(self.root.handlers):
            self.root.removeHandler(handler)
            handler.close()

    def tearDown(self) -> None:
        self._clear_root_handlers()
        for handler in self.original_handlers:
            self.root.addHandler(handler)
        self.root.setLevel(self.original_level)

    def test_configure_logging_writes_to_daily_server_log(self) -> None:
        from .logging_config import configure_logging

        with tempfile.TemporaryDirectory() as tmp:
            try:
                log_path = configure_logging(log_dir=Path(tmp), verbose=False)

                logging.getLogger("server.test").info("daily log smoke")
                for handler in self.root.handlers:
                    handler.flush()

                self.assertEqual(log_path, Path(tmp) / "server.log")
                self.assertTrue(log_path.exists())
                self.assertIn("daily log smoke", log_path.read_text(encoding="utf-8"))
            finally:
                self._clear_root_handlers()

    def test_configure_logging_rotates_at_local_midnight_and_keeps_30_days(self) -> None:
        from logging.handlers import TimedRotatingFileHandler

        from .logging_config import configure_logging

        with tempfile.TemporaryDirectory() as tmp:
            try:
                configure_logging(log_dir=Path(tmp), verbose=True)

                daily_handlers = [
                    h
                    for h in self.root.handlers
                    if isinstance(h, TimedRotatingFileHandler)
                ]

                self.assertEqual(len(daily_handlers), 1)
                handler = daily_handlers[0]
                self.assertEqual(handler.when, "MIDNIGHT")
                self.assertEqual(handler.backupCount, 30)
                self.assertFalse(handler.utc)
            finally:
                self._clear_root_handlers()

    def test_configure_logging_removes_rotated_logs_older_than_30_days(self) -> None:
        from .logging_config import configure_logging

        today = date.today()
        old_day = today - timedelta(days=31)
        recent_day = today - timedelta(days=29)

        with tempfile.TemporaryDirectory() as tmp:
            try:
                log_dir = Path(tmp)
                old_log = log_dir / f"server.log.{old_day:%Y-%m-%d}"
                recent_log = log_dir / f"server.log.{recent_day:%Y-%m-%d}"
                old_log.write_text("old", encoding="utf-8")
                recent_log.write_text("recent", encoding="utf-8")

                configure_logging(log_dir=log_dir)

                self.assertFalse(old_log.exists())
                self.assertTrue(recent_log.exists())
            finally:
                self._clear_root_handlers()

    def test_configure_logging_uses_env_log_dir_when_not_explicit(self) -> None:
        from .logging_config import configure_logging

        with tempfile.TemporaryDirectory() as tmp:
            try:
                with patch.dict(os.environ, {"HNREADER_LOG_DIR": tmp}):
                    log_path = configure_logging()

                self.assertEqual(log_path, Path(tmp) / "server.log")
            finally:
                self._clear_root_handlers()

    def test_launcher_whitelists_log_dir_for_systemd_sandbox(self) -> None:
        launcher = Path(__file__).with_name("launcher.sh").read_text(encoding="utf-8")

        self.assertIn(
            'HNREADER_LOG_DIR="${HNREADER_LOG_DIR:-${SERVER_DIR}/logs}"',
            launcher,
        )
        self.assertIn('$(env_line HNREADER_LOG_DIR "$HNREADER_LOG_DIR")', launcher)
        self.assertIn(
            'run_root mkdir -p "$(dirname "$HNREADER_DB_PATH")" '
            '"$HNREADER_LOG_DIR" "$ENV_DIR"',
            launcher,
        )
        self.assertGreaterEqual(
            launcher.count("ReadWritePaths=${db_dir} ${HNREADER_LOG_DIR}"),
            2,
        )
        self.assertIn('_rw_property_args rw_args "$db_dir" "$HNREADER_LOG_DIR"', launcher)


class LauncherUbuntuBootstrapContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.launcher = Path(__file__).with_name("launcher.sh").read_text(
            encoding="utf-8"
        )

    def test_default_command_runs_ubuntu_bootstrap(self) -> None:
        self.assertIn("bootstrap     guided first-run setup for Ubuntu 22.04", self.launcher)
        self.assertIn('case "${1:-bootstrap}" in', self.launcher)
        self.assertIn("bootstrap)\n    bootstrap_ubuntu22", self.launcher)

    def test_bootstrap_installs_ubuntu_system_dependencies_and_venv(self) -> None:
        self.assertIn("bootstrap_ubuntu22()", self.launcher)
        self.assertIn("require_ubuntu_2204", self.launcher)
        self.assertIn("install_system_dependencies", self.launcher)
        self.assertIn("apt-get update", self.launcher)
        self.assertIn(
            "apt-get install -y python3 python3-venv python3-pip git curl ufw sqlite3 acl",
            self.launcher,
        )
        self.assertIn("ensure_virtualenv", self.launcher)
        self.assertIn('"$PYTHON_BIN" -m pip install -r "$REQUIREMENTS_FILE" -c "$CONSTRAINTS_FILE"', self.launcher)
        self.assertIn("ensure_project_read_access", self.launcher)

    def test_bootstrap_guides_required_configuration_into_env_local(self) -> None:
        self.assertIn("run_interactive_config_wizard()", self.launcher)
        self.assertIn("HNREADER_AI_PROVIDER=none", self.launcher)
        self.assertIn("HNREADER_CLOUD_SYNC_ENABLED=1", self.launcher)
        self.assertIn("HNREADER_CLOUD_PUSH_URL=", self.launcher)
        self.assertIn("HNREADER_CLOUD_PUSH_SECRET=", self.launcher)
        self.assertIn("HNREADER_ADMIN_EMAIL_ENABLED=false", self.launcher)
        self.assertIn("chmod 600 \"$PROJECT_ENV_FILE\"", self.launcher)

    def test_launcher_supports_direct_server_checkout_layout(self) -> None:
        self.assertIn("SERVER_DIR=", self.launcher)
        self.assertIn("SERVER_MODULE=", self.launcher)
        self.assertIn("REQUIREMENTS_FILE=", self.launcher)
        self.assertIn("CONSTRAINTS_FILE=", self.launcher)
        self.assertIn("PYTHONPATH_ROOT=", self.launcher)
        self.assertIn("Environment=PYTHONPATH=${PYTHONPATH_ROOT}", self.launcher)
        self.assertIn("ExecStart=${PYTHON_BIN} -m ${SERVER_MODULE}.ingest --loop", self.launcher)


if __name__ == "__main__":
    unittest.main()

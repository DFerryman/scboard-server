from __future__ import annotations

import sqlite3
import json
import os
import stat
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import migration


class MigrationArchiveTest(unittest.TestCase):
    def _make_project(self, root: Path) -> None:
        (root / "ingest.py").write_text("# marker\n", encoding="utf-8")
        (root / "__init__.py").write_text("", encoding="utf-8")
        (root / "launcher.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    def _make_db(self, path: Path, marker: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE marker(value TEXT NOT NULL)")
            conn.execute("INSERT INTO marker(value) VALUES (?)", (marker,))
            conn.commit()
        finally:
            conn.close()

    def _read_db_marker(self, path: Path) -> str:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = conn.execute("SELECT value FROM marker").fetchone()
        finally:
            conn.close()
        return row[0]

    def _rewrite_archive_member(
        self,
        archive: Path,
        member_name: str,
        replacement: bytes,
    ) -> None:
        rewritten = archive.with_name(archive.name + ".rewritten")
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-rewrite-") as tmp:
            extracted = Path(tmp)
            with tarfile.open(archive, "r:gz") as tf:
                for member in tf.getmembers():
                    target = extracted / member.name
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    src = tf.extractfile(member)
                    self.assertIsNotNone(src)
                    with src, target.open("wb") as dst:
                        dst.write(src.read())
            target = extracted / member_name
            target.write_bytes(replacement)
            with tarfile.open(rewritten, "w:gz") as tf:
                for child in sorted(extracted.iterdir()):
                    tf.add(child, arcname=child.name)
        rewritten.replace(archive)

    def _rewrite_manifest(self, archive: Path, rewrite) -> None:
        with tarfile.open(archive, "r:gz") as tf:
            manifest_file = tf.extractfile("hnreader-migration-manifest.json")
            self.assertIsNotNone(manifest_file)
            manifest = json.loads(manifest_file.read().decode("utf-8"))
        rewrite(manifest)
        self._rewrite_archive_member(
            archive,
            "hnreader-migration-manifest.json",
            (json.dumps(manifest, ensure_ascii=False) + "\n").encode("utf-8"),
        )

    def test_export_packages_runtime_state_with_self_contained_db(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as tmp:
            project = Path(tmp)
            self._make_project(project)
            (project / ".env.local").write_text(
                "HNREADER_AI_PROVIDER=none\n", encoding="utf-8"
            )
            (project / "logs").mkdir()
            (project / "logs" / "server.log").write_text("log\n", encoding="utf-8")
            self._make_db(project / "data" / "hnreader.db", "from-export")

            archive = project / "migration.tar.gz"

            manifest = migration.export_archive(
                archive,
                project_dir=project,
                include_service_env=False,
            )

            self.assertEqual(manifest["format"], "hnreader-migration-v1")
            with tarfile.open(archive, "r:gz") as tf:
                names = set(tf.getnames())

            self.assertIn("hnreader-migration-manifest.json", names)
            self.assertIn("payload/.env.local", names)
            self.assertIn("payload/data/hnreader.db", names)
            self.assertIn("payload/logs/server.log", names)
            self.assertNotIn("payload/data/hnreader.db-wal", names)
            self.assertNotIn("payload/data/hnreader.db-shm", names)

            with tempfile.TemporaryDirectory(prefix="hnreader-migration-check-") as out:
                out_db = Path(out) / "hnreader.db"
                with tarfile.open(archive, "r:gz") as tf:
                    db_member = tf.extractfile("payload/data/hnreader.db")
                    self.assertIsNotNone(db_member)
                    out_db.write_bytes(db_member.read())
                self.assertEqual(self._read_db_marker(out_db), "from-export")

    @unittest.skipIf(os.name == "nt", "POSIX mode bits required for this test")
    def test_export_archive_is_owner_only_because_it_contains_secrets(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as tmp:
            project = Path(tmp)
            self._make_project(project)
            (project / ".env.local").write_text(
                "HNREADER_CLOUD_PUSH_SECRET=secret\n", encoding="utf-8"
            )

            archive = project / "migration.tar.gz"
            migration.export_archive(
                archive,
                project_dir=project,
                include_service_env=False,
            )

            self.assertEqual(stat.S_IMODE(archive.stat().st_mode), 0o600)

    @unittest.skipIf(os.name == "nt", "POSIX mode bits required for this test")
    def test_import_env_files_are_owner_only(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as src_tmp:
            source = Path(src_tmp)
            self._make_project(source)
            (source / ".env.local").write_text(
                "HNREADER_CLOUD_PUSH_SECRET=secret\n", encoding="utf-8"
            )
            archive = source / "migration.tar.gz"
            migration.export_archive(
                archive,
                project_dir=source,
                include_service_env=False,
            )

            with tempfile.TemporaryDirectory(prefix="hnreader-migration-dst-") as dst_tmp:
                dest = Path(dst_tmp)
                self._make_project(dest)
                migration.import_archive(
                    archive,
                    project_dir=dest,
                    include_service_env=False,
                    assume_yes=True,
                    stop_services=False,
                )

                self.assertEqual(
                    stat.S_IMODE((dest / ".env.local").stat().st_mode),
                    0o600,
                )

    def test_import_replaces_current_runtime_state_instead_of_merging(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as src_tmp:
            source = Path(src_tmp)
            self._make_project(source)
            (source / ".env.local").write_text(
                "HNREADER_AI_PROVIDER=none\n", encoding="utf-8"
            )
            (source / "logs").mkdir()
            (source / "logs" / "server.log").write_text("new log\n", encoding="utf-8")
            (source / "data").mkdir()
            (source / "data" / "cloud.json").write_text("new\n", encoding="utf-8")
            self._make_db(source / "data" / "hnreader.db", "new-db")

            archive = source / "migration.tar.gz"
            migration.export_archive(
                archive,
                project_dir=source,
                include_service_env=False,
            )

            with tempfile.TemporaryDirectory(prefix="hnreader-migration-dst-") as dst_tmp:
                dest = Path(dst_tmp)
                self._make_project(dest)
                (dest / ".env.local").write_text("STALE=1\n", encoding="utf-8")
                (dest / ".env.old.local").write_text("STALE=1\n", encoding="utf-8")
                (dest / "data").mkdir()
                (dest / "data" / "stale.txt").write_text("stale\n", encoding="utf-8")
                (dest / "logs").mkdir()
                (dest / "logs" / "old.log").write_text("stale\n", encoding="utf-8")

                migration.import_archive(
                    archive,
                    project_dir=dest,
                    include_service_env=False,
                    assume_yes=True,
                    stop_services=False,
                )

                self.assertEqual(
                    (dest / ".env.local").read_text(encoding="utf-8"),
                    "HNREADER_AI_PROVIDER=none\n",
                )
                self.assertFalse((dest / ".env.old.local").exists())
                self.assertFalse((dest / "data" / "stale.txt").exists())
                self.assertEqual(
                    (dest / "data" / "cloud.json").read_text(encoding="utf-8"),
                    "new\n",
                )
                self.assertEqual(
                    (dest / "logs" / "server.log").read_text(encoding="utf-8"),
                    "new log\n",
                )
                self.assertFalse((dest / "logs" / "old.log").exists())
                self.assertEqual(
                    self._read_db_marker(dest / "data" / "hnreader.db"),
                    "new-db",
                )

    def test_service_env_can_be_exported_and_replaced_explicitly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as src_tmp:
            source = Path(src_tmp)
            self._make_project(source)
            service_env = source / "server.env"
            service_env.write_text(
                'HNREADER_DB_PATH="/srv/hnreader/data/hnreader.db"\n',
                encoding="utf-8",
            )

            archive = source / "migration.tar.gz"
            migration.export_archive(
                archive,
                project_dir=source,
                service_env_file=service_env,
                include_service_env=True,
            )

            with tempfile.TemporaryDirectory(prefix="hnreader-migration-dst-") as dst_tmp:
                dest = Path(dst_tmp)
                self._make_project(dest)
                imported_service_env = dest / "server.env"
                imported_service_env.write_text("STALE=1\n", encoding="utf-8")

                migration.import_archive(
                    archive,
                    project_dir=dest,
                    service_env_file=imported_service_env,
                    include_service_env=True,
                    assume_yes=True,
                    stop_services=False,
                )

                self.assertEqual(
                    imported_service_env.read_text(encoding="utf-8"),
                    'HNREADER_DB_PATH="/srv/hnreader/data/hnreader.db"\n',
                )

    def test_export_includes_runtime_path_overrides_outside_db_dir(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as src_tmp:
            source = Path(src_tmp)
            self._make_project(source)
            self._make_db(source / "data" / "hnreader.db", "db")
            (source / ".env.local").write_text(
                "\n".join(
                    [
                        "HNREADER_CLOUD_SYNC_OUTPUT_DIR=cloud-out",
                        "HNREADER_ALERT_OUTBOX_PATH=alerts/outbox.jsonl",
                        "HNREADER_AI_CONFIG_STATUS_CACHE_PATH=ai/status.json",
                        "HNREADER_AI_CONFIG_FILE=ai/provider.json",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (source / "cloud-out").mkdir()
            (source / "cloud-out" / "stories.jsonl").write_text(
                "{}\n", encoding="utf-8"
            )
            (source / "alerts").mkdir()
            (source / "alerts" / "outbox.jsonl").write_text(
                "{}\n", encoding="utf-8"
            )
            (source / "ai").mkdir()
            (source / "ai" / "status.json").write_text("{}", encoding="utf-8")
            (source / "ai" / "provider.json").write_text(
                '{"provider":"none"}', encoding="utf-8"
            )

            archive = source / "migration.tar.gz"
            manifest = migration.export_archive(
                archive,
                project_dir=source,
                include_service_env=False,
            )

            kinds = {entry["kind"] for entry in manifest["entries"]}
            self.assertIn("cloud_sync_output_dir", kinds)
            self.assertIn("alert_outbox_file", kinds)
            self.assertIn("ai_status_cache_file", kinds)
            self.assertIn("ai_config_file", kinds)

            with tarfile.open(archive, "r:gz") as tf:
                names = set(tf.getnames())
            self.assertIn("payload/cloud-out/stories.jsonl", names)
            self.assertIn("payload/alerts/outbox.jsonl", names)
            self.assertIn("payload/ai/status.json", names)
            self.assertIn("payload/ai/provider.json", names)

    def test_import_replaces_runtime_path_overrides(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as src_tmp:
            source = Path(src_tmp)
            self._make_project(source)
            self._make_db(source / "data" / "hnreader.db", "db")
            env_text = (
                "HNREADER_CLOUD_SYNC_OUTPUT_DIR=cloud-out\n"
                "HNREADER_ALERT_OUTBOX_PATH=alerts/outbox.jsonl\n"
                "HNREADER_AI_CONFIG_STATUS_CACHE_PATH=ai/status.json\n"
                "HNREADER_AI_CONFIG_FILE=ai/provider.json\n"
            )
            (source / ".env.local").write_text(env_text, encoding="utf-8")
            (source / "cloud-out").mkdir()
            (source / "cloud-out" / "stories.jsonl").write_text(
                "new\n", encoding="utf-8"
            )
            (source / "alerts").mkdir()
            (source / "alerts" / "outbox.jsonl").write_text(
                "new-alert\n", encoding="utf-8"
            )
            (source / "ai").mkdir()
            (source / "ai" / "status.json").write_text(
                "new-status", encoding="utf-8"
            )
            (source / "ai" / "provider.json").write_text(
                "new-provider", encoding="utf-8"
            )
            archive = source / "migration.tar.gz"
            migration.export_archive(
                archive,
                project_dir=source,
                include_service_env=False,
            )

            with tempfile.TemporaryDirectory(prefix="hnreader-migration-dst-") as dst_tmp:
                dest = Path(dst_tmp)
                self._make_project(dest)
                (dest / ".env.local").write_text(env_text, encoding="utf-8")
                (dest / "cloud-out").mkdir()
                (dest / "cloud-out" / "stale.jsonl").write_text(
                    "stale\n", encoding="utf-8"
                )
                (dest / "alerts").mkdir()
                (dest / "alerts" / "outbox.jsonl").write_text(
                    "stale-alert\n", encoding="utf-8"
                )
                (dest / "ai").mkdir()
                (dest / "ai" / "status.json").write_text(
                    "stale-status", encoding="utf-8"
                )
                (dest / "ai" / "provider.json").write_text(
                    "stale-provider", encoding="utf-8"
                )

                migration.import_archive(
                    archive,
                    project_dir=dest,
                    include_service_env=False,
                    assume_yes=True,
                    stop_services=False,
                )

                self.assertFalse((dest / "cloud-out" / "stale.jsonl").exists())
                self.assertEqual(
                    (dest / "cloud-out" / "stories.jsonl").read_text(
                        encoding="utf-8"
                    ),
                    "new\n",
                )
                self.assertEqual(
                    (dest / "alerts" / "outbox.jsonl").read_text(
                        encoding="utf-8"
                    ),
                    "new-alert\n",
                )
                self.assertEqual(
                    (dest / "ai" / "status.json").read_text(encoding="utf-8"),
                    "new-status",
                )
                self.assertEqual(
                    (dest / "ai" / "provider.json").read_text(encoding="utf-8"),
                    "new-provider",
                )

    def test_import_rejects_corrupt_archived_db_before_removing_current_state(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as src_tmp:
            source = Path(src_tmp)
            self._make_project(source)
            self._make_db(source / "data" / "hnreader.db", "new-db")
            archive = source / "migration.tar.gz"
            migration.export_archive(
                archive,
                project_dir=source,
                include_service_env=False,
            )
            self._rewrite_archive_member(
                archive,
                "payload/data/hnreader.db",
                b"not a sqlite database",
            )

            with tempfile.TemporaryDirectory(prefix="hnreader-migration-dst-") as dst_tmp:
                dest = Path(dst_tmp)
                self._make_project(dest)
                self._make_db(dest / "data" / "hnreader.db", "current-db")

                with self.assertRaises(SystemExit) as ctx:
                    migration.import_archive(
                        archive,
                        project_dir=dest,
                        include_service_env=False,
                        assume_yes=True,
                        stop_services=False,
                    )

                self.assertIn("integrity_check failed", str(ctx.exception))
                self.assertEqual(
                    self._read_db_marker(dest / "data" / "hnreader.db"),
                    "current-db",
                )

    def test_import_rejects_manifest_absolute_target_not_declared_by_env(self) -> None:
        with tempfile.TemporaryDirectory(prefix="hnreader-migration-src-") as src_tmp:
            source = Path(src_tmp)
            self._make_project(source)
            self._make_db(source / "data" / "hnreader.db", "new-db")
            archive = source / "migration.tar.gz"
            migration.export_archive(
                archive,
                project_dir=source,
                include_service_env=False,
            )

            with tempfile.TemporaryDirectory(prefix="hnreader-migration-victim-") as victim_tmp:
                victim = Path(victim_tmp) / "victim-dir"
                victim.mkdir()
                (victim / "keep.txt").write_text("keep", encoding="utf-8")

                def _point_data_dir_elsewhere(manifest):
                    for entry in manifest["entries"]:
                        if entry["kind"] == "data_dir":
                            entry["target_type"] = "absolute"
                            entry["target"] = str(victim)

                self._rewrite_manifest(archive, _point_data_dir_elsewhere)

                with tempfile.TemporaryDirectory(prefix="hnreader-migration-dst-") as dst_tmp:
                    dest = Path(dst_tmp)
                    self._make_project(dest)
                    self._make_db(dest / "data" / "hnreader.db", "current-db")

                    with self.assertRaises(SystemExit) as ctx:
                        migration.import_archive(
                            archive,
                            project_dir=dest,
                            include_service_env=False,
                            assume_yes=True,
                            stop_services=False,
                        )

                    self.assertIn("absolute target is not declared", str(ctx.exception))
                    self.assertEqual(
                        (victim / "keep.txt").read_text(encoding="utf-8"),
                        "keep",
                    )
                    self.assertEqual(
                        self._read_db_marker(dest / "data" / "hnreader.db"),
                        "current-db",
                    )

    def test_stop_services_aborts_when_loaded_unit_cannot_stop(self) -> None:
        with patch("migration.os.name", "posix"), patch(
            "migration.shutil.which", return_value="/usr/bin/systemctl"
        ), patch("migration._systemctl_unit_exists", return_value=True), patch(
            "migration._run_root", return_value=1
        ):
            with self.assertRaises(SystemExit) as ctx:
                migration._stop_services("hnreader")

        self.assertIn("failed to stop", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

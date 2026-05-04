from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pipeline.runtime_rollback as runtime_rollback
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from run import main as run_main
from run import run_rollback_runtime_scoring


class RuntimeRollbackTests(unittest.TestCase):
    def test_rollback_restores_runtime_scoring_and_preserves_proxy_config(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            config_path = repo_root / "config.yaml"
            dump_yaml_file(
                config_path,
                {
                    "proxy_scanner": {"cost_gates": {"inspect_min_score": 0.40}},
                    "runtime_analysis": {
                        "scoring": {
                            "event_weights": {
                                "medal_seen": 0.45,
                                "ability_seen": 0.18,
                                "pov_character_identified": 0.08,
                            },
                            "event_caps": {
                                "medal_seen": 2,
                                "ability_seen": 3,
                                "pov_character_identified": 1,
                            },
                            "detection_support_weight": 0.03,
                            "max_detection_support": 0.12,
                            "action_thresholds": {"inspect": 0.25, "highlight_candidate": 0.45},
                        }
                    },
                },
            )

            snapshot_dir = root / "snapshot"
            self._write_snapshot(
                snapshot_dir,
                previous_scoring={"action_thresholds": {"highlight_candidate": 0.60}},
                applied_scoring={"action_thresholds": {"highlight_candidate": 0.45}},
            )
            history_root = root / "history"

            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_rollback, "DEFAULT_HISTORY_ROOT", history_root):
                result = run_rollback_runtime_scoring(snapshot_dir)

            self.assertTrue(result["ok"])
            updated = load_yaml_file(config_path)
            self.assertEqual(updated["proxy_scanner"]["cost_gates"]["inspect_min_score"], 0.40)
            self.assertEqual(updated["runtime_analysis"]["scoring"]["action_thresholds"]["highlight_candidate"], 0.60)
            self.assertTrue(Path(result["rollback_snapshot_paths"]["snapshot_dir"]).is_dir())
            self.assertTrue(Path(result["rollback_snapshot_paths"]["rollback_record_path"]).is_file())

    def test_rollback_materializes_config_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            snapshot_dir = root / "snapshot"
            self._write_snapshot(
                snapshot_dir,
                previous_scoring={"action_thresholds": {"highlight_candidate": 0.60}},
                applied_scoring={"action_thresholds": {"highlight_candidate": 0.45}},
            )
            history_root = root / "history"

            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_rollback, "DEFAULT_HISTORY_ROOT", history_root):
                result = run_rollback_runtime_scoring(snapshot_dir)

            self.assertTrue(result["ok"])
            config_path = repo_root / "config.yaml"
            self.assertTrue(config_path.is_file())
            updated = load_yaml_file(config_path)
            self.assertEqual(updated["runtime_analysis"]["scoring"]["action_thresholds"]["highlight_candidate"], 0.60)

    def test_rollback_fails_for_missing_snapshot_files(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            snapshot_dir = root / "snapshot"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            result = run_rollback_runtime_scoring(snapshot_dir)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "missing_snapshot_file")

    def test_rollback_fails_for_invalid_snapshot_dir_or_payload(self) -> None:
        invalid_dir_result = run_rollback_runtime_scoring("/tmp/does-not-exist-runtime-rollback")
        self.assertFalse(invalid_dir_result["ok"])
        self.assertEqual(invalid_dir_result["status"], "invalid_snapshot_dir")

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            snapshot_dir = root / "snapshot"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            (snapshot_dir / "previous_scoring.yaml").write_text("- not-a-dict\n", encoding="utf-8")
            (snapshot_dir / "applied_scoring.yaml").write_text("{}\n", encoding="utf-8")
            (snapshot_dir / "promotion_record.json").write_text(json.dumps({"trial_name": "trial-a"}), encoding="utf-8")
            result = run_rollback_runtime_scoring(snapshot_dir)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_restore_payload")

    def test_rollback_snapshot_is_written_before_config_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            config_path = repo_root / "config.yaml"
            dump_yaml_file(config_path, {"proxy_scanner": {"cost_gates": {"inspect_min_score": 0.40}}})
            snapshot_dir = root / "snapshot"
            self._write_snapshot(
                snapshot_dir,
                previous_scoring={"action_thresholds": {"highlight_candidate": 0.60}},
                applied_scoring={"action_thresholds": {"highlight_candidate": 0.45}},
            )
            history_root = root / "history"
            original_dump = runtime_rollback.dump_yaml_file

            def failing_dump(path: str | Path, data: object) -> None:
                if Path(path).resolve() == config_path.resolve():
                    raise OSError("boom")
                original_dump(path, data)

            before = config_path.read_text(encoding="utf-8")
            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_rollback, "DEFAULT_HISTORY_ROOT", history_root), patch.object(runtime_rollback, "dump_yaml_file", side_effect=failing_dump):
                result = run_rollback_runtime_scoring(snapshot_dir)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "config_write_failed")
            self.assertTrue(Path(result["rollback_snapshot_paths"]["snapshot_dir"]).is_dir())
            self.assertEqual(config_path.read_text(encoding="utf-8"), before)

    def test_cli_routes_to_runtime_rollback(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--rollback-runtime-scoring",
                "/tmp/snapshot-dir",
                "--rollback-name",
                "rollback-a",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_rollback_runtime_scoring",
                return_value={"ok": True, "config_changed": True, "restore_source": "/tmp/snapshot-dir"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/snapshot-dir",
                output_path=None,
                debug_output_dir=None,
                rollback_name="rollback-a",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def _write_snapshot(self, snapshot_dir: Path, *, previous_scoring: dict[str, object], applied_scoring: dict[str, object]) -> None:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        dump_yaml_file(snapshot_dir / "previous_scoring.yaml", previous_scoring)
        dump_yaml_file(snapshot_dir / "applied_scoring.yaml", applied_scoring)
        (snapshot_dir / "promotion_record.json").write_text(
            json.dumps({"trial_name": "trial-a", "config_path": "/tmp/config.yaml"}, indent=2),
            encoding="utf-8",
        )


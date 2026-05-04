from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.evaluation_fixtures import EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION
from pipeline.fixture_source_manifest import (
    FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION,
    load_fixture_source_manifest,
)
from run import main as run_main
from run import run_compare_fixture_trials, run_fixture_trial


def _write_fixture_manifest(path: Path, fixture_ids: list[str]) -> Path:
    fixtures = []
    for fixture_id in fixture_ids:
        fixtures.append(
            {
                "fixture_id": fixture_id,
                "label": fixture_id,
                "task_intent": "test",
                "expected_review_outcome": "approved",
                "latency_budget_class": "smoke",
                "expected_artifacts": {"proxy": True, "runtime": True, "fused": True},
                "artifact_refs": {},
            }
        )
    path.write_text(
        json.dumps(
            {
                "schema_version": EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION,
                "fixtures": fixtures,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_source_manifest(path: Path, rows: list[dict[str, object]]) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION,
                "fixtures": rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


class FixtureSourceManifestTests(unittest.TestCase):
    def test_load_fixture_source_manifest_rejects_missing_fixture_id(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "fixture_sources.json"
            _write_source_manifest(
                path,
                [
                    {
                        "game": "marvel_rivals",
                        "source_path": "./clip.mp4",
                    }
                ],
            )
            with self.assertRaises(ValueError):
                load_fixture_source_manifest(path)

    def test_load_fixture_source_manifest_rejects_missing_game(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "fixture_sources.json"
            _write_source_manifest(
                path,
                [
                    {
                        "fixture_id": "fixture-a",
                        "source_path": "./clip.mp4",
                    }
                ],
            )
            with self.assertRaises(ValueError):
                load_fixture_source_manifest(path)

    def test_load_fixture_source_manifest_rejects_missing_source_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "fixture_sources.json"
            _write_source_manifest(
                path,
                [
                    {
                        "fixture_id": "fixture-a",
                        "game": "marvel_rivals",
                    }
                ],
            )
            with self.assertRaises(ValueError):
                load_fixture_source_manifest(path)

    def test_load_fixture_source_manifest_rejects_duplicate_fixture_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "fixture_sources.json"
            _write_source_manifest(
                path,
                [
                    {
                        "fixture_id": "fixture-a",
                        "game": "marvel_rivals",
                        "source_path": "./a.mp4",
                    },
                    {
                        "fixture_id": "fixture-a",
                        "game": "marvel_rivals",
                        "source_path": "./b.mp4",
                    },
                ],
            )
            with self.assertRaises(ValueError):
                load_fixture_source_manifest(path)


class FixtureTrialRunnerTests(unittest.TestCase):
    def test_run_fixture_trial_uses_baseline_preset_and_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = _write_fixture_manifest(root / "fixtures.json", ["fixture-a"])
            source_manifest = _write_source_manifest(
                root / "fixture_sources.json",
                [
                    {
                        "fixture_id": "fixture-a",
                        "game": "marvel_rivals",
                        "source_path": "./fixture-a.mp4",
                        "produce_layers": {"proxy": True, "runtime": True, "fused": True},
                    }
                ],
            )
            captured_configs: list[dict[str, object]] = []

            def _scan_stub(source: str, game: str, *, config: dict, config_warnings: list[dict], chat_log: str | None = None) -> dict:
                captured_configs.append(config)
                sidecar_path = Path(config["proxy_scanner"]["sidecar"]["output_dir"]) / game / str(
                    config["proxy_scanner"]["sidecar"]["filename_override"]
                )
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.write_text("{}", encoding="utf-8")
                return {"ok": True, "status": "ok", "sidecar_path": str(sidecar_path)}

            with patch("run._scan_vod_with_config", side_effect=_scan_stub):
                result = run_fixture_trial(
                    fixture_manifest,
                    fixture_source_manifest=source_manifest,
                    trial_name="baseline",
                    output_root=root / "runs",
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["effective_overrides"]["proposal_backend"], "transnetv2")
            self.assertEqual(result["effective_overrides"]["asr_backend"], "whisper")
            self.assertTrue(Path(result["manifest_path"]).is_file())
            self.assertEqual(result["completed_fixture_count"], 1)
            self.assertEqual(
                captured_configs[0]["proxy_scanner"]["sources"]["hf_multimodal"]["components"]["shot_detector"]["runtime_options"]["proposal_backend"],
                "transnetv2",
            )
            self.assertEqual(
                captured_configs[0]["proxy_scanner"]["sources"]["hf_multimodal"]["components"]["asr"]["runtime_options"]["asr_backend"],
                "whisper",
            )
            self.assertTrue(
                result["fixtures"][0]["layers"]["proxy"]["sidecar_path"].endswith("fixture-a.proxy_scan.json")
            )

    def test_run_fixture_trial_applies_combined_preset_and_explicit_override(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = _write_fixture_manifest(root / "fixtures.json", ["fixture-a"])
            source_manifest = _write_source_manifest(
                root / "fixture_sources.json",
                [{"fixture_id": "fixture-a", "game": "marvel_rivals", "source_path": "./fixture-a.mp4"}],
            )
            captured_configs: list[dict[str, object]] = []

            def _scan_stub(source: str, game: str, *, config: dict, config_warnings: list[dict], chat_log: str | None = None) -> dict:
                captured_configs.append(config)
                sidecar_path = Path(config["proxy_scanner"]["sidecar"]["output_dir"]) / game / str(
                    config["proxy_scanner"]["sidecar"]["filename_override"]
                )
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.write_text("{}", encoding="utf-8")
                return {"ok": True, "status": "ok", "sidecar_path": str(sidecar_path)}

            with patch("run._scan_vod_with_config", side_effect=_scan_stub):
                result = run_fixture_trial(
                    fixture_manifest,
                    fixture_source_manifest=source_manifest,
                    trial_name="cheap-stage-combined",
                    proposal_backend="transnetv2",
                    output_root=root / "runs",
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["effective_overrides"]["proposal_backend"], "transnetv2")
            self.assertEqual(result["effective_overrides"]["asr_backend"], "distil_whisper")
            self.assertEqual(
                captured_configs[0]["proxy_scanner"]["sources"]["hf_multimodal"]["components"]["shot_detector"]["runtime_options"]["proposal_backend"],
                "transnetv2",
            )
            self.assertEqual(
                captured_configs[0]["proxy_scanner"]["sources"]["hf_multimodal"]["components"]["asr"]["runtime_options"]["asr_backend"],
                "distil_whisper",
            )

    def test_run_fixture_trial_honors_pattern_limit_and_continues_on_missing_source(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = _write_fixture_manifest(root / "fixtures.json", ["fixture-a", "fixture-b", "fixture-c"])
            source_manifest = _write_source_manifest(
                root / "fixture_sources.json",
                [
                    {"fixture_id": "fixture-a", "game": "marvel_rivals", "source_path": "./fixture-a.mp4"},
                    {"fixture_id": "fixture-b", "game": "marvel_rivals", "source_path": "./fixture-b.mp4"},
                ],
            )

            def _scan_stub(source: str, game: str, *, config: dict, config_warnings: list[dict], chat_log: str | None = None) -> dict:
                fixture_filename = str(config["proxy_scanner"]["sidecar"]["filename_override"])
                sidecar_path = Path(config["proxy_scanner"]["sidecar"]["output_dir"]) / game / fixture_filename
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.write_text("{}", encoding="utf-8")
                return {"ok": True, "status": "ok", "sidecar_path": str(sidecar_path)}

            with patch("run._scan_vod_with_config", side_effect=_scan_stub):
                result = run_fixture_trial(
                    fixture_manifest,
                    fixture_source_manifest=source_manifest,
                    trial_name="baseline",
                    output_root=root / "runs",
                    pattern="fixture-*",
                    limit=3,
                )

            self.assertFalse(result["ok"])
            self.assertEqual(result["failed_fixture_count"], 1)
            failed_rows = [row for row in result["fixtures"] if row["status"] == "failed"]
            self.assertEqual(failed_rows[0]["failure_reason"], "missing_source_fixture")

    def test_run_fixture_trial_emits_runtime_and_fused_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = _write_fixture_manifest(root / "fixtures.json", ["fixture-a"])
            source_manifest = _write_source_manifest(
                root / "fixture_sources.json",
                [
                    {
                        "fixture_id": "fixture-a",
                        "game": "marvel_rivals",
                        "source_path": "./fixture-a.mp4",
                        "produce_layers": {"proxy": True, "runtime": True, "fused": True},
                    }
                ],
            )

            def _scan_stub(source: str, game: str, *, config: dict, config_warnings: list[dict], chat_log: str | None = None) -> dict:
                sidecar_path = Path(config["proxy_scanner"]["sidecar"]["output_dir"]) / game / str(
                    config["proxy_scanner"]["sidecar"]["filename_override"]
                )
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.write_text("{}", encoding="utf-8")
                return {"ok": True, "status": "ok", "sidecar_path": str(sidecar_path)}

            def _runtime_stub(source: str, game: str, *, output_path: str | Path | None = None, **_: object) -> dict:
                assert output_path is not None
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text("{}", encoding="utf-8")
                return {"ok": True, "status": "ok", "sidecar_path": str(output_file)}

            def _fused_stub(source: str, game: str, *, output_path: str | Path | None = None, **_: object) -> dict:
                assert output_path is not None
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text("{}", encoding="utf-8")
                return {"ok": True, "status": "ok", "sidecar_path": str(output_file)}

            with (
                patch("run._scan_vod_with_config", side_effect=_scan_stub),
                patch("run.run_analyze_roi_runtime", side_effect=_runtime_stub) as runtime_mock,
                patch("run.run_fuse_clip_signals", side_effect=_fused_stub) as fused_mock,
            ):
                result = run_fixture_trial(
                    fixture_manifest,
                    fixture_source_manifest=source_manifest,
                    trial_name="baseline",
                    output_root=root / "runs",
                    emit_runtime=True,
                    emit_fused=True,
                )

            self.assertTrue(result["ok"])
            runtime_mock.assert_called_once()
            fused_mock.assert_called_once()
            self.assertTrue(result["fixtures"][0]["layers"]["runtime"]["requested"])
            self.assertTrue(result["fixtures"][0]["layers"]["fused"]["requested"])

    def test_run_compare_fixture_trials_uses_run_manifest_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            baseline_root.mkdir(parents=True, exist_ok=True)
            trial_root.mkdir(parents=True, exist_ok=True)
            (baseline_root / "fixture_trial_run_manifest.json").write_text(
                json.dumps(
                    {
                        "schema_version": "fixture_trial_run_v1",
                        "proxy_sidecar_root": str((baseline_root / "proxy-sidecars").resolve()),
                    }
                ),
                encoding="utf-8",
            )
            (trial_root / "fixture_trial_run_manifest.json").write_text(
                json.dumps(
                    {
                        "schema_version": "fixture_trial_run_v1",
                        "proxy_sidecar_root": str((trial_root / "proxy-sidecars").resolve()),
                    }
                ),
                encoding="utf-8",
            )
            fixture_manifest = _write_fixture_manifest(root / "fixtures.json", ["fixture-a"])

            with patch(
                "run.run_compare_fixture_sidecars",
                return_value={"ok": True, "status": "ok"},
            ) as mock_compare:
                result = run_compare_fixture_trials(
                    fixture_manifest,
                    baseline_run_root=baseline_root,
                    trial_run_root=trial_root,
                    artifact_layer="proxy",
                )

            self.assertTrue(result["ok"])
            mock_compare.assert_called_once_with(
                fixture_manifest,
                baseline_sidecar_root=(baseline_root / "proxy-sidecars").resolve(),
                trial_sidecar_root=(trial_root / "proxy-sidecars").resolve(),
                artifact_layer="proxy",
                game=None,
                output_path=None,
            )
            self.assertTrue(str(result["baseline_run_manifest_path"]).endswith("fixture_trial_run_manifest.json"))

    def test_cli_routes_to_run_fixture_trial(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--run-fixture-trial",
                "/tmp/fixtures.json",
                "--fixture-source-manifest",
                "/tmp/fixture_sources.json",
                "--trial-name",
                "baseline",
                "--output-root",
                "/tmp/runs",
                "--proposal-backend",
                "pyscenedetect",
                "--emit-runtime",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_fixture_trial",
                return_value={"ok": True, "status": "ok"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/fixtures.json",
                fixture_source_manifest="/tmp/fixture_sources.json",
                trial_name="baseline",
                output_root="/tmp/runs",
                game=None,
                pattern="*.mp4",
                limit=None,
                proposal_backend="pyscenedetect",
                asr_backend=None,
                emit_runtime=True,
                emit_fused=False,
            )
            self.assertTrue(json.loads(stdout.getvalue())["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_compare_fixture_trials(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--compare-fixture-trials",
                "/tmp/fixtures.json",
                "--baseline-run-root",
                "/tmp/baseline",
                "--trial-run-root",
                "/tmp/trial",
                "--artifact-layer",
                "runtime",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_compare_fixture_trials",
                return_value={"ok": True, "status": "ok"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/fixtures.json",
                baseline_run_root="/tmp/baseline",
                trial_run_root="/tmp/trial",
                artifact_layer="runtime",
                game=None,
                output_path=None,
            )
            self.assertTrue(json.loads(stdout.getvalue())["ok"])
        finally:
            sys.argv = original_argv


if __name__ == "__main__":
    unittest.main()

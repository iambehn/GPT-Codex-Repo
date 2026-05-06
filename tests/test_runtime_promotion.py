from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pipeline.runtime_promotion as runtime_promotion
from pipeline.simple_yaml import load_yaml_file
from run import main as run_main
from run import run_promote_runtime_scoring


def _event(event_type: str, *, confidence: float = 0.95) -> dict[str, object]:
    return {
        "event_id": f"{event_type}-event",
        "event_type": event_type,
        "timestamp": 1.0,
        "start_timestamp": 1.0,
        "end_timestamp": 1.5,
        "asset_id": f"{event_type}-asset",
        "roi_ref": "hero_portrait",
        "confidence": confidence,
        "evidence": {"peak_score": confidence},
        "source_detection_count": 3,
    }


def _detection(*, roi_ref: str = "hero_portrait", asset_family: str = "hero_portrait") -> dict[str, object]:
    return {
        "asset_id": f"{asset_family}-asset",
        "roi_ref": roi_ref,
        "asset_family": asset_family,
        "first_timestamp": 1.0,
        "last_timestamp": 1.5,
        "peak_score": 0.98,
        "supporting_frames": 4,
        "temporal_window": 3,
    }


def _runtime_sidecar(
    *,
    analysis_id: str,
    game: str,
    source: str,
    events: list[dict[str, object]],
    detections: list[dict[str, object]],
    review_status: str | None = None,
    ok: bool = True,
    schema_version: str = "runtime_analysis_v1",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": schema_version,
        "analysis_id": analysis_id,
        "ok": ok,
        "status": "ok" if ok else "failed",
        "game": game,
        "source": source,
        "sidecar_path": f"/tmp/{analysis_id}.runtime_analysis.json",
        "game_pack": {"game_id": game, "pack_format": "published"},
        "matcher": {
            "status": "ok",
            "frame_count": 12,
            "sample_fps": 4.0,
            "template_count": 3,
            "summary": {
                "total_confirmed_detections": len(detections),
                "detections_by_roi": {},
                "detections_by_asset_family": {},
            },
            "top_scores": {},
            "unseen_templates": [],
            "confirmed_detections": detections,
        },
        "events": {
            "status": "ok",
            "event_count": len(events),
            "event_summary": {},
            "rows": events,
        },
    }
    if review_status is not None:
        payload["runtime_review"] = {"review_status": review_status}
    return payload


class RuntimePromotionTests(unittest.TestCase):
    def test_promotion_materializes_runtime_scoring_and_preserves_proxy_config(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            config_path = repo_root / "config.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "proxy_scanner:",
                        "  cost_gates:",
                        "    inspect_min_score: 0.40",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            sidecar_root = root / "sidecars"
            self._write_prefer_trial_sidecars(sidecar_root)
            trial_config = root / "trial.yaml"
            trial_config.write_text("action_thresholds:\n  highlight_candidate: 0.45\n", encoding="utf-8")

            history_root = root / "history"
            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_promotion, "DEFAULT_HISTORY_ROOT", history_root):
                result = run_promote_runtime_scoring(
                    trial_config,
                    sidecar_root=sidecar_root,
                    game="marvel_rivals",
                    min_reviewed=2,
                )

            self.assertTrue(result["ok"])
            updated = load_yaml_file(config_path)
            self.assertEqual(updated["proxy_scanner"]["cost_gates"]["inspect_min_score"], 0.40)
            self.assertEqual(updated["runtime_analysis"]["scoring"]["action_thresholds"]["highlight_candidate"], 0.45)
            self.assertTrue(Path(result["snapshot_paths"]["snapshot_dir"]).is_dir())
            self.assertTrue(Path(result["snapshot_paths"]["previous_scoring_path"]).is_file())
            self.assertTrue(Path(result["snapshot_paths"]["applied_scoring_path"]).is_file())

    def test_promotion_blocks_when_replay_is_inconclusive(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            sidecar_root = root / "sidecars"
            self._write_prefer_trial_sidecars(sidecar_root)
            trial_config = root / "trial.yaml"
            trial_config.write_text("action_thresholds:\n  highlight_candidate: 0.60\n", encoding="utf-8")

            with patch("run.REPO_ROOT", repo_root):
                result = run_promote_runtime_scoring(
                    trial_config,
                    sidecar_root=sidecar_root,
                    game="marvel_rivals",
                    min_reviewed=2,
                )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "promotion_blocked_by_replay")

    def test_force_allows_promotion_after_insufficient_review_data(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            sidecar_root = root / "sidecars"
            self._write_sidecar(
                sidecar_root / "approved.runtime_analysis.json",
                _runtime_sidecar(
                    analysis_id="approved",
                    game="marvel_rivals",
                    source="clip-approved.mp4",
                    events=[_event("medal_seen")],
                    detections=[_detection(roi_ref="medal_area", asset_family="medal_icon")],
                    review_status="approved",
                ),
            )
            trial_config = root / "trial.yaml"
            trial_config.write_text("action_thresholds:\n  highlight_candidate: 0.45\n", encoding="utf-8")
            history_root = root / "history"

            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_promotion, "DEFAULT_HISTORY_ROOT", history_root):
                result = run_promote_runtime_scoring(
                    trial_config,
                    sidecar_root=sidecar_root,
                    game="marvel_rivals",
                    min_reviewed=2,
                    force=True,
                )

            self.assertTrue(result["ok"])
            self.assertTrue(result["force_used"])
            self.assertEqual(result["replay_recommendation"]["decision"], "inconclusive")

    def test_promotion_rejects_invalid_replay_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            trial_config = root / "trial.yaml"
            trial_config.write_text("action_thresholds:\n  highlight_candidate: 0.45\n", encoding="utf-8")

            bad_replay = {
                "ok": True,
                "status": "ok",
                "trial_name": "trial",
                "trial_scoring": {},
                "recommendation": {
                    "decision": "prefer_trial",
                    "reason": "missing structured fields",
                },
                "warnings": [],
            }

            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_promotion, "replay_runtime_scoring", return_value=bad_replay):
                result = run_promote_runtime_scoring(
                    trial_config,
                    sidecar_root=root / "sidecars",
                    game="marvel_rivals",
                    min_reviewed=1,
                )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_replay_result")
            self.assertIn("missing field", result["error"])

    def test_missing_sidecar_root_and_invalid_trial_config_return_clean_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            trial_config = root / "trial.yaml"
            trial_config.write_text("action_thresholds:\n  highlight_candidate: 0.45\n", encoding="utf-8")

            with patch("run.REPO_ROOT", repo_root):
                missing_sidecar = run_promote_runtime_scoring(trial_config, sidecar_root=None)
                invalid_trial = run_promote_runtime_scoring(
                    root / "missing.yaml",
                    sidecar_root=root / "sidecars",
                )

            self.assertFalse(missing_sidecar["ok"])
            self.assertEqual(missing_sidecar["status"], "missing_sidecar_root")
            self.assertFalse(invalid_trial["ok"])
            self.assertEqual(invalid_trial["status"], "replay_failed")

    def test_snapshot_is_written_before_config_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            config_path = repo_root / "config.yaml"
            config_path.write_text("proxy_scanner:\n  cost_gates:\n    inspect_min_score: 0.40\n", encoding="utf-8")
            sidecar_root = root / "sidecars"
            self._write_prefer_trial_sidecars(sidecar_root)
            trial_config = root / "trial.yaml"
            trial_config.write_text("action_thresholds:\n  highlight_candidate: 0.45\n", encoding="utf-8")
            history_root = root / "history"

            original_dump = runtime_promotion.dump_yaml_file

            def failing_dump(path: str | Path, data: object) -> None:
                if Path(path).resolve() == config_path.resolve():
                    raise OSError("boom")
                original_dump(path, data)

            before = config_path.read_text(encoding="utf-8")
            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_promotion, "DEFAULT_HISTORY_ROOT", history_root), patch.object(runtime_promotion, "dump_yaml_file", side_effect=failing_dump):
                result = run_promote_runtime_scoring(
                    trial_config,
                    sidecar_root=sidecar_root,
                    game="marvel_rivals",
                    min_reviewed=2,
                )

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "config_write_failed")
            self.assertTrue(Path(result["snapshot_paths"]["snapshot_dir"]).is_dir())
            self.assertEqual(config_path.read_text(encoding="utf-8"), before)

    def test_cli_routes_to_runtime_promotion(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--promote-runtime-scoring",
                "/tmp/trial.yaml",
                "--sidecar-root",
                "/tmp/runtime-sidecars",
                "--game",
                "marvel_rivals",
                "--force",
                "--trial-name",
                "trial-a",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_promote_runtime_scoring",
                return_value={"ok": True, "trial_name": "trial-a", "config_changed": True},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/trial.yaml",
                sidecar_root="/tmp/runtime-sidecars",
                game="marvel_rivals",
                min_reviewed=3,
                force=True,
                output_path=None,
                debug_output_dir=None,
                trial_name="trial-a",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def _write_prefer_trial_sidecars(self, sidecar_root: Path) -> None:
        self._write_sidecar(
            sidecar_root / "approved.runtime_analysis.json",
            _runtime_sidecar(
                analysis_id="approved",
                game="marvel_rivals",
                source="clip-approved.mp4",
                events=[_event("medal_seen")],
                detections=[_detection(roi_ref="medal_area", asset_family="medal_icon")],
                review_status="approved",
            ),
        )
        self._write_sidecar(
            sidecar_root / "rejected.runtime_analysis.json",
            _runtime_sidecar(
                analysis_id="rejected",
                game="marvel_rivals",
                source="clip-rejected.mp4",
                events=[_event("pov_character_identified")],
                detections=[_detection()],
                review_status="rejected",
            ),
        )

    def _write_sidecar(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

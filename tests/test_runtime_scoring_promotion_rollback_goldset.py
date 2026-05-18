from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline.runtime_promotion as runtime_promotion
import pipeline.runtime_rollback as runtime_rollback
from pipeline.simple_yaml import dump_yaml_file, load_yaml_file
from run import run_promote_runtime_scoring, run_rollback_runtime_scoring
from tests.test_runtime_promotion import _detection, _event, _runtime_sidecar


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "runtime_scoring_promotion_rollback_goldsets"
    / "runtime_scoring_promotion_rollback_goldset.json"
)


class RuntimeScoringPromotionRollbackGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "runtime_scoring_promotion_rollback_goldset_v1")
        self.assertGreaterEqual(len(payload["cases"]), 4)

    def test_runtime_scoring_promotion_and_rollback_cases_match_expectations(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                scenario = str(case["scenario"])
                if scenario == "promotion":
                    self._assert_promotion_case(case)
                elif scenario == "rollback":
                    self._assert_rollback_case(case)
                else:
                    self.fail(f"unsupported scenario: {scenario}")

    def _assert_promotion_case(self, case: dict[str, object]) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            repo_root = root / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)
            config_path = repo_root / "config.yaml"
            dump_yaml_file(
                config_path,
                {
                    "proxy_scanner": {"cost_gates": {"inspect_min_score": 0.40}},
                },
            )
            sidecar_root = root / "sidecars"
            self._write_sidecar_profile(sidecar_root, profile=str(case["sidecar_profile"]), game=str(case["game"]))
            trial_config = root / "trial.yaml"
            trial_config.write_text(
                f"action_thresholds:\n  highlight_candidate: {float(case['trial_threshold'])}\n",
                encoding="utf-8",
            )
            history_root = root / "history"

            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_promotion, "DEFAULT_HISTORY_ROOT", history_root):
                result = run_promote_runtime_scoring(
                    trial_config,
                    sidecar_root=sidecar_root,
                    game=str(case["game"]),
                    min_reviewed=int(case["min_reviewed"]),
                )

            self.assertEqual(bool(result["ok"]), bool(case["expected_ok"]))
            self.assertEqual(result["status"], case["expected_status"])

            if result["ok"]:
                updated = load_yaml_file(config_path)
                self.assertEqual(
                    updated["proxy_scanner"]["cost_gates"]["inspect_min_score"],
                    case["expected_proxy_inspect_min_score"],
                )
                self.assertEqual(
                    updated["runtime_analysis"]["scoring"]["action_thresholds"]["highlight_candidate"],
                    case["expected_highlight_threshold"],
                )
                self.assertEqual(bool(result["force_used"]), bool(case["expected_force_used"]))
                self.assertTrue(Path(result["snapshot_paths"]["snapshot_dir"]).is_dir())

    def _assert_rollback_case(self, case: dict[str, object]) -> None:
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
                            "action_thresholds": {"highlight_candidate": float(case.get("current_threshold", 0.45))}
                        }
                    },
                },
            )
            snapshot_dir = root / "snapshot"
            if case.get("invalid_snapshot_payload"):
                snapshot_dir.mkdir(parents=True, exist_ok=True)
                (snapshot_dir / "previous_scoring.yaml").write_text("- not-a-dict\n", encoding="utf-8")
                (snapshot_dir / "applied_scoring.yaml").write_text("{}\n", encoding="utf-8")
                (snapshot_dir / "promotion_record.json").write_text(
                    json.dumps({"trial_name": "trial-a", "config_path": "/tmp/config.yaml"}, indent=2),
                    encoding="utf-8",
                )
            else:
                self._write_snapshot(
                    snapshot_dir,
                    previous_scoring={"action_thresholds": {"highlight_candidate": float(case["previous_threshold"])}},
                    applied_scoring={"action_thresholds": {"highlight_candidate": float(case["current_threshold"])}},
                )
            history_root = root / "history"

            with patch("run.REPO_ROOT", repo_root), patch.object(runtime_rollback, "DEFAULT_HISTORY_ROOT", history_root):
                result = run_rollback_runtime_scoring(
                    snapshot_dir,
                    rollback_name=str(case.get("rollback_name") or "goldset-rollback"),
                )

            self.assertEqual(bool(result["ok"]), bool(case["expected_ok"]))
            self.assertEqual(result["status"], case["expected_status"])

            if result["ok"]:
                updated = load_yaml_file(config_path)
                self.assertEqual(
                    updated["proxy_scanner"]["cost_gates"]["inspect_min_score"],
                    case["expected_proxy_inspect_min_score"],
                )
                self.assertEqual(
                    updated["runtime_analysis"]["scoring"]["action_thresholds"]["highlight_candidate"],
                    case["expected_restored_highlight_threshold"],
                )
                self.assertTrue(Path(result["rollback_snapshot_paths"]["snapshot_dir"]).is_dir())

    def _write_sidecar_profile(self, sidecar_root: Path, *, profile: str, game: str) -> None:
        if profile != "prefer_trial":
            raise ValueError(f"unsupported sidecar profile: {profile}")
        self._write_sidecar(
            sidecar_root / "approved.runtime_analysis.json",
            _runtime_sidecar(
                analysis_id="approved",
                game=game,
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
                game=game,
                source="clip-rejected.mp4",
                events=[_event("pov_character_identified")],
                detections=[_detection()],
                review_status="rejected",
            ),
        )

    def _write_sidecar(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _write_snapshot(self, snapshot_dir: Path, *, previous_scoring: dict[str, object], applied_scoring: dict[str, object]) -> None:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        dump_yaml_file(snapshot_dir / "previous_scoring.yaml", previous_scoring)
        dump_yaml_file(snapshot_dir / "applied_scoring.yaml", applied_scoring)
        (snapshot_dir / "promotion_record.json").write_text(
            json.dumps({"trial_name": "trial-a", "config_path": "/tmp/config.yaml"}, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()

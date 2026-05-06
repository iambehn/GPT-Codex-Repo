from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.hook_candidate_comparison import compare_hook_candidates
from run import main as run_main


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fixture_manifest(path: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "evaluation_fixture_manifest_v1",
            "fixture_count": 1,
            "fixtures": [
                {
                    "fixture_id": "fixture-a",
                    "label": "fixture-a",
                    "game": "marvel_rivals",
                    "expected_artifacts": {"fused": True},
                    "artifact_refs": {"fused_sidecar": "alpha.fused_analysis.json"},
                    "expected_review_outcome": "approved",
                    "latency_budget_class": "smoke",
                }
            ],
        },
    )


def _hook_manifest(path: Path, *, hook_mode: str, hook_strength: float, hook_archetype: str = "flex", candidate_id: str = "candidate-123", lifecycle_state: str = "selected_for_export") -> None:
    _write_json(
        path,
        {
            "schema_version": "hook_candidate_v1",
            "game": "marvel_rivals",
            "source": "/tmp/alpha.mp4",
            "fused_sidecar_path": "/tmp/alpha.fused_analysis.json",
            "hook_candidate_count": 1,
            "hook_candidates": [
                {
                    "hook_id": "hook-123",
                    "candidate_id": candidate_id,
                    "event_id": "fused-1",
                    "lifecycle_state": lifecycle_state,
                    "start_seconds": 0.5,
                    "end_seconds": 3.0,
                    "final_score": 0.91,
                    "recommended_action": "highlight_candidate",
                    "gate_status": "confirmed",
                    "event_type": "ability_plus_medal_combo",
                    "hook_archetype": hook_archetype,
                    "hook_strength": hook_strength,
                    "intensity_score": 0.8,
                    "clarity_score": 0.75,
                    "novelty_score": 0.62,
                    "context_sufficiency_score": 0.7,
                    "payoff_readability_score": 0.72,
                    "title_thumbnail_potential_score": 0.68,
                    "authenticity_risk_score": 0.2 if hook_mode != "reject" else 0.7,
                    "sound_off_legibility_score": 0.66,
                    "hook_mode": hook_mode,
                    "packaging_strategy": "tight_context_then_payoff" if hook_mode != "reject" else None,
                    "rejection_reason": "authenticity_risk_too_high" if hook_mode == "reject" else None,
                    "contributing_signal_ids": ["signal-1"],
                    "entity_id": "punisher",
                    "metadata_summary": {"entity_id": "punisher"},
                }
            ],
        },
    )


class HookCandidateComparisonTests(unittest.TestCase):
    def test_compare_hook_candidates_writes_report_and_prefers_trial(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = root / "fixtures.json"
            _fixture_manifest(fixture_manifest)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            _hook_manifest(baseline_root / "alpha.hook_candidates.json", hook_mode="reject", hook_strength=0.38)
            _hook_manifest(trial_root / "alpha.hook_candidates.json", hook_mode="synthetic", hook_strength=0.66)

            result = compare_hook_candidates(
                fixture_manifest,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
                output_path=root / "hook_comparison.json",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["recommendation"]["decision"], "prefer_trial")
            self.assertEqual(result["comparison_row_count"], 1)
            row = result["comparison"]["fixture_rows"][0]
            self.assertEqual(row["comparison_status"], "matched")
            self.assertTrue(row["reject_to_synthetic"])
            self.assertEqual(row["recommendation_signal"], "trial_better")

    def test_compare_hook_candidates_can_keep_current(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = root / "fixtures.json"
            _fixture_manifest(fixture_manifest)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            _hook_manifest(baseline_root / "alpha.hook_candidates.json", hook_mode="natural", hook_strength=0.84, hook_archetype="clutch")
            _hook_manifest(trial_root / "alpha.hook_candidates.json", hook_mode="synthetic", hook_strength=0.58, hook_archetype="clutch")

            result = compare_hook_candidates(
                fixture_manifest,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["recommendation"]["decision"], "keep_current")
            self.assertTrue(result["comparison"]["fixture_rows"][0]["natural_to_synthetic"])

    def test_compare_hook_candidates_handles_missing_trial_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = root / "fixtures.json"
            _fixture_manifest(fixture_manifest)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            _hook_manifest(baseline_root / "alpha.hook_candidates.json", hook_mode="natural", hook_strength=0.84)
            trial_root.mkdir(parents=True, exist_ok=True)

            result = compare_hook_candidates(
                fixture_manifest,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["comparison"]["fixture_rows"][0]["comparison_status"], "baseline_only")
            self.assertEqual(result["recommendation"]["decision"], "inconclusive")

    def test_registry_refresh_ingests_hook_comparison_and_query_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = root / "fixtures.json"
            _fixture_manifest(fixture_manifest)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            _hook_manifest(baseline_root / "alpha.hook_candidates.json", hook_mode="reject", hook_strength=0.38)
            _hook_manifest(trial_root / "alpha.hook_candidates.json", hook_mode="synthetic", hook_strength=0.66)
            report_path = root / "hook_comparison.json"
            compare_hook_candidates(
                fixture_manifest,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
                output_path=report_path,
            )

            first = refresh_clip_registry(root, registry_path=root / "registry.sqlite")
            second = refresh_clip_registry(root, registry_path=root / "registry.sqlite")

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["hook_comparison_report_count"], 1)
            query = query_clip_registry(
                mode="hook-comparisons",
                registry_path=root / "registry.sqlite",
                fixture_id="fixture-a",
                hook_mode="synthetic",
                recommendation_decision="prefer_trial",
                comparison_status="matched",
            )
            self.assertTrue(query["ok"])
            self.assertEqual(query["row_count"], 1)
            self.assertEqual(query["rows"][0]["candidate_id"], "candidate-123")

    def test_cli_routes_to_compare_hook_candidates(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--compare-hook-candidates",
                "/tmp/fixtures.json",
                "--baseline-sidecar-root",
                "/tmp/baseline",
                "--trial-sidecar-root",
                "/tmp/trial",
            ]
            with patch("run.run_compare_hook_candidates", return_value={"ok": True, "comparison_row_count": 1}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()

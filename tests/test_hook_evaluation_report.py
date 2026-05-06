from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.hook_evaluation_report import report_hook_evaluation
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


def _hook_manifest(
    path: Path,
    *,
    hook_mode: str,
    hook_strength: float,
    hook_archetype: str = "flex",
    candidate_id: str = "candidate-123",
    lifecycle_state: str = "selected_for_export",
) -> None:
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


def _export_batch_manifest(path: Path, *, candidate_id: str = "candidate-123") -> None:
    _write_json(
        path,
        {
            "schema_version": "highlight_export_batch_v1",
            "export_batch_id": "export-batch-1",
            "game": "marvel_rivals",
            "export_count": 1,
            "created_at": "2026-05-05T00:00:00+00:00",
            "linked_inputs": {
                "fused_sidecar_paths": ["/tmp/alpha.fused_analysis.json"],
                "hook_manifest_paths": ["/tmp/selected.hook_candidates.json"],
                "selection_manifest_paths": ["/tmp/selection.json"],
            },
            "exports": [
                {
                    "export_id": "export-1",
                    "candidate_id": candidate_id,
                    "event_id": "fused-1",
                    "hook_id": "hook-123",
                    "fixture_id": "fixture-a",
                    "source": "/tmp/alpha.mp4",
                    "fused_sidecar_path": "/tmp/alpha.fused_analysis.json",
                    "hook_manifest_path": "/tmp/selected.hook_candidates.json",
                    "highlight_selection_manifest_path": "/tmp/selection.json",
                    "start_seconds": 0.5,
                    "end_seconds": 3.0,
                    "final_score": 0.91,
                    "hook_archetype": "clutch",
                    "hook_mode": "natural",
                    "packaging_strategy": "tight_context_then_payoff",
                    "export_status": "exported",
                    "export_artifact_path": "/tmp/export-1.mp4",
                    "otio_path": "/tmp/export-1.otio",
                    "metadata_json": {"test": True},
                }
            ],
        },
    )


class HookEvaluationReportTests(unittest.TestCase):
    def test_report_hook_evaluation_writes_unified_report(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = root / "fixtures.json"
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            registry_source = root / "registry_source"
            registry_path = root / "registry.sqlite"
            report_path = root / "reports" / "hooks.hook_evaluation_report.json"

            _fixture_manifest(fixture_manifest)
            _hook_manifest(baseline_root / "alpha.hook_candidates.json", hook_mode="reject", hook_strength=0.38)
            _hook_manifest(trial_root / "alpha.hook_candidates.json", hook_mode="synthetic", hook_strength=0.66)
            _hook_manifest(
                registry_source / "selected.hook_candidates.json",
                hook_mode="natural",
                hook_strength=0.84,
                hook_archetype="clutch",
                candidate_id="candidate-123",
                lifecycle_state="selected_for_export",
            )
            _export_batch_manifest(registry_source / "exports" / "batch.highlight_export_batch.json")
            refresh_clip_registry(registry_source, registry_path=registry_path)

            result = report_hook_evaluation(
                fixture_manifest,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
                registry_path=registry_path,
                game="marvel_rivals",
                output_path=report_path,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["schema_version"], "hook_evaluation_report_v1")
            self.assertEqual(result["policy"]["hook_artifacts_policy"], "advisory")
            self.assertEqual(result["trial_comparison"]["recommendation"]["decision"], "prefer_trial")
            self.assertEqual(result["candidate_rollups"]["selected_or_approved"]["candidate_count"], 1)
            self.assertEqual(result["candidate_rollups"]["exported"]["candidate_count"], 1)
            self.assertEqual(result["fused_hook_disagreement"]["reject_to_synthetic_count"], 1)
            self.assertTrue(report_path.exists())

    def test_registry_refresh_ingests_hook_evaluation_report_and_rollups(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            fixture_manifest = root / "fixtures.json"
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            registry_source = root / "registry_source"
            registry_path = root / "registry.sqlite"
            report_path = registry_source / "reports" / "hooks.hook_evaluation_report.json"

            _fixture_manifest(fixture_manifest)
            _hook_manifest(baseline_root / "alpha.hook_candidates.json", hook_mode="reject", hook_strength=0.38)
            _hook_manifest(trial_root / "alpha.hook_candidates.json", hook_mode="synthetic", hook_strength=0.66)
            _hook_manifest(
                registry_source / "selected.hook_candidates.json",
                hook_mode="natural",
                hook_strength=0.84,
                hook_archetype="clutch",
                candidate_id="candidate-123",
                lifecycle_state="selected_for_export",
            )
            _export_batch_manifest(registry_source / "exports" / "batch.highlight_export_batch.json")
            refresh_clip_registry(registry_source, registry_path=registry_path)
            report_hook_evaluation(
                fixture_manifest,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
                registry_path=registry_path,
                game="marvel_rivals",
                output_path=report_path,
            )

            refreshed = refresh_clip_registry(registry_source, registry_path=registry_path)
            self.assertTrue(refreshed["ok"])
            self.assertEqual(refreshed["hook_evaluation_report_count"], 1)

            reports = query_clip_registry(
                mode="hook-evaluation-reports",
                registry_path=registry_path,
                game="marvel_rivals",
                recommendation_decision="prefer_trial",
                status="insufficient_evidence",
            )
            self.assertTrue(reports["ok"])
            self.assertEqual(reports["row_count"], 1)
            self.assertEqual(reports["rows"][0]["reject_to_synthetic_count"], 1)

            rollups = query_clip_registry(
                mode="hook-quality-rollups",
                registry_path=registry_path,
                game="marvel_rivals",
            )
            self.assertTrue(rollups["ok"])
            self.assertEqual(rollups["query_shape"], "aggregate")
            self.assertEqual(rollups["row_count"], 1)
            row = rollups["rows"][0]
            self.assertEqual(row["selected_candidate_count"], 1)
            self.assertEqual(row["exported_candidate_count"], 1)
            self.assertEqual(row["reject_to_synthetic_count"], 1)
            self.assertEqual(json.loads(row["selected_hook_mode_counts_json"])["natural"], 1)

    def test_cli_routes_to_report_hook_evaluation(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--report-hook-evaluation",
                "/tmp/fixtures.json",
                "--baseline-sidecar-root",
                "/tmp/baseline",
                "--trial-sidecar-root",
                "/tmp/trial",
                "--registry-path",
                "/tmp/registry.sqlite",
            ]
            with patch("run.run_report_hook_evaluation", return_value={"ok": True, "report_path": "/tmp/report.json"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()

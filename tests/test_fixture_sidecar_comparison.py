from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import patch

from pipeline.fixture_sidecar_comparison import compare_fixture_sidecars
from run import main as run_main


def _manifest(fixtures: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "evaluation_fixture_manifest_v1",
        "fixtures": fixtures,
    }


def _proxy_sidecar(
    *,
    source: Path,
    scan_id: str,
    score: float,
    action: str,
    review_status: str,
    shortlist: list[tuple[float, float]] | None = None,
    rerank_order: list[tuple[float, float]] | None = None,
) -> dict[str, Any]:
    shortlist_rows = [
        {"start_seconds": start, "end_seconds": end}
        for start, end in (shortlist or [(0.0, 5.0)])
    ]
    rerank_rows = [
        {"start_seconds": start, "end_seconds": end}
        for start, end in (rerank_order or shortlist or [(0.0, 5.0)])
    ]
    return {
        "schema_version": "proxy_scan_v1",
        "scan_id": scan_id,
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "sidecar_path": f"/tmp/{scan_id}.proxy_scan.json",
        "windows": [
            {
                "start_seconds": 0.0,
                "end_seconds": 5.0,
                "proxy_score": score,
                "recommended_action": action,
                "source_families": ["hf_multimodal"],
                "signals": [],
            }
        ],
        "proxy_review": {"review_status": review_status},
        "source_results": {
            "hf_multimodal": {
                "status": "ok",
                "metadata": {
                    "stages": {
                        "shot_detector": {"duration_ms": 12.0},
                        "asr": {"duration_ms": 21.0},
                    },
                    "structured_outputs": {
                        "shortlisted_candidates": shortlist_rows,
                        "reranked_candidates": rerank_rows,
                    },
                },
            }
        },
    }


def _runtime_sidecar(
    *,
    source: Path,
    analysis_id: str,
    review_status: str,
    medal_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime_analysis_v1",
        "analysis_id": analysis_id,
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "runtime_review": {"review_status": review_status},
        "matcher": {
            "confirmed_detections": [{"first_timestamp": 0.5, "last_timestamp": 1.0, "peak_score": 0.9}],
        },
        "events": {
            "rows": [
                {
                    "event_type": "medal_seen",
                    "start_timestamp": float(index),
                    "end_timestamp": float(index) + 0.25,
                    "confidence": 0.9,
                }
                for index in range(medal_count)
            ]
        },
    }


class FixtureSidecarComparisonTests(unittest.TestCase):
    def test_compare_fixture_sidecars_reports_proxy_deltas_and_prefers_trial(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            baseline_root.mkdir()
            trial_root.mkdir()
            source = root / "clip.mp4"
            source.write_bytes(b"video")
            manifest_path = root / "fixtures.json"
            manifest_path.write_text(
                json.dumps(
                    _manifest(
                        [
                            {
                                "fixture_id": "fixture-a",
                                "label": "Fixture A",
                                "task_intent": "approved path",
                                "expected_review_outcome": "approved",
                                "latency_budget_class": "smoke",
                                "expected_artifacts": {"proxy": True},
                                "artifact_refs": {"proxy_sidecar": "fixture-a.proxy_scan.json"},
                            },
                            {
                                "fixture_id": "fixture-b",
                                "label": "Fixture B",
                                "task_intent": "approved path",
                                "expected_review_outcome": "approved",
                                "latency_budget_class": "smoke",
                                "expected_artifacts": {"proxy": True},
                                "artifact_refs": {"proxy_sidecar": "fixture-b.proxy_scan.json"},
                            },
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (baseline_root / "fixture-a.proxy_scan.json").write_text(
                json.dumps(_proxy_sidecar(source=source, scan_id="fixture-a", score=0.61, action="inspect", review_status="approved"), indent=2),
                encoding="utf-8",
            )
            (trial_root / "fixture-a.proxy_scan.json").write_text(
                json.dumps(_proxy_sidecar(source=source, scan_id="fixture-a", score=0.91, action="download_candidate", review_status="approved"), indent=2),
                encoding="utf-8",
            )
            (baseline_root / "fixture-b.proxy_scan.json").write_text(
                json.dumps(_proxy_sidecar(source=source, scan_id="fixture-b", score=0.55, action="inspect", review_status="approved"), indent=2),
                encoding="utf-8",
            )
            (trial_root / "fixture-b.proxy_scan.json").write_text(
                json.dumps(
                    _proxy_sidecar(
                        source=source,
                        scan_id="fixture-b",
                        score=0.82,
                        action="download_candidate",
                        review_status="approved",
                        shortlist=[(0.0, 5.0)],
                        rerank_order=[(3.0, 8.0)],
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = compare_fixture_sidecars(
                manifest_path,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
                artifact_layer="proxy",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["recommendation"]["decision"], "prefer_trial")
            rows = result["comparison"]["fixture_rows"]
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["recommendation_signal"] == "trial_better" for row in rows))
            self.assertIn("shot_detector", rows[0]["stage_latency_deltas"])

    def test_compare_fixture_sidecars_reports_runtime_delta_and_coverage_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            baseline_root.mkdir()
            trial_root.mkdir()
            source = root / "clip.mp4"
            source.write_bytes(b"video")
            manifest_path = root / "fixtures.json"
            manifest_path.write_text(
                json.dumps(
                    _manifest(
                        [
                            {
                                "fixture_id": "fixture-a",
                                "label": "Fixture A",
                                "task_intent": "runtime path",
                                "expected_review_outcome": "approved",
                                "latency_budget_class": "smoke",
                                "expected_artifacts": {"runtime": True, "fused": True},
                                "artifact_refs": {"runtime_sidecar": "fixture-a.runtime_analysis.json"},
                            }
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (baseline_root / "fixture-a.runtime_analysis.json").write_text(
                json.dumps(_runtime_sidecar(source=source, analysis_id="fixture-a", review_status="approved", medal_count=1), indent=2),
                encoding="utf-8",
            )
            (trial_root / "fixture-a.runtime_analysis.json").write_text(
                json.dumps(_runtime_sidecar(source=source, analysis_id="fixture-a", review_status="approved", medal_count=2), indent=2),
                encoding="utf-8",
            )

            result = compare_fixture_sidecars(
                manifest_path,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
                artifact_layer="all",
            )

            runtime_rows = [row for row in result["comparison"]["fixture_rows"] if row["artifact_layer"] == "runtime"]
            fused_rows = [row for row in result["comparison"]["fixture_rows"] if row["artifact_layer"] == "fused"]
            self.assertEqual(len(runtime_rows), 1)
            self.assertGreater(runtime_rows[0]["score_delta"], 0.0)
            self.assertEqual(runtime_rows[0]["recommendation_signal"], "trial_better")
            self.assertEqual(fused_rows[0]["coverage_status"], "missing")
            self.assertEqual(fused_rows[0]["recommendation_signal"], "coverage_gap")

    def test_compare_fixture_sidecars_writes_report_bundle_and_skips_malformed_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            baseline_root = root / "baseline"
            trial_root = root / "trial"
            baseline_root.mkdir()
            trial_root.mkdir()
            source = root / "clip.mp4"
            source.write_bytes(b"video")
            manifest_path = root / "fixtures.json"
            manifest_path.write_text(
                json.dumps(
                    _manifest(
                        [
                            {
                                "fixture_id": "fixture-a",
                                "label": "Fixture A",
                                "task_intent": "rejected path",
                                "expected_review_outcome": "rejected",
                                "latency_budget_class": "smoke",
                                "expected_artifacts": {"proxy": True},
                                "artifact_refs": {"proxy_sidecar": "fixture-a.proxy_scan.json"},
                            }
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )
            (baseline_root / "fixture-a.proxy_scan.json").write_text("{not-json", encoding="utf-8")
            (trial_root / "fixture-a.proxy_scan.json").write_text(
                json.dumps(_proxy_sidecar(source=source, scan_id="fixture-a", score=0.7, action="download_candidate", review_status="rejected"), indent=2),
                encoding="utf-8",
            )
            output_path = root / "comparison.json"

            result = compare_fixture_sidecars(
                manifest_path,
                baseline_sidecar_root=baseline_root,
                trial_sidecar_root=trial_root,
                artifact_layer="proxy",
                output_path=output_path,
            )

            self.assertTrue(output_path.is_file())
            self.assertTrue(Path(result["csv_path"]).is_file())
            self.assertTrue(Path(result["warnings_path"]).is_file())
            self.assertTrue(any(row["reason"] == "malformed_json" for row in result["warnings"]))

    def test_cli_routes_to_compare_fixture_sidecars(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--compare-fixture-sidecars",
                "/tmp/fixtures.json",
                "--baseline-sidecar-root",
                "/tmp/baseline",
                "--trial-sidecar-root",
                "/tmp/trial",
                "--artifact-layer",
                "proxy",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_compare_fixture_sidecars",
                return_value={"ok": True, "status": "ok"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/fixtures.json",
                baseline_sidecar_root="/tmp/baseline",
                trial_sidecar_root="/tmp/trial",
                artifact_layer="proxy",
                game=None,
                output_path=None,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()

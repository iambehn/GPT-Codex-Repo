from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.highlight_export_batch import (
    create_highlight_export_batch,
    report_posted_performance,
    record_post_ledger,
    record_posted_metrics_snapshot,
)
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from pipeline.workflow_run_state import create_workflow_run
from run import main as run_main


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fused_sidecar(path: Path, *, game: str, source: Path) -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_analysis_v1",
            "fusion_id": "fusion-001",
            "ok": True,
            "status": "ok",
            "game": game,
            "source": str(source.resolve()),
            "normalized_signals": [
                {
                    "signal_id": "signal-1",
                    "signal_type": "character_identity",
                    "producer_family": "runtime",
                }
            ],
            "fused_events": [
                {
                    "event_id": "fused-event-1",
                    "event_type": "ability_plus_medal_combo",
                    "confidence": 0.84,
                    "final_score": 0.92,
                    "gate_status": "confirmed",
                    "synergy_applied": True,
                    "minimum_required_signals_met": True,
                    "suggested_start_timestamp": 0.5,
                    "suggested_end_timestamp": 3.2,
                    "contributing_signals": ["signal-1"],
                    "metadata": {"entity_id": "punisher", "ability_id": "ult"},
                }
            ],
            "fused_review": {
                "session_id": "fused-session-1",
                "reviewed_event_count": 1,
                "events": {"fused-event-1": {"review_status": "approved"}},
            },
            "sidecar_path": str(path.resolve()),
        },
    )


def _prepare_posted_candidate(root: Path) -> tuple[Path, dict, str]:
    media = root / "media" / "alpha.mp4"
    media.parent.mkdir(parents=True, exist_ok=True)
    media.write_bytes(b"video")
    fused_path = root / "fused" / "alpha.fused_analysis.json"
    registry_path = root / "registry.sqlite"
    _fused_sidecar(fused_path, game="marvel_rivals", source=media)
    refresh_clip_registry(root, registry_path=registry_path)
    export_highlight_selection(fused_sidecar=fused_path, output_path=root / "selection" / "alpha.highlight_selection.json")
    refresh_clip_registry(root, registry_path=registry_path)
    derive_hook_candidates(fused_path, registry_path=registry_path, output_path=root / "hooks" / "alpha.hook_candidates.json")
    refresh_clip_registry(root, registry_path=registry_path)
    workflow = create_workflow_run("export_queue", registry_path=registry_path, output_path=root / "workflow" / "export.workflow_run.json")
    export_batch = create_highlight_export_batch(
        registry_path=registry_path,
        workflow_run_id=workflow["workflow_run_id"],
        output_path=root / "exports" / "batch.highlight_export_batch.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)
    ledger = record_post_ledger(
        export_batch["manifest_path"],
        workflow_run_id=workflow["workflow_run_id"],
        platform="youtube",
        account_id="acct-1",
        output_path=root / "posted" / "ledger.posted_highlight_ledger.json",
    )
    refresh_clip_registry(root, registry_path=registry_path)
    candidate_rows = query_clip_registry(mode="candidate-lifecycles", lifecycle_state="posted", registry_path=registry_path)
    candidate_id = str(candidate_rows["rows"][0]["candidate_id"])
    return registry_path, ledger, candidate_id


class PostedMetricsSnapshotTests(unittest.TestCase):
    def test_record_posted_metrics_snapshot_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, ledger, candidate_id = _prepare_posted_candidate(root)
            result = record_posted_metrics_snapshot(
                ledger["manifest_path"],
                workflow_run_id="workflow-1",
                platform="youtube",
                account_id="acct-1",
                output_path=root / "metrics" / "snapshot.posted_highlight_metrics_snapshot.json",
                view_count=1200,
                like_count=80,
                comment_count=7,
                share_count=5,
                save_count=3,
                watch_time_seconds=456.7,
                average_watch_time_seconds=12.3,
                completion_rate=0.61,
                engagement_rate=0.079,
            )

            self.assertTrue(result["ok"])
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "posted_highlight_metrics_snapshot_v1")
            self.assertEqual(manifest["snapshot_count"], 1)
            row = manifest["snapshots"][0]
            self.assertEqual(row["candidate_id"], candidate_id)
            self.assertEqual(row["platform"], "youtube")
            self.assertEqual(row["view_count"], 1200)
            self.assertEqual(row["engagement_rate"], 0.079)

    def test_registry_refresh_ingests_multiple_snapshots_and_rollups(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, ledger, candidate_id = _prepare_posted_candidate(root)
            record_posted_metrics_snapshot(
                ledger["manifest_path"],
                workflow_run_id="workflow-1",
                platform="youtube",
                account_id="acct-1",
                output_path=root / "metrics" / "snapshot-a.posted_highlight_metrics_snapshot.json",
                view_count=100,
                like_count=10,
                comment_count=2,
                share_count=1,
                save_count=1,
                watch_time_seconds=50.0,
                average_watch_time_seconds=5.0,
                completion_rate=0.4,
                engagement_rate=0.13,
            )
            record_posted_metrics_snapshot(
                ledger["manifest_path"],
                workflow_run_id="workflow-1",
                platform="youtube",
                account_id="acct-1",
                output_path=root / "metrics" / "snapshot-b.posted_highlight_metrics_snapshot.json",
                view_count=300,
                like_count=30,
                comment_count=4,
                share_count=3,
                save_count=2,
                watch_time_seconds=90.0,
                average_watch_time_seconds=6.0,
                completion_rate=0.5,
                engagement_rate=0.12,
            )

            first = refresh_clip_registry(root, registry_path=registry_path)
            second = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["posted_metrics_snapshot_manifest_count"], 2)
            self.assertEqual(first["posted_metrics_snapshot_row_count"], 2)

            rows = query_clip_registry(
                mode="posted-metrics",
                registry_path=registry_path,
                candidate_id=candidate_id,
                platform="youtube",
                account_id="acct-1",
            )
            self.assertEqual(rows["row_count"], 2)
            self.assertEqual(rows["rows"][0]["candidate_id"], candidate_id)
            self.assertEqual(rows["rows"][0]["hook_mode"], "natural")
            self.assertTrue(rows["rows"][0]["is_latest_snapshot"])
            self.assertEqual(rows["rows"][0]["metrics_coverage_status"], "complete")
            self.assertEqual(rows["rows"][0]["post_performance_coverage_tier"], "posted_usable_metrics")
            self.assertTrue(rows["rows"][0]["post_performance_label_eligible"])
            self.assertIsNotNone(rows["rows"][0]["post_performance_target_score"])
            self.assertEqual(rows["rows"][0]["post_performance_label_reason"], "eligible_usable_metrics")
            self.assertTrue(rows["rows"][0]["post_performance_recoverable"])
            self.assertEqual(rows["rows"][0]["post_performance_missing_fields"], [])
            self.assertEqual(rows["rows"][0]["post_performance_recoverability_reason"], "eligible_usable_metrics")

            rollups = query_clip_registry(
                mode="posted-performance-rollups",
                registry_path=registry_path,
                platform="youtube",
                account_id="acct-1",
            )
            self.assertEqual(rollups["row_count"], 1)
            row = rollups["rows"][0]
            by_platform = json.loads(row["by_platform_json"])
            by_hook_mode = json.loads(row["by_hook_mode_json"])
            by_packaging = json.loads(row["by_packaging_strategy_json"])
            by_tier = json.loads(row["post_performance_coverage_tiers_json"])
            by_missing = json.loads(row["post_performance_missing_field_counts_json"])
            self.assertEqual(row["post_count"], 1)
            self.assertEqual(row["snapshot_count"], 2)
            self.assertEqual(row["metrics_coverage_status"], "complete")
            self.assertEqual(row["post_performance_eligible_snapshot_count"], 2)
            self.assertEqual(row["post_performance_eligible_post_count"], 1)
            self.assertEqual(row["post_performance_recoverable_snapshot_count"], 2)
            self.assertEqual(row["post_performance_recoverable_post_count"], 1)
            self.assertEqual(by_tier["posted_usable_metrics"], 2)
            self.assertEqual(by_missing, {})
            self.assertEqual(by_platform["youtube"]["sum_view_count"], 400)
            self.assertEqual(by_hook_mode["natural"]["sum_like_count"], 40)
            self.assertEqual(by_packaging["tight_context_then_payoff"]["sum_comment_count"], 6)

    def test_multiple_post_ledgers_for_one_candidate_preserve_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, ledger, candidate_id = _prepare_posted_candidate(root)

            second_ledger = record_post_ledger(
                json.loads(Path(ledger["manifest_path"]).read_text(encoding="utf-8"))["posted_records"][0]["export_batch_manifest_path"],
                workflow_run_id="workflow-2",
                platform="tiktok",
                account_id="acct-2",
                output_path=root / "posted" / "ledger-2.posted_highlight_ledger.json",
            )
            refresh_clip_registry(root, registry_path=registry_path)

            posted = query_clip_registry(
                mode="candidate-lifecycles",
                candidate_id=candidate_id,
                lifecycle_state="posted",
                registry_path=registry_path,
            )
            post_rows = query_clip_registry(
                mode="post-ledger-records",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )

            self.assertTrue(second_ledger["ok"])
            self.assertEqual(posted["row_count"], 1)
            self.assertEqual(post_rows["row_count"], 2)

    def test_sparse_metrics_snapshot_is_ineligible_for_post_performance_label(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, ledger, candidate_id = _prepare_posted_candidate(root)
            record_posted_metrics_snapshot(
                ledger["manifest_path"],
                workflow_run_id="workflow-1",
                platform="youtube",
                account_id="acct-1",
                output_path=root / "metrics" / "sparse.posted_highlight_metrics_snapshot.json",
                view_count=100,
                average_watch_time_seconds=5.0,
            )
            refresh_clip_registry(root, registry_path=registry_path)

            rows = query_clip_registry(
                mode="posted-metrics",
                registry_path=registry_path,
                candidate_id=candidate_id,
                platform="youtube",
                account_id="acct-1",
            )
            sparse_row = next(
                row for row in rows["rows"] if row["view_count"] == 100 and row["completion_rate"] is None and row["engagement_rate"] is None
            )
            self.assertEqual(sparse_row["post_performance_coverage_tier"], "posted_sparse_metrics")
            self.assertFalse(sparse_row["post_performance_label_eligible"])
            self.assertIsNone(sparse_row["post_performance_target_score"])
            self.assertEqual(sparse_row["post_performance_label_reason"], "insufficient_core_engagement_coverage")
            self.assertTrue(sparse_row["post_performance_recoverable"])
            self.assertEqual(sparse_row["post_performance_missing_fields"], ["completion_rate"])
            self.assertEqual(sparse_row["post_performance_minimum_signal_set"], ["completion_rate", "average_watch_time_seconds"])
            self.assertEqual(sparse_row["post_performance_recoverability_reason"], "one_field_away_from_eligibility")

    def test_malformed_metrics_snapshot_emits_warnings_without_corrupting_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, ledger, candidate_id = _prepare_posted_candidate(root)
            record_posted_metrics_snapshot(
                ledger["manifest_path"],
                workflow_run_id="workflow-1",
                platform="youtube",
                account_id="acct-1",
                output_path=root / "metrics" / "good.posted_highlight_metrics_snapshot.json",
                view_count=100,
            )
            _write_json(
                root / "metrics" / "bad.posted_highlight_metrics_snapshot.json",
                {
                    "schema_version": "posted_highlight_metrics_snapshot_v1",
                    "snapshot_id": "bad-snapshot",
                    "captured_at": "2026-05-04T00:00:00+00:00",
                    "snapshot_count": 1,
                    "snapshots": [
                        {
                            "snapshot_row_id": "broken-row",
                            "post_record_id": "missing-post",
                            "export_id": "missing-export",
                            "candidate_id": candidate_id,
                            "post_ledger_manifest_path": str(root / "posted" / "missing.posted_highlight_ledger.json"),
                        }
                    ],
                },
            )

            result = refresh_clip_registry(root, registry_path=registry_path)
            rows = query_clip_registry(mode="posted-metrics", registry_path=registry_path, candidate_id=candidate_id)

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["warning_count"], 2)
            self.assertEqual(rows["row_count"], 2)

    def test_cli_routes_to_metrics_snapshot_and_report(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--record-posted-metrics-snapshot",
                "--post-ledger-manifest",
                "/tmp/post.json",
                "--platform",
                "youtube",
                "--view-count",
                "100",
            ]
            with patch("run.run_record_posted_metrics_snapshot", return_value={"ok": True, "manifest_path": "/tmp/metrics.json"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())

            __import__("sys").argv = [
                "run.py",
                "--report-posted-performance",
                "--platform",
                "youtube",
            ]
            with patch("run.run_report_posted_performance", return_value={"ok": True, "rows": []}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv

    def test_report_posted_performance_exposes_lineage_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path, ledger, _candidate_id = _prepare_posted_candidate(root)
            record_posted_metrics_snapshot(
                ledger["manifest_path"],
                workflow_run_id="workflow-1",
                platform="youtube",
                account_id="acct-1",
                output_path=root / "metrics" / "snapshot.posted_highlight_metrics_snapshot.json",
                view_count=1200,
                like_count=80,
                comment_count=7,
                share_count=5,
                save_count=3,
                watch_time_seconds=456.7,
                average_watch_time_seconds=12.3,
                completion_rate=0.61,
                engagement_rate=0.12,
            )
            refresh_clip_registry(root, registry_path=registry_path)

            result = report_posted_performance(
                registry_path=registry_path,
                platform="youtube",
                account_id="acct-1",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["lineage_summary"]["selected_event_type_counts"], {"ability_plus_medal_combo": 1})
            self.assertEqual(result["lineage_summary"]["selected_producer_family_counts"], {"runtime": 1})


if __name__ == "__main__":
    unittest.main()

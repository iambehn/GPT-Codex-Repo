from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.highlight_export_batch import create_highlight_export_batch, materialize_synthetic_post_coverage, record_post_ledger
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.hook_candidate_export import derive_hook_candidates
from pipeline.workflow_run_state import create_workflow_run
from run import main as run_main


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fused_sidecar(path: Path, *, game: str, source: Path, review_status: str = "approved") -> None:
    _write_json(
        path,
        {
            "schema_version": "fused_analysis_v1",
            "fusion_id": f"fusion-{path.stem}",
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
                "events": {"fused-event-1": {"review_status": review_status}},
            },
            "sidecar_path": str(path.resolve()),
        },
    )


class HighlightExportBatchTests(unittest.TestCase):
    def test_create_highlight_export_batch_writes_deterministic_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
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
            workflow = create_workflow_run(
                "export_queue",
                registry_path=registry_path,
                output_path=root / "workflow" / "export.workflow_run.json",
            )

            first = create_highlight_export_batch(
                registry_path=registry_path,
                workflow_run_id=workflow["workflow_run_id"],
                output_path=root / "exports" / "batch.highlight_export_batch.json",
            )
            second = create_highlight_export_batch(
                registry_path=registry_path,
                workflow_run_id=workflow["workflow_run_id"],
                output_path=root / "exports" / "batch.highlight_export_batch.json",
            )

            self.assertTrue(first["ok"])
            self.assertEqual(first["export_batch_id"], second["export_batch_id"])
            manifest = json.loads(Path(first["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "highlight_export_batch_v1")
            self.assertEqual(manifest["workflow_run_id"], workflow["workflow_run_id"])
            self.assertEqual(manifest["export_count"], 1)
            row = manifest["exports"][0]
            self.assertEqual(row["export_status"], "exported")
            self.assertTrue(str(row["hook_mode"]))
            self.assertTrue(Path(row["otio_path"]).exists())

    def test_record_post_ledger_writes_generic_posted_records(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "media" / "alpha.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"video")
            fused_path = root / "fused" / "alpha.fused_analysis.json"
            registry_path = root / "registry.sqlite"
            _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            export_highlight_selection(fused_sidecar=fused_path, output_path=root / "selection" / "alpha.highlight_selection.json")
            refresh_clip_registry(root, registry_path=registry_path)
            export_batch = create_highlight_export_batch(
                registry_path=registry_path,
                output_path=root / "exports" / "batch.highlight_export_batch.json",
            )

            result = record_post_ledger(
                export_batch["manifest_path"],
                platform="youtube",
                account_id="acct-1",
                output_path=root / "posted" / "ledger.posted_highlight_ledger.json",
            )

            self.assertTrue(result["ok"])
            ledger = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(ledger["schema_version"], "posted_highlight_ledger_v1")
            self.assertEqual(ledger["posted_count"], 1)
            record = ledger["posted_records"][0]
            self.assertEqual(record["platform"], "youtube")
            self.assertEqual(record["account_id"], "acct-1")
            self.assertEqual(record["post_status"], "posted")

    def test_registry_refresh_promotes_exported_and_posted_and_supports_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
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

            first = refresh_clip_registry(root, registry_path=registry_path)
            self.assertTrue(first["ok"])
            exported = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="exported",
                registry_path=registry_path,
            )
            self.assertEqual(exported["row_count"], 1)
            candidate_id = exported["rows"][0]["candidate_id"]
            self.assertTrue(exported["rows"][0]["export_artifact_path"].endswith(".otio.json"))

            ledger = record_post_ledger(
                export_batch["manifest_path"],
                workflow_run_id=workflow["workflow_run_id"],
                platform="youtube",
                account_id="acct-1",
                output_path=root / "posted" / "ledger.posted_highlight_ledger.json",
            )
            second = refresh_clip_registry(root, registry_path=registry_path)
            third = refresh_clip_registry(root, registry_path=registry_path)

            self.assertTrue(second["ok"])
            self.assertTrue(third["ok"])
            posted = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="posted",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(posted["row_count"], 1)
            transitions = json.loads(posted["rows"][0]["transitions_json"])
            self.assertEqual(len(transitions), 4)
            export_query = query_clip_registry(
                mode="highlight-exports",
                workflow_run_id=workflow["workflow_run_id"],
                export_status="exported",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            post_query = query_clip_registry(
                mode="post-ledger-records",
                workflow_run_id=workflow["workflow_run_id"],
                post_status="posted",
                platform="youtube",
                candidate_id=candidate_id,
                registry_path=registry_path,
            )
            self.assertEqual(export_query["row_count"], 1)
            self.assertEqual(post_query["row_count"], 1)
            self.assertEqual(post_query["rows"][0]["candidate_id"], candidate_id)
            self.assertTrue(posted["rows"][0]["post_ledger_path"].endswith(".posted_highlight_ledger.json"))
            self.assertEqual(ledger["posted_count"], 1)

    def test_materialize_synthetic_post_coverage_creates_downstream_artifacts_and_refreshes_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = root / "registry.sqlite"
            for name in ("alpha", "beta"):
                media = root / "media" / f"{name}.mp4"
                media.parent.mkdir(parents=True, exist_ok=True)
                media.write_bytes(b"video")
                fused_path = root / "fused" / f"{name}.fused_analysis.json"
                _fused_sidecar(fused_path, game="marvel_rivals", source=media)
            refresh_clip_registry(root, registry_path=registry_path)
            derive_hook_candidates(root / "fused" / "alpha.fused_analysis.json", registry_path=registry_path, output_path=root / "hooks" / "alpha.hook_candidates.json")
            derive_hook_candidates(root / "fused" / "beta.fused_analysis.json", registry_path=registry_path, output_path=root / "hooks" / "beta.hook_candidates.json")
            refresh_clip_registry(root, registry_path=registry_path)

            result = materialize_synthetic_post_coverage(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct",
                output_root=root / "synthetic_post_coverage",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["candidate_count"], 2)
            self.assertEqual(result["created_selection_count"], 2)
            self.assertEqual(result["posted_candidate_count_after_refresh"], 2)
            self.assertTrue(Path(result["export_manifest_path"]).exists())
            self.assertTrue(Path(result["post_ledger_manifest_path"]).exists())
            self.assertTrue(Path(result["metrics_snapshot_manifest_path"]).exists())

            posted = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="posted",
                registry_path=registry_path,
                game="marvel_rivals",
            )
            metrics = query_clip_registry(
                mode="posted-metrics",
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct",
            )
            self.assertEqual(posted["row_count"], 2)
            self.assertEqual(metrics["row_count"], 2)
            self.assertTrue(all(row["post_performance_label_eligible"] for row in metrics["rows"]))
            self.assertTrue(all(row["post_performance_coverage_tier"] == "posted_usable_metrics" for row in metrics["rows"]))
            self.assertTrue(all(json.loads(row["metadata_json"])["synthetic_benchmark"] for row in metrics["rows"]))

    def test_materialize_synthetic_post_coverage_can_include_rejected_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = root / "registry.sqlite"

            approved_media = root / "media" / "approved.mp4"
            approved_media.parent.mkdir(parents=True, exist_ok=True)
            approved_media.write_bytes(b"video")
            rejected_media = root / "media" / "rejected.mp4"
            rejected_media.write_bytes(b"video")

            _fused_sidecar(root / "fused" / "approved.fused_analysis.json", game="marvel_rivals", source=approved_media, review_status="approved")
            _fused_sidecar(root / "fused" / "rejected.fused_analysis.json", game="marvel_rivals", source=rejected_media, review_status="rejected")
            refresh_clip_registry(root, registry_path=registry_path)

            baseline = materialize_synthetic_post_coverage(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct",
                output_root=root / "synthetic_approved_only",
            )
            self.assertTrue(baseline["ok"])
            self.assertEqual(baseline["candidate_count"], 1)

            rejected_candidate_id = next(
                row["candidate_id"]
                for row in query_clip_registry(
                    mode="candidate-lifecycles",
                    registry_path=registry_path,
                    game="marvel_rivals",
                )["rows"]
                if row["latest_review_status"] == "rejected"
            )
            rejected_post_before = query_clip_registry(
                mode="candidate-lifecycles",
                registry_path=registry_path,
                game="marvel_rivals",
                candidate_id=rejected_candidate_id,
            )
            self.assertEqual(rejected_post_before["rows"][0]["lifecycle_state"], "rejected")

            result = materialize_synthetic_post_coverage(
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct-rejected",
                output_root=root / "synthetic_with_rejected",
                include_rejected=True,
            )

            self.assertTrue(result["ok"])
            self.assertTrue(result["include_rejected"])
            self.assertEqual(result["candidate_count"], 1)

            rejected_post_after = query_clip_registry(
                mode="candidate-lifecycles",
                lifecycle_state="posted",
                registry_path=registry_path,
                game="marvel_rivals",
                candidate_id=rejected_candidate_id,
            )
            self.assertEqual(rejected_post_after["row_count"], 1)
            metrics = query_clip_registry(
                mode="posted-metrics",
                registry_path=registry_path,
                game="marvel_rivals",
                platform="youtube",
                account_id="synthetic-acct-rejected",
                candidate_id=rejected_candidate_id,
            )
            self.assertEqual(metrics["row_count"], 1)
            self.assertTrue(metrics["rows"][0]["post_performance_label_eligible"])

    def test_cli_routes_to_export_batch_and_post_ledger(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = [
                "run.py",
                "--create-highlight-export-batch",
                "--workflow-run-id",
                "workflow-123",
            ]
            with patch("run.run_create_highlight_export_batch", return_value={"ok": True, "manifest_path": "/tmp/export.json"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())

            __import__("sys").argv = [
                "run.py",
                "--record-post-ledger",
                "--export-manifest",
                "/tmp/export.json",
                "--platform",
                "youtube",
            ]
            with patch("run.run_record_post_ledger", return_value={"ok": True, "manifest_path": "/tmp/post.json"}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv


if __name__ == "__main__":
    unittest.main()

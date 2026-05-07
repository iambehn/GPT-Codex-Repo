from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.approval_target_dataset_adapter import adapt_approval_target_dataset
from pipeline.approval_target_dataset import build_approval_target_dataset
from pipeline.clip_registry import refresh_clip_registry, transition_candidate_lifecycle
from pipeline.shadow_model_training import train_shadow_ranking_model


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fused_sidecar(
    path: Path,
    *,
    game: str,
    source: Path,
    fusion_id: str,
    event_id: str,
    final_score: float,
    review_status: str | None,
) -> None:
    payload = {
        "schema_version": "fused_analysis_v1",
        "fusion_id": fusion_id,
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source.resolve()),
        "normalized_signals": [
            {
                "signal_id": f"{event_id}-signal",
                "signal_type": "character_identity",
                "producer_family": "runtime",
            }
        ],
        "fused_events": [
            {
                "event_id": event_id,
                "event_type": "ability_plus_medal_combo",
                "confidence": 0.84,
                "final_score": final_score,
                "gate_status": "confirmed",
                "synergy_applied": True,
                "minimum_required_signals_met": True,
                "suggested_start_timestamp": 0.5,
                "suggested_end_timestamp": 3.2,
                "contributing_signals": [f"{event_id}-signal"],
                "metadata": {"entity_id": "punisher", "ability_id": "ult"},
            }
        ],
        "sidecar_path": str(path.resolve()),
    }
    if review_status is not None:
        payload["fused_review"] = {
            "session_id": f"{fusion_id}-review",
            "reviewed_event_count": 1,
            "events": {event_id: {"review_status": review_status}},
        }
    _write_json(path, payload)


def _candidate_ids_by_source(registry_path: Path) -> dict[str, str]:
    from pipeline.clip_registry import query_clip_registry
    rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)["rows"]
    return {Path(str(row["source"])).stem: str(row["candidate_id"]) for row in rows}


def _prepare_approval_target_manifest(root: Path) -> Path:
    registry_path = root / "registry.sqlite"
    candidates = [
        ("alpha", 0.91, "approved"),
        ("beta", 0.73, "rejected"),
        ("gamma", 0.69, None),
    ]
    for name, final_score, review_status in candidates:
        media = root / "media" / f"{name}.mp4"
        media.parent.mkdir(parents=True, exist_ok=True)
        media.write_bytes(name.encode("utf-8"))
        fused_path = root / "fused" / f"{name}.fused_analysis.json"
        _fused_sidecar(
            fused_path,
            game="marvel_rivals",
            source=media,
            fusion_id=f"fusion-{name}",
            event_id=f"event-{name}",
            final_score=final_score,
            review_status=review_status,
        )
    refresh_clip_registry(root, registry_path=registry_path)
    candidate_ids = _candidate_ids_by_source(registry_path)
    transition_candidate_lifecycle(candidate_ids["gamma"], "approved", registry_path=registry_path)
    transition_candidate_lifecycle(candidate_ids["gamma"], "selected_for_export", registry_path=registry_path)
    approval_dataset = build_approval_target_dataset(
        registry_path=registry_path,
        output_root=root / "approval_exports",
        game="marvel_rivals",
    )
    return Path(approval_dataset["manifest_path"])


class ApprovalTargetDatasetAdapterTests(unittest.TestCase):
    def test_adapt_approval_target_dataset_writes_minimal_v2_export(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            approval_manifest = _prepare_approval_target_manifest(root)

            result = adapt_approval_target_dataset(
                approval_manifest,
                output_root=root / "adapted_exports",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], "v2_training_dataset_export_v1")
            self.assertEqual(manifest["coverage_counts"]["candidate_count"], 3)
            self.assertEqual(manifest["coverage_counts"]["hook_count"], 0)
            self.assertEqual(manifest["coverage_counts"]["outcome_count"], 0)
            self.assertEqual(manifest["coverage_counts"]["performance_count"], 0)
            self.assertEqual(manifest["source_approval_target_manifest_path"], str(approval_manifest.resolve()))
            self.assertEqual(manifest["source_approval_target_schema_version"], "approval_target_dataset_v1")
            self.assertEqual(manifest["source_approval_target_dataset_id"], json.loads(approval_manifest.read_text(encoding="utf-8"))["dataset_id"])

            candidate_rows = [
                json.loads(line)
                for line in Path(result["dataset_views"]["candidates"]["jsonl_path"]).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(candidate_rows), 3)
            alpha = next(row for row in candidate_rows if Path(str(row["source"])).stem == "alpha")
            beta = next(row for row in candidate_rows if Path(str(row["source"])).stem == "beta")
            gamma = next(row for row in candidate_rows if Path(str(row["source"])).stem == "gamma")
            self.assertEqual(alpha["review_outcome"], "approved")
            self.assertEqual(beta["review_outcome"], "rejected")
            self.assertEqual(gamma["lifecycle_state"], "selected_for_export")
            self.assertFalse(alpha["export_present"])
            self.assertFalse(alpha["post_present"])
            self.assertFalse(alpha["metrics_present"])
            self.assertEqual(alpha["coverage_tier"], "reviewed")

    def test_adapt_approval_target_dataset_rejects_invalid_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            invalid_manifest = root / "invalid.approval.json"
            invalid_manifest.write_text("{}", encoding="utf-8")

            result = adapt_approval_target_dataset(invalid_manifest)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "unsupported_approval_target_manifest")

    def test_adapted_manifest_trains_with_existing_shadow_stack(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            approval_manifest = _prepare_approval_target_manifest(root)
            adapted = adapt_approval_target_dataset(
                approval_manifest,
                output_root=root / "adapted_exports",
            )

            model = train_shadow_ranking_model(
                adapted["manifest_path"],
                model_output_path=root / "models" / "shadow_model.json",
                training_target="approved_or_selected_probability",
            )

            self.assertTrue(model["ok"])
            self.assertEqual(model["status"], "ok")
            self.assertEqual(model["row_count"], 3)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.approval_target_dataset import (
    _approval_label_from_registry_row,
    build_approval_target_dataset,
)
from pipeline.clip_registry import query_clip_registry, refresh_clip_registry, transition_candidate_lifecycle


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
    rows = query_clip_registry(mode="candidate-lifecycles", registry_path=registry_path)["rows"]
    return {Path(str(row["source"])).stem: str(row["candidate_id"]) for row in rows}


def _prepare_registry(root: Path) -> Path:
    registry_path = root / "registry.sqlite"
    candidates = [
        ("alpha", 0.91, "approved"),
        ("beta", 0.73, "rejected"),
        ("gamma", 0.69, None),
        ("delta", 0.66, None),
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
    transition_candidate_lifecycle(candidate_ids["delta"], "approved", registry_path=registry_path)
    transition_candidate_lifecycle(candidate_ids["delta"], "selected_for_export", registry_path=registry_path)
    transition_candidate_lifecycle(candidate_ids["delta"], "exported", registry_path=registry_path)
    transition_candidate_lifecycle(candidate_ids["delta"], "posted", registry_path=registry_path)
    return registry_path


def _prepare_positive_only_registry(root: Path) -> Path:
    registry_path = root / "registry.sqlite"
    candidates = [
        ("alpha", 0.91, "approved"),
        ("gamma", 0.69, None),
        ("delta", 0.66, None),
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
    transition_candidate_lifecycle(candidate_ids["delta"], "approved", registry_path=registry_path)
    transition_candidate_lifecycle(candidate_ids["delta"], "selected_for_export", registry_path=registry_path)
    transition_candidate_lifecycle(candidate_ids["delta"], "exported", registry_path=registry_path)
    transition_candidate_lifecycle(candidate_ids["delta"], "posted", registry_path=registry_path)
    return registry_path


class ApprovalTargetDatasetTests(unittest.TestCase):
    def test_approval_label_uses_explicit_review_outcome(self) -> None:
        self.assertEqual(
            _approval_label_from_registry_row({"latest_review_status": "approved", "lifecycle_state": "posted"}),
            {"approval_label": 1.0, "label_source": "review_outcome"},
        )
        self.assertEqual(
            _approval_label_from_registry_row({"latest_review_status": "rejected", "lifecycle_state": "approved"}),
            {"approval_label": 0.0, "label_source": "review_outcome"},
        )

    def test_approval_label_falls_back_to_lifecycle(self) -> None:
        self.assertEqual(
            _approval_label_from_registry_row({"latest_review_status": None, "lifecycle_state": "selected_for_export"}),
            {"approval_label": 1.0, "label_source": "lifecycle_state"},
        )
        self.assertEqual(
            _approval_label_from_registry_row({"latest_review_status": None, "lifecycle_state": "invalidated"}),
            {"approval_label": 0.0, "label_source": "lifecycle_state"},
        )

    def test_approval_label_excludes_posted_without_review(self) -> None:
        self.assertIsNone(
            _approval_label_from_registry_row({"latest_review_status": None, "lifecycle_state": "posted"})
        )
        self.assertIsNone(
            _approval_label_from_registry_row({"latest_review_status": None, "lifecycle_state": "exported"})
        )

    def test_build_approval_target_dataset_reports_training_ready_both_class_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = _prepare_registry(root)

            result = build_approval_target_dataset(
                registry_path=registry_path,
                output_root=root / "approval_exports",
                game="marvel_rivals",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertTrue(result["training_ready"])
            self.assertEqual(result["readiness_reason"], "ready")
            self.assertEqual(result["positive_count"], 2)
            self.assertEqual(result["negative_count"], 1)
            self.assertEqual(result["row_count"], 3)
            self.assertTrue(Path(result["manifest_path"]).exists())
            self.assertTrue(Path(result["csv_path"]).exists())

            rows_by_source = {Path(str(row["source"])).stem: row for row in result["rows"]}
            self.assertEqual(set(rows_by_source), {"alpha", "beta", "gamma"})
            self.assertEqual(rows_by_source["alpha"]["approval_label"], 1.0)
            self.assertEqual(rows_by_source["alpha"]["label_source"], "review_outcome")
            self.assertEqual(rows_by_source["alpha"]["fused_confidence"], 0.84)
            self.assertEqual(rows_by_source["beta"]["approval_label"], 0.0)
            self.assertEqual(rows_by_source["gamma"]["approval_label"], 1.0)
            self.assertEqual(rows_by_source["gamma"]["label_source"], "lifecycle_state")

    def test_build_approval_target_dataset_reports_one_class_not_training_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = _prepare_positive_only_registry(root)

            result = build_approval_target_dataset(
                registry_path=registry_path,
                output_root=root / "approval_exports",
                game="marvel_rivals",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertFalse(result["training_ready"])
            self.assertEqual(result["readiness_reason"], "no_negative_labels")
            self.assertEqual(result["positive_count"], 2)
            self.assertEqual(result["negative_count"], 0)

    def test_build_approval_target_dataset_reports_no_rows_for_posted_only_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            registry_path = root / "registry.sqlite"
            media = root / "media" / "delta.mp4"
            media.parent.mkdir(parents=True, exist_ok=True)
            media.write_bytes(b"delta")
            _fused_sidecar(
                root / "fused" / "delta.fused_analysis.json",
                game="marvel_rivals",
                source=media,
                fusion_id="fusion-delta",
                event_id="event-delta",
                final_score=0.66,
                review_status=None,
            )
            refresh_clip_registry(root, registry_path=registry_path)
            candidate_id = _candidate_ids_by_source(registry_path)["delta"]
            transition_candidate_lifecycle(candidate_id, "approved", registry_path=registry_path)
            transition_candidate_lifecycle(candidate_id, "selected_for_export", registry_path=registry_path)
            transition_candidate_lifecycle(candidate_id, "exported", registry_path=registry_path)
            transition_candidate_lifecycle(candidate_id, "posted", registry_path=registry_path)

            result = build_approval_target_dataset(
                registry_path=registry_path,
                output_root=root / "approval_exports",
                game="marvel_rivals",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "no_rows")
            self.assertFalse(result["training_ready"])
            self.assertEqual(result["readiness_reason"], "no_rows")
            self.assertEqual(result["row_count"], 0)


if __name__ == "__main__":
    unittest.main()

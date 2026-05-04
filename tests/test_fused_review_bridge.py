from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pipeline.fused_review_bridge as fused_review_bridge
from run import (
    main as run_main,
    run_apply_fused_review,
    run_cleanup_fused_review,
    run_prepare_fused_review,
)
from tests.test_run import _write_gpt_review_repo


def _write_fused_sidecar(
    path: Path,
    *,
    game: str,
    source: Path,
    events: list[dict[str, object]],
    ok: bool = True,
    schema_version: str = "fused_analysis_v1",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": schema_version,
        "fusion_id": f"{game}-{path.stem}",
        "ok": ok,
        "status": "ok" if ok else "failed",
        "game": game,
        "source": str(source.resolve()),
        "sidecar_path": str(path.resolve()),
        "normalized_signals": [],
        "fused_events": events,
        "fusion_summary": {"event_count": len(events)},
        "rule_matches": [],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _event(
    *,
    event_id: str,
    event_type: str,
    final_score: float,
    gate_status: str = "confirmed",
    synergy_applied: bool = False,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "confidence": round(final_score - 0.05, 4),
        "final_score": final_score,
        "gate_status": gate_status,
        "synergy_applied": synergy_applied,
        "suggested_start_timestamp": 4.5,
        "suggested_end_timestamp": 9.25,
        "metadata": {
            "entity_id": "hero_001",
            "ability_id": "ability_001",
            "event_row_id": f"runtime-{event_id}",
            "matched_signal_types": ["medal_visibility", "chat_spike"],
        },
    }


class FusedReviewBridgeTests(unittest.TestCase):
    def test_prepare_fused_review_creates_one_item_per_fused_event(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            source_path = media_root / "alpha.mp4"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_bytes(b"video")
            _write_fused_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.fused_analysis.json",
                game="marvel_rivals",
                source=source_path,
                events=[
                    _event(event_id="event-1", event_type="medal_seen", final_score=0.91, synergy_applied=True),
                    _event(event_id="event-2", event_type="ability_plus_medal_combo", final_score=0.88),
                ],
            )

            with (
                patch.object(fused_review_bridge, "REPO_ROOT", root),
                patch.object(fused_review_bridge, "_materialize_segment", side_effect=self._fake_materialize_segment),
            ):
                result = run_prepare_fused_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                )

            self.assertEqual(result["selection_action_filter"], "review_default")
            self.assertEqual(result["item_count"], 2)
            self.assertEqual(result["items"][0]["event_id"], "event-1")
            self.assertEqual(result["items"][1]["event_id"], "event-2")
            meta = json.loads(Path(result["items"][0]["gpt_meta_path"]).read_text(encoding="utf-8"))
            self.assertEqual(meta["selected_template_id"], "fused_review_bridge")
            self.assertEqual(meta["scoring"]["clip_type"], "fused_event_candidate")
            self.assertEqual(meta["fused_review_bridge"]["event_type"], "medal_seen")

    def test_prepare_fused_review_event_type_filter_limits_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            source_path = media_root / "alpha.mp4"
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_bytes(b"video")
            _write_fused_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.fused_analysis.json",
                game="marvel_rivals",
                source=source_path,
                events=[
                    _event(event_id="event-1", event_type="medal_seen", final_score=0.91),
                    _event(event_id="event-2", event_type="ability_plus_medal_combo", final_score=0.88),
                ],
            )

            with (
                patch.object(fused_review_bridge, "REPO_ROOT", root),
                patch.object(fused_review_bridge, "_materialize_segment", side_effect=self._fake_materialize_segment),
            ):
                result = run_prepare_fused_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                    event_type="medal_seen",
                )

            self.assertEqual(result["item_count"], 1)
            self.assertEqual(result["items"][0]["event_type"], "medal_seen")

    def test_apply_fused_review_updates_fused_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            alpha = media_root / "alpha.mp4"
            bravo = media_root / "bravo.mp4"
            alpha.parent.mkdir(parents=True, exist_ok=True)
            alpha.write_bytes(b"alpha")
            bravo.write_bytes(b"bravo")

            _write_fused_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.fused_analysis.json",
                game="marvel_rivals",
                source=alpha,
                events=[_event(event_id="event-1", event_type="medal_seen", final_score=0.91)],
            )
            _write_fused_sidecar(
                sidecar_root / "marvel_rivals" / "bravo.fused_analysis.json",
                game="marvel_rivals",
                source=bravo,
                events=[_event(event_id="event-2", event_type="ability_seen", final_score=0.58, gate_status="ambiguous")],
            )

            with (
                patch.object(fused_review_bridge, "REPO_ROOT", root),
                patch.object(fused_review_bridge, "_materialize_segment", side_effect=self._fake_materialize_segment),
            ):
                prepared = run_prepare_fused_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                    action="all_non_skip",
                )

                approved_meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                rejected_meta_path = Path(prepared["items"][1]["gpt_meta_path"])

                approved_meta = json.loads(approved_meta_path.read_text(encoding="utf-8"))
                approved_final = gpt_repo / "accepted" / "marvel_rivals" / f"{approved_meta['clip_id']}.mp4"
                approved_final.parent.mkdir(parents=True, exist_ok=True)
                approved_final.write_bytes(b"approved")
                approved_meta["review_status"] = "accepted"
                approved_meta["reviewed_at"] = "2026-05-02T12:00:00Z"
                approved_meta["final_path"] = str(approved_final)
                approved_meta_path.write_text(json.dumps(approved_meta, indent=2), encoding="utf-8")

                rejected_meta = json.loads(rejected_meta_path.read_text(encoding="utf-8"))
                rejected_meta["review_status"] = "rejected"
                rejected_meta["reviewed_at"] = "2026-05-02T12:05:00Z"
                rejected_meta_path.write_text(json.dumps(rejected_meta, indent=2), encoding="utf-8")

                result = run_apply_fused_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertEqual(result["approved_count"], 1)
            self.assertEqual(result["rejected_count"], 1)

            alpha_sidecar = json.loads((sidecar_root / "marvel_rivals" / "alpha.fused_analysis.json").read_text(encoding="utf-8"))
            bravo_sidecar = json.loads((sidecar_root / "marvel_rivals" / "bravo.fused_analysis.json").read_text(encoding="utf-8"))
            self.assertEqual(alpha_sidecar["fused_review"]["events"]["event-1"]["review_status"], "approved")
            self.assertEqual(bravo_sidecar["fused_review"]["events"]["event-2"]["review_status"], "rejected")
            self.assertNotIn("runtime_review", alpha_sidecar)

    def test_cleanup_fused_review_removes_generated_bridge_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            alpha = media_root / "alpha.mp4"
            alpha.parent.mkdir(parents=True, exist_ok=True)
            alpha.write_bytes(b"alpha")
            _write_fused_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.fused_analysis.json",
                game="marvel_rivals",
                source=alpha,
                events=[_event(event_id="event-1", event_type="medal_seen", final_score=0.91)],
            )

            with (
                patch.object(fused_review_bridge, "REPO_ROOT", root),
                patch.object(fused_review_bridge, "_materialize_segment", side_effect=self._fake_materialize_segment),
            ):
                prepared = run_prepare_fused_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                )
                item = prepared["items"][0]
                meta_path = Path(item["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                final_path = gpt_repo / "accepted" / "marvel_rivals" / f"{meta['clip_id']}.mp4"
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(b"accepted")
                meta["final_path"] = str(final_path)
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = run_cleanup_fused_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertFalse(Path(item["gpt_processed_path"]).exists())
            self.assertFalse(Path(item["gpt_meta_path"]).exists())
            self.assertFalse(final_path.exists())

    def test_cli_routes_to_fused_review_commands(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--prepare-fused-review", "marvel_rivals"]
            with patch("run.run_prepare_fused_review", return_value={"ok": True, "item_count": 1}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv

    @staticmethod
    def _fake_materialize_segment(source_path: Path, output_path: Path, *, start_seconds: float, end_seconds: float) -> None:
        del source_path, start_seconds, end_seconds
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"segment")


if __name__ == "__main__":
    unittest.main()

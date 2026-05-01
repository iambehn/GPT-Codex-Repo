from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pipeline.runtime_review_bridge as runtime_review_bridge
from run import (
    main as run_main,
    run_apply_runtime_review,
    run_cleanup_runtime_review,
    run_prepare_runtime_review,
)
from tests.test_run import _write_gpt_review_repo


def _write_runtime_sidecar(
    path: Path,
    *,
    game: str,
    source: Path,
    highlight_score: float,
    action: str,
    event_types: list[str],
    ok: bool = True,
    schema_version: str = "runtime_analysis_v1",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": schema_version,
        "analysis_id": f"{game}-{path.stem}",
        "ok": ok,
        "status": "ok" if ok else "failed",
        "game": game,
        "source": str(source.resolve()),
        "sidecar_path": str(path.resolve()),
        "game_pack": {"game_id": game},
        "matcher": {
            "status": "ok",
            "frame_count": 12,
            "sample_fps": 4.0,
            "template_count": 3,
            "summary": {"total_confirmed_detections": len(event_types)},
            "top_scores": {},
            "unseen_templates": [],
            "confirmed_detections": [{"asset_id": f"{event_type}-asset"} for event_type in event_types],
        },
        "events": {
            "status": "ok",
            "event_count": len(event_types),
            "event_summary": {"counts_by_event_type": {event_type: 1 for event_type in event_types}},
            "rows": [
                {
                    "event_id": f"{path.stem}-{index}",
                    "event_type": event_type,
                    "asset_id": f"{event_type}-asset",
                    "roi_ref": "hero_portrait",
                    "timestamp": 1.0,
                    "start_timestamp": 1.0,
                    "end_timestamp": 1.5,
                    "confidence": 0.95,
                    "source_detection_count": 3,
                }
                for index, event_type in enumerate(event_types)
            ],
        },
        "runtime_review": {
            "highlight_score": highlight_score,
            "recommended_action": action,
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class RuntimeReviewBridgeTests(unittest.TestCase):
    def test_prepare_runtime_review_selects_highlights_and_top_inspect_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            media_root.mkdir(parents=True, exist_ok=True)
            alpha = media_root / "alpha.mp4"
            bravo = media_root / "bravo.mp4"
            charlie = media_root / "charlie.mp4"
            delta = media_root / "delta.mp4"
            for path, payload in (
                (alpha, b"alpha"),
                (bravo, b"bravo"),
                (charlie, b"charlie"),
                (delta, b"delta"),
            ):
                path.write_bytes(payload)

            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.runtime_analysis.json",
                game="marvel_rivals",
                source=alpha,
                highlight_score=0.91,
                action="highlight_candidate",
                event_types=["medal_seen"],
            )
            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "bravo.runtime_analysis.json",
                game="marvel_rivals",
                source=bravo,
                highlight_score=0.82,
                action="highlight_candidate",
                event_types=["ability_seen"],
            )
            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "charlie.runtime_analysis.json",
                game="marvel_rivals",
                source=charlie,
                highlight_score=0.56,
                action="inspect",
                event_types=["pov_character_identified"],
            )
            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "delta.runtime_analysis.json",
                game="marvel_rivals",
                source=delta,
                highlight_score=0.42,
                action="inspect",
                event_types=["ability_seen"],
            )

            with patch.object(runtime_review_bridge, "REPO_ROOT", root):
                result = run_prepare_runtime_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                )

            self.assertEqual(result["selection_action_filter"], "review_default")
            self.assertEqual(result["item_count"], 4)
            self.assertEqual(Path(result["items"][0]["source"]).name, "alpha.mp4")
            self.assertEqual(Path(result["items"][1]["source"]).name, "bravo.mp4")
            self.assertEqual(Path(result["items"][2]["source"]).name, "charlie.mp4")
            self.assertEqual(Path(result["items"][3]["source"]).name, "delta.mp4")
            meta = json.loads(Path(result["items"][0]["gpt_meta_path"]).read_text(encoding="utf-8"))
            self.assertEqual(meta["selected_template_id"], "runtime_review_bridge")
            self.assertEqual(meta["scoring"]["clip_type"], "runtime_candidate")
            self.assertTrue(meta["runtime_review_bridge"]["bridge_owned"])

    def test_prepare_runtime_review_highlight_only_excludes_inspect(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            media_root.mkdir(parents=True, exist_ok=True)
            alpha = media_root / "alpha.mp4"
            bravo = media_root / "bravo.mp4"
            alpha.write_bytes(b"alpha")
            bravo.write_bytes(b"bravo")

            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.runtime_analysis.json",
                game="marvel_rivals",
                source=alpha,
                highlight_score=0.91,
                action="highlight_candidate",
                event_types=["medal_seen"],
            )
            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "bravo.runtime_analysis.json",
                game="marvel_rivals",
                source=bravo,
                highlight_score=0.42,
                action="inspect",
                event_types=["ability_seen"],
            )

            with patch.object(runtime_review_bridge, "REPO_ROOT", root):
                result = run_prepare_runtime_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                    action="highlight_candidate",
                )

            self.assertEqual(result["item_count"], 1)
            self.assertEqual(Path(result["items"][0]["source"]).name, "alpha.mp4")

    def test_apply_runtime_review_updates_sidecars_without_proxy_review(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            media_root.mkdir(parents=True, exist_ok=True)
            alpha = media_root / "alpha.mp4"
            bravo = media_root / "bravo.mp4"
            alpha.write_bytes(b"alpha")
            bravo.write_bytes(b"bravo")

            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.runtime_analysis.json",
                game="marvel_rivals",
                source=alpha,
                highlight_score=0.91,
                action="highlight_candidate",
                event_types=["medal_seen"],
            )
            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "bravo.runtime_analysis.json",
                game="marvel_rivals",
                source=bravo,
                highlight_score=0.56,
                action="inspect",
                event_types=["ability_seen"],
            )

            with patch.object(runtime_review_bridge, "REPO_ROOT", root):
                prepared = run_prepare_runtime_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                )

                alpha_meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                bravo_meta_path = Path(prepared["items"][1]["gpt_meta_path"])

                alpha_meta = json.loads(alpha_meta_path.read_text(encoding="utf-8"))
                alpha_final = gpt_repo / "accepted" / "marvel_rivals" / f"{alpha_meta['clip_id']}.mp4"
                alpha_final.parent.mkdir(parents=True, exist_ok=True)
                alpha_final.write_bytes(b"approved")
                alpha_meta["review_status"] = "accepted"
                alpha_meta["reviewed_at"] = "2026-05-01T12:00:00Z"
                alpha_meta["final_path"] = str(alpha_final)
                alpha_meta_path.write_text(json.dumps(alpha_meta, indent=2), encoding="utf-8")

                bravo_meta = json.loads(bravo_meta_path.read_text(encoding="utf-8"))
                bravo_meta["review_status"] = "rejected"
                bravo_meta["reviewed_at"] = "2026-05-01T12:05:00Z"
                bravo_meta_path.write_text(json.dumps(bravo_meta, indent=2), encoding="utf-8")

                result = run_apply_runtime_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertEqual(result["approved_count"], 1)
            self.assertEqual(result["rejected_count"], 1)

            alpha_sidecar = json.loads((sidecar_root / "marvel_rivals" / "alpha.runtime_analysis.json").read_text(encoding="utf-8"))
            bravo_sidecar = json.loads((sidecar_root / "marvel_rivals" / "bravo.runtime_analysis.json").read_text(encoding="utf-8"))
            self.assertEqual(alpha_sidecar["runtime_review"]["review_status"], "approved")
            self.assertEqual(bravo_sidecar["runtime_review"]["review_status"], "rejected")
            self.assertNotIn("proxy_review", alpha_sidecar)

    def test_cleanup_runtime_review_removes_generated_bridge_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            media_root.mkdir(parents=True, exist_ok=True)
            alpha = media_root / "alpha.mp4"
            alpha.write_bytes(b"alpha")
            _write_runtime_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.runtime_analysis.json",
                game="marvel_rivals",
                source=alpha,
                highlight_score=0.91,
                action="highlight_candidate",
                event_types=["medal_seen"],
            )

            with patch.object(runtime_review_bridge, "REPO_ROOT", root):
                prepared = run_prepare_runtime_review(
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

                result = run_cleanup_runtime_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertFalse(Path(item["gpt_processed_path"]).exists())
            self.assertFalse(Path(item["gpt_meta_path"]).exists())
            self.assertFalse(final_path.exists())

    def test_cli_routes_to_runtime_review_commands(self) -> None:
        original_argv = __import__("sys").argv
        try:
            __import__("sys").argv = ["run.py", "--prepare-runtime-review", "marvel_rivals"]
            with patch("run.run_prepare_runtime_review", return_value={"ok": True, "item_count": 1}):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            self.assertIn('"ok": true', buffer.getvalue())
        finally:
            __import__("sys").argv = original_argv

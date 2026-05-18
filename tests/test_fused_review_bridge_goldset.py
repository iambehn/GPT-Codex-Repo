from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline.fused_review_bridge as fused_review_bridge
from tests.test_fused_review_bridge import _event, _write_fused_sidecar
from tests.test_run import _write_gpt_review_repo


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = REPO_ROOT / "tests" / "fixtures" / "fused_review_bridge_goldsets" / "fused_review_bridge_goldset.json"


class FusedReviewBridgeGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "fused_review_bridge_goldset_v1")
        self.assertGreaterEqual(len(payload["cases"]), 4)

    def test_fused_review_bridge_scenarios_match_goldset_expectations(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                with tempfile.TemporaryDirectory() as tempdir:
                    root = Path(tempdir)
                    sidecar_root = root / "sidecars"
                    media_root = root / "media"
                    gpt_repo = root / "gpt"
                    _write_gpt_review_repo(gpt_repo)
                    self._write_sidecars(
                        sidecar_root=sidecar_root,
                        media_root=media_root,
                        game=str(case["game"]),
                        sidecars=list(case.get("sidecars", [])),
                    )

                    with (
                        patch.object(fused_review_bridge, "REPO_ROOT", root),
                        patch.object(fused_review_bridge, "_materialize_segment", side_effect=self._fake_materialize_segment),
                    ):
                        scenario = str(case["scenario"])
                        if scenario == "prepare":
                            self._assert_prepare_case(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
                        elif scenario == "apply":
                            self._assert_apply_case(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
                        elif scenario == "cleanup":
                            self._assert_cleanup_case(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
                        else:
                            self.fail(f"unsupported scenario: {scenario}")

    def _write_sidecars(
        self,
        *,
        sidecar_root: Path,
        media_root: Path,
        game: str,
        sidecars: list[dict[str, object]],
    ) -> None:
        media_root.mkdir(parents=True, exist_ok=True)
        for row in sidecars:
            stem = str(row["stem"])
            source = media_root / f"{stem}.mp4"
            source.write_bytes(stem.encode("utf-8"))
            events = [
                _event(
                    event_id=str(item["event_id"]),
                    event_type=str(item["event_type"]),
                    final_score=float(item["final_score"]),
                    gate_status=str(item.get("gate_status", "confirmed")),
                    synergy_applied=bool(item.get("synergy_applied", False)),
                )
                for item in list(row.get("events", []))
            ]
            _write_fused_sidecar(
                sidecar_root / game / f"{stem}.fused_analysis.json",
                game=game,
                source=source,
                events=events,
            )

    def _prepare_session(self, case: dict[str, object], *, sidecar_root: Path, gpt_repo: Path) -> dict[str, object]:
        kwargs: dict[str, object] = {
            "sidecar_root": sidecar_root,
            "gpt_repo": gpt_repo,
        }
        selection_mode = str(case.get("selection_mode", "review_default"))
        if selection_mode != "review_default":
            kwargs["action"] = selection_mode
        if case.get("event_type"):
            kwargs["event_type"] = case["event_type"]
        return fused_review_bridge.prepare_fused_review(str(case["game"]), **kwargs)

    def _assert_prepare_case(self, case: dict[str, object], *, sidecar_root: Path, gpt_repo: Path) -> None:
        result = self._prepare_session(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
        self.assertEqual(result["item_count"], case["expected_item_count"])
        self.assertEqual(
            [str(row["event_id"]) for row in result["items"]],
            list(case["expected_ordered_event_ids"]),
        )
        if "expected_selection_action_filter" in case:
            self.assertEqual(result["selection_action_filter"], case["expected_selection_action_filter"])
        if result["items"]:
            meta = json.loads(Path(result["items"][0]["gpt_meta_path"]).read_text(encoding="utf-8"))
            if "expected_selected_template_id" in case:
                self.assertEqual(meta["selected_template_id"], case["expected_selected_template_id"])
            if "expected_clip_type" in case:
                self.assertEqual(meta["scoring"]["clip_type"], case["expected_clip_type"])
            if "expected_first_event_type" in case:
                self.assertEqual(meta["fused_review_bridge"]["event_type"], case["expected_first_event_type"])

    def _assert_apply_case(self, case: dict[str, object], *, sidecar_root: Path, gpt_repo: Path) -> None:
        prepared = self._prepare_session(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
        self._apply_review_updates(prepared=prepared, gpt_repo=gpt_repo, updates=list(case.get("review_updates", [])))
        result = fused_review_bridge.apply_fused_review(prepared["manifest_path"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["approved_count"], case["expected_approved_count"])
        self.assertEqual(result["rejected_count"], case["expected_rejected_count"])
        for compound_key, expected_status in dict(case.get("expected_sidecar_review_statuses", {})).items():
            stem, event_id = compound_key.split(":", 1)
            sidecar = json.loads((sidecar_root / str(case["game"]) / f"{stem}.fused_analysis.json").read_text(encoding="utf-8"))
            self.assertEqual(sidecar["fused_review"]["events"][event_id]["review_status"], expected_status)
            self.assertNotIn("runtime_review", sidecar)

    def _assert_cleanup_case(self, case: dict[str, object], *, sidecar_root: Path, gpt_repo: Path) -> None:
        prepared = self._prepare_session(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
        self._apply_review_updates(prepared=prepared, gpt_repo=gpt_repo, updates=list(case.get("review_updates", [])))
        item = prepared["items"][0]
        meta_path = Path(str(item["gpt_meta_path"]))
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        final_path = Path(str(meta["final_path"]))
        result = fused_review_bridge.cleanup_fused_review(prepared["manifest_path"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["cleanup_count"], case["expected_cleanup_count"])
        self.assertFalse(Path(item["gpt_processed_path"]).exists())
        self.assertFalse(meta_path.exists())
        self.assertFalse(final_path.exists())

    def _apply_review_updates(self, *, prepared: dict[str, object], gpt_repo: Path, updates: list[dict[str, object]]) -> None:
        by_event_id = {str(item["event_id"]): item for item in list(prepared.get("items", []))}
        for row in updates:
            event_id = str(row["event_id"])
            item = by_event_id[event_id]
            meta_path = Path(str(item["gpt_meta_path"]))
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if row.get("write_final_clip"):
                final_path = gpt_repo / "accepted" / str(meta["game"]) / f"{meta['clip_id']}.mp4"
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(b"accepted")
                meta["final_path"] = str(final_path)
            if "review_status" in row:
                meta["review_status"] = row["review_status"]
            if "reviewed_at" in row:
                meta["reviewed_at"] = row["reviewed_at"]
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    @staticmethod
    def _fake_materialize_segment(source_path: Path, output_path: Path, *, start_seconds: float, end_seconds: float) -> None:
        del source_path, start_seconds, end_seconds
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"segment")


if __name__ == "__main__":
    unittest.main()

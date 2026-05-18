from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline.runtime_review_bridge as runtime_review_bridge
from tests.test_run import _write_gpt_review_repo
from tests.test_runtime_review_bridge import _write_runtime_sidecar


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = (
    REPO_ROOT / "tests" / "fixtures" / "runtime_review_bridge_goldsets" / "runtime_review_bridge_goldset.json"
)


class RuntimeReviewBridgeGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "runtime_review_bridge_goldset_v1")
        self.assertGreaterEqual(len(payload["cases"]), 4)

    def test_runtime_review_bridge_scenarios_match_goldset_expectations(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                with tempfile.TemporaryDirectory() as tempdir:
                    root = Path(tempdir)
                    sidecar_root = root / "sidecars"
                    media_root = root / "media"
                    gpt_repo = root / "gpt"
                    _write_gpt_review_repo(gpt_repo)
                    created = self._write_sidecars(
                        sidecar_root=sidecar_root,
                        media_root=media_root,
                        game=str(case["game"]),
                        sidecars=list(case.get("sidecars", [])),
                    )

                    with patch.object(runtime_review_bridge, "REPO_ROOT", root):
                        scenario = str(case["scenario"])
                        if scenario == "prepare":
                            self._assert_prepare_case(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
                        elif scenario == "apply":
                            self._assert_apply_case(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo, created=created)
                        elif scenario == "cleanup":
                            self._assert_cleanup_case(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo, created=created)
                        else:
                            self.fail(f"unsupported scenario: {scenario}")

    def _write_sidecars(
        self,
        *,
        sidecar_root: Path,
        media_root: Path,
        game: str,
        sidecars: list[dict[str, object]],
    ) -> dict[str, Path]:
        media_root.mkdir(parents=True, exist_ok=True)
        created: dict[str, Path] = {}
        for row in sidecars:
            stem = str(row["stem"])
            source = media_root / f"{stem}.mp4"
            source.write_bytes(stem.encode("utf-8"))
            created[stem] = source
            _write_runtime_sidecar(
                sidecar_root / game / f"{stem}.runtime_analysis.json",
                game=game,
                source=source,
                highlight_score=float(row["highlight_score"]),
                action=str(row["action"]),
                event_types=[str(value) for value in list(row.get("event_types", []))],
            )
        return created

    def _assert_prepare_case(self, case: dict[str, object], *, sidecar_root: Path, gpt_repo: Path) -> None:
        result = runtime_review_bridge.prepare_runtime_review(
            str(case["game"]),
            sidecar_root=sidecar_root,
            gpt_repo=gpt_repo,
            action=case.get("action_filter"),
        )
        self.assertEqual(result["selection_action_filter"], case["expected_selection_action_filter"])
        self.assertEqual(result["item_count"], case["expected_item_count"])
        self.assertEqual(
            [Path(row["source"]).name for row in result["items"]],
            list(case["expected_ordered_sources"]),
        )

        if result["items"]:
            meta = json.loads(Path(result["items"][0]["gpt_meta_path"]).read_text(encoding="utf-8"))
            if "expected_selected_template_id" in case:
                self.assertEqual(meta["selected_template_id"], case["expected_selected_template_id"])
            if "expected_clip_type" in case:
                self.assertEqual(meta["scoring"]["clip_type"], case["expected_clip_type"])
            if "expected_bridge_owned" in case:
                self.assertEqual(meta["runtime_review_bridge"]["bridge_owned"], case["expected_bridge_owned"])

    def _assert_apply_case(
        self,
        case: dict[str, object],
        *,
        sidecar_root: Path,
        gpt_repo: Path,
        created: dict[str, Path],
    ) -> None:
        prepared = runtime_review_bridge.prepare_runtime_review(
            str(case["game"]),
            sidecar_root=sidecar_root,
            gpt_repo=gpt_repo,
        )
        self._apply_review_updates(prepared=prepared, gpt_repo=gpt_repo, updates=list(case.get("review_updates", [])))
        result = runtime_review_bridge.apply_runtime_review(prepared["manifest_path"])

        self.assertTrue(result["ok"])
        self.assertEqual(result["approved_count"], case["expected_approved_count"])
        self.assertEqual(result["rejected_count"], case["expected_rejected_count"])

        expected_statuses = dict(case.get("expected_sidecar_review_statuses", {}))
        for stem, expected_status in expected_statuses.items():
            sidecar_path = sidecar_root / str(case["game"]) / f"{stem}.runtime_analysis.json"
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            self.assertEqual(sidecar["runtime_review"]["review_status"], expected_status)
            if case.get("expected_proxy_review_present") is False:
                self.assertNotIn("proxy_review", sidecar)

    def _assert_cleanup_case(
        self,
        case: dict[str, object],
        *,
        sidecar_root: Path,
        gpt_repo: Path,
        created: dict[str, Path],
    ) -> None:
        del created
        prepared = runtime_review_bridge.prepare_runtime_review(
            str(case["game"]),
            sidecar_root=sidecar_root,
            gpt_repo=gpt_repo,
        )
        self._apply_review_updates(prepared=prepared, gpt_repo=gpt_repo, updates=list(case.get("review_updates", [])))
        item = prepared["items"][0]
        result = runtime_review_bridge.cleanup_runtime_review(prepared["manifest_path"])

        self.assertTrue(result["ok"])
        self.assertEqual(result["cleanup_count"], case["expected_cleanup_count"])
        self.assertFalse(Path(item["gpt_processed_path"]).exists())
        self.assertFalse(Path(item["gpt_meta_path"]).exists())
        meta_path = Path(item["gpt_meta_path"])
        final_path = gpt_repo / "accepted" / str(case["game"]) / f"{meta_path.stem.replace('.meta', '')}.mp4"
        self.assertFalse(final_path.exists())

    def _apply_review_updates(self, *, prepared: dict[str, object], gpt_repo: Path, updates: list[dict[str, object]]) -> None:
        by_stem = {Path(str(item["source"])).stem: item for item in list(prepared.get("items", []))}
        for row in updates:
            stem = str(row["stem"])
            item = by_stem[stem]
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


if __name__ == "__main__":
    unittest.main()

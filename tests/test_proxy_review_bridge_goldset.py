from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline.proxy_review_bridge as proxy_review_bridge
from tests.test_run import _write_gpt_review_repo, _write_proxy_sidecar


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = REPO_ROOT / "tests" / "fixtures" / "proxy_review_bridge_goldsets" / "proxy_review_bridge_goldset.json"


class ProxyReviewBridgeGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "proxy_review_bridge_goldset_v1")
        self.assertGreaterEqual(len(payload["cases"]), 4)

    def test_proxy_review_bridge_scenarios_match_goldset_expectations(self) -> None:
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
                    batch_report = self._write_batch_report(
                        root=root,
                        game=str(case["game"]),
                        created=created,
                        rows=list(case.get("batch_report_rows", [])),
                    )

                    with patch.object(proxy_review_bridge, "REPO_ROOT", root):
                        scenario = str(case["scenario"])
                        if scenario == "prepare":
                            self._assert_prepare_case(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo, batch_report=batch_report)
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
    ) -> dict[str, dict[str, Path]]:
        media_root.mkdir(parents=True, exist_ok=True)
        created: dict[str, dict[str, Path]] = {}
        for row in sidecars:
            stem = str(row["stem"])
            source = media_root / f"{stem}.mp4"
            source.write_bytes(stem.encode("utf-8"))
            sidecar_path = sidecar_root / game / f"{stem}.proxy_scan.json"
            if row.get("empty_windows"):
                sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                sidecar_path.write_text(
                    json.dumps(
                        {
                            "schema_version": "proxy_scan_v1",
                            "game": game,
                            "source": str(source),
                            "window_count": 0,
                            "signal_count": 0,
                            "windows": [],
                        }
                    ),
                    encoding="utf-8",
                )
            else:
                _write_proxy_sidecar(
                    sidecar_path,
                    game=game,
                    source=source,
                    score=float(row["score"]),
                    action=str(row["action"]),
                    sources=[str(value) for value in list(row.get("sources", []))],
                    source_families=[str(value) for value in list(row.get("source_families", []))],
                )
            created[stem] = {"source": source, "sidecar": sidecar_path}
        return created

    def _write_batch_report(
        self,
        *,
        root: Path,
        game: str,
        created: dict[str, dict[str, Path]],
        rows: list[dict[str, object]],
    ) -> Path | None:
        if not rows:
            return None
        payload_rows: list[dict[str, object]] = []
        for row in rows:
            stem = str(row["stem"])
            payload_row: dict[str, object] = {
                "sidecar_path": str(created[stem]["sidecar"]),
                "top_recommended_action": row["top_recommended_action"],
                "top_proxy_score": row["top_proxy_score"],
                "sources": list(row.get("sources", [])),
                "source_families": list(row.get("source_families", [])),
            }
            if row.get("include_source"):
                payload_row["source"] = str(created[stem]["source"])
            payload_rows.append(payload_row)
        batch_report = root / f"{game}.proxy_batch.json"
        batch_report.write_text(json.dumps({"results": payload_rows}, indent=2), encoding="utf-8")
        return batch_report

    def _prepare_session(
        self,
        case: dict[str, object],
        *,
        sidecar_root: Path,
        gpt_repo: Path,
        batch_report: Path | None = None,
    ) -> dict[str, object]:
        kwargs = {
            "sidecar_root": sidecar_root,
            "gpt_repo": gpt_repo,
            "action": str(case.get("action_filter", "download_candidate")),
        }
        if batch_report is not None:
            kwargs["batch_report"] = batch_report
        return proxy_review_bridge.prepare_proxy_review(str(case["game"]), **kwargs)

    def _assert_prepare_case(
        self,
        case: dict[str, object],
        *,
        sidecar_root: Path,
        gpt_repo: Path,
        batch_report: Path | None,
    ) -> None:
        result = self._prepare_session(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo, batch_report=batch_report)
        self.assertEqual(result["item_count"], case["expected_item_count"])
        self.assertEqual(
            [Path(row["source"]).name for row in result["items"]],
            list(case["expected_ordered_sources"]),
        )
        if "expected_selection_action_filter" in case:
            self.assertEqual(result["selection_action_filter"], case["expected_selection_action_filter"])
        if result["items"]:
            meta = json.loads(Path(result["items"][0]["gpt_meta_path"]).read_text(encoding="utf-8"))
            if "expected_selected_template_id" in case:
                self.assertEqual(meta["selected_template_id"], case["expected_selected_template_id"])
            if "expected_clip_type" in case:
                self.assertEqual(meta["scoring"]["clip_type"], case["expected_clip_type"])
            if "expected_highlight_score" in case:
                self.assertEqual(meta["scoring"]["highlight_score"], case["expected_highlight_score"])
            if "expected_bridge_owned" in case:
                self.assertEqual(meta["proxy_review_bridge"]["bridge_owned"], case["expected_bridge_owned"])
            if "expected_top_proxy_score" in case:
                self.assertAlmostEqual(float(result["items"][0]["top_proxy_score"]), float(case["expected_top_proxy_score"]))

    def _assert_apply_case(
        self,
        case: dict[str, object],
        *,
        sidecar_root: Path,
        gpt_repo: Path,
    ) -> None:
        result = self._prepare_session(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
        self._apply_review_updates(prepared=result, gpt_repo=gpt_repo, updates=list(case.get("review_updates", [])))
        applied = proxy_review_bridge.apply_proxy_review(result["manifest_path"])
        self.assertTrue(applied["ok"])
        self.assertEqual(applied["approved_count"], case["expected_approved_count"])
        self.assertEqual(applied["rejected_count"], case["expected_rejected_count"])
        self.assertEqual(applied["unreviewed_count"], case["expected_unreviewed_count"])
        for stem, expected_status in dict(case.get("expected_sidecar_review_statuses", {})).items():
            sidecar = json.loads((sidecar_root / str(case["game"]) / f"{stem}.proxy_scan.json").read_text(encoding="utf-8"))
            self.assertEqual(sidecar["proxy_review"]["review_status"], expected_status)

    def _assert_cleanup_case(
        self,
        case: dict[str, object],
        *,
        sidecar_root: Path,
        gpt_repo: Path,
    ) -> None:
        result = self._prepare_session(case, sidecar_root=sidecar_root, gpt_repo=gpt_repo)
        self._apply_review_updates(prepared=result, gpt_repo=gpt_repo, updates=list(case.get("review_updates", [])))
        item = result["items"][0]
        meta_path = Path(str(item["gpt_meta_path"]))
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        final_path = Path(str(meta["final_path"]))
        cleaned = proxy_review_bridge.cleanup_proxy_review(result["manifest_path"])
        self.assertTrue(cleaned["ok"])
        self.assertEqual(cleaned["cleanup_count"], case["expected_cleanup_count"])
        self.assertFalse(Path(item["gpt_processed_path"]).exists())
        self.assertFalse(meta_path.exists())
        self.assertFalse(final_path.exists())

    def _apply_review_updates(self, *, prepared: dict[str, object], gpt_repo: Path, updates: list[dict[str, object]]) -> None:
        by_stem = {Path(str(item["source"])).stem: item for item in list(prepared.get("items", []))}
        for row in updates:
            stem = str(row["stem"])
            item = by_stem[stem]
            meta_path = Path(str(item["gpt_meta_path"]))
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if row.get("write_final_clip"):
                bucket = str(row.get("final_bucket", "accepted"))
                final_path = gpt_repo / bucket / str(meta["game"]) / f"{meta['clip_id']}.mp4"
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(bucket.encode("utf-8"))
                meta["final_path"] = str(final_path)
            if "review_status" in row:
                meta["review_status"] = row["review_status"]
            if "reviewed_at" in row:
                meta["reviewed_at"] = row["reviewed_at"]
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()

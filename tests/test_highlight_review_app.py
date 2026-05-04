from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.highlight_review_app import launch_highlight_review_app, load_highlight_review_records


def _proxy_sidecar(source: Path) -> dict[str, object]:
    return {
        "schema_version": "proxy_scan_v1",
        "scan_id": "proxy-123abc",
        "ok": True,
        "game": "marvel_rivals",
        "source": str(source.resolve()),
        "proxy_review": {"review_status": "approved"},
        "windows": [],
    }


def _fixture_comparison_report(sidecar_path: Path) -> dict[str, object]:
    return {
        "ok": True,
        "comparison": {
            "fixture_rows": [
                {
                    "fixture_id": "commentary-heavy-001",
                    "artifact_layer": "proxy",
                    "coverage_status": "both",
                    "review_status": "approved",
                    "baseline_sidecar_path": str(sidecar_path.resolve()),
                    "trial_sidecar_path": str(sidecar_path.resolve()),
                    "baseline_action": "inspect",
                    "trial_action": "download_candidate",
                    "score_delta": 0.1,
                    "shortlist_changed": False,
                    "rerank_changed": False,
                    "stage_latency_deltas": {},
                    "recommendation_signal": "trial_better",
                }
            ]
        },
        "recommendation": {"decision": "prefer_trial"},
    }


def _fixture_trial_batch_manifest(report_path: Path) -> dict[str, object]:
    return {
        "ok": True,
        "schema_version": "fixture_trial_batch_v1",
        "batch_name": "nightly",
        "baseline_trial_name": "baseline",
        "overall_recommendation": {"decision": "adopt_trial", "trial_name": "distil-whisper"},
        "trial_comparisons": [
            {
                "trial_name": "distil-whisper",
                "comparison_status": "ok",
                "comparison_report_path": str(report_path.resolve()),
                "artifact_layer": "proxy",
                "recommendation": {"decision": "prefer_trial"},
            }
        ],
    }


class _FakeBlocks:
    def __init__(self, *args, **kwargs) -> None:
        self.loaded = False

    def __enter__(self) -> "_FakeBlocks":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def load(self, *args, **kwargs) -> None:
        self.loaded = True

    def launch(self) -> None:
        return None


class _FakeComponent:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs

    def change(self, *args, **kwargs) -> None:
        return None


class HighlightReviewAppTests(unittest.TestCase):
    def test_load_highlight_review_records_reads_fixture_and_sidecar_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.proxy_scan.json"
            sidecar.write_text(json.dumps(_proxy_sidecar(media), indent=2), encoding="utf-8")
            report = root / "fixture_comparison.json"
            batch_manifest = root / "fixture_trial_batch_manifest.json"
            report.write_text(json.dumps(_fixture_comparison_report(sidecar), indent=2), encoding="utf-8")
            batch_manifest.write_text(json.dumps(_fixture_trial_batch_manifest(report), indent=2), encoding="utf-8")

            records = load_highlight_review_records(
                sidecar_root=root,
                fixture_manifest_path="assets/evaluation/fixture_manifest.json",
                fixture_comparison_report=report,
                fixture_trial_batch_manifest=batch_manifest,
            )

            self.assertGreaterEqual(len(records), 6)
            self.assertTrue(any(row["kind"] == "fixture" for row in records))
            self.assertTrue(any(row["kind"] == "sidecar" and row.get("proxy_review_status") == "approved" for row in records))
            fixture_row = next(row for row in records if row["kind"] == "fixture" and row["record_id"] == "commentary-heavy-001")
            self.assertEqual(len(fixture_row["fixture_comparison_rows"]), 1)
            self.assertEqual(len(fixture_row["fixture_trial_batch_rows"]), 1)

    def test_launch_highlight_review_app_builds_without_mutating_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media = root / "alpha.mp4"
            media.write_bytes(b"video")
            sidecar = root / "alpha.proxy_scan.json"
            original_text = json.dumps(_proxy_sidecar(media), indent=2)
            sidecar.write_text(original_text, encoding="utf-8")
            report = root / "fixture_comparison.json"
            batch_manifest = root / "fixture_trial_batch_manifest.json"
            report.write_text(json.dumps(_fixture_comparison_report(sidecar), indent=2), encoding="utf-8")
            batch_manifest.write_text(json.dumps(_fixture_trial_batch_manifest(report), indent=2), encoding="utf-8")

            fake_gradio = type(
                "FakeGradio",
                (),
                {
                    "Blocks": _FakeBlocks,
                    "Markdown": _FakeComponent,
                    "Dropdown": _FakeComponent,
                    "Textbox": _FakeComponent,
                    "Code": _FakeComponent,
                },
            )()
            with patch("pipeline.highlight_review_app.importlib.import_module", return_value=fake_gradio):
                result = launch_highlight_review_app(
                    sidecar_root=root,
                    fixture_manifest_path="assets/evaluation/fixture_manifest.json",
                    fixture_comparison_report=report,
                    fixture_trial_batch_manifest=batch_manifest,
                    launch=False,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(sidecar.read_text(encoding="utf-8"), original_text)


if __name__ == "__main__":
    unittest.main()

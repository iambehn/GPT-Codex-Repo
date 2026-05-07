from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from pipeline.accepted_proxy_review_prep import prepare_accepted_proxy_review


def _batch_payload(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "ok": True,
        "status": "ok",
        "schema_version": "accepted_fixture_trial_batch_v1",
        "batch_id": "batch-123",
        "created_at": "2026-05-07T00:00:00+00:00",
        "source_manifest_path": "/tmp/source.json",
        "source_manifest_id": "source-123",
        "game": "marvel_rivals",
        "fixture_count": len(rows),
        "success_count": sum(1 for row in rows if row.get("status") == "ok"),
        "failed_count": sum(1 for row in rows if row.get("status") != "ok"),
        "results": rows,
        "manifest_path": "/tmp/batch.json",
    }


class AcceptedProxyReviewPrepTests(unittest.TestCase):
    def test_prepare_accepted_proxy_review_succeeds_for_all_eligible_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_a = root / "a.proxy_scan.json"
            sidecar_b = root / "b.proxy_scan.json"
            sidecar_a.write_text("{}", encoding="utf-8")
            sidecar_b.write_text("{}", encoding="utf-8")
            batch_path = root / "accepted_fixture_trial_batch.json"
            batch_path.write_text(
                json.dumps(
                    _batch_payload(
                        [
                            {"fixture_id": "fixture-a", "status": "ok", "proxy_sidecar_path": str(sidecar_a)},
                            {"fixture_id": "fixture-b", "status": "ok", "proxy_sidecar_path": str(sidecar_b)},
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            def _prepare_proxy_review(*args, **kwargs):
                report = json.loads(Path(kwargs["batch_report"]).read_text(encoding="utf-8"))
                self.assertEqual(len(report["results"]), 2)
                return {
                    "manifest_path": str(root / "proxy_review_session.json"),
                    "session_id": "proxy-session-123",
                    "items": [
                        {"sidecar_path": str(sidecar_a), "gpt_meta_path": str(root / "a.meta.json")},
                        {"sidecar_path": str(sidecar_b), "gpt_meta_path": str(root / "b.meta.json")},
                    ],
                }

            result = prepare_accepted_proxy_review(
                batch_path,
                output_root=root / "outputs",
                review_preparer=_prepare_proxy_review,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["fixture_count"], 2)
            self.assertEqual(result["prepared_count"], 2)
            self.assertEqual(result["skipped_count"], 0)
            self.assertEqual(result["proxy_review_session_id"], "proxy-session-123")
            self.assertTrue(Path(result["manifest_path"]).is_file())
            self.assertEqual([row["status"] for row in result["results"]], ["ok", "ok"])

    def test_prepare_accepted_proxy_review_marks_ineligible_rows_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_a = root / "a.proxy_scan.json"
            sidecar_a.write_text("{}", encoding="utf-8")
            batch_path = root / "accepted_fixture_trial_batch.json"
            batch_path.write_text(
                json.dumps(
                    _batch_payload(
                        [
                            {"fixture_id": "fixture-a", "status": "ok", "proxy_sidecar_path": str(sidecar_a)},
                            {"fixture_id": "fixture-b", "status": "failed", "proxy_sidecar_path": None},
                            {"fixture_id": "fixture-c", "status": "ok", "proxy_sidecar_path": None},
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            def _prepare_proxy_review(*args, **kwargs):
                return {
                    "manifest_path": str(root / "proxy_review_session.json"),
                    "session_id": "proxy-session-123",
                    "items": [{"sidecar_path": str(sidecar_a), "gpt_meta_path": str(root / "a.meta.json")}],
                }

            result = prepare_accepted_proxy_review(batch_path, review_preparer=_prepare_proxy_review)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["prepared_count"], 1)
            self.assertEqual(result["skipped_count"], 2)
            self.assertEqual([row["status"] for row in result["results"]], ["ok", "skipped", "skipped"])

    def test_prepare_accepted_proxy_review_reports_partial_when_bridge_omits_item(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_a = root / "a.proxy_scan.json"
            sidecar_b = root / "b.proxy_scan.json"
            sidecar_a.write_text("{}", encoding="utf-8")
            sidecar_b.write_text("{}", encoding="utf-8")
            batch_path = root / "accepted_fixture_trial_batch.json"
            batch_path.write_text(
                json.dumps(
                    _batch_payload(
                        [
                            {"fixture_id": "fixture-a", "status": "ok", "proxy_sidecar_path": str(sidecar_a)},
                            {"fixture_id": "fixture-b", "status": "ok", "proxy_sidecar_path": str(sidecar_b)},
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            def _prepare_proxy_review(*args, **kwargs):
                return {
                    "manifest_path": str(root / "proxy_review_session.json"),
                    "session_id": "proxy-session-123",
                    "items": [{"sidecar_path": str(sidecar_a), "gpt_meta_path": str(root / "a.meta.json")}],
                }

            result = prepare_accepted_proxy_review(batch_path, review_preparer=_prepare_proxy_review)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["prepared_count"], 1)
            self.assertEqual(result["skipped_count"], 0)
            failed = next(row for row in result["results"] if row["fixture_id"] == "fixture-b")
            self.assertEqual(failed["status"], "failed")

    def test_prepare_accepted_proxy_review_rejects_invalid_batch_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            batch_path = Path(tempdir) / "accepted_fixture_trial_batch.json"
            batch_path.write_text(json.dumps({"schema_version": "wrong"}), encoding="utf-8")
            result = prepare_accepted_proxy_review(batch_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_accepted_fixture_trial_batch")

    def test_prepare_accepted_proxy_review_handles_no_proxy_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            batch_path = Path(tempdir) / "accepted_fixture_trial_batch.json"
            batch_path.write_text(
                json.dumps(
                    _batch_payload(
                        [
                            {"fixture_id": "fixture-a", "status": "failed", "proxy_sidecar_path": None},
                            {"fixture_id": "fixture-b", "status": "ok", "proxy_sidecar_path": None},
                        ]
                    ),
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = prepare_accepted_proxy_review(batch_path)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "no_proxy_sidecars")
            self.assertEqual(result["prepared_count"], 0)
            self.assertEqual(result["skipped_count"], 2)
            self.assertEqual([row["status"] for row in result["results"]], ["skipped", "skipped"])

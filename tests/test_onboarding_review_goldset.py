from __future__ import annotations

import json
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = REPO_ROOT / "tests" / "fixtures" / "onboarding_review_goldsets" / "derived_row_review_goldset.json"


class DerivedRowReviewGoldsetTests(unittest.TestCase):
    def test_goldset_fixture_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "derived_row_review_goldset_v1")
        self.assertIsInstance(payload.get("cases"), list)
        self.assertGreaterEqual(len(payload["cases"]), 6)

    def test_reviewed_decisions_match_goldset_expectations(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                review_path = REPO_ROOT / case["review_file_path"]
                self.assertTrue(review_path.exists(), f"missing review file: {review_path}")
                review_payload = json.loads(review_path.read_text(encoding="utf-8"))

                self.assertEqual(review_payload["schema_version"], "derived_row_review_v1")
                self.assertEqual(review_payload["review_status"], "approved")
                self.assertEqual(review_payload["apply_status"], "applied")
                self.assertEqual(review_payload["review_decision"], case["expected_review_decision"])
                self.assertEqual(review_payload["candidate_option_count"], case["expected_candidate_option_count"])
                self.assertEqual(review_payload["row_snapshot"]["asset_family"], case["expected_asset_family"])
                self.assertEqual(
                    review_payload["row_snapshot"]["target_display_name"],
                    case["expected_target_display_name"],
                )

                selected_candidate_id = str(review_payload.get("selected_candidate_id") or "").strip()
                if case["selected_candidate_required"]:
                    self.assertTrue(selected_candidate_id)
                else:
                    self.assertEqual(selected_candidate_id, "")

                if case["expected_review_decision"] == "accept_candidate":
                    self.assertEqual(review_payload["recommended_decision"], "accept_candidate")
                    candidate_ids = {
                        str(row.get("candidate_id") or "").strip()
                        for row in review_payload.get("candidate_options", [])
                        if isinstance(row, dict)
                    }
                    self.assertIn(selected_candidate_id, candidate_ids)
                elif case["expected_review_decision"] == "reject_all_candidates":
                    self.assertEqual(review_payload["candidate_option_count"], 0)
                    self.assertEqual(review_payload["recommended_decision"], "defer_row")
                elif case["expected_review_decision"] == "defer_row":
                    self.assertEqual(selected_candidate_id, "")
                    self.assertEqual(review_payload["recommended_decision"], "defer_row")
                    self.assertTrue(str(review_payload.get("review_notes") or "").strip())


if __name__ == "__main__":
    unittest.main()

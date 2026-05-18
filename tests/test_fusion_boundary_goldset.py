from __future__ import annotations

import json
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDSET_PATH = REPO_ROOT / "tests" / "fixtures" / "fusion_boundary_goldsets" / "marvel_rivals_fusion_boundary_goldset.json"


class FusionBoundaryGoldsetTests(unittest.TestCase):
    def test_goldset_manifest_is_valid(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "fusion_boundary_goldset_v1")
        self.assertIsInstance(payload.get("cases"), list)
        self.assertEqual(len(payload["cases"]), 6)

    def test_fixture_expectations_match_boundary_goldset(self) -> None:
        payload = json.loads(GOLDSET_PATH.read_text(encoding="utf-8"))
        for case in payload["cases"]:
            with self.subTest(case_id=case["case_id"]):
                fixture_path = REPO_ROOT / case["fixture_path"]
                self.assertTrue(fixture_path.exists(), f"missing fixture: {fixture_path}")
                fixture_payload = json.loads(fixture_path.read_text(encoding="utf-8"))

                self.assertEqual(fixture_payload["schema_version"], "fusion_goldset_clip_v1")
                self.assertIn(case["expected_coverage_tag"], fixture_payload.get("coverage_tags", []))
                self.assertEqual(len(fixture_payload.get("expected_detections", [])), case["expected_detection_count"])
                self.assertEqual(len(fixture_payload.get("expected_runtime_events", [])), case["expected_runtime_event_count"])
                self.assertEqual(len(fixture_payload.get("expected_fused_events", [])), case["expected_fused_event_count"])
                self.assertEqual(len(fixture_payload.get("expected_boundaries", [])), case["expected_boundary_count"])
                self.assertIn("fixture_review", fixture_payload)

                expected_type = case.get("expected_first_fused_event_type")
                fused_events = fixture_payload.get("expected_fused_events", [])
                if expected_type is None:
                    self.assertEqual(fused_events, [])
                else:
                    self.assertTrue(fused_events)
                    first_event = fused_events[0]
                    self.assertEqual(first_event.get("event_type"), expected_type)
                    if "expected_gate_status" in case:
                        self.assertEqual(first_event.get("gate_status"), case["expected_gate_status"])
                    if "expected_synergy_expected" in case:
                        self.assertEqual(first_event.get("synergy_expected"), case["expected_synergy_expected"])
                    if "expected_minimum_required_signals_met" in case:
                        self.assertEqual(
                            first_event.get("minimum_required_signals_met"),
                            case["expected_minimum_required_signals_met"],
                        )
                    if "expected_required_signal_types" in case:
                        self.assertEqual(
                            sorted(first_event.get("required_signal_types", [])),
                            sorted(case["expected_required_signal_types"]),
                        )

                if case["expected_boundary_count"] == 0:
                    self.assertEqual(fixture_payload.get("expected_boundaries", []), [])
                else:
                    first_boundary = fixture_payload["expected_boundaries"][0]
                    self.assertEqual(first_boundary.get("gate_status"), case["expected_gate_status"])


if __name__ == "__main__":
    unittest.main()

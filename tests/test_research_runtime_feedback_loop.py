from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pipeline.research_runtime.adapter import StubResearchModelAdapter
from pipeline.research_runtime.feedback_loop import run_research_runtime_feedback_loop


class ResearchRuntimeFeedbackLoopTests(unittest.TestCase):
    def test_feedback_loop_stops_after_initial_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_research_runtime_feedback_loop(
                topic="Feedback pass",
                output_dir=tempdir,
                adapter=StubResearchModelAdapter(),
                max_feedback_loops=1,
            )
            self.assertEqual(result["iteration_count"], 1)
            self.assertEqual(result["final_overall_status"], "pass")
            self.assertEqual(result["stopped_reason"], "pass")

    def test_feedback_loop_reruns_once_after_warn_and_then_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            adapter = StubResearchModelAdapter(
                scripted_responses={
                    "domain_framing": [
                        {
                            "target_section": "domain_framing",
                            "artifact_payload": {
                                "problem_statement": "Maybe this runtime should broaden scope.",
                                "research_goal": "Shape the runtime",
                                "assumptions": ["A"],
                                "uncertainties": ["U"],
                                "failure_modes": ["F"],
                            },
                            "source_refs": [
                                {
                                    "source_type": "stub",
                                    "locator": "stub://warn",
                                    "citation": "Stub warning citation",
                                    "excerpt": None,
                                }
                            ],
                            "runtime_meta": {
                                "adapter_name": "stub_research_model_adapter",
                                "attempt_index": 1,
                                "corrective_retry": False,
                                "notes": ["warn-first"],
                            },
                        },
                        {
                            "target_section": "domain_framing",
                            "artifact_payload": {
                                "problem_statement": "Need a bounded research runtime.",
                                "research_goal": "Shape the runtime",
                                "assumptions": ["A"],
                                "uncertainties": ["U"],
                                "failure_modes": ["F"],
                            },
                            "source_refs": [
                                {
                                    "source_type": "stub",
                                    "locator": "stub://pass",
                                    "citation": "Stub pass citation",
                                    "excerpt": None,
                                }
                            ],
                            "runtime_meta": {
                                "adapter_name": "stub_research_model_adapter",
                                "attempt_index": 1,
                                "corrective_retry": False,
                                "notes": ["pass-second"],
                            },
                        },
                    ]
                }
            )
            result = run_research_runtime_feedback_loop(
                topic="Feedback rerun",
                output_dir=tempdir,
                adapter=adapter,
                turn_plan=["domain_framing"],
                max_feedback_loops=1,
            )
            self.assertEqual(result["iteration_count"], 2)
            self.assertEqual(result["final_overall_status"], "pass")
            self.assertEqual(result["stopped_reason"], "pass")
            report = json.loads(Path(result["report_path"]).read_text(encoding="utf-8"))
            self.assertEqual(report["iterations"][0]["overall_status"], "warn")
            self.assertEqual(report["iterations"][1]["overall_status"], "pass")
            second_trace = json.loads(Path(report["iterations"][1]["trace_path"]).read_text(encoding="utf-8"))
            second_turn_input = second_trace["rows"][0]["request_payload"]["turn_input"]
            evaluator_feedback = second_turn_input["evidence_bundle"]["evaluator_feedback"]
            self.assertTrue(evaluator_feedback["escalation_instructions"])
            self.assertIn("Remove speculative language", evaluator_feedback["prompt_appendix"])
            self.assertIn("evaluator_escalation_prompt", second_turn_input["prompt_bundle"])

    def test_feedback_loop_respects_max_feedback_loops(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            always_warn = {
                "target_section": "domain_framing",
                "artifact_payload": {
                    "problem_statement": "Maybe this runtime should broaden scope.",
                    "research_goal": "Shape the runtime",
                    "assumptions": ["A"],
                    "uncertainties": ["U"],
                    "failure_modes": ["F"],
                },
                "source_refs": [
                    {
                        "source_type": "stub",
                        "locator": "stub://warn",
                        "citation": "Stub warning citation",
                        "excerpt": None,
                    }
                ],
                "runtime_meta": {
                    "adapter_name": "stub_research_model_adapter",
                    "attempt_index": 1,
                    "corrective_retry": False,
                    "notes": ["warn"],
                },
            }
            adapter = StubResearchModelAdapter(scripted_responses={"domain_framing": [always_warn, always_warn]})
            result = run_research_runtime_feedback_loop(
                topic="Feedback limit",
                output_dir=tempdir,
                adapter=adapter,
                turn_plan=["domain_framing"],
                max_feedback_loops=1,
            )
            self.assertEqual(result["iteration_count"], 2)
            self.assertEqual(result["final_overall_status"], "warn")
            self.assertEqual(result["stopped_reason"], "max_feedback_loops_reached")

    def test_cli_feedback_loop_reports_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            completed = subprocess.run(
                [
                    sys.executable,
                    "tools/research_runtime_feedback_loop.py",
                    "--topic",
                    "CLI feedback loop",
                    "--output-dir",
                    tempdir,
                    "--use-stub-adapter",
                    "--max-feedback-loops",
                    "1",
                ],
                cwd=Path(__file__).resolve().parent.parent,
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["final_overall_status"], "pass")
            self.assertTrue(Path(payload["report_path"]).is_file())


if __name__ == "__main__":
    unittest.main()

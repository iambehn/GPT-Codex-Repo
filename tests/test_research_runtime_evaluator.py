from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pipeline.research_runtime.adapter import StubResearchModelAdapter
from pipeline.research_runtime.prompt_assets import build_prompt_bundle
from pipeline.research_runtime.evaluator import evaluate_research_runtime_artifacts, evaluate_research_runtime_payloads
from pipeline.research_runtime.runner import run_research_runtime


class ResearchRuntimeEvaluatorTests(unittest.TestCase):
    def test_evaluator_passes_on_stub_happy_path(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            artifact_path = root / "artifact.json"
            result = run_research_runtime(
                topic="Evaluator happy path",
                output_path=artifact_path,
                adapter=StubResearchModelAdapter(),
            )
            evaluation = evaluate_research_runtime_artifacts(
                artifact_path=artifact_path,
                trace_path=result["trace_output_path"],
            )
            self.assertEqual(evaluation["overall_status"], "pass")
            self.assertEqual(evaluation["summary"]["fail_count"], 0)
            self.assertEqual(evaluation["summary"]["warn_count"], 0)

    def test_evaluator_warns_on_speculative_language(self) -> None:
        artifact = {
            "dossier_template": "pipeline_design_research_v1",
            "turn_plan": ["domain_framing"],
            "sections": {
                "domain_framing": {
                    "payload": {
                        "problem_statement": "Maybe the runtime should broaden its scope.",
                        "research_goal": "Bound the runtime",
                        "assumptions": ["The current pipeline remains in scope."],
                        "uncertainties": ["Provider behavior may drift."],
                        "failure_modes": ["Scope may spread."],
                    },
                    "source_refs": [{"source_type": "stub", "locator": "x", "citation": "y", "excerpt": None}],
                }
            },
        }
        trace = {
            "rows": [
                {
                    "turn_index": 1,
                    "target_section": "domain_framing",
                    "request_payload": {
                        "turn_input": {
                            "target_section": "domain_framing",
                            "prompt_bundle": build_prompt_bundle(
                                section_name="domain_framing",
                                dossier_template="pipeline_design_research_v1",
                            ),
                        }
                    },
                    "committed": True,
                    "quarantined": False,
                    "validation_error": None,
                }
            ]
        }
        evaluation = evaluate_research_runtime_payloads(artifact=artifact, trace=trace)
        self.assertEqual(evaluation["overall_status"], "warn")
        self.assertTrue(any(check["name"] == "speculation_markers" and check["status"] == "warn" for check in evaluation["checks"]))

    def test_evaluator_fails_when_required_section_lists_are_missing(self) -> None:
        artifact = {
            "dossier_template": "pipeline_design_research_v1",
            "turn_plan": ["domain_framing"],
            "sections": {
                "domain_framing": {
                    "payload": {
                        "problem_statement": "Need bounded runtime.",
                        "research_goal": "Shape the agent.",
                        "assumptions": [],
                        "uncertainties": [],
                        "failure_modes": [],
                    },
                    "source_refs": [{"source_type": "stub", "locator": "x", "citation": "y", "excerpt": None}],
                }
            },
        }
        trace = {
            "rows": [
                {
                    "turn_index": 1,
                    "target_section": "domain_framing",
                    "request_payload": {
                        "turn_input": {
                            "target_section": "domain_framing",
                            "prompt_bundle": build_prompt_bundle(
                                section_name="domain_framing",
                                dossier_template="pipeline_design_research_v1",
                            ),
                        }
                    },
                    "committed": True,
                    "quarantined": False,
                    "validation_error": None,
                }
            ]
        }
        evaluation = evaluate_research_runtime_payloads(artifact=artifact, trace=trace)
        self.assertEqual(evaluation["overall_status"], "fail")
        self.assertGreaterEqual(evaluation["summary"]["fail_count"], 1)

    def test_evaluator_fails_when_trace_rows_have_no_terminal_outcome(self) -> None:
        artifact = {
            "dossier_template": "pipeline_design_research_v1",
            "turn_plan": ["domain_framing"],
            "sections": {
                "domain_framing": {
                    "payload": {
                        "problem_statement": "Need bounded runtime.",
                        "research_goal": "Shape the agent.",
                        "assumptions": ["A"],
                        "uncertainties": ["U"],
                        "failure_modes": ["F"],
                    },
                    "source_refs": [{"source_type": "stub", "locator": "x", "citation": "y", "excerpt": None}],
                }
            },
        }
        trace = {
            "rows": [
                {
                    "turn_index": 1,
                    "target_section": "domain_framing",
                    "request_payload": {
                        "turn_input": {
                            "target_section": "domain_framing",
                            "prompt_bundle": build_prompt_bundle(
                                section_name="domain_framing",
                                dossier_template="pipeline_design_research_v1",
                            ),
                        }
                    },
                    "committed": False,
                    "quarantined": False,
                    "validation_error": None,
                }
            ]
        }
        evaluation = evaluate_research_runtime_payloads(artifact=artifact, trace=trace)
        self.assertEqual(evaluation["overall_status"], "fail")
        self.assertTrue(any(check["name"] == "trace_rows_have_outcomes" and check["status"] == "fail" for check in evaluation["checks"]))

    def test_evaluator_warns_on_vague_citation_quality(self) -> None:
        artifact = {
            "dossier_template": "pipeline_design_research_v1",
            "turn_plan": ["domain_framing"],
            "sections": {
                "domain_framing": {
                    "payload": {
                        "problem_statement": "Need bounded runtime.",
                        "research_goal": "Shape the agent.",
                        "assumptions": ["A"],
                        "uncertainties": ["U"],
                        "failure_modes": ["F"],
                    },
                    "source_refs": [{"source_type": "stub", "locator": "n/a", "citation": "source", "excerpt": None}],
                }
            },
        }
        trace = {
            "rows": [
                {
                    "turn_index": 1,
                    "target_section": "domain_framing",
                    "request_payload": {
                        "turn_input": {
                            "target_section": "domain_framing",
                            "prompt_bundle": build_prompt_bundle(
                                section_name="domain_framing",
                                dossier_template="pipeline_design_research_v1",
                            ),
                        }
                    },
                    "committed": True,
                    "quarantined": False,
                    "validation_error": None,
                }
            ]
        }
        evaluation = evaluate_research_runtime_payloads(artifact=artifact, trace=trace)
        self.assertEqual(evaluation["overall_status"], "warn")
        self.assertTrue(any(check["name"] == "source_ref_quality" and check["status"] == "warn" for check in evaluation["checks"]))

    def test_evaluator_fails_on_acceptance_criteria_coverage_gap(self) -> None:
        artifact = {
            "dossier_template": "pipeline_design_research_v1",
            "turn_plan": ["question_decomposition"],
            "sections": {
                "question_decomposition": {
                    "payload": {
                        "primary_question": "What should we research?",
                        "subquestions": ["Only one subquestion"],
                        "assumptions": ["A"],
                        "uncertainties": ["U"],
                        "failure_modes": ["F"],
                    },
                    "source_refs": [{"source_type": "stub", "locator": "stub://x", "citation": "Adequate citation", "excerpt": None}],
                }
            },
        }
        trace = {
            "rows": [
                {
                    "turn_index": 1,
                    "target_section": "question_decomposition",
                    "request_payload": {
                        "turn_input": {
                            "target_section": "question_decomposition",
                            "prompt_bundle": build_prompt_bundle(
                                section_name="question_decomposition",
                                dossier_template="pipeline_design_research_v1",
                            ),
                        }
                    },
                    "committed": True,
                    "quarantined": False,
                    "validation_error": None,
                }
            ]
        }
        evaluation = evaluate_research_runtime_payloads(artifact=artifact, trace=trace)
        self.assertEqual(evaluation["overall_status"], "fail")
        self.assertTrue(any(check["name"] == "acceptance_criteria_coverage" and check["status"] == "fail" for check in evaluation["checks"]))

    def test_evaluator_fails_on_prompt_bundle_drift(self) -> None:
        artifact = {
            "dossier_template": "pipeline_design_research_v1",
            "turn_plan": ["domain_framing"],
            "sections": {
                "domain_framing": {
                    "payload": {
                        "problem_statement": "Need bounded runtime.",
                        "research_goal": "Shape the agent.",
                        "assumptions": ["A"],
                        "uncertainties": ["U"],
                        "failure_modes": ["F"],
                    },
                    "source_refs": [{"source_type": "stub", "locator": "stub://x", "citation": "Adequate citation", "excerpt": None}],
                }
            },
        }
        prompt_bundle = build_prompt_bundle(
            section_name="domain_framing",
            dossier_template="pipeline_design_research_v1",
        )
        prompt_bundle["acceptance_criteria"] = ["Drifted criterion"]
        trace = {
            "rows": [
                {
                    "turn_index": 1,
                    "target_section": "domain_framing",
                    "request_payload": {
                        "turn_input": {
                            "target_section": "domain_framing",
                            "prompt_bundle": prompt_bundle,
                        }
                    },
                    "committed": True,
                    "quarantined": False,
                    "validation_error": None,
                }
            ]
        }
        evaluation = evaluate_research_runtime_payloads(artifact=artifact, trace=trace)
        self.assertEqual(evaluation["overall_status"], "fail")
        self.assertTrue(any(check["name"] == "prompt_bundle_alignment" and check["status"] == "fail" for check in evaluation["checks"]))

    def test_cli_evaluator_reports_compact_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            artifact_path = root / "artifact.json"
            result = run_research_runtime(
                topic="CLI evaluator",
                output_path=artifact_path,
                adapter=StubResearchModelAdapter(),
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    "tools/evaluate_research_runtime_artifacts.py",
                    "--artifact",
                    str(artifact_path),
                    "--trace",
                    str(result["trace_output_path"]),
                ],
                cwd=Path(__file__).resolve().parent.parent,
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["overall_status"], "pass")
            self.assertEqual(payload["fail_count"], 0)


if __name__ == "__main__":
    unittest.main()

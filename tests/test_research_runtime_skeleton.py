from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipeline.research_runtime.adapter import OpenAIResponsesResearchModelAdapter, StubResearchModelAdapter
from pipeline.research_runtime.dossier import DEFAULT_DOSSIER_TEMPLATE, default_turn_plan, dossier_metadata
from pipeline.research_runtime.prompt_assets import build_prompt_bundle
from pipeline.research_runtime.runner import build_research_adapter, run_research_runtime


class ResearchRuntimeSkeletonTests(unittest.TestCase):
    def test_valid_turn_writes_exactly_one_section(self) -> None:
        result = run_research_runtime(
            topic="Research runtime skeleton",
            turn_plan=["domain_framing"],
            adapter=StubResearchModelAdapter(),
        )
        sections = result["artifact"]["sections"]
        self.assertEqual(sorted(sections.keys()), ["domain_framing"])
        self.assertEqual(result["quarantine_count"], 0)

    def test_wrong_target_section_is_rejected_and_quarantined_after_retry(self) -> None:
        adapter = StubResearchModelAdapter(
            scripted_responses={
                "domain_framing": [
                    _valid_question_decomposition_envelope(),
                    _valid_question_decomposition_envelope(),
                ]
            }
        )
        result = run_research_runtime(
            topic="Mismatch case",
            turn_plan=["domain_framing"],
            adapter=adapter,
        )
        self.assertEqual(result["quarantine_count"], 1)
        self.assertEqual(result["artifact"]["sections"], {})
        self.assertIn("assigned section", result["artifact"]["quarantine_records"][0]["validation_error"])

    def test_payload_with_extra_keys_fails_validation(self) -> None:
        adapter = StubResearchModelAdapter(
            scripted_responses={
                "domain_framing": [
                    {
                        "target_section": "domain_framing",
                        "artifact_payload": {
                            "problem_statement": "x",
                            "research_goal": "y",
                            "assumptions": ["a"],
                            "uncertainties": ["u"],
                            "failure_modes": ["f"],
                            "junk": "not allowed",
                        },
                        "source_refs": [_source_ref()],
                        "runtime_meta": _runtime_meta(attempt_index=1),
                    },
                    {
                        "target_section": "domain_framing",
                        "artifact_payload": {
                            "problem_statement": "x",
                            "research_goal": "y",
                            "assumptions": ["a"],
                            "uncertainties": ["u"],
                            "failure_modes": ["f"],
                            "junk": "not allowed",
                        },
                        "source_refs": [_source_ref()],
                        "runtime_meta": _runtime_meta(attempt_index=2, corrective_retry=True),
                    },
                ]
            }
        )
        result = run_research_runtime(
            topic="Extra key case",
            turn_plan=["domain_framing"],
            adapter=adapter,
        )
        self.assertEqual(result["quarantine_count"], 1)
        self.assertIn("unsupported keys", result["artifact"]["quarantine_records"][0]["validation_error"])

    def test_first_invalid_output_retries_once_then_succeeds(self) -> None:
        adapter = StubResearchModelAdapter(
            scripted_responses={
                "domain_framing": [
                    {
                        "target_section": "domain_framing",
                        "artifact_payload": {"problem_statement": "missing other required keys"},
                        "source_refs": [_source_ref()],
                        "runtime_meta": _runtime_meta(attempt_index=1),
                    },
                    _valid_domain_framing_envelope(attempt_index=2, corrective_retry=True),
                ]
            }
        )
        result = run_research_runtime(
            topic="Retry success",
            turn_plan=["domain_framing"],
            adapter=adapter,
        )
        self.assertEqual(result["quarantine_count"], 0)
        record = result["artifact"]["sections"]["domain_framing"]
        self.assertEqual(record["runtime_meta"]["attempt_index"], 2)
        self.assertTrue(record["runtime_meta"]["corrective_retry"])
        self.assertEqual(result["turn_trace_count"], 2)
        self.assertIsNotNone(result["turn_traces"][0]["validation_error"])
        self.assertTrue(result["turn_traces"][1]["committed"])

    def test_second_invalid_output_is_quarantined(self) -> None:
        invalid = {
            "target_section": "domain_framing",
            "artifact_payload": {"problem_statement": "missing everything else"},
            "source_refs": [_source_ref()],
            "runtime_meta": _runtime_meta(attempt_index=1),
        }
        adapter = StubResearchModelAdapter(scripted_responses={"domain_framing": [invalid, invalid]})
        result = run_research_runtime(
            topic="Retry fail",
            turn_plan=["domain_framing"],
            adapter=adapter,
        )
        self.assertEqual(result["quarantine_count"], 1)
        self.assertEqual(result["artifact"]["sections"], {})

    def test_checkpoint_emitted_on_configured_boundary(self) -> None:
        result = run_research_runtime(
            topic="Checkpoint case",
            turn_plan=["domain_framing", "question_decomposition"],
            checkpoint_every=1,
            adapter=StubResearchModelAdapter(),
        )
        checkpoints = result["artifact"]["checkpoints"]
        self.assertEqual(len(checkpoints), 2)
        self.assertEqual(checkpoints[0]["turn_index"], 1)
        self.assertEqual(checkpoints[1]["turn_index"], 2)

    def test_finalized_artifact_contains_only_committed_sections(self) -> None:
        adapter = StubResearchModelAdapter(
            scripted_responses={
                "question_decomposition": [
                    _valid_question_decomposition_envelope(),
                ],
                "architecture_dataflow": [
                    _invalid_architecture_envelope(),
                    _invalid_architecture_envelope(attempt_index=2, corrective_retry=True),
                ],
            }
        )
        result = run_research_runtime(
            topic="Committed sections only",
            turn_plan=["question_decomposition", "architecture_dataflow"],
            adapter=adapter,
        )
        self.assertEqual(sorted(result["artifact"]["sections"].keys()), ["question_decomposition"])
        self.assertEqual(result["quarantine_count"], 1)

    def test_stub_adapter_drives_happy_path_multi_turn_run(self) -> None:
        result = run_research_runtime(
            topic="Happy path",
            turn_plan=["domain_framing", "question_decomposition", "architecture_dataflow"],
            adapter=StubResearchModelAdapter(),
        )
        self.assertEqual(result["completed_turn_count"], 3)
        self.assertEqual(
            sorted(result["artifact"]["sections"].keys()),
            ["architecture_dataflow", "domain_framing", "question_decomposition"],
        )
        self.assertEqual(result["artifact"]["schema_version"], "research_runtime_pipeline_design_dossier_v1")
        self.assertEqual(result["artifact"]["dossier_template"], DEFAULT_DOSSIER_TEMPLATE)

    def test_default_dossier_turn_plan_is_used_when_turn_plan_is_omitted(self) -> None:
        result = run_research_runtime(
            topic="Default dossier",
            adapter=StubResearchModelAdapter(),
        )
        self.assertEqual(result["artifact"]["turn_plan"], default_turn_plan())
        self.assertEqual(result["dossier_template"], DEFAULT_DOSSIER_TEMPLATE)
        self.assertEqual(result["trace"]["row_count"], 3)

    def test_prompt_bundle_loads_system_section_and_retry_assets(self) -> None:
        bundle = build_prompt_bundle(
            section_name="domain_framing",
            dossier_template=DEFAULT_DOSSIER_TEMPLATE,
            corrective_retry=True,
        )
        self.assertIn("one turn, one lens", bundle["system_prompt"])
        self.assertIn("Write the `domain_framing` section", bundle["section_prompt"])
        self.assertIn("previous output failed validation", bundle["corrective_retry_prompt"])
        self.assertEqual(bundle["dossier_template"], DEFAULT_DOSSIER_TEMPLATE)

    def test_dossier_metadata_is_stable(self) -> None:
        metadata = dossier_metadata()
        self.assertEqual(metadata["schema_version"], "research_runtime_pipeline_design_dossier_v1")
        self.assertEqual(metadata["dossier_template"], DEFAULT_DOSSIER_TEMPLATE)
        self.assertEqual(metadata["turn_plan"], default_turn_plan())

    def test_cli_tool_runs_with_stub_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            turn_plan_path = root / "turn-plan.json"
            output_path = root / "artifact.json"
            turn_plan_path.write_text(json.dumps(["domain_framing", "question_decomposition"]), encoding="utf-8")
            command = [
                sys.executable,
                "tools/research_runtime_skeleton.py",
                "--topic",
                "CLI skeleton",
                "--turn-plan",
                str(turn_plan_path),
                "--output-path",
                str(output_path),
                "--checkpoint-every",
                "1",
                "--use-stub-adapter",
            ]
            completed = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parent.parent,
                capture_output=True,
                text=True,
                check=True,
            )
            summary = json.loads(completed.stdout)
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["completed_sections"], ["domain_framing", "question_decomposition"])
            self.assertTrue(summary["trace_output_path"].endswith(".trace.json"))
            artifact = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(sorted(artifact["sections"].keys()), ["domain_framing", "question_decomposition"])
            trace_path = Path(summary["trace_output_path"])
            trace_payload = json.loads(trace_path.read_text(encoding="utf-8"))
            self.assertEqual(trace_payload["row_count"], 2)

    def test_cli_tool_uses_default_dossier_without_explicit_turn_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            output_path = root / "artifact.json"
            command = [
                sys.executable,
                "tools/research_runtime_skeleton.py",
                "--topic",
                "CLI default dossier",
                "--output-path",
                str(output_path),
                "--use-stub-adapter",
            ]
            completed = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parent.parent,
                capture_output=True,
                text=True,
                check=True,
            )
            summary = json.loads(completed.stdout)
            self.assertEqual(summary["dossier_template"], DEFAULT_DOSSIER_TEMPLATE)
            artifact = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(artifact["turn_plan"], default_turn_plan())

    def test_runner_writes_default_trace_file_next_to_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            output_path = root / "artifact.json"
            result = run_research_runtime(
                topic="Trace persistence",
                output_path=output_path,
                adapter=StubResearchModelAdapter(),
            )
            self.assertEqual(Path(result["trace_output_path"]).name, "artifact.trace.json")
            trace_payload = json.loads(Path(result["trace_output_path"]).read_text(encoding="utf-8"))
            self.assertEqual(trace_payload["schema_version"], "research_runtime_turn_trace_v1")
            self.assertEqual(trace_payload["row_count"], 3)

    def test_build_research_adapter_returns_openai_adapter(self) -> None:
        adapter = build_research_adapter(
            adapter_name="openai",
            model="gpt-5",
            api_key="test-key",
        )
        self.assertIsInstance(adapter, OpenAIResponsesResearchModelAdapter)

    def test_openai_adapter_requires_api_key(self) -> None:
        with self.assertRaises(ValueError):
            OpenAIResponsesResearchModelAdapter(api_key=None)

    def test_openai_adapter_builds_responses_request_and_parses_output_text(self) -> None:
        adapter = OpenAIResponsesResearchModelAdapter(api_key="test-key", model="gpt-5")
        response_payload = {
            "id": "resp_123",
            "output_text": json.dumps(_valid_domain_framing_envelope()),
        }

        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(response_payload).encode("utf-8")

        with mock.patch("pipeline.research_runtime.adapter.request.urlopen", return_value=_FakeResponse()) as mocked_urlopen:
            result = adapter.generate_turn(
                {
                    "target_section": "domain_framing",
                    "canonical_brief": "OpenAI adapter topic",
                    "turn_lens": "domain_framing",
                    "target_schema": {"section_name": "domain_framing"},
                    "checkpoint_summary": None,
                    "evidence_bundle": {},
                    "prompt_bundle": build_prompt_bundle(
                        section_name="domain_framing",
                        dossier_template=DEFAULT_DOSSIER_TEMPLATE,
                        corrective_retry=False,
                    ),
                }
            )
        self.assertEqual(result.output_payload["target_section"], "domain_framing")
        request_obj = mocked_urlopen.call_args[0][0]
        request_body = json.loads(request_obj.data.decode("utf-8"))
        self.assertEqual(request_body["model"], "gpt-5")
        self.assertEqual(request_body["text"]["format"]["type"], "json_schema")
        self.assertEqual(request_body["text"]["format"]["strict"], True)


def _source_ref() -> dict[str, str | None]:
    return {
        "source_type": "stub",
        "locator": "stub://section",
        "citation": "Stub source",
        "excerpt": None,
    }


def _runtime_meta(*, attempt_index: int, corrective_retry: bool = False) -> dict[str, object]:
    return {
        "adapter_name": "stub_research_model_adapter",
        "attempt_index": attempt_index,
        "corrective_retry": corrective_retry,
        "notes": ["test"],
    }


def _valid_domain_framing_envelope(*, attempt_index: int = 1, corrective_retry: bool = False) -> dict[str, object]:
    return {
        "target_section": "domain_framing",
        "artifact_payload": {
            "problem_statement": "Need a bounded research runtime",
            "research_goal": "Prove section ownership and validation flow",
            "assumptions": ["The runtime remains local and deterministic."],
            "uncertainties": ["Provider integration is deferred."],
            "failure_modes": ["The skeleton may overfit the initial sections."],
        },
        "source_refs": [_source_ref()],
        "runtime_meta": _runtime_meta(attempt_index=attempt_index, corrective_retry=corrective_retry),
    }


def _valid_question_decomposition_envelope(*, attempt_index: int = 1, corrective_retry: bool = False) -> dict[str, object]:
    return {
        "target_section": "question_decomposition",
        "artifact_payload": {
            "primary_question": "What must the runtime enforce?",
            "subquestions": ["How is section ownership validated?", "How are retries bounded?"],
            "assumptions": ["Turn plan order is explicit."],
            "uncertainties": ["Additional sections may require new schemas."],
            "failure_modes": ["Question set may omit operational concerns."],
        },
        "source_refs": [_source_ref()],
        "runtime_meta": _runtime_meta(attempt_index=attempt_index, corrective_retry=corrective_retry),
    }


def _invalid_architecture_envelope(*, attempt_index: int = 1, corrective_retry: bool = False) -> dict[str, object]:
    return {
        "target_section": "architecture_dataflow",
        "artifact_payload": {
            "current_pipeline_summary": "x",
            "stages": ["a"],
            "crossmodal_dependencies": ["b"],
            "assumptions": ["c"],
            "uncertainties": ["d"],
            "failure_modes": ["e"],
            "extra": "invalid",
        },
        "source_refs": [_source_ref()],
        "runtime_meta": _runtime_meta(attempt_index=attempt_index, corrective_retry=corrective_retry),
    }


if __name__ == "__main__":
    unittest.main()

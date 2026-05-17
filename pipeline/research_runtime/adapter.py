from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any
from urllib import request

from .schemas import turn_output_envelope_json_schema


@dataclass(slots=True)
class AdapterTurnResult:
    output_payload: Any
    raw_response: Any
    request_payload: dict[str, Any]
    adapter_name: str

from abc import ABC, abstractmethod
from typing import Any


class ResearchModelAdapter(ABC):
    @property
    @abstractmethod
    def adapter_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def generate_turn(self, turn_input: dict[str, Any]) -> AdapterTurnResult:
        raise NotImplementedError


class StubResearchModelAdapter(ResearchModelAdapter):
    def __init__(self, *, scripted_responses: dict[str, list[Any]] | None = None) -> None:
        self._scripted_responses = {key: list(value) for key, value in (scripted_responses or {}).items()}
        self._response_index: dict[str, int] = {}

    @property
    def adapter_name(self) -> str:
        return "stub_research_model_adapter"

    def generate_turn(self, turn_input: dict[str, Any]) -> AdapterTurnResult:
        section = str(turn_input.get("target_section") or "")
        index = self._response_index.get(section, 0)
        scripted = self._scripted_responses.get(section, [])
        if index < len(scripted):
            response = scripted[index]
            self._response_index[section] = index + 1
            return AdapterTurnResult(
                output_payload=response,
                raw_response={"source": "scripted_response", "section": section, "index": index},
                request_payload={"turn_input": turn_input},
                adapter_name=self.adapter_name,
            )
        self._response_index[section] = index + 1
        payload = self._default_response(turn_input)
        return AdapterTurnResult(
            output_payload=payload,
            raw_response={"source": "stub_default_response", "section": section, "index": index},
            request_payload={"turn_input": turn_input},
            adapter_name=self.adapter_name,
        )

    def _default_response(self, turn_input: dict[str, Any]) -> dict[str, Any]:
        section = str(turn_input.get("target_section") or "")
        topic = str(turn_input.get("canonical_brief") or "")
        attempt_index = int(turn_input.get("attempt_index") or 1)
        corrective_retry = bool(turn_input.get("corrective_retry"))
        if section == "domain_framing":
            payload = {
                "problem_statement": f"Research scope for {topic}",
                "research_goal": f"Define the bounded research objective for {topic}",
                "assumptions": ["Current pipeline constraints remain in scope."],
                "uncertainties": ["Full evidence coverage may expand later."],
                "failure_modes": ["Framing may be too broad for implementation."],
            }
        elif section == "question_decomposition":
            payload = {
                "primary_question": f"What is the most defensible next research question for {topic}?",
                "subquestions": [
                    "Which pipeline contract is under-specified?",
                    "What evidence is missing from the current design state?",
                ],
                "assumptions": ["The current framework doc is authoritative for the runtime contract."],
                "uncertainties": ["Section ordering may shift in later runs."],
                "failure_modes": ["Subquestions may duplicate prior work if checkpoints are stale."],
            }
        else:
            payload = {
                "current_pipeline_summary": f"{topic} depends on bounded research artifacts before implementation.",
                "stages": ["frame_problem", "decompose_questions", "commit_section"],
                "crossmodal_dependencies": ["research protocol", "typed artifact validation"],
                "assumptions": ["The runtime remains single-path in v1."],
                "uncertainties": ["Future evidence expansion may require more stages."],
                "failure_modes": ["Dataflow may hide section ownership errors if validation is weak."],
            }
        return {
            "target_section": section,
            "artifact_payload": payload,
            "source_refs": [
                {
                    "source_type": "stub",
                    "locator": f"stub://{section}",
                    "citation": f"Stub adapter generated section for {section}",
                    "excerpt": None,
                }
            ],
            "runtime_meta": {
                "adapter_name": self.adapter_name,
                "attempt_index": attempt_index,
                "corrective_retry": corrective_retry,
                "notes": ["Deterministic stub response"],
            },
        }


class OpenAIResponsesResearchModelAdapter(ResearchModelAdapter):
    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = "gpt-5",
        timeout_seconds: int = 60,
        base_url: str = "https://api.openai.com/v1/responses",
    ) -> None:
        self._api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self._api_key:
            raise ValueError("OPENAI_API_KEY is required for the OpenAI research adapter")
        self._model = model
        self._timeout_seconds = timeout_seconds
        self._base_url = base_url

    @property
    def adapter_name(self) -> str:
        return "openai_responses_research_model_adapter"

    def generate_turn(self, turn_input: dict[str, Any]) -> AdapterTurnResult:
        section = str(turn_input.get("target_section") or "")
        request_payload = self._build_request_payload(turn_input)
        raw_response = self._post_request(request_payload)
        output_payload = self._extract_output_payload(raw_response)
        return AdapterTurnResult(
            output_payload=output_payload,
            raw_response=raw_response,
            request_payload=request_payload,
            adapter_name=self.adapter_name,
        )

    def _build_request_payload(self, turn_input: dict[str, Any]) -> dict[str, Any]:
        section = str(turn_input.get("target_section") or "")
        prompt_bundle = dict(turn_input.get("prompt_bundle") or {})
        evidence_bundle = turn_input.get("evidence_bundle") or {}
        checkpoint_summary = turn_input.get("checkpoint_summary")
        schema = turn_output_envelope_json_schema(section)
        user_payload = {
            "topic": turn_input.get("canonical_brief"),
            "target_section": section,
            "turn_lens": turn_input.get("turn_lens"),
            "acceptance_criteria": prompt_bundle.get("acceptance_criteria", []),
            "target_schema": turn_input.get("target_schema"),
            "checkpoint_summary": checkpoint_summary,
            "evidence_bundle": evidence_bundle,
        }
        system_instructions = prompt_bundle.get("system_prompt", "")
        section_prompt = prompt_bundle.get("section_prompt", "")
        retry_prompt = prompt_bundle.get("corrective_retry_prompt")
        evaluator_escalation_prompt = prompt_bundle.get("evaluator_escalation_prompt")
        if retry_prompt:
            system_instructions = f"{system_instructions}\n\n{retry_prompt}"
        if evaluator_escalation_prompt:
            system_instructions = f"{system_instructions}\n\n{evaluator_escalation_prompt}"
        return {
            "model": self._model,
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": system_instructions,
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"{section_prompt}\n\nReturn JSON only.\n\n{json.dumps(user_payload, indent=2)}",
                        }
                    ],
                },
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": f"{section}_turn_output_envelope",
                    "schema": schema,
                    "strict": True,
                }
            },
        }

    def _post_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        http_request = request.Request(
            self._base_url,
            data=body,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with request.urlopen(http_request, timeout=self._timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _extract_output_payload(self, raw_response: dict[str, Any]) -> Any:
        output_text = raw_response.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return json.loads(output_text)
        output_items = raw_response.get("output")
        if isinstance(output_items, list):
            for item in output_items:
                if not isinstance(item, dict):
                    continue
                content_items = item.get("content")
                if not isinstance(content_items, list):
                    continue
                for content in content_items:
                    if not isinstance(content, dict):
                        continue
                    text_value = content.get("text")
                    if isinstance(text_value, str) and text_value.strip():
                        return json.loads(text_value)
        raise ValueError("OpenAI response did not contain structured output text")

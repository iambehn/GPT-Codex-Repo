from __future__ import annotations

from pathlib import Path

from .schemas import SECTION_MODELS

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROTOCOL_PATH = REPO_ROOT / "RESEARCH_PROTOCOL.md"
DEFAULT_RETRY_LIMIT = 1
DEFAULT_CHECKPOINT_TURNS = (3, 6, 9)
NO_CROSS_SECTION_WRITES = True


def load_protocol_lock() -> str:
    return PROTOCOL_PATH.read_text(encoding="utf-8")


def supported_sections() -> list[str]:
    return list(SECTION_MODELS.keys())


def checkpoint_turns(*, max_turns: int, checkpoint_every: int | None = None) -> tuple[int, ...]:
    if checkpoint_every is not None and checkpoint_every > 0:
        return tuple(index for index in range(checkpoint_every, max_turns + 1, checkpoint_every))
    return tuple(turn for turn in DEFAULT_CHECKPOINT_TURNS if turn <= max_turns)


def describe_section_schema(section_name: str) -> dict[str, object]:
    if section_name == "domain_framing":
        return {
            "section_name": section_name,
            "required_keys": ["problem_statement", "research_goal", "assumptions", "uncertainties", "failure_modes"],
        }
    if section_name == "question_decomposition":
        return {
            "section_name": section_name,
            "required_keys": ["primary_question", "subquestions", "assumptions", "uncertainties", "failure_modes"],
        }
    if section_name == "architecture_dataflow":
        return {
            "section_name": section_name,
            "required_keys": [
                "current_pipeline_summary",
                "stages",
                "crossmodal_dependencies",
                "assumptions",
                "uncertainties",
                "failure_modes",
            ],
        }
    raise ValueError(f"unsupported section: {section_name}")

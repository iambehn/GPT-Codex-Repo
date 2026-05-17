from __future__ import annotations

from pathlib import Path
from typing import Any

from .dossier import DEFAULT_DOSSIER_TEMPLATE, dossier_section_spec

PROMPTS_ROOT = Path(__file__).resolve().parent / "prompts"


def _read_prompt(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def load_system_prompt() -> str:
    return _read_prompt(PROMPTS_ROOT / "system_prompt.txt")


def load_corrective_retry_prompt() -> str:
    return _read_prompt(PROMPTS_ROOT / "corrective_retry_prompt.txt")


def load_section_prompt(section_name: str) -> str:
    return _read_prompt(PROMPTS_ROOT / "sections" / f"{section_name}.txt")


def build_prompt_bundle(
    *,
    section_name: str,
    dossier_template: str = DEFAULT_DOSSIER_TEMPLATE,
    corrective_retry: bool = False,
) -> dict[str, Any]:
    section_spec = dossier_section_spec(section_name, template_name=dossier_template)
    bundle = {
        "system_prompt": load_system_prompt(),
        "section_prompt": load_section_prompt(section_name),
        "turn_lens": section_spec["turn_lens"],
        "acceptance_criteria": list(section_spec["acceptance_criteria"]),
        "dossier_template": dossier_template,
    }
    if corrective_retry:
        bundle["corrective_retry_prompt"] = load_corrective_retry_prompt()
    return bundle

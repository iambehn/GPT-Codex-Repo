"""
Stage 6 — Template Selection

This module now handles template selection only.
The actual accept/reject/quarantine decision happens earlier in pipeline.clip_judge.

To switch templates, change `default_template_id` in config.yaml — no code
change required.

The clip's .meta.json is updated with: selected_template_id, template_version.
"""

import json
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

_FALLBACK_TEMPLATE_ID = "fast_hype"


def _load_templates(templates_dir: Path) -> dict[str, dict]:
    """Load all non-deprecated templates, keyed by template_id.

    For each template_id, only the highest version is kept.
    """
    raw: list[dict] = []
    for json_file in sorted(templates_dir.rglob("*.json")):
        if "schema" in json_file.parts:
            continue
        try:
            template = json.loads(json_file.read_text())
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load template {json_file}: {e}")
            continue
        if template.get("deprecated", False):
            continue
        raw.append(template)

    by_id: dict[str, dict] = {}
    for t in raw:
        tid = t.get("template_id", "")
        if tid not in by_id or t.get("version", 0) > by_id[tid].get("version", 0):
            by_id[tid] = t

    return by_id


def select_template(metadata: dict, config: dict) -> dict:
    """Return the configured default template for this clip.

    Args:
        metadata: Feature dict produced by run_feature_extraction.
        config: Full parsed config.yaml dict.

    Returns:
        The full template dict, or {} if no templates are found.
    """
    templates_dir = Path(config["paths"]["templates"])
    templates = _load_templates(templates_dir)

    if not templates:
        logger.error("No templates found — check templates/ directory.")
        return {}

    template_id = config.get("default_template_id", _FALLBACK_TEMPLATE_ID)
    template = templates.get(template_id) or templates.get(_FALLBACK_TEMPLATE_ID)
    if template is None:
        template = next(iter(templates.values()))
        logger.warning(f"Template '{template_id}' not found, using '{template.get('template_id')}'.")

    selected_id = template.get("template_id")
    selected_ver = template.get("version")
    logger.info(f"Selected template '{selected_id}' v{selected_ver} for clip: {metadata.get('clip_id', 'unknown')}")

    clip_path = metadata.get("clip_path")
    if clip_path:
        meta_path = Path(clip_path).with_suffix(".meta.json")
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            meta["selected_template_id"] = selected_id
            meta["template_version"] = selected_ver
            meta_path.write_text(json.dumps(meta, indent=2))

    return template

"""
Stage 4 — Decision Engine

Selects a creative template for a clip using simple deterministic routing.
Templates are creative presets (aspect ratio, color grade, effects, captions);
the engine's job is to pick the most appropriate one based on a few obvious
signals, not to score or rank.

Routing priority (first match wins):
  1. tutorial_tips    — clip keywords contain tutorial/guide terms
  2. recap_montage    — duration >= 120 seconds
  3. cinematic_highlight — low motion AND low/medium audio (slow moment)
  4. commentary_reaction — low motion with active mic (talking head)
  5. fast_hype        — default for all other FPS gaming clips

The clip's .meta.json is updated with: selected_template_id, template_version.
"""

import json
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_TEMPLATE_ID = "fast_hype"

# Keywords that indicate a tutorial or guide clip
_TUTORIAL_KEYWORDS = {
    "how", "guide", "tip", "tips", "trick", "tricks",
    "tutorial", "walkthrough", "beginner", "basics", "explained",
    "learn", "build", "setup", "settings", "loadout",
}


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

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

    logger.debug(f"Loaded {len(by_id)} template(s): {list(by_id.keys())}")
    return by_id


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

def _route(metadata: dict) -> str:
    """Return the template_id that best fits this clip.

    Args:
        metadata: Feature dict from Stage 3 (feature_extraction).

    Returns:
        A template_id string.
    """
    duration = metadata.get("duration_seconds", 0)
    motion = metadata.get("motion_level", "medium")
    audio = metadata.get("audio_energy", "medium")
    keywords = {k.lower() for k in metadata.get("keywords", [])}

    if keywords & _TUTORIAL_KEYWORDS:
        return "tutorial_tips"

    if duration >= 120:
        return "recap_montage"

    if motion == "low" and audio in ("low", "medium"):
        return "cinematic_highlight"

    if motion == "low" and audio == "high":
        return "commentary_reaction"

    return DEFAULT_TEMPLATE_ID


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def select_template(metadata: dict, config: dict) -> dict:
    """Select a template for a clip and persist the result to .meta.json.

    Args:
        metadata: Full feature dict produced by run_feature_extraction.
        config: Full parsed config.yaml dict.

    Returns:
        The full template dict of the selected template, or {} on failure.
    """
    templates_dir = Path(config["paths"]["templates"])
    templates = _load_templates(templates_dir)

    if not templates:
        logger.error("No templates found — check templates/ directory.")
        return {}

    template_id = _route(metadata)

    # Fall back to default if the routed template isn't available
    template = templates.get(template_id) or templates.get(DEFAULT_TEMPLATE_ID)
    if template is None:
        template = next(iter(templates.values()))
        logger.warning(f"Default template '{DEFAULT_TEMPLATE_ID}' not found, using '{template.get('template_id')}'.")

    selected_id = template.get("template_id")
    selected_ver = template.get("version")
    logger.info(
        f"Selected template '{selected_id}' v{selected_ver} "
        f"(routed via: {template_id}) for clip: {metadata.get('clip_id', 'unknown')}"
    )

    # Persist selection into the clip's .meta.json
    clip_path = metadata.get("clip_path")
    if clip_path:
        meta_path = Path(clip_path).with_suffix(".meta.json")
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            meta["selected_template_id"] = selected_id
            meta["template_version"] = selected_ver
            meta_path.write_text(json.dumps(meta, indent=2))

    return template

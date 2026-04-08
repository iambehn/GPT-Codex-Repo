"""
Stage 4 — Decision Engine

Loads all available (non-deprecated) templates from the templates/ directory,
scores each one against a clip's feature metadata using the weighted condition
system defined in template.schema.v1.json, and returns the highest-scoring
eligible template.

Scoring algorithm:
  1. For each template, evaluate every trigger_condition against the metadata.
  2. Sum the weights of all passing conditions → weighted_score.
  3. If weighted_score >= template's min_score_threshold → template is eligible.
  4. Return the eligible template with the highest weighted_score.
  5. If no template is eligible, fall back to 'fast_hype' (default FPS template).

When multiple versions of the same template exist (e.g. fast_hype.v1.json and
fast_hype.v2.json), only the highest version is considered.

Supported condition operators (from schema):
  eq           — metadata[field] == value
  in           — metadata[field] in value  (value is a list)
  gte          — metadata[field] >= value
  lte          — metadata[field] <= value
  between      — value[0] <= metadata[field] <= value[1]
  contains_any — any(kw in metadata['keywords'] for kw in value)

The clip's .meta.json is updated with: selected_template_id, template_version,
template_score.
"""

import json
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

FALLBACK_TEMPLATE_ID = "fast_hype"


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

def _load_templates(templates_dir: Path) -> list[dict]:
    """Discover and load all non-deprecated template JSON files.

    Skips anything inside a 'schema' subdirectory. For each template_id, only
    the highest version number is kept.

    Args:
        templates_dir: Path to the templates/ folder.

    Returns:
        List of template dicts, one per unique template_id.
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
            logger.debug(f"Skipping deprecated template: {template.get('template_id')} v{template.get('version')}")
            continue

        raw.append(template)

    # Keep only the highest version per template_id
    by_id: dict[str, dict] = {}
    for t in raw:
        tid = t.get("template_id", "")
        if tid not in by_id or t.get("version", 0) > by_id[tid].get("version", 0):
            by_id[tid] = t

    templates = list(by_id.values())
    logger.debug(f"Loaded {len(templates)} template(s): {[t['template_id'] for t in templates]}")
    return templates


# ---------------------------------------------------------------------------
# Condition evaluation
# ---------------------------------------------------------------------------

def _evaluate_condition(condition: dict, metadata: dict) -> bool:
    """Test a single trigger_condition against clip metadata.

    Args:
        condition: Condition dict with keys: field, operator, value, weight.
        metadata: Full clip feature dict from Feature Extraction.

    Returns:
        True if the condition passes, False otherwise.
    """
    field = condition["field"]
    operator = condition["operator"]
    value = condition["value"]

    meta_val = metadata.get(field)
    if meta_val is None:
        return False

    try:
        if operator == "eq":
            return meta_val == value

        if operator == "in":
            return meta_val in value

        if operator == "gte":
            return float(meta_val) >= float(value)

        if operator == "lte":
            return float(meta_val) <= float(value)

        if operator == "between":
            lo, hi = float(value[0]), float(value[1])
            return lo <= float(meta_val) <= hi

        if operator == "contains_any":
            # meta_val is the clip's keywords list; value is the terms to check
            if not isinstance(meta_val, list):
                return False
            meta_lower = [k.lower() for k in meta_val]
            return any(v.lower() in meta_lower for v in value)

    except (TypeError, ValueError, IndexError) as e:
        logger.warning(f"Condition eval error (field={field}, op={operator}): {e}")
        return False

    logger.warning(f"Unknown operator '{operator}' in condition for field '{field}'")
    return False


def _score_template(template: dict, metadata: dict) -> float:
    """Compute the weighted match score for a template against clip metadata.

    Args:
        template: Full template dict.
        metadata: Clip feature dict from Feature Extraction.

    Returns:
        Float in [0.0, 1.0] — sum of weights for all passing conditions.
    """
    conditions = template.get("trigger_conditions", {}).get("conditions", [])
    return sum(
        c["weight"] for c in conditions if _evaluate_condition(c, metadata)
    )


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def select_template(metadata: dict, config: dict) -> dict:
    """Select the best-matching template for a clip.

    Scores every loaded template against the clip's metadata, returns the
    highest-scoring eligible one. Falls back to fast_hype if none qualify.
    Updates the clip's .meta.json with the selection result.

    Args:
        metadata: Full feature dict produced by run_feature_extraction.
        config: Full parsed config.yaml dict.

    Returns:
        The full template dict of the selected template.
    """
    templates_dir = Path(config["paths"]["templates"])
    templates = _load_templates(templates_dir)

    if not templates:
        logger.error("No templates found — check templates/ directory.")
        return {}

    best_template: dict | None = None
    best_score: float = -1.0
    scores: dict[str, float] = {}

    for template in templates:
        score = _score_template(template, metadata)
        threshold = template.get("trigger_conditions", {}).get("min_score_threshold", 1.0)
        tid = template.get("template_id", "unknown")
        scores[tid] = round(score, 4)

        if score >= threshold and score > best_score:
            best_score = score
            best_template = template

    logger.debug(f"Template scores: {scores}")

    if best_template is None:
        logger.warning(
            f"No template met its threshold (scores: {scores}). "
            f"Falling back to '{FALLBACK_TEMPLATE_ID}'."
        )
        fallback = next(
            (t for t in templates if t.get("template_id") == FALLBACK_TEMPLATE_ID),
            templates[0],
        )
        best_template = fallback
        best_score = scores.get(FALLBACK_TEMPLATE_ID, 0.0)

    selected_id = best_template.get("template_id")
    selected_ver = best_template.get("version")
    logger.info(
        f"Selected template '{selected_id}' v{selected_ver} "
        f"(score={round(best_score, 4)}) for clip: "
        f"{metadata.get('clip_id', 'unknown')}"
    )

    # Persist selection into the clip's .meta.json
    clip_path = metadata.get("clip_path")
    if clip_path:
        meta_path = Path(clip_path).with_suffix(".meta.json")
        if meta_path.exists():
            meta = json.loads(meta_path.read_text())
            meta["selected_template_id"] = selected_id
            meta["template_version"] = selected_ver
            meta["template_score"] = round(best_score, 4)
            meta_path.write_text(json.dumps(meta, indent=2))

    return best_template

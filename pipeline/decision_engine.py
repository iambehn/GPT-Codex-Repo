"""
Stage 4 — Decision Engine

Loads all available templates from the templates/ directory and scores each one
against the clip's feature metadata using the weighted condition system defined
in template.schema.v1.json. Returns the highest-scoring eligible template.

Matching logic:
  1. For each template, evaluate every trigger_condition against the clip metadata.
  2. Sum the weights of passing conditions → weighted_score.
  3. If weighted_score >= template's min_score_threshold, the template is eligible.
  4. Return the eligible template with the highest weighted_score.
  5. If no template is eligible, fall back to fast_hype (default FPS template).

Template selection is currently rule-based. ML-based selection is a future upgrade.
"""

from utils.logger import get_logger

logger = get_logger(__name__)


def select_template(metadata: dict, config: dict) -> dict:
    """Select the best-matching template for a clip based on its feature metadata.

    Args:
        metadata: Feature dict produced by run_feature_extraction.
        config: Full parsed config.yaml dict.

    Returns:
        The full template dict of the selected template (parsed JSON).
    """
    pass

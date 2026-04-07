"""
Stage 6 — AI Scoring Engine

Sends the finished processed clip's transcript and metadata to the Claude API
and receives a structured virality score in return. The score is stored in the
clip's metadata JSON and surfaced in the Manual Review UI.

Claude prompt includes:
  - Game name and clip duration
  - Full transcript text
  - Key metadata signals (motion_level, audio_energy, keywords matched)
  - Template applied
  - Scoring instructions: return JSON with highlight_score (0-100), clip_type,
    suggested_title, suggested_caption, and score_reasoning

The scoring weights are not hardcoded — they are part of the Claude prompt and
can be tuned via the Optimize stage without code changes.
"""

from utils.logger import get_logger

logger = get_logger(__name__)


def run_scoring(clip_path: str, metadata: dict, config: dict) -> dict:
    """Score a processed clip using the Claude API.

    Args:
        clip_path: Path to the processed clip in processing/{game}/.
        metadata: Feature dict from run_feature_extraction (updated with template info).
        config: Full parsed config.yaml dict.

    Returns:
        Scoring result dict with keys: highlight_score, clip_type, suggested_title,
        suggested_caption, score_reasoning. Also written into the clip's .meta.json.
    """
    pass

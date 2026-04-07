"""
Stage 5 — Processing

Applies the template selected by the Decision Engine to a clip using FFmpeg.
All video manipulation is done via FFmpeg subprocess calls — no Python video
libraries. Output files land in processing/{game}/ before moving to accepted/
or rejected/ after Manual Review.

Operations driven by template settings:
  - Lossless trim to target duration (-c copy where possible)
  - Vertical fill: blur pillarbox composite (16:9 → 9:16)
  - Zoom effects: zoompan filter (slow_push, punch_in, dynamic)
  - Color grade: eq filter (brightness, contrast, saturation) + optional LUT
  - Caption burn-in: subtitles filter from Whisper SRT output
  - Transitions: xfade filter (crossfade, zoom_blur, flash)
  - Vignette, film grain, chromatic aberration: custom filter chains
  - Audio: volume, loudnorm, amix for background music, acompressor for ducking
  - Final encode: libx264, CRF-based quality, AAC audio
"""

from utils.logger import get_logger

logger = get_logger(__name__)


def run_processing(clip_path: str, template: dict, metadata: dict, config: dict) -> str:
    """Apply a template to a clip and produce the processed output file.

    Args:
        clip_path: Path to the source clip in inbox/{game}/.
        template: Full template dict from select_template.
        metadata: Feature dict from run_feature_extraction.
        config: Full parsed config.yaml dict.

    Returns:
        Path to the processed output file in processing/{game}/.
    """
    pass

"""
Stage 3 — Feature Extraction

Reads a clip and its Whisper transcript and produces a structured metadata JSON
file used by the Decision Engine to select a template.

Extracted fields:
  - duration_seconds       (from FFprobe)
  - resolution_height      (from FFprobe)
  - fps                    (from FFprobe)
  - has_audio              (from FFprobe)
  - audio_peak_db          (from FFprobe / ffmpeg volumedetect)
  - audio_avg_db           (from FFprobe / ffmpeg volumedetect)
  - silence_ratio          (fraction of clip that is silent)
  - motion_level           (scene change frequency via PySceneDetect — low/medium/high)
  - audio_energy           (derived from audio levels — low/medium/high)
  - keywords               (FPS keyword matches found in transcript)
  - quality_tag            (carried over from Ingestion)
"""

from utils.logger import get_logger

logger = get_logger(__name__)


def run_feature_extraction(clip_path: str, transcript: dict, config: dict) -> dict:
    """Extract structured metadata from a clip and its transcript.

    Args:
        clip_path: Path to the video file.
        transcript: Whisper result dict (output of run_transcription).
        config: Full parsed config.yaml dict.

    Returns:
        Feature dict written to <clip_path>.meta.json and returned in memory.
    """
    pass

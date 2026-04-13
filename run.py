"""
Gaming Clip Farming Bot — Pipeline Entry Point

Usage:
    python run.py --game arc_raiders
    python run.py --game marvel_rivals
    python run.py --game deadlock
    python run.py --game all          # run for all configured games
    python run.py --distribute        # upload all approved clips, log analytics, backup

The pipeline runs each stage in sequence for the given game:
  1. Ingestion          — download clips from Twitch
  2. Transcription      — Whisper speech-to-text
  3. Feature Extraction — build metadata JSON per clip
  4. Decision Engine    — select template per clip
  5. Processing         — FFmpeg render
  6. AI Scoring         — Claude API virality score
  7. [Manual Review launched separately: python -m pipeline.review.app]
  8. Distribution       — python run.py --distribute
     a. Upload to enabled social media platforms
     b. Log row to Google Sheets (analytics)
     c. Back up clip + meta to Google Drive
"""

import argparse
import json
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

from utils.analytics import log_clip
from utils.backup import backup_clip
from utils.file_utils import ensure_dirs
from utils.logger import get_logger

from pipeline.ingestion import run_ingestion
from pipeline.transcription import run_transcription
from pipeline.feature_extraction import run_feature_extraction
from pipeline.decision_engine import select_template
from pipeline.processing import run_processing
from pipeline.scoring import run_scoring
from pipeline.distribution import run_distribution

logger = get_logger(__name__)


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_pipeline_for_game(game: str, config: dict) -> None:
    if game not in config["games"]:
        logger.error(f"Unknown game '{game}'. Valid options: {list(config['games'].keys())}")
        sys.exit(1)

    logger.info(f"Starting pipeline for: {config['games'][game]['display_name']}")

    clips = run_ingestion(game, config)
    if not clips:
        logger.info("No clips ingested. Exiting.")
        return

    for clip in clips:
        if clip.get("review_status"):
            logger.debug(f"Skipping already-reviewed clip: {clip.get('clip_id')}")
            continue

        clip_path = clip["clip_path"]
        logger.info(f"Processing clip: {clip_path}")

        transcript = run_transcription(clip_path, config)
        metadata = run_feature_extraction(clip_path, transcript, config)
        template = select_template(metadata, config)
        processed_path = run_processing(clip_path, template, metadata, config)
        score = run_scoring(processed_path, metadata, config)

        logger.info(
            f"Clip ready for review — score: {score.get('highlight_score', 'n/a')} "
            f"| template: {template.get('template_id', 'n/a')}"
        )

    logger.info(f"Pipeline complete for {game}. Launch review UI: python -m pipeline.review.app")


def run_distribution_for_all(config: dict) -> None:
    """Distribute all approved clips that have not yet been posted.

    Scans accepted/{game}/ for .mp4 files, loads their metadata from
    inbox/{game}/, then runs distribution → analytics → backup in sequence.
    Fully idempotent: already-distributed platforms are skipped.
    """
    accepted_root = Path(config["paths"]["accepted"])
    inbox_root = Path(config["paths"]["inbox"])
    total = 0
    distributed = 0

    for game in config["games"]:
        game_dir = accepted_root / game
        if not game_dir.exists():
            continue

        for clip_file in sorted(game_dir.glob("*.mp4")):
            total += 1
            # Find the matching meta.json in inbox/
            # Meta files are named after clip_id; try to match by stem components
            meta_path = _find_meta_for_clip(clip_file, inbox_root / game)
            if meta_path is None:
                logger.warning(f"No meta.json found for {clip_file.name} — skipping distribution.")
                continue

            metadata = json.loads(meta_path.read_text())

            # Only distribute accepted clips
            if metadata.get("review_status") != "accepted":
                continue

            logger.info(f"Distributing: {clip_file.name}")
            dist_results = run_distribution(str(clip_file), metadata, config)

            # Reload metadata after distribution (it may have been updated)
            metadata = json.loads(meta_path.read_text())

            log_clip(metadata, dist_results)
            backup_clip(str(clip_file), metadata, config)
            distributed += 1

    logger.info(f"Distribution complete: {distributed}/{total} clip(s) processed.")
    if distributed == 0 and total == 0:
        logger.info("No clips in accepted/ yet. Run the pipeline then approve clips in the review UI.")


def _find_meta_for_clip(clip_file: Path, inbox_game_dir: Path) -> Path | None:
    """Locate the inbox .meta.json that matches an accepted clip.

    The accepted clip filename format is: {game}_{date}_{clip_id}.mp4
    We look for a .meta.json whose 'clip_id' field matches the clip_id
    portion of the filename, or fall back to a full-stem name match.
    """
    # Extract clip_id: everything after {game}_{date}_
    parts = clip_file.stem.split("_", 2)
    clip_id_guess = parts[2] if len(parts) == 3 else clip_file.stem

    # Fast path: look for <clip_id>.meta.json directly
    candidate = inbox_game_dir / f"{clip_id_guess}.meta.json"
    if candidate.exists():
        return candidate

    # Slow path: scan all meta files and match by clip_id field
    for meta_file in inbox_game_dir.glob("*.meta.json"):
        try:
            meta = json.loads(meta_file.read_text())
            if meta.get("clip_id") == clip_id_guess:
                return meta_file
        except (json.JSONDecodeError, OSError):
            continue

    return None


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Gaming Clip Farming Bot")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--game",
        help="Game key to process (e.g. arc_raiders) or 'all' for every configured game.",
    )
    group.add_argument(
        "--distribute",
        action="store_true",
        help="Upload all approved clips from accepted/ to social media, log analytics, and back up to Drive.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml).",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    ensure_dirs(config)

    if args.distribute:
        run_distribution_for_all(config)
    elif args.game == "all":
        for game in config["games"]:
            run_pipeline_for_game(game, config)
    else:
        run_pipeline_for_game(args.game, config)


if __name__ == "__main__":
    main()

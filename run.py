"""
Gaming Clip Farming Bot — Pipeline Entry Point

Usage:
    python run.py --game arc_raiders
    python run.py --game marvel_rivals
    python run.py --game deadlock
    python run.py --game all          # run for all configured games

The pipeline runs each stage in sequence for the given game:
  1. Ingestion       — download clips from Twitch
  2. Transcription   — Whisper speech-to-text
  3. Feature Extraction — build metadata JSON per clip
  4. Decision Engine — select template per clip
  5. Processing      — FFmpeg render
  6. AI Scoring      — Claude API virality score
  7. [Manual Review is launched separately via: python -m pipeline.review.app]
  8. Distribution    — publish approved clips (triggered post-review)
"""

import argparse
import os
import sys

import yaml
from dotenv import load_dotenv

from utils.file_utils import ensure_dirs
from utils.logger import get_logger

from pipeline.ingestion import run_ingestion
from pipeline.transcription import run_transcription
from pipeline.feature_extraction import run_feature_extraction
from pipeline.decision_engine import select_template
from pipeline.processing import run_processing
from pipeline.scoring import run_scoring

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


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Gaming Clip Farming Bot")
    parser.add_argument(
        "--game",
        required=True,
        help="Game key to process (e.g. arc_raiders) or 'all' for every configured game.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml).",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    ensure_dirs(config)

    if args.game == "all":
        for game in config["games"]:
            run_pipeline_for_game(game, config)
    else:
        run_pipeline_for_game(args.game, config)


if __name__ == "__main__":
    main()

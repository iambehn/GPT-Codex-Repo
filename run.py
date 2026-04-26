"""
Gaming Clip Farming Bot — Pipeline Entry Point

Usage:
    python run.py --game arc_raiders
    python run.py --game marvel_rivals
    python run.py --game deadlock
    python run.py --game all
    python run.py --init-game valorant
    python run.py --validate-game-pack marvel_rivals
    python run.py --evaluate-game-pack marvel_rivals
    python run.py --build-yolo-dataset marvel_rivals
    python run.py --train-yolo marvel_rivals
    python run.py --refresh-weapon-detector marvel_rivals
    python run.py --audit-weapon-detector marvel_rivals
    python run.py --render-weapon-audit-review marvel_rivals --report assets/games/marvel_rivals/reports/weapon_detector/20260425-231327.json
    python run.py --promote-weapon-audit-crop marvel_rivals --rank 1 --overwrite
    python run.py --review-feedback marvel_rivals
    python run.py --apply-feedback marvel_rivals
    python run.py --perf-feedback marvel_rivals
    python run.py --apply-perf-feedback marvel_rivals
    python run.py --enrich-quarantine marvel_rivals
    python run.py --enrich-game-from-wiki marvel_rivals --wiki-url https://example.fandom.com/wiki/Characters
    python run.py --distribute
    python run.py --schedule-distribution
    python run.py --run-distribution-queue
    python run.py --distribution-status

Pipeline order:
  1. Ingestion
  2. Audio / CV / AI detectors
  3. Hook Enforcer
  4. Clip Judge
  5. Transcription
  6. Feature Extraction
  7. Template Selection
  8. Processing
  9. AI Scoring
  10. Manual Review
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv

from pipeline.audio_detector import run_audio_detector
from pipeline.clip_judge import evaluate as evaluate_clip
from pipeline.decision_engine import select_template
from pipeline.distribution import list_reddit_flairs, poll_tiktok_pending
from pipeline.distribution_queue import (
    distribution_status,
    mark_manual_posted,
    run_distribution_queue,
    schedule_distribution_tasks,
)
from pipeline.feature_extraction import run_feature_extraction
from pipeline.game_pack import (
    get_game_metadata,
    list_supported_games,
    load_game_pack,
    print_validation_report,
    scaffold_game_pack,
    validate_game_pack,
)
from pipeline.game_pack_evaluator import evaluate_game_pack, scaffold_gold_set
from pipeline.ingestion import run_ingestion
from pipeline.hook_enforcer import run_hook_enforcer
from pipeline.kill_feed import run_kill_feed_parser
from pipeline.montage import run_montage
from pipeline.niceshot_detector import run_niceshot_detector
from pipeline.processing import run_processing
from pipeline.performance_feedback import apply_performance_updates
from pipeline.review_feedback import apply_feedback_updates, summarize_feedback
from pipeline.scoring import run_scoring
from pipeline.title_engine import generate_title
from pipeline.transcription import run_transcription
from pipeline.weapon_detector import run_weapon_detector
from pipeline.weapon_detector_audit import audit_weapon_detector
from pipeline.weapon_asset_review import render_weapon_audit_review
from pipeline.weapon_icon_promotion import promote_weapon_audit_crop
from pipeline.wiki_enrichment import enrich_game_from_wiki
from pipeline.yolo_detector import run_yolo_detector
from pipeline.yolo_dataset import build_yolo_dataset
from pipeline.yolo_training import train_yolo_model
from utils.file_utils import ensure_dirs, move_to_quarantine
from utils.logger import get_logger
from utils.metadata_injector import inject_metadata

logger = get_logger(__name__)


def load_config(config_path: str = "config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _detector_enabled(config: dict, game_pack: dict, detector_name: str) -> bool:
    pack_detectors = (game_pack.get("game") or {}).get("detectors") or {}
    if detector_name in pack_detectors:
        return bool((pack_detectors.get(detector_name) or {}).get("enabled", False))
    return bool((config.get(detector_name) or {}).get("enabled", False))


def _load_meta(meta_path: Path) -> dict:
    return json.loads(meta_path.read_text()) if meta_path.exists() else {}


def _write_meta(meta_path: Path, meta: dict) -> None:
    meta_path.write_text(json.dumps(meta, indent=2))


def _move_to_stage_with_sidecar(src: Path, dest_dir: Path, meta_updates: dict | None = None) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.move(str(src), str(dest))

    meta_src = src.with_suffix(".meta.json")
    if meta_src.exists():
        meta_dest = dest.with_suffix(".meta.json")
        shutil.move(str(meta_src), str(meta_dest))
        meta = _load_meta(meta_dest)
        meta["clip_path"] = str(dest)
        meta["meta_path"] = str(meta_dest)
        if meta_updates:
            meta.update(meta_updates)
        _write_meta(meta_dest, meta)

    return dest


def _process_clip(clip_path: Path, game: str, config: dict, force_detectors: bool = False) -> None:
    game_pack = load_game_pack(game, config, create_missing=True)

    if _detector_enabled(config, game_pack, "audio_detector"):
        run_audio_detector(clip_path, game, config, force=force_detectors)

    if _detector_enabled(config, game_pack, "kill_feed"):
        kf_result = run_kill_feed_parser(clip_path, game, config, force=force_detectors)
        if not kf_result["passed"]:
            logger.debug(
                f"[kill_feed] Low sweat score ({kf_result['sweat_score']}) "
                f"for {clip_path.name} — continuing into clip_judge."
            )

    if _detector_enabled(config, game_pack, "weapon_detector"):
        run_weapon_detector(clip_path, game, config, force=force_detectors)

    run_niceshot_detector(clip_path, game, config, game_pack=game_pack, force=force_detectors)
    run_yolo_detector(clip_path, game, config, game_pack=game_pack, force=force_detectors)
    run_hook_enforcer(clip_path, game, config, game_pack=game_pack, force=force_detectors)

    judge = evaluate_clip(clip_path, game_pack, config, force=force_detectors)
    decision = judge.get("decision", {})
    status = decision.get("status", "quarantine")
    quarantine_reason = (judge.get("quarantine") or {}).get("reason") or "low_confidence"

    if status == "quarantine":
        move_to_quarantine(clip_path, game, config, reason=quarantine_reason)
        logger.info(f"[clip_judge] Quarantined {clip_path.name} ({quarantine_reason})")
        return

    if status == "reject":
        dest = _move_to_stage_with_sidecar(
            clip_path,
            Path(config["paths"]["rejected"]) / game,
            meta_updates={"pre_review_status": "rejected"},
        )
        logger.info(f"[clip_judge] Rejected {dest.name} before transcription.")
        return

    transcript = run_transcription(str(clip_path), config)
    if transcript is None:
        logger.info(f"Skipping clip (language filter): {clip_path}")
        return

    metadata = run_feature_extraction(str(clip_path), transcript, config)
    template = select_template(metadata, config)
    processed_path = run_processing(str(clip_path), template, metadata, config)
    score = run_scoring(processed_path, metadata, config)

    if config.get("title_engine", {}).get("enabled", False):
        title_result = generate_title(clip_path, game, config)
        logger.info(f"[title_engine] Proposed title: '{title_result.get('title')}'")
        inject_metadata(Path(processed_path), clip_path, config)

    logger.info(
        f"Clip ready for review — score: {score.get('highlight_score', 'n/a')} "
        f"| template: {template.get('template_id', 'n/a')}"
    )


def run_pipeline_for_game(game: str, config: dict) -> None:
    known_games = list_supported_games(config)
    if game not in known_games:
        logger.error(f"Unknown game '{game}'. Valid options: {known_games}")
        sys.exit(1)

    game_pack = load_game_pack(game, config, create_missing=True)
    game_meta = get_game_metadata(game, config, game_pack)
    logger.info(f"Starting pipeline for: {game_meta['display_name']}")

    clips = run_ingestion(game, config)
    if not clips:
        logger.info("No clips ingested. Exiting.")
        return

    for clip in clips:
        if clip.get("review_status"):
            logger.debug(f"Skipping already-reviewed clip: {clip.get('clip_id')}")
            continue

        clip_path = Path(clip["clip_path"])
        logger.info(f"Processing clip: {clip_path}")
        _process_clip(clip_path, game, config)

    logger.info(f"Pipeline complete for {game}. Launch review UI: python -m pipeline.review.app")


def run_distribution_for_all(config: dict, dry_run: bool = False) -> None:
    """Compatibility wrapper: schedule accepted clips, then run due queue items."""
    logger.info("--distribute now uses the SQLite distribution queue.")
    scheduled = schedule_distribution_tasks(config)
    logger.info(
        "Scheduled distribution tasks: "
        f"created={scheduled.get('created', 0)}, skipped={scheduled.get('skipped', 0)}, "
        f"manual={scheduled.get('manual', 0)}, paused={scheduled.get('paused', 0)}"
    )
    result = run_distribution_queue(config, dry_run=dry_run)
    logger.info(
        "Distribution queue run: "
        f"due={result.get('due', 0)}, posted={result.get('posted', 0)}, "
        f"retryable={result.get('retryable', 0)}, terminal={result.get('terminal', 0)}, "
        f"skipped={result.get('skipped', 0)}"
    )


def _find_meta_for_clip(clip_file: Path, inbox_game_dir: Path) -> Path | None:
    """Locate the inbox .meta.json that matches an accepted clip."""
    parts = clip_file.stem.split("_", 2)
    clip_id_guess = parts[2] if len(parts) == 3 else clip_file.stem

    candidate = inbox_game_dir / f"{clip_id_guess}.meta.json"
    if candidate.exists():
        return candidate

    for meta_file in inbox_game_dir.glob("*.meta.json"):
        try:
            meta = json.loads(meta_file.read_text())
            if meta.get("clip_id") == clip_id_guess:
                return meta_file
        except (json.JSONDecodeError, OSError):
            continue

    return None


def enrich_quarantine(game: str, config: dict) -> None:
    quarantine_root = Path(config["paths"]["quarantine"]) / game
    inbox_root = Path(config["paths"]["inbox"]) / game

    if not quarantine_root.exists():
        logger.info(f"No quarantine folder found for {game}.")
        return

    meta_files = sorted(quarantine_root.rglob("*.meta.json"))
    if not meta_files:
        logger.info(f"No quarantined sidecars found for {game}.")
        return

    restored = 0
    for meta_file in meta_files:
        meta = _load_meta(meta_file)
        clip_path = Path(meta.get("clip_path", meta_file.with_suffix(".mp4")))
        if not clip_path.exists():
            sibling_clip = meta_file.with_suffix(".mp4")
            if sibling_clip.exists():
                clip_path = sibling_clip
            else:
                logger.warning(f"Skipping {meta_file.name} — clip missing.")
                continue

        game_pack = load_game_pack(game, config, create_missing=True)
        judge = evaluate_clip(clip_path, game_pack, config, force=True)
        if (judge.get("decision") or {}).get("status") != "accept":
            logger.info(
                f"[enrich_quarantine] {clip_path.name} still unresolved "
                f"({(judge.get('quarantine') or {}).get('reason', 'quarantine')})"
            )
            continue

        restored_clip = _move_to_stage_with_sidecar(clip_path, inbox_root)
        restored += 1
        logger.info(f"[enrich_quarantine] Restored {restored_clip.name} to inbox.")
        _process_clip(restored_clip, game, config, force_detectors=False)

    logger.info(f"[enrich_quarantine] Restored {restored} clip(s) for {game}.")


def refresh_weapon_detector(game: str, config: dict, frame_sample: str | None = None) -> dict:
    stage_keys = ("inbox", "quarantine", "processing", "accepted")
    refreshed = 0
    skipped_missing = 0
    scanned = 0
    detector_config = dict(config)
    detector_config["weapon_detector"] = dict(config.get("weapon_detector") or {})
    if frame_sample:
        detector_config["weapon_detector"]["frame_sample"] = frame_sample

    for stage in stage_keys:
        root = Path(config["paths"].get(stage, "")) / game
        if not root.exists():
            continue

        for meta_file in sorted(root.rglob("*.meta.json")):
            scanned += 1
            meta = _load_meta(meta_file)
            clip_path = Path(meta.get("clip_path", "")) if meta.get("clip_path") else None
            if clip_path is None or not clip_path.exists():
                sibling_candidates = [meta_file.with_suffix(ext) for ext in (".mp4", ".mov", ".m4v", ".webm")]
                clip_path = next((candidate for candidate in sibling_candidates if candidate.exists()), None)
            if clip_path is None or not clip_path.exists():
                skipped_missing += 1
                logger.warning(f"[refresh_weapon_detector] Missing clip for {meta_file}")
                continue

            run_weapon_detector(clip_path, game, detector_config, force=True)
            refreshed += 1

    summary = {
        "ok": True,
        "game": game,
        "scanned_meta_files": scanned,
        "refreshed": refreshed,
        "skipped_missing": skipped_missing,
        "stages": list(stage_keys),
        "frame_sample": detector_config["weapon_detector"].get("frame_sample"),
    }
    logger.info(
        f"[refresh_weapon_detector] {game}: refreshed={refreshed}, "
        f"scanned={scanned}, missing={skipped_missing}, "
        f"frame_sample={detector_config['weapon_detector'].get('frame_sample')}"
    )
    return summary


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Gaming Clip Farming Bot")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--game", help="Game key to process or 'all'.")
    group.add_argument(
        "--distribute",
        action="store_true",
        help="Compatibility wrapper: schedule accepted clips and run due distribution queue items.",
    )
    group.add_argument(
        "--schedule-distribution",
        action="store_true",
        dest="schedule_distribution",
        help="Create distribution queue tasks for accepted clips.",
    )
    group.add_argument(
        "--run-distribution-queue",
        action="store_true",
        dest="run_distribution_queue",
        help="Run due official-API distribution queue tasks.",
    )
    group.add_argument(
        "--distribution-status",
        action="store_true",
        dest="distribution_status",
        help="Print distribution queue counts and recent tasks.",
    )
    group.add_argument(
        "--mark-manual-posted",
        metavar="TASK_ID",
        dest="mark_manual_posted",
        help="Mark a human-assisted distribution task as posted. Requires --url.",
    )
    group.add_argument(
        "--watch",
        action="store_true",
        help="Continuously run the pipeline for all games on a loop.",
    )
    group.add_argument(
        "--poll-tiktok",
        action="store_true",
        dest="poll_tiktok",
        help="Check TikTok processing status for uploaded clips that don't have a URL yet.",
    )
    group.add_argument(
        "--list-reddit-flairs",
        action="store_true",
        dest="list_reddit_flairs",
        help="Print available link flairs for each configured subreddit, then exit.",
    )
    group.add_argument(
        "--montage",
        metavar="GAME",
        help="Assemble a montage from accepted clips for GAME (or 'all').",
    )
    group.add_argument(
        "--init-game",
        metavar="GAME",
        dest="init_game",
        help="Scaffold a draft game pack under assets/games/GAME.",
    )
    group.add_argument(
        "--validate-game-pack",
        metavar="GAME",
        dest="validate_game_pack",
        help="Validate the required files and references for a game pack.",
    )
    group.add_argument(
        "--evaluate-game-pack",
        metavar="GAME",
        dest="evaluate_game_pack",
        help="Evaluate a game pack against assets/games/GAME/examples/gold_set/manifest.yaml.",
    )
    group.add_argument(
        "--build-yolo-dataset",
        metavar="GAME",
        dest="build_yolo_dataset",
        help="Build the per-game YOLO dataset registry, labels, and seed manifest.",
    )
    group.add_argument(
        "--train-yolo",
        metavar="GAME",
        dest="train_yolo",
        help="Train a YOLO model from the per-game exported dataset and promote best.pt into the model registry.",
    )
    group.add_argument(
        "--refresh-weapon-detector",
        metavar="GAME",
        dest="refresh_weapon_detector",
        help="Rerun the weapon detector across existing clips for a game and refresh sidecar metadata.",
    )
    group.add_argument(
        "--audit-weapon-detector",
        metavar="GAME",
        dest="audit_weapon_detector",
        help="Rank weapon-detector near misses for a game and export candidate ROI crops from real clips.",
    )
    group.add_argument(
        "--promote-weapon-audit-crop",
        metavar="GAME",
        dest="promote_weapon_audit_crop",
        help="Promote one exported audit crop into assets/weapon_icons/<game>/ with optional backup/overwrite.",
    )
    group.add_argument(
        "--render-weapon-audit-review",
        metavar="GAME",
        dest="render_weapon_audit_review",
        help="Render side-by-side comparison images from a weapon-detector audit report.",
    )
    group.add_argument(
        "--review-feedback",
        metavar="GAME",
        dest="review_feedback",
        help="Summarize review feedback, ROI requests, and retrain pressure for a game.",
    )
    group.add_argument(
        "--apply-feedback",
        metavar="GAME",
        dest="apply_feedback",
        help="Apply bounded clip-judge weight updates from recorded review feedback.",
    )
    group.add_argument(
        "--perf-feedback",
        metavar="GAME",
        dest="perf_feedback",
        help="Report social performance → weight recommendations for GAME (dry run, no changes written).",
    )
    group.add_argument(
        "--apply-perf-feedback",
        metavar="GAME",
        dest="apply_perf_feedback",
        help="Apply social performance weight updates to GAME's weights.yaml.",
    )
    group.add_argument(
        "--enrich-quarantine",
        metavar="GAME",
        dest="enrich_quarantine",
        help="Retry quarantined clips for GAME after adding new ROIs, templates, or labels.",
    )
    group.add_argument(
        "--enrich-game-from-wiki",
        metavar="GAME",
        dest="enrich_game_from_wiki",
        help="Create a draft game-pack enrichment from an explicit supported wiki URL.",
    )
    parser.add_argument(
        "--wiki-url",
        dest="wiki_url",
        help="Wiki URL to use with --enrich-game-from-wiki.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --distribute or --run-distribution-queue: show what would be uploaded without posting.",
    )
    parser.add_argument(
        "--url",
        dest="url",
        help="Published post URL for --mark-manual-posted.",
    )
    parser.add_argument(
        "--skip-eval-detectors",
        action="store_true",
        help="With --evaluate-game-pack: use existing sidecar detector metadata instead of rerunning detectors.",
    )
    parser.add_argument(
        "--weapon-frame-sample",
        choices=["middle", "kill_timestamps", "all"],
        help="With --refresh-weapon-detector: temporarily override the weapon detector frame sampling mode.",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml).",
    )
    parser.add_argument(
        "--report",
        help="With --promote-weapon-audit-crop: explicit audit report JSON path. Defaults to the latest report for the game.",
    )
    parser.add_argument(
        "--rank",
        type=int,
        default=1,
        help="With --promote-weapon-audit-crop: 1-based ranked candidate to promote (default: 1).",
    )
    parser.add_argument(
        "--crop-source",
        choices=["auto", "candidate", "roi"],
        default="auto",
        help="With --promote-weapon-audit-crop: which exported crop type to promote.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacement of an existing asset and create a timestamped backup first.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="With --render-weapon-audit-review: number of ranked candidates to render (default: 10).",
    )
    args = parser.parse_args()

    config = load_config(args.config)

    if args.montage:
        ensure_dirs(config)
        games_to_process = list_supported_games(config) if args.montage == "all" else [args.montage]
        for game in games_to_process:
            if game not in list_supported_games(config):
                logger.error(f"Unknown game '{game}'. Valid options: {list_supported_games(config)}")
                sys.exit(1)
            run_montage(game, config)
    elif args.init_game:
        result = scaffold_game_pack(args.init_game, config, force=False)
        logger.info(f"Scaffolded game pack at {result['pack_dir']}")
        print(print_validation_report(validate_game_pack(args.init_game, config)))
    elif args.validate_game_pack:
        result = validate_game_pack(args.validate_game_pack, config)
        print(print_validation_report(result))
        if not result["valid"]:
            sys.exit(1)
    elif args.evaluate_game_pack:
        scaffold_gold_set(args.evaluate_game_pack, config)
        result = evaluate_game_pack(
            args.evaluate_game_pack,
            config,
            run_detectors=not args.skip_eval_detectors,
            force=True,
        )
        print(json.dumps(result, indent=2))
        if result.get("status") == "failed":
            sys.exit(1)
    elif args.build_yolo_dataset:
        result = build_yolo_dataset(args.build_yolo_dataset, config)
        print(json.dumps(result, indent=2))
        if not result.get("ok"):
            sys.exit(1)
    elif args.train_yolo:
        result = train_yolo_model(args.train_yolo, config, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
        if not result.get("ok"):
            sys.exit(1)
    elif args.refresh_weapon_detector:
        ensure_dirs(config)
        result = refresh_weapon_detector(args.refresh_weapon_detector, config, frame_sample=args.weapon_frame_sample)
        print(json.dumps(result, indent=2))
    elif args.audit_weapon_detector:
        ensure_dirs(config)
        result = audit_weapon_detector(args.audit_weapon_detector, config)
        print(json.dumps(result, indent=2))
    elif args.promote_weapon_audit_crop:
        ensure_dirs(config)
        result = promote_weapon_audit_crop(
            args.promote_weapon_audit_crop,
            config,
            report_path=args.report,
            rank=args.rank,
            source=args.crop_source,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, indent=2))
        if not result.get("ok"):
            sys.exit(1)
    elif args.render_weapon_audit_review:
        ensure_dirs(config)
        result = render_weapon_audit_review(
            args.render_weapon_audit_review,
            config,
            report_path=args.report,
            top_k=args.top_k,
        )
        print(json.dumps(result, indent=2))
        if not result.get("ok"):
            sys.exit(1)
    elif args.review_feedback:
        result = summarize_feedback(args.review_feedback, config)
        print(json.dumps(result, indent=2))
    elif args.apply_feedback:
        result = apply_feedback_updates(args.apply_feedback, config, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
    elif args.perf_feedback:
        result = apply_performance_updates(args.perf_feedback, config, dry_run=True)
        print(json.dumps(result, indent=2))
    elif args.apply_perf_feedback:
        result = apply_performance_updates(args.apply_perf_feedback, config, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
        if not result.get("ok"):
            sys.exit(1)
    elif args.enrich_quarantine:
        ensure_dirs(config)
        enrich_quarantine(args.enrich_quarantine, config)
    elif args.enrich_game_from_wiki:
        if not args.wiki_url:
            parser.error("--enrich-game-from-wiki requires --wiki-url")
        result = enrich_game_from_wiki(args.enrich_game_from_wiki, args.wiki_url, config)
        print(json.dumps(result, indent=2))
        if result.get("status") == "failed":
            sys.exit(1)
    elif args.schedule_distribution:
        ensure_dirs(config)
        result = schedule_distribution_tasks(config)
        print(json.dumps(result, indent=2))
    elif args.run_distribution_queue:
        ensure_dirs(config)
        result = run_distribution_queue(config, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
    elif args.distribution_status:
        result = distribution_status(config)
        print(json.dumps(result, indent=2))
    elif args.mark_manual_posted:
        if not args.url:
            parser.error("--mark-manual-posted requires --url")
        result = mark_manual_posted(args.mark_manual_posted, args.url, config)
        print(json.dumps(result, indent=2))
        if not result.get("ok"):
            sys.exit(1)
    elif args.distribute:
        ensure_dirs(config)
        run_distribution_for_all(config, dry_run=args.dry_run)
    elif args.poll_tiktok:
        poll_tiktok_pending(config)
    elif args.list_reddit_flairs:
        list_reddit_flairs(config)
    elif args.watch:
        ensure_dirs(config)
        interval = config.get("pipeline", {}).get("watch_interval_seconds", 300)
        logger.info(f"Watch mode active — running all games every {interval}s. Ctrl+C to stop.")
        while True:
            for game in list_supported_games(config):
                run_pipeline_for_game(game, config)
            logger.info(f"Watch mode: next run in {interval}s...")
            time.sleep(interval)
    elif args.game == "all":
        ensure_dirs(config)
        for game in list_supported_games(config):
            run_pipeline_for_game(game, config)
    else:
        ensure_dirs(config)
        run_pipeline_for_game(args.game, config)


if __name__ == "__main__":
    main()

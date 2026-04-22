#!/usr/bin/env python3
"""
tools/snip_roi.py — Interactive ROI snipping and template registration

Opens a frame from a gameplay clip, lets the operator draw an ROI, saves the
crop as a template image, and appends metadata into assets/games/<game>/hud.yaml.

Usage:
    python tools/snip_roi.py \
        --clip inbox/marvel_rivals/clip.mp4 \
        --game marvel_rivals \
        --template-id kill_medal_headshot \
        --detector roi_matcher \
        --semantic-type medal
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.game_pack import get_game_pack_dir, load_game_pack

TARGET_WIDTH = 1920
TARGET_HEIGHT = 1080


def main() -> None:
    parser = argparse.ArgumentParser(description="Snip an ROI from a clip and register it in the game pack.")
    parser.add_argument("--clip", required=True, help="Source gameplay clip (.mp4)")
    parser.add_argument("--game", required=True, help="Game key / game-pack folder")
    parser.add_argument("--template-id", required=True, help="Unique ID for this template crop")
    parser.add_argument("--detector", default="roi_matcher", help="Detector or subsystem using this template")
    parser.add_argument("--semantic-type", default="hud_icon", help="What this crop represents")
    parser.add_argument("--label", default=None, help="Optional human-readable label")
    parser.add_argument("--time", type=float, default=None, help="Timestamp to sample; defaults to mid-clip")
    parser.add_argument("--roi-name", default=None, help="Name of ROI box to save into hud.yaml")
    parser.add_argument("--match-threshold", type=float, default=0.84, help="Template-match threshold")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    try:
        import cv2
    except ImportError:
        print("ERROR: OpenCV not installed. Run: pip install opencv-python-headless", file=sys.stderr)
        sys.exit(1)

    config_path = ROOT / args.config
    if not config_path.exists():
        print(f"ERROR: config not found at {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f)

    game_pack = load_game_pack(args.game, config, create_missing=True)
    pack_dir = get_game_pack_dir(args.game, config)
    hud_path = pack_dir / "hud.yaml"
    hud = game_pack.get("hud") or {}
    hud.setdefault("rois", {})
    hud.setdefault("roi_templates", [])

    clip_path = Path(args.clip)
    if not clip_path.exists():
        print(f"ERROR: Clip not found: {clip_path}", file=sys.stderr)
        sys.exit(1)

    cap = cv2.VideoCapture(str(clip_path))
    if not cap.isOpened():
        print(f"ERROR: Could not open clip: {clip_path}", file=sys.stderr)
        sys.exit(1)

    try:
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 1
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        duration = total_frames / fps
        sample_time = args.time if args.time is not None else duration / 2
        sample_time = max(0.0, min(sample_time, duration))

        cap.set(cv2.CAP_PROP_POS_MSEC, sample_time * 1000)
        ok, frame = cap.read()
        if not ok:
            print("ERROR: Could not read frame from clip.", file=sys.stderr)
            sys.exit(1)

        h, w = frame.shape[:2]
        if (w, h) != (TARGET_WIDTH, TARGET_HEIGHT):
            frame = cv2.resize(frame, (TARGET_WIDTH, TARGET_HEIGHT), interpolation=cv2.INTER_LINEAR)

        x, y, rw, rh = cv2.selectROI("Snip ROI", frame, fromCenter=False, showCrosshair=True)
        cv2.destroyAllWindows()
        if rw <= 0 or rh <= 0:
            print("ERROR: No ROI selected.", file=sys.stderr)
            sys.exit(1)

        crop = frame[int(y):int(y + rh), int(x):int(x + rw)]
        if crop.size == 0:
            print("ERROR: Selected ROI is empty.", file=sys.stderr)
            sys.exit(1)

        template_dir = pack_dir / "roi_templates"
        template_dir.mkdir(parents=True, exist_ok=True)
        template_path = template_dir / f"{args.template_id}.png"
        cv2.imwrite(str(template_path), crop)

        roi_name = args.roi_name or args.template_id
        hud["rois"][roi_name] = {"x": int(x), "y": int(y), "w": int(rw), "h": int(rh)}
        hud["roi_templates"] = [
            entry for entry in hud.get("roi_templates", [])
            if entry.get("id") != args.template_id
        ]
        hud["roi_templates"].append({
            "id": args.template_id,
            "label": args.label or args.template_id.replace("_", " ").title(),
            "detector": args.detector,
            "semantic_type": args.semantic_type,
            "roi_ref": roi_name,
            "asset_path": str(template_path.relative_to(ROOT)),
            "match_threshold": float(args.match_threshold),
            "source_clip_id": clip_path.stem,
            "frame_time_seconds": round(sample_time, 3),
        })

        hud_path.write_text(yaml.safe_dump(hud, sort_keys=False, allow_unicode=False))

        rel_template = template_path.relative_to(ROOT) if template_path.is_relative_to(ROOT) else template_path
        print(f"Saved template: {rel_template}")
        print(f"Updated ROI '{roi_name}' in: {hud_path.relative_to(ROOT)}")
    finally:
        cap.release()


if __name__ == "__main__":
    main()

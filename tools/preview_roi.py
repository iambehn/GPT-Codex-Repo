"""
tools/preview_roi.py — Preview all configured ROIs on a frame from a gameplay clip

Saves a single annotated PNG showing every ROI rectangle (kill_feed, weapon_detector)
that is configured for the given game. Use this to verify coordinates are correct
before running the full pipeline, or after a HUD patch changes icon positions.

Usage:
    python tools/preview_roi.py --clip inbox/deadlock/myclip.mp4 --game deadlock

    # Sample a specific timestamp instead of mid-clip:
    python tools/preview_roi.py --clip inbox/deadlock/myclip.mp4 --game deadlock --time 8.0

Output:
    assets/roi_preview_{game}.png   — open this file to inspect ROI placement
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TARGET_WIDTH = 1920
TARGET_HEIGHT = 1080

# (label, BGR colour)
_ROI_STYLES = [
    ("kill_feed",       (0,  200,  0)),   # green
    ("weapon_detector", (0,  180, 255)),  # orange
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Save an annotated preview frame showing all configured ROIs for a game."
    )
    parser.add_argument("--clip", required=True, help="Path to a gameplay clip (.mp4)")
    parser.add_argument("--game", required=True, help="Game key matching config.yaml")
    parser.add_argument("--time", type=float, default=None,
                        help="Timestamp in seconds to sample (default: mid-clip)")
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

    clip_path = Path(args.clip)
    if not clip_path.exists():
        print(f"ERROR: clip not found: {clip_path}", file=sys.stderr)
        sys.exit(1)

    cap = cv2.VideoCapture(str(clip_path))
    if not cap.isOpened():
        print(f"ERROR: could not open clip: {clip_path}", file=sys.stderr)
        sys.exit(1)

    try:
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 1
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        duration = total_frames / fps

        sample_time = args.time if args.time is not None else duration / 2
        cap.set(cv2.CAP_PROP_POS_MSEC, sample_time * 1000)
        ok, frame = cap.read()
        if not ok:
            print("ERROR: could not read frame.", file=sys.stderr)
            sys.exit(1)

        h, w = frame.shape[:2]
        if w != TARGET_WIDTH or h != TARGET_HEIGHT:
            frame = cv2.resize(frame, (TARGET_WIDTH, TARGET_HEIGHT), interpolation=cv2.INTER_LINEAR)

        rois_drawn = 0

        # Kill-feed ROI
        kf_cfg = config.get("kill_feed", {}).get("games", {}).get(args.game)
        if kf_cfg:
            roi = kf_cfg.get("roi", {})
            rx, ry, rw, rh = roi.get("x", 0), roi.get("y", 0), roi.get("w", 100), roi.get("h", 100)
            color = _ROI_STYLES[0][1]
            cv2.rectangle(frame, (rx, ry), (rx + rw, ry + rh), color, 2)
            cv2.putText(frame, "kill_feed ROI", (rx, max(ry - 6, 0)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)
            rois_drawn += 1

        # Weapon-detector ROI
        wd_game = config.get("weapon_detector", {}).get("games", {}).get(args.game)
        if wd_game:
            roi = wd_game.get("roi", {})
            rx, ry, rw, rh = roi.get("x", 0), roi.get("y", 0), roi.get("w", 100), roi.get("h", 80)
            color = _ROI_STYLES[1][1]
            cv2.rectangle(frame, (rx, ry), (rx + rw, ry + rh), color, 2)
            cv2.putText(frame, "weapon ROI", (rx, max(ry - 6, 0)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)
            rois_drawn += 1

        out_path = ROOT / "assets" / f"roi_preview_{args.game}.png"
        cv2.imwrite(str(out_path), frame)
        print(f"Saved preview: {out_path.relative_to(ROOT)}  ({rois_drawn} ROI(s) drawn)")
        print(f"  Sampled at t={sample_time:.1f}s of {duration:.1f}s total")
        if rois_drawn == 0:
            print("  WARNING: No ROIs found for this game in config.yaml")
    finally:
        cap.release()


if __name__ == "__main__":
    main()

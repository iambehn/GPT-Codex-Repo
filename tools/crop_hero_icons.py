#!/usr/bin/env python3
"""
Bulk-crop hero/weapon portrait icons from an in-game selection screen screenshot.

Detects portrait tiles by finding the white-text name labels at the bottom of
each thumbnail, OCRs each label to get the hero/weapon name, maps it to a
hero_id via the game's roster YAML, and saves the portrait crop as a PNG.

Usage:
    # Dry-run first to verify detections without saving anything:
    python tools/crop_hero_icons.py \
        --screenshot path/to/hero_select.png \
        --game marvel_rivals \
        --dry-run

    # Save icons (default output: assets/weapon_icons/{game}/):
    python tools/crop_hero_icons.py \
        --screenshot path/to/hero_select.png \
        --game marvel_rivals

    # Custom output dir or icon size (default 128×128):
    python tools/crop_hero_icons.py \
        --screenshot path/to/screenshot.png \
        --game arc_raiders \
        --output assets/weapon_icons \
        --size 64

If you have multiple screenshots (e.g. one per role section), run the command
once per screenshot — icons accumulate in the same output folder.

Requires: opencv-python, easyocr, pyyaml  (all in requirements.txt)
First run downloads the EasyOCR English model (~100 MB, cached after that).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import cv2
import numpy as np
import yaml

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from pipeline.game_pack import get_primary_entities, load_game_pack


# ---------------------------------------------------------------------------
# Roster loading
# ---------------------------------------------------------------------------

def load_roster(game: str) -> dict[str, str]:
    """Return {normalized_display_name: hero_id} from the game's roster YAML."""
    config_path = _ROOT / "config.yaml"
    if config_path.exists():
        data = yaml.safe_load(config_path.read_text()) or {}
        pack = load_game_pack(game, data)
        _, entries = get_primary_entities(pack)
        if entries:
            return {
                _norm(v["display_name"]): k
                for k, v in entries.items()
                if isinstance(v, dict) and "display_name" in v
            }

    roster_path = _ROOT / "assets" / "rosters" / f"{game}.yaml"
    if not roster_path.exists():
        sys.exit(f"Roster not found: {roster_path}")
    data = yaml.safe_load(roster_path.read_text()) or {}
    entries = data.get("heroes") or data.get("weapons") or {}
    return {
        _norm(v["display_name"]): k
        for k, v in entries.items()
        if isinstance(v, dict) and "display_name" in v
    }


def _norm(s: str) -> str:
    """Normalize a string for fuzzy matching — lowercase alphanum only."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


_SECTION_HEADERS = {
    _norm(s) for s in [
        "vanguard", "duelist", "strategist", "multi-role", "multi role",
        "multirole", "team up", "abilities", "normal attack", "passive",
    ]
}


# ---------------------------------------------------------------------------
# Label detection
# ---------------------------------------------------------------------------

def find_label_rects(img: np.ndarray) -> list[tuple[int, int, int, int]]:
    """Return (x, y, w, h) bounding boxes for name label bars.

    Labels are white uppercase text on a near-black background at the bottom
    of each portrait tile. We threshold for bright pixels then dilate
    horizontally to connect characters within the same label.
    """
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, bright = cv2.threshold(gray, 190, 255, cv2.THRESH_BINARY)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (28, 4))
    dilated = cv2.dilate(bright, kernel)
    contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    rects = []
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        # Labels: short names like "HELA" are ~30px wide; keep aspect ratio loose
        if w > 25 and 6 <= h <= 28 and w / h > 1.5:
            rects.append((x, y, w, h))

    return sorted(rects, key=lambda r: (r[1], r[0]))


def cluster_rows(rects: list[tuple], gap: int = 25) -> list[list[tuple]]:
    """Group label rects into horizontal rows by y-proximity."""
    if not rects:
        return []
    rows: list[list[tuple]] = []
    current: list[tuple] = [rects[0]]
    for rect in rects[1:]:
        if abs(rect[1] - current[-1][1]) <= gap:
            current.append(rect)
        else:
            rows.append(sorted(current, key=lambda r: r[0]))
            current = [rect]
    rows.append(sorted(current, key=lambda r: r[0]))
    return rows


# ---------------------------------------------------------------------------
# OCR + matching
# ---------------------------------------------------------------------------

def ocr_label(gray: np.ndarray, x: int, y: int, w: int, h: int, reader) -> str:
    """OCR a label region; return cleaned uppercase text."""
    pad = 5
    crop = gray[max(0, y - pad): y + h + pad, max(0, x - pad): x + w + pad]
    results = reader.readtext(crop, detail=0, paragraph=True)
    return " ".join(results).strip().upper()


def match_hero(raw_text: str, roster: dict[str, str]) -> str | None:
    """Map OCR'd label text to a hero_id. Returns None on no match."""
    import difflib
    n = _norm(raw_text)
    if not n or n in _SECTION_HEADERS:
        return None
    # Exact match
    if n in roster:
        return roster[n]
    # Substring fallback
    for roster_norm, hero_id in roster.items():
        if n in roster_norm or roster_norm in n:
            return hero_id
    # Fuzzy fallback — catches OCR errors like "DEAOPOOL" → "deadpool"
    matches = difflib.get_close_matches(n, roster.keys(), n=1, cutoff=0.75)
    if matches:
        return roster[matches[0]]
    return None


# ---------------------------------------------------------------------------
# Portrait cropping
# ---------------------------------------------------------------------------

def crop_portrait(
    img: np.ndarray,
    label_x: int, label_y: int, label_w: int,
    portrait_height: int, size: int,
) -> np.ndarray:
    """Crop the portrait art above the label bar and resize to a square."""
    x1 = max(0, label_x - 6)
    x2 = min(img.shape[1], label_x + label_w + 6)
    y2 = label_y
    y1 = max(0, label_y - portrait_height)
    portrait = img[y1:y2, x1:x2]
    if portrait.size == 0:
        portrait = img[max(0, y2 - size): y2, x1:x2]
    return cv2.resize(portrait, (size, size), interpolation=cv2.INTER_LANCZOS4)


def estimate_portrait_height(rows: list[list[tuple]]) -> int:
    """Estimate portrait height from the vertical gap between label rows."""
    row_ys = [min(r[1] for r in row) for row in rows]
    if len(row_ys) > 1:
        gaps = [row_ys[i + 1] - row_ys[i] for i in range(len(row_ys) - 1)]
        median_gap = sorted(gaps)[len(gaps) // 2]
        return int(median_gap * 0.80)
    # Single row — estimate from label height
    avg_label_h = sum(r[3] for r in rows[0]) / len(rows[0])
    return int(avg_label_h * 3.5)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk-crop hero/weapon icons from a hero-select screenshot"
    )
    parser.add_argument("--screenshot", required=True, help="Path to screenshot PNG/JPG")
    parser.add_argument(
        "--game", required=True,
        choices=["marvel_rivals", "deadlock", "arc_raiders"],
    )
    parser.add_argument(
        "--output", default=str(_ROOT / "assets" / "weapon_icons"),
        help="Root output dir (icons saved to <output>/<game>/)",
    )
    parser.add_argument("--size", type=int, default=128, help="Output icon size in px (square)")
    parser.add_argument("--dry-run", action="store_true", help="Detect and print without saving")
    args = parser.parse_args()

    img = cv2.imread(args.screenshot)
    if img is None:
        sys.exit(f"Cannot read image: {args.screenshot}")
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    roster = load_roster(args.game)
    print(f"Roster loaded: {len(roster)} entries for {args.game}")

    print("Running OCR (first run downloads ~100 MB EasyOCR model)...")
    import easyocr
    reader = easyocr.Reader(["en"], gpu=False, verbose=False)

    label_rects = find_label_rects(img)
    if not label_rects:
        sys.exit("No portrait labels detected. Verify --screenshot shows in-game hero names.")

    rows = cluster_rows(label_rects)
    portrait_height = estimate_portrait_height(rows)
    print(f"Detected {len(label_rects)} labels across {len(rows)} row(s). "
          f"Estimated portrait height: {portrait_height}px\n")

    out_dir = Path(args.output) / args.game
    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    saved, unmatched = 0, []

    for row in rows:
        for (x, y, w, h) in row:
            raw = ocr_label(gray, x, y, w, h, reader)
            hero_id = match_hero(raw, roster)

            if not hero_id:
                unmatched.append(raw)
                print(f"  [no match] '{raw}'")
                continue

            portrait = crop_portrait(img, x, y, w, portrait_height, args.size)
            out_path = out_dir / f"{hero_id}.png"

            if args.dry_run:
                print(f"  [dry-run]  '{raw}' → {hero_id}.png")
            else:
                cv2.imwrite(str(out_path), portrait)
                print(f"  Saved  {hero_id}.png  ← '{raw}'")
                saved += 1

    print(f"\n{'[dry-run] ' if args.dry_run else ''}Done. "
          f"{saved} icon(s) {'would be ' if args.dry_run else ''}saved to {out_dir}")

    if unmatched:
        print(f"\nUnmatched labels ({len(unmatched)}): {unmatched}")
        print("If OCR misread a name, re-run with --dry-run and manually copy/rename the crop.")


if __name__ == "__main__":
    main()

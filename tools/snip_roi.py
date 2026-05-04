from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from pipeline.roi_matcher import RoiMatcherError, load_published_runtime_pack
from tools.preview_roi import _ffmpeg_path, _resolve_path, _sampled_time_for_source


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "roi_snips"


def snip_roi(
    source: str | Path,
    game: str,
    roi_ref: str,
    asset_id: str,
    crop: str,
    *,
    time_seconds: float | None = None,
    output_dir: str | Path | None = None,
    write_debug_frame: bool = False,
) -> dict[str, Any]:
    source_path = _resolve_path(source)
    if not source_path.exists() or not source_path.is_file():
        return {
            "ok": False,
            "status": "missing_source",
            "game": game,
            "source": str(source_path),
            "error": "source media does not exist or is not a file",
        }

    try:
        runtime_pack = load_published_runtime_pack(game)
    except (FileNotFoundError, RoiMatcherError) as exc:
        return {
            "ok": False,
            "status": getattr(exc, "status", "invalid_game_pack"),
            "game": game,
            "source": str(source_path),
            "error": str(exc),
        }

    if roi_ref not in runtime_pack.rois:
        return {
            "ok": False,
            "status": "unknown_roi_ref",
            "game": game,
            "source": str(source_path),
            "error": f"ROI '{roi_ref}' is not defined in the published pack",
        }

    try:
        crop_bounds = _parse_crop(crop)
    except ValueError as exc:
        return {
            "ok": False,
            "status": "invalid_crop",
            "game": game,
            "source": str(source_path),
            "error": str(exc),
        }

    if crop_bounds["w"] <= 0 or crop_bounds["h"] <= 0:
        return {
            "ok": False,
            "status": "invalid_crop",
            "game": game,
            "source": str(source_path),
            "error": "crop width and height must be positive",
        }

    width = runtime_pack.width
    height = runtime_pack.height
    if (
        crop_bounds["x"] < 0
        or crop_bounds["y"] < 0
        or crop_bounds["x"] + crop_bounds["w"] > width
        or crop_bounds["y"] + crop_bounds["h"] > height
    ):
        return {
            "ok": False,
            "status": "invalid_crop",
            "game": game,
            "source": str(source_path),
            "error": f"crop exceeds normalized frame bounds {width}x{height}",
        }

    sampled_time = _sampled_time_for_source(source_path, time_seconds)
    output_root = _snip_output_dir(game, asset_id, output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    crop_path = output_root / f"{asset_id}.png"
    source_frame_path = output_root / f"{asset_id}.source_frame.png"
    provenance_path = output_root / f"{asset_id}.provenance.json"
    snippet_path = output_root / f"{asset_id}.manifest_snippet.json"
    debug_path = output_root / f"{asset_id}.debug_frame.png"

    if not _render_png_frame(
        source_path,
        source_frame_path,
        sampled_time=sampled_time,
        filter_chain=[f"scale={width}:{height}:flags=fast_bilinear"],
    ):
        return {
            "ok": False,
            "status": "snip_render_failed",
            "game": game,
            "source": str(source_path),
            "error": f"failed to write source frame to {source_frame_path}",
        }
    if not _render_png_frame(
        source_path,
        crop_path,
        sampled_time=sampled_time,
        filter_chain=[
            f"scale={width}:{height}:flags=fast_bilinear",
            f"crop={crop_bounds['w']}:{crop_bounds['h']}:{crop_bounds['x']}:{crop_bounds['y']}",
        ],
    ):
        return {
            "ok": False,
            "status": "snip_render_failed",
            "game": game,
            "source": str(source_path),
            "error": f"failed to write crop image to {crop_path}",
        }

    if write_debug_frame:
        roi_bounds = runtime_pack.rois[roi_ref]
        debug_filters = [
            f"scale={width}:{height}:flags=fast_bilinear",
            (
                "drawbox="
                f"x={roi_bounds.x}:y={roi_bounds.y}:w={roi_bounds.width}:h={roi_bounds.height}:"
                "color=0x22c55e:thickness=2"
            ),
            (
                "drawbox="
                f"x={crop_bounds['x']}:y={crop_bounds['y']}:w={crop_bounds['w']}:h={crop_bounds['h']}:"
                "color=0xf59e0b:thickness=2"
            ),
        ]
        if not _render_png_frame(
            source_path,
            debug_path,
            sampled_time=sampled_time,
            filter_chain=debug_filters,
        ):
            return {
                "ok": False,
                "status": "snip_render_failed",
                "game": game,
                "source": str(source_path),
                "error": f"failed to write debug frame to {debug_path}",
            }

    provenance = {
        "schema_version": "roi_snip_provenance_v1",
        "game": game,
        "asset_id": asset_id,
        "roi_ref": roi_ref,
        "source": str(source_path),
        "sampled_time_seconds": round(sampled_time, 5),
        "normalized_frame": {"width": width, "height": height},
        "crop": crop_bounds,
        "published_pack_root": str(runtime_pack.root),
        "artifact_paths": {
            "crop_png": str(crop_path),
            "source_frame_png": str(source_frame_path),
            "debug_frame_png": str(debug_path) if write_debug_frame else None,
        },
    }
    snippet = {
        "asset_id": asset_id,
        "game": game,
        "roi_ref": roi_ref,
        "template_path_candidate": str(crop_path),
        "provenance_path": str(provenance_path),
        "template_defaults": {
            "roi_ref": roi_ref,
            "match_method": "TM_CCOEFF_NORMED",
            "threshold": 0.93,
            "scale_set": [1.0],
            "temporal_window": 3,
        },
    }
    provenance_path.write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    snippet_path.write_text(json.dumps(snippet, indent=2), encoding="utf-8")

    result = {
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source_path),
        "roi_ref": roi_ref,
        "asset_id": asset_id,
        "sampled_time_seconds": round(sampled_time, 5),
        "crop": crop_bounds,
        "output_dir": str(output_root),
        "crop_png_path": str(crop_path),
        "source_frame_png_path": str(source_frame_path),
        "provenance_path": str(provenance_path),
        "manifest_snippet_path": str(snippet_path),
        "debug_frame_png_path": str(debug_path) if write_debug_frame else None,
    }
    return result


def _parse_crop(value: str) -> dict[str, int]:
    parts = [part.strip() for part in str(value).split(",")]
    if len(parts) != 4:
        raise ValueError("crop must be formatted as x,y,w,h")
    try:
        x, y, w, h = (int(part) for part in parts)
    except ValueError as exc:
        raise ValueError("crop values must be integers") from exc
    return {"x": x, "y": y, "w": w, "h": h}


def _snip_output_dir(game: str, asset_id: str, output_dir: str | Path | None) -> Path:
    if output_dir is not None:
        return _resolve_path(output_dir)
    return DEFAULT_OUTPUT_ROOT / game / asset_id


def _render_png_frame(
    source: Path,
    output_path: Path,
    *,
    sampled_time: float,
    filter_chain: list[str],
) -> bool:
    command = [
        str(_ffmpeg_path()),
        "-v",
        "error",
        "-y",
        "-ss",
        f"{sampled_time:.3f}",
        "-i",
        str(source),
        "-frames:v",
        "1",
        "-vf",
        ",".join(filter_chain),
        str(output_path),
    ]
    result = subprocess.run(command, capture_output=True, check=False)
    return result.returncode == 0 and output_path.exists()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract a ROI crop artifact from a normalized gameplay frame.")
    parser.add_argument("--source", required=True, help="Path to a local media file.")
    parser.add_argument("--game", required=True, help="Published game pack id.")
    parser.add_argument("--roi-ref", required=True, help="ROI reference from hud.yaml.")
    parser.add_argument("--asset-id", required=True, help="Candidate asset id used for artifact naming.")
    parser.add_argument("--crop", required=True, help="Crop rectangle in normalized coordinates: x,y,w,h")
    parser.add_argument("--time", type=float, default=None, help="Optional timestamp in seconds. Defaults to mid-clip.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory for generated artifacts.")
    parser.add_argument("--debug-frame", action="store_true", help="Also write an annotated debug frame showing the ROI and crop.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        result = snip_roi(
            args.source,
            args.game,
            args.roi_ref,
            args.asset_id,
            args.crop,
            time_seconds=args.time,
            output_dir=args.output_dir,
            write_debug_frame=args.debug_frame,
        )
    except Exception as exc:
        result = {
            "ok": False,
            "status": "snip_failed",
            "game": args.game,
            "source": str(args.source),
            "error": str(exc),
        }
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())

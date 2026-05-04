from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

from pipeline.roi_matcher import RoiMatcherError, load_published_runtime_pack


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "roi_previews"


def preview_roi(
    source: str | Path,
    game: str,
    *,
    time_seconds: float | None = None,
    output_path: str | Path | None = None,
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

    sampled_time = _sampled_time_for_source(source_path, time_seconds)
    resolved_output = _preview_output_path(game, source_path, output_path)
    resolved_output.parent.mkdir(parents=True, exist_ok=True)
    try:
        _render_preview_png(
            source=source_path,
            output_path=resolved_output,
            width=runtime_pack.width,
            height=runtime_pack.height,
            sampled_time=sampled_time,
            rois=runtime_pack.rois,
        )
    except RuntimeError as exc:
        return {
            "ok": False,
            "status": "preview_render_failed",
            "game": game,
            "source": str(source_path),
            "error": str(exc),
        }

    return {
        "ok": True,
        "status": "ok",
        "game": game,
        "source": str(source_path),
        "output_path": str(resolved_output),
        "sampled_time_seconds": round(sampled_time, 5),
        "frame_width": runtime_pack.width,
        "frame_height": runtime_pack.height,
        "roi_count": len(runtime_pack.rois),
        "rois": [
            {
                "roi_ref": name,
                "x": bounds.x,
                "y": bounds.y,
                "width": bounds.width,
                "height": bounds.height,
            }
            for name, bounds in sorted(runtime_pack.rois.items())
        ],
    }


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _preview_output_path(game: str, source_path: Path, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    return DEFAULT_OUTPUT_ROOT / game / f"{source_path.stem}.roi_preview.png"


def _sampled_time_for_source(source: Path, time_seconds: float | None) -> float:
    if time_seconds is not None:
        return max(0.0, float(time_seconds))
    duration = _probe_duration_seconds(source)
    if duration is None or duration <= 0:
        return 0.0
    return max(0.0, duration / 2.0)


def _probe_duration_seconds(source: Path) -> float | None:
    ffprobe = shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe"
    ffprobe_path = Path(ffprobe)
    if not ffprobe_path.exists():
        return None
    command = [
        str(ffprobe_path),
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(source),
    ]
    result = subprocess.run(command, capture_output=True, check=False)
    if result.returncode != 0:
        return None
    raw = result.stdout.decode("utf-8", errors="ignore").strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _render_preview_png(
    *,
    source: Path,
    output_path: Path,
    width: int,
    height: int,
    sampled_time: float,
    rois: dict[str, object],
) -> None:
    filter_chain = [f"scale={width}:{height}:flags=fast_bilinear"]
    for roi_name, bounds in sorted(rois.items()):
        color = _color_for_name(roi_name)
        hex_color = _color_to_hex(color)
        filter_chain.append(
            "drawbox="
            f"x={bounds.x}:y={bounds.y}:w={bounds.width}:h={bounds.height}:"
            f"color={hex_color}:thickness=2"
        )
    if not _run_ffmpeg_frame_render(source, output_path, sampled_time=sampled_time, filter_chain=filter_chain):
        label_filters = [f"scale={width}:{height}:flags=fast_bilinear"]
        for roi_name, bounds in sorted(rois.items()):
            color = _color_for_name(roi_name)
            hex_color = _color_to_hex(color)
            label_filters.append(
                "drawbox="
                f"x={bounds.x}:y={bounds.y}:w={bounds.width}:h={bounds.height}:"
                f"color={hex_color}:thickness=2"
            )
            label_filters.append(
                "drawtext="
                f"text='{_escape_drawtext(roi_name)}':"
                f"x={bounds.x}:y={max(0, bounds.y - 18)}:"
                f"fontcolor={hex_color}:fontsize=14:box=1:boxcolor=black@0.55"
            )
        if not _run_ffmpeg_frame_render(source, output_path, sampled_time=sampled_time, filter_chain=label_filters):
            raise RuntimeError(f"failed to render ROI preview image to {output_path}")


def _run_ffmpeg_frame_render(
    source: Path,
    output_path: Path,
    *,
    sampled_time: float,
    filter_chain: list[str],
) -> bool:
    ffmpeg_path = _ffmpeg_path()
    command = [
        str(ffmpeg_path),
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


def _ffmpeg_path() -> Path:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    ffmpeg_path = Path(ffmpeg)
    if not ffmpeg_path.exists():
        raise RuntimeError(f"ffmpeg not found: {ffmpeg_path}")
    return ffmpeg_path


def _color_for_name(name: str) -> tuple[int, int, int]:
    digest = sum(ord(char) for char in name)
    palette = [
        (56, 189, 248),
        (34, 197, 94),
        (250, 204, 21),
        (248, 113, 113),
        (192, 132, 252),
        (251, 146, 60),
    ]
    rgb = palette[digest % len(palette)]
    return rgb


def _color_to_hex(rgb: tuple[int, int, int]) -> str:
    return f"0x{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"


def _escape_drawtext(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
        .replace(",", "\\,")
        .replace("[", "\\[")
        .replace("]", "\\]")
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render an annotated frame preview showing all configured ROIs for a game.")
    parser.add_argument("--source", required=True, help="Path to a local media file.")
    parser.add_argument("--game", required=True, help="Published game pack id.")
    parser.add_argument("--time", type=float, default=None, help="Optional timestamp in seconds. Defaults to mid-clip.")
    parser.add_argument("--output", default=None, help="Optional output image path.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        result = preview_roi(args.source, args.game, time_seconds=args.time, output_path=args.output)
    except Exception as exc:
        result = {
            "ok": False,
            "status": "preview_failed",
            "game": args.game,
            "source": str(args.source),
            "error": str(exc),
        }
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())

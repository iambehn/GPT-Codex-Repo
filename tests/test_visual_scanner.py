from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from pipeline.visual_scanner import scan_visual_source


def _ffmpeg_path() -> str:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    if not Path(ffmpeg).exists():
        raise RuntimeError("ffmpeg not found for visual scanner tests")
    return ffmpeg


def _write_pgm_frame(path: Path, width: int, height: int, pixels: bytes) -> None:
    header = f"P5\n{width} {height}\n255\n".encode("ascii")
    path.write_bytes(header + pixels)


def _write_test_video(path: Path, frame_pixels: list[bytes], width: int = 64, height: int = 36, fps: int = 4) -> None:
    with tempfile.TemporaryDirectory() as frames_dir:
        frame_dir = Path(frames_dir)
        for index, pixels in enumerate(frame_pixels):
            _write_pgm_frame(frame_dir / f"frame{index:03d}.pgm", width, height, pixels)

        command = [
            _ffmpeg_path(),
            "-v",
            "error",
            "-y",
            "-framerate",
            str(fps),
            "-i",
            str(frame_dir / "frame%03d.pgm"),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ]
        subprocess.run(command, check=True, capture_output=True)


def _solid_frame(value: int, width: int = 64, height: int = 36) -> bytes:
    return bytes([value]) * (width * height)


def _moving_box_frame(position: int, width: int = 64, height: int = 36) -> bytes:
    pixels = bytearray([90]) * (width * height)
    box_width = 10
    box_height = 10
    start_x = min(width - box_width, position)
    start_y = 12
    for y in range(start_y, start_y + box_height):
        row_start = y * width
        for x in range(start_x, start_x + box_width):
            pixels[row_start + x] = 240
    return bytes(pixels)


class VisualScannerTests(unittest.TestCase):
    def test_scan_visual_source_emits_motion_spike_for_moving_box(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        frames = (
            [_solid_frame(90) for _ in range(8)]
            + [_moving_box_frame(position) for position in (0, 10, 20, 30)]
            + [_solid_frame(90) for _ in range(4)]
        )
        _write_test_video(path, frames)

        signals = scan_visual_source(
            path,
            {
                "sample_fps": 4.0,
                "default_confidence": 0.70,
                "rolling_baseline_frames": 6,
                "motion_z_score_threshold": 2.0,
                "flash_z_score_threshold": 5.0,
                "suppress_initial_seconds": 0.0,
                "suppress_final_seconds": 0.0,
                "min_cluster_frames": 2,
            },
            media_duration_seconds=4.0,
        )

        self.assertIn("visual_motion_spike", [signal.source for signal in signals])

    def test_scan_visual_source_emits_flash_spike_for_brightness_jump(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        frames = (
            [_solid_frame(80) for _ in range(8)]
            + [_solid_frame(240)]
            + [_solid_frame(80) for _ in range(6)]
        )
        _write_test_video(path, frames)

        signals = scan_visual_source(
            path,
            {
                "sample_fps": 4.0,
                "default_confidence": 0.70,
                "rolling_baseline_frames": 6,
                "motion_z_score_threshold": 6.0,
                "flash_z_score_threshold": 2.0,
                "suppress_initial_seconds": 0.0,
                "suppress_final_seconds": 0.0,
                "min_cluster_frames": 2,
            },
            media_duration_seconds=4.0,
        )

        sources = [signal.source for signal in signals]
        self.assertIn("visual_flash_spike", sources)
        self.assertEqual(sources.count("visual_flash_spike"), 1)

    def test_scan_visual_source_stays_quiet_for_flat_video(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        _write_test_video(path, [_solid_frame(100) for _ in range(16)])

        signals = scan_visual_source(
            path,
            {
                "sample_fps": 4.0,
                "default_confidence": 0.70,
                "rolling_baseline_frames": 6,
                "motion_z_score_threshold": 2.0,
                "flash_z_score_threshold": 2.0,
                "suppress_initial_seconds": 0.0,
                "suppress_final_seconds": 0.0,
                "min_cluster_frames": 2,
            },
            media_duration_seconds=4.0,
        )

        self.assertEqual(signals, [])

    def test_scan_visual_source_suppresses_intro_and_end_spikes(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        frames = (
            [_solid_frame(240)]
            + [_solid_frame(90) for _ in range(12)]
            + [_solid_frame(240)]
        )
        _write_test_video(path, frames)

        signals = scan_visual_source(
            path,
            {
                "sample_fps": 4.0,
                "default_confidence": 0.70,
                "rolling_baseline_frames": 6,
                "motion_z_score_threshold": 2.0,
                "flash_z_score_threshold": 2.0,
                "suppress_initial_seconds": 1.0,
                "suppress_final_seconds": 1.0,
                "min_cluster_frames": 2,
            },
            media_duration_seconds=4.0,
        )

        self.assertEqual(signals, [])

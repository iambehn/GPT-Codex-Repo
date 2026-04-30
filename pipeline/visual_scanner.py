from __future__ import annotations

import math
import shutil
import subprocess
from pathlib import Path
from typing import Any

from pipeline.proxy_scanner import ProxySignal


FRAME_WIDTH = 64
FRAME_HEIGHT = 36
FRAME_BYTES = FRAME_WIDTH * FRAME_HEIGHT


class VisualScanError(RuntimeError):
    pass


def scan_visual_source(
    source: str | Path,
    config: dict[str, Any],
    media_duration_seconds: float | None = None,
) -> list[ProxySignal]:
    sample_fps = float(config.get("sample_fps", 4.0))
    default_confidence = float(config.get("default_confidence", 0.70))
    rolling_baseline_frames = int(config.get("rolling_baseline_frames", 12))
    motion_z_score_threshold = float(config.get("motion_z_score_threshold", 2.8))
    flash_z_score_threshold = float(config.get("flash_z_score_threshold", 3.2))
    suppress_initial_seconds = float(config.get("suppress_initial_seconds", 1.0))
    suppress_final_seconds = float(config.get("suppress_final_seconds", 1.0))
    min_cluster_frames = int(config.get("min_cluster_frames", 2))

    frames = _decode_frames(source, sample_fps)
    if len(frames) < 2:
        return []

    motion_values: list[float] = []
    flash_values: list[float] = []
    for previous_frame, current_frame in zip(frames, frames[1:]):
        motion_values.append(_frame_delta(previous_frame, current_frame))
        flash_values.append(abs(_frame_mean(current_frame) - _frame_mean(previous_frame)))

    motion_signals = _series_to_signals(
        values=motion_values,
        sample_fps=sample_fps,
        signal_source="visual_motion_spike",
        default_confidence=default_confidence,
        z_score_threshold=motion_z_score_threshold,
        rolling_baseline_frames=rolling_baseline_frames,
        media_duration_seconds=media_duration_seconds,
        suppress_initial_seconds=suppress_initial_seconds,
        suppress_final_seconds=suppress_final_seconds,
        min_cluster_frames=min_cluster_frames,
    )
    flash_signals = _series_to_signals(
        values=flash_values,
        sample_fps=sample_fps,
        signal_source="visual_flash_spike",
        default_confidence=default_confidence,
        z_score_threshold=flash_z_score_threshold,
        rolling_baseline_frames=rolling_baseline_frames,
        media_duration_seconds=media_duration_seconds,
        suppress_initial_seconds=suppress_initial_seconds,
        suppress_final_seconds=suppress_final_seconds,
        min_cluster_frames=min_cluster_frames,
    )

    signals = motion_signals + flash_signals
    signals.sort(key=lambda signal: (signal.timestamp, signal.source))
    return signals


def _decode_frames(source: str | Path, sample_fps: float) -> list[bytes]:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    if not Path(ffmpeg).exists():
        raise VisualScanError("ffmpeg not found")

    command = [
        ffmpeg,
        "-v",
        "error",
        "-i",
        str(source),
        "-vf",
        f"fps={sample_fps},scale={FRAME_WIDTH}:{FRAME_HEIGHT}:flags=fast_bilinear,format=gray",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "gray",
        "-",
    ]
    try:
        result = subprocess.run(command, capture_output=True, check=False, timeout=20)
    except subprocess.TimeoutExpired as exc:
        raise VisualScanError("visual decode timed out") from exc

    if result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="ignore").strip() or "visual decode failed"
        raise VisualScanError(message)
    if not result.stdout:
        return []

    data = result.stdout
    frame_count = len(data) // FRAME_BYTES
    frames: list[bytes] = []
    for index in range(frame_count):
        start = index * FRAME_BYTES
        end = start + FRAME_BYTES
        frame = data[start:end]
        if len(frame) == FRAME_BYTES:
            frames.append(frame)
    return frames


def _series_to_signals(
    *,
    values: list[float],
    sample_fps: float,
    signal_source: str,
    default_confidence: float,
    z_score_threshold: float,
    rolling_baseline_frames: int,
    media_duration_seconds: float | None,
    suppress_initial_seconds: float,
    suppress_final_seconds: float,
    min_cluster_frames: int,
) -> list[ProxySignal]:
    hot_frames: list[tuple[int, float, float]] = []
    for index, value in enumerate(values):
        history_start = max(0, index - rolling_baseline_frames)
        history = values[history_start:index]
        if not history:
            continue

        baseline = sum(history) / len(history)
        variance = sum((item - baseline) ** 2 for item in history) / len(history)
        std_dev = math.sqrt(variance)
        ratio = value / max(baseline, 1e-6)

        if std_dev > 1e-6:
            z_score = (value - baseline) / std_dev
        elif value > baseline and ratio >= 1.5:
            z_score = z_score_threshold + min(5.0, ratio - 1.0)
        else:
            z_score = 0.0

        timestamp = (index + 1.5) / sample_fps
        if _suppressed_by_boundary(
            timestamp,
            media_duration_seconds=media_duration_seconds,
            suppress_initial_seconds=suppress_initial_seconds,
            suppress_final_seconds=suppress_final_seconds,
        ):
            continue

        if z_score >= z_score_threshold and ratio >= 1.5:
            hot_frames.append((index, z_score, ratio))

    if not hot_frames:
        return []

    signals: list[ProxySignal] = []
    cluster: list[tuple[int, float, float]] = [hot_frames[0]]
    for item in hot_frames[1:]:
        if item[0] == cluster[-1][0] + 1:
            cluster.append(item)
            continue
        signal = _cluster_to_signal(
            cluster=cluster,
            sample_fps=sample_fps,
            signal_source=signal_source,
            default_confidence=default_confidence,
            z_score_threshold=z_score_threshold,
            min_cluster_frames=min_cluster_frames,
        )
        if signal is not None:
            signals.append(signal)
        cluster = [item]

    signal = _cluster_to_signal(
        cluster=cluster,
        sample_fps=sample_fps,
        signal_source=signal_source,
        default_confidence=default_confidence,
        z_score_threshold=z_score_threshold,
        min_cluster_frames=min_cluster_frames,
    )
    if signal is not None:
        signals.append(signal)
    return signals


def _cluster_to_signal(
    *,
    cluster: list[tuple[int, float, float]],
    sample_fps: float,
    signal_source: str,
    default_confidence: float,
    z_score_threshold: float,
    min_cluster_frames: int,
) -> ProxySignal | None:
    if len(cluster) < min_cluster_frames:
        return None

    peak_index, peak_z_score, peak_ratio = max(cluster, key=lambda item: (item[1], item[2]))
    timestamp = (peak_index + 1.5) / sample_fps
    strength = min(1.0, max(0.10, peak_z_score / max(z_score_threshold * 2.0, 0.1)))
    signal_label = signal_source.replace("_", " ")
    return ProxySignal(
        source=signal_source,
        source_family="visual_prepass",
        timestamp=timestamp,
        strength=strength,
        confidence=default_confidence,
        reason=f"{signal_label} z={peak_z_score:.2f} ratio={peak_ratio:.2f}",
    )


def _frame_mean(frame: bytes) -> float:
    return sum(frame) / (len(frame) * 255.0)


def _frame_delta(previous_frame: bytes, current_frame: bytes) -> float:
    return sum(abs(current - previous) for previous, current in zip(previous_frame, current_frame)) / (
        len(current_frame) * 255.0
    )


def _suppressed_by_boundary(
    timestamp: float,
    media_duration_seconds: float | None,
    suppress_initial_seconds: float,
    suppress_final_seconds: float,
) -> bool:
    if suppress_initial_seconds > 0 and timestamp <= suppress_initial_seconds:
        return True
    if (
        media_duration_seconds is not None
        and suppress_final_seconds > 0
        and timestamp >= max(0.0, media_duration_seconds - suppress_final_seconds)
    ):
        return True
    return False

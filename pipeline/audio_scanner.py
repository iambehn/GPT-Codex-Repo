from __future__ import annotations

import io
import math
import shutil
import struct
import subprocess
import wave
from pathlib import Path
from typing import Any

from pipeline.proxy_scanner import ProxySignal


class AudioScanError(RuntimeError):
    pass


def scan_audio_source(
    source: str | Path,
    config: dict[str, Any],
    media_duration_seconds: float | None = None,
) -> list[ProxySignal]:
    sample_rate = int(config.get("sample_rate", 16000))
    window_ms = int(config.get("window_ms", 250))
    rolling_baseline_windows = int(config.get("rolling_baseline_windows", 20))
    z_score_threshold = float(config.get("z_score_threshold", 3.0))
    default_confidence = float(config.get("default_confidence", 0.70))
    suppress_initial_seconds = float(config.get("suppress_initial_seconds", 1.0))
    suppress_final_seconds = float(config.get("suppress_final_seconds", 1.0))
    min_cluster_windows = int(config.get("min_cluster_windows", 2))
    min_peak_ratio = float(config.get("min_peak_ratio", 3.0))

    audio_bytes = _decode_audio_to_wav_bytes(source, sample_rate)
    energies = _compute_window_energies(audio_bytes, window_ms)
    if not energies:
        return []

    window_seconds = window_ms / 1000.0
    hot_windows: list[tuple[int, float, float]] = []
    for index, energy in enumerate(energies):
        history_start = max(0, index - rolling_baseline_windows)
        history = energies[history_start:index]
        if not history:
            continue
        baseline = sum(history) / len(history)
        variance = sum((item - baseline) ** 2 for item in history) / len(history)
        std_dev = math.sqrt(variance)
        ratio = energy / max(baseline, 1e-6)

        if std_dev > 1e-6:
            z_score = (energy - baseline) / std_dev
        elif energy > baseline and ratio >= 1.5:
            z_score = z_score_threshold + min(5.0, ratio - 1.0)
        else:
            z_score = 0.0

        timestamp = _window_center_seconds(index, window_seconds)
        if _suppressed_by_boundary(
            timestamp,
            media_duration_seconds=media_duration_seconds,
            suppress_initial_seconds=suppress_initial_seconds,
            suppress_final_seconds=suppress_final_seconds,
        ):
            continue

        if z_score >= z_score_threshold and ratio >= 1.5:
            hot_windows.append((index, z_score, ratio))

    if not hot_windows:
        return []

    signals: list[ProxySignal] = []
    cluster: list[tuple[int, float, float]] = [hot_windows[0]]
    for item in hot_windows[1:]:
        if item[0] == cluster[-1][0] + 1:
            cluster.append(item)
            continue
        signal = _cluster_to_signal(
            cluster,
            window_seconds,
            default_confidence,
            z_score_threshold,
            min_cluster_windows,
            min_peak_ratio,
        )
        if signal is not None:
            signals.append(signal)
        cluster = [item]

    signal = _cluster_to_signal(
        cluster,
        window_seconds,
        default_confidence,
        z_score_threshold,
        min_cluster_windows,
        min_peak_ratio,
    )
    if signal is not None:
        signals.append(signal)
    return signals


def _decode_audio_to_wav_bytes(source: str | Path, sample_rate: int) -> bytes:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    if not Path(ffmpeg).exists():
        raise AudioScanError("ffmpeg not found")

    command = [
        ffmpeg,
        "-v",
        "error",
        "-i",
        str(source),
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        "-f",
        "wav",
        "-",
    ]
    try:
        result = subprocess.run(command, capture_output=True, check=False, timeout=15)
    except subprocess.TimeoutExpired as exc:
        raise AudioScanError("audio decode timed out") from exc
    if result.returncode != 0 or not result.stdout:
        message = result.stderr.decode("utf-8", errors="ignore").strip() or "audio decode failed"
        raise AudioScanError(message)
    return result.stdout


def _compute_window_energies(audio_bytes: bytes, window_ms: int) -> list[float]:
    with wave.open(io.BytesIO(audio_bytes), "rb") as handle:
        frame_rate = handle.getframerate()
        sample_width = handle.getsampwidth()
        if sample_width != 2:
            raise AudioScanError(f"unsupported sample width: {sample_width}")
        channels = handle.getnchannels()
        if channels != 1:
            raise AudioScanError(f"expected mono audio, got {channels} channels")
        frame_count = handle.getnframes()
        frames = handle.readframes(frame_count)

    if not frames:
        return []

    sample_count = len(frames) // 2
    samples = struct.unpack("<" + ("h" * sample_count), frames)
    samples_per_window = max(1, int(frame_rate * window_ms / 1000))
    energies: list[float] = []

    for start in range(0, len(samples), samples_per_window):
        chunk = samples[start : start + samples_per_window]
        if not chunk:
            continue
        square_mean = sum((sample / 32768.0) ** 2 for sample in chunk) / len(chunk)
        energies.append(math.sqrt(square_mean))

    return energies


def _cluster_to_signal(
    cluster: list[tuple[int, float, float]],
    window_seconds: float,
    default_confidence: float,
    z_score_threshold: float,
    min_cluster_windows: int,
    min_peak_ratio: float,
) -> ProxySignal | None:
    peak_index, peak_z_score, peak_ratio = max(cluster, key=lambda item: (item[1], item[2]))
    if len(cluster) < min_cluster_windows and peak_ratio < min_peak_ratio:
        return None
    timestamp = (peak_index + 0.5) * window_seconds
    strength = min(1.0, max(0.10, peak_z_score / max(z_score_threshold * 2.0, 0.1)))
    return ProxySignal(
        source="audio_spike",
        source_family="audio_prepass",
        timestamp=timestamp,
        strength=strength,
        confidence=default_confidence,
        reason=f"audio energy spike z={peak_z_score:.2f} ratio={peak_ratio:.2f}",
    )


def _window_center_seconds(index: int, window_seconds: float) -> float:
    return (index + 0.5) * window_seconds


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

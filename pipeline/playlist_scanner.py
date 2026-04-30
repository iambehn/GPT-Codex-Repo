from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

from pipeline.proxy_scanner import ProxySignal


@dataclass
class _PlaylistSegment:
    timestamp: float
    duration: float
    discontinuity: bool


def is_playlist_source(source: str | Path) -> bool:
    source_text = str(source)
    parsed = urlparse(source_text)
    if parsed.scheme in {"http", "https"}:
        return parsed.path.lower().endswith(".m3u8")
    return Path(source_text).suffix.lower() == ".m3u8"


def scan_playlist_source(source: str | Path, config: dict[str, Any]) -> list[ProxySignal]:
    if not is_playlist_source(source):
        return []

    text = _load_playlist_text(source)
    if "#EXTM3U" not in text:
        return []

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if any(line.startswith("#EXT-X-STREAM-INF") for line in lines):
        return []

    segments, target_duration = _parse_media_playlist(lines)
    if not segments:
        return []

    duration_spike_ratio = float(config.get("duration_spike_ratio", 1.75))
    variance_window_segments = int(config.get("variance_window_segments", 3))
    default_confidence = float(config.get("default_confidence", 0.65))
    discontinuity_confidence = float(config.get("discontinuity_confidence", 0.80))

    durations = [segment.duration for segment in segments]
    baseline = float(target_duration or median(durations) or 1.0)
    signals: list[ProxySignal] = []
    abnormal_flags = [
        duration / max(baseline, 0.001) >= duration_spike_ratio for duration in durations
    ]

    for index, segment in enumerate(segments):
        if segment.discontinuity:
            signals.append(
                ProxySignal(
                    source="playlist_discontinuity",
                    source_family="playlist_hls",
                    timestamp=segment.timestamp,
                    strength=0.80,
                    confidence=discontinuity_confidence,
                    reason="playlist discontinuity marker",
                )
            )

        ratio = segment.duration / max(baseline, 0.001)
        if ratio >= duration_spike_ratio:
            strength = min(1.0, max(0.10, (ratio - 1.0) / max(duration_spike_ratio - 1.0, 0.10)))
            signals.append(
                ProxySignal(
                    source="playlist_spike",
                    source_family="playlist_hls",
                    timestamp=segment.timestamp,
                    strength=strength,
                    confidence=default_confidence,
                    reason=f"segment duration spike {segment.duration:.2f}s vs baseline {baseline:.2f}s",
                )
            )

        if variance_window_segments <= 1 or index + 1 < variance_window_segments:
            continue

        window_start = index + 1 - variance_window_segments
        abnormal_count = sum(abnormal_flags[window_start : index + 1])
        previous_count = sum(abnormal_flags[max(0, window_start - 1) : index])
        if abnormal_count >= 2 and previous_count < 2:
            window = segments[window_start : index + 1]
            peak_segment = max(window, key=lambda item: item.duration)
            spread = max(item.duration for item in window) / max(min(item.duration for item in window), 0.001)
            signals.append(
                ProxySignal(
                    source="playlist_spike",
                    source_family="playlist_hls",
                    timestamp=peak_segment.timestamp,
                    strength=min(1.0, max(0.15, (spread - 1.0) / 2.0)),
                    confidence=default_confidence,
                    reason=f"bursty duration variance across {variance_window_segments} segments",
                )
            )

    return signals


def _load_playlist_text(source: str | Path) -> str:
    source_text = str(source)
    parsed = urlparse(source_text)
    if parsed.scheme in {"http", "https"}:
        with urlopen(source_text, timeout=10) as response:
            return response.read().decode("utf-8")
    return Path(source_text).read_text(encoding="utf-8")


def _parse_media_playlist(lines: list[str]) -> tuple[list[_PlaylistSegment], float | None]:
    segments: list[_PlaylistSegment] = []
    target_duration: float | None = None
    current_timestamp = 0.0
    pending_duration: float | None = None
    pending_discontinuity = False

    for line in lines:
        if line.startswith("#EXT-X-TARGETDURATION:"):
            try:
                target_duration = float(line.split(":", 1)[1].strip())
            except ValueError:
                target_duration = None
            continue

        if line.startswith("#EXT-X-DISCONTINUITY"):
            pending_discontinuity = True
            continue

        if line.startswith("#EXTINF:"):
            raw_duration = line.split(":", 1)[1].split(",", 1)[0].strip()
            pending_duration = float(raw_duration)
            continue

        if line.startswith("#"):
            continue

        if pending_duration is None:
            continue

        segments.append(
            _PlaylistSegment(
                timestamp=current_timestamp,
                duration=pending_duration,
                discontinuity=pending_discontinuity,
            )
        )
        current_timestamp += pending_duration
        pending_duration = None
        pending_discontinuity = False

    return segments, target_duration

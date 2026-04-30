from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from pipeline.audio_scanner import scan_audio_source
from pipeline.chat_scanner import scan_chat_log
from pipeline.media_probe import probe_has_video_stream
from pipeline.playlist_scanner import is_playlist_source, scan_playlist_source
from pipeline.proxy_scanner import ProxySignal
from pipeline.visual_scanner import scan_visual_source


@dataclass(frozen=True)
class ProxyScanContext:
    source: str | Path
    chat_log: str | Path | None = None
    media_duration_seconds: float | None = None


@dataclass(frozen=True)
class ProxySourceDefinition:
    name: str
    scan: Callable[[ProxyScanContext, dict[str, Any]], list[ProxySignal]]


class ProxySourceSkipped(RuntimeError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def run_proxy_sources(
    context: ProxyScanContext,
    proxy_config: dict[str, Any],
) -> tuple[list[ProxySignal], dict[str, dict[str, Any]]]:
    source_config = proxy_config.get("sources", {})
    signals: list[ProxySignal] = []
    source_results: dict[str, dict[str, Any]] = {}

    for definition in _SOURCE_REGISTRY:
        config = source_config.get(definition.name, {})
        if not config.get("enabled", True):
            source_results[definition.name] = {
                "status": "skipped",
                "signal_count": 0,
                "reason": "disabled by config",
            }
            continue

        try:
            emitted = definition.scan(context, config)
        except ProxySourceSkipped as exc:
            source_results[definition.name] = {
                "status": "skipped",
                "signal_count": 0,
                "reason": exc.reason,
            }
        except Exception as exc:
            source_results[definition.name] = {
                "status": "failed",
                "signal_count": 0,
                "reason": str(exc),
            }
        else:
            signals.extend(emitted)
            source_results[definition.name] = {
                "status": "ok",
                "signal_count": len(emitted),
            }

    return signals, source_results


def _scan_playlist_hls(context: ProxyScanContext, config: dict[str, Any]) -> list[ProxySignal]:
    if not is_playlist_source(context.source):
        raise ProxySourceSkipped("source is not a playlist")
    return scan_playlist_source(context.source, config)


def _scan_audio_prepass(context: ProxyScanContext, config: dict[str, Any]) -> list[ProxySignal]:
    if _is_local_playlist_snapshot(context.source):
        raise ProxySourceSkipped("local playlist snapshot is not decoded for audio prepass")
    return scan_audio_source(context.source, config, media_duration_seconds=context.media_duration_seconds)


def _scan_chat_velocity(context: ProxyScanContext, config: dict[str, Any]) -> list[ProxySignal]:
    if context.chat_log is None:
        raise ProxySourceSkipped("no chat log provided")
    return scan_chat_log(context.chat_log, config)


def _scan_visual_prepass(context: ProxyScanContext, config: dict[str, Any]) -> list[ProxySignal]:
    if _is_remote_source(context.source):
        raise ProxySourceSkipped("visual prepass supports local media only")
    if is_playlist_source(context.source):
        raise ProxySourceSkipped("source is not a decodable local video file")

    source_path = Path(str(context.source))
    if not source_path.exists() or not source_path.is_file():
        raise ProxySourceSkipped("local media file is missing or unreadable")

    has_video_stream = probe_has_video_stream(source_path)
    if has_video_stream is False:
        raise ProxySourceSkipped("source has no video stream")

    return scan_visual_source(source_path, config, media_duration_seconds=context.media_duration_seconds)


def _is_local_playlist_snapshot(source: str | Path) -> bool:
    if not is_playlist_source(source):
        return False
    parsed = urlparse(str(source))
    return parsed.scheme not in {"http", "https"}


def _is_remote_source(source: str | Path) -> bool:
    parsed = urlparse(str(source))
    return parsed.scheme in {"http", "https"}


_SOURCE_REGISTRY = (
    ProxySourceDefinition(name="playlist_hls", scan=_scan_playlist_hls),
    ProxySourceDefinition(name="audio_prepass", scan=_scan_audio_prepass),
    ProxySourceDefinition(name="visual_prepass", scan=_scan_visual_prepass),
    ProxySourceDefinition(name="chat_velocity", scan=_scan_chat_velocity),
)

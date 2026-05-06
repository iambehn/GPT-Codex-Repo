from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from pipeline.audio_scanner import scan_audio_source
from pipeline.chat_scanner import scan_chat_log
from pipeline.hf_highlight import ProxySourceEmission, scan_hf_multimodal_source
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
    scan: Callable[[ProxyScanContext, dict[str, Any]], list[ProxySignal] | ProxySourceEmission]


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
            if isinstance(emitted, ProxySourceEmission):
                enriched_signals = _enrich_proxy_signals(
                    emitted.signals,
                    producer=definition.name,
                    source_ref=_source_ref_for_definition(context, definition.name),
                )
                signals.extend(enriched_signals)
                source_results[definition.name] = {
                    "status": "ok",
                    "signal_count": len(enriched_signals),
                    **({"metadata": emitted.metadata} if emitted.metadata else {}),
                }
            else:
                enriched_signals = _enrich_proxy_signals(
                    emitted,
                    producer=definition.name,
                    source_ref=_source_ref_for_definition(context, definition.name),
                )
                signals.extend(enriched_signals)
                source_results[definition.name] = {
                    "status": "ok",
                    "signal_count": len(enriched_signals),
                }

    return signals, source_results


def _enrich_proxy_signals(
    signals: list[ProxySignal],
    *,
    producer: str,
    source_ref: str,
) -> list[ProxySignal]:
    enriched: list[ProxySignal] = []
    for signal in signals:
        enriched.append(
            replace(
                signal,
                producer=signal.producer or producer,
                source_ref=signal.source_ref or source_ref,
                start_timestamp=signal.start_timestamp if signal.start_timestamp is not None else signal.timestamp,
                end_timestamp=signal.end_timestamp if signal.end_timestamp is not None else signal.timestamp,
            )
        )
    return enriched


def _source_ref_for_definition(context: ProxyScanContext, definition_name: str) -> str:
    if definition_name == "chat_velocity" and context.chat_log is not None:
        return str(context.chat_log)
    return str(context.source)


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


def _scan_hf_multimodal(context: ProxyScanContext, config: dict[str, Any]) -> ProxySourceEmission:
    if _is_remote_source(context.source):
        raise ProxySourceSkipped("hf multimodal source supports local media only")
    if is_playlist_source(context.source):
        raise ProxySourceSkipped("source is not a decodable local video file")

    source_path = Path(str(context.source))
    if not source_path.exists() or not source_path.is_file():
        raise ProxySourceSkipped("local media file is missing or unreadable")

    has_video_stream = probe_has_video_stream(source_path)
    if has_video_stream is False:
        raise ProxySourceSkipped("source has no video stream")

    return scan_hf_multimodal_source(
        source_path,
        config,
        media_duration_seconds=context.media_duration_seconds,
    )


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
    ProxySourceDefinition(name="hf_multimodal", scan=_scan_hf_multimodal),
    ProxySourceDefinition(name="chat_velocity", scan=_scan_chat_velocity),
)

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ProxySignal:
    source: str
    source_family: str
    timestamp: float
    strength: float
    confidence: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "source_family": self.source_family,
            "timestamp": round(self.timestamp, 3),
            "strength": round(self.strength, 4),
            "confidence": round(self.confidence, 4),
            "reason": self.reason,
        }


@dataclass
class ProxyWindow:
    start_seconds: float
    end_seconds: float
    proxy_score: float
    signal_count: int
    sources: list[str]
    source_families: list[str]
    recommended_action: str
    signals: list[ProxySignal] = field(default_factory=list)
    explanation: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_seconds": round(self.start_seconds, 3),
            "end_seconds": round(self.end_seconds, 3),
            "proxy_score": round(self.proxy_score, 4),
            "signal_count": self.signal_count,
            "sources": self.sources,
            "source_families": self.source_families,
            "recommended_action": self.recommended_action,
            "signals": [signal.to_dict() for signal in self.signals],
            "explanation": self.explanation,
        }


def build_proxy_windows(
    signals: list[ProxySignal],
    config: dict[str, Any],
    media_duration_seconds: float | None = None,
) -> list[ProxyWindow]:
    weights = config.get("weights", {"chat_spike": 3.5})
    selection = config.get("candidate_selection", {})
    dedupe_gap = float(selection.get("dedupe_gap_seconds", 3))
    merge_gap = float(selection.get("merge_gap_seconds", 30))
    audio_only_merge_gap = float(selection.get("audio_only_merge_gap_seconds", 8))
    window_pre = float(selection.get("window_pre_seconds", 10))
    window_post = float(selection.get("window_post_seconds", 25))
    audio_only_window_pre = float(selection.get("audio_only_window_pre_seconds", 3))
    audio_only_window_post = float(selection.get("audio_only_window_post_seconds", 6))
    min_proxy_score = float(selection.get("min_proxy_score", 0.30))
    max_windows = int(selection.get("max_windows", 20))
    agreement_bonus_per_extra_source = float(selection.get("agreement_bonus_per_extra_source", 0.10))
    max_agreement_bonus = float(selection.get("max_agreement_bonus", 0.25))
    cost_gates = config.get("cost_gates", {})

    deduped = _dedupe_signals(signals, dedupe_gap_seconds=dedupe_gap)
    clusters = _group_signals(
        deduped,
        merge_gap_seconds=merge_gap,
        audio_only_merge_gap_seconds=audio_only_merge_gap,
    )

    windows: list[ProxyWindow] = []
    for cluster in clusters:
        proxy_score, explanation = _score_cluster(
            cluster=cluster,
            weights=weights,
            agreement_bonus_per_extra_source=agreement_bonus_per_extra_source,
            max_agreement_bonus=max_agreement_bonus,
        )
        if proxy_score < min_proxy_score:
            continue
        timestamps = [signal.timestamp for signal in cluster]
        sources = sorted({signal.source for signal in cluster})
        source_families = sorted({signal.source_family for signal in cluster})
        audio_only = set(source_families) == {"audio_prepass"}
        effective_window_pre = audio_only_window_pre if audio_only else window_pre
        effective_window_post = audio_only_window_post if audio_only else window_post
        start_seconds = max(0.0, min(timestamps) - effective_window_pre)
        end_seconds = max(timestamps) + effective_window_post
        if media_duration_seconds is not None:
            start_seconds = min(start_seconds, media_duration_seconds)
            end_seconds = min(end_seconds, media_duration_seconds)
        windows.append(
            ProxyWindow(
                start_seconds=start_seconds,
                end_seconds=end_seconds,
                proxy_score=proxy_score,
                signal_count=len(cluster),
                sources=sources,
                source_families=source_families,
                recommended_action=_recommended_action(proxy_score, len(source_families), cost_gates),
                signals=list(cluster),
                explanation=explanation,
            )
        )

    windows.sort(key=lambda window: (-window.proxy_score, window.start_seconds))
    return windows[:max_windows]


def _dedupe_signals(signals: list[ProxySignal], dedupe_gap_seconds: float) -> list[ProxySignal]:
    ordered = sorted(signals, key=lambda signal: (signal.source, signal.timestamp))
    kept: list[ProxySignal] = []
    last_by_source: dict[str, ProxySignal] = {}

    for signal in ordered:
        previous = last_by_source.get(signal.source)
        if previous is None or abs(signal.timestamp - previous.timestamp) >= dedupe_gap_seconds:
            kept.append(signal)
            last_by_source[signal.source] = signal
            continue
        if signal.strength * signal.confidence > previous.strength * previous.confidence:
            kept.remove(previous)
            kept.append(signal)
            last_by_source[signal.source] = signal

    kept.sort(key=lambda signal: signal.timestamp)
    return kept


def _group_signals(
    signals: list[ProxySignal],
    merge_gap_seconds: float,
    audio_only_merge_gap_seconds: float,
) -> list[list[ProxySignal]]:
    if not signals:
        return []
    groups: list[list[ProxySignal]] = [[signals[0]]]
    for signal in signals[1:]:
        current = groups[-1]
        current_families = {item.source_family for item in current}
        candidate_families = current_families | {signal.source_family}
        effective_gap = audio_only_merge_gap_seconds if candidate_families == {"audio_prepass"} else merge_gap_seconds
        if signal.timestamp - current[-1].timestamp <= effective_gap:
            current.append(signal)
        else:
            groups.append([signal])
    return groups


def _score_cluster(
    cluster: list[ProxySignal],
    weights: dict[str, float],
    agreement_bonus_per_extra_source: float,
    max_agreement_bonus: float,
) -> tuple[float, list[str]]:
    strongest_by_source: dict[str, ProxySignal] = {}
    for signal in cluster:
        existing = strongest_by_source.get(signal.source)
        if existing is None or signal.strength * signal.confidence > existing.strength * existing.confidence:
            strongest_by_source[signal.source] = signal

    total_weight = 0.0
    weighted_score = 0.0
    explanation: list[str] = []
    for source, signal in sorted(strongest_by_source.items()):
        weight = float(weights.get(source, 1.0))
        contribution = weight * signal.strength * signal.confidence
        total_weight += weight
        weighted_score += contribution
        explanation.append(
            f"{source}: weight={weight:.2f} strength={signal.strength:.2f} confidence={signal.confidence:.2f}"
        )

    base_score = weighted_score / total_weight if total_weight else 0.0
    family_count = len({signal.source_family for signal in strongest_by_source.values()})
    agreement_bonus = min(max_agreement_bonus, max(0, family_count - 1) * agreement_bonus_per_extra_source)
    final_score = min(1.0, base_score + agreement_bonus)
    if agreement_bonus:
        explanation.append(f"agreement_bonus={agreement_bonus:.2f} families={family_count}")
    explanation.append(f"proxy_score={final_score:.2f}")
    return final_score, explanation


def _recommended_action(proxy_score: float, source_count: int, cost_gates: dict[str, Any]) -> str:
    inspect_min_score = float(cost_gates.get("inspect_min_score", 0.40))
    download_candidate_min_score = float(cost_gates.get("download_candidate_min_score", 0.75))
    download_candidate_min_sources = int(cost_gates.get("download_candidate_min_sources", 2))

    if proxy_score < inspect_min_score:
        return "skip"
    if proxy_score >= download_candidate_min_score and source_count >= download_candidate_min_sources:
        return "download_candidate"
    return "inspect"

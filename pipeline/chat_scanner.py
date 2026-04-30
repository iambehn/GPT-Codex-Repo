from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

from pipeline.proxy_scanner import ProxySignal


KEYWORD_WEIGHTS = {
    "clip it": 2.5,
    "clip": 1.8,
    "wtf": 2.0,
    "no way": 2.0,
    "insane": 1.8,
    "omg": 1.8,
    "pogchamp": 1.5,
    "pog": 1.5,
    "lmao": 1.4,
    "lul": 1.2,
    "???": 1.2,
}

_BRACKETED_RE = re.compile(r"^\[(\d{2}):(\d{2}):(\d{2})\]\s+[^:]+:\s*(.*)$")
_OFFSET_RE = re.compile(r"^(\d+(?:\.\d+)?)\s+[^:]+:\s*(.*)$")


def scan_chat_log(log_path: str | Path, config: dict[str, Any]) -> list[ProxySignal]:
    records = _parse_log(log_path)
    return _compute_velocity_signals(records, config)


def _parse_log(log_path: str | Path) -> list[dict[str, float]]:
    records: list[dict[str, float]] = []
    for raw_line in Path(log_path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        seconds, message = _parse_line(line)
        if seconds is None or message is None:
            continue
        weight = _message_weight(message)
        if weight <= 0:
            continue
        records.append({"seconds": seconds, "weight": weight})
    return records


def _parse_line(line: str) -> tuple[float | None, str | None]:
    bracketed = _BRACKETED_RE.match(line)
    if bracketed:
        hours, minutes, seconds, message = bracketed.groups()
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds), message
    offset = _OFFSET_RE.match(line)
    if offset:
        seconds, message = offset.groups()
        return float(seconds), message
    return None, None


def _message_weight(message: str) -> float:
    lowered = message.lower()
    matched: set[str] = set()
    total = 0.0
    for keyword, weight in KEYWORD_WEIGHTS.items():
        if keyword in lowered and keyword not in matched:
            matched.add(keyword)
            total += weight
    return total


def _compute_velocity_signals(records: list[dict[str, float]], config: dict[str, Any]) -> list[ProxySignal]:
    if not records:
        return []
    bucket_seconds = int(config.get("bucket_seconds", 5))
    rolling_baseline_seconds = int(config.get("rolling_baseline_seconds", 300))
    burst_threshold = float(config.get("burst_threshold", 3.0))
    default_confidence = float(config.get("default_confidence", 0.70))

    bucket_scores: dict[int, float] = {}
    for record in records:
        bucket = int(record["seconds"] // bucket_seconds) * bucket_seconds
        bucket_scores[bucket] = bucket_scores.get(bucket, 0.0) + float(record["weight"])

    ordered_buckets = sorted(bucket_scores)
    baseline_bucket_count = max(1, rolling_baseline_seconds // bucket_seconds)
    signals: list[ProxySignal] = []

    for index, bucket in enumerate(ordered_buckets):
        history_start = max(0, index - baseline_bucket_count)
        history = ordered_buckets[history_start:index]
        baseline = sum(bucket_scores[item] for item in history) / len(history) if history else 0.0
        raw_score = bucket_scores[bucket]
        velocity = raw_score / (baseline + 0.1)
        if velocity < burst_threshold:
            continue
        strength = min(1.0, velocity / 10.0)
        signals.append(
            ProxySignal(
                source="chat_spike",
                source_family="chat_velocity",
                timestamp=float(bucket),
                strength=strength,
                confidence=default_confidence,
                reason=f"chat velocity {velocity:.1f}x baseline",
            )
        )
    return signals

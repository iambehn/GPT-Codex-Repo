from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.game_pack import load_game_pack
from pipeline.roi_matcher import validate_published_pack
from pipeline.runtime_ontology import load_runtime_signal_event_ontology, validate_group_by_fields
from pipeline.simple_yaml import load_yaml_file


REPO_ROOT = Path(__file__).resolve().parent.parent
FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "fused_analysis"
PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"

SUPPORTED_CONFIDENCE_METHODS = {"max", "mean"}
MAX_SYNERGY_MULTIPLIER = 1.5


class FusionAnalysisError(RuntimeError):
    def __init__(self, status: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message

    def to_dict(self, *, game: str | None = None, source: str | Path | None = None) -> dict[str, Any]:
        payload = {
            "ok": False,
            "status": self.status,
            "error": self.message,
        }
        if game is not None:
            payload["game"] = game
        if source is not None:
            payload["source"] = str(source)
        return payload


@dataclass(frozen=True)
class FusionRule:
    rule_id: str
    event_type: str
    signal_types: tuple[str, ...]
    required_signal_types: tuple[str, ...]
    window_seconds: float
    min_signal_count: int
    confidence_method: str
    corroboration_bonus_per_extra_signal: float
    max_bonus: float
    low_confidence_threshold: float
    low_confidence_penalty: float
    group_by: tuple[str, ...]
    anchor_signal_types: tuple[str, ...]
    dependent_signal_types: tuple[str, ...]
    anchor_event_types: tuple[str, ...]
    lag_window_seconds: float | None
    require_for_confirm: bool
    confirm_multiplier: float
    ambiguous_confidence_multiplier: float
    clip_start_lead_seconds: float
    clip_end_lag_seconds: float
    synergy_enabled: bool
    synergy_minimum_required_signals: int
    synergy_interactions: tuple["SynergyInteraction", ...]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SynergyInteraction:
    signal_type: str
    multiplier: float
    required: bool
    label: str | None
    metadata: dict[str, Any]


def fuse_analysis(
    source: str | Path,
    game: str,
    *,
    proxy_sidecar: dict[str, Any],
    runtime_sidecar: dict[str, Any],
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    rules_path: str | Path | None = None,
) -> dict[str, Any]:
    _validate_sidecar_game_and_source(proxy_sidecar, game, source, sidecar_kind="proxy")
    _validate_sidecar_game_and_source(runtime_sidecar, game, source, sidecar_kind="runtime")
    rules = load_fusion_rules(game, rules_path=rules_path)
    normalized_signals = normalize_fusion_signals(proxy_sidecar=proxy_sidecar, runtime_sidecar=runtime_sidecar)
    fused_events, rule_matches = _fuse_normalized_signals(normalized_signals, rules, game=game)

    sidecar_path = _fused_analysis_path(source, game, output_path)
    payload = {
        "schema_version": FUSED_ANALYSIS_SCHEMA_VERSION,
        "fusion_id": _fusion_id(game, source),
        "ok": True,
        "status": "ok" if fused_events else "no_fused_events",
        "game": game,
        "source": str(source),
        "sidecar_path": str(sidecar_path),
        "proxy": {
            "schema_version": proxy_sidecar.get("schema_version"),
            "scan_id": proxy_sidecar.get("scan_id"),
            "sidecar_path": proxy_sidecar.get("sidecar_path"),
            "signal_count": int(proxy_sidecar.get("signal_count", 0) or 0),
            "window_count": int(proxy_sidecar.get("window_count", 0) or 0),
            "source_results": proxy_sidecar.get("source_results", {}),
        },
        "runtime": {
            "schema_version": runtime_sidecar.get("schema_version"),
            "analysis_id": runtime_sidecar.get("analysis_id"),
            "sidecar_path": runtime_sidecar.get("sidecar_path"),
            "signal_count": int(runtime_sidecar.get("events", {}).get("signal_count", 0) or 0),
            "event_count": int(runtime_sidecar.get("events", {}).get("event_count", 0) or 0),
            "matcher_status": runtime_sidecar.get("matcher", {}).get("status"),
            "events_status": runtime_sidecar.get("events", {}).get("status"),
        },
        "normalized_signals": normalized_signals,
        "fused_events": fused_events,
        "fusion_summary": _fusion_summary(normalized_signals, fused_events, rules, game=game),
        "rule_matches": rule_matches,
    }
    try:
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:
        raise FusionAnalysisError("sidecar_write_failed", str(exc)) from exc

    if debug_output_dir is not None:
        _write_debug_bundle(_resolve_path(debug_output_dir), payload)
    return payload


def load_proxy_sidecar(path: str | Path) -> dict[str, Any]:
    payload = _load_json_object(path, status_prefix="proxy_sidecar")
    if payload.get("schema_version") != PROXY_SCAN_SCHEMA_VERSION:
        raise FusionAnalysisError(
            "invalid_proxy_sidecar",
            f"proxy sidecar must use schema_version '{PROXY_SCAN_SCHEMA_VERSION}'",
        )
    return payload


def load_runtime_sidecar(path: str | Path) -> dict[str, Any]:
    payload = _load_json_object(path, status_prefix="runtime_sidecar")
    if payload.get("schema_version") != RUNTIME_ANALYSIS_SCHEMA_VERSION:
        raise FusionAnalysisError(
            "invalid_runtime_sidecar",
            f"runtime sidecar must use schema_version '{RUNTIME_ANALYSIS_SCHEMA_VERSION}'",
        )
    return payload


def load_fusion_rules(game: str, *, rules_path: str | Path | None = None) -> list[FusionRule]:
    ontology = load_runtime_signal_event_ontology()
    try:
        game_pack = load_game_pack(game)
    except FileNotFoundError as exc:
        raise FusionAnalysisError("missing_game_pack", str(exc)) from exc
    resolved_rules_path = _resolve_path(rules_path) if rules_path is not None else game_pack.root / "manifests" / "fusion_rules.yaml"
    if not resolved_rules_path.exists():
        raise FusionAnalysisError(
            "missing_fusion_rules",
            f"published pack '{game}' is missing fusion rules manifest: {resolved_rules_path}",
        )
    payload = load_yaml_file(resolved_rules_path)
    if not isinstance(payload, dict):
        raise FusionAnalysisError("invalid_fusion_rules", "fusion_rules.yaml must parse to a top-level object")
    raw_rules = payload.get("rules", [])
    if not isinstance(raw_rules, list) or not raw_rules:
        raise FusionAnalysisError("invalid_fusion_rules", "fusion_rules.yaml must define a non-empty top-level 'rules' list")

    rules: list[FusionRule] = []
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, dict):
            raise FusionAnalysisError("invalid_fusion_rules", "each fusion rule must be an object")
        rule_id = str(raw_rule.get("rule_id", "")).strip()
        event_type = str(raw_rule.get("event_type", "")).strip()
        signal_types = _normalized_string_list(raw_rule.get("signal_types"))
        required_signal_types = _normalized_string_list(raw_rule.get("required_signal_types")) or list(signal_types)
        confidence_method = str(raw_rule.get("confidence_method", "max")).strip() or "max"
        if not rule_id or not event_type or not signal_types:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                "each fusion rule must include non-empty rule_id, event_type, and signal_types",
            )
        if event_type not in ontology.event_types:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' uses unknown ontology event_type '{event_type}'",
            )
        unknown_signal_types = sorted(signal_type for signal_type in signal_types if signal_type not in ontology.signal_types)
        if unknown_signal_types:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' uses unknown ontology signal_types: {unknown_signal_types}",
            )
        missing_required_types = sorted(signal_type for signal_type in required_signal_types if signal_type not in signal_types)
        if missing_required_types:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' requires signal_types not listed in signal_types: {missing_required_types}",
            )
        if confidence_method not in SUPPORTED_CONFIDENCE_METHODS:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' uses unsupported confidence_method '{confidence_method}'",
            )
        window_seconds = float(raw_rule.get("window_seconds", 0.0) or 0.0)
        min_signal_count = int(raw_rule.get("min_signal_count", 1) or 1)
        if window_seconds <= 0:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' must use a positive window_seconds",
            )
        if min_signal_count <= 0:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' must use a positive min_signal_count",
            )
        gate_field_names = {
            "anchor_signal_types",
            "dependent_signal_types",
            "anchor_event_types",
            "lag_window_seconds",
            "require_for_confirm",
            "confirm_multiplier",
            "ambiguous_confidence_multiplier",
            "clip_start_lead_seconds",
            "clip_end_lag_seconds",
        }
        gate_configured = any(field_name in raw_rule for field_name in gate_field_names)
        anchor_signal_types = _normalized_string_list(raw_rule.get("anchor_signal_types"))
        dependent_signal_types = _normalized_string_list(raw_rule.get("dependent_signal_types"))
        anchor_event_types = _normalized_string_list(raw_rule.get("anchor_event_types"))
        lag_window_seconds = raw_rule.get("lag_window_seconds")
        if gate_configured:
            if not anchor_signal_types or not dependent_signal_types:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"gated fusion rule '{rule_id}' must define non-empty anchor_signal_types and dependent_signal_types",
                )
            unknown_anchor_signal_types = sorted(
                signal_type for signal_type in anchor_signal_types if signal_type not in signal_types
            )
            if unknown_anchor_signal_types:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"fusion rule '{rule_id}' uses anchor_signal_types not listed in signal_types: {unknown_anchor_signal_types}",
                )
            unknown_dependent_signal_types = sorted(
                signal_type for signal_type in dependent_signal_types if signal_type not in ontology.signal_types
            )
            if unknown_dependent_signal_types:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"fusion rule '{rule_id}' uses unknown ontology dependent_signal_types: {unknown_dependent_signal_types}",
                )
            if lag_window_seconds is None or float(lag_window_seconds or 0.0) <= 0.0:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"gated fusion rule '{rule_id}' must use a positive lag_window_seconds",
                )
            if float(raw_rule.get("confirm_multiplier", 0.0) or 0.0) <= 0.0:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"gated fusion rule '{rule_id}' must use a positive confirm_multiplier",
                )
            if float(raw_rule.get("ambiguous_confidence_multiplier", 0.0) or 0.0) <= 0.0:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"gated fusion rule '{rule_id}' must use a positive ambiguous_confidence_multiplier",
                )
            if float(raw_rule.get("clip_start_lead_seconds", -1.0) or 0.0) < 0.0:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"gated fusion rule '{rule_id}' must use a non-negative clip_start_lead_seconds",
                )
            if float(raw_rule.get("clip_end_lag_seconds", -1.0) or 0.0) < 0.0:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"gated fusion rule '{rule_id}' must use a non-negative clip_end_lag_seconds",
                )
        raw_synergy = raw_rule.get("synergy")
        synergy_enabled = False
        synergy_minimum_required_signals = 0
        synergy_interactions: list[SynergyInteraction] = []
        if raw_synergy is not None:
            if not isinstance(raw_synergy, dict):
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"fusion rule '{rule_id}' synergy block must be an object when present",
                )
            synergy_enabled = bool(raw_synergy.get("enabled", False))
            synergy_minimum_required_signals = int(raw_synergy.get("minimum_required_signals", 0) or 0)
            if synergy_minimum_required_signals < 0:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"fusion rule '{rule_id}' must use a non-negative synergy.minimum_required_signals",
                )
            raw_interactions = raw_synergy.get("interactions", [])
            if raw_interactions is None:
                raw_interactions = []
            if not isinstance(raw_interactions, list):
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"fusion rule '{rule_id}' synergy.interactions must be a list",
                )
            allowed_synergy_signal_types = set(signal_types) | set(dependent_signal_types)
            for raw_interaction in raw_interactions:
                if not isinstance(raw_interaction, dict):
                    raise FusionAnalysisError(
                        "invalid_fusion_rules",
                        f"fusion rule '{rule_id}' synergy interactions must be objects",
                    )
                interaction_signal_type = str(raw_interaction.get("signal_type", "")).strip()
                if not interaction_signal_type:
                    raise FusionAnalysisError(
                        "invalid_fusion_rules",
                        f"fusion rule '{rule_id}' synergy interactions must define signal_type",
                    )
                if interaction_signal_type not in ontology.signal_types:
                    raise FusionAnalysisError(
                        "invalid_fusion_rules",
                        f"fusion rule '{rule_id}' uses unknown ontology synergy signal_type '{interaction_signal_type}'",
                    )
                if interaction_signal_type not in allowed_synergy_signal_types:
                    raise FusionAnalysisError(
                        "invalid_fusion_rules",
                        f"fusion rule '{rule_id}' synergy signal_type '{interaction_signal_type}' must be listed in signal_types or dependent_signal_types",
                    )
                interaction_multiplier = float(raw_interaction.get("multiplier", 0.0) or 0.0)
                if interaction_multiplier <= 0.0:
                    raise FusionAnalysisError(
                        "invalid_fusion_rules",
                        f"fusion rule '{rule_id}' synergy interaction '{interaction_signal_type}' must use a positive multiplier",
                    )
                interaction_metadata = raw_interaction.get("metadata", {})
                if interaction_metadata is None:
                    interaction_metadata = {}
                if not isinstance(interaction_metadata, dict):
                    raise FusionAnalysisError(
                        "invalid_fusion_rules",
                        f"fusion rule '{rule_id}' synergy interaction '{interaction_signal_type}' metadata must be an object",
                    )
                label = str(raw_interaction.get("label", "")).strip() or None
                synergy_interactions.append(
                    SynergyInteraction(
                        signal_type=interaction_signal_type,
                        multiplier=interaction_multiplier,
                        required=bool(raw_interaction.get("required", False)),
                        label=label,
                        metadata=interaction_metadata,
                    )
                )
            if synergy_enabled and not synergy_interactions:
                raise FusionAnalysisError(
                    "invalid_fusion_rules",
                    f"fusion rule '{rule_id}' enables synergy but defines no interactions",
                )
        metadata = raw_rule.get("metadata", {})
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' metadata must be an object when present",
            )
        invalid_group_by_fields = validate_group_by_fields(
            ontology,
            [str(item).strip() for item in _normalized_string_list(raw_rule.get("group_by")) if str(item).strip()],
        )
        if invalid_group_by_fields:
            raise FusionAnalysisError(
                "invalid_fusion_rules",
                f"fusion rule '{rule_id}' uses invalid ontology group_by fields: {invalid_group_by_fields}",
            )
        rules.append(
            FusionRule(
                rule_id=rule_id,
                event_type=event_type,
                signal_types=tuple(signal_types),
                required_signal_types=tuple(required_signal_types),
                window_seconds=window_seconds,
                min_signal_count=min_signal_count,
                confidence_method=confidence_method,
                corroboration_bonus_per_extra_signal=float(raw_rule.get("corroboration_bonus_per_extra_signal", 0.0) or 0.0),
                max_bonus=float(raw_rule.get("max_bonus", 0.0) or 0.0),
                low_confidence_threshold=float(raw_rule.get("low_confidence_threshold", 0.5) or 0.5),
                low_confidence_penalty=float(raw_rule.get("low_confidence_penalty", 0.0) or 0.0),
                group_by=tuple(_normalized_string_list(raw_rule.get("group_by"))),
                anchor_signal_types=tuple(anchor_signal_types),
                dependent_signal_types=tuple(dependent_signal_types),
                anchor_event_types=tuple(anchor_event_types),
                lag_window_seconds=float(lag_window_seconds) if lag_window_seconds is not None else None,
                require_for_confirm=bool(raw_rule.get("require_for_confirm", True)),
                confirm_multiplier=float(raw_rule.get("confirm_multiplier", 1.0) or 1.0),
                ambiguous_confidence_multiplier=float(raw_rule.get("ambiguous_confidence_multiplier", 1.0) or 1.0),
                clip_start_lead_seconds=float(raw_rule.get("clip_start_lead_seconds", 0.0) or 0.0),
                clip_end_lag_seconds=float(raw_rule.get("clip_end_lag_seconds", 0.0) or 0.0),
                synergy_enabled=synergy_enabled,
                synergy_minimum_required_signals=synergy_minimum_required_signals,
                synergy_interactions=tuple(synergy_interactions),
                metadata=metadata,
            )
        )
    return rules


def normalize_fusion_signals(*, proxy_sidecar: dict[str, Any], runtime_sidecar: dict[str, Any]) -> list[dict[str, Any]]:
    signals = normalize_proxy_signals(proxy_sidecar) + normalize_runtime_signals(runtime_sidecar)
    signals.sort(key=lambda row: (float(row["start_timestamp"]), row["signal_type"], row["signal_id"]))
    return signals


def normalize_proxy_signals(proxy_sidecar: dict[str, Any]) -> list[dict[str, Any]]:
    signals = proxy_sidecar.get("signals", [])
    windows = proxy_sidecar.get("windows", [])
    if not isinstance(signals, list):
        raise FusionAnalysisError("invalid_proxy_sidecar", "proxy sidecar must contain a top-level 'signals' list")
    result: list[dict[str, Any]] = []
    for index, row in enumerate(signals):
        if not isinstance(row, dict):
            raise FusionAnalysisError("invalid_proxy_sidecar", "proxy sidecar signals must be objects")
        timestamp = float(row.get("timestamp", 0.0) or 0.0)
        source = str(row.get("source", "")).strip()
        if not source:
            continue
        signal_id = f"{proxy_sidecar.get('scan_id', 'proxy')}:proxy:{index}"
        matching_windows = [
            {
                "window_index": window_index,
                "recommended_action": window.get("recommended_action"),
                "proxy_score": window.get("proxy_score"),
            }
            for window_index, window in enumerate(windows)
            if isinstance(window, dict)
            and float(window.get("start_seconds", timestamp)) <= timestamp <= float(window.get("end_seconds", timestamp))
        ]
        result.append(
            {
                "signal_id": signal_id,
                "producer_family": "proxy",
                "signal_type": source,
                "timestamp": round(timestamp, 5),
                "start_timestamp": round(timestamp, 5),
                "end_timestamp": round(timestamp, 5),
                "confidence": round(float(row.get("confidence", 0.0) or 0.0), 5),
                "strength": round(float(row.get("strength", 0.0) or 0.0), 5),
                "source_ref": str(proxy_sidecar.get("sidecar_path") or proxy_sidecar.get("scan_id") or ""),
                "source_family": str(row.get("source_family", "")).strip() or None,
                "evidence": {
                    "reason": row.get("reason"),
                    "matching_windows": matching_windows,
                },
            }
        )
    return result


def normalize_runtime_signals(runtime_sidecar: dict[str, Any]) -> list[dict[str, Any]]:
    matcher_section = runtime_sidecar.get("matcher", {})
    raw_signals = matcher_section.get("signals", [])
    if not isinstance(raw_signals, list):
        raise FusionAnalysisError("invalid_runtime_sidecar", "runtime sidecar matcher.signals must be a list")
    result: list[dict[str, Any]] = []
    for row in raw_signals:
        if not isinstance(row, dict):
            raise FusionAnalysisError("invalid_runtime_sidecar", "runtime sidecar signals must be objects")
        signal_type = str(row.get("signal_type", "")).strip()
        signal_id = str(row.get("signal_id", "")).strip()
        if not signal_type or not signal_id:
            continue
        normalized = {
            "signal_id": signal_id,
            "producer_family": "runtime",
            "signal_type": signal_type,
            "timestamp": round(float(row.get("timestamp", 0.0) or 0.0), 5),
            "start_timestamp": round(float(row.get("start_timestamp", 0.0) or 0.0), 5),
            "end_timestamp": round(float(row.get("end_timestamp", 0.0) or 0.0), 5),
            "confidence": round(float(row.get("confidence", 0.0) or 0.0), 5),
            "strength": round(float(row.get("confidence", 0.0) or 0.0), 5),
            "source_ref": str(runtime_sidecar.get("sidecar_path") or runtime_sidecar.get("analysis_id") or ""),
            "source_family": row.get("asset_family"),
            "asset_id": row.get("asset_id"),
            "asset_family": row.get("asset_family"),
            "roi_ref": row.get("roi_ref"),
            "evidence": row.get("evidence", {}),
        }
        for field in ("entity_id", "ability_id", "equipment_id", "event_row_id", "display_name"):
            if field in row:
                normalized[field] = row[field]
        result.append(normalized)
    return result


def _fuse_normalized_signals(
    normalized_signals: list[dict[str, Any]],
    rules: list[FusionRule],
    *,
    game: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events: list[dict[str, Any]] = []
    rule_matches: list[dict[str, Any]] = []
    for rule in rules:
        candidates = [row for row in normalized_signals if row["signal_type"] in rule.signal_types]
        grouped = _group_signals_for_rule(candidates, rule)
        for group_key, group_rows in grouped.items():
            for cluster in _cluster_signals(group_rows, rule.window_seconds):
                if len(cluster) < rule.min_signal_count:
                    continue
                signal_types = {str(row["signal_type"]) for row in cluster}
                if any(signal_type not in signal_types for signal_type in rule.required_signal_types):
                    continue
                event = _build_fused_event(game, rule, cluster, group_key, normalized_signals=normalized_signals)
                events.append(event)
                rule_matches.append(
                    {
                        "rule_id": rule.rule_id,
                        "event_id": event["event_id"],
                        "group_key": list(group_key),
                        "signal_count": len(cluster),
                        "signal_ids": [row["signal_id"] for row in cluster],
                        "gate_status": event["gate_status"],
                        "anchor_timestamp": event["anchor_timestamp"],
                        "dependent_signal_ids": event["dependent_signal_ids"],
                        "synergy_applied": event["synergy_applied"],
                        "synergy_multiplier": event["synergy_multiplier"],
                        "synergy_matches": event["synergy_matches"],
                        "suggested_start_timestamp": event["suggested_start_timestamp"],
                        "suggested_end_timestamp": event["suggested_end_timestamp"],
                    }
                )
    events.sort(key=lambda row: (float(row["start_timestamp"]), row["event_type"], row["event_id"]))
    rule_matches.sort(key=lambda row: (row["rule_id"], row["event_id"]))
    return events, rule_matches


def _group_signals_for_rule(signals: list[dict[str, Any]], rule: FusionRule) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for signal in signals:
        if rule.group_by:
            key = tuple(str(signal.get(field, "") or "") for field in rule.group_by)
        else:
            key = ("__all__",)
        grouped.setdefault(key, []).append(signal)
    for rows in grouped.values():
        rows.sort(key=lambda row: (float(row["start_timestamp"]), float(row["end_timestamp"]), row["signal_id"]))
    return grouped


def _cluster_signals(signals: list[dict[str, Any]], window_seconds: float) -> list[list[dict[str, Any]]]:
    if not signals:
        return []
    clusters: list[list[dict[str, Any]]] = [[signals[0]]]
    cluster_end = float(signals[0]["end_timestamp"])
    for signal in signals[1:]:
        signal_start = float(signal["start_timestamp"])
        if signal_start <= cluster_end + window_seconds:
            clusters[-1].append(signal)
            cluster_end = max(cluster_end, float(signal["end_timestamp"]))
            continue
        clusters.append([signal])
        cluster_end = float(signal["end_timestamp"])
    return clusters


def _build_fused_event(
    game: str,
    rule: FusionRule,
    cluster: list[dict[str, Any]],
    group_key: tuple[str, ...],
    *,
    normalized_signals: list[dict[str, Any]],
) -> dict[str, Any]:
    ordered_cluster = sorted(cluster, key=lambda row: (float(row["start_timestamp"]), row["signal_id"]))
    confidences = [float(row["confidence"]) for row in ordered_cluster]
    base_confidence = max(confidences) if rule.confidence_method == "max" else (sum(confidences) / len(confidences))
    extra_signal_count = max(0, len(ordered_cluster) - len(rule.required_signal_types))
    bonus_value = min(rule.max_bonus, extra_signal_count * rule.corroboration_bonus_per_extra_signal)
    low_confidence_count = sum(1 for confidence in confidences if confidence < rule.low_confidence_threshold)
    penalty_value = low_confidence_count * rule.low_confidence_penalty
    pre_gate_confidence = max(0.0, min(1.0, base_confidence + bonus_value - penalty_value))
    entropy = max(0.0, min(1.0, low_confidence_count / len(ordered_cluster)))
    start_timestamp = min(float(row["start_timestamp"]) for row in ordered_cluster)
    end_timestamp = max(float(row["end_timestamp"]) for row in ordered_cluster)
    anchor_timestamp = start_timestamp
    gate_state = _evaluate_temporal_gate(
        rule,
        ordered_cluster,
        normalized_signals,
        anchor_timestamp=anchor_timestamp,
    )
    post_gate_confidence = max(0.0, min(1.0, pre_gate_confidence * gate_state["multiplier_applied"]))
    synergy_state = _evaluate_synergy(rule, ordered_cluster, gate_state)
    final_confidence = max(0.0, min(1.0, post_gate_confidence * synergy_state["synergy_multiplier"]))
    event_id = _fused_event_id(game, rule.rule_id, start_timestamp, end_timestamp, ordered_cluster)
    penalties = []
    bonuses = []
    if low_confidence_count:
        penalties.append(
            {
                "type": "low_confidence_signals",
                "count": low_confidence_count,
                "value": round(penalty_value, 5),
                "threshold": rule.low_confidence_threshold,
            }
        )
    if bonus_value:
        bonuses.append(
            {
                "type": "corroboration_bonus",
                "extra_signal_count": extra_signal_count,
                "value": round(bonus_value, 5),
            }
        )
    metadata = dict(rule.metadata)
    metadata.update(_shared_signal_metadata(ordered_cluster))
    metadata["rule_id"] = rule.rule_id
    metadata["group_key"] = list(group_key)
    metadata["matched_signal_types"] = synergy_state["matched_signal_types"]
    if gate_state["dependent_signal_types"]:
        metadata["dependent_signal_types"] = gate_state["dependent_signal_types"]
    return {
        "event_id": event_id,
        "event_type": rule.event_type,
        "start_timestamp": round(start_timestamp, 5),
        "end_timestamp": round(end_timestamp, 5),
        "timestamp": round((start_timestamp + end_timestamp) / 2.0, 5),
        "base_confidence": round(pre_gate_confidence, 5),
        "post_gate_confidence": round(post_gate_confidence, 5),
        "confidence": round(final_confidence, 5),
        "final_score": round(final_confidence, 5),
        "entropy": round(entropy, 5),
        "gate_status": gate_state["gate_status"],
        "anchor_timestamp": round(anchor_timestamp, 5),
        "lag_window_seconds": round(gate_state["lag_window_seconds"], 5) if gate_state["lag_window_seconds"] is not None else None,
        "multiplier_applied": round(gate_state["multiplier_applied"], 5),
        "dependent_signal_ids": gate_state["dependent_signal_ids"],
        "dependent_signal_types": gate_state["dependent_signal_types"],
        "synergy_applied": synergy_state["synergy_applied"],
        "synergy_score": round(final_confidence - post_gate_confidence, 5),
        "synergy_multiplier": round(synergy_state["synergy_multiplier"], 5),
        "synergy_matches": synergy_state["synergy_matches"],
        "minimum_required_signals_met": synergy_state["minimum_required_signals_met"],
        "suggested_start_timestamp": round(gate_state["suggested_start_timestamp"], 5),
        "suggested_end_timestamp": round(gate_state["suggested_end_timestamp"], 5),
        "contributing_signals": [row["signal_id"] for row in ordered_cluster],
        "contributing_sources": sorted(
            {
                str(row.get("source_family") or row.get("producer_family") or "")
                for row in ordered_cluster
                if str(row.get("source_family") or row.get("producer_family") or "")
            }
        ),
        "penalties": penalties,
        "bonuses": bonuses,
        "metadata": metadata,
    }


def _evaluate_synergy(
    rule: FusionRule,
    cluster: list[dict[str, Any]],
    gate_state: dict[str, Any],
) -> dict[str, Any]:
    matched_signal_types = sorted(
        {str(row["signal_type"]) for row in cluster} | {str(value) for value in gate_state.get("dependent_signal_types", []) if str(value)}
    )
    matched_signal_type_set = set(matched_signal_types)
    minimum_required_signals_met = len(matched_signal_type_set) >= rule.synergy_minimum_required_signals
    if not rule.synergy_enabled:
        return {
            "synergy_applied": False,
            "synergy_multiplier": 1.0,
            "synergy_matches": [],
            "minimum_required_signals_met": minimum_required_signals_met,
            "matched_signal_types": matched_signal_types,
        }

    synergy_matches: list[dict[str, Any]] = []
    missing_required_signal_types: list[str] = []
    multiplier = 1.0
    for interaction in rule.synergy_interactions:
        matched = interaction.signal_type in matched_signal_type_set
        if interaction.required and not matched:
            missing_required_signal_types.append(interaction.signal_type)
        if matched:
            multiplier *= interaction.multiplier
            synergy_matches.append(
                {
                    "signal_type": interaction.signal_type,
                    "multiplier": round(interaction.multiplier, 5),
                    "required": interaction.required,
                    "label": interaction.label,
                    "metadata": interaction.metadata,
                }
            )
    if not minimum_required_signals_met or missing_required_signal_types or not synergy_matches:
        return {
            "synergy_applied": False,
            "synergy_multiplier": 1.0,
            "synergy_matches": synergy_matches,
            "minimum_required_signals_met": minimum_required_signals_met,
            "matched_signal_types": matched_signal_types,
        }
    return {
        "synergy_applied": True,
        "synergy_multiplier": max(1.0, min(MAX_SYNERGY_MULTIPLIER, multiplier)),
        "synergy_matches": synergy_matches,
        "minimum_required_signals_met": True,
        "matched_signal_types": matched_signal_types,
    }


def _evaluate_temporal_gate(
    rule: FusionRule,
    cluster: list[dict[str, Any]],
    normalized_signals: list[dict[str, Any]],
    *,
    anchor_timestamp: float,
) -> dict[str, Any]:
    default_start = max(0.0, anchor_timestamp)
    default_end = anchor_timestamp
    if not rule.anchor_signal_types or not rule.dependent_signal_types or rule.lag_window_seconds is None:
        return {
            "gate_status": "not_applicable",
            "lag_window_seconds": None,
            "multiplier_applied": 1.0,
            "dependent_signal_ids": [],
            "dependent_signal_types": [],
            "suggested_start_timestamp": default_start,
            "suggested_end_timestamp": default_end,
        }
    if rule.anchor_event_types and rule.event_type not in rule.anchor_event_types:
        return {
            "gate_status": "not_applicable",
            "lag_window_seconds": None,
            "multiplier_applied": 1.0,
            "dependent_signal_ids": [],
            "dependent_signal_types": [],
            "suggested_start_timestamp": default_start,
            "suggested_end_timestamp": default_end,
        }
    cluster_signal_ids = {str(row["signal_id"]) for row in cluster}
    dependent_rows = [
        row
        for row in normalized_signals
        if str(row.get("signal_id", "")) not in cluster_signal_ids
        and str(row.get("signal_type", "")) in rule.dependent_signal_types
        and float(row.get("timestamp", 0.0) or 0.0) >= anchor_timestamp
        and float(row.get("timestamp", 0.0) or 0.0) <= anchor_timestamp + rule.lag_window_seconds
    ]
    dependent_rows.sort(key=lambda row: (float(row.get("timestamp", 0.0) or 0.0), str(row.get("signal_id", ""))))
    if dependent_rows:
        latest_dependent_timestamp = max(float(row.get("timestamp", 0.0) or 0.0) for row in dependent_rows)
        return {
            "gate_status": "confirmed",
            "lag_window_seconds": rule.lag_window_seconds,
            "multiplier_applied": rule.confirm_multiplier,
            "dependent_signal_ids": [str(row["signal_id"]) for row in dependent_rows],
            "dependent_signal_types": sorted({str(row["signal_type"]) for row in dependent_rows}),
            "suggested_start_timestamp": max(0.0, anchor_timestamp - rule.clip_start_lead_seconds),
            "suggested_end_timestamp": latest_dependent_timestamp + rule.clip_end_lag_seconds,
        }
    return {
        "gate_status": "ambiguous",
        "lag_window_seconds": rule.lag_window_seconds,
        "multiplier_applied": rule.ambiguous_confidence_multiplier,
        "dependent_signal_ids": [],
        "dependent_signal_types": [],
        "suggested_start_timestamp": max(0.0, anchor_timestamp - rule.clip_start_lead_seconds),
        "suggested_end_timestamp": anchor_timestamp + rule.clip_end_lag_seconds,
    }


def _shared_signal_metadata(cluster: list[dict[str, Any]]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for field in ("entity_id", "ability_id", "equipment_id", "event_row_id", "asset_id", "roi_ref"):
        values = sorted({str(row.get(field, "")).strip() for row in cluster if str(row.get(field, "")).strip()})
        if len(values) == 1:
            metadata[field] = values[0]
        elif values:
            metadata[field] = values
    return metadata


def _fusion_summary(
    normalized_signals: list[dict[str, Any]],
    fused_events: list[dict[str, Any]],
    rules: list[FusionRule],
    *,
    game: str,
) -> dict[str, Any]:
    signals_by_producer_family: dict[str, int] = {}
    signals_by_type: dict[str, int] = {}
    events_by_type: dict[str, int] = {}
    gate_status_counts: dict[str, int] = {}
    synergy_rule_counts: dict[str, int] = {}
    synergy_multiplier_values: list[float] = []
    for row in normalized_signals:
        producer_family = str(row["producer_family"])
        signal_type = str(row["signal_type"])
        signals_by_producer_family[producer_family] = signals_by_producer_family.get(producer_family, 0) + 1
        signals_by_type[signal_type] = signals_by_type.get(signal_type, 0) + 1
    for row in fused_events:
        event_type = str(row["event_type"])
        events_by_type[event_type] = events_by_type.get(event_type, 0) + 1
        gate_status = str(row.get("gate_status", "not_applicable"))
        gate_status_counts[gate_status] = gate_status_counts.get(gate_status, 0) + 1
        if bool(row.get("synergy_applied", False)):
            rule_id = str(row.get("metadata", {}).get("rule_id", ""))
            if rule_id:
                synergy_rule_counts[rule_id] = synergy_rule_counts.get(rule_id, 0) + 1
            synergy_multiplier_values.append(float(row.get("synergy_multiplier", 1.0) or 1.0))
    return {
        "normalized_signal_count": len(normalized_signals),
        "fused_event_count": len(fused_events),
        "signals_by_producer_family": signals_by_producer_family,
        "signals_by_type": signals_by_type,
        "events_by_type": events_by_type,
        "gate_status_counts": gate_status_counts,
        "synergy_applied_count": len(synergy_multiplier_values),
        "synergy_rule_counts": synergy_rule_counts,
        "average_synergy_multiplier": round(sum(synergy_multiplier_values) / len(synergy_multiplier_values), 4)
        if synergy_multiplier_values
        else 1.0,
        "contract_summary": _fusion_contract_summary(game=game),
        "rule_count": len(rules),
        "rule_ids": [rule.rule_id for rule in rules],
    }


def _fusion_contract_summary(game: str) -> dict[str, Any]:
    if not game:
        return {"status": "unknown", "active_legacy_modes": []}
    try:
        validation = validate_published_pack(game)
    except Exception:
        return {"status": "missing", "active_legacy_modes": []}
    return {
        "status": validation.get("contract_status", "canonical"),
        "active_legacy_modes": validation.get("active_legacy_modes", []),
        "canonical_contracts": validation.get("canonical_contracts", {}),
        "ontology_version": validation.get("ontology_version"),
        "ontology_status": validation.get("ontology_status"),
    }


def _load_json_object(path: str | Path, *, status_prefix: str) -> dict[str, Any]:
    resolved = _resolve_path(path)
    if not resolved.exists():
        raise FusionAnalysisError(f"missing_{status_prefix}", f"{status_prefix.replace('_', ' ')} does not exist: {resolved}")
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FusionAnalysisError(f"invalid_{status_prefix}", f"failed to read {status_prefix.replace('_', ' ')}: {exc}") from exc
    if not isinstance(payload, dict):
        raise FusionAnalysisError(f"invalid_{status_prefix}", f"{status_prefix.replace('_', ' ')} must be a JSON object")
    return payload


def _validate_sidecar_game_and_source(
    payload: dict[str, Any],
    game: str,
    source: str | Path,
    *,
    sidecar_kind: str,
) -> None:
    payload_game = str(payload.get("game", "")).strip()
    payload_source = str(payload.get("source", "")).strip()
    if payload_game and payload_game != game:
        raise FusionAnalysisError(
            f"{sidecar_kind}_game_mismatch",
            f"{sidecar_kind} sidecar game '{payload_game}' does not match requested game '{game}'",
        )
    if payload_source and _normalized_source_ref(payload_source) != _normalized_source_ref(source):
        raise FusionAnalysisError(
            f"{sidecar_kind}_source_mismatch",
            f"{sidecar_kind} sidecar source '{payload_source}' does not match requested source '{source}'",
        )


def _resolve_path(path: str | Path) -> Path:
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (Path.cwd() / expanded).resolve()


def _normalized_source_ref(source: str | Path) -> str:
    source_text = str(source)
    if "://" in source_text:
        return source_text
    return str(_resolve_path(source_text))


def _normalized_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise FusionAnalysisError("invalid_fusion_rules", "expected a list of strings")
    return [str(item).strip() for item in value if str(item).strip()]


def _fused_analysis_path(source: str | Path, game: str, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    source_slug = _source_slug(source)
    source_hash = hashlib.sha1(str(source).encode("utf-8")).hexdigest()[:12]
    filename = f"{source_slug}-{source_hash}.fused_analysis.json"
    return DEFAULT_OUTPUT_ROOT / game / filename


def _source_slug(source: str | Path) -> str:
    source_text = str(source)
    stem = Path(source_text).stem
    if "://" in source_text:
        path_part = source_text.split("://", 1)[1].split("?", 1)[0]
        stem = Path(path_part).stem or stem
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", stem.lower()).strip("-")
    return slug or "fused"


def _fusion_id(game: str, source: str | Path) -> str:
    digest = hashlib.sha1(f"{game}\n{source}".encode("utf-8")).hexdigest()[:12]
    return f"{game}-fusion-{digest}"


def _fused_event_id(
    game: str,
    rule_id: str,
    start_timestamp: float,
    end_timestamp: float,
    cluster: list[dict[str, Any]],
) -> str:
    signal_ids = "\n".join(row["signal_id"] for row in cluster)
    digest = hashlib.sha1(f"{game}\n{rule_id}\n{start_timestamp}\n{end_timestamp}\n{signal_ids}".encode("utf-8")).hexdigest()[:12]
    return f"{rule_id}-{digest}"


def _write_debug_bundle(debug_root: Path, payload: dict[str, Any]) -> None:
    debug_root.mkdir(parents=True, exist_ok=True)
    _write_csv(debug_root / "normalized_signals.csv", list(payload.get("normalized_signals", [])))
    _write_csv(debug_root / "fused_events.csv", list(payload.get("fused_events", [])))
    (debug_root / "fusion_report.json").write_text(json.dumps(payload.get("fusion_summary", {}), indent=2), encoding="utf-8")
    (debug_root / "rule_matches.json").write_text(json.dumps(payload.get("rule_matches", []), indent=2), encoding="utf-8")
    gate_matches = [row for row in payload.get("rule_matches", []) if row.get("gate_status") != "not_applicable"]
    (debug_root / "gate_matches.json").write_text(json.dumps(gate_matches, indent=2), encoding="utf-8")
    synergy_matches = [row for row in payload.get("rule_matches", []) if row.get("synergy_applied")]
    (debug_root / "synergy_matches.json").write_text(json.dumps(synergy_matches, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                    for key, value in row.items()
                }
            )

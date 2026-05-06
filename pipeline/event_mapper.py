from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from pipeline.roi_matcher import (
    RoiMatcherError,
    RuntimeCvRule,
    load_published_runtime_pack,
    match_roi_templates,
    resolve_template_target_value,
)
from pipeline.simple_yaml import load_yaml_file


class EventMapperError(RuntimeError):
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
class AssetEventMetadata:
    asset_id: str
    asset_family: str
    display_name: str
    roi_ref: str
    signal_type: str | None = None
    target_value: str | None = None
    event_type: str | None = None
    target_field: str | None = None
    identity_competition: str | None = None
    collapse_strategy: str = "contiguous_cluster"
    cluster_gap_seconds: float | None = None
    event_timestamp_mode: str = "midpoint"


def map_roi_events(
    source: str | Path,
    game: str,
    *,
    matcher_report: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    runtime_rule_overrides: dict[str, dict[str, Any]] | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    if matcher_report is not None:
        matcher_result = load_matcher_report(matcher_report)
    else:
        matcher_result = match_roi_templates(
            source,
            game,
            sample_fps=sample_fps,
            limit_frames=limit_frames,
        )
    result = map_matcher_result(game, matcher_result, fallback_source=source, runtime_rule_overrides=runtime_rule_overrides)
    if output_path is not None:
        Path(output_path).write_text(json.dumps(result, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        write_event_debug_bundle(debug_output_dir, result)
    return result


def load_matcher_report(path: str | Path) -> dict[str, Any]:
    report_path = Path(path).expanduser().resolve()
    if not report_path.exists():
        raise EventMapperError("missing_matcher_report", f"matcher report does not exist: {report_path}")
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EventMapperError("invalid_matcher_report", f"failed to read matcher report: {exc}") from exc
    if not isinstance(payload, dict):
        raise EventMapperError("invalid_matcher_report", "matcher report must be a JSON object")
    return payload


def map_matcher_result(
    game: str,
    matcher_result: dict[str, Any],
    *,
    fallback_source: str | Path | None = None,
    runtime_rule_overrides: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not matcher_result.get("ok", False):
        raise EventMapperError(
            "matcher_failed",
            str(matcher_result.get("error") or matcher_result.get("status") or "matcher result was not successful"),
        )

    source = str(matcher_result.get("source") or fallback_source or "")
    metadata_by_asset = _load_asset_event_metadata(game, runtime_rule_overrides=runtime_rule_overrides)
    confirmed_rows = matcher_result.get("confirmed_detections", [])
    if not isinstance(confirmed_rows, list):
        raise EventMapperError("invalid_matcher_report", "matcher report must contain a list of confirmed_detections")

    signals = build_runtime_signals(
        game,
        confirmed_rows,
        metadata_by_asset,
        sample_fps=float(matcher_result.get("sample_fps", 0.0) or 0.0),
    )
    for signal in signals:
        signal["producer_family"] = "runtime"
        signal["source_ref"] = source
    event_candidates = _build_event_candidates(game, signals)
    events, identity_competition_drop_count = _resolve_identity_competition(event_candidates)
    events.sort(key=lambda row: (float(row["start_timestamp"]), row["event_type"], row["asset_id"]))

    status = "ok" if events else "no_events"
    result = {
        "ok": True,
        "status": status,
        "game": game,
        "source": source,
        "frame_count": int(matcher_result.get("frame_count", 0) or 0),
        "sample_fps": float(matcher_result.get("sample_fps", 0.0) or 0.0),
        "signal_count": len(signals),
        "signals": signals,
        "event_count": len(events),
        "events": events,
        "event_summary": _event_summary(events, identity_competition_drop_count=identity_competition_drop_count),
    }
    return result


def _load_asset_event_metadata(
    game: str,
    runtime_rule_overrides: dict[str, dict[str, Any]] | None = None,
) -> dict[str, AssetEventMetadata]:
    runtime_pack = _load_runtime_pack_for_events(game)
    runtime_rules = _apply_runtime_rule_overrides(runtime_pack.runtime_rules, runtime_rule_overrides)
    metadata_by_asset: dict[str, AssetEventMetadata] = {}
    for template in runtime_pack.templates:
        rule = runtime_rules.get(template.asset_family)
        target_value = resolve_template_target_value(game, template, rule) if rule is not None else None
        if rule is not None and rule.target_field and target_value is None:
            raise EventMapperError(
                "template_rule_target_mismatch",
                f"template '{template.asset_id}' could not resolve target field '{rule.target_field}'",
            )
        metadata_by_asset[template.asset_id] = AssetEventMetadata(
            asset_id=template.asset_id,
            asset_family=template.asset_family,
            display_name=template.display_name or "",
            roi_ref=template.roi_ref,
            signal_type=rule.signal_type if rule is not None else None,
            target_value=target_value,
            event_type=rule.event_type if rule is not None else None,
            target_field=rule.target_field if rule is not None else None,
            identity_competition=rule.identity_competition if rule is not None else None,
            collapse_strategy=rule.collapse_strategy if rule is not None else "contiguous_cluster",
            cluster_gap_seconds=rule.cluster_gap_seconds if rule is not None else None,
            event_timestamp_mode=rule.event_timestamp_mode if rule is not None else "midpoint",
        )
    return metadata_by_asset


def _load_runtime_pack_for_events(game: str):
    try:
        return load_published_runtime_pack(game)
    except RoiMatcherError as exc:
        raise EventMapperError(exc.status, exc.message) from exc


def load_runtime_rule_trial_overrides(path: str | Path) -> dict[str, dict[str, Any]]:
    trial_path = Path(path).expanduser()
    if not trial_path.is_absolute():
        trial_path = (Path.cwd() / trial_path).resolve()
    else:
        trial_path = trial_path.resolve()
    if not trial_path.exists() or not trial_path.is_file():
        raise EventMapperError("invalid_runtime_rule_trial", f"runtime rule trial path does not exist or is not a file: {trial_path}")
    try:
        if trial_path.suffix.lower() == ".json":
            payload = json.loads(trial_path.read_text(encoding="utf-8"))
        else:
            payload = load_yaml_file(trial_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise EventMapperError("invalid_runtime_rule_trial", f"failed to load runtime rule trial file: {exc}") from exc
    if not isinstance(payload, dict):
        raise EventMapperError("invalid_runtime_rule_trial", "runtime rule trial file must be a mapping")
    raw_overrides = payload.get("event_mappings", payload)
    if not isinstance(raw_overrides, dict):
        raise EventMapperError(
            "invalid_runtime_rule_trial",
            "runtime rule trial file must contain a mapping of asset_family to override rows",
        )
    overrides: dict[str, dict[str, Any]] = {}
    for asset_family, row in raw_overrides.items():
        normalized_family = str(asset_family).strip()
        if normalized_family in {"trial_name", "name"} and not isinstance(row, dict):
            continue
        if not normalized_family:
            raise EventMapperError("invalid_runtime_rule_trial", "runtime rule trial rows must use non-empty asset_family keys")
        if not isinstance(row, dict):
            raise EventMapperError("invalid_runtime_rule_trial", f"runtime rule trial row for '{normalized_family}' must be an object")
        override: dict[str, Any] = {}
        for key in row:
            if key not in {"collapse_strategy", "identity_competition", "cluster_gap_seconds", "event_timestamp_mode"}:
                raise EventMapperError(
                    "invalid_runtime_rule_trial",
                    f"runtime rule trial row for '{normalized_family}' uses unsupported field '{key}'",
                )
        if "collapse_strategy" in row:
            collapse_strategy = str(row["collapse_strategy"]).strip() or "contiguous_cluster"
            if collapse_strategy not in {"contiguous_cluster", "strict_cluster", "per_detection"}:
                raise EventMapperError(
                    "invalid_runtime_rule_trial",
                    f"runtime rule trial row for '{normalized_family}' uses unsupported collapse_strategy '{collapse_strategy}'",
                )
            override["collapse_strategy"] = collapse_strategy
        if "identity_competition" in row:
            identity_competition = str(row["identity_competition"]).strip() or None
            if identity_competition not in {None, "strongest_overlap"}:
                raise EventMapperError(
                    "invalid_runtime_rule_trial",
                    f"runtime rule trial row for '{normalized_family}' uses unsupported identity_competition '{identity_competition}'",
                )
            override["identity_competition"] = identity_competition
        if "cluster_gap_seconds" in row:
            override["cluster_gap_seconds"] = float(row["cluster_gap_seconds"])
        if "event_timestamp_mode" in row:
            event_timestamp_mode = str(row["event_timestamp_mode"]).strip() or "midpoint"
            if event_timestamp_mode not in {"midpoint", "start", "end"}:
                raise EventMapperError(
                    "invalid_runtime_rule_trial",
                    f"runtime rule trial row for '{normalized_family}' uses unsupported event_timestamp_mode '{event_timestamp_mode}'",
                )
            override["event_timestamp_mode"] = event_timestamp_mode
        if not override:
            raise EventMapperError(
                "invalid_runtime_rule_trial",
                f"runtime rule trial row for '{normalized_family}' must override at least one runtime mapping field",
            )
        overrides[normalized_family] = override
    if not overrides:
        raise EventMapperError("invalid_runtime_rule_trial", "runtime rule trial file must contain at least one override row")
    return overrides


def _apply_runtime_rule_overrides(
    runtime_rules: dict[str, RuntimeCvRule],
    runtime_rule_overrides: dict[str, dict[str, Any]] | None,
) -> dict[str, RuntimeCvRule]:
    if not runtime_rule_overrides:
        return runtime_rules
    unknown_families = sorted(asset_family for asset_family in runtime_rule_overrides if asset_family not in runtime_rules)
    if unknown_families:
        raise EventMapperError(
            "invalid_runtime_rule_trial",
            f"runtime rule trial overrides reference unknown asset_family values: {unknown_families}",
        )
    overridden: dict[str, RuntimeCvRule] = {}
    for asset_family, rule in runtime_rules.items():
        row = runtime_rule_overrides.get(asset_family)
        if row is None:
            overridden[asset_family] = rule
            continue
        overridden[asset_family] = replace(
            rule,
            collapse_strategy=str(row.get("collapse_strategy", rule.collapse_strategy)),
            identity_competition=row.get("identity_competition", rule.identity_competition),
            cluster_gap_seconds=float(row["cluster_gap_seconds"]) if row.get("cluster_gap_seconds") is not None else rule.cluster_gap_seconds,
            event_timestamp_mode=str(row.get("event_timestamp_mode", rule.event_timestamp_mode)),
        )
    return overridden


def build_runtime_signals(
    game: str,
    confirmed_rows: list[dict[str, Any]],
    metadata_by_asset: dict[str, AssetEventMetadata],
    *,
    sample_fps: float,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in confirmed_rows:
        asset_id = str(row.get("asset_id", "")).strip()
        roi_ref = str(row.get("roi_ref", "")).strip()
        if not asset_id or not roi_ref:
            continue
        grouped.setdefault((asset_id, roi_ref), []).append(row)

    candidates: list[dict[str, Any]] = []
    for (asset_id, roi_ref), rows in grouped.items():
        metadata = metadata_by_asset.get(asset_id)
        if metadata is None or not metadata.signal_type:
            continue
        merged_rows = _collapse_confirmed_rows(
            rows,
            sample_fps=sample_fps,
            collapse_strategy=metadata.collapse_strategy,
            cluster_gap_seconds=metadata.cluster_gap_seconds,
        )
        for cluster in merged_rows:
            confidence = round(float(cluster["peak_score"]), 5)
            signal_id = _signal_id(game, metadata.signal_type, asset_id, cluster["start_timestamp"], cluster["end_timestamp"])
            timestamp = _cluster_timestamp(
                float(cluster["start_timestamp"]),
                float(cluster["end_timestamp"]),
                mode=metadata.event_timestamp_mode,
            )
            signal_row = {
                "signal_id": signal_id,
                "signal_type": metadata.signal_type,
                "event_type": metadata.event_type,
                "timestamp": round(timestamp, 5),
                "start_timestamp": round(float(cluster["start_timestamp"]), 5),
                "end_timestamp": round(float(cluster["end_timestamp"]), 5),
                "asset_id": asset_id,
                "asset_family": metadata.asset_family,
                "roi_ref": roi_ref,
                "confidence": confidence,
                "evidence": {
                    "peak_score": confidence,
                    "supporting_frames": int(cluster["supporting_frames"]),
                    "temporal_window": int(cluster["temporal_window"]),
                    "source_detection_count": int(cluster["source_detection_count"]),
                },
                "source_detection_count": int(cluster["source_detection_count"]),
                "producer": "runtime_cv_template_matcher",
            }
            if metadata.target_field:
                signal_row[metadata.target_field] = metadata.target_value
            if metadata.identity_competition:
                signal_row["_identity_competition"] = metadata.identity_competition
            if metadata.display_name:
                signal_row["display_name"] = metadata.display_name
            candidates.append(signal_row)
    return candidates


def _build_event_candidates(game: str, signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for signal in signals:
        event_type = str(signal.get("event_type") or "").strip()
        if not event_type:
            asset_id = str(signal.get("asset_id", ""))
            raise EventMapperError("invalid_runtime_signal", f"signal is missing event_type for asset '{asset_id}'")
        event_id = _event_id(
            game,
            event_type,
            str(signal["asset_id"]),
            float(signal["start_timestamp"]),
            float(signal["end_timestamp"]),
        )
        event_row = {
            "event_id": event_id,
            "event_type": event_type,
            "timestamp": signal["timestamp"],
            "start_timestamp": signal["start_timestamp"],
            "end_timestamp": signal["end_timestamp"],
            "asset_id": signal["asset_id"],
            "asset_family": signal["asset_family"],
            "roi_ref": signal["roi_ref"],
            "confidence": signal["confidence"],
            "signal_id": signal["signal_id"],
            "signal_type": signal["signal_type"],
            "evidence": signal["evidence"],
            "source_detection_count": signal["source_detection_count"],
            "producer": signal["producer"],
            "producer_family": signal.get("producer_family"),
            "source_ref": signal.get("source_ref"),
        }
        for field in ("entity_id", "ability_id", "equipment_id", "event_row_id", "display_name", "_identity_competition"):
            if field in signal:
                event_row[field] = signal[field]
        candidates.append(event_row)
    return candidates


def _collapse_confirmed_rows(
    rows: list[dict[str, Any]],
    *,
    sample_fps: float,
    collapse_strategy: str,
    cluster_gap_seconds: float | None,
) -> list[dict[str, Any]]:
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("first_timestamp", 0.0)), float(row.get("last_timestamp", 0.0))))
    collapsed: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for row in sorted_rows:
        first_timestamp = float(row.get("first_timestamp", 0.0))
        last_timestamp = float(row.get("last_timestamp", first_timestamp))
        temporal_window = max(1, int(row.get("temporal_window", 1)))
        if collapse_strategy == "per_detection":
            gap_seconds = -1.0
        elif cluster_gap_seconds is not None:
            gap_seconds = float(cluster_gap_seconds)
        elif collapse_strategy == "strict_cluster":
            gap_seconds = 0.0
        else:
            gap_seconds = (temporal_window / sample_fps) if sample_fps > 0 else 0.5
        if current is None:
            current = {
                "start_timestamp": first_timestamp,
                "end_timestamp": last_timestamp,
                "peak_score": float(row.get("peak_score", 0.0)),
                "supporting_frames": int(row.get("supporting_frames", 0)),
                "temporal_window": temporal_window,
                "source_detection_count": int(row.get("supporting_frames", 0)),
            }
            continue
        if first_timestamp <= float(current["end_timestamp"]) + gap_seconds:
            current["end_timestamp"] = max(float(current["end_timestamp"]), last_timestamp)
            current["peak_score"] = max(float(current["peak_score"]), float(row.get("peak_score", 0.0)))
            current["supporting_frames"] = int(current["supporting_frames"]) + int(row.get("supporting_frames", 0))
            current["source_detection_count"] = int(current["source_detection_count"]) + int(row.get("supporting_frames", 0))
            current["temporal_window"] = max(int(current["temporal_window"]), temporal_window)
            continue
        collapsed.append(current)
        current = {
            "start_timestamp": first_timestamp,
            "end_timestamp": last_timestamp,
            "peak_score": float(row.get("peak_score", 0.0)),
            "supporting_frames": int(row.get("supporting_frames", 0)),
            "temporal_window": temporal_window,
            "source_detection_count": int(row.get("supporting_frames", 0)),
        }
    if current is not None:
        collapsed.append(current)
    return collapsed


def _resolve_identity_competition(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    identity_events = [row for row in events if str(row.get("_identity_competition", "")) == "strongest_overlap"]
    non_identity_events = [row for row in events if str(row.get("_identity_competition", "")) != "strongest_overlap"]
    if not identity_events:
        for row in events:
            row.pop("_identity_competition", None)
        return events, 0

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in identity_events:
        grouped.setdefault(str(row["roi_ref"]), []).append(row)

    kept_identity: list[dict[str, Any]] = []
    dropped_identity = 0
    for roi_ref, rows in grouped.items():
        del roi_ref
        ordered = sorted(rows, key=lambda row: (float(row["start_timestamp"]), -float(row["confidence"])))
        active: list[dict[str, Any]] = []
        for row in ordered:
            overlap = next(
                (
                    existing for existing in active
                    if float(row["start_timestamp"]) <= float(existing["end_timestamp"])
                    and float(row["end_timestamp"]) >= float(existing["start_timestamp"])
                ),
                None,
            )
            if overlap is None:
                active.append(row)
                continue
            existing_strength = (float(overlap["confidence"]), int(overlap["source_detection_count"]))
            candidate_strength = (float(row["confidence"]), int(row["source_detection_count"]))
            if candidate_strength > existing_strength:
                active.remove(overlap)
                active.append(row)
                dropped_identity += 1
                continue
            dropped_identity += 1
        kept_identity.extend(active)
    resolved = non_identity_events + kept_identity
    for row in resolved:
        row.pop("_identity_competition", None)
    return resolved, dropped_identity


def _event_summary(events: list[dict[str, Any]], *, identity_competition_drop_count: int) -> dict[str, Any]:
    counts_by_type: dict[str, int] = {}
    counts_by_roi: dict[str, int] = {}
    counts_by_asset: dict[str, int] = {}
    for row in events:
        counts_by_type[row["event_type"]] = counts_by_type.get(row["event_type"], 0) + 1
        counts_by_roi[row["roi_ref"]] = counts_by_roi.get(row["roi_ref"], 0) + 1
        counts_by_asset[row["asset_id"]] = counts_by_asset.get(row["asset_id"], 0) + 1
    return {
        "counts_by_event_type": counts_by_type,
        "counts_by_roi": counts_by_roi,
        "counts_by_asset": counts_by_asset,
        "identity_competition_drop_count": identity_competition_drop_count,
    }


def _cluster_timestamp(start_timestamp: float, end_timestamp: float, *, mode: str) -> float:
    if mode == "start":
        return start_timestamp
    if mode == "end":
        return end_timestamp
    return (start_timestamp + end_timestamp) / 2.0


def _target_id_from_asset_id(game: str, asset_id: str, asset_family: str) -> str | None:
    prefix = f"{game}."
    suffix = f".{asset_family}"
    if asset_id.startswith(prefix) and asset_id.endswith(suffix):
        target = asset_id[len(prefix) : len(asset_id) - len(suffix)]
        return target or None
    return None


def _signal_id(game: str, signal_type: str, asset_id: str, start_timestamp: float, end_timestamp: float) -> str:
    digest = hashlib.sha1(
        f"{game}\n{signal_type}\n{asset_id}\n{start_timestamp:.5f}\n{end_timestamp:.5f}".encode("utf-8")
    ).hexdigest()[:10]
    return f"{game}.{signal_type}.{digest}"


def _event_id(game: str, event_type: str, asset_id: str, start_timestamp: float, end_timestamp: float) -> str:
    digest = hashlib.sha1(f"{game}\n{event_type}\n{asset_id}\n{start_timestamp:.5f}\n{end_timestamp:.5f}".encode("utf-8")).hexdigest()[:10]
    return f"{game}.{event_type}.{digest}"


def write_event_debug_bundle(debug_output_dir: str | Path, result: dict[str, Any]) -> None:
    debug_root = Path(debug_output_dir)
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "event_report.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (debug_root / "event_summary.json").write_text(json.dumps(result.get("event_summary", {}), indent=2), encoding="utf-8")
    _write_csv(debug_root / "signals.csv", result.get("signals", []))
    _write_csv(debug_root / "events.csv", result.get("events", []))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    headers = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        if not rows:
            writer.writerow({"empty": ""})
            return
        for row in rows:
            writer.writerow({key: json.dumps(value) if isinstance(value, (list, dict)) else value for key, value in row.items()})

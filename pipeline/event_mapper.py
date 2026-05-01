from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.game_pack import load_game_pack
from pipeline.roi_matcher import RoiMatcherError, match_roi_templates


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


_EVENT_TYPE_BY_ASSET_FAMILY = {
    "hero_portrait": "pov_character_identified",
    "medal_icon": "medal_seen",
    "ability_icon": "ability_seen",
    "equipment_icon": "ability_seen",
}
_IDENTITY_EVENT_TYPES = {"pov_character_identified"}


def map_roi_events(
    source: str | Path,
    game: str,
    *,
    matcher_report: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
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
    result = map_matcher_result(game, matcher_result, fallback_source=source)
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


def map_matcher_result(game: str, matcher_result: dict[str, Any], *, fallback_source: str | Path | None = None) -> dict[str, Any]:
    if not matcher_result.get("ok", False):
        raise EventMapperError(
            "matcher_failed",
            str(matcher_result.get("error") or matcher_result.get("status") or "matcher result was not successful"),
        )

    source = str(matcher_result.get("source") or fallback_source or "")
    metadata_by_asset = _load_asset_event_metadata(game)
    confirmed_rows = matcher_result.get("confirmed_detections", [])
    if not isinstance(confirmed_rows, list):
        raise EventMapperError("invalid_matcher_report", "matcher report must contain a list of confirmed_detections")

    event_candidates = _build_event_candidates(
        game,
        confirmed_rows,
        metadata_by_asset,
        sample_fps=float(matcher_result.get("sample_fps", 0.0) or 0.0),
    )
    events = _resolve_identity_competition(event_candidates)
    events.sort(key=lambda row: (float(row["start_timestamp"]), row["event_type"], row["asset_id"]))

    status = "ok" if events else "no_events"
    result = {
        "ok": True,
        "status": status,
        "game": game,
        "source": source,
        "frame_count": int(matcher_result.get("frame_count", 0) or 0),
        "sample_fps": float(matcher_result.get("sample_fps", 0.0) or 0.0),
        "event_count": len(events),
        "events": events,
        "event_summary": _event_summary(events),
    }
    return result


def _load_asset_event_metadata(game: str) -> dict[str, AssetEventMetadata]:
    game_pack = load_game_pack(game)
    if game_pack.pack_format != "published":
        raise EventMapperError("published_pack_required", f"game pack '{game}' is not a published runtime pack")
    template_rows = game_pack.files.get("manifests/cv_templates.yaml", {}).get("templates", [])
    if not isinstance(template_rows, list):
        raise EventMapperError("invalid_template_manifest", "cv_templates.yaml must define a top-level 'templates' list")
    metadata_by_asset: dict[str, AssetEventMetadata] = {}
    for row in template_rows:
        if not isinstance(row, dict):
            continue
        asset_id = str(row.get("asset_id", "")).strip()
        if not asset_id:
            continue
        metadata_by_asset[asset_id] = AssetEventMetadata(
            asset_id=asset_id,
            asset_family=str(row.get("asset_family", "")).strip(),
            display_name=str(row.get("display_name", "")).strip(),
            roi_ref=str(row.get("roi_ref", "")).strip(),
        )
    return metadata_by_asset


def _build_event_candidates(
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
        asset_family = metadata.asset_family if metadata is not None else ""
        event_type = _EVENT_TYPE_BY_ASSET_FAMILY.get(asset_family)
        if event_type is None:
            continue
        merged_rows = _collapse_confirmed_rows(rows, sample_fps=sample_fps)
        for cluster in merged_rows:
            confidence = round(float(cluster["peak_score"]), 5)
            event_id = _event_id(game, event_type, asset_id, cluster["start_timestamp"], cluster["end_timestamp"])
            event_row = {
                "event_id": event_id,
                "event_type": event_type,
                "timestamp": round((float(cluster["start_timestamp"]) + float(cluster["end_timestamp"])) / 2.0, 5),
                "start_timestamp": round(float(cluster["start_timestamp"]), 5),
                "end_timestamp": round(float(cluster["end_timestamp"]), 5),
                "asset_id": asset_id,
                "roi_ref": roi_ref,
                "confidence": confidence,
                "evidence": {
                    "peak_score": confidence,
                    "supporting_frames": int(cluster["supporting_frames"]),
                    "temporal_window": int(cluster["temporal_window"]),
                    "source_detection_count": int(cluster["source_detection_count"]),
                },
                "source_detection_count": int(cluster["source_detection_count"]),
            }
            target_id = _target_id_from_asset_id(game, asset_id, asset_family)
            if event_type == "pov_character_identified":
                event_row["entity_id"] = target_id
            elif event_type == "ability_seen":
                event_row["ability_id"] = target_id
            elif event_type == "medal_seen":
                event_row["event_row_id"] = target_id
            if metadata is not None and metadata.display_name:
                event_row["display_name"] = metadata.display_name
            candidates.append(event_row)
    return candidates


def _collapse_confirmed_rows(rows: list[dict[str, Any]], *, sample_fps: float) -> list[dict[str, Any]]:
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("first_timestamp", 0.0)), float(row.get("last_timestamp", 0.0))))
    collapsed: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for row in sorted_rows:
        first_timestamp = float(row.get("first_timestamp", 0.0))
        last_timestamp = float(row.get("last_timestamp", first_timestamp))
        temporal_window = max(1, int(row.get("temporal_window", 1)))
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


def _resolve_identity_competition(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    identity_events = [row for row in events if row["event_type"] in _IDENTITY_EVENT_TYPES]
    non_identity_events = [row for row in events if row["event_type"] not in _IDENTITY_EVENT_TYPES]
    if not identity_events:
        return events

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in identity_events:
        grouped.setdefault(str(row["roi_ref"]), []).append(row)

    kept_identity: list[dict[str, Any]] = []
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
        kept_identity.extend(active)
    return non_identity_events + kept_identity


def _event_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
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
    }


def _target_id_from_asset_id(game: str, asset_id: str, asset_family: str) -> str | None:
    prefix = f"{game}."
    suffix = f".{asset_family}"
    if asset_id.startswith(prefix) and asset_id.endswith(suffix):
        target = asset_id[len(prefix) : len(asset_id) - len(suffix)]
        return target or None
    return None


def _event_id(game: str, event_type: str, asset_id: str, start_timestamp: float, end_timestamp: float) -> str:
    digest = hashlib.sha1(f"{game}\n{event_type}\n{asset_id}\n{start_timestamp:.5f}\n{end_timestamp:.5f}".encode("utf-8")).hexdigest()[:10]
    return f"{game}.{event_type}.{digest}"


def write_event_debug_bundle(debug_output_dir: str | Path, result: dict[str, Any]) -> None:
    debug_root = Path(debug_output_dir)
    debug_root.mkdir(parents=True, exist_ok=True)
    (debug_root / "event_report.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (debug_root / "event_summary.json").write_text(json.dumps(result.get("event_summary", {}), indent=2), encoding="utf-8")
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

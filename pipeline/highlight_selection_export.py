from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from pipeline.fused_export import DEFAULT_ACTION_THRESHOLDS


REPO_ROOT = Path(__file__).resolve().parent.parent
HIGHLIGHT_SELECTION_SCHEMA_VERSION = "highlight_selection_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "highlight_selection_exports"
SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"


def export_highlight_selection(
    proxy_sidecar: str | Path | None = None,
    *,
    fused_sidecar: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    if fused_sidecar is not None:
        return _export_from_fused_sidecar(fused_sidecar, output_path=output_path)
    if proxy_sidecar is not None:
        return _export_from_proxy_sidecar(proxy_sidecar, output_path=output_path)
    return {
        "ok": False,
        "status": "missing_sidecar",
        "error": "one of proxy_sidecar or fused_sidecar is required",
    }


def _export_from_proxy_sidecar(
    proxy_sidecar: str | Path,
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    sidecar_path = _resolve_path(proxy_sidecar)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SUPPORTED_PROXY_SCAN_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_proxy_sidecar",
            "proxy_sidecar_path": str(sidecar_path),
            "error": "proxy sidecar does not use proxy_scan_v1",
        }

    game = str(payload.get("game", "")).strip() or "unknown_game"
    source = str(payload.get("source", "")).strip()
    selected_highlights = _selected_proxy_windows(payload)
    manifest = {
        "schema_version": HIGHLIGHT_SELECTION_SCHEMA_VERSION,
        "game": game,
        "source": source,
        "selection_basis": "proxy",
        "proxy_sidecar_path": str(sidecar_path),
        "selected_highlight_count": len(selected_highlights),
        "selected_highlights": selected_highlights,
    }
    return _write_manifest(manifest, output_path=output_path)


def _export_from_fused_sidecar(
    fused_sidecar: str | Path,
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    sidecar_path = _resolve_path(fused_sidecar)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_fused_sidecar",
            "fused_sidecar_path": str(sidecar_path),
            "error": "fused sidecar does not use fused_analysis_v1",
        }

    game = str(payload.get("game", "")).strip() or "unknown_game"
    source = str(payload.get("source", "")).strip()
    selected_highlights = _selected_fused_events(payload, sidecar_path=sidecar_path)
    manifest = {
        "schema_version": HIGHLIGHT_SELECTION_SCHEMA_VERSION,
        "game": game,
        "source": source,
        "selection_basis": "fused",
        "fused_sidecar_path": str(sidecar_path),
        "selected_highlight_count": len(selected_highlights),
        "selected_highlights": selected_highlights,
    }
    return _write_manifest(manifest, output_path=output_path)


def _write_manifest(manifest: dict[str, Any], *, output_path: str | Path | None) -> dict[str, Any]:
    game = str(manifest.get("game", "")).strip() or "unknown_game"
    source = str(manifest.get("source", "")).strip()
    target = _resolve_path(output_path) if output_path is not None else _default_output_path(game, source)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    otio_path = target.with_suffix(".otio.json")
    otio_payload = _otio_skeleton(manifest)
    otio_path.write_text(json.dumps(otio_payload, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "status": "ok",
        "schema_version": HIGHLIGHT_SELECTION_SCHEMA_VERSION,
        "selection_basis": manifest.get("selection_basis"),
        "manifest_path": str(target),
        "otio_skeleton_path": str(otio_path),
        "selected_highlight_count": len(list(manifest.get("selected_highlights", []))),
    }


def _selected_proxy_windows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, window in enumerate(list(payload.get("windows", []))):
        if not isinstance(window, dict):
            continue
        action = str(window.get("recommended_action", "skip")).strip() or "skip"
        if action == "skip":
            continue
        rows.append(
            {
                "highlight_id": f"highlight-{index}",
                "start_seconds": round(float(window.get("start_seconds", 0.0)), 4),
                "end_seconds": round(float(window.get("end_seconds", 0.0)), 4),
                "proxy_score": round(float(window.get("proxy_score", 0.0)), 4),
                "recommended_action": action,
                "source_families": [str(item) for item in list(window.get("source_families", []))],
                "sources": [str(item) for item in list(window.get("sources", []))],
                "signal_count": int(window.get("signal_count", 0) or 0),
            }
        )
    rows.sort(key=lambda row: (-float(row["proxy_score"]), float(row["start_seconds"])))
    return rows


def _selected_fused_events(payload: dict[str, Any], *, sidecar_path: Path) -> list[dict[str, Any]]:
    source = str(payload.get("source", "")).strip()
    game = str(payload.get("game", "")).strip()
    fusion_id = str(payload.get("fusion_id", "")).strip() or None
    normalized_signal_lookup = {
        str(row.get("signal_id") or "").strip(): row
        for row in list(payload.get("normalized_signals", []))
        if isinstance(row, dict) and str(row.get("signal_id") or "").strip()
    }
    rows: list[dict[str, Any]] = []
    for index, event in enumerate(list(payload.get("fused_events", []))):
        if not isinstance(event, dict):
            continue
        final_score = float(event.get("final_score", event.get("confidence", 0.0)) or 0.0)
        recommended_action = _recommended_fused_action(final_score)
        if recommended_action == "skip":
            continue
        event_id = str(event.get("event_id", "")).strip()
        if not event_id:
            event_id = f"fused-event-{index}"
        metadata = event.get("metadata", {}) if isinstance(event.get("metadata"), dict) else {}
        start_seconds = round(float(event.get("suggested_start_timestamp", event.get("start_timestamp", 0.0)) or 0.0), 4)
        end_seconds = round(
            max(
                start_seconds,
                float(event.get("suggested_end_timestamp", event.get("end_timestamp", start_seconds)) or start_seconds),
            ),
            4,
        )
        contributing_signal_ids = [str(value) for value in list(event.get("contributing_signals", [])) if str(value).strip()]
        contributing_producer_families = sorted(
            {
                str(normalized_signal_lookup.get(signal_id, {}).get("producer_family") or "").strip()
                for signal_id in contributing_signal_ids
                if str(normalized_signal_lookup.get(signal_id, {}).get("producer_family") or "").strip()
            }
        )
        rows.append(
            {
                "highlight_id": f"highlight-{index}",
                "candidate_id": _candidate_id(
                    game=game,
                    source=source,
                    fused_sidecar_path=str(sidecar_path),
                    event_id=event_id,
                ),
                "fusion_id": fusion_id,
                "event_id": event_id,
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "final_score": round(final_score, 4),
                "recommended_action": recommended_action,
                "gate_status": str(event.get("gate_status", "")).strip() or None,
                "contributing_signal_ids": contributing_signal_ids,
                "contributing_producer_families": contributing_producer_families,
                "event_type": str(event.get("event_type", "")).strip() or None,
                "entity_id": str(metadata.get("entity_id", "")).strip() or None,
                "metadata_summary": _metadata_summary(metadata),
            }
        )
    rows.sort(key=lambda row: (-float(row["final_score"]), float(row["start_seconds"])))
    return rows


def _candidate_id(*, game: str, source: str, fused_sidecar_path: str, event_id: str) -> str:
    digest = hashlib.sha1(
        "::".join([game.strip(), source.strip(), fused_sidecar_path.strip(), event_id.strip()]).encode("utf-8")
    ).hexdigest()[:16]
    return f"candidate-{digest}"


def _recommended_fused_action(final_score: float) -> str:
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["highlight_candidate"]):
        return "highlight_candidate"
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["inspect"]):
        return "inspect"
    return "skip"


def _metadata_summary(metadata: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in ("entity_id", "ability_id", "equipment_id", "matched_signal_types"):
        value = metadata.get(key)
        if value in (None, "", [], {}):
            continue
        summary[key] = value
    return summary


def _otio_skeleton(manifest: dict[str, Any]) -> dict[str, Any]:
    selection_basis = str(manifest.get("selection_basis") or "proxy")
    children = []
    for row in list(manifest.get("selected_highlights", [])):
        start_seconds = float(row.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
        metadata = {
            "recommended_action": row.get("recommended_action"),
            "selection_basis": selection_basis,
        }
        if selection_basis == "proxy":
            metadata["proxy_score"] = row.get("proxy_score")
            metadata["source_families"] = row.get("source_families", [])
        else:
            metadata["candidate_id"] = row.get("candidate_id")
            metadata["fusion_id"] = row.get("fusion_id")
            metadata["event_id"] = row.get("event_id")
            metadata["final_score"] = row.get("final_score")
            metadata["gate_status"] = row.get("gate_status")
            metadata["contributing_signal_ids"] = row.get("contributing_signal_ids", [])
            metadata["contributing_producer_families"] = row.get("contributing_producer_families", [])
            metadata["event_type"] = row.get("event_type")
        children.append(
            {
                "OTIO_SCHEMA": "Clip.2",
                "name": str(row.get("highlight_id")),
                "source_range": {
                    "OTIO_SCHEMA": "TimeRange.1",
                    "start_time": {"OTIO_SCHEMA": "RationalTime.1", "value": start_seconds, "rate": 1},
                    "duration": {"OTIO_SCHEMA": "RationalTime.1", "value": round(end_seconds - start_seconds, 4), "rate": 1},
                },
                "metadata": metadata,
            }
        )
    timeline_metadata = {
        "schema_version": HIGHLIGHT_SELECTION_SCHEMA_VERSION,
        "game": manifest.get("game"),
        "selection_basis": selection_basis,
        "proxy_sidecar_path": manifest.get("proxy_sidecar_path"),
        "fused_sidecar_path": manifest.get("fused_sidecar_path"),
    }
    return {
        "OTIO_SCHEMA": "Timeline.1",
        "name": Path(str(manifest.get("source", ""))).name or "highlight-selection",
        "metadata": timeline_metadata,
        "tracks": {
            "OTIO_SCHEMA": "Stack.1",
            "children": [
                {
                    "OTIO_SCHEMA": "Track.1",
                    "name": "highlights",
                    "kind": "Video",
                    "children": children,
                }
            ],
        },
    }


def _default_output_path(game: str, source: str) -> Path:
    stem = Path(source).stem or "highlight-selection"
    safe_stem = "".join(char if char.isalnum() else "-" for char in stem.lower()).strip("-") or "highlight-selection"
    return DEFAULT_OUTPUT_ROOT / game / f"{safe_stem}.highlight_selection.json"


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path

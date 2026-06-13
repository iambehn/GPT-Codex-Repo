from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from pipeline.editorial_replay_contract import fused_editorial_object_id
from pipeline.fused_export import DEFAULT_ACTION_THRESHOLDS


REPO_ROOT = Path(__file__).resolve().parent.parent
HIGHLIGHT_SELECTION_SCHEMA_VERSION = "highlight_selection_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "highlight_selection_exports"
SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
CONTEXT_EXPANSION_POLICY = "signal_aware_bounded_v1"
CONTEXT_EXPANSION_CAPS = {
    "call_of_duty": {"pre_roll_seconds": 1.5, "post_roll_seconds": 2.0},
    "marvel_rivals": {"pre_roll_seconds": 2.0, "post_roll_seconds": 2.5},
}
FALLBACK_PRE_ROLL_SECONDS = 0.5
FALLBACK_POST_ROLL_SECONDS = 0.75


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


def load_selected_highlight_details(
    selection_manifest: str | Path | None,
    *,
    candidate_id: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any] | None:
    if selection_manifest is None:
        return None
    path = _resolve_path(selection_manifest)
    if not path.exists() or not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    for row in list(payload.get("selected_highlights", [])):
        if not isinstance(row, dict):
            continue
        if candidate_id and str(row.get("candidate_id") or "").strip() == str(candidate_id).strip():
            return row
        if event_id and str(row.get("event_id") or "").strip() == str(event_id).strip():
            return row
    return None


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
    proxy_shape_error = _proxy_shape_error(payload)
    if proxy_shape_error is not None:
        return {
            "ok": False,
            "status": "invalid_proxy_sidecar",
            "proxy_sidecar_path": str(sidecar_path),
            "error": proxy_shape_error,
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
    fused_shape_error = _fused_shape_error(payload)
    if fused_shape_error is not None:
        return {
            "ok": False,
            "status": "invalid_fused_sidecar",
            "fused_sidecar_path": str(sidecar_path),
            "error": fused_shape_error,
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


def _proxy_shape_error(payload: dict[str, Any]) -> str | None:
    windows = payload.get("windows", [])
    if not isinstance(windows, list):
        return "proxy sidecar windows must be a list"
    for index, window in enumerate(windows):
        if not isinstance(window, dict):
            return f"proxy sidecar windows[{index}] must be an object"
        sources = window.get("sources", [])
        source_families = window.get("source_families", [])
        if not isinstance(sources, list):
            return f"proxy sidecar windows[{index}].sources must be a list"
        if not isinstance(source_families, list):
            return f"proxy sidecar windows[{index}].source_families must be a list"
    return None


def _fused_shape_error(payload: dict[str, Any]) -> str | None:
    normalized_signals = payload.get("normalized_signals", [])
    fused_events = payload.get("fused_events", [])
    if not isinstance(normalized_signals, list):
        return "fused sidecar normalized_signals must be a list"
    if not isinstance(fused_events, list):
        return "fused sidecar fused_events must be a list"
    for index, row in enumerate(normalized_signals):
        if not isinstance(row, dict):
            return f"fused sidecar normalized_signals[{index}] must be an object"
    for index, event in enumerate(fused_events):
        if not isinstance(event, dict):
            return f"fused sidecar fused_events[{index}] must be an object"
        metadata = event.get("metadata", {})
        contributing_signals = event.get("contributing_signals", [])
        if metadata is not None and not isinstance(metadata, dict):
            return f"fused sidecar fused_events[{index}].metadata must be an object when present"
        if not isinstance(contributing_signals, list):
            return f"fused sidecar fused_events[{index}].contributing_signals must be a list"
    return None


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
        context_details = _derive_context_window(
            game=game,
            event=event,
            metadata=metadata,
            normalized_signal_lookup=normalized_signal_lookup,
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
                "editorial_object_id": fused_editorial_object_id(
                    game=game,
                    source=source,
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
                **context_details,
            }
        )
    rows.sort(key=lambda row: (-float(row["final_score"]), float(row["start_seconds"])))
    return rows


def _derive_context_window(
    *,
    game: str,
    event: dict[str, Any],
    metadata: dict[str, Any],
    normalized_signal_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    anchor_start = round(float(event.get("suggested_start_timestamp", event.get("start_timestamp", 0.0)) or 0.0), 4)
    anchor_end = round(
        max(
            anchor_start,
            float(event.get("suggested_end_timestamp", event.get("end_timestamp", anchor_start)) or anchor_start),
        ),
        4,
    )
    caps = CONTEXT_EXPANSION_CAPS.get(game)
    if caps is None:
        return _context_window_payload(
            anchor_start=anchor_start,
            anchor_end=anchor_end,
            context_start=anchor_start,
            context_end=anchor_end,
            reasons=[],
            overlapping_signal_ids=[],
            pre_signal_types=[],
            post_signal_types=[],
            policy=None,
        )

    pre_cap = float(caps["pre_roll_seconds"])
    post_cap = float(caps["post_roll_seconds"])
    contributing_signal_ids = [str(value) for value in list(event.get("contributing_signals", [])) if str(value).strip()]
    normalized_rows = [row for row in normalized_signal_lookup.values() if isinstance(row, dict)]
    preferred_pre = _preferred_pre_signal_types(metadata)
    preferred_post = _preferred_post_signal_types(event_type=str(event.get("event_type") or "").strip())

    pre_candidates: list[dict[str, Any]] = []
    post_candidates: list[dict[str, Any]] = []
    for row in normalized_rows:
        signal_start = _signal_start(row)
        signal_end = _signal_end(row)
        signal_type = str(row.get("signal_type") or "").strip()
        if not signal_type:
            continue
        signal_id = str(row.get("signal_id") or "").strip()
        if signal_start < anchor_start and signal_end <= anchor_start + 1e-6 and anchor_start - signal_end <= pre_cap + 1e-6:
            pre_candidates.append(row)
        elif signal_end > anchor_start and signal_start < anchor_start and anchor_start - signal_start <= pre_cap + 1e-6:
            pre_candidates.append(row)
        if signal_end > anchor_end and signal_start <= anchor_end + post_cap + 1e-6 and signal_start >= anchor_end - 1e-6:
            post_candidates.append(row)
        elif signal_start < anchor_end and signal_end > anchor_end and signal_end - anchor_end <= post_cap + 1e-6:
            post_candidates.append(row)
        elif signal_id in contributing_signal_ids and signal_end > anchor_end:
            post_candidates.append(row)

    selected_pre = _select_context_signals(pre_candidates, preferred_types=preferred_pre, anchor="pre")
    selected_post = _select_context_signals(post_candidates, preferred_types=preferred_post, anchor="post")
    context_start = anchor_start
    context_end = anchor_end
    reasons: list[str] = []
    pre_signal_types = sorted({str(row.get("signal_type") or "").strip() for row in selected_pre if str(row.get("signal_type") or "").strip()})
    post_signal_types = sorted({str(row.get("signal_type") or "").strip() for row in selected_post if str(row.get("signal_type") or "").strip()})

    if selected_pre:
        target_start = min(_signal_start(row) for row in selected_pre)
        context_start = round(max(0.0, anchor_start - pre_cap, target_start), 4)
        reasons.extend([f"pre_signal:{signal_type}" for signal_type in pre_signal_types])
    elif anchor_start > 0.0:
        context_start = round(max(0.0, anchor_start - min(FALLBACK_PRE_ROLL_SECONDS, pre_cap)), 4)
        if context_start < anchor_start:
            reasons.append("fallback_pre_pad")

    if selected_post:
        target_end = max(_signal_end(row) for row in selected_post)
        context_end = round(min(anchor_end + post_cap, target_end), 4)
        reasons.extend([f"post_signal:{signal_type}" for signal_type in post_signal_types])
    else:
        context_end = round(anchor_end + min(FALLBACK_POST_ROLL_SECONDS, post_cap), 4)
        if context_end > anchor_end:
            reasons.append("fallback_post_pad")

    if context_end < context_start:
        context_end = context_start

    overlapping_signal_ids = sorted(
        {
            str(row.get("signal_id") or "").strip()
            for row in normalized_rows
            if str(row.get("signal_id") or "").strip()
            and _signal_overlaps_window(row, start_seconds=context_start, end_seconds=context_end)
        }
    )
    return _context_window_payload(
        anchor_start=anchor_start,
        anchor_end=anchor_end,
        context_start=context_start,
        context_end=context_end,
        reasons=reasons,
        overlapping_signal_ids=overlapping_signal_ids,
        pre_signal_types=pre_signal_types,
        post_signal_types=post_signal_types,
        policy=CONTEXT_EXPANSION_POLICY,
    )


def _context_window_payload(
    *,
    anchor_start: float,
    anchor_end: float,
    context_start: float,
    context_end: float,
    reasons: list[str],
    overlapping_signal_ids: list[str],
    pre_signal_types: list[str],
    post_signal_types: list[str],
    policy: str | None,
) -> dict[str, Any]:
    anchor_duration = max(0.0, anchor_end - anchor_start)
    context_duration = max(0.0, context_end - context_start)
    return {
        "anchor_start_seconds": round(anchor_start, 4),
        "anchor_end_seconds": round(anchor_end, 4),
        "context_start_seconds": round(context_start, 4),
        "context_end_seconds": round(context_end, 4),
        "context_expansion_seconds": round(max(0.0, context_duration - anchor_duration), 4),
        "context_expansion_policy": policy,
        "context_expansion_reasons": reasons,
        "context_signal_count": len(overlapping_signal_ids),
        "context_pre_signal_types": pre_signal_types,
        "context_post_signal_types": post_signal_types,
        "context_signal_ids": overlapping_signal_ids,
    }


def _preferred_pre_signal_types(metadata: dict[str, Any]) -> set[str]:
    preferred = {"character_identity", "ability_activation", "ability_visibility", "round_state_visibility"}
    if str(metadata.get("entity_id") or "").strip():
        preferred.add("character_identity")
    if str(metadata.get("ability_id") or "").strip():
        preferred.update({"ability_activation", "ability_visibility"})
    return preferred


def _preferred_post_signal_types(*, event_type: str) -> set[str]:
    preferred = {"team_wipe_visibility", "multikill", "reaction", "round_state_visibility"}
    text = event_type.lower()
    if "team_wipe" in text:
        preferred.add("team_wipe_visibility")
    if "combo" in text or "medal" in text:
        preferred.add("multikill")
    return preferred


def _select_context_signals(rows: list[dict[str, Any]], *, preferred_types: set[str], anchor: str) -> list[dict[str, Any]]:
    if not rows:
        return []

    def _score(row: dict[str, Any]) -> tuple[int, float, float]:
        signal_type = str(row.get("signal_type") or "").strip()
        confidence = float(row.get("confidence", row.get("strength", 0.0)) or 0.0)
        span = max(0.0, _signal_end(row) - _signal_start(row))
        preferred = 1 if signal_type in preferred_types else 0
        return (preferred, confidence, span)

    ordered = sorted(rows, key=_score, reverse=True)
    # Keep the set intentionally small to avoid turning bounded context into reselection.
    return ordered[:2]


def _signal_start(row: dict[str, Any]) -> float:
    return float(row.get("start_timestamp", row.get("timestamp", 0.0)) or 0.0)


def _signal_end(row: dict[str, Any]) -> float:
    start = _signal_start(row)
    end = float(row.get("end_timestamp", row.get("timestamp", start)) or start)
    return max(start, end)


def _signal_overlaps_window(row: dict[str, Any], *, start_seconds: float, end_seconds: float) -> bool:
    signal_start = _signal_start(row)
    signal_end = _signal_end(row)
    return signal_end >= start_seconds and signal_start <= end_seconds


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

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
REPLAY_VIEWER_SCHEMA_VERSION = "replay_viewer_v1"
SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "replay_viewers"


def render_replay_viewer(
    runtime_sidecar: str | Path,
    *,
    fused_sidecar: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    runtime_path = _resolve_path(runtime_sidecar)
    runtime_payload = _load_json(runtime_path)
    if runtime_payload.get("schema_version") != SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_runtime_sidecar",
            "runtime_sidecar_path": str(runtime_path),
            "error": "runtime sidecar does not use runtime_analysis_v1",
        }

    fused_payload: dict[str, Any] | None = None
    fused_path: Path | None = None
    warnings: list[dict[str, Any]] = []
    if fused_sidecar is not None:
        fused_path = _resolve_path(fused_sidecar)
        fused_payload = _load_json(fused_path)
        if fused_payload.get("schema_version") != SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION:
            return {
                "ok": False,
                "status": "invalid_fused_sidecar",
                "runtime_sidecar_path": str(runtime_path),
                "fused_sidecar_path": str(fused_path),
                "error": "fused sidecar does not use fused_analysis_v1",
            }
        mismatch = _sidecar_mismatch(runtime_payload, fused_payload)
        if mismatch is not None:
            return {
                "ok": False,
                "status": "mismatched_sidecars",
                "runtime_sidecar_path": str(runtime_path),
                "fused_sidecar_path": str(fused_path),
                "error": mismatch,
            }

    viewer_path = _viewer_output_path(runtime_payload, runtime_path, output_path)
    media_path = _resolve_media_path(runtime_payload.get("source"))
    media_exists = bool(media_path and media_path.exists() and media_path.is_file())
    if media_path is not None and not media_exists:
        warnings.append(
            {
                "status": "missing_media_source",
                "path": str(media_path),
                "message": "runtime source media path does not exist locally; viewer will render without inline media playback",
            }
        )

    derived = _derived_payload(runtime_payload, fused_payload, runtime_path, fused_path, media_path, media_exists)
    html_text = _render_html(runtime_payload, fused_payload, derived)

    viewer_path.parent.mkdir(parents=True, exist_ok=True)
    viewer_path.write_text(html_text, encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": REPLAY_VIEWER_SCHEMA_VERSION,
        "viewer_path": str(viewer_path),
        "runtime_sidecar_path": str(runtime_path),
        "fused_sidecar_path": str(fused_path) if fused_path is not None else None,
        "media_path": str(media_path) if media_path is not None else None,
        "media_embed_available": media_exists,
        "timeline_entry_count": len(derived["timeline"]),
        "runtime_signal_count": derived["runtime_signal_count"],
        "runtime_event_count": derived["runtime_event_count"],
        "fused_event_count": derived["fused_event_count"],
        "interactive_section_count": 4,
        "fused_group_count": len(derived["fused_groups"]),
        "linked_runtime_event_count": derived["linked_runtime_event_count"],
        "linked_detection_count": derived["linked_detection_count"],
        "warnings": warnings,
    }


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sidecar_mismatch(runtime_payload: dict[str, Any], fused_payload: dict[str, Any]) -> str | None:
    runtime_game = str(runtime_payload.get("game", "")).strip()
    fused_game = str(fused_payload.get("game", "")).strip()
    if runtime_game and fused_game and runtime_game != fused_game:
        return "runtime and fused sidecars refer to different games"

    runtime_source = str(runtime_payload.get("source", "")).strip()
    fused_source = str(fused_payload.get("source", "")).strip()
    if runtime_source and fused_source and runtime_source != fused_source:
        return "runtime and fused sidecars refer to different sources"
    return None


def _viewer_output_path(runtime_payload: dict[str, Any], runtime_path: Path, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)

    game = str(runtime_payload.get("game") or "unknown_game").strip() or "unknown_game"
    analysis_id = str(runtime_payload.get("analysis_id") or runtime_path.stem).strip() or runtime_path.stem
    stem = f"{_slug(analysis_id)}.replay_view.html"
    return DEFAULT_OUTPUT_ROOT / game / stem


def _slug(value: str) -> str:
    lowered = value.lower()
    return "".join(char if char.isalnum() else "-" for char in lowered).strip("-") or "replay-view"


def _resolve_media_path(value: Any) -> Path | None:
    source_text = str(value or "").strip()
    if not source_text:
        return None
    path = Path(source_text).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _derived_payload(
    runtime_payload: dict[str, Any],
    fused_payload: dict[str, Any] | None,
    runtime_path: Path,
    fused_path: Path | None,
    media_path: Path | None,
    media_exists: bool,
) -> dict[str, Any]:
    matcher = runtime_payload.get("matcher", {}) if isinstance(runtime_payload.get("matcher"), dict) else {}
    events = runtime_payload.get("events", {}) if isinstance(runtime_payload.get("events"), dict) else {}
    detections = list(matcher.get("confirmed_detections", []))
    runtime_signals = list(matcher.get("signals", []))
    runtime_events = list(events.get("rows", []))

    fused_events: list[dict[str, Any]] = []
    normalized_signals: list[dict[str, Any]] = []
    fused_review = {}
    if isinstance(fused_payload, dict):
        fused_events = list(fused_payload.get("fused_events", []))
        normalized_signals = list(fused_payload.get("normalized_signals", []))
        fused_review = fused_payload.get("fused_review", {}) if isinstance(fused_payload.get("fused_review"), dict) else {}

    detection_rows = [_decorate_detection(index, row) for index, row in enumerate(detections)]
    runtime_signal_rows = [_decorate_runtime_signal(index, row) for index, row in enumerate(runtime_signals)]
    runtime_event_rows = [_decorate_runtime_event(index, row) for index, row in enumerate(runtime_events)]
    normalized_signal_rows = [_decorate_normalized_signal(index, row) for index, row in enumerate(normalized_signals)]
    fused_event_rows = [_decorate_fused_event(index, row, fused_review) for index, row in enumerate(fused_events)]

    runtime_signals_by_id = {row["signal_id"]: row for row in runtime_signal_rows if row.get("signal_id")}
    normalized_signals_by_id = {row["signal_id"]: row for row in normalized_signal_rows if row.get("signal_id")}

    fused_groups: list[dict[str, Any]] = []
    linked_runtime_event_count = 0
    linked_detection_count = 0
    for row in fused_event_rows:
        metadata = row.get("metadata", {}) if isinstance(row.get("metadata"), dict) else {}
        contributing_ids = list(row.get("contributing_signals", []))
        linked_runtime_signals = [runtime_signals_by_id[item] for item in contributing_ids if item in runtime_signals_by_id]
        linked_normalized_signals = [normalized_signals_by_id[item] for item in contributing_ids if item in normalized_signals_by_id]
        linked_runtime_events = [
            event_row
            for event_row in runtime_event_rows
            if _rows_overlap(row["start_timestamp"], row["end_timestamp"], event_row["start_timestamp"], event_row["end_timestamp"])
            and _semantic_overlap(metadata, event_row)
        ]
        linked_detections = [
            detection_row
            for detection_row in detection_rows
            if _rows_overlap(row["start_timestamp"], row["end_timestamp"], detection_row["start_timestamp"], detection_row["end_timestamp"])
            and _detection_overlap(metadata, detection_row, linked_runtime_signals)
        ]
        linked_runtime_event_count += len(linked_runtime_events)
        linked_detection_count += len(linked_detections)
        fused_groups.append(
            {
                "group_id": f"fused-group-{row['event_id'] or row['row_id']}",
                "event": row,
                "linked_runtime_signals": linked_runtime_signals,
                "linked_normalized_signals": linked_normalized_signals,
                "linked_runtime_events": linked_runtime_events,
                "linked_detections": linked_detections,
                "score_summary": _score_summary(row),
                "semantic_target_summary": _semantic_target_summary(metadata),
                "recommended_jump_timestamp": row["jump_timestamp"],
            }
        )
    fused_groups.sort(key=lambda item: (-float(item["event"].get("final_score", item["event"].get("confidence", 0.0)) or 0.0), str(item["event"].get("event_type") or "")))

    timeline = _build_timeline(detection_rows, runtime_signal_rows, runtime_event_rows, fused_event_rows)
    raw_sections = [
        {"title": "Confirmed Detections", "rows": detection_rows, "section_id": "raw-detections"},
        {"title": "Runtime Signals", "rows": runtime_signal_rows, "section_id": "raw-runtime-signals"},
        {"title": "Runtime Events", "rows": runtime_event_rows, "section_id": "raw-runtime-events"},
        {"title": "Fused Events", "rows": fused_event_rows, "section_id": "raw-fused-events"},
        {"title": "Fused Normalized Signals", "rows": normalized_signal_rows, "section_id": "raw-fused-signals"},
    ]

    return {
        "runtime_sidecar_path": str(runtime_path),
        "fused_sidecar_path": str(fused_path) if fused_path is not None else None,
        "media_path": str(media_path) if media_path is not None else None,
        "media_uri": media_path.as_uri() if media_exists and media_path is not None else None,
        "media_exists": media_exists,
        "runtime_review": runtime_payload.get("runtime_review", {}) if isinstance(runtime_payload.get("runtime_review"), dict) else {},
        "fused_review": fused_review,
        "runtime_signal_count": len(runtime_signal_rows),
        "runtime_event_count": len(runtime_event_rows),
        "fused_event_count": len(fused_event_rows),
        "normalized_signal_count": len(normalized_signal_rows),
        "timeline": timeline,
        "fused_groups": fused_groups,
        "linked_runtime_event_count": linked_runtime_event_count,
        "linked_detection_count": linked_detection_count,
        "raw_sections": raw_sections,
        "runtime_sidecar_json": runtime_payload,
        "fused_sidecar_json": fused_payload or {},
    }


def _decorate_detection(index: int, row: dict[str, Any]) -> dict[str, Any]:
    start = float(row.get("first_timestamp", row.get("timestamp", 0.0)) or 0.0)
    end = float(row.get("last_timestamp", start) or start)
    return {
        **row,
        "row_id": f"detection-{index}",
        "kind": "detection",
        "start_timestamp": round(start, 5),
        "end_timestamp": round(end, 5),
        "jump_timestamp": round(start, 5),
        "asset_id": row.get("asset_id"),
        "roi_ref": row.get("roi_ref"),
        "score": row.get("peak_score"),
        "label": str(row.get("asset_id") or "detection"),
        "search_text": " ".join(
            str(value)
            for value in (
                row.get("asset_id"),
                row.get("roi_ref"),
                row.get("entity_id"),
                row.get("ability_id"),
                row.get("equipment_id"),
            )
            if value is not None
        ).lower(),
    }


def _decorate_runtime_signal(index: int, row: dict[str, Any]) -> dict[str, Any]:
    start = float(row.get("start_timestamp", row.get("timestamp", 0.0)) or 0.0)
    end = float(row.get("end_timestamp", start) or start)
    return {
        **row,
        "row_id": f"runtime-signal-{index}",
        "kind": "runtime_signal",
        "start_timestamp": round(start, 5),
        "end_timestamp": round(end, 5),
        "jump_timestamp": round(start, 5),
        "score": row.get("confidence"),
        "label": str(row.get("signal_type") or row.get("signal_id") or "runtime_signal"),
        "search_text": " ".join(
            str(value)
            for value in (
                row.get("signal_id"),
                row.get("signal_type"),
                row.get("asset_id"),
                row.get("roi_ref"),
                row.get("entity_id"),
                row.get("ability_id"),
                row.get("equipment_id"),
                row.get("event_row_id"),
            )
            if value is not None
        ).lower(),
    }


def _decorate_runtime_event(index: int, row: dict[str, Any]) -> dict[str, Any]:
    start = float(row.get("start_timestamp", row.get("timestamp", 0.0)) or 0.0)
    end = float(row.get("end_timestamp", start) or start)
    return {
        **row,
        "row_id": f"runtime-event-{index}",
        "kind": "runtime_event",
        "start_timestamp": round(start, 5),
        "end_timestamp": round(end, 5),
        "jump_timestamp": round(start, 5),
        "score": row.get("confidence"),
        "label": str(row.get("event_type") or row.get("event_id") or "runtime_event"),
        "search_text": " ".join(
            str(value)
            for value in (
                row.get("event_id"),
                row.get("event_type"),
                row.get("entity_id"),
                row.get("ability_id"),
                row.get("equipment_id"),
                row.get("event_row_id"),
            )
            if value is not None
        ).lower(),
    }


def _decorate_normalized_signal(index: int, row: dict[str, Any]) -> dict[str, Any]:
    start = float(row.get("start_timestamp", row.get("timestamp", 0.0)) or 0.0)
    end = float(row.get("end_timestamp", start) or start)
    return {
        **row,
        "row_id": f"normalized-signal-{index}",
        "kind": "normalized_signal",
        "start_timestamp": round(start, 5),
        "end_timestamp": round(end, 5),
        "jump_timestamp": round(start, 5),
        "score": row.get("confidence", row.get("strength")),
        "label": str(row.get("signal_type") or row.get("signal_id") or "normalized_signal"),
        "search_text": " ".join(
            str(value)
            for value in (
                row.get("signal_id"),
                row.get("signal_type"),
                row.get("producer_family"),
                row.get("asset_id"),
                row.get("roi_ref"),
            )
            if value is not None
        ).lower(),
    }


def _decorate_fused_event(index: int, row: dict[str, Any], fused_review: dict[str, Any]) -> dict[str, Any]:
    start = float(row.get("start_timestamp", 0.0) or 0.0)
    end = float(row.get("end_timestamp", start) or start)
    jump_timestamp = float(row.get("suggested_start_timestamp", start) or start)
    metadata = row.get("metadata", {}) if isinstance(row.get("metadata"), dict) else {}
    event_id = str(row.get("event_id") or f"fused-event-{index}")
    event_review = {}
    if isinstance(fused_review.get("events"), dict):
        event_review = fused_review["events"].get(event_id, {}) if isinstance(fused_review["events"].get(event_id), dict) else {}
    return {
        **row,
        "row_id": f"fused-event-{index}",
        "event_id": event_id,
        "kind": "fused_event",
        "start_timestamp": round(start, 5),
        "end_timestamp": round(end, 5),
        "jump_timestamp": round(jump_timestamp, 5),
        "segment_duration_seconds": round(max(0.0, float(row.get("suggested_end_timestamp", end) or end) - jump_timestamp), 5),
        "score": row.get("final_score", row.get("confidence")),
        "label": str(row.get("event_type") or event_id),
        "review": event_review,
        "metadata": metadata,
        "search_text": " ".join(
            str(value)
            for value in (
                event_id,
                row.get("event_type"),
                row.get("gate_status"),
                metadata.get("entity_id"),
                metadata.get("ability_id"),
                metadata.get("equipment_id"),
                metadata.get("event_row_id"),
            )
            if value is not None
        ).lower(),
    }


def _rows_overlap(start_a: float, end_a: float, start_b: float, end_b: float) -> bool:
    return max(start_a, start_b) <= min(end_a, end_b) + 1e-6


def _semantic_overlap(metadata: dict[str, Any], runtime_event: dict[str, Any]) -> bool:
    keys = ("entity_id", "ability_id", "equipment_id", "event_row_id")
    for key in keys:
        left = metadata.get(key)
        right = runtime_event.get(key)
        if left and right and left == right:
            return True
    return not any(metadata.get(key) for key in keys)


def _detection_overlap(
    metadata: dict[str, Any],
    detection_row: dict[str, Any],
    runtime_signals: list[dict[str, Any]],
) -> bool:
    asset_id = detection_row.get("asset_id")
    if asset_id and any(signal.get("asset_id") == asset_id for signal in runtime_signals):
        return True
    roi_ref = detection_row.get("roi_ref")
    if roi_ref and any(signal.get("roi_ref") == roi_ref for signal in runtime_signals):
        return True
    for key in ("entity_id", "ability_id", "equipment_id"):
        left = metadata.get(key)
        right = detection_row.get(key)
        if left and right and left == right:
            return True
    return not runtime_signals


def _score_summary(row: dict[str, Any]) -> str:
    final_score = row.get("final_score")
    confidence = row.get("confidence")
    synergy_multiplier = row.get("synergy_multiplier")
    parts = []
    if final_score is not None:
        parts.append(f"final={float(final_score):.3f}")
    if confidence is not None:
        parts.append(f"base={float(confidence):.3f}")
    if synergy_multiplier is not None:
        parts.append(f"synergy_x={float(synergy_multiplier):.3f}")
    gate_status = row.get("gate_status")
    if gate_status:
        parts.append(f"gate={gate_status}")
    return " | ".join(parts)


def _semantic_target_summary(metadata: dict[str, Any]) -> str:
    pairs = [
        ("entity", metadata.get("entity_id")),
        ("ability", metadata.get("ability_id")),
        ("equipment", metadata.get("equipment_id")),
        ("event_row", metadata.get("event_row_id")),
    ]
    rendered = [f"{label}:{value}" for label, value in pairs if value]
    return ", ".join(rendered) if rendered else "none"


def _build_timeline(
    detections: list[dict[str, Any]],
    runtime_signals: list[dict[str, Any]],
    runtime_events: list[dict[str, Any]],
    fused_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for row in detections:
        entries.append(
            {
                "entry_id": row["row_id"],
                "target_id": row["row_id"],
                "kind": "detection",
                "label": str(row.get("asset_id") or "detection"),
                "start_timestamp": row["start_timestamp"],
                "end_timestamp": row["end_timestamp"],
                "jump_timestamp": row["jump_timestamp"],
                "score": row.get("peak_score"),
                "detail": str(row.get("roi_ref") or ""),
                "search_text": row["search_text"],
            }
        )
    for row in runtime_signals:
        entries.append(
            {
                "entry_id": row["row_id"],
                "target_id": row["row_id"],
                "kind": "runtime_signal",
                "label": str(row.get("signal_type") or "runtime_signal"),
                "start_timestamp": row["start_timestamp"],
                "end_timestamp": row["end_timestamp"],
                "jump_timestamp": row["jump_timestamp"],
                "score": row.get("confidence"),
                "detail": str(row.get("signal_id") or ""),
                "search_text": row["search_text"],
            }
        )
    for row in runtime_events:
        entries.append(
            {
                "entry_id": row["row_id"],
                "target_id": row["row_id"],
                "kind": "runtime_event",
                "label": str(row.get("event_type") or "runtime_event"),
                "start_timestamp": row["start_timestamp"],
                "end_timestamp": row["end_timestamp"],
                "jump_timestamp": row["jump_timestamp"],
                "score": row.get("confidence"),
                "detail": str(row.get("event_id") or ""),
                "search_text": row["search_text"],
            }
        )
    for row in fused_events:
        entries.append(
            {
                "entry_id": row["row_id"],
                "target_id": f"card-{row['event_id']}",
                "kind": "fused_event",
                "label": str(row.get("event_type") or "fused_event"),
                "start_timestamp": round(float(row.get("suggested_start_timestamp", row["start_timestamp"]) or row["start_timestamp"]), 5),
                "end_timestamp": round(float(row.get("suggested_end_timestamp", row["end_timestamp"]) or row["end_timestamp"]), 5),
                "jump_timestamp": row["jump_timestamp"],
                "score": row.get("final_score", row.get("confidence")),
                "detail": str(row.get("gate_status") or ""),
                "search_text": row["search_text"],
            }
        )
    entries.sort(key=lambda row: (float(row.get("start_timestamp", 0.0) or 0.0), str(row.get("kind", "")), str(row.get("label", ""))))
    return entries


def _render_html(runtime_payload: dict[str, Any], fused_payload: dict[str, Any] | None, derived: dict[str, Any]) -> str:
    game = html.escape(str(runtime_payload.get("game") or "unknown_game"))
    source = html.escape(str(runtime_payload.get("source") or ""))
    title = f"Replay Viewer - {game}"
    media_html = ""
    if derived.get("media_uri"):
        media_html = (
            "<section>"
            "<h2>Media</h2>"
            f'<video id="viewer-media" controls preload="metadata" src="{html.escape(str(derived["media_uri"]))}" '
            'style="width: 100%; max-height: 480px; background: #111;"></video>'
            "</section>"
        )

    sections = [
        _summary_cards(runtime_payload, fused_payload, derived),
        media_html,
        _controls_section(),
        _fused_event_explorer(derived["fused_groups"], derived.get("media_exists", False)),
        _timeline_section(derived["timeline"]),
        _raw_sections(derived["raw_sections"]),
        _json_section("Runtime Review Metadata", derived.get("runtime_review", {}), open_by_default=False),
        _json_section("Fused Review Metadata", derived.get("fused_review", {}), open_by_default=False),
        _json_section("Runtime Sidecar", derived["runtime_sidecar_json"], open_by_default=False),
        _json_section("Fused Sidecar", derived["fused_sidecar_json"], open_by_default=False),
    ]

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      color-scheme: light dark;
      --bg: #0f172a;
      --panel: #111827;
      --panel-2: #1f2937;
      --panel-3: #0b1220;
      --text: #e5e7eb;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --border: #334155;
      --good: #22c55e;
      --warn: #f59e0b;
      --bad: #ef4444;
    }}
    body {{
      margin: 0;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      background: linear-gradient(180deg, #020617 0%, #0f172a 100%);
      color: var(--text);
    }}
    main {{
      max-width: 1500px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1, h2, h3 {{
      margin: 0 0 12px 0;
    }}
    section {{
      background: rgba(17, 24, 39, 0.92);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 18px;
      margin-bottom: 18px;
      box-shadow: 0 8px 30px rgba(0,0,0,0.18);
    }}
    .meta {{
      color: var(--muted);
      margin: 8px 0 0 0;
      word-break: break-word;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .card {{
      background: rgba(30, 41, 59, 0.9);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
    }}
    .card-label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .card-value {{
      font-size: 24px;
      margin-top: 6px;
      color: var(--accent);
    }}
    .controls {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      align-items: end;
    }}
    .control-block label {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .control-input {{
      width: 100%;
      box-sizing: border-box;
      border: 1px solid var(--border);
      background: var(--panel-3);
      color: var(--text);
      border-radius: 10px;
      padding: 10px 12px;
      font: inherit;
    }}
    .checkbox-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .checkbox-pill {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 6px 10px;
      background: rgba(15, 23, 42, 0.8);
      color: var(--muted);
    }}
    .explorer {{
      display: grid;
      gap: 14px;
    }}
    .event-card {{
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      background: rgba(15, 23, 42, 0.88);
    }}
    .event-card-header {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      flex-wrap: wrap;
    }}
    .event-card h3 {{
      margin-bottom: 6px;
    }}
    .muted {{
      color: var(--muted);
    }}
    .pill {{
      display: inline-block;
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 3px 9px;
      margin-right: 6px;
      margin-bottom: 6px;
      color: var(--muted);
      background: rgba(15, 23, 42, 0.8);
    }}
    .pill.kind-fused_event {{ border-color: var(--accent); color: var(--accent); }}
    .pill.kind-runtime_event {{ border-color: var(--good); color: var(--good); }}
    .pill.kind-runtime_signal {{ border-color: var(--warn); color: var(--warn); }}
    .pill.kind-detection {{ border-color: #c084fc; color: #c084fc; }}
    .pill.review-approved {{ border-color: var(--good); color: var(--good); }}
    .pill.review-rejected {{ border-color: var(--bad); color: var(--bad); }}
    .pill.gate-confirmed {{ border-color: var(--good); color: var(--good); }}
    .pill.gate-ambiguous {{ border-color: var(--warn); color: var(--warn); }}
    .pill.gate-not_applicable {{ border-color: var(--muted); color: var(--muted); }}
    .event-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 10px;
      margin-top: 12px;
    }}
    .metric {{
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px;
      background: rgba(30, 41, 59, 0.75);
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
    }}
    .metric-value {{
      font-size: 15px;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }}
    button.jump-btn,
    a.jump-link {{
      border: 1px solid var(--border);
      background: var(--panel-2);
      color: var(--text);
      border-radius: 10px;
      padding: 8px 12px;
      text-decoration: none;
      cursor: pointer;
      font: inherit;
    }}
    button.jump-btn:hover,
    a.jump-link:hover {{
      border-color: var(--accent);
      color: var(--accent);
    }}
    .link-list {{
      margin-top: 12px;
    }}
    .subsection {{
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid rgba(148, 163, 184, 0.18);
    }}
    .hidden-by-filter {{
      display: none !important;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border-bottom: 1px solid var(--border);
      padding: 8px;
      text-align: left;
      vertical-align: top;
      word-break: break-word;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
    }}
    tr.timeline-row {{
      cursor: pointer;
    }}
    tr.kind-fused_event td:first-child {{
      color: var(--accent);
    }}
    tr.kind-runtime_event td:first-child {{
      color: var(--good);
    }}
    tr.kind-runtime_signal td:first-child {{
      color: var(--warn);
    }}
    tr.kind-detection td:first-child {{
      color: #c084fc;
    }}
    details {{
      margin-top: 10px;
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
      overflow-x: auto;
      background: rgba(2, 6, 23, 0.9);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <main>
    <section>
      <h1>{html.escape(title)}</h1>
      <p class="meta">Game: {game}</p>
      <p class="meta">Source: {source}</p>
      <p class="meta">Runtime sidecar: {html.escape(str(derived["runtime_sidecar_path"]))}</p>
      <p class="meta">Fused sidecar: {html.escape(str(derived.get("fused_sidecar_path") or "not provided"))}</p>
      <p class="meta">Media source: {html.escape(str(derived.get("media_path") or "not available"))}</p>
    </section>
    {"".join(section for section in sections if section)}
  </main>
  <script>
    const viewerState = {{ playTimer: null }};
    function jumpToTimestamp(timestamp, playUntil) {{
      const media = document.getElementById("viewer-media");
      if (!media || Number.isNaN(timestamp)) {{
        return;
      }}
      media.currentTime = Number(timestamp);
      media.focus();
      if (viewerState.playTimer) {{
        clearInterval(viewerState.playTimer);
        viewerState.playTimer = null;
      }}
      if (playUntil !== undefined && playUntil !== null) {{
        media.play();
        viewerState.playTimer = window.setInterval(() => {{
          if (media.currentTime >= Number(playUntil) || media.paused || media.ended) {{
            media.pause();
            clearInterval(viewerState.playTimer);
            viewerState.playTimer = null;
          }}
        }}, 120);
      }}
    }}

    function applyViewerFilters() {{
      const searchText = String(document.getElementById("viewer-search").value || "").toLowerCase();
      const minScoreValue = document.getElementById("viewer-min-score").value;
      const minScore = minScoreValue === "" ? null : Number(minScoreValue);
      const enabledKinds = new Set(Array.from(document.querySelectorAll("[data-filter-kind]")).filter((node) => node.checked).map((node) => node.getAttribute("data-filter-kind")));

      document.querySelectorAll("[data-kind]").forEach((node) => {{
        const kind = node.getAttribute("data-kind") || "";
        const scoreText = node.getAttribute("data-score") || "";
        const score = scoreText === "" ? null : Number(scoreText);
        const haystack = String(node.getAttribute("data-search") || "").toLowerCase();
        const showKind = enabledKinds.has(kind);
        const showText = !searchText || haystack.includes(searchText);
        const showScore = minScore === null || score === null || score >= minScore;
        node.classList.toggle("hidden-by-filter", !(showKind && showText && showScore));
      }});
    }}

    document.addEventListener("click", (event) => {{
      const target = event.target.closest("[data-jump-target]");
      if (target) {{
        const jumpTo = Number(target.getAttribute("data-jump-target"));
        const playUntilRaw = target.getAttribute("data-play-until");
        const playUntil = playUntilRaw === null || playUntilRaw === "" ? null : Number(playUntilRaw);
        jumpToTimestamp(jumpTo, playUntil);
      }}
    }});

    document.addEventListener("input", (event) => {{
      if (event.target.matches("#viewer-search, #viewer-min-score, [data-filter-kind]")) {{
        applyViewerFilters();
      }}
    }});

    applyViewerFilters();
  </script>
</body>
</html>
"""


def _summary_cards(runtime_payload: dict[str, Any], fused_payload: dict[str, Any] | None, derived: dict[str, Any]) -> str:
    matcher = runtime_payload.get("matcher", {}) if isinstance(runtime_payload.get("matcher"), dict) else {}
    events = runtime_payload.get("events", {}) if isinstance(runtime_payload.get("events"), dict) else {}
    cards = [
        ("Runtime status", runtime_payload.get("status")),
        ("Frames", matcher.get("frame_count")),
        ("Detections", len(list(matcher.get("confirmed_detections", [])))),
        ("Signals", derived.get("runtime_signal_count")),
        ("Runtime events", events.get("event_count")),
        ("Fused events", derived.get("fused_event_count")),
        ("Linked runtime events", derived.get("linked_runtime_event_count")),
        ("Linked detections", derived.get("linked_detection_count")),
        ("Media inline", "yes" if derived.get("media_exists") else "no"),
        ("Fused loaded", "yes" if isinstance(fused_payload, dict) else "no"),
    ]
    rendered = "".join(
        f'<div class="card"><div class="card-label">{html.escape(str(label))}</div><div class="card-value">{html.escape(str(value))}</div></div>'
        for label, value in cards
    )
    return f"<section><h2>Summary</h2><div class=\"cards\">{rendered}</div></section>"


def _controls_section() -> str:
    return """
<section>
  <h2>Viewer Controls</h2>
  <div class="controls">
    <div class="control-block">
      <label for="viewer-search">Filter by event type, signal type, or id</label>
      <input id="viewer-search" class="control-input" type="text" placeholder="ability_plus_medal_combo, signal-1, hero_portrait">
    </div>
    <div class="control-block">
      <label for="viewer-min-score">Minimum score</label>
      <input id="viewer-min-score" class="control-input" type="number" min="0" max="1" step="0.01" placeholder="0.80">
    </div>
    <div class="control-block">
      <label>Visible layers</label>
      <div class="checkbox-row">
        <label class="checkbox-pill"><input type="checkbox" data-filter-kind="fused_event" checked> fused events</label>
        <label class="checkbox-pill"><input type="checkbox" data-filter-kind="runtime_event" checked> runtime events</label>
        <label class="checkbox-pill"><input type="checkbox" data-filter-kind="runtime_signal" checked> runtime signals</label>
        <label class="checkbox-pill"><input type="checkbox" data-filter-kind="detection" checked> detections</label>
      </div>
    </div>
  </div>
</section>
"""


def _fused_event_explorer(groups: list[dict[str, Any]], media_exists: bool) -> str:
    if not groups:
        return '<section><h2>Event Explorer</h2><p class="meta">No fused events loaded. Runtime-only sections remain available below.</p></section>'
    cards = []
    for group in groups:
        event = group["event"]
        review_status = str(group["event"].get("review", {}).get("review_status") or "unreviewed")
        gate_status = str(event.get("gate_status") or "not_applicable")
        cards.append(
            f"""
<article id="card-{html.escape(str(event['event_id']))}" class="event-card" data-kind="fused_event" data-score="{html.escape(str(event.get('final_score', event.get('confidence', ''))))}" data-search="{html.escape(str(event.get('search_text') or ''))}">
  <div class="event-card-header">
    <div>
      <h3>{html.escape(str(event.get("event_type") or event["event_id"]))}</h3>
      <div class="muted">{html.escape(group["score_summary"])}</div>
    </div>
    <div>
      <span class="pill kind-fused_event">fused event</span>
      <span class="pill gate-{html.escape(gate_status)}">{html.escape(gate_status)}</span>
      <span class="pill">{'synergy_applied' if event.get('synergy_applied') else 'no_synergy'}</span>
      <span class="pill review-{html.escape(review_status)}">{html.escape(review_status)}</span>
    </div>
  </div>
  <div class="event-grid">
    {_metric("final_score", event.get("final_score"))}
    {_metric("confidence", event.get("confidence"))}
    {_metric("synergy_multiplier", event.get("synergy_multiplier"))}
    {_metric("minimum_required_signals_met", event.get("minimum_required_signals_met"))}
    {_metric("segment_start", event.get("suggested_start_timestamp"))}
    {_metric("segment_end", event.get("suggested_end_timestamp"))}
    {_metric("segment_duration", event.get("segment_duration_seconds"))}
    {_metric("semantic_targets", group.get("semantic_target_summary"))}
  </div>
  <div class="actions">
    <button class="jump-btn" data-jump-target="{html.escape(str(event['jump_timestamp']))}">Jump To Event</button>
    {_play_segment_button(event, media_exists)}
    <a class="jump-link" href="#{html.escape(str(event['row_id']))}">Jump To Timeline Row</a>
  </div>
  <div class="subsection">
    <h4>Contributing Signals</h4>
    {_linked_pills(group["linked_runtime_signals"], "runtime_signal", include_jump=True)}
    {_linked_pills(group["linked_normalized_signals"], "normalized_signal", include_jump=False)}
  </div>
  <div class="subsection">
    <h4>Linked Runtime Events</h4>
    {_linked_pills(group["linked_runtime_events"], "runtime_event", include_jump=True)}
  </div>
  <div class="subsection">
    <h4>Linked Detections</h4>
    {_linked_pills(group["linked_detections"], "detection", include_jump=True)}
  </div>
  <details>
    <summary>Show fused event JSON</summary>
    <pre>{html.escape(json.dumps(event, indent=2, sort_keys=True))}</pre>
  </details>
</article>
"""
        )
    return f'<section><h2>Event Explorer</h2><div class="explorer">{"".join(cards)}</div></section>'


def _play_segment_button(event: dict[str, Any], media_exists: bool) -> str:
    if not media_exists:
        return ""
    start = event.get("suggested_start_timestamp")
    end = event.get("suggested_end_timestamp")
    if start is None or end is None:
        return ""
    return (
        f'<button class="jump-btn" data-jump-target="{html.escape(str(start))}" '
        f'data-play-until="{html.escape(str(end))}">Play Segment</button>'
    )


def _metric(label: str, value: Any) -> str:
    return (
        '<div class="metric">'
        f'<div class="metric-label">{html.escape(label)}</div>'
        f'<div class="metric-value">{html.escape("" if value is None else str(value))}</div>'
        "</div>"
    )


def _linked_pills(rows: list[dict[str, Any]], kind: str, *, include_jump: bool) -> str:
    if not rows:
        return '<p class="meta">No linked rows.</p>'
    rendered = []
    for row in rows:
        label = row.get("label") or row.get("signal_id") or row.get("event_id") or row.get("asset_id") or row.get("row_id")
        content = f'{html.escape(str(label))} <span class="muted">({html.escape(str(row.get("row_id")))})</span>'
        if include_jump:
            rendered.append(
                f'<a class="jump-link pill kind-{html.escape(kind)}" href="#{html.escape(str(row["row_id"]))}" data-jump-target="{html.escape(str(row["jump_timestamp"]))}">{content}</a>'
            )
        else:
            rendered.append(f'<span class="pill kind-{html.escape(kind)}">{content}</span>')
    return "".join(rendered)


def _timeline_section(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<section><h2>Timeline</h2><p class="meta">No rows.</p></section>'
    body_rows = []
    for row in rows:
        body_rows.append(
            "<tr "
            f'id="{html.escape(str(row["entry_id"]))}" '
            f'class="timeline-row kind-{html.escape(str(row["kind"]))}" '
            f'data-kind="{html.escape(str(row["kind"]))}" '
            f'data-search="{html.escape(str(row.get("search_text") or ""))}" '
            f'data-score="{html.escape("" if row.get("score") is None else str(row.get("score")))}" '
            f'data-jump-target="{html.escape(str(row["jump_timestamp"]))}"'
            ">"
            f'<td><a class="jump-link" href="#{html.escape(str(row["target_id"]))}">{html.escape(str(row["kind"]))}</a></td>'
            f'<td>{html.escape(str(row["label"]))}</td>'
            f'<td>{html.escape(str(row["start_timestamp"]))}</td>'
            f'<td>{html.escape(str(row["end_timestamp"]))}</td>'
            f'<td>{html.escape("" if row.get("score") is None else str(row.get("score")))}</td>'
            f'<td>{html.escape(str(row.get("detail") or ""))}</td>'
            "</tr>"
        )
    return (
        "<section><h2>Timeline</h2>"
        "<table><thead><tr><th>kind</th><th>label</th><th>start</th><th>end</th><th>score</th><th>detail</th></tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody></table></section>"
    )


def _raw_sections(sections: list[dict[str, Any]]) -> str:
    rendered = []
    for section in sections:
        rendered.append(
            f"<section id=\"{html.escape(str(section['section_id']))}\"><h2>{html.escape(section['title'])}</h2>"
            f"<details><summary>Show rows</summary>{_table(section['rows'])}</details></section>"
        )
    return "".join(rendered)


def _table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="meta">No rows.</p>'
    fieldnames = sorted({key for row in rows for key in row.keys()})
    header = "".join(f"<th>{html.escape(name)}</th>" for name in fieldnames)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{_format_cell(row.get(name))}</td>" for name in fieldnames)
        body_rows.append(f'<tr id="{html.escape(str(row.get("row_id") or ""))}" data-kind="{html.escape(str(row.get("kind") or ""))}" data-search="{html.escape(str(row.get("search_text") or ""))}" data-score="{html.escape("" if row.get("score") is None else str(row.get("score")))}">{cells}</tr>')
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def _json_section(title: str, payload: Any, *, open_by_default: bool) -> str:
    if not payload:
        return f"<section><h2>{html.escape(title)}</h2><p class=\"meta\">No data.</p></section>"
    text = json.dumps(payload, indent=2, sort_keys=True)
    open_attr = " open" if open_by_default else ""
    return (
        f"<section><h2>{html.escape(title)}</h2>"
        f"<details{open_attr}><summary>Show JSON</summary><pre>{html.escape(text)}</pre></details></section>"
    )


def _format_cell(value: Any) -> str:
    if isinstance(value, list):
        if not value:
            return ""
        if all(not isinstance(item, (dict, list)) for item in value):
            return "".join(f'<span class="pill">{html.escape(str(item))}</span>' for item in value)
        return f"<pre>{html.escape(json.dumps(value, indent=2, sort_keys=True))}</pre>"
    if isinstance(value, dict):
        return f"<pre>{html.escape(json.dumps(value, indent=2, sort_keys=True))}</pre>"
    if value is None:
        return ""
    return html.escape(str(value))

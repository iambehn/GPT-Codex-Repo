from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
HIGHLIGHT_SELECTION_SCHEMA_VERSION = "highlight_selection_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "highlight_selection_exports"
SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"


def export_highlight_selection(
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
    selected_highlights = _selected_windows(payload)
    manifest = {
        "schema_version": HIGHLIGHT_SELECTION_SCHEMA_VERSION,
        "game": game,
        "source": source,
        "proxy_sidecar_path": str(sidecar_path),
        "selected_highlight_count": len(selected_highlights),
        "selected_highlights": selected_highlights,
    }

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
        "manifest_path": str(target),
        "otio_skeleton_path": str(otio_path),
        "selected_highlight_count": len(selected_highlights),
    }


def _selected_windows(payload: dict[str, Any]) -> list[dict[str, Any]]:
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


def _otio_skeleton(manifest: dict[str, Any]) -> dict[str, Any]:
    children = []
    for row in list(manifest.get("selected_highlights", [])):
        start_seconds = float(row.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
        children.append(
            {
                "OTIO_SCHEMA": "Clip.2",
                "name": str(row.get("highlight_id")),
                "source_range": {
                    "OTIO_SCHEMA": "TimeRange.1",
                    "start_time": {"OTIO_SCHEMA": "RationalTime.1", "value": start_seconds, "rate": 1},
                    "duration": {"OTIO_SCHEMA": "RationalTime.1", "value": round(end_seconds - start_seconds, 4), "rate": 1},
                },
                "metadata": {
                    "proxy_score": row.get("proxy_score"),
                    "recommended_action": row.get("recommended_action"),
                    "source_families": row.get("source_families", []),
                },
            }
        )
    return {
        "OTIO_SCHEMA": "Timeline.1",
        "name": Path(str(manifest.get("source", ""))).name or "highlight-selection",
        "metadata": {
            "schema_version": HIGHLIGHT_SELECTION_SCHEMA_VERSION,
            "game": manifest.get("game"),
            "proxy_sidecar_path": manifest.get("proxy_sidecar_path"),
        },
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

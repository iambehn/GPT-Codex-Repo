from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
FUSED_EXPORT_SCHEMA_VERSION = "fused_export_v1"
SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "fused_exports"
DEFAULT_ACTION_THRESHOLDS = {
    "highlight_candidate": 0.75,
    "inspect": 0.45,
}


def export_fused_analysis(sidecar_root: str | Path, game: str | None = None) -> dict[str, Any]:
    root = Path(sidecar_root).expanduser()
    if not root.is_absolute():
        root = (Path.cwd() / root).resolve()
    else:
        root = root.resolve()

    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "sidecar_root": str(root),
            "error": "sidecar root does not exist or is not a directory",
        }

    dataset = _collect_dataset(root, game)
    paths = _dataset_paths(root, game)
    _write_dataset_artifacts(dataset, paths)

    manifest = dataset["manifest"]
    return {
        "ok": True,
        "sidecar_root": str(root),
        "dataset_id": dataset["dataset_id"],
        "candidates_jsonl_path": str(paths["candidates_jsonl_path"]),
        "candidates_csv_path": str(paths["candidates_csv_path"]),
        "events_jsonl_path": str(paths["events_jsonl_path"]),
        "events_csv_path": str(paths["events_csv_path"]),
        "signal_references_jsonl_path": str(paths["signal_references_jsonl_path"]),
        "signal_references_csv_path": str(paths["signal_references_csv_path"]),
        "manifest_path": str(paths["manifest_path"]),
        "candidate_row_count": len(dataset["candidate_rows"]),
        "event_row_count": len(dataset["event_rows"]),
        "signal_reference_row_count": len(dataset["signal_reference_rows"]),
        "scanned_sidecar_count": manifest["scanned_sidecar_count"],
        "exported_sidecar_count": manifest["exported_sidecar_count"],
        "skipped_sidecar_count": manifest["skipped_sidecar_count"],
        **({"game_filter": game} if game is not None else {}),
    }


def _collect_dataset(root: Path, game: str | None) -> dict[str, Any]:
    dataset_id = _dataset_id(root, game)
    sidecar_paths = sorted(root.rglob("*.fused_analysis.json"))
    candidate_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    signal_reference_rows: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    manifest = {
        "dataset_id": dataset_id,
        "schema_version": FUSED_EXPORT_SCHEMA_VERSION,
        "sidecar_root": str(root),
        "scanned_sidecar_count": len(sidecar_paths),
        "exported_sidecar_count": 0,
        "skipped_sidecar_count": 0,
        "skipped_malformed_count": 0,
        "skipped_schema_mismatch_count": 0,
        "skipped_failed_analysis_count": 0,
        "skipped_game_filter_mismatch_count": 0,
        "warnings": warnings,
    }
    if game is not None:
        manifest["game_filter"] = game

    for sidecar_path in sidecar_paths:
        skip_reason, sidecar, clip_candidate_rows, clip_event_rows, clip_signal_ref_rows = _rows_from_sidecar(sidecar_path, dataset_id, game)
        if skip_reason is not None:
            manifest["skipped_sidecar_count"] += 1
            warnings.append({"path": str(sidecar_path), "reason": skip_reason})
            if skip_reason == "malformed_json":
                manifest["skipped_malformed_count"] += 1
            elif skip_reason == "unsupported_schema_version":
                manifest["skipped_schema_mismatch_count"] += 1
            elif skip_reason == "failed_analysis":
                manifest["skipped_failed_analysis_count"] += 1
            elif skip_reason == "game_filter_mismatch":
                manifest["skipped_game_filter_mismatch_count"] += 1
            continue
        del sidecar
        manifest["exported_sidecar_count"] += 1
        candidate_rows.extend(clip_candidate_rows)
        event_rows.extend(clip_event_rows)
        signal_reference_rows.extend(clip_signal_ref_rows)

    return {
        "dataset_id": dataset_id,
        "candidate_rows": candidate_rows,
        "event_rows": event_rows,
        "signal_reference_rows": signal_reference_rows,
        "manifest": manifest,
    }


def _rows_from_sidecar(
    sidecar_path: Path,
    dataset_id: str,
    game_filter: str | None,
) -> tuple[str | None, dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "malformed_json", None, [], [], []

    if sidecar.get("schema_version") != SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION:
        return "unsupported_schema_version", sidecar, [], [], []
    if game_filter is not None and sidecar.get("game") != game_filter:
        return "game_filter_mismatch", sidecar, [], [], []
    if not sidecar.get("ok", False):
        return "failed_analysis", sidecar, [], [], []

    candidate_rows, event_rows, signal_reference_rows = _build_rows_for_sidecar(sidecar_path, sidecar, dataset_id)
    return None, sidecar, candidate_rows, event_rows, signal_reference_rows


def _build_rows_for_sidecar(
    sidecar_path: Path,
    sidecar: dict[str, Any],
    dataset_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    normalized_signals = list(sidecar.get("normalized_signals", []))
    signal_by_id = {str(row.get("signal_id")): row for row in normalized_signals if str(row.get("signal_id", "")).strip()}
    fused_event_rows = list(sidecar.get("fused_events", []))
    candidate_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    signal_reference_rows: list[dict[str, Any]] = []

    for index, row in enumerate(fused_event_rows):
        final_score = float(row.get("final_score", row.get("confidence", 0.0)) or 0.0)
        recommended_action = _recommended_action(final_score)
        metadata = row.get("metadata", {}) if isinstance(row.get("metadata"), dict) else {}
        contributing_signals = [str(value) for value in row.get("contributing_signals", []) if str(value).strip()]
        candidate_row = {
            "schema_version": FUSED_EXPORT_SCHEMA_VERSION,
            "dataset_id": dataset_id,
            "fusion_id": sidecar.get("fusion_id"),
            "event_id": row.get("event_id"),
            "candidate_id": f"{sidecar.get('fusion_id')}:{row.get('event_id')}",
            "candidate_index": index,
            "game": sidecar.get("game"),
            "source": sidecar.get("source"),
            "sidecar_path": str(sidecar_path.resolve()),
            "event_type": row.get("event_type"),
            "final_score": round(final_score, 5),
            "confidence": row.get("confidence"),
            "gate_status": row.get("gate_status"),
            "synergy_applied": bool(row.get("synergy_applied", False)),
            "synergy_multiplier": row.get("synergy_multiplier"),
            "minimum_required_signals_met": row.get("minimum_required_signals_met"),
            "suggested_start_timestamp": row.get("suggested_start_timestamp"),
            "suggested_end_timestamp": row.get("suggested_end_timestamp"),
            "segment_duration_seconds": _segment_duration(row),
            "recommended_action": recommended_action,
            "matched_signal_types": metadata.get("matched_signal_types", []),
            "contributing_signals": contributing_signals,
            "contributing_signal_count": len(contributing_signals),
            "entity_id": metadata.get("entity_id"),
            "ability_id": metadata.get("ability_id"),
            "equipment_id": metadata.get("equipment_id"),
            "event_row_id": metadata.get("event_row_id"),
        }
        candidate_rows.append(candidate_row)

        event_row = {
            "schema_version": FUSED_EXPORT_SCHEMA_VERSION,
            "dataset_id": dataset_id,
            "fusion_id": sidecar.get("fusion_id"),
            "event_index": index,
            "game": sidecar.get("game"),
            "source": sidecar.get("source"),
            "sidecar_path": str(sidecar_path.resolve()),
            "recommended_action": recommended_action,
            **row,
        }
        event_rows.append(event_row)

        for signal_index, signal_id in enumerate(contributing_signals):
            signal_row = signal_by_id.get(signal_id, {})
            signal_reference_rows.append(
                {
                    "schema_version": FUSED_EXPORT_SCHEMA_VERSION,
                    "dataset_id": dataset_id,
                    "fusion_id": sidecar.get("fusion_id"),
                    "event_id": row.get("event_id"),
                    "signal_index": signal_index,
                    "signal_id": signal_id,
                    "signal_type": signal_row.get("signal_type"),
                    "producer_family": signal_row.get("producer_family"),
                    "source_family": signal_row.get("source_family"),
                    "asset_id": signal_row.get("asset_id"),
                    "roi_ref": signal_row.get("roi_ref"),
                }
            )
    return candidate_rows, event_rows, signal_reference_rows


def _recommended_action(final_score: float) -> str:
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["highlight_candidate"]):
        return "highlight_candidate"
    if final_score >= float(DEFAULT_ACTION_THRESHOLDS["inspect"]):
        return "inspect"
    return "skip"


def _segment_duration(row: dict[str, Any]) -> float:
    start = float(row.get("suggested_start_timestamp", row.get("start_timestamp", 0.0)) or 0.0)
    end = float(row.get("suggested_end_timestamp", row.get("end_timestamp", start)) or start)
    return round(max(0.0, end - start), 5)


def _write_dataset_artifacts(dataset: dict[str, Any], paths: dict[str, Path]) -> None:
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    _write_jsonl(paths["candidates_jsonl_path"], dataset["candidate_rows"])
    _write_csv(paths["candidates_csv_path"], dataset["candidate_rows"])
    _write_jsonl(paths["events_jsonl_path"], dataset["event_rows"])
    _write_csv(paths["events_csv_path"], dataset["event_rows"])
    _write_jsonl(paths["signal_references_jsonl_path"], dataset["signal_reference_rows"])
    _write_csv(paths["signal_references_csv_path"], dataset["signal_reference_rows"])

    manifest = dict(dataset["manifest"])
    manifest["candidates_jsonl_path"] = str(paths["candidates_jsonl_path"])
    manifest["candidates_csv_path"] = str(paths["candidates_csv_path"])
    manifest["events_jsonl_path"] = str(paths["events_jsonl_path"])
    manifest["events_csv_path"] = str(paths["events_csv_path"])
    manifest["signal_references_jsonl_path"] = str(paths["signal_references_jsonl_path"])
    manifest["signal_references_csv_path"] = str(paths["signal_references_csv_path"])
    manifest["candidate_row_count"] = len(dataset["candidate_rows"])
    manifest["event_row_count"] = len(dataset["event_rows"])
    manifest["signal_reference_row_count"] = len(dataset["signal_reference_rows"])
    paths["manifest_path"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value for key, value in row.items()})


def _dataset_paths(root: Path, game: str | None) -> dict[str, Path]:
    export_root = DEFAULT_OUTPUT_ROOT / (game or "all_games")
    slug = _source_slug(root)
    digest = _dataset_hash(root, game)
    stem = f"{slug}-{digest}"
    return {
        "candidates_jsonl_path": export_root / f"{stem}.fused_candidates.jsonl",
        "candidates_csv_path": export_root / f"{stem}.fused_candidates.csv",
        "events_jsonl_path": export_root / f"{stem}.fused_events.jsonl",
        "events_csv_path": export_root / f"{stem}.fused_events.csv",
        "signal_references_jsonl_path": export_root / f"{stem}.fused_signal_references.jsonl",
        "signal_references_csv_path": export_root / f"{stem}.fused_signal_references.csv",
        "manifest_path": export_root / f"{stem}.fused_export_manifest.json",
    }


def _dataset_id(root: Path, game: str | None) -> str:
    return f"fused-export-{_dataset_hash(root, game)}"


def _dataset_hash(root: Path, game: str | None) -> str:
    payload = f"{root}\n{game or 'all_games'}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def _source_slug(source: str | Path) -> str:
    stem = Path(str(source)).name or "fused-export"
    return "".join(ch if ch.isalnum() else "-" for ch in stem.lower()).strip("-") or "fused-export"

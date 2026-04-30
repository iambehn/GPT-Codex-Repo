from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
TRAINING_EXPORT_SCHEMA_VERSION = "training_export_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "training_exports"
SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
KNOWN_SOURCE_STATUS_COLUMNS = (
    "playlist_hls",
    "audio_prepass",
    "visual_prepass",
    "chat_velocity",
)
KNOWN_SIGNAL_SOURCE_COLUMNS = (
    "chat_spike",
    "playlist_spike",
    "playlist_discontinuity",
    "audio_spike",
    "visual_motion_spike",
    "visual_flash_spike",
)


def export_training_data(sidecar_root: str | Path, game: str | None = None) -> dict[str, Any]:
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
    result = {
        "ok": True,
        "sidecar_root": str(root),
        "dataset_id": dataset["dataset_id"],
        "jsonl_path": str(paths["jsonl_path"]),
        "csv_path": str(paths["csv_path"]),
        "manifest_path": str(paths["manifest_path"]),
        "row_count": len(dataset["rows"]),
        "scanned_sidecar_count": manifest["scanned_sidecar_count"],
        "exported_sidecar_count": manifest["exported_sidecar_count"],
        "skipped_sidecar_count": manifest["skipped_sidecar_count"],
    }
    if game is not None:
        result["game_filter"] = game
    return result


def _collect_dataset(root: Path, game: str | None) -> dict[str, Any]:
    dataset_id = _dataset_id(root, game)
    sidecar_paths = sorted(root.rglob("*.proxy_scan.json"))
    rows: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    manifest = {
        "dataset_id": dataset_id,
        "schema_version": TRAINING_EXPORT_SCHEMA_VERSION,
        "sidecar_root": str(root),
        "scanned_sidecar_count": len(sidecar_paths),
        "exported_sidecar_count": 0,
        "skipped_sidecar_count": 0,
        "skipped_malformed_count": 0,
        "skipped_schema_mismatch_count": 0,
        "skipped_failed_scan_count": 0,
        "skipped_empty_scan_count": 0,
        "skipped_game_filter_mismatch_count": 0,
        "warnings": warnings,
    }
    if game is not None:
        manifest["game_filter"] = game

    for sidecar_path in sidecar_paths:
        skip_reason, sidecar, sidecar_rows = _rows_from_sidecar(sidecar_path, dataset_id, game)
        if skip_reason is not None:
            manifest["skipped_sidecar_count"] += 1
            warnings.append({"path": str(sidecar_path), "reason": skip_reason})
            if skip_reason == "malformed_json":
                manifest["skipped_malformed_count"] += 1
            elif skip_reason == "unsupported_schema_version":
                manifest["skipped_schema_mismatch_count"] += 1
            elif skip_reason == "failed_scan":
                manifest["skipped_failed_scan_count"] += 1
            elif skip_reason == "empty_scan":
                manifest["skipped_empty_scan_count"] += 1
            elif skip_reason == "game_filter_mismatch":
                manifest["skipped_game_filter_mismatch_count"] += 1
            continue

        manifest["exported_sidecar_count"] += 1
        rows.extend(sidecar_rows)

    return {
        "dataset_id": dataset_id,
        "rows": rows,
        "manifest": manifest,
    }


def _rows_from_sidecar(
    sidecar_path: Path,
    dataset_id: str,
    game_filter: str | None,
) -> tuple[str | None, dict[str, Any] | None, list[dict[str, Any]]]:
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "malformed_json", None, []

    if sidecar.get("schema_version") != SUPPORTED_PROXY_SCAN_SCHEMA_VERSION:
        return "unsupported_schema_version", sidecar, []
    if game_filter is not None and sidecar.get("game") != game_filter:
        return "game_filter_mismatch", sidecar, []
    if not sidecar.get("ok", False):
        return "failed_scan", sidecar, []
    windows = sidecar.get("windows", [])
    if not windows:
        return "empty_scan", sidecar, []

    return None, sidecar, _build_rows_for_sidecar(sidecar_path, sidecar, dataset_id)


def _build_rows_for_sidecar(sidecar_path: Path, sidecar: dict[str, Any], dataset_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, window in enumerate(sidecar.get("windows", [])):
        sources = list(window.get("sources", []))
        signal_features = _signal_features(window, sidecar.get("source_results", {}))
        rows.append(
            {
                "schema_version": TRAINING_EXPORT_SCHEMA_VERSION,
                "dataset_id": dataset_id,
                "sidecar_path": str(sidecar_path.resolve()),
                "scan_id": sidecar.get("scan_id"),
                "game": sidecar.get("game"),
                "source": sidecar.get("source"),
                "window_index": index,
                "start_seconds": float(window.get("start_seconds", 0.0)),
                "end_seconds": float(window.get("end_seconds", 0.0)),
                "duration_seconds": float(window.get("end_seconds", 0.0)) - float(window.get("start_seconds", 0.0)),
                "proxy_score": float(window.get("proxy_score", 0.0)),
                "recommended_action": window.get("recommended_action"),
                "signal_count": int(window.get("signal_count", 0)),
                "source_count": len(sources),
                "source_families": list(window.get("source_families", [])),
                "source_family_count": len(window.get("source_families", [])),
                "sources": sources,
                "source_results": sidecar.get("source_results", {}),
                "signal_features": signal_features,
                "label": None,
                "label_source": None,
                "label_notes": None,
            }
        )
    return rows


def _signal_features(window: dict[str, Any], source_results: dict[str, Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    max_strength: dict[str, float] = {}
    max_confidence: dict[str, float] = {}

    for signal in window.get("signals", []):
        signal_source = str(signal.get("source", "unknown"))
        counts[signal_source] = counts.get(signal_source, 0) + 1
        strength = float(signal.get("strength", 0.0))
        confidence = float(signal.get("confidence", 0.0))
        max_strength[signal_source] = max(strength, max_strength.get(signal_source, 0.0))
        max_confidence[signal_source] = max(confidence, max_confidence.get(signal_source, 0.0))

    source_status = {
        source_name: str(result.get("status", "unknown"))
        for source_name, result in source_results.items()
    }
    source_family_counts: dict[str, int] = {}
    for signal in window.get("signals", []):
        family_name = str(signal.get("source_family", "unknown"))
        source_family_counts[family_name] = source_family_counts.get(family_name, 0) + 1

    return {
        "source_count": len(window.get("sources", [])),
        "source_family_count": len(window.get("source_families", [])),
        "signal_count": int(window.get("signal_count", 0)),
        "signal_counts": counts,
        "signal_counts_by_family": source_family_counts,
        "max_strength_by_signal_source": max_strength,
        "max_confidence_by_signal_source": max_confidence,
        "source_status": source_status,
    }


def _write_dataset_artifacts(dataset: dict[str, Any], paths: dict[str, Path]) -> None:
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    _write_jsonl(paths["jsonl_path"], dataset["rows"])
    _write_csv(paths["csv_path"], dataset["rows"])

    manifest = dict(dataset["manifest"])
    manifest["jsonl_path"] = str(paths["jsonl_path"])
    manifest["csv_path"] = str(paths["csv_path"])
    manifest["row_count"] = len(dataset["rows"])
    paths["manifest_path"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = _csv_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(_flatten_row_for_csv(row))


def _csv_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    fieldnames = [
        "dataset_id",
        "scan_id",
        "game",
        "source",
        "sidecar_path",
        "window_index",
        "start_seconds",
        "end_seconds",
        "duration_seconds",
        "proxy_score",
        "recommended_action",
        "source_count",
        "signal_count",
    ]

    source_status_keys = set(KNOWN_SOURCE_STATUS_COLUMNS)
    signal_source_keys = set(KNOWN_SIGNAL_SOURCE_COLUMNS)
    for row in rows:
        features = row.get("signal_features", {})
        source_status_keys.update(features.get("source_status", {}).keys())
        signal_source_keys.update(features.get("signal_counts", {}).keys())
        signal_source_keys.update(features.get("max_strength_by_signal_source", {}).keys())
        signal_source_keys.update(features.get("max_confidence_by_signal_source", {}).keys())

    for source_name in sorted(source_status_keys):
        fieldnames.append(f"source_status_{source_name}")
    for source_name in sorted(signal_source_keys):
        fieldnames.append(f"signal_count_{source_name}")
    for source_name in sorted(signal_source_keys):
        fieldnames.append(f"max_strength_{source_name}")
    for source_name in sorted(signal_source_keys):
        fieldnames.append(f"max_confidence_{source_name}")

    fieldnames.extend(["label", "label_source", "label_notes"])
    return fieldnames


def _flatten_row_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    features = row.get("signal_features", {})
    flattened = {
        "dataset_id": row.get("dataset_id"),
        "scan_id": row.get("scan_id"),
        "game": row.get("game"),
        "source": row.get("source"),
        "sidecar_path": row.get("sidecar_path"),
        "window_index": row.get("window_index"),
        "start_seconds": row.get("start_seconds"),
        "end_seconds": row.get("end_seconds"),
        "duration_seconds": row.get("duration_seconds"),
        "proxy_score": row.get("proxy_score"),
        "recommended_action": row.get("recommended_action"),
        "source_count": row.get("source_count"),
        "signal_count": row.get("signal_count"),
        "label": row.get("label"),
        "label_source": row.get("label_source"),
        "label_notes": row.get("label_notes"),
    }

    for source_name, status in features.get("source_status", {}).items():
        flattened[f"source_status_{source_name}"] = status
    for source_name, value in features.get("signal_counts", {}).items():
        flattened[f"signal_count_{source_name}"] = value
    for source_name, value in features.get("max_strength_by_signal_source", {}).items():
        flattened[f"max_strength_{source_name}"] = value
    for source_name, value in features.get("max_confidence_by_signal_source", {}).items():
        flattened[f"max_confidence_{source_name}"] = value

    return flattened


def _dataset_paths(root: Path, game: str | None) -> dict[str, Path]:
    scope = game or "all"
    root_hash = _root_hash(root, game)
    base_dir = DEFAULT_OUTPUT_ROOT / scope
    base_name = f"{scope}-{root_hash}"
    return {
        "jsonl_path": base_dir / f"{base_name}.windows.jsonl",
        "csv_path": base_dir / f"{base_name}.windows.csv",
        "manifest_path": base_dir / f"{base_name}.manifest.json",
    }


def _dataset_id(root: Path, game: str | None) -> str:
    scope = game or "all"
    return f"{scope}-{_root_hash(root, game)}"


def _root_hash(root: Path, game: str | None) -> str:
    key = f"{root.resolve()}\n{game or ''}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]

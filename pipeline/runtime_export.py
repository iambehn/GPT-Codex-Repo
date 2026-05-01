from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_EXPORT_SCHEMA_VERSION = "runtime_export_v1"
SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION = "runtime_analysis_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "runtime_exports"
DEFAULT_SCORING_CONFIG = {
    "event_weights": {
        "medal_seen": 0.45,
        "ability_seen": 0.18,
        "pov_character_identified": 0.08,
    },
    "event_caps": {
        "medal_seen": 2,
        "ability_seen": 3,
        "pov_character_identified": 1,
    },
    "detection_support_weight": 0.03,
    "max_detection_support": 0.12,
    "action_thresholds": {
        "inspect": 0.25,
        "highlight_candidate": 0.60,
    },
}


def export_runtime_analysis(
    sidecar_root: str | Path,
    game: str | None = None,
    *,
    scoring_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
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

    config = merged_scoring_config(scoring_config or {})
    dataset = _collect_dataset(root, game, config)
    paths = _dataset_paths(root, game)
    _write_dataset_artifacts(dataset, paths)

    manifest = dataset["manifest"]
    return {
        "ok": True,
        "sidecar_root": str(root),
        "dataset_id": dataset["dataset_id"],
        "clips_jsonl_path": str(paths["clips_jsonl_path"]),
        "clips_csv_path": str(paths["clips_csv_path"]),
        "events_jsonl_path": str(paths["events_jsonl_path"]),
        "events_csv_path": str(paths["events_csv_path"]),
        "detections_jsonl_path": str(paths["detections_jsonl_path"]),
        "detections_csv_path": str(paths["detections_csv_path"]),
        "manifest_path": str(paths["manifest_path"]),
        "clip_row_count": len(dataset["clip_rows"]),
        "event_row_count": len(dataset["event_rows"]),
        "detection_row_count": len(dataset["detection_rows"]),
        "scanned_sidecar_count": manifest["scanned_sidecar_count"],
        "exported_sidecar_count": manifest["exported_sidecar_count"],
        "skipped_sidecar_count": manifest["skipped_sidecar_count"],
        **({"game_filter": game} if game is not None else {}),
    }


def _collect_dataset(root: Path, game: str | None, scoring_config: dict[str, Any]) -> dict[str, Any]:
    dataset_id = _dataset_id(root, game)
    sidecar_paths = sorted(root.rglob("*.runtime_analysis.json"))
    clip_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    detection_rows: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    manifest = {
        "dataset_id": dataset_id,
        "schema_version": RUNTIME_EXPORT_SCHEMA_VERSION,
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
        skip_reason, sidecar, clip_row, clip_event_rows, clip_detection_rows = _rows_from_sidecar(
            sidecar_path, dataset_id, game, scoring_config
        )
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
        clip_rows.append(clip_row)
        event_rows.extend(clip_event_rows)
        detection_rows.extend(clip_detection_rows)

    return {
        "dataset_id": dataset_id,
        "clip_rows": clip_rows,
        "event_rows": event_rows,
        "detection_rows": detection_rows,
        "manifest": manifest,
    }


def _rows_from_sidecar(
    sidecar_path: Path,
    dataset_id: str,
    game_filter: str | None,
    scoring_config: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "malformed_json", None, None, [], []

    if sidecar.get("schema_version") != SUPPORTED_RUNTIME_ANALYSIS_SCHEMA_VERSION:
        return "unsupported_schema_version", sidecar, None, [], []
    if game_filter is not None and sidecar.get("game") != game_filter:
        return "game_filter_mismatch", sidecar, None, [], []
    if not sidecar.get("ok", False):
        return "failed_analysis", sidecar, None, [], []

    clip_row, event_rows, detection_rows = _build_rows_for_sidecar(sidecar_path, sidecar, dataset_id, scoring_config)
    return None, sidecar, clip_row, event_rows, detection_rows


def _build_rows_for_sidecar(
    sidecar_path: Path,
    sidecar: dict[str, Any],
    dataset_id: str,
    scoring_config: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    events_payload = sidecar.get("events", {})
    matcher_payload = sidecar.get("matcher", {})
    event_rows_payload = list(events_payload.get("rows", []))
    detection_rows_payload = list(matcher_payload.get("confirmed_detections", []))
    score = score_runtime_clip(event_rows_payload, detection_rows_payload, scoring_config)

    clip_row = {
        "schema_version": RUNTIME_EXPORT_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "analysis_id": sidecar.get("analysis_id"),
        "game": sidecar.get("game"),
        "source": sidecar.get("source"),
        "sidecar_path": str(sidecar_path.resolve()),
        "frame_count": int(matcher_payload.get("frame_count", 0) or 0),
        "sample_fps": float(matcher_payload.get("sample_fps", 0.0) or 0.0),
        "template_count": int(matcher_payload.get("template_count", 0) or 0),
        "confirmed_detection_count": len(detection_rows_payload),
        "event_count": int(events_payload.get("event_count", len(event_rows_payload)) or 0),
        "highlight_score": score["highlight_score"],
        "recommended_action": score["recommended_action"],
        "score_breakdown": score["score_breakdown"],
        "score_reasoning": score["score_reasoning"],
        "matcher_summary": matcher_payload.get("summary", {}),
        "event_summary": events_payload.get("event_summary", {}),
    }

    event_rows: list[dict[str, Any]] = []
    for index, row in enumerate(event_rows_payload):
        event_row = {
            "schema_version": RUNTIME_EXPORT_SCHEMA_VERSION,
            "dataset_id": dataset_id,
            "analysis_id": sidecar.get("analysis_id"),
            "event_index": index,
            "game": sidecar.get("game"),
            "source": sidecar.get("source"),
            "sidecar_path": str(sidecar_path.resolve()),
            "highlight_score": score["highlight_score"],
            "recommended_action": score["recommended_action"],
            **row,
        }
        event_rows.append(event_row)

    detection_rows: list[dict[str, Any]] = []
    for index, row in enumerate(detection_rows_payload):
        detection_row = {
            "schema_version": RUNTIME_EXPORT_SCHEMA_VERSION,
            "dataset_id": dataset_id,
            "analysis_id": sidecar.get("analysis_id"),
            "detection_index": index,
            "game": sidecar.get("game"),
            "source": sidecar.get("source"),
            "sidecar_path": str(sidecar_path.resolve()),
            "highlight_score": score["highlight_score"],
            "recommended_action": score["recommended_action"],
            **row,
        }
        detection_rows.append(detection_row)

    return clip_row, event_rows, detection_rows


def score_runtime_clip(
    event_rows: list[dict[str, Any]],
    detection_rows: list[dict[str, Any]],
    scoring_config: dict[str, Any],
) -> dict[str, Any]:
    event_weights = scoring_config["event_weights"]
    event_caps = scoring_config["event_caps"]
    event_counts: dict[str, int] = {}
    event_contributions: dict[str, float] = {}

    total = 0.0
    for row in event_rows:
        event_type = str(row.get("event_type", "unknown"))
        event_counts[event_type] = event_counts.get(event_type, 0) + 1

    for event_type, count in sorted(event_counts.items()):
        weight = float(event_weights.get(event_type, 0.0))
        capped_count = min(count, int(event_caps.get(event_type, count)))
        contribution = weight * capped_count
        event_contributions[event_type] = round(contribution, 4)
        total += contribution

    detection_support_weight = float(scoring_config["detection_support_weight"])
    max_detection_support = float(scoring_config["max_detection_support"])
    detection_support = min(max_detection_support, len(detection_rows) * detection_support_weight)
    total += detection_support

    highlight_score = round(min(1.0, total), 4)
    thresholds = scoring_config["action_thresholds"]
    if highlight_score >= float(thresholds["highlight_candidate"]):
        recommended_action = "highlight_candidate"
    elif highlight_score >= float(thresholds["inspect"]):
        recommended_action = "inspect"
    else:
        recommended_action = "skip"

    score_breakdown = {
        "event_counts": event_counts,
        "event_contributions": event_contributions,
        "detection_support": round(detection_support, 4),
    }
    event_bits = [
        f"{event_type} x{count} -> {event_contributions.get(event_type, 0.0):.2f}"
        for event_type, count in sorted(event_counts.items())
    ]
    if detection_rows:
        event_bits.append(f"detection_support -> {detection_support:.2f}")
    score_reasoning = "; ".join(event_bits) if event_bits else "no runtime events or detections"
    return {
        "highlight_score": highlight_score,
        "recommended_action": recommended_action,
        "score_breakdown": score_breakdown,
        "score_reasoning": score_reasoning,
    }


def _write_dataset_artifacts(dataset: dict[str, Any], paths: dict[str, Path]) -> None:
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    _write_jsonl(paths["clips_jsonl_path"], dataset["clip_rows"])
    _write_csv(paths["clips_csv_path"], dataset["clip_rows"])
    _write_jsonl(paths["events_jsonl_path"], dataset["event_rows"])
    _write_csv(paths["events_csv_path"], dataset["event_rows"])
    _write_jsonl(paths["detections_jsonl_path"], dataset["detection_rows"])
    _write_csv(paths["detections_csv_path"], dataset["detection_rows"])

    manifest = dict(dataset["manifest"])
    manifest["clips_jsonl_path"] = str(paths["clips_jsonl_path"])
    manifest["clips_csv_path"] = str(paths["clips_csv_path"])
    manifest["events_jsonl_path"] = str(paths["events_jsonl_path"])
    manifest["events_csv_path"] = str(paths["events_csv_path"])
    manifest["detections_jsonl_path"] = str(paths["detections_jsonl_path"])
    manifest["detections_csv_path"] = str(paths["detections_csv_path"])
    manifest["clip_row_count"] = len(dataset["clip_rows"])
    manifest["event_row_count"] = len(dataset["event_rows"])
    manifest["detection_row_count"] = len(dataset["detection_rows"])
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
    return sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]


def _flatten_row_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
        for key, value in row.items()
    }


def _dataset_paths(root: Path, game: str | None) -> dict[str, Path]:
    scope = game or "all"
    root_hash = _root_hash(root, game)
    base_dir = DEFAULT_OUTPUT_ROOT / scope
    base_name = f"{scope}-{root_hash}"
    return {
        "clips_jsonl_path": base_dir / f"{base_name}.clips.jsonl",
        "clips_csv_path": base_dir / f"{base_name}.clips.csv",
        "events_jsonl_path": base_dir / f"{base_name}.events.jsonl",
        "events_csv_path": base_dir / f"{base_name}.events.csv",
        "detections_jsonl_path": base_dir / f"{base_name}.detections.jsonl",
        "detections_csv_path": base_dir / f"{base_name}.detections.csv",
        "manifest_path": base_dir / f"{base_name}.manifest.json",
    }


def _dataset_id(root: Path, game: str | None) -> str:
    scope = game or "all"
    return f"{scope}-{_root_hash(root, game)}"


def _root_hash(root: Path, game: str | None) -> str:
    key = f"{root.resolve()}\n{game or ''}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def merged_scoring_config(override: dict[str, Any] | None = None) -> dict[str, Any]:
    override = override or {}
    merged = json.loads(json.dumps(DEFAULT_SCORING_CONFIG))
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


_score_clip = score_runtime_clip
_merged_scoring_config = merged_scoring_config

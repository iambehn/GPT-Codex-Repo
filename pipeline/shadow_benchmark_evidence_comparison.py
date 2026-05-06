from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.shadow_benchmark_matrix import SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION
from pipeline.shadow_benchmark_review import SHADOW_BENCHMARK_REVIEW_SCHEMA_VERSION, _review_targets
from pipeline.shadow_ranking_replay import _load_json, _resolve_path


REPO_ROOT = Path(__file__).resolve().parent.parent
SHADOW_BENCHMARK_EVIDENCE_COMPARISON_SCHEMA_VERSION = "shadow_benchmark_evidence_comparison_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_benchmark_evidence_comparisons"


def compare_shadow_benchmark_evidence_modes(
    real_manifest: str | Path,
    synthetic_manifest: str | Path,
    *,
    output_path: str | Path | None = None,
    training_target: str | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    real_payload = _load_review_payload(real_manifest, training_target=training_target, game=game, platform=platform)
    if not real_payload.get("ok"):
        return real_payload
    synthetic_payload = _load_review_payload(synthetic_manifest, training_target=training_target, game=game, platform=platform)
    if not synthetic_payload.get("ok"):
        return synthetic_payload

    real_rows = {str(row.get("training_target") or ""): row for row in real_payload["target_reviews"]}
    synthetic_rows = {str(row.get("training_target") or ""): row for row in synthetic_payload["target_reviews"]}
    targets = sorted({*real_rows.keys(), *synthetic_rows.keys()})
    rows: list[dict[str, Any]] = []
    for index, target in enumerate(targets):
        if not target:
            continue
        real_row = real_rows.get(target, {})
        synthetic_row = synthetic_rows.get(target, {})
        rows.append(
            {
                "row_index": index,
                "training_target": target,
                "real_manifest_path": real_payload["manifest_path"],
                "synthetic_manifest_path": synthetic_payload["manifest_path"],
                "real_current_best_family": real_row.get("current_best_family"),
                "synthetic_current_best_family": synthetic_row.get("current_best_family"),
                "real_best_recommendation_decision": real_row.get("best_recommendation_decision"),
                "synthetic_best_recommendation_decision": synthetic_row.get("best_recommendation_decision"),
                "real_current_best_evidence_mode": real_row.get("current_best_evidence_mode"),
                "synthetic_current_best_evidence_mode": synthetic_row.get("current_best_evidence_mode"),
                "real_readiness_classification": real_row.get("readiness_classification"),
                "synthetic_readiness_classification": synthetic_row.get("readiness_classification"),
                "real_primary_metric_name": real_row.get("primary_metric_name"),
                "synthetic_primary_metric_name": synthetic_row.get("primary_metric_name"),
                "real_primary_metric_delta": real_row.get("primary_metric_delta"),
                "synthetic_primary_metric_delta": synthetic_row.get("primary_metric_delta"),
                "primary_metric_delta_gap": _metric_gap(real_row.get("primary_metric_delta"), synthetic_row.get("primary_metric_delta")),
                "real_confidence_level": real_row.get("confidence_level"),
                "synthetic_confidence_level": synthetic_row.get("confidence_level"),
                "real_successful_run_count": real_row.get("successful_run_count"),
                "synthetic_successful_run_count": synthetic_row.get("successful_run_count"),
                "real_run_count": real_row.get("run_count"),
                "synthetic_run_count": synthetic_row.get("run_count"),
                "family_winner_changed": real_row.get("current_best_family") != synthetic_row.get("current_best_family"),
                "readiness_changed": real_row.get("readiness_classification") != synthetic_row.get("readiness_classification"),
                "recommendation_changed": real_row.get("best_recommendation_decision") != synthetic_row.get("best_recommendation_decision"),
                "disagreement_indicators": _disagreement_indicators(real_row, synthetic_row),
                "game": real_row.get("game") or synthetic_row.get("game") or game,
                "platform": real_row.get("platform") or synthetic_row.get("platform") or platform,
            }
        )

    comparison_id = _comparison_id(real_payload["manifest_path"], synthetic_payload["manifest_path"], training_target=training_target, game=game, platform=platform)
    result = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_BENCHMARK_EVIDENCE_COMPARISON_SCHEMA_VERSION,
        "comparison_id": comparison_id,
        "created_at": datetime.now(UTC).isoformat(),
        "real_manifest_path": real_payload["manifest_path"],
        "synthetic_manifest_path": synthetic_payload["manifest_path"],
        "filters": {key: value for key, value in {"training_target": training_target, "game": game, "platform": platform}.items() if value is not None},
        "row_count": len(rows),
        "rows": rows,
        "summary": {
            "target_count": len(rows),
            "family_winner_changed_count": sum(1 for row in rows if row["family_winner_changed"]),
            "readiness_changed_count": sum(1 for row in rows if row["readiness_changed"]),
            "recommendation_changed_count": sum(1 for row in rows if row["recommendation_changed"]),
            "synthetic_only_ready_count": sum(1 for row in rows if "ready_only_under_synthetic" in row["disagreement_indicators"]),
            "real_only_ready_count": sum(1 for row in rows if "ready_only_under_real" in row["disagreement_indicators"]),
        },
    }
    target = _resolve_path(output_path) if output_path is not None else DEFAULT_OUTPUT_ROOT / f"{comparison_id}.shadow_benchmark_evidence_comparison.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    csv_path = target.with_suffix(".csv")
    _write_csv(csv_path, rows)
    result["manifest_path"] = str(target)
    result["csv_path"] = str(csv_path)
    return result


def _load_review_payload(
    manifest: str | Path,
    *,
    training_target: str | None,
    game: str | None,
    platform: str | None,
) -> dict[str, Any]:
    manifest_path = _resolve_path(manifest)
    payload = _load_json(manifest_path)
    if payload is None:
        return {
            "ok": False,
            "status": "invalid_evidence_mode_manifest",
            "manifest_path": str(manifest_path),
            "error": "manifest is missing or malformed",
        }
    schema_version = str(payload.get("schema_version") or "").strip()
    if schema_version == SHADOW_BENCHMARK_REVIEW_SCHEMA_VERSION:
        review_filters = payload.get("filters", {}) if isinstance(payload.get("filters"), dict) else {}
        rows = [
            _normalize_target_review_row(
                row,
                default_game=str(review_filters.get("game") or "").strip() or None,
                default_platform=str(review_filters.get("platform") or "").strip() or None,
            )
            for row in list(payload.get("target_reviews", []))
            if isinstance(row, dict)
        ]
    elif schema_version == SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION:
        benchmark_filters = payload.get("benchmark_config", {}).get("filters", {}) if isinstance(payload.get("benchmark_config"), dict) else {}
        rows = _review_targets(
            [
                {
                    **row,
                    "benchmark_manifest_path": str(manifest_path),
                    "game": str(benchmark_filters.get("game") or "").strip() or None,
                    "platform": str(benchmark_filters.get("platform") or "").strip() or None,
                }
                for row in list(payload.get("runs", []))
                if isinstance(row, dict)
            ]
        )
        rows = [
            _normalize_target_review_row(
                row,
                default_game=str(benchmark_filters.get("game") or "").strip() or None,
                default_platform=str(benchmark_filters.get("platform") or "").strip() or None,
            )
            for row in rows
        ]
    else:
        return {
            "ok": False,
            "status": "unsupported_evidence_mode_manifest",
            "manifest_path": str(manifest_path),
            "error": f"unsupported schema version: {schema_version}",
        }
    filtered = []
    for row in rows:
        if training_target is not None and str(row.get("training_target") or "") != training_target:
            continue
        if game is not None and str(row.get("game") or "").strip() not in {"", game}:
            continue
        if platform is not None and str(row.get("platform") or "").strip() not in {"", platform}:
            continue
        filtered.append(row)
    return {
        "ok": True,
        "manifest_path": str(manifest_path),
        "target_reviews": filtered,
    }


def _normalize_target_review_row(
    row: dict[str, Any],
    *,
    default_game: str | None,
    default_platform: str | None,
) -> dict[str, Any]:
    normalized = dict(row)
    normalized["training_target"] = str(
        row.get("training_target")
        or row.get("evaluation_target")
        or row.get("target")
        or ""
    ).strip()
    normalized["current_best_family"] = _first_non_empty_string(
        row.get("current_best_family"),
        row.get("best_family"),
        row.get("model_family"),
    )
    normalized["best_recommendation_decision"] = _first_non_empty_string(
        row.get("best_recommendation_decision"),
        row.get("recommendation_decision"),
    )
    normalized["current_best_evidence_mode"] = _first_non_empty_string(
        row.get("current_best_evidence_mode"),
        row.get("evidence_mode"),
    )
    normalized["readiness_classification"] = _first_non_empty_string(
        row.get("readiness_classification"),
        row.get("target_readiness"),
    )
    normalized["primary_metric_name"] = _first_non_empty_string(
        row.get("primary_metric_name"),
        row.get("global_primary_metric_name"),
    )
    normalized["primary_metric_delta"] = _first_non_null(
        row.get("primary_metric_delta"),
        row.get("global_primary_metric_delta"),
    )
    normalized["confidence_level"] = _first_non_empty_string(
        row.get("confidence_level"),
        row.get("confidence"),
    )
    normalized["successful_run_count"] = _first_non_null(
        row.get("successful_run_count"),
        row.get("ok_run_count"),
    )
    normalized["run_count"] = _first_non_null(
        row.get("run_count"),
        row.get("total_run_count"),
    )
    normalized["game"] = _first_non_empty_string(
        row.get("game"),
        default_game,
    )
    normalized["platform"] = _first_non_empty_string(
        row.get("platform"),
        default_platform,
    )
    return normalized


def _first_non_empty_string(*values: Any) -> str | None:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return None


def _first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _comparison_id(real_manifest: str, synthetic_manifest: str, *, training_target: str | None, game: str | None, platform: str | None) -> str:
    payload = "\n".join([real_manifest, synthetic_manifest, str(training_target or ""), str(game or ""), str(platform or "")])
    return f"shadow-evidence-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _metric_gap(real_value: Any, synthetic_value: Any) -> float | None:
    try:
        if real_value is None or synthetic_value is None:
            return None
        return round(float(synthetic_value) - float(real_value), 6)
    except (TypeError, ValueError):
        return None


def _disagreement_indicators(real_row: dict[str, Any], synthetic_row: dict[str, Any]) -> list[str]:
    indicators: list[str] = []
    real_ready = str(real_row.get("readiness_classification") or "") == "ready_for_next_iteration"
    synthetic_ready = str(synthetic_row.get("readiness_classification") or "") == "ready_for_next_iteration"
    if synthetic_ready and not real_ready:
        indicators.append("ready_only_under_synthetic")
    if real_ready and not synthetic_ready:
        indicators.append("ready_only_under_real")
    if real_row.get("current_best_family") != synthetic_row.get("current_best_family"):
        indicators.append("family_winner_changed")
    if real_row.get("best_recommendation_decision") != synthetic_row.get("best_recommendation_decision"):
        indicators.append("recommendation_changed")
    if real_row.get("readiness_classification") != synthetic_row.get("readiness_classification"):
        indicators.append("readiness_changed")
    return indicators


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _csv_value(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True)
    return value

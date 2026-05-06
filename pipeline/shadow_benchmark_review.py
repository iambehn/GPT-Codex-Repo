from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.clip_registry import query_clip_registry
from pipeline.shadow_benchmark_matrix import SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION
from pipeline.shadow_ranking_replay import _load_json, _resolve_path


REPO_ROOT = Path(__file__).resolve().parent.parent
SHADOW_BENCHMARK_REVIEW_SCHEMA_VERSION = "shadow_benchmark_review_v1"
DEFAULT_REVIEW_OUTPUT_ROOT = REPO_ROOT / "outputs" / "shadow_benchmark_reviews"


def review_shadow_benchmark_results(
    benchmark_manifests: list[str | Path],
    *,
    output_path: str | Path | None = None,
    training_target: str | None = None,
    model_family: str | None = None,
    game: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    manifest_paths = [_resolve_path(item) for item in benchmark_manifests]
    target_rows: list[dict[str, Any]] = []
    source_benchmarks: list[str] = []
    warnings: list[dict[str, Any]] = []

    for manifest_path in manifest_paths:
        payload = _load_json(manifest_path)
        if payload is None:
            warnings.append({"code": "invalid_benchmark_manifest", "benchmark_manifest_path": str(manifest_path)})
            continue
        if payload.get("schema_version") != SHADOW_BENCHMARK_MATRIX_SCHEMA_VERSION:
            warnings.append(
                {
                    "code": "unsupported_benchmark_schema",
                    "benchmark_manifest_path": str(manifest_path),
                    "detail": str(payload.get("schema_version")),
                }
            )
            continue
        source_benchmarks.append(str(manifest_path))
        for row in list(payload.get("runs", [])):
            if not isinstance(row, dict):
                continue
            if training_target is not None and str(row.get("training_target") or "") != training_target:
                continue
            if model_family is not None and str(row.get("model_family") or "") != model_family:
                continue
            if game is not None and str(payload.get("benchmark_config", {}).get("filters", {}).get("game") or "").strip() not in {"", game}:
                continue
            if platform is not None and str(payload.get("benchmark_config", {}).get("filters", {}).get("platform") or "").strip() not in {"", platform}:
                continue
            target_rows.append(
                {
                    **row,
                    "benchmark_manifest_path": str(manifest_path),
                    "game": str(payload.get("benchmark_config", {}).get("filters", {}).get("game") or "").strip() or None,
                    "platform": str(payload.get("benchmark_config", {}).get("filters", {}).get("platform") or "").strip() or None,
                }
            )

    review_targets = _review_targets(target_rows)
    review_id = _review_id(source_benchmarks=source_benchmarks, filters={"training_target": training_target, "model_family": model_family, "game": game, "platform": platform})
    artifact = {
        "ok": True,
        "status": "ok",
        "schema_version": SHADOW_BENCHMARK_REVIEW_SCHEMA_VERSION,
        "review_id": review_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source_benchmark_manifest_paths": source_benchmarks,
        "reviewed_targets": [row["training_target"] for row in review_targets],
        "reviewed_families": sorted({str(row.get("current_best_family") or "") for row in review_targets if str(row.get("current_best_family") or "")}),
        "filters": {key: value for key, value in {"training_target": training_target, "model_family": model_family, "game": game, "platform": platform}.items() if value is not None},
        "target_reviews": review_targets,
        "aggregate_conclusions": _aggregate_conclusions(review_targets),
        "recommended_follow_up_actions": _follow_up_actions(review_targets),
        "warning_count": len(warnings),
        "warnings": warnings,
    }
    target = _default_review_output_path(review_id) if output_path is None else _resolve_path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    csv_path = target.with_suffix(".csv")
    _write_csv(csv_path, review_targets)
    artifact["manifest_path"] = str(target)
    artifact["csv_path"] = str(csv_path)
    return artifact


def summarize_shadow_target_readiness(
    review_manifest: str | Path | None = None,
    *,
    registry_path: str | Path | None = None,
    training_target: str | None = None,
    game: str | None = None,
    platform: str | None = None,
    model_family: str | None = None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]]
    if review_manifest is not None:
        manifest_path = _resolve_path(review_manifest)
        payload = _load_json(manifest_path)
        if payload is None:
            return {
                "ok": False,
                "status": "invalid_shadow_benchmark_review",
                "review_manifest_path": str(manifest_path),
                "error": "shadow benchmark review manifest is missing or malformed",
            }
        if payload.get("schema_version") != SHADOW_BENCHMARK_REVIEW_SCHEMA_VERSION:
            return {
                "ok": False,
                "status": "unsupported_shadow_benchmark_review",
                "review_manifest_path": str(manifest_path),
                "error": f"unsupported review schema version: {payload.get('schema_version')}",
            }
        rows = [row for row in list(payload.get("target_reviews", [])) if isinstance(row, dict)]
    else:
        registry_payload = query_clip_registry(
            mode="shadow-target-readiness",
            registry_path=registry_path,
            training_target=training_target,
            game=game,
            platform=platform,
            model_family=model_family,
        )
        if not registry_payload.get("ok"):
            return registry_payload
        rows = [row for row in list(registry_payload.get("rows", [])) if isinstance(row, dict)]

    filtered = []
    for row in rows:
        if training_target is not None and str(row.get("training_target") or "") != training_target:
            continue
        if game is not None and str(row.get("game") or "").strip() not in {"", game}:
            continue
        if platform is not None and str(row.get("platform") or "").strip() not in {"", platform}:
            continue
        if model_family is not None and str(row.get("current_best_family") or "") != model_family:
            continue
        filtered.append(row)
    return {
        "ok": True,
        "status": "ok",
        "row_count": len(filtered),
        "rows": filtered,
    }


def _review_targets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("training_target") or "unknown"), []).append(row)

    reviews: list[dict[str, Any]] = []
    for training_target, bucket in sorted(grouped.items()):
        ok_rows = [row for row in bucket if str(row.get("status") or "") == "ok"]
        prefer_shadow_rows = [row for row in ok_rows if str(row.get("recommendation_decision") or "") == "prefer_shadow"]
        keep_current_rows = [row for row in ok_rows if str(row.get("recommendation_decision") or "") == "keep_current"]
        blocked_rows = [row for row in ok_rows if str(row.get("recommendation_decision") or "") == "blocked_by_policy"]
        inconclusive_rows = [row for row in ok_rows if str(row.get("recommendation_decision") or "") == "inconclusive"]
        failed_rows = [row for row in bucket if str(row.get("status") or "") != "ok"]
        current_best = _best_run(ok_rows)
        dominant_failure_modes = _dominant_failure_modes(bucket)
        readiness = _readiness_classification(
            training_target=training_target,
            total_rows=len(bucket),
            prefer_shadow_count=len(prefer_shadow_rows),
            keep_current_count=len(keep_current_rows),
            blocked_count=len(blocked_rows),
            inconclusive_count=len(inconclusive_rows),
            failed_count=len(failed_rows),
            dominant_failure_modes=dominant_failure_modes,
        )
        confidence = _confidence_level(
            total_rows=len(bucket),
            prefer_shadow_count=len(prefer_shadow_rows),
            blocked_count=len(blocked_rows),
            inconclusive_count=len(inconclusive_rows),
            failed_count=len(failed_rows),
        )
        reviews.append(
            {
                "training_target": training_target,
                "current_best_family": current_best.get("model_family"),
                "best_recommendation_decision": current_best.get("recommendation_decision"),
                "current_best_evidence_mode": current_best.get("evidence_mode"),
                "evidence_modes": sorted({str(row.get("evidence_mode") or "") for row in ok_rows if str(row.get("evidence_mode") or "")}),
                "synthetic_augmented_run_count": sum(1 for row in ok_rows if str(row.get("evidence_mode") or "") == "synthetic_augmented"),
                "real_only_run_count": sum(1 for row in ok_rows if str(row.get("evidence_mode") or "") == "real_only"),
                "primary_metric_name": current_best.get("primary_metric_name"),
                "primary_metric_delta": current_best.get("primary_metric_delta"),
                "run_count": len(bucket),
                "successful_run_count": len(ok_rows),
                "win_count": len(prefer_shadow_rows),
                "keep_current_count": len(keep_current_rows),
                "blocked_count": len(blocked_rows),
                "inconclusive_count": len(inconclusive_rows),
                "failed_count": len(failed_rows),
                "dominant_failure_modes": dominant_failure_modes,
                "confidence_level": confidence,
                "readiness_classification": readiness["readiness_classification"],
                "recommended_next_action": readiness["recommended_next_action"],
                "game": current_best.get("game"),
                "platform": current_best.get("platform"),
            }
        )
    return reviews


def _best_run(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    return sorted(
        rows,
        key=lambda row: (
            -_recommendation_priority(str(row.get("recommendation_decision") or "")),
            -float(row.get("primary_metric_delta") or 0.0),
            str(row.get("model_family") or ""),
        ),
    )[0]


def _dominant_failure_modes(rows: list[dict[str, Any]]) -> list[str]:
    counts: dict[str, int] = {}
    for row in rows:
        if str(row.get("status") or "") != "ok":
            failure = str(row.get("failure_reason") or "failed_run")
            counts[failure] = counts.get(failure, 0) + 1
        for reason in list(row.get("blocking_reasons", [])) if isinstance(row.get("blocking_reasons"), list) else []:
            key = str(reason or "").strip()
            if key:
                counts[key] = counts.get(key, 0) + 1
    return [key for key, _value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:3]]


def _readiness_classification(
    *,
    training_target: str,
    total_rows: int,
    prefer_shadow_count: int,
    keep_current_count: int,
    blocked_count: int,
    inconclusive_count: int,
    failed_count: int,
    dominant_failure_modes: list[str],
) -> dict[str, str]:
    if total_rows == 0 or prefer_shadow_count == 0 and inconclusive_count >= max(1, total_rows // 2):
        return {
            "readiness_classification": "not_ready_due_to_coverage",
            "recommended_next_action": "defer_target",
        }
    if blocked_count > 0:
        action = "refine_labels" if any(reason == "insufficient_coverage" or reason == "insufficient_positive_labels" or reason.startswith("platform=") for reason in dominant_failure_modes) else "prune_features"
        return {
            "readiness_classification": "needs_label_calibration" if action == "refine_labels" else "needs_feature_cleanup",
            "recommended_next_action": action,
        }
    if training_target == "post_performance_score" and keep_current_count > 0 and prefer_shadow_count == 0:
        return {
            "readiness_classification": "needs_feature_cleanup",
            "recommended_next_action": "prune_features",
        }
    if failed_count > 0 and prefer_shadow_count == 0:
        return {
            "readiness_classification": "needs_label_calibration",
            "recommended_next_action": "refine_labels",
        }
    return {
        "readiness_classification": "ready_for_next_iteration",
        "recommended_next_action": "keep_target_as_is",
    }


def _confidence_level(
    *,
    total_rows: int,
    prefer_shadow_count: int,
    blocked_count: int,
    inconclusive_count: int,
    failed_count: int,
) -> str:
    if total_rows == 0 or inconclusive_count >= total_rows:
        return "low"
    if prefer_shadow_count > 0 and blocked_count == 0 and failed_count == 0:
        return "high"
    if prefer_shadow_count > 0:
        return "medium"
    return "low"


def _aggregate_conclusions(target_reviews: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "target_count": len(target_reviews),
        "ready_target_count": sum(1 for row in target_reviews if row.get("readiness_classification") == "ready_for_next_iteration"),
        "label_calibration_target_count": sum(1 for row in target_reviews if row.get("readiness_classification") == "needs_label_calibration"),
        "feature_cleanup_target_count": sum(1 for row in target_reviews if row.get("readiness_classification") == "needs_feature_cleanup"),
        "coverage_blocked_target_count": sum(1 for row in target_reviews if row.get("readiness_classification") == "not_ready_due_to_coverage"),
    }


def _follow_up_actions(target_reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions = []
    for row in target_reviews:
        actions.append(
            {
                "training_target": row.get("training_target"),
                "recommended_next_action": row.get("recommended_next_action"),
                "readiness_classification": row.get("readiness_classification"),
                "current_best_family": row.get("current_best_family"),
            }
        )
    return actions


def _recommendation_priority(decision: str) -> int:
    if decision == "prefer_shadow":
        return 4
    if decision == "inconclusive":
        return 3
    if decision == "keep_current":
        return 2
    if decision == "blocked_by_policy":
        return 1
    return 0


def _review_id(*, source_benchmarks: list[str], filters: dict[str, Any]) -> str:
    payload = json.dumps({"source_benchmarks": source_benchmarks, "filters": filters}, sort_keys=True)
    return f"shadow-review-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _default_review_output_path(review_id: str) -> Path:
    return DEFAULT_REVIEW_OUTPUT_ROOT / f"{review_id}.shadow_benchmark_review.json"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})

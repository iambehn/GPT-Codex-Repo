from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION = "evaluation_fixture_manifest_v1"
DEFAULT_EVALUATION_FIXTURE_MANIFEST_PATH = REPO_ROOT / "assets" / "evaluation" / "fixture_manifest.json"


def load_evaluation_fixture_manifest(path: str | Path | None = None) -> dict[str, Any]:
    manifest_path = _resolve_path(path or DEFAULT_EVALUATION_FIXTURE_MANIFEST_PATH)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return _validate_manifest(payload, manifest_path)


def _validate_manifest(payload: Any, manifest_path: Path) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("evaluation fixture manifest must be a mapping")
    if payload.get("schema_version") != EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            f"evaluation fixture manifest must use {EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION}"
        )
    fixtures = payload.get("fixtures")
    if not isinstance(fixtures, list) or not fixtures:
        raise ValueError("evaluation fixture manifest must include a non-empty fixtures list")

    fixture_ids: set[str] = set()
    validated: list[dict[str, Any]] = []
    for index, row in enumerate(fixtures):
        if not isinstance(row, dict):
            raise ValueError(f"fixture at index {index} must be a mapping")
        fixture_id = str(row.get("fixture_id", "")).strip()
        if not fixture_id:
            raise ValueError(f"fixture at index {index} is missing fixture_id")
        if fixture_id in fixture_ids:
            raise ValueError(f"fixture_id '{fixture_id}' is duplicated")
        fixture_ids.add(fixture_id)
        expected_review = str(row.get("expected_review_outcome", "")).strip().lower()
        if expected_review not in {"approved", "rejected"}:
            raise ValueError(
                f"fixture '{fixture_id}' must set expected_review_outcome to approved or rejected"
            )
        latency_budget = str(row.get("latency_budget_class", "")).strip().lower()
        if latency_budget not in {"smoke", "integration", "golden", "slow"}:
            raise ValueError(
                f"fixture '{fixture_id}' must set latency_budget_class to smoke, integration, golden, or slow"
            )
        artifact_refs = row.get("artifact_refs", {})
        if not isinstance(artifact_refs, dict):
            raise ValueError(f"fixture '{fixture_id}' artifact_refs must be a mapping")
        expected_artifacts = row.get("expected_artifacts", {})
        if expected_artifacts is None:
            expected_artifacts = {}
        if not isinstance(expected_artifacts, dict):
            raise ValueError(f"fixture '{fixture_id}' expected_artifacts must be a mapping")
        normalized_expected_artifacts: dict[str, bool] = {}
        for key, value in expected_artifacts.items():
            normalized_key = str(key).strip().lower()
            if normalized_key not in {"proxy", "runtime", "fused"}:
                raise ValueError(
                    f"fixture '{fixture_id}' expected_artifacts keys must be proxy, runtime, or fused"
                )
            normalized_expected_artifacts[normalized_key] = bool(value)
        validated.append(
            {
                "fixture_id": fixture_id,
                "label": str(row.get("label", fixture_id)).strip(),
                "task_intent": str(row.get("task_intent", "")).strip(),
                "expected_review_outcome": expected_review,
                "latency_budget_class": latency_budget,
                "notes": str(row.get("notes", "")).strip(),
                "artifact_refs": {
                    str(key): str(value).strip()
                    for key, value in artifact_refs.items()
                    if str(value).strip()
                },
                "expected_artifacts": normalized_expected_artifacts,
            }
        )

    return {
        "schema_version": EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION,
        "manifest_path": str(manifest_path),
        "fixtures": validated,
        "fixture_count": len(validated),
    }


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path

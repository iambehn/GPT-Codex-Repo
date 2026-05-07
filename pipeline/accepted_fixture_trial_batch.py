from __future__ import annotations

import fnmatch
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from pipeline.evaluation_fixtures import EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION
from pipeline.fixture_source_manifest import FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION, load_fixture_source_manifest


REPO_ROOT = Path(__file__).resolve().parent.parent
ACCEPTED_FIXTURE_TRIAL_BATCH_SCHEMA_VERSION = "accepted_fixture_trial_batch_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "accepted_fixture_trials"


def run_accepted_fixture_trial_batch(
    fixture_source_manifest: str | Path,
    *,
    trial_runner: Callable[..., dict[str, Any]],
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
    game: str | None = None,
    pattern: str | None = None,
    limit: int | None = None,
    emit_runtime: bool = False,
    emit_fused: bool = False,
) -> dict[str, Any]:
    manifest_path = _resolve_path(fixture_source_manifest)
    if not manifest_path.exists() or not manifest_path.is_file():
        return {
            "ok": False,
            "status": "invalid_fixture_source_manifest",
            "fixture_source_manifest": str(manifest_path),
            "error": "fixture source manifest does not exist or is not a file",
        }

    try:
        raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "ok": False,
            "status": "invalid_fixture_source_manifest",
            "fixture_source_manifest": str(manifest_path),
            "error": "fixture source manifest is not readable JSON",
        }

    if not isinstance(raw_manifest, dict):
        return {
            "ok": False,
            "status": "invalid_fixture_source_manifest",
            "fixture_source_manifest": str(manifest_path),
            "error": "fixture source manifest must be a mapping",
        }
    if raw_manifest.get("schema_version") != FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_fixture_source_manifest",
            "fixture_source_manifest": str(manifest_path),
            "error": f"fixture source manifest must use {FIXTURE_SOURCE_MANIFEST_SCHEMA_VERSION}",
        }

    source_manifest = load_fixture_source_manifest(manifest_path)
    fixtures = list(source_manifest.get("fixtures", []))
    if not fixtures:
        return {
            "ok": True,
            "status": "no_rows",
            "schema_version": ACCEPTED_FIXTURE_TRIAL_BATCH_SCHEMA_VERSION,
            "batch_id": _batch_id(str(manifest_path), str(raw_manifest.get("adapted_manifest_id") or ""), game or "", []),
            "created_at": datetime.now(UTC).isoformat(),
            "source_manifest_path": str(manifest_path),
            "source_manifest_id": raw_manifest.get("adapted_manifest_id"),
            "game": game,
            "fixture_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "results": [],
            "manifest_path": None,
        }

    batch_game = game or _infer_game(fixtures)
    selected_fixtures = _select_fixtures(fixtures, game=batch_game, pattern=pattern, limit=limit)
    batch_id = _batch_id(
        str(manifest_path),
        str(raw_manifest.get("adapted_manifest_id") or ""),
        str(batch_game or ""),
        selected_fixtures,
    )
    if not selected_fixtures:
        return {
            "ok": True,
            "status": "no_rows",
            "schema_version": ACCEPTED_FIXTURE_TRIAL_BATCH_SCHEMA_VERSION,
            "batch_id": batch_id,
            "created_at": datetime.now(UTC).isoformat(),
            "source_manifest_path": str(manifest_path),
            "source_manifest_id": raw_manifest.get("adapted_manifest_id"),
            "game": batch_game,
            "fixture_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "results": [],
            "manifest_path": None,
        }
    batch_root = _resolve_batch_root(output_root, batch_id=batch_id, game=str(batch_game or "unknown"))
    generated_manifest_root = batch_root / "generated_fixture_manifests"
    results: list[dict[str, Any]] = []

    for fixture in selected_fixtures:
        synthetic_fixture_manifest = _write_single_fixture_manifest(
            generated_manifest_root / f"{fixture['fixture_id']}.fixture_manifest.json",
            fixture,
            emit_runtime=emit_runtime,
            emit_fused=emit_fused,
        )
        trial_name = f"accepted-{fixture['fixture_id']}"
        try:
            trial_result = trial_runner(
                synthetic_fixture_manifest,
                fixture_source_manifest=manifest_path,
                trial_name=trial_name,
                output_root=batch_root / "runs",
                game=batch_game,
                pattern=fixture["fixture_id"],
                limit=1,
                proposal_backend=None,
                asr_backend=None,
                emit_runtime=emit_runtime,
                emit_fused=emit_fused,
            )
            results.append(_result_row(fixture, trial_result=trial_result))
        except Exception as exc:  # pragma: no cover - defensive runtime capture
            results.append(
                {
                    "fixture_id": fixture["fixture_id"],
                    "game": fixture["game"],
                    "source_path": fixture["source_path"],
                    "status": "failed",
                    "error": str(exc),
                    "proxy_sidecar_path": None,
                    "runtime_sidecar_path": None,
                    "fused_sidecar_path": None,
                    "trial_result_path": None,
                }
            )

    success_count = sum(1 for row in results if row.get("status") == "ok")
    failed_count = len(results) - success_count
    if success_count == len(results):
        status = "ok"
        ok = True
    elif success_count > 0:
        status = "partial"
        ok = True
    else:
        status = "failed"
        ok = False

    final_output_path = _default_output_path(
        output_root=output_root,
        output_path=output_path,
        game=str(batch_game or "unknown"),
        batch_id=batch_id,
    )
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ok": ok,
        "status": status,
        "schema_version": ACCEPTED_FIXTURE_TRIAL_BATCH_SCHEMA_VERSION,
        "batch_id": batch_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source_manifest_path": str(manifest_path),
        "source_manifest_id": raw_manifest.get("adapted_manifest_id"),
        "game": batch_game,
        "fixture_count": len(results),
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
        "manifest_path": str(final_output_path),
    }
    final_output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def _select_fixtures(
    fixtures: list[dict[str, Any]],
    *,
    game: str | None,
    pattern: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    selected = fixtures
    if game is not None:
        selected = [row for row in selected if str(row.get("game", "")).strip() == str(game).strip()]
    if pattern:
        selected = [
            row for row in selected
            if _matches_pattern(pattern, row)
        ]
    if limit is not None:
        selected = selected[:limit]
    return selected


def _matches_pattern(pattern: str, row: dict[str, Any]) -> bool:
    pattern_text = str(pattern).strip()
    if not pattern_text:
        return True
    fixture_id = str(row.get("fixture_id", "")).strip()
    source_path = str(row.get("source_path", "")).strip()
    source_name = Path(source_path).name if source_path else ""
    haystacks = [fixture_id, source_path, source_name]
    lowered_pattern = pattern_text.lower()
    return any(
        fnmatch.fnmatchcase(item.lower(), lowered_pattern) or lowered_pattern in item.lower()
        for item in haystacks
        if item
    )


def _write_single_fixture_manifest(
    path: Path,
    fixture: dict[str, Any],
    *,
    emit_runtime: bool,
    emit_fused: bool,
) -> str:
    expected_artifacts = {
        "proxy": True,
        "runtime": bool(emit_runtime or emit_fused),
        "fused": bool(emit_fused),
    }
    payload = {
        "schema_version": EVALUATION_FIXTURE_MANIFEST_SCHEMA_VERSION,
        "fixtures": [
            {
                "fixture_id": fixture["fixture_id"],
                "label": fixture["fixture_id"],
                "task_intent": "accepted_clip_trial",
                "expected_review_outcome": "approved",
                "latency_budget_class": "integration",
                "notes": str(fixture.get("notes", "")).strip(),
                "artifact_refs": {},
                "expected_artifacts": expected_artifacts,
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _result_row(fixture: dict[str, Any], *, trial_result: dict[str, Any]) -> dict[str, Any]:
    fixture_rows = list(trial_result.get("fixtures", []))
    fixture_row = fixture_rows[0] if fixture_rows else {}
    layers = fixture_row.get("layers", {}) if isinstance(fixture_row, dict) else {}
    return {
        "fixture_id": fixture["fixture_id"],
        "game": fixture["game"],
        "source_path": fixture["source_path"],
        "status": "ok" if trial_result.get("ok") and fixture_row.get("status") == "ok" else "failed",
        "error": fixture_row.get("error"),
        "proxy_sidecar_path": (layers.get("proxy") or {}).get("sidecar_path"),
        "runtime_sidecar_path": (layers.get("runtime") or {}).get("sidecar_path"),
        "fused_sidecar_path": (layers.get("fused") or {}).get("sidecar_path"),
        "trial_result_path": trial_result.get("manifest_path"),
    }


def _infer_game(fixtures: list[dict[str, Any]]) -> str | None:
    for fixture in fixtures:
        game = str(fixture.get("game", "")).strip()
        if game:
            return game
    return None


def _batch_id(source_manifest_path: str, source_manifest_id: str, game: str, fixtures: list[dict[str, Any]]) -> str:
    raw = json.dumps(
        {
            "source_manifest_path": source_manifest_path,
            "source_manifest_id": source_manifest_id,
            "game": game,
            "fixture_ids": [row.get("fixture_id") for row in fixtures],
        },
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _resolve_batch_root(output_root: str | Path | None, *, batch_id: str, game: str) -> Path:
    root = DEFAULT_OUTPUT_ROOT if output_root is None else _resolve_path(output_root)
    return root / game / batch_id


def _default_output_path(
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    game: str,
    batch_id: str,
) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    return _resolve_batch_root(output_root, batch_id=batch_id, game=game) / "accepted_fixture_trial_batch.json"


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path

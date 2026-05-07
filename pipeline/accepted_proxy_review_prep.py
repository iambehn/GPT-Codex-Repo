from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from pipeline.proxy_review_bridge import prepare_proxy_review


REPO_ROOT = Path(__file__).resolve().parent.parent
ACCEPTED_FIXTURE_TRIAL_BATCH_SCHEMA_VERSION = "accepted_fixture_trial_batch_v1"
ACCEPTED_PROXY_REVIEW_PREP_SCHEMA_VERSION = "accepted_proxy_review_prep_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "accepted_proxy_reviews"
DEFAULT_ACTION = "download_candidate"


def prepare_accepted_proxy_review(
    accepted_fixture_trial_batch_manifest: str | Path,
    *,
    output_root: str | Path | None = None,
    output_path: str | Path | None = None,
    gpt_repo: str | Path | None = None,
    review_preparer: Callable[..., dict[str, Any]] = prepare_proxy_review,
) -> dict[str, Any]:
    manifest_path = _resolve_path(accepted_fixture_trial_batch_manifest)
    if not manifest_path.exists() or not manifest_path.is_file():
        return {
            "ok": False,
            "status": "invalid_accepted_fixture_trial_batch",
            "accepted_fixture_trial_batch_manifest": str(manifest_path),
            "error": "accepted fixture trial batch manifest does not exist or is not a file",
        }

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "ok": False,
            "status": "invalid_accepted_fixture_trial_batch",
            "accepted_fixture_trial_batch_manifest": str(manifest_path),
            "error": "accepted fixture trial batch manifest is not readable JSON",
        }

    validation_error = _validate_batch_manifest(payload)
    if validation_error is not None:
        return {
            "ok": False,
            "status": validation_error["status"],
            "accepted_fixture_trial_batch_manifest": str(manifest_path),
            "error": validation_error["error"],
        }

    all_rows = list(payload["results"])
    eligible_rows = [row for row in all_rows if _is_eligible(row)]
    review_prep_id = _review_prep_id(
        source_batch_manifest_path=str(manifest_path),
        source_batch_id=str(payload["batch_id"]),
        game=str(payload["game"]),
        rows=all_rows,
    )
    created_at = datetime.now(UTC).isoformat()

    if not eligible_rows:
        results = [_skipped_result(row, error=_skip_reason(row)) for row in all_rows]
        return _write_payload(
            result={
                "ok": True,
                "status": "no_proxy_sidecars",
                "schema_version": ACCEPTED_PROXY_REVIEW_PREP_SCHEMA_VERSION,
                "review_prep_id": review_prep_id,
                "created_at": created_at,
                "source_batch_manifest_path": str(manifest_path),
                "source_batch_id": payload["batch_id"],
                "game": payload["game"],
                "fixture_count": len(all_rows),
                "prepared_count": 0,
                "skipped_count": len(results),
                "proxy_review_session_manifest_path": None,
                "proxy_review_session_id": None,
                "results": results,
            },
            output_root=output_root,
            output_path=output_path,
            game=str(payload["game"]),
            review_prep_id=review_prep_id,
        )

    review_root = _resolve_review_root(
        output_root=output_root,
        game=str(payload["game"]),
        review_prep_id=review_prep_id,
    )
    review_root.mkdir(parents=True, exist_ok=True)
    bridge_batch_report_path = review_root / "bridge_batch_report.json"
    bridge_batch_report = {
        "schema_version": "accepted_proxy_bridge_batch_report_v1",
        "created_at": created_at,
        "source_batch_manifest_path": str(manifest_path),
        "source_batch_id": payload["batch_id"],
        "results": [_bridge_report_row(row) for row in eligible_rows],
    }
    bridge_batch_report_path.write_text(json.dumps(bridge_batch_report, indent=2), encoding="utf-8")

    try:
        bridge_result = review_preparer(
            str(payload["game"]),
            batch_report=bridge_batch_report_path,
            action=DEFAULT_ACTION,
            gpt_repo=gpt_repo,
            session_name=f"accepted-{review_prep_id}",
        )
    except Exception as exc:  # pragma: no cover - defensive runtime capture
        results = [
            (
                _skipped_result(row, error=_skip_reason(row))
                if not _is_eligible(row)
                else {
                    "fixture_id": str(row["fixture_id"]),
                    "proxy_sidecar_path": str(_resolve_path(str(row["proxy_sidecar_path"]))),
                    "status": "failed",
                    "error": str(exc),
                    "prepared_review_path": None,
                }
            )
            for row in all_rows
        ]
        return _write_payload(
            result={
                "ok": False,
                "status": "failed",
                "schema_version": ACCEPTED_PROXY_REVIEW_PREP_SCHEMA_VERSION,
                "review_prep_id": review_prep_id,
                "created_at": created_at,
                "source_batch_manifest_path": str(manifest_path),
                "source_batch_id": payload["batch_id"],
                "game": payload["game"],
                "fixture_count": len(all_rows),
                "prepared_count": 0,
                "skipped_count": sum(1 for row in results if row["status"] == "skipped"),
                "proxy_review_session_manifest_path": None,
                "proxy_review_session_id": None,
                "results": results,
            },
            output_root=output_root,
            output_path=output_path,
            game=str(payload["game"]),
            review_prep_id=review_prep_id,
        )
    sidecar_to_item = {
        str(_resolve_path(str(item["sidecar_path"]))): item
        for item in bridge_result.get("items", [])
        if isinstance(item, dict) and item.get("sidecar_path")
    }

    results: list[dict[str, Any]] = []
    for row in all_rows:
        if not _is_eligible(row):
            results.append(_skipped_result(row, error=_skip_reason(row)))
            continue
        sidecar_path = str(_resolve_path(str(row["proxy_sidecar_path"])))
        prepared_item = sidecar_to_item.get(sidecar_path)
        if prepared_item is None:
            results.append(
                {
                    "fixture_id": str(row["fixture_id"]),
                    "proxy_sidecar_path": sidecar_path,
                    "status": "failed",
                    "error": "proxy review bridge did not return a prepared item for this sidecar",
                    "prepared_review_path": None,
                }
            )
            continue
        prepared_review_path = prepared_item.get("gpt_meta_path")
        results.append(
            {
                "fixture_id": str(row["fixture_id"]),
                "proxy_sidecar_path": sidecar_path,
                "status": "ok",
                "error": None,
                "prepared_review_path": str(prepared_review_path) if prepared_review_path else None,
            }
        )

    prepared_count = sum(1 for row in results if row["status"] == "ok")
    skipped_count = sum(1 for row in results if row["status"] == "skipped")
    failed_count = sum(1 for row in results if row["status"] == "failed")
    if prepared_count == len(eligible_rows) and failed_count == 0:
        status = "ok"
        ok = True
    elif prepared_count > 0:
        status = "partial"
        ok = True
    else:
        status = "failed"
        ok = False

    return _write_payload(
        result={
            "ok": ok,
            "status": status,
            "schema_version": ACCEPTED_PROXY_REVIEW_PREP_SCHEMA_VERSION,
            "review_prep_id": review_prep_id,
            "created_at": created_at,
            "source_batch_manifest_path": str(manifest_path),
            "source_batch_id": payload["batch_id"],
            "game": payload["game"],
            "fixture_count": len(all_rows),
            "prepared_count": prepared_count,
            "skipped_count": skipped_count,
            "proxy_review_session_manifest_path": bridge_result.get("manifest_path"),
            "proxy_review_session_id": bridge_result.get("session_id"),
            "results": results,
        },
        output_root=output_root,
        output_path=output_path,
        game=str(payload["game"]),
        review_prep_id=review_prep_id,
    )


def _validate_batch_manifest(payload: Any) -> dict[str, str] | None:
    if not isinstance(payload, dict):
        return {
            "status": "invalid_accepted_fixture_trial_batch",
            "error": "accepted fixture trial batch manifest must be a mapping",
        }
    if payload.get("schema_version") != ACCEPTED_FIXTURE_TRIAL_BATCH_SCHEMA_VERSION:
        return {
            "status": "invalid_accepted_fixture_trial_batch",
            "error": f"accepted fixture trial batch manifest must use {ACCEPTED_FIXTURE_TRIAL_BATCH_SCHEMA_VERSION}",
        }
    batch_id = str(payload.get("batch_id", "")).strip()
    game = str(payload.get("game", "")).strip()
    rows = payload.get("results")
    if not batch_id:
        return {
            "status": "invalid_accepted_fixture_trial_batch",
            "error": "accepted fixture trial batch manifest is missing batch_id",
        }
    if not game:
        return {
            "status": "invalid_accepted_fixture_trial_batch",
            "error": "accepted fixture trial batch manifest is missing game",
        }
    if not isinstance(rows, list):
        return {
            "status": "invalid_accepted_fixture_trial_batch",
            "error": "accepted fixture trial batch results must be a list",
        }
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            return {
                "status": "invalid_accepted_fixture_trial_batch",
                "error": f"accepted fixture trial batch result at index {index} must be a mapping",
            }
        fixture_id = str(row.get("fixture_id", "")).strip()
        status = str(row.get("status", "")).strip()
        if not fixture_id:
            return {
                "status": "invalid_accepted_fixture_trial_batch",
                "error": f"accepted fixture trial batch result at index {index} is missing fixture_id",
            }
        if not status:
            return {
                "status": "invalid_accepted_fixture_trial_batch",
                "error": f"accepted fixture trial batch result at index {index} is missing status",
            }
    return None


def _is_eligible(row: dict[str, Any]) -> bool:
    return str(row.get("status", "")).strip() == "ok" and _optional_text(row.get("proxy_sidecar_path")) is not None


def _skip_reason(row: dict[str, Any]) -> str:
    if str(row.get("status", "")).strip() != "ok":
        return f"fixture trial batch row status is {row.get('status')}"
    if _optional_text(row.get("proxy_sidecar_path")) is None:
        return "proxy sidecar path is missing"
    return "row was not selected for proxy review prep"


def _skipped_result(row: dict[str, Any], *, error: str) -> dict[str, Any]:
    sidecar_path = _optional_text(row.get("proxy_sidecar_path"))
    return {
        "fixture_id": str(row.get("fixture_id", "")),
        "proxy_sidecar_path": str(_resolve_path(sidecar_path)) if sidecar_path else None,
        "status": "skipped",
        "error": error,
        "prepared_review_path": None,
    }


def _bridge_report_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "fixture_id": row["fixture_id"],
        "sidecar_path": str(_resolve_path(str(row["proxy_sidecar_path"]))),
        "top_recommended_action": DEFAULT_ACTION,
        "top_proxy_score": 0.0,
        "sources": [],
        "source_families": [],
    }
    source_path = _optional_text(row.get("source_path"))
    if source_path is not None:
        payload["source"] = str(_resolve_path(source_path))
    return payload


def _write_payload(
    *,
    result: dict[str, Any],
    output_root: str | Path | None,
    output_path: str | Path | None,
    game: str,
    review_prep_id: str,
) -> dict[str, Any]:
    final_output_path = _default_output_path(
        output_root=output_root,
        output_path=output_path,
        game=game,
        review_prep_id=review_prep_id,
    )
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    result["manifest_path"] = str(final_output_path)
    final_output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def _review_prep_id(
    *,
    source_batch_manifest_path: str,
    source_batch_id: str,
    game: str,
    rows: list[dict[str, Any]],
) -> str:
    raw = json.dumps(
        {
            "source_batch_manifest_path": source_batch_manifest_path,
            "source_batch_id": source_batch_id,
            "game": game,
            "fixture_ids": [str(row.get("fixture_id", "")).strip() for row in rows],
            "proxy_sidecar_paths": [str(row.get("proxy_sidecar_path", "")).strip() for row in rows],
        },
        sort_keys=True,
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _resolve_review_root(*, output_root: str | Path | None, game: str, review_prep_id: str) -> Path:
    root = _resolve_path(output_root) if output_root is not None else DEFAULT_OUTPUT_ROOT
    return root / game / review_prep_id


def _default_output_path(
    *,
    output_root: str | Path | None,
    output_path: str | Path | None,
    game: str,
    review_prep_id: str,
) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    return _resolve_review_root(output_root=output_root, game=game, review_prep_id=review_prep_id) / "accepted_proxy_review_prep.json"


def _resolve_path(value: str | Path | None) -> Path:
    return Path(value).expanduser().resolve() if value is not None else Path.cwd().resolve()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    return text

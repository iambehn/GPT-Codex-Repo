from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.detector_calibration_next_action_apply import (
    apply_detector_calibration_next_action,
    _load_json,
    _resolve_path,
    _validate_next_actions_manifest,
)

LEDGER_SCHEMA_VERSION = "detector_calibration_next_action_apply_ledger_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "detector_calibration"


def _build_emitted_batch(
    *,
    source_next_actions_manifest_path: Path,
    selected_row_count: int,
    applied_row_count: int,
    created_count: int,
    reused_count: int,
    failed_count: int,
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    top_result = None
    for result in results:
        if result.get("ok"):
            top_result = {
                "asset_id": result.get("asset_id"),
                "applied_action": result.get("applied_action"),
                "reuse_mode": result.get("reuse_mode"),
                "expansion_manifest_path": result.get("expansion_manifest_path"),
            }
            break
    failed_asset_ids = [
        str(result.get("asset_id") or "").strip()
        for result in results
        if not result.get("ok") and str(result.get("asset_id") or "").strip()
    ]
    return {
        "source_next_actions_manifest_path": str(source_next_actions_manifest_path),
        "selected_row_count": selected_row_count,
        "applied_row_count": applied_row_count,
        "created_count": created_count,
        "reused_count": reused_count,
        "failed_count": failed_count,
        "top_result": top_result,
        "failed_asset_ids": failed_asset_ids,
    }


def _build_ledger_row(*, run_id: str, recorded_at: str, batch_result: dict[str, Any]) -> dict[str, Any]:
    emitted_batch = dict(batch_result.get("emitted_batch") or {})
    top_result = emitted_batch.get("top_result") if isinstance(emitted_batch.get("top_result"), dict) else None
    return {
        "run_id": run_id,
        "recorded_at": recorded_at,
        "source_next_actions_manifest_path": batch_result.get("source_next_actions_manifest_path"),
        "status": batch_result.get("status"),
        "ok": bool(batch_result.get("ok")),
        "selected_row_count": int(batch_result.get("selected_row_count") or 0),
        "applied_row_count": int(batch_result.get("applied_row_count") or 0),
        "created_count": int(batch_result.get("created_count") or 0),
        "reused_count": int(batch_result.get("reused_count") or 0),
        "failed_count": int(batch_result.get("failed_count") or 0),
        "top_asset_id": top_result.get("asset_id") if top_result else None,
        "failed_asset_ids": list(emitted_batch.get("failed_asset_ids") or []),
        "top_expansion_manifest_path": top_result.get("expansion_manifest_path") if top_result else None,
    }


def _default_ledger_path(*, game: str) -> Path:
    return (
        DEFAULT_OUTPUT_ROOT
        / game
        / "batch_apply_history"
        / "detector_calibration_next_action_apply_ledger.json"
    )


def _load_or_initialize_ledger(*, ledger_path: Path, game: str) -> dict[str, Any]:
    if ledger_path.exists():
        payload = _load_json(ledger_path)
        if str(payload.get("schema_version") or "").strip() != LEDGER_SCHEMA_VERSION:
            raise ValueError("batch apply ledger schema_version is invalid")
        if str(payload.get("game") or "").strip() != game:
            raise ValueError("batch apply ledger game does not match requested game")
        if not isinstance(payload.get("rows"), list):
            raise ValueError("batch apply ledger rows must be a list")
        return payload
    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "game": game,
        "updated_at": None,
        "row_count": 0,
        "rows": [],
    }


def _record_batch_ledger(
    *,
    batch_result: dict[str, Any],
    ledger_path: str | Path | None,
    game: str | None,
) -> tuple[Path, str]:
    resolved_game = str(game or "").strip()
    resolved_ledger_path = _resolve_path(ledger_path) if ledger_path is not None else None
    if not resolved_game:
        if resolved_ledger_path is not None and resolved_ledger_path.exists():
            existing = _load_json(resolved_ledger_path)
            resolved_game = str(existing.get("game") or "").strip()
        if not resolved_game:
            raise ValueError("game could not be resolved for batch apply ledger recording")
    if resolved_ledger_path is None:
        resolved_ledger_path = _default_ledger_path(game=resolved_game)
    now = datetime.now(UTC)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    recorded_at = now.isoformat().replace("+00:00", "Z")
    ledger = _load_or_initialize_ledger(ledger_path=resolved_ledger_path, game=resolved_game)
    ledger["rows"].append(_build_ledger_row(run_id=run_id, recorded_at=recorded_at, batch_result=batch_result))
    ledger["updated_at"] = recorded_at
    ledger["row_count"] = len(ledger["rows"])
    resolved_ledger_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_ledger_path.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
    return resolved_ledger_path, run_id


def apply_detector_calibration_next_actions_batch(
    *,
    next_actions_manifest: str | Path,
    publish_decision_manifest: str | Path | None = None,
    evidence_expansion_root: str | Path | None = None,
    record_ledger: bool = False,
    ledger_path: str | Path | None = None,
) -> dict[str, Any]:
    next_actions_path = _resolve_path(next_actions_manifest)
    game: str | None = None
    try:
        next_actions_payload = _load_json(next_actions_path)
        _validate_next_actions_manifest(next_actions_payload)
        game = str(next_actions_payload.get("game") or "").strip() or None
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        emitted_batch = _build_emitted_batch(
            source_next_actions_manifest_path=next_actions_path,
            selected_row_count=0,
            applied_row_count=0,
            created_count=0,
            reused_count=0,
            failed_count=0,
            results=[],
        )
        result = {
            "ok": False,
            "status": "invalid_next_actions_manifest",
            "source_next_actions_manifest_path": str(next_actions_path),
            "error": str(exc),
            "selected_row_count": 0,
            "applied_row_count": 0,
            "created_count": 0,
            "reused_count": 0,
            "failed_count": 0,
            "results": [],
            "emitted_batch": emitted_batch,
        }
        if record_ledger:
            try:
                resolved_ledger_path, ledger_run_id = _record_batch_ledger(
                    batch_result=result,
                    ledger_path=ledger_path,
                    game=game,
                )
            except (OSError, json.JSONDecodeError, ValueError) as ledger_exc:
                result["ok"] = False
                result["status"] = "ledger_record_failed"
                result["error"] = str(ledger_exc)
                return result
            result["ledger_recorded"] = True
            result["ledger_path"] = str(resolved_ledger_path)
            result["ledger_run_id"] = ledger_run_id
        return result

    selected_rows: list[tuple[int, dict[str, Any]]] = []
    for row_index, row in enumerate(next_actions_payload.get("rows") or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("action_status") or "").strip() != "collect_more_evidence":
            continue
        selected_rows.append((row_index, row))

    if not selected_rows:
        emitted_batch = _build_emitted_batch(
            source_next_actions_manifest_path=next_actions_path,
            selected_row_count=0,
            applied_row_count=0,
            created_count=0,
            reused_count=0,
            failed_count=0,
            results=[],
        )
        result = {
            "ok": False,
            "status": "no_actionable_rows",
            "source_next_actions_manifest_path": str(next_actions_path),
            "selected_row_count": 0,
            "applied_row_count": 0,
            "created_count": 0,
            "reused_count": 0,
            "failed_count": 0,
            "results": [],
            "emitted_batch": emitted_batch,
        }
        if record_ledger:
            try:
                resolved_ledger_path, ledger_run_id = _record_batch_ledger(
                    batch_result=result,
                    ledger_path=ledger_path,
                    game=game,
                )
            except (OSError, json.JSONDecodeError, ValueError) as ledger_exc:
                result["ok"] = False
                result["status"] = "ledger_record_failed"
                result["error"] = str(ledger_exc)
                return result
            result["ledger_recorded"] = True
            result["ledger_path"] = str(resolved_ledger_path)
            result["ledger_run_id"] = ledger_run_id
        return result

    results: list[dict[str, Any]] = []
    applied_row_count = 0
    created_count = 0
    reused_count = 0
    failed_count = 0
    for row_index, row in selected_rows:
        asset_id = str(row.get("asset_id") or "").strip()
        result = apply_detector_calibration_next_action(
            next_actions_manifest=next_actions_path,
            asset_id=asset_id,
            publish_decision_manifest=publish_decision_manifest,
            evidence_expansion_root=evidence_expansion_root,
        )
        row_result = dict(result)
        row_result["asset_id"] = asset_id
        row_result["row_index"] = row_index
        results.append(row_result)
        if result.get("ok"):
            applied_row_count += 1
            reuse_mode = str(result.get("reuse_mode") or "").strip()
            if reuse_mode == "created_new":
                created_count += 1
            elif reuse_mode == "reused_existing":
                reused_count += 1
        else:
            failed_count += 1

    if applied_row_count == 0:
        status = "partial_failure"
        ok = False
    elif failed_count > 0:
        status = "partial_failure"
        ok = True
    else:
        status = "ok"
        ok = True

    emitted_batch = _build_emitted_batch(
        source_next_actions_manifest_path=next_actions_path,
        selected_row_count=len(selected_rows),
        applied_row_count=applied_row_count,
        created_count=created_count,
        reused_count=reused_count,
        failed_count=failed_count,
        results=results,
    )

    batch_result = {
        "ok": ok,
        "status": status,
        "source_next_actions_manifest_path": str(next_actions_path),
        "selected_row_count": len(selected_rows),
        "applied_row_count": applied_row_count,
        "created_count": created_count,
        "reused_count": reused_count,
        "failed_count": failed_count,
        "results": results,
        "emitted_batch": emitted_batch,
    }
    if not record_ledger:
        return batch_result
    try:
        resolved_ledger_path, ledger_run_id = _record_batch_ledger(
            batch_result=batch_result,
            ledger_path=ledger_path,
            game=game,
        )
    except (OSError, json.JSONDecodeError, ValueError) as ledger_exc:
        batch_result["ok"] = False
        batch_result["status"] = "ledger_record_failed"
        batch_result["error"] = str(ledger_exc)
        return batch_result
    batch_result["ledger_recorded"] = True
    batch_result["ledger_path"] = str(resolved_ledger_path)
    batch_result["ledger_run_id"] = ledger_run_id
    return batch_result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply every actionable detector calibration next-action row into evidence-expansion work"
    )
    parser.add_argument("--next-actions-manifest", required=True)
    parser.add_argument("--publish-decision-manifest")
    parser.add_argument("--evidence-expansion-root")
    parser.add_argument("--record-ledger", action="store_true")
    parser.add_argument("--ledger-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = apply_detector_calibration_next_actions_batch(
        next_actions_manifest=args.next_actions_manifest,
        publish_decision_manifest=args.publish_decision_manifest,
        evidence_expansion_root=args.evidence_expansion_root,
        record_ledger=bool(args.record_ledger),
        ledger_path=args.ledger_path,
    )
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

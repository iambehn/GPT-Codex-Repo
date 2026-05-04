from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

from pipeline.evaluation_fixtures import load_evaluation_fixture_manifest
from pipeline.unified_replay_viewer import render_unified_replay_viewer


def load_highlight_review_records(
    *,
    sidecar_root: str | Path | None = None,
    fixture_manifest_path: str | Path | None = None,
    fixture_comparison_report: str | Path | None = None,
    fixture_trial_batch_manifest: str | Path | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    comparison_rows_by_fixture: dict[str, list[dict[str, Any]]] = {}
    batch_summary_by_fixture: dict[str, list[dict[str, Any]]] = {}
    if fixture_comparison_report is not None:
        comparison_payload = _load_json(_resolve_path(fixture_comparison_report))
        for row in list(comparison_payload.get("comparison", {}).get("fixture_rows", [])):
            if not isinstance(row, dict):
                continue
            fixture_id = str(row.get("fixture_id", "")).strip()
            if not fixture_id:
                continue
            comparison_rows_by_fixture.setdefault(fixture_id, []).append(row)
    if fixture_trial_batch_manifest is not None:
        batch_payload = _load_json(_resolve_path(fixture_trial_batch_manifest))
        for comparison in list(batch_payload.get("trial_comparisons", [])):
            if not isinstance(comparison, dict):
                continue
            report_path = str(comparison.get("comparison_report_path", "")).strip()
            if not report_path:
                continue
            report_payload = _load_json(_resolve_path(report_path))
            for row in list(report_payload.get("comparison", {}).get("fixture_rows", [])):
                if not isinstance(row, dict):
                    continue
                fixture_id = str(row.get("fixture_id", "")).strip()
                if not fixture_id:
                    continue
                batch_summary_by_fixture.setdefault(fixture_id, []).append(
                    {
                        "trial_name": str(comparison.get("trial_name", "")).strip(),
                        "artifact_layer": str(comparison.get("artifact_layer", "")).strip(),
                        "comparison_status": str(comparison.get("comparison_status", "")).strip(),
                        "recommendation": dict(comparison.get("recommendation", {})),
                        "comparison_report_path": report_path,
                    }
                )
    if fixture_manifest_path is not None:
        manifest = load_evaluation_fixture_manifest(fixture_manifest_path)
        for row in list(manifest.get("fixtures", [])):
            records.append(
                {
                    "record_id": str(row["fixture_id"]),
                    "label": str(row["label"]),
                    "kind": "fixture",
                    "game": None,
                    "source": None,
                    "expected_review_outcome": str(row["expected_review_outcome"]),
                    "latency_budget_class": str(row["latency_budget_class"]),
                    "artifact_refs": dict(row.get("artifact_refs", {})),
                    "expected_artifacts": dict(row.get("expected_artifacts", {})),
                    "notes": str(row.get("notes", "")),
                    "fixture_comparison_rows": comparison_rows_by_fixture.get(str(row["fixture_id"]), []),
                    "fixture_trial_batch_rows": batch_summary_by_fixture.get(str(row["fixture_id"]), []),
                }
            )

    if sidecar_root is not None:
        root = _resolve_path(sidecar_root)
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for path in sorted(root.rglob("*.json")):
            if path.name.endswith(".proxy_scan.json"):
                payload = _load_json(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["proxy_sidecar_path"] = str(path.resolve())
                review = payload.get("proxy_review", {})
                if isinstance(review, dict):
                    record["proxy_review_status"] = str(review.get("review_status", "")).strip() or None
            elif path.name.endswith(".runtime_analysis.json"):
                payload = _load_json(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["runtime_sidecar_path"] = str(path.resolve())
                review = payload.get("runtime_review", {})
                if isinstance(review, dict):
                    record["runtime_review_status"] = str(review.get("review_status", "")).strip() or None
            elif path.name.endswith(".fused_analysis.json"):
                payload = _load_json(path)
                key = (str(payload.get("game", "")), str(payload.get("source", "")))
                record = grouped.setdefault(key, _base_sidecar_record(payload))
                record["fused_sidecar_path"] = str(path.resolve())
                review = payload.get("fused_review", {})
                if isinstance(review, dict):
                    events = review.get("events", {})
                    if isinstance(events, dict):
                        statuses = {
                            str(event.get("review_status", "")).strip()
                            for event in events.values()
                            if isinstance(event, dict) and str(event.get("review_status", "")).strip()
                        }
                        if len(statuses) == 1:
                            record["fused_review_status"] = next(iter(statuses))
                        elif statuses:
                            record["fused_review_status"] = "mixed"

        records.extend(
            sorted(grouped.values(), key=lambda row: (str(row.get("game") or ""), str(row.get("source") or "")))
        )
    return records


def launch_highlight_review_app(
    *,
    sidecar_root: str | Path | None = None,
    fixture_manifest_path: str | Path | None = None,
    fixture_comparison_report: str | Path | None = None,
    fixture_trial_batch_manifest: str | Path | None = None,
    proxy_calibration_report: str | Path | None = None,
    proxy_replay_report: str | Path | None = None,
    runtime_calibration_report: str | Path | None = None,
    runtime_replay_report: str | Path | None = None,
    output_path: str | Path | None = None,
    launch: bool = True,
) -> dict[str, Any]:
    records = load_highlight_review_records(
        sidecar_root=sidecar_root,
        fixture_manifest_path=fixture_manifest_path,
        fixture_comparison_report=fixture_comparison_report,
        fixture_trial_batch_manifest=fixture_trial_batch_manifest,
    )
    if not records:
        return {
            "ok": False,
            "status": "no_review_records",
            "error": "no fixture records or sidecar records were found",
        }

    try:
        gradio = importlib.import_module("gradio")
    except Exception as exc:
        return {
            "ok": False,
            "status": "missing_review_runtime",
            "error": f"gradio is required for the highlight review app: {exc}",
        }

    records_by_id = {str(row["record_id"]): row for row in records}
    choices = [(str(row["label"]), str(row["record_id"])) for row in records]

    def _render_record(record_id: str) -> tuple[str, str, str, str]:
        row = records_by_id[str(record_id)]
        summary = _record_summary(row)
        if row.get("kind") == "fixture":
            comparison_rows = list(row.get("fixture_comparison_rows", []))
            batch_rows = list(row.get("fixture_trial_batch_rows", []))
            baseline_path = ""
            trial_path = ""
            render_payload: dict[str, Any] = {"fixture": row}
            if comparison_rows:
                preferred = _preferred_fixture_row(comparison_rows)
                baseline_path = _render_fixture_viewer(
                    preferred,
                    side="baseline",
                    fixture_comparison_report=fixture_comparison_report,
                    fixture_trial_batch_manifest=fixture_trial_batch_manifest,
                    proxy_calibration_report=proxy_calibration_report,
                    proxy_replay_report=proxy_replay_report,
                    runtime_calibration_report=runtime_calibration_report,
                    runtime_replay_report=runtime_replay_report,
                    output_path=output_path,
                )
                trial_path = _render_fixture_viewer(
                    preferred,
                    side="trial",
                    fixture_comparison_report=fixture_comparison_report,
                    fixture_trial_batch_manifest=fixture_trial_batch_manifest,
                    proxy_calibration_report=proxy_calibration_report,
                    proxy_replay_report=proxy_replay_report,
                    runtime_calibration_report=runtime_calibration_report,
                    runtime_replay_report=runtime_replay_report,
                    output_path=output_path,
                )
                render_payload["preferred_comparison"] = preferred
            if batch_rows:
                render_payload["fixture_trial_batch_rows"] = batch_rows
            return summary, baseline_path, trial_path, json.dumps(render_payload, indent=2)
        result = render_unified_replay_viewer(
            proxy_sidecar=row.get("proxy_sidecar_path"),
            runtime_sidecar=row.get("runtime_sidecar_path"),
            fused_sidecar=row.get("fused_sidecar_path"),
            fixture_comparison_report=fixture_comparison_report,
            fixture_trial_batch_manifest=fixture_trial_batch_manifest,
            proxy_calibration_report=proxy_calibration_report,
            proxy_replay_report=proxy_replay_report,
            runtime_calibration_report=runtime_calibration_report,
            runtime_replay_report=runtime_replay_report,
            output_path=output_path,
        )
        return summary, str(result.get("viewer_path", "")), "", json.dumps(result, indent=2)

    with gradio.Blocks(title="Highlight Review App") as app:
        gradio.Markdown("# Highlight Review App")
        selector = gradio.Dropdown(choices=choices, value=choices[0][1], label="Fixture or reviewed clip")
        summary_box = gradio.Markdown()
        baseline_viewer_path_box = gradio.Textbox(label="Baseline viewer path")
        trial_viewer_path_box = gradio.Textbox(label="Trial viewer path")
        payload_box = gradio.Code(label="Viewer render payload", language="json")
        selector.change(
            _render_record,
            inputs=selector,
            outputs=[summary_box, baseline_viewer_path_box, trial_viewer_path_box, payload_box],
        )
        app.load(
            lambda: _render_record(choices[0][1]),
            inputs=None,
            outputs=[summary_box, baseline_viewer_path_box, trial_viewer_path_box, payload_box],
        )

    if launch:
        app.launch()

    return {
        "ok": True,
        "status": "ok",
        "record_count": len(records),
        "app": app,
    }


def _base_sidecar_record(payload: dict[str, Any]) -> dict[str, Any]:
    game = str(payload.get("game", "")).strip() or None
    source = str(payload.get("source", "")).strip() or None
    return {
        "record_id": f"{game or 'unknown'}::{source or 'unknown'}",
        "label": Path(source or "unknown").name,
        "kind": "sidecar",
        "game": game,
        "source": source,
        "proxy_sidecar_path": None,
        "runtime_sidecar_path": None,
        "fused_sidecar_path": None,
        "proxy_review_status": None,
        "runtime_review_status": None,
        "fused_review_status": None,
    }


def _record_summary(row: dict[str, Any]) -> str:
    if row.get("kind") == "fixture":
        comparison_rows = list(row.get("fixture_comparison_rows", []))
        batch_rows = list(row.get("fixture_trial_batch_rows", []))
        comparison_note = "n/a"
        if comparison_rows:
            preferred = _preferred_fixture_row(comparison_rows)
            comparison_note = (
                f"{preferred.get('artifact_layer')} | {preferred.get('coverage_status')} | "
                f"{preferred.get('recommendation_signal')}"
            )
        disagreement_rows = [
            row
            for row in comparison_rows
            if str(row.get("review_status", "")).strip()
            and str(row.get("recommendation_signal", "")).strip()
            and str(row.get("recommendation_signal", "")).strip() not in {"n/a", ""}
        ]
        batch_trials = ",".join(sorted({str(item.get("trial_name", "")).strip() for item in batch_rows if str(item.get("trial_name", "")).strip()}))
        return "\n".join(
            [
                f"## {row['label']}",
                f"- Expected review: `{row['expected_review_outcome']}`",
                f"- Latency class: `{row['latency_budget_class']}`",
                f"- Expected artifacts: `{','.join(sorted(key for key, value in dict(row.get('expected_artifacts', {})).items() if value)) or 'n/a'}`",
                f"- Fixture comparison: `{comparison_note}`",
                f"- Comparison rows with review/recommendation context: `{len(disagreement_rows)}`",
                f"- Batch trials: `{batch_trials or 'n/a'}`",
                f"- Notes: {row['notes'] or 'n/a'}",
            ]
        )
    review_statuses = {
        str(row.get("proxy_review_status") or "").strip(),
        str(row.get("runtime_review_status") or "").strip(),
        str(row.get("fused_review_status") or "").strip(),
    }
    review_statuses.discard("")
    return "\n".join(
        [
            f"## {row['label']}",
            f"- Game: `{row.get('game') or 'unknown'}`",
            f"- Proxy review: `{row.get('proxy_review_status') or 'unreviewed'}`",
            f"- Runtime review: `{row.get('runtime_review_status') or 'unreviewed'}`",
            f"- Fused review: `{row.get('fused_review_status') or 'unreviewed'}`",
            f"- Cross-layer review disagreement: `{'yes' if len(review_statuses) > 1 else 'no'}`",
        ]
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _preferred_fixture_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(
        rows,
        key=lambda row: (
            {"proxy": 0, "runtime": 1, "fused": 2}.get(str(row.get("artifact_layer", "")), 3),
            0 if str(row.get("coverage_status", "")) == "both" else 1,
        ),
    )
    return ranked[0] if ranked else {}


def _render_fixture_viewer(
    row: dict[str, Any],
    *,
    side: str,
    fixture_comparison_report: str | Path | None,
    fixture_trial_batch_manifest: str | Path | None,
    proxy_calibration_report: str | Path | None,
    proxy_replay_report: str | Path | None,
    runtime_calibration_report: str | Path | None,
    runtime_replay_report: str | Path | None,
    output_path: str | Path | None,
) -> str:
    proxy_sidecar = row.get(f"{side}_sidecar_path") if str(row.get("artifact_layer")) == "proxy" else None
    runtime_sidecar = row.get(f"{side}_sidecar_path") if str(row.get("artifact_layer")) == "runtime" else None
    fused_sidecar = row.get(f"{side}_sidecar_path") if str(row.get("artifact_layer")) == "fused" else None
    if not any([proxy_sidecar, runtime_sidecar, fused_sidecar]):
        return ""
    resolved_output_path = None
    if output_path is not None:
        target = _resolve_path(output_path)
        resolved_output_path = str(target.with_name(f"{target.stem}-{side}{target.suffix or '.html'}"))
    result = render_unified_replay_viewer(
        proxy_sidecar=proxy_sidecar,
        runtime_sidecar=runtime_sidecar,
        fused_sidecar=fused_sidecar,
        fixture_comparison_report=fixture_comparison_report,
        fixture_trial_batch_manifest=fixture_trial_batch_manifest,
        proxy_calibration_report=proxy_calibration_report,
        proxy_replay_report=proxy_replay_report,
        runtime_calibration_report=runtime_calibration_report,
        runtime_replay_report=runtime_replay_report,
        output_path=resolved_output_path,
    )
    return str(result.get("viewer_path", ""))

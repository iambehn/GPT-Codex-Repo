from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
PROXY_REPLAY_VIEWER_SCHEMA_VERSION = "proxy_replay_viewer_v1"
SUPPORTED_PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "proxy_replay_viewers"

_HF_SIGNAL_STAGE_MAP = {
    "hf_shot_boundary": "proposal",
    "hf_proposal": "proposal",
    "hf_transcript_salience": "transcript",
    "hf_semantic_match": "semantic",
    "hf_semantic_highlight": "semantic",
    "hf_keyframe_novelty": "novelty",
    "hf_rerank_highlight": "rerank",
}


def render_proxy_replay_viewer(
    proxy_sidecar: str | Path,
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    sidecar_path = _resolve_path(proxy_sidecar)
    payload = _load_json(sidecar_path)
    if payload.get("schema_version") != SUPPORTED_PROXY_SCAN_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_proxy_sidecar",
            "proxy_sidecar_path": str(sidecar_path),
            "error": "proxy sidecar does not use proxy_scan_v1",
        }

    viewer_path = _viewer_output_path(payload, sidecar_path, output_path)
    media_path = _resolve_media_path(payload.get("source"))
    media_exists = bool(media_path and media_path.exists() and media_path.is_file())
    warnings: list[dict[str, Any]] = []
    if media_path is not None and not media_exists:
        warnings.append(
            {
                "status": "missing_media_source",
                "path": str(media_path),
                "message": "proxy source media path does not exist locally; viewer will render without inline media playback",
            }
        )

    derived = _derived_payload(payload, sidecar_path, media_path, media_exists)
    html_text = _render_html(payload, derived)
    viewer_path.parent.mkdir(parents=True, exist_ok=True)
    viewer_path.write_text(html_text, encoding="utf-8")
    return {
        "ok": True,
        "status": "ok",
        "schema_version": PROXY_REPLAY_VIEWER_SCHEMA_VERSION,
        "viewer_path": str(viewer_path),
        "proxy_sidecar_path": str(sidecar_path),
        "media_path": str(media_path) if media_path is not None else None,
        "media_embed_available": media_exists,
        "window_count": len(derived["windows"]),
        "has_hf_multimodal": derived["hf_pipeline"]["available"],
        "selected_window_id": derived["top_window"]["window_id"] if derived["top_window"] else None,
        "review_status": derived["review"]["review_status"],
        "interactive_section_count": 5,
        "warnings": warnings,
    }


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    else:
        resolved = resolved.resolve()
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _viewer_output_path(payload: dict[str, Any], sidecar_path: Path, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return _resolve_path(output_path)
    game = str(payload.get("game") or "unknown_game").strip() or "unknown_game"
    scan_id = str(payload.get("scan_id") or sidecar_path.stem).strip() or sidecar_path.stem
    return DEFAULT_OUTPUT_ROOT / game / f"{_slug(scan_id)}.proxy_replay_view.html"


def _slug(value: str) -> str:
    lowered = value.lower()
    return "".join(char if char.isalnum() else "-" for char in lowered).strip("-") or "proxy-replay-view"


def _resolve_media_path(value: Any) -> Path | None:
    source_text = str(value or "").strip()
    if not source_text:
        return None
    path = Path(source_text).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _derived_payload(payload: dict[str, Any], sidecar_path: Path, media_path: Path | None, media_exists: bool) -> dict[str, Any]:
    windows = _normalize_windows(payload.get("windows", []))
    top_window = windows[0] if windows else None
    review = _normalize_review(payload.get("proxy_review"))
    source_results = payload.get("source_results", {}) if isinstance(payload.get("source_results"), dict) else {}
    hf_result = source_results.get("hf_multimodal", {}) if isinstance(source_results.get("hf_multimodal"), dict) else {}
    hf_metadata = hf_result.get("metadata", {}) if isinstance(hf_result.get("metadata"), dict) else {}
    hf_pipeline = _normalize_hf_pipeline(hf_result, hf_metadata, windows)
    raw_sections = [
        {"section_id": "raw-proxy-sidecar", "title": "Proxy Sidecar", "payload": payload},
        {"section_id": "raw-source-results", "title": "Source Results", "payload": source_results},
        {"section_id": "raw-hf-structured", "title": "HF Structured Outputs", "payload": hf_pipeline["structured_outputs_raw"]},
    ]
    return {
        "proxy_sidecar_path": str(sidecar_path),
        "media_path": str(media_path) if media_path is not None else None,
        "media_uri": media_path.as_uri() if media_exists and media_path is not None else None,
        "media_exists": media_exists,
        "clip": {
            "game": str(payload.get("game") or ""),
            "scan_id": str(payload.get("scan_id") or sidecar_path.stem),
            "source": str(payload.get("source") or ""),
            "sidecar_path": str(sidecar_path),
        },
        "proxy_summary": {
            "ok": bool(payload.get("ok", False)),
            "status": str(payload.get("status") or "unknown"),
            "signal_count": int(payload.get("signal_count") or 0),
            "window_count": len(windows),
            "top_recommended_action": top_window.get("recommended_action") if top_window else None,
            "top_proxy_score": top_window.get("proxy_score") if top_window else None,
        },
        "windows": windows,
        "top_window": top_window,
        "hf_pipeline": hf_pipeline,
        "review": review,
        "raw_sections": raw_sections,
        "raw_artifacts": {
            "proxy_sidecar_json": payload,
            "source_results_json": source_results,
            "hf_metadata_json": hf_metadata,
        },
    }


def _normalize_review(review: Any) -> dict[str, Any]:
    if not isinstance(review, dict):
        return {"review_status": None, "is_reviewed": False}
    status = str(review.get("review_status") or "").strip().lower() or None
    return {
        "review_status": status,
        "is_reviewed": status in {"approved", "rejected"},
        "raw": review,
    }


def _normalize_windows(rows: Any) -> list[dict[str, Any]]:
    windows: list[dict[str, Any]] = []
    if not isinstance(rows, list):
        return windows
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        start = _as_float(row.get("start_seconds"), 0.0)
        end = _as_float(row.get("end_seconds"), start)
        signals = list(row.get("signals", [])) if isinstance(row.get("signals"), list) else []
        normalized_signals = []
        stage_signal_counts = {"proposal": 0, "transcript": 0, "semantic": 0, "novelty": 0, "rerank": 0, "other": 0}
        for signal_index, signal in enumerate(signals):
            if not isinstance(signal, dict):
                continue
            source = str(signal.get("source") or "")
            stage = _HF_SIGNAL_STAGE_MAP.get(source, "other")
            stage_signal_counts[stage] = int(stage_signal_counts.get(stage, 0)) + 1
            normalized_signals.append(
                {
                    "signal_id": f"window-{index}-signal-{signal_index}",
                    "source": source,
                    "source_family": str(signal.get("source_family") or ""),
                    "strength": _as_float(signal.get("strength"), 0.0),
                    "confidence": _as_float(signal.get("confidence"), 0.0),
                    "timestamp": _as_float(signal.get("timestamp"), start),
                    "reason": str(signal.get("reason") or ""),
                    "stage": stage,
                }
            )
        explanation = row.get("explanation")
        if isinstance(explanation, list):
            explanation_rows = [str(item) for item in explanation]
        elif explanation is None:
            explanation_rows = []
        else:
            explanation_rows = [str(explanation)]
        windows.append(
            {
                "window_id": f"window-{index}",
                "index": index,
                "start_seconds": round(start, 5),
                "end_seconds": round(end, 5),
                "duration_seconds": round(max(0.0, end - start), 5),
                "proxy_score": _as_float(row.get("proxy_score"), 0.0),
                "signal_count": int(row.get("signal_count") or len(normalized_signals)),
                "recommended_action": str(row.get("recommended_action") or ""),
                "sources": [str(item) for item in row.get("sources", []) if str(item).strip()] if isinstance(row.get("sources"), list) else [],
                "source_families": [str(item) for item in row.get("source_families", []) if str(item).strip()] if isinstance(row.get("source_families"), list) else [],
                "signals": normalized_signals,
                "explanation": explanation_rows,
                "stage_signal_counts": stage_signal_counts,
                "search_text": " ".join(
                    [
                        str(row.get("recommended_action") or ""),
                        *[str(item) for item in row.get("sources", []) if isinstance(item, str)],
                        *[str(item) for item in row.get("source_families", []) if isinstance(item, str)],
                    ]
                ).lower(),
            }
        )
    windows.sort(key=lambda item: (-float(item["proxy_score"]), item["start_seconds"], item["index"]))
    return windows


def _normalize_hf_pipeline(hf_result: dict[str, Any], hf_metadata: dict[str, Any], windows: list[dict[str, Any]]) -> dict[str, Any]:
    structured_outputs = hf_metadata.get("structured_outputs", {}) if isinstance(hf_metadata.get("structured_outputs"), dict) else {}
    stages = hf_metadata.get("stages", {}) if isinstance(hf_metadata.get("stages"), dict) else {}
    stage_statuses = hf_metadata.get("stage_statuses", {}) if isinstance(hf_metadata.get("stage_statuses"), dict) else {}
    pipeline_config = hf_metadata.get("pipeline", {}) if isinstance(hf_metadata.get("pipeline"), dict) else {}
    normalized_stage_rows = []
    for stage_name in ("shot_detector", "asr", "semantic", "keyframes", "reranker"):
        stage_payload = stages.get(stage_name, {}) if isinstance(stages.get(stage_name), dict) else {}
        output_counts = stage_payload.get("output_counts", {}) if isinstance(stage_payload.get("output_counts"), dict) else {}
        normalized_stage_rows.append(
            {
                "stage_name": stage_name,
                "status": str(stage_statuses.get(stage_name) or stage_payload.get("status") or "unknown"),
                "reason": str(stage_payload.get("reason") or ""),
                "duration_ms": _as_float(stage_payload.get("duration_ms"), 0.0),
                "output_counts": {str(key): int(value) if isinstance(value, (int, float)) else value for key, value in output_counts.items()},
            }
        )

    proposals = _normalize_window_rows(structured_outputs.get("segment_proposals"), score_field="proposal_score")
    transcript_features = _normalize_window_rows(structured_outputs.get("transcript_features"), text_field="text", score_field="salience_score", list_field="keyword_hits")
    semantic_scores = _normalize_window_rows(structured_outputs.get("semantic_scores"), score_field="semantic_score", extra_fields=("top_query", "query_scores"))
    keyframe_features = _normalize_window_rows(structured_outputs.get("keyframe_features"), score_field="novelty_score", extra_fields=("keyframe_timestamp_seconds", "cluster_id"))
    reranked_candidates = _normalize_window_rows(
        structured_outputs.get("reranked_candidates"),
        score_field="rerank_score",
        extra_fields=("proposal_score", "transcript_score", "semantic_score", "novelty_score", "base_score", "reason", "reason_codes"),
    )

    window_details = {}
    for window in windows:
        matched_proposal = _best_overlap(window, proposals)
        matched_transcript = _best_overlap(window, transcript_features)
        matched_semantic = _best_overlap(window, semantic_scores)
        matched_keyframe = _best_overlap(window, keyframe_features)
        matched_rerank = _best_overlap(window, reranked_candidates)
        score_breakdown = {
            "proposal_score": matched_proposal.get("proposal_score") if matched_proposal else None,
            "transcript_score": matched_transcript.get("salience_score") if matched_transcript else None,
            "semantic_score": matched_semantic.get("semantic_score") if matched_semantic else None,
            "novelty_score": matched_keyframe.get("novelty_score") if matched_keyframe else None,
            "base_score": matched_rerank.get("base_score") if matched_rerank else None,
            "rerank_score": matched_rerank.get("rerank_score") if matched_rerank else None,
        }
        signal_contributions = []
        for signal in window.get("signals", []):
            signal_contributions.append(
                {
                    "stage": signal.get("stage"),
                    "source": signal.get("source"),
                    "strength": signal.get("strength"),
                    "confidence": signal.get("confidence"),
                    "reason": signal.get("reason"),
                }
            )
        window_details[window["window_id"]] = {
            "proposal": matched_proposal,
            "transcript": matched_transcript,
            "semantic": matched_semantic,
            "keyframe": matched_keyframe,
            "rerank": matched_rerank,
            "score_breakdown": score_breakdown,
            "signal_contributions": signal_contributions,
        }

    return {
        "available": bool(hf_result),
        "status": str(hf_result.get("status") or "missing") if hf_result else "missing",
        "signal_count": int(hf_result.get("signal_count") or 0) if hf_result else 0,
        "pipeline": {
            "duration_seconds": _as_float(pipeline_config.get("duration_seconds"), 0.0),
            "shortlist_count": int(pipeline_config.get("shortlist_count") or 0),
            "stage_weights": pipeline_config.get("stage_weights", {}) if isinstance(pipeline_config.get("stage_weights"), dict) else {},
            "signal_thresholds": pipeline_config.get("signal_thresholds", {}) if isinstance(pipeline_config.get("signal_thresholds"), dict) else {},
        },
        "stages": normalized_stage_rows,
        "structured_outputs_raw": structured_outputs,
        "structured_outputs": {
            "segment_proposals": proposals,
            "transcript_features": transcript_features,
            "semantic_scores": semantic_scores,
            "keyframe_features": keyframe_features,
            "reranked_candidates": reranked_candidates,
        },
        "window_details": window_details,
    }


def _normalize_window_rows(
    rows: Any,
    *,
    score_field: str | None = None,
    text_field: str | None = None,
    list_field: str | None = None,
    extra_fields: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    if not isinstance(rows, list):
        return normalized
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        start = _as_float(row.get("start_seconds"), 0.0)
        end = _as_float(row.get("end_seconds"), start)
        item = {
            "row_id": f"hf-row-{index}",
            "start_seconds": round(start, 5),
            "end_seconds": round(end, 5),
            "duration_seconds": round(max(0.0, end - start), 5),
        }
        if score_field is not None:
            item[score_field] = _as_float(row.get(score_field), 0.0)
        if text_field is not None:
            item[text_field] = str(row.get(text_field) or "")
        if list_field is not None:
            item[list_field] = [str(value) for value in row.get(list_field, []) if str(value).strip()] if isinstance(row.get(list_field), list) else []
        for field_name in extra_fields:
            item[field_name] = row.get(field_name)
        normalized.append(item)
    return normalized


def _best_overlap(window: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_score = -1.0
    for row in rows:
        overlap = _overlap_seconds(
            window["start_seconds"],
            window["end_seconds"],
            _as_float(row.get("start_seconds"), 0.0),
            _as_float(row.get("end_seconds"), 0.0),
        )
        if overlap > best_score:
            best_score = overlap
            best = row
    return best if best_score > 0.0 else None


def _overlap_seconds(left_start: float, left_end: float, right_start: float, right_end: float) -> float:
    return max(0.0, min(left_end, right_end) - max(left_start, right_start))


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _render_html(payload: dict[str, Any], derived: dict[str, Any]) -> str:
    game = html.escape(str(derived["clip"]["game"] or "unknown_game"))
    media_tag = (
        f'<video id="viewer-media" controls preload="metadata" src="{html.escape(str(derived["media_uri"]))}" class="video-player"></video>'
        if derived["media_exists"]
        else '<div class="media-missing">Local media source not available. The viewer still renders all sidecar diagnostics.</div>'
    )
    initial_window = derived["top_window"] or {}
    data_json = json.dumps(derived, sort_keys=True)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Proxy Replay Viewer - {game}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4f6fb;
      --panel: #ffffff;
      --muted: #5f6b7a;
      --text: #122033;
      --border: #d7dfeb;
      --accent: #1166cc;
      --good: #0f8a5f;
      --bad: #b43737;
      --warn: #9d6400;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: var(--bg); color: var(--text); }}
    .page {{ max-width: 1480px; margin: 0 auto; padding: 20px; }}
    .hero, .panel {{ background: var(--panel); border: 1px solid var(--border); border-radius: 14px; box-shadow: 0 10px 24px rgba(17, 24, 39, 0.05); }}
    .hero {{ padding: 20px; margin-bottom: 18px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .meta {{ color: var(--muted); font-size: 14px; display: grid; gap: 4px; }}
    .top-grid {{ display: grid; grid-template-columns: 1.4fr 1fr; gap: 18px; margin-bottom: 18px; }}
    .panel {{ padding: 16px; }}
    .video-player {{ width: 100%; border-radius: 10px; background: #000; }}
    .media-missing {{ padding: 18px; border: 1px dashed var(--border); border-radius: 10px; color: var(--muted); }}
    .summary-grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 14px; }}
    .metric {{ border: 1px solid var(--border); border-radius: 10px; padding: 12px; background: #fbfcff; }}
    .metric-label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
    .metric-value {{ margin-top: 4px; font-size: 20px; font-weight: 700; }}
    .tag {{ display: inline-flex; align-items: center; border-radius: 999px; padding: 3px 10px; font-size: 12px; font-weight: 600; border: 1px solid var(--border); background: #f9fbff; margin-right: 6px; margin-bottom: 6px; }}
    .tag.good {{ color: var(--good); border-color: rgba(15,138,95,.25); background: rgba(15,138,95,.08); }}
    .tag.bad {{ color: var(--bad); border-color: rgba(180,55,55,.25); background: rgba(180,55,55,.08); }}
    .tag.warn {{ color: var(--warn); border-color: rgba(157,100,0,.25); background: rgba(157,100,0,.08); }}
    .controls {{ display: grid; grid-template-columns: 1fr 180px; gap: 12px; margin-bottom: 14px; }}
    .control-input {{ width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid var(--border); background: #fff; }}
    .layout {{ display: grid; grid-template-columns: 360px 1fr; gap: 18px; }}
    .window-list {{ display: grid; gap: 10px; max-height: 780px; overflow: auto; padding-right: 4px; }}
    .window-card {{ border: 1px solid var(--border); border-radius: 12px; padding: 12px; background: #fbfcff; cursor: pointer; }}
    .window-card.active {{ border-color: var(--accent); box-shadow: inset 0 0 0 1px var(--accent); background: #f4f9ff; }}
    .window-card h3 {{ margin: 0 0 8px; font-size: 15px; }}
    .window-meta {{ font-size: 13px; color: var(--muted); display: grid; gap: 4px; }}
    .button-row {{ display: flex; gap: 8px; margin-top: 10px; }}
    button {{ border: 1px solid var(--border); background: #fff; color: var(--text); border-radius: 10px; padding: 8px 12px; cursor: pointer; }}
    button:hover {{ border-color: var(--accent); color: var(--accent); }}
    .details-grid {{ display: grid; gap: 16px; }}
    .subgrid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }}
    .section-title {{ margin: 0 0 10px; font-size: 18px; }}
    .kv {{ display: grid; grid-template-columns: 180px 1fr; gap: 8px; font-size: 14px; margin-bottom: 6px; }}
    .kv .key {{ color: var(--muted); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; font-size: 12px; background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 10px; max-height: 360px; overflow: auto; }}
    .timeline-track {{ position: relative; height: 52px; border-radius: 10px; background: linear-gradient(90deg, #edf3ff 0%, #f9fbff 100%); border: 1px solid var(--border); overflow: hidden; }}
    .timeline-window {{ position: absolute; top: 8px; bottom: 8px; border-radius: 8px; background: rgba(17,102,204,.25); border: 1px solid rgba(17,102,204,.45); }}
    .timeline-window.active {{ background: rgba(17,102,204,.45); }}
    details {{ border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; background: #fff; }}
    summary {{ cursor: pointer; font-weight: 600; }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Proxy Replay Viewer - {game}</h1>
      <div class="meta">
        <div><strong>Scan:</strong> {html.escape(str(derived["clip"]["scan_id"]))}</div>
        <div><strong>Sidecar:</strong> {html.escape(str(derived["clip"]["sidecar_path"]))}</div>
        <div><strong>Source:</strong> {html.escape(str(derived["clip"]["source"]))}</div>
      </div>
    </section>

    <section class="top-grid">
      <div class="panel">
        <h2 class="section-title">Clip Playback</h2>
        {media_tag}
        <div class="summary-grid">
          <div class="metric"><div class="metric-label">Top Action</div><div class="metric-value" id="summary-action">{html.escape(str(derived["proxy_summary"]["top_recommended_action"] or "n/a"))}</div></div>
          <div class="metric"><div class="metric-label">Top Proxy Score</div><div class="metric-value" id="summary-score">{_fmt_number(derived["proxy_summary"]["top_proxy_score"])}</div></div>
          <div class="metric"><div class="metric-label">Window Count</div><div class="metric-value">{len(derived["windows"])}</div></div>
          <div class="metric"><div class="metric-label">Review</div><div class="metric-value" id="summary-review">{html.escape(str(derived["review"]["review_status"] or "unreviewed"))}</div></div>
        </div>
      </div>
      <div class="panel">
        <h2 class="section-title">HF Pipeline Summary</h2>
        <div id="hf-summary-tags">
          {_render_hf_summary_tags(derived["hf_pipeline"])}
        </div>
        <div class="kv"><div class="key">HF status</div><div id="hf-status">{html.escape(str(derived["hf_pipeline"]["status"]))}</div></div>
        <div class="kv"><div class="key">Signal count</div><div>{derived["hf_pipeline"]["signal_count"]}</div></div>
        <div class="kv"><div class="key">Shortlist count</div><div>{derived["hf_pipeline"]["pipeline"]["shortlist_count"]}</div></div>
        <div class="kv"><div class="key">Duration (s)</div><div>{_fmt_number(derived["hf_pipeline"]["pipeline"]["duration_seconds"])}</div></div>
      </div>
    </section>

    <section class="panel" style="margin-bottom: 18px;">
      <h2 class="section-title">Viewer Controls</h2>
      <div class="controls">
        <input id="viewer-search" class="control-input" type="text" placeholder="filter by action, source family, signal, or reason code">
        <input id="viewer-min-score" class="control-input" type="number" min="0" max="1" step="0.01" placeholder="min proxy score">
      </div>
      <div class="timeline-track" id="timeline-track">
        {_render_timeline_windows(derived["windows"], initial_window.get("window_id"))}
      </div>
    </section>

    <section class="layout">
      <div class="panel">
        <h2 class="section-title">Proxy Windows</h2>
        <div class="window-list" id="window-list">
          {_render_window_cards(derived["windows"], initial_window.get("window_id"))}
        </div>
      </div>
      <div class="details-grid">
        <div class="panel">
          <h2 class="section-title">Selected Window</h2>
          <div id="window-detail"></div>
        </div>
        <div class="subgrid">
          <div class="panel">
            <h2 class="section-title">HF Stage Status</h2>
            <div id="hf-stage-table">{_render_stage_table(derived["hf_pipeline"]["stages"])}</div>
          </div>
          <div class="panel">
            <h2 class="section-title">Review Alignment</h2>
            <div id="review-panel">{_render_review_panel(derived["review"], derived["top_window"])}</div>
          </div>
        </div>
        <div class="panel">
          <h2 class="section-title">Raw JSON Inspector</h2>
          {_render_raw_sections(derived["raw_sections"])}
        </div>
      </div>
    </section>
  </div>
  <script>
    const VIEWER_DATA = {data_json};
    const viewerState = {{
      selectedWindowId: {json.dumps(initial_window.get("window_id"))},
    }};

    function formatNumber(value) {{
      if (value === null || value === undefined || value === "") return "n/a";
      const number = Number(value);
      if (Number.isNaN(number)) return String(value);
      return number.toFixed(3).replace(/\\.000$/, ".000").replace(/0+$/, "").replace(/\\.$/, "");
    }}

    function escapeHtml(value) {{
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }}

    function getSelectedWindow() {{
      return VIEWER_DATA.windows.find((item) => item.window_id === viewerState.selectedWindowId) || VIEWER_DATA.windows[0] || null;
    }}

    function jumpToWindow(windowRow) {{
      const media = document.getElementById("viewer-media");
      if (!media || !windowRow) return;
      media.currentTime = Number(windowRow.start_seconds || 0);
      media.play().catch(() => undefined);
    }}

    function selectWindow(windowId, options = {{ jump: false }}) {{
      viewerState.selectedWindowId = windowId;
      renderSelection();
      if (options.jump) {{
        jumpToWindow(getSelectedWindow());
      }}
    }}

    function applyFilters() {{
      const searchText = String(document.getElementById("viewer-search").value || "").toLowerCase().trim();
      const minScoreText = String(document.getElementById("viewer-min-score").value || "").trim();
      const minScore = minScoreText ? Number(minScoreText) : null;
      for (const card of document.querySelectorAll(".window-card")) {{
        const row = VIEWER_DATA.windows.find((item) => item.window_id === card.dataset.windowId);
        if (!row) continue;
        const matchesSearch = !searchText || row.search_text.includes(searchText) || JSON.stringify(row).toLowerCase().includes(searchText);
        const matchesScore = minScore === null || Number(row.proxy_score) >= minScore;
        card.style.display = matchesSearch && matchesScore ? "" : "none";
      }}
    }}

    function renderSelection() {{
      const windowRow = getSelectedWindow();
      if (!windowRow) return;
      for (const card of document.querySelectorAll(".window-card")) {{
        card.classList.toggle("active", card.dataset.windowId === windowRow.window_id);
      }}
      for (const bar of document.querySelectorAll(".timeline-window")) {{
        bar.classList.toggle("active", bar.dataset.windowId === windowRow.window_id);
      }}
      document.getElementById("summary-action").textContent = windowRow.recommended_action || "n/a";
      document.getElementById("summary-score").textContent = formatNumber(windowRow.proxy_score);
      document.getElementById("window-detail").innerHTML = renderWindowDetail(windowRow);
    }}

    function renderWindowDetail(windowRow) {{
      const hfDetails = VIEWER_DATA.hf_pipeline.window_details[windowRow.window_id] || {{}};
      const rerank = hfDetails.rerank || null;
      const tags = []
        .concat((windowRow.source_families || []).map((item) => `<span class="tag">${{escapeHtml(item)}}</span>`))
        .concat((windowRow.sources || []).map((item) => `<span class="tag">${{escapeHtml(item)}}</span>`))
        .join("");
      const reasonCodes = Array.isArray(rerank?.reason_codes) ? rerank.reason_codes.map((item) => `<span class="tag warn">${{escapeHtml(item)}}</span>`).join("") : "";
      const explanationRows = (windowRow.explanation || []).map((item) => `<li>${{escapeHtml(item)}}</li>`).join("");
      const contributionRows = (hfDetails.signal_contributions || []).map((item) => `
        <tr>
          <td>${{escapeHtml(item.stage || "other")}}</td>
          <td>${{escapeHtml(item.source || "")}}</td>
          <td>${{formatNumber(item.strength)}}</td>
          <td>${{formatNumber(item.confidence)}}</td>
          <td>${{escapeHtml(item.reason || "")}}</td>
        </tr>
      `).join("");
      return `
        <div class="subgrid">
          <div>
            <div class="kv"><div class="key">Time range</div><div>${{formatNumber(windowRow.start_seconds)}}s - ${{formatNumber(windowRow.end_seconds)}}s</div></div>
            <div class="kv"><div class="key">Duration</div><div>${{formatNumber(windowRow.duration_seconds)}}s</div></div>
            <div class="kv"><div class="key">Proxy score</div><div>${{formatNumber(windowRow.proxy_score)}}</div></div>
            <div class="kv"><div class="key">Recommended action</div><div>${{escapeHtml(windowRow.recommended_action || "n/a")}}</div></div>
            <div class="kv"><div class="key">Signal count</div><div>${{windowRow.signal_count}}</div></div>
            <div class="kv"><div class="key">HF base / rerank</div><div>${{formatNumber(hfDetails.score_breakdown?.base_score)}} / ${{formatNumber(hfDetails.score_breakdown?.rerank_score)}}</div></div>
            <div>${{tags}}</div>
          </div>
          <div>
            <div class="kv"><div class="key">Proposal</div><div>${{formatNumber(hfDetails.score_breakdown?.proposal_score)}}</div></div>
            <div class="kv"><div class="key">Transcript</div><div>${{formatNumber(hfDetails.score_breakdown?.transcript_score)}}</div></div>
            <div class="kv"><div class="key">Semantic</div><div>${{formatNumber(hfDetails.score_breakdown?.semantic_score)}}</div></div>
            <div class="kv"><div class="key">Novelty</div><div>${{formatNumber(hfDetails.score_breakdown?.novelty_score)}}</div></div>
            <div class="kv"><div class="key">Rerank reason</div><div>${{escapeHtml(rerank?.reason || "n/a")}}</div></div>
            <div>${{reasonCodes}}</div>
          </div>
        </div>
        <div class="button-row">
          <button type="button" data-jump-window="${{escapeHtml(windowRow.window_id)}}">Jump To Window</button>
        </div>
        <h3>Timeline Explanation</h3>
        <ul>${{explanationRows || "<li>No explanation rows.</li>"}}</ul>
        <h3>Signal Contributions</h3>
        <table>
          <thead><tr><th>Stage</th><th>Source</th><th>Strength</th><th>Confidence</th><th>Reason</th></tr></thead>
          <tbody>${{contributionRows || '<tr><td colspan="5">No signal contributions available.</td></tr>'}}</tbody>
        </table>
      `;
    }}

    document.addEventListener("click", (event) => {{
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      const windowCard = target.closest(".window-card");
      if (windowCard) {{
        selectWindow(windowCard.dataset.windowId, {{ jump: false }});
        return;
      }}
      const jumpButton = target.closest("[data-jump-window]");
      if (jumpButton) {{
        selectWindow(jumpButton.getAttribute("data-jump-window"), {{ jump: true }});
      }}
    }});

    document.getElementById("viewer-search").addEventListener("input", applyFilters);
    document.getElementById("viewer-min-score").addEventListener("input", applyFilters);
    renderSelection();
    applyFilters();
  </script>
</body>
</html>"""


def _render_hf_summary_tags(hf_pipeline: dict[str, Any]) -> str:
    if not hf_pipeline.get("available"):
        return '<span class="tag warn">HF multimodal not present</span>'
    tags = [f'<span class="tag good">HF status: {html.escape(str(hf_pipeline.get("status") or "unknown"))}</span>']
    for row in hf_pipeline.get("stages", []):
        status = str(row.get("status") or "unknown")
        css = "good" if status == "ok" else "warn" if status in {"skipped", "missing"} else "bad"
        tags.append(f'<span class="tag {css}">{html.escape(str(row.get("stage_name")))}: {html.escape(status)}</span>')
    return "".join(tags)


def _render_timeline_windows(windows: list[dict[str, Any]], active_window_id: str | None) -> str:
    if not windows:
        return '<div class="timeline-window" style="left:0%;width:100%;">No proxy windows</div>'
    max_end = max(float(item["end_seconds"]) for item in windows) or 1.0
    bars = []
    for row in windows:
        left = max(0.0, min(100.0, (float(row["start_seconds"]) / max_end) * 100.0))
        width = max(1.2, min(100.0 - left, ((float(row["end_seconds"]) - float(row["start_seconds"])) / max_end) * 100.0))
        active_class = " active" if row["window_id"] == active_window_id else ""
        bars.append(
            f'<div class="timeline-window{active_class}" data-window-id="{html.escape(row["window_id"])}" '
            f'style="left:{left:.3f}%;width:{width:.3f}%;" title="{html.escape(row["recommended_action"])} {row["start_seconds"]}-{row["end_seconds"]}"></div>'
        )
    return "".join(bars)


def _render_window_cards(windows: list[dict[str, Any]], active_window_id: str | None) -> str:
    if not windows:
        return '<div class="window-card"><h3>No windows</h3><div class="window-meta">The sidecar has no proxy windows.</div></div>'
    cards = []
    for row in windows:
        active_class = " active" if row["window_id"] == active_window_id else ""
        source_families = ", ".join(row["source_families"]) or "n/a"
        cards.append(
            f'<div class="window-card{active_class}" data-window-id="{html.escape(row["window_id"])}">'
            f'<h3>{html.escape(str(row["recommended_action"] or "window"))}</h3>'
            f'<div class="window-meta">'
            f'<div>Score: {_fmt_number(row["proxy_score"])} | Signals: {row["signal_count"]}</div>'
            f'<div>{_fmt_number(row["start_seconds"])}s - {_fmt_number(row["end_seconds"])}s</div>'
            f'<div>Families: {html.escape(source_families)}</div>'
            f'</div>'
            f'</div>'
        )
    return "".join(cards)


def _render_stage_table(stages: list[dict[str, Any]]) -> str:
    if not stages:
        return "<p>No HF stage metadata available.</p>"
    rows = []
    for row in stages:
        counts = row.get("output_counts", {}) if isinstance(row.get("output_counts"), dict) else {}
        counts_text = ", ".join(f"{key}={value}" for key, value in counts.items()) or "n/a"
        rows.append(
            f"<tr>"
            f"<td>{html.escape(str(row.get('stage_name') or ''))}</td>"
            f"<td>{html.escape(str(row.get('status') or ''))}</td>"
            f"<td>{_fmt_number(row.get('duration_ms'))}</td>"
            f"<td>{html.escape(counts_text)}</td>"
            f"<td>{html.escape(str(row.get('reason') or ''))}</td>"
            f"</tr>"
        )
    return (
        "<table><thead><tr><th>Stage</th><th>Status</th><th>Duration ms</th><th>Output counts</th><th>Reason</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _render_review_panel(review: dict[str, Any], top_window: dict[str, Any] | None) -> str:
    status = str(review.get("review_status") or "unreviewed")
    badge_class = "good" if status == "approved" else "bad" if status == "rejected" else "warn"
    top_action = str(top_window.get("recommended_action") or "n/a") if isinstance(top_window, dict) else "n/a"
    top_score = _fmt_number(top_window.get("proxy_score")) if isinstance(top_window, dict) else "n/a"
    disagreement_hint = "No review disagreement signal available yet."
    if status == "rejected" and isinstance(top_window, dict) and float(top_window.get("proxy_score") or 0.0) >= 0.75:
        disagreement_hint = "High score plus rejected review. This is a tuning disagreement case worth inspecting."
    elif status == "approved" and isinstance(top_window, dict) and float(top_window.get("proxy_score") or 0.0) <= 0.50:
        disagreement_hint = "Low score plus approved review. This suggests missed ranking strength."
    return (
        f'<div><span class="tag {badge_class}">{html.escape(status)}</span></div>'
        f'<div class="kv"><div class="key">Top action</div><div>{html.escape(top_action)}</div></div>'
        f'<div class="kv"><div class="key">Top proxy score</div><div>{top_score}</div></div>'
        f'<div class="kv"><div class="key">Alignment note</div><div>{html.escape(disagreement_hint)}</div></div>'
    )


def _render_raw_sections(sections: list[dict[str, Any]]) -> str:
    blocks = []
    for section in sections:
        section_id = str(section.get("section_id") or "raw")
        title = str(section.get("title") or "Raw")
        text = json.dumps(section.get("payload", {}), indent=2, sort_keys=True)
        blocks.append(
            f'<details id="{html.escape(section_id)}"><summary>Show JSON - {html.escape(title)}</summary>'
            f"<pre>{html.escape(text)}</pre></details>"
        )
    return "".join(blocks)


def _fmt_number(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return html.escape(str(value))
    text = f"{number:.3f}"
    text = text.rstrip("0").rstrip(".") if "." in text else text
    return text

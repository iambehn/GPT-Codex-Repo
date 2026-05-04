from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Any

from pipeline.hf_adapters import (
    HFAdapterError,
    SigLIPAdapter,
    SmolVLM2Adapter,
    TransNetV2Adapter,
    WhisperAdapter,
    XClipAdapter,
)
from pipeline.media_probe import probe_media_duration, probe_has_video_stream
from pipeline.proxy_scanner import ProxySignal


DEFAULT_TRANSCRIPT_KEYWORDS = (
    "ace",
    "clutch",
    "crazy",
    "huge",
    "insane",
    "lets go",
    "no way",
    "team wipe",
    "wow",
)
DEFAULT_GENERIC_QUERIES = (
    "highlight moment",
    "clutch play",
    "high action combat",
    "objective swing",
)


@dataclass
class ProxySourceEmission:
    signals: list[ProxySignal]
    metadata: dict[str, Any] = field(default_factory=dict)


def scan_hf_multimodal_source(
    source: str | Path,
    config: dict[str, Any],
    *,
    media_duration_seconds: float | None = None,
) -> ProxySourceEmission:
    source_path = Path(str(source))
    if not source_path.exists() or not source_path.is_file():
        raise ValueError("local media file is missing or unreadable")
    if probe_has_video_stream(source_path) is False:
        raise ValueError("source has no video stream")

    duration_seconds = media_duration_seconds or probe_media_duration(source_path)
    shortlist_count = max(1, int(config.get("shortlist_count", 5)))
    transcript_keywords = tuple(
        str(item).strip().lower()
        for item in config.get("transcript_keywords", DEFAULT_TRANSCRIPT_KEYWORDS)
        if str(item).strip()
    )
    generic_queries = [
        str(item).strip()
        for item in config.get("generic_queries", DEFAULT_GENERIC_QUERIES)
        if str(item).strip()
    ]
    stage_weights = {
        "proposal": float(config.get("stage_weights", {}).get("proposal", 0.35)),
        "transcript": float(config.get("stage_weights", {}).get("transcript", 0.20)),
        "semantic": float(config.get("stage_weights", {}).get("semantic", 0.25)),
        "novelty": float(config.get("stage_weights", {}).get("novelty", 0.20)),
    }
    signal_thresholds = {
        "proposal": float(config.get("signal_thresholds", {}).get("proposal", 0.55)),
        "transcript": float(config.get("signal_thresholds", {}).get("transcript", 0.60)),
        "semantic": float(config.get("signal_thresholds", {}).get("semantic", 0.60)),
        "novelty": float(config.get("signal_thresholds", {}).get("novelty", 0.60)),
        "rerank": float(config.get("signal_thresholds", {}).get("rerank", 0.65)),
    }

    shot_stage = _run_stage(
        stage_name="shot_detector",
        enabled=bool(config.get("components", {}).get("shot_detector", {}).get("enabled", True)),
        callback=lambda: TransNetV2Adapter(config.get("components", {}).get("shot_detector", {})).detect_shots(source_path),
    )
    asr_stage = _run_stage(
        stage_name="asr",
        enabled=bool(config.get("components", {}).get("asr", {}).get("enabled", True)),
        callback=lambda: WhisperAdapter(config.get("components", {}).get("asr", {})).transcribe(source_path),
    )

    proposals = _coerce_proposals(shot_stage.get("output"), duration_seconds)
    semantic_stage = _run_stage(
        stage_name="semantic",
        enabled=bool(config.get("components", {}).get("semantic", {}).get("enabled", True)),
        callback=lambda: XClipAdapter(config.get("components", {}).get("semantic", {})).score_segments(
            source_path,
            proposals,
            generic_queries,
        ),
    )
    keyframe_stage = _run_stage(
        stage_name="keyframes",
        enabled=bool(config.get("components", {}).get("keyframes", {}).get("enabled", True)),
        callback=lambda: SigLIPAdapter(config.get("components", {}).get("keyframes", {})).embed_segments(
            source_path,
            proposals,
        ),
    )

    rerank_stage = _run_stage(
        stage_name="reranker",
        enabled=bool(config.get("components", {}).get("reranker", {}).get("enabled", True)),
        callback=lambda: SmolVLM2Adapter(config.get("components", {}).get("reranker", {})).rerank_candidates(
            source_path,
            sorted(
                _build_candidate_segments(
                    proposals,
                    transcript_features=_build_transcript_features(
                        asr_stage.get("output"),
                        proposals,
                        transcript_keywords=transcript_keywords,
                    ),
                    semantic_scores=_index_by_window(semantic_stage.get("output"), "segment_scores", "semantic_score"),
                    novelty_scores=_index_keyframes(keyframe_stage.get("output")),
                    stage_weights=stage_weights,
                ),
                key=lambda row: (-row["base_score"], row["start_seconds"]),
            )[:shortlist_count],
        ),
    )
    structured_outputs = {
        "segment_boundaries": list(shot_stage.get("output", {}).get("boundaries", [])) if shot_stage.get("output") else [],
        "segment_proposals": proposals,
        "transcript_features": _build_transcript_features(
            asr_stage.get("output"),
            proposals,
            transcript_keywords=transcript_keywords,
        ),
        "semantic_scores": list(semantic_stage.get("output", {}).get("segment_scores", []))
        if semantic_stage.get("output")
        else [],
        "keyframe_features": list(keyframe_stage.get("output", {}).get("segments", []))
        if keyframe_stage.get("output")
        else [],
        "reranked_candidates": list(rerank_stage.get("output", {}).get("candidates", []))
        if rerank_stage.get("output")
        else [],
    }
    analysis = reconstruct_hf_multimodal_outputs(
        structured_outputs,
        {
            "duration_seconds": duration_seconds,
            "shortlist_count": shortlist_count,
            "stage_weights": stage_weights,
            "signal_thresholds": signal_thresholds,
        },
    )

    metadata = {
        "pipeline": {
            "duration_seconds": duration_seconds,
            "shortlist_count": shortlist_count,
            "generic_queries": generic_queries,
            "transcript_keywords": list(transcript_keywords),
            "stage_weights": stage_weights,
            "signal_thresholds": signal_thresholds,
        },
        "stage_statuses": {
            "shot_detector": shot_stage["status"],
            "asr": asr_stage["status"],
            "semantic": semantic_stage["status"],
            "keyframes": keyframe_stage["status"],
            "reranker": rerank_stage["status"],
        },
        "stages": {
            "shot_detector": _stage_payload(shot_stage),
            "asr": _stage_payload(asr_stage),
            "semantic": _stage_payload(semantic_stage),
            "keyframes": _stage_payload(keyframe_stage),
            "reranker": _stage_payload(rerank_stage),
        },
        "structured_outputs": analysis["structured_outputs"],
    }
    return ProxySourceEmission(signals=analysis["signals"], metadata=metadata)


def _run_stage(*, stage_name: str, enabled: bool, callback: Any) -> dict[str, Any]:
    if not enabled:
        return {"status": "skipped", "reason": "disabled by config", "output": None, "duration_ms": 0.0, "output_counts": {}}
    started_at = perf_counter()
    try:
        output = callback()
    except HFAdapterError as exc:
        return {
            "status": "failed",
            "reason": exc.message,
            "output": None,
            "duration_ms": round((perf_counter() - started_at) * 1000.0, 3),
            "output_counts": {},
        }
    except Exception as exc:  # pragma: no cover - defensive path
        return {
            "status": "failed",
            "reason": str(exc),
            "output": None,
            "duration_ms": round((perf_counter() - started_at) * 1000.0, 3),
            "output_counts": {},
        }
    return {
        "status": "ok",
        "reason": None,
        "output": output,
        "duration_ms": round((perf_counter() - started_at) * 1000.0, 3),
        "output_counts": _stage_output_counts(stage_name, output),
    }


def reconstruct_hf_multimodal_outputs(
    structured_outputs: dict[str, Any] | None,
    pipeline_config: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = structured_outputs if isinstance(structured_outputs, dict) else {}
    config = pipeline_config if isinstance(pipeline_config, dict) else {}
    duration_seconds = _as_float(config.get("duration_seconds"), default=None)
    proposals = _coerce_proposals({"proposals": list(payload.get("segment_proposals", []))}, duration_seconds)
    transcript_features = [
        _normalized_window_row(row, include_text=True)
        for row in list(payload.get("transcript_features", []))
        if isinstance(row, dict)
    ]
    semantic_rows = [_normalized_window_row(row, score_field="semantic_score") for row in list(payload.get("semantic_scores", [])) if isinstance(row, dict)]
    keyframe_rows = [
        _normalized_window_row(row, score_field="novelty_score", include_keyframe=True)
        for row in list(payload.get("keyframe_features", []))
        if isinstance(row, dict)
    ]
    rerank_rows = [
        _normalized_window_row(row, score_field="rerank_score", include_reasoning=True, include_base_score=True)
        for row in list(payload.get("reranked_candidates", []))
        if isinstance(row, dict)
    ]
    stage_weights = {
        "proposal": float(config.get("stage_weights", {}).get("proposal", 0.35)),
        "transcript": float(config.get("stage_weights", {}).get("transcript", 0.20)),
        "semantic": float(config.get("stage_weights", {}).get("semantic", 0.25)),
        "novelty": float(config.get("stage_weights", {}).get("novelty", 0.20)),
    }
    signal_thresholds = {
        "proposal": float(config.get("signal_thresholds", {}).get("proposal", 0.55)),
        "transcript": float(config.get("signal_thresholds", {}).get("transcript", 0.60)),
        "semantic": float(config.get("signal_thresholds", {}).get("semantic", 0.60)),
        "novelty": float(config.get("signal_thresholds", {}).get("novelty", 0.60)),
        "rerank": float(config.get("signal_thresholds", {}).get("rerank", 0.65)),
    }
    semantic_scores = _index_by_window({"segment_scores": semantic_rows}, "segment_scores", "semantic_score")
    novelty_scores = _index_keyframes({"segments": keyframe_rows})
    base_candidates = _build_candidate_segments(
        proposals,
        transcript_features=transcript_features,
        semantic_scores=semantic_scores,
        novelty_scores=novelty_scores,
        stage_weights=stage_weights,
    )
    shortlist_count = max(1, int(config.get("shortlist_count", 5)))
    shortlisted = sorted(base_candidates, key=lambda row: (-row["base_score"], row["start_seconds"]))[:shortlist_count]
    reranked_candidates = _merge_reranked_candidates(shortlisted, {"candidates": rerank_rows})
    signals = _signals_from_hf_outputs(
        proposals=proposals,
        transcript_features=transcript_features,
        semantic_scores=semantic_scores,
        novelty_scores=novelty_scores,
        reranked_candidates=reranked_candidates,
        thresholds=signal_thresholds,
    )
    return {
        "proposals": proposals,
        "transcript_features": transcript_features,
        "semantic_rows": semantic_rows,
        "keyframe_rows": keyframe_rows,
        "base_candidates": base_candidates,
        "shortlisted_candidates": shortlisted,
        "reranked_candidates": reranked_candidates,
        "signals": signals,
        "structured_outputs": {
            "segment_boundaries": list(payload.get("segment_boundaries", [])),
            "segment_proposals": proposals,
            "transcript_features": transcript_features,
            "semantic_scores": semantic_rows,
            "keyframe_features": keyframe_rows,
            "reranked_candidates": reranked_candidates,
        },
    }


def _coerce_proposals(shot_output: dict[str, Any] | None, duration_seconds: float | None) -> list[dict[str, Any]]:
    proposals = [dict(row) for row in list((shot_output or {}).get("proposals", []))]
    if proposals:
        return proposals
    if duration_seconds is None or duration_seconds <= 0:
        duration_seconds = 30.0
    return [
        {
            "start_seconds": 0.0,
            "end_seconds": round(float(duration_seconds), 4),
            "proposal_score": 0.5,
        }
    ]


def _build_transcript_features(
    asr_output: dict[str, Any] | None,
    proposals: list[dict[str, Any]],
    *,
    transcript_keywords: tuple[str, ...],
) -> list[dict[str, Any]]:
    if not asr_output:
        return []
    asr_segments = list(asr_output.get("segments", []))
    features: list[dict[str, Any]] = []
    if asr_segments:
        for proposal in proposals:
            start_seconds = float(proposal["start_seconds"])
            end_seconds = float(proposal["end_seconds"])
            overlapping = [
                row
                for row in asr_segments
                if float(row.get("start_seconds", 0.0)) < end_seconds and float(row.get("end_seconds", 0.0)) > start_seconds
            ]
            if not overlapping:
                continue
            text = " ".join(str(row.get("text", "")).strip() for row in overlapping).strip()
            features.append(
                {
                    "start_seconds": round(start_seconds, 4),
                    "end_seconds": round(end_seconds, 4),
                    "text": text,
                    **_transcript_salience(text, transcript_keywords),
                }
            )
        return features

    transcript = str(asr_output.get("transcript", "")).strip()
    if transcript:
        start_seconds = float(proposals[0]["start_seconds"])
        end_seconds = float(proposals[-1]["end_seconds"])
        features.append(
            {
                "start_seconds": round(start_seconds, 4),
                "end_seconds": round(end_seconds, 4),
                "text": transcript,
                **_transcript_salience(transcript, transcript_keywords),
            }
        )
    return features


def _transcript_salience(text: str, keywords: tuple[str, ...]) -> dict[str, Any]:
    lowered = text.lower()
    hits = sorted({keyword for keyword in keywords if keyword in lowered})
    punctuation_bonus = min(0.2, 0.05 * sum(lowered.count(token) for token in ("!", "?")))
    salience_score = min(1.0, len(hits) * 0.2 + punctuation_bonus)
    return {
        "keyword_hits": hits,
        "salience_score": round(salience_score, 4),
    }


def _index_by_window(output: dict[str, Any] | None, field_name: str, score_field: str) -> dict[tuple[float, float], dict[str, Any]]:
    indexed: dict[tuple[float, float], dict[str, Any]] = {}
    if not output:
        return indexed
    for row in list(output.get(field_name, [])):
        key = (round(float(row.get("start_seconds", 0.0)), 4), round(float(row.get("end_seconds", 0.0)), 4))
        indexed[key] = dict(row)
        indexed[key][score_field] = float(row.get(score_field, 0.0))
    return indexed


def _index_keyframes(output: dict[str, Any] | None) -> dict[tuple[float, float], dict[str, Any]]:
    indexed: dict[tuple[float, float], dict[str, Any]] = {}
    if not output:
        return indexed
    for row in list(output.get("segments", [])):
        key = (round(float(row.get("start_seconds", 0.0)), 4), round(float(row.get("end_seconds", 0.0)), 4))
        indexed[key] = dict(row)
        indexed[key]["novelty_score"] = float(row.get("novelty_score", 0.0))
    return indexed


def _build_candidate_segments(
    proposals: list[dict[str, Any]],
    *,
    transcript_features: list[dict[str, Any]],
    semantic_scores: dict[tuple[float, float], dict[str, Any]],
    novelty_scores: dict[tuple[float, float], dict[str, Any]],
    stage_weights: dict[str, float],
) -> list[dict[str, Any]]:
    transcript_index = {
        (round(float(row["start_seconds"]), 4), round(float(row["end_seconds"]), 4)): row
        for row in transcript_features
    }
    candidates: list[dict[str, Any]] = []
    for proposal in proposals:
        key = (round(float(proposal["start_seconds"]), 4), round(float(proposal["end_seconds"]), 4))
        proposal_score = float(proposal.get("proposal_score", 0.0))
        transcript_score = float(transcript_index.get(key, {}).get("salience_score", 0.0))
        semantic_score = float(semantic_scores.get(key, {}).get("semantic_score", 0.0))
        novelty_score = float(novelty_scores.get(key, {}).get("novelty_score", 0.0))
        base_score = min(
            1.0,
            proposal_score * stage_weights["proposal"]
            + transcript_score * stage_weights["transcript"]
            + semantic_score * stage_weights["semantic"]
            + novelty_score * stage_weights["novelty"],
        )
        candidates.append(
            {
                "start_seconds": key[0],
                "end_seconds": key[1],
                "proposal_score": round(proposal_score, 4),
                "transcript_score": round(transcript_score, 4),
                "semantic_score": round(semantic_score, 4),
                "novelty_score": round(novelty_score, 4),
                "base_score": round(base_score, 4),
            }
        )
    return candidates


def _merge_reranked_candidates(
    shortlisted: list[dict[str, Any]],
    rerank_output: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    reranked_index = _index_by_window(rerank_output, "candidates", "rerank_score")
    merged: list[dict[str, Any]] = []
    for row in shortlisted:
        key = (round(float(row["start_seconds"]), 4), round(float(row["end_seconds"]), 4))
        payload = dict(row)
        rerank_row = reranked_index.get(key)
        if rerank_row is not None:
            payload["rerank_score"] = round(float(rerank_row.get("rerank_score", row["base_score"])), 4)
            payload["reason"] = str(rerank_row.get("reason", "")).strip()
            payload["reason_codes"] = [str(item) for item in list(rerank_row.get("reason_codes", []))]
        else:
            payload["rerank_score"] = payload["base_score"]
            payload["reason"] = ""
            payload["reason_codes"] = []
        merged.append(payload)
    return merged


def _signals_from_hf_outputs(
    *,
    proposals: list[dict[str, Any]],
    transcript_features: list[dict[str, Any]],
    semantic_scores: dict[tuple[float, float], dict[str, Any]],
    novelty_scores: dict[tuple[float, float], dict[str, Any]],
    reranked_candidates: list[dict[str, Any]],
    thresholds: dict[str, float],
) -> list[ProxySignal]:
    signals: list[ProxySignal] = []
    for proposal in proposals:
        score = float(proposal.get("proposal_score", 0.0))
        if score < thresholds["proposal"]:
            continue
        signals.append(
            ProxySignal(
                source="hf_shot_boundary",
                source_family="hf_multimodal",
                timestamp=round(float(proposal["start_seconds"]), 3),
                strength=round(score, 4),
                confidence=0.7,
                reason=f"shot proposal score={score:.2f}",
            )
        )
    for row in transcript_features:
        score = float(row.get("salience_score", 0.0))
        if score < thresholds["transcript"]:
            continue
        timestamp = (float(row["start_seconds"]) + float(row["end_seconds"])) / 2.0
        signals.append(
            ProxySignal(
                source="hf_transcript_salience",
                source_family="hf_multimodal",
                timestamp=round(timestamp, 3),
                strength=round(score, 4),
                confidence=0.74,
                reason=f"transcript salience hits={','.join(row.get('keyword_hits', [])) or 'none'}",
            )
        )
    for row in semantic_scores.values():
        score = float(row.get("semantic_score", 0.0))
        if score < thresholds["semantic"]:
            continue
        timestamp = (float(row["start_seconds"]) + float(row["end_seconds"])) / 2.0
        signals.append(
            ProxySignal(
                source="hf_semantic_match",
                source_family="hf_multimodal",
                timestamp=round(timestamp, 3),
                strength=round(score, 4),
                confidence=0.76,
                reason=f"semantic top_query={row.get('top_query', '') or 'unknown'}",
            )
        )
    for row in novelty_scores.values():
        score = float(row.get("novelty_score", 0.0))
        if score < thresholds["novelty"]:
            continue
        signals.append(
            ProxySignal(
                source="hf_keyframe_novelty",
                source_family="hf_multimodal",
                timestamp=round(float(row.get("keyframe_timestamp_seconds", row["start_seconds"])), 3),
                strength=round(score, 4),
                confidence=0.72,
                reason=f"keyframe novelty cluster={row.get('cluster_id', 0)}",
            )
        )
    for row in reranked_candidates:
        score = float(row.get("rerank_score", 0.0))
        if score < thresholds["rerank"]:
            continue
        timestamp = (float(row["start_seconds"]) + float(row["end_seconds"])) / 2.0
        signals.append(
            ProxySignal(
                source="hf_rerank_highlight",
                source_family="hf_multimodal",
                timestamp=round(timestamp, 3),
                strength=round(score, 4),
                confidence=0.80,
                reason=str(row.get("reason", "")).strip() or "reranker promoted candidate",
            )
        )
    signals.sort(key=lambda signal: (signal.timestamp, signal.source))
    return signals


def _stage_payload(stage: dict[str, Any]) -> dict[str, Any]:
    payload = {"status": stage["status"]}
    if stage.get("reason"):
        payload["reason"] = stage["reason"]
    payload["duration_ms"] = round(float(stage.get("duration_ms", 0.0)), 3)
    payload["output_counts"] = dict(stage.get("output_counts", {}))
    if stage.get("output") is not None:
        payload["output"] = stage["output"]
    return payload


def _stage_output_counts(stage_name: str, output: dict[str, Any] | None) -> dict[str, int]:
    if not isinstance(output, dict):
        return {}
    if stage_name == "shot_detector":
        return {
            "boundary_count": len(list(output.get("boundaries", []))),
            "proposal_count": len(list(output.get("proposals", []))),
        }
    if stage_name == "asr":
        return {"segment_count": len(list(output.get("segments", [])))}
    if stage_name == "semantic":
        return {"segment_count": len(list(output.get("segment_scores", [])))}
    if stage_name == "keyframes":
        return {"segment_count": len(list(output.get("segments", [])))}
    if stage_name == "reranker":
        return {"candidate_count": len(list(output.get("candidates", [])))}
    return {}


def _normalized_window_row(
    row: dict[str, Any],
    *,
    score_field: str | None = None,
    include_text: bool = False,
    include_keyframe: bool = False,
    include_reasoning: bool = False,
    include_base_score: bool = False,
) -> dict[str, Any]:
    payload = {
        "start_seconds": round(float(row.get("start_seconds", 0.0)), 4),
        "end_seconds": round(float(row.get("end_seconds", 0.0)), 4),
    }
    if score_field is not None:
        payload[score_field] = round(float(row.get(score_field, 0.0)), 4)
    if include_text:
        payload["text"] = str(row.get("text", "")).strip()
        payload["keyword_hits"] = [str(item) for item in list(row.get("keyword_hits", []))]
        payload["salience_score"] = round(float(row.get("salience_score", 0.0)), 4)
    if include_keyframe:
        payload["keyframe_timestamp_seconds"] = round(float(row.get("keyframe_timestamp_seconds", payload["start_seconds"])), 4)
        payload["cluster_id"] = int(row.get("cluster_id", 0) or 0)
    if include_reasoning:
        payload["reason"] = str(row.get("reason", "")).strip()
        payload["reason_codes"] = [str(item) for item in list(row.get("reason_codes", []))]
    if include_base_score:
        payload["base_score"] = round(float(row.get("base_score", 0.0)), 4)
    if "query_scores" in row:
        payload["query_scores"] = {
            str(key): round(float(value), 4)
            for key, value in dict(row.get("query_scores", {})).items()
        }
    if "top_query" in row:
        payload["top_query"] = str(row.get("top_query", "")).strip()
    if "proposal_score" in row:
        payload["proposal_score"] = round(float(row.get("proposal_score", 0.0)), 4)
    if "transcript_score" in row:
        payload["transcript_score"] = round(float(row.get("transcript_score", 0.0)), 4)
    if "semantic_score" in row and score_field != "semantic_score":
        payload["semantic_score"] = round(float(row.get("semantic_score", 0.0)), 4)
    if "novelty_score" in row and score_field != "novelty_score":
        payload["novelty_score"] = round(float(row.get("novelty_score", 0.0)), 4)
    if "rerank_score" in row and score_field != "rerank_score":
        payload["rerank_score"] = round(float(row.get("rerank_score", 0.0)), 4)
    return payload


def _as_float(value: Any, *, default: float | None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

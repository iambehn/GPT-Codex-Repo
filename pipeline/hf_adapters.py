from __future__ import annotations

import math
import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline.hf_runtime import (
    check_hf_runtime,
    format_runtime_failure,
    resolve_ffmpeg_path,
    resolve_torch_device,
)


class HFAdapterError(RuntimeError):
    def __init__(self, stage: str, message: str) -> None:
        super().__init__(message)
        self.stage = stage
        self.message = message


@dataclass(frozen=True)
class HFModelSpec:
    model_id: str
    revision: str
    execution_mode: str = "local"
    runtime_options: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "revision": self.revision,
            "execution_mode": self.execution_mode,
            "runtime_options": dict(self.runtime_options or {}),
        }


def model_spec_from_config(config: dict[str, Any], *, defaults: HFModelSpec) -> HFModelSpec:
    return HFModelSpec(
        model_id=str(config.get("model_id", defaults.model_id)),
        revision=str(config.get("revision", defaults.revision)),
        execution_mode=str(config.get("execution_mode", defaults.execution_mode)),
        runtime_options=dict(config.get("runtime_options", defaults.runtime_options or {})),
    )


TRANSNETV2_DEFAULT = HFModelSpec(
    model_id="georgesung/shot-boundary-detection-transnet-v2",
    revision="main",
)
WHISPER_DEFAULT = HFModelSpec(
    model_id="openai/whisper-large-v3-turbo",
    revision="main",
)
DISTIL_WHISPER_DEFAULT = HFModelSpec(
    model_id="distil-whisper/distil-large-v3.5",
    revision="main",
)
XCLIP_DEFAULT = HFModelSpec(
    model_id="microsoft/xclip-base-patch32",
    revision="main",
)
SIGLIP_DEFAULT = HFModelSpec(
    model_id="google/siglip-so400m-patch14-384",
    revision="main",
)
SMOLVLM2_DEFAULT = HFModelSpec(
    model_id="HuggingFaceTB/SmolVLM2-2.2B-Instruct",
    revision="main",
)
ALLOWED_RERANK_REASON_CODES = (
    "high_action",
    "clutch_moment",
    "objective_swing",
    "crowd_or_text_hype",
    "novel_visual_event",
    "low_visual_signal",
    "repetitive_scene",
    "unclear_highlight",
)


class TransNetV2Adapter:
    stage_name = "shot_detector"

    def __init__(self, config: dict[str, Any]) -> None:
        self.spec = model_spec_from_config(config, defaults=TRANSNETV2_DEFAULT)

    def detect_shots(self, source: str | Path) -> dict[str, Any]:
        result = _run_transnetv2_backend(Path(str(source)), self.spec)
        return _normalize_shot_output(result, self.spec)


class WhisperAdapter:
    stage_name = "asr"

    def __init__(self, config: dict[str, Any]) -> None:
        self.spec = model_spec_from_config(config, defaults=WHISPER_DEFAULT)

    def transcribe(self, source: str | Path) -> dict[str, Any]:
        result = _run_whisper_backend(Path(str(source)), self.spec)
        return _normalize_asr_output(result, self.spec)


class XClipAdapter:
    stage_name = "semantic"

    def __init__(self, config: dict[str, Any]) -> None:
        self.spec = model_spec_from_config(config, defaults=XCLIP_DEFAULT)

    def score_segments(
        self,
        source: str | Path,
        segments: list[dict[str, Any]],
        queries: list[str],
    ) -> dict[str, Any]:
        result = _run_xclip_backend(Path(str(source)), segments, queries, self.spec)
        return _normalize_semantic_output(result, self.spec, queries)


class SigLIPAdapter:
    stage_name = "keyframes"

    def __init__(self, config: dict[str, Any]) -> None:
        self.spec = model_spec_from_config(config, defaults=SIGLIP_DEFAULT)

    def embed_segments(self, source: str | Path, segments: list[dict[str, Any]]) -> dict[str, Any]:
        result = _run_siglip_backend(Path(str(source)), segments, self.spec)
        return _normalize_keyframe_output(result, self.spec)


class SmolVLM2Adapter:
    stage_name = "reranker"

    def __init__(self, config: dict[str, Any]) -> None:
        self.spec = model_spec_from_config(config, defaults=SMOLVLM2_DEFAULT)

    def rerank_candidates(
        self,
        source: str | Path,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        result = _run_smolvlm_backend(Path(str(source)), candidates, self.spec)
        return _normalize_rerank_output(result, self.spec)


def _normalize_shot_output(result: dict[str, Any], spec: HFModelSpec) -> dict[str, Any]:
    boundaries: list[dict[str, Any]] = []
    for row in list(result.get("boundaries", [])):
        timestamp = float(row.get("timestamp_seconds", 0.0))
        boundaries.append(
            {
                "timestamp_seconds": round(timestamp, 4),
                "boundary_score": round(float(row.get("boundary_score", 0.0)), 4),
            }
        )

    proposals: list[dict[str, Any]] = []
    for row in list(result.get("proposals", [])):
        start_seconds = float(row.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
        proposals.append(
            {
                "start_seconds": round(start_seconds, 4),
                "end_seconds": round(end_seconds, 4),
                "proposal_score": round(float(row.get("proposal_score", 0.0)), 4),
            }
        )

    return {
        "stage": "shot_detector",
        "backend": _shot_detector_backend(spec),
        "model": spec.to_dict(),
        "boundaries": boundaries,
        "proposals": proposals,
    }


def _normalize_asr_output(result: dict[str, Any], spec: HFModelSpec) -> dict[str, Any]:
    transcript = str(result.get("transcript", "")).strip()
    segments: list[dict[str, Any]] = []
    for row in list(result.get("segments", [])):
        start_seconds = float(row.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
        segments.append(
            {
                "start_seconds": round(start_seconds, 4),
                "end_seconds": round(end_seconds, 4),
                "text": str(row.get("text", "")).strip(),
            }
        )
    return {
        "stage": "asr",
        "backend": _asr_backend(spec),
        "model": spec.to_dict(),
        "transcript": transcript,
        "segments": segments,
    }


def _normalize_semantic_output(result: dict[str, Any], spec: HFModelSpec, queries: list[str]) -> dict[str, Any]:
    segment_scores: list[dict[str, Any]] = []
    for row in list(result.get("segment_scores", [])):
        start_seconds = float(row.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
        query_scores = {
            str(key): round(float(value), 4)
            for key, value in dict(row.get("query_scores", {})).items()
        }
        segment_scores.append(
            {
                "start_seconds": round(start_seconds, 4),
                "end_seconds": round(end_seconds, 4),
                "query_scores": query_scores,
                "top_query": str(row.get("top_query", "")),
                "semantic_score": round(float(row.get("semantic_score", 0.0)), 4),
            }
        )
    return {
        "stage": "semantic",
        "model": spec.to_dict(),
        "queries": list(queries),
        "segment_scores": segment_scores,
    }


def _normalize_keyframe_output(result: dict[str, Any], spec: HFModelSpec) -> dict[str, Any]:
    segments: list[dict[str, Any]] = []
    for row in list(result.get("segments", [])):
        start_seconds = float(row.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
        segments.append(
            {
                "start_seconds": round(start_seconds, 4),
                "end_seconds": round(end_seconds, 4),
                "keyframe_timestamp_seconds": round(float(row.get("keyframe_timestamp_seconds", start_seconds)), 4),
                "novelty_score": round(float(row.get("novelty_score", 0.0)), 4),
                "cluster_id": int(row.get("cluster_id", 0) or 0),
            }
        )
    return {
        "stage": "keyframes",
        "model": spec.to_dict(),
        "segments": segments,
    }


def _normalize_rerank_output(result: dict[str, Any], spec: HFModelSpec) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for row in list(result.get("candidates", [])):
        start_seconds = float(row.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
        candidates.append(
            {
                "start_seconds": round(start_seconds, 4),
                "end_seconds": round(end_seconds, 4),
                "base_score": round(float(row.get("base_score", 0.0)), 4),
                "rerank_score": round(float(row.get("rerank_score", 0.0)), 4),
                "reason": str(row.get("reason", "")).strip(),
                "reason_codes": [str(item) for item in list(row.get("reason_codes", []))],
            }
        )
    return {
        "stage": "reranker",
        "model": spec.to_dict(),
        "candidates": candidates,
    }


def _run_transnetv2_backend(source: Path, spec: HFModelSpec) -> dict[str, Any]:
    backend = _shot_detector_backend(spec)
    if backend == "pyscenedetect":
        return _run_pyscenedetect_backend(source, spec)
    if backend != "transnetv2":
        raise HFAdapterError("shot_detector", f"unsupported proposal backend '{backend}'")
    runtime = check_hf_runtime(
        stage_name="shot_detector",
        execution_mode=spec.execution_mode,
        packages=("torch", "transnetv2_pytorch", "cv2"),
        needs_ffmpeg=True,
    )
    if not runtime["ok"]:
        raise HFAdapterError("shot_detector", format_runtime_failure(runtime))

    try:
        model = _load_transnetv2_model(spec)
        fps = _video_fps(source)
        threshold = float((spec.runtime_options or {}).get("threshold", 0.5))
        predictions, scenes = _run_transnetv2_detection(model, source, threshold=threshold)
    except HFAdapterError:
        raise
    except Exception as exc:
        raise HFAdapterError("shot_detector", f"backend execution failed: {exc}") from exc

    proposals = _scene_rows_to_proposals(scenes, fps=fps)
    boundaries = _prediction_rows_to_boundaries(predictions, fps=fps, threshold=threshold)
    if not proposals and boundaries:
        proposals = _boundaries_to_proposals(boundaries, fps=fps, frame_total=len(predictions))
    return {
        "boundaries": boundaries,
        "proposals": proposals,
    }


def _run_whisper_backend(source: Path, spec: HFModelSpec) -> dict[str, Any]:
    backend = _asr_backend(spec)
    if backend not in {"whisper", "distil_whisper"}:
        raise HFAdapterError("asr", f"unsupported asr backend '{backend}'")
    runtime = check_hf_runtime(
        stage_name="asr",
        execution_mode=spec.execution_mode,
        packages=("torch", "transformers"),
        needs_ffmpeg=True,
    )
    if not runtime["ok"]:
        raise HFAdapterError("asr", format_runtime_failure(runtime))

    audio_path: Path | None = None
    try:
        pipeline = _load_whisper_pipeline(spec, backend=backend)
        audio_path = _extract_audio_for_whisper(source, spec)
        chunk_length_s = float((spec.runtime_options or {}).get("chunk_length_s", 30.0))
        batch_size = int((spec.runtime_options or {}).get("batch_size", 8))
        result = pipeline(
            str(audio_path),
            return_timestamps=True,
            chunk_length_s=chunk_length_s,
            batch_size=batch_size,
        )
    except HFAdapterError:
        raise
    except Exception as exc:
        raise HFAdapterError("asr", f"backend execution failed: {exc}") from exc
    finally:
        if audio_path is not None and audio_path.exists():
            audio_path.unlink()

    chunks = list(result.get("chunks", [])) or list(result.get("segments", []))
    segments: list[dict[str, Any]] = []
    for row in chunks:
        timestamp = row.get("timestamp") or row.get("timestamps") or (0.0, 0.0)
        start_seconds, end_seconds = _coerce_timestamp_pair(timestamp)
        segments.append(
            {
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "text": str(row.get("text", "")).strip(),
            }
        )
    return {
        "transcript": str(result.get("text", "")).strip(),
        "segments": segments,
    }


def _run_pyscenedetect_backend(source: Path, spec: HFModelSpec) -> dict[str, Any]:
    runtime = check_hf_runtime(
        stage_name="shot_detector",
        execution_mode=spec.execution_mode,
        packages=("cv2", "scenedetect"),
        needs_ffmpeg=True,
    )
    if not runtime["ok"]:
        raise HFAdapterError("shot_detector", format_runtime_failure(runtime))

    try:
        threshold = float((spec.runtime_options or {}).get("threshold", 27.0))
        scenes = _run_pyscenedetect_detection(source, threshold=threshold)
    except HFAdapterError:
        raise
    except Exception as exc:
        raise HFAdapterError("shot_detector", f"backend execution failed: {exc}") from exc

    proposals = [
        {
            "start_seconds": round(float(start_seconds), 4),
            "end_seconds": round(max(float(start_seconds), float(end_seconds)), 4),
            "proposal_score": 0.8,
        }
        for start_seconds, end_seconds in scenes
    ]
    boundaries = [
        {
            "timestamp_seconds": round(float(end_seconds), 4),
            "boundary_score": 0.8,
        }
        for _, end_seconds in scenes[:-1]
    ]
    return {
        "boundaries": boundaries,
        "proposals": proposals,
    }


def _run_xclip_backend(
    source: Path,
    segments: list[dict[str, Any]],
    queries: list[str],
    spec: HFModelSpec,
) -> dict[str, Any]:
    runtime = check_hf_runtime(
        stage_name="semantic",
        execution_mode=spec.execution_mode,
        packages=("torch", "transformers", "numpy", "cv2"),
    )
    if not runtime["ok"]:
        raise HFAdapterError("semantic", format_runtime_failure(runtime))
    if not segments or not queries:
        return {"segment_scores": []}

    try:
        runtime_objects = _load_xclip_runtime(spec)
    except HFAdapterError:
        raise
    except Exception as exc:
        raise HFAdapterError("semantic", f"backend execution failed: {exc}") from exc

    segment_scores: list[dict[str, Any]] = []
    frame_count = max(1, int((spec.runtime_options or {}).get("frame_count", 8)))
    for segment in segments:
        start_seconds = float(segment.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(segment.get("end_seconds", start_seconds)))
        try:
            frames = _sample_segment_frames(
                source,
                start_seconds=start_seconds,
                end_seconds=end_seconds,
                frame_count=frame_count,
            )
            scores = _score_xclip_queries(runtime_objects, frames=frames, queries=queries)
        except HFAdapterError:
            raise
        except Exception as exc:
            raise HFAdapterError("semantic", f"backend execution failed: {exc}") from exc

        top_query = max(scores, key=scores.get) if scores else ""
        segment_scores.append(
            {
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "query_scores": scores,
                "top_query": top_query,
                "semantic_score": float(scores.get(top_query, 0.0)),
            }
        )
    return {"segment_scores": segment_scores}


def _run_siglip_backend(
    source: Path,
    segments: list[dict[str, Any]],
    spec: HFModelSpec,
) -> dict[str, Any]:
    runtime = check_hf_runtime(
        stage_name="keyframes",
        execution_mode=spec.execution_mode,
        packages=("torch", "transformers", "numpy", "cv2"),
    )
    if not runtime["ok"]:
        raise HFAdapterError("keyframes", format_runtime_failure(runtime))
    if not segments:
        return {"segments": []}

    try:
        runtime_objects = _load_siglip_runtime(spec)
    except HFAdapterError:
        raise
    except Exception as exc:
        raise HFAdapterError("keyframes", f"backend execution failed: {exc}") from exc

    keyframe_rows: list[dict[str, Any]] = []
    embeddings: list[Any] = []
    for segment in segments:
        start_seconds = float(segment.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(segment.get("end_seconds", start_seconds)))
        keyframe_timestamp_seconds = round((start_seconds + end_seconds) / 2.0, 4)
        try:
            frame = _sample_segment_midpoint_frame(
                source,
                start_seconds=start_seconds,
                end_seconds=end_seconds,
            )
            embedding = _embed_siglip_frame(runtime_objects, frame=frame)
        except HFAdapterError:
            raise
        except Exception as exc:
            raise HFAdapterError("keyframes", f"backend execution failed: {exc}") from exc

        keyframe_rows.append(
            {
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "keyframe_timestamp_seconds": keyframe_timestamp_seconds,
            }
        )
        embeddings.append(embedding)

    novelty_scores, cluster_ids = _derive_siglip_novelty_and_clusters(embeddings, spec)
    return {
        "segments": [
            {
                **row,
                "novelty_score": float(novelty_scores[index]),
                "cluster_id": int(cluster_ids[index]),
            }
            for index, row in enumerate(keyframe_rows)
        ]
    }


def _run_smolvlm_backend(
    source: Path,
    candidates: list[dict[str, Any]],
    spec: HFModelSpec,
) -> dict[str, Any]:
    runtime = check_hf_runtime(
        stage_name="reranker",
        execution_mode=spec.execution_mode,
        packages=("torch", "transformers", "cv2", "PIL"),
    )
    if not runtime["ok"]:
        raise HFAdapterError("reranker", format_runtime_failure(runtime))
    if not candidates:
        return {"candidates": []}

    try:
        runtime_objects = _load_smolvlm_runtime(spec)
    except HFAdapterError:
        raise
    except Exception as exc:
        raise HFAdapterError("reranker", f"backend execution failed: {exc}") from exc

    reranked_candidates: list[dict[str, Any]] = []
    frame_count = max(1, int((spec.runtime_options or {}).get("frames_per_candidate", 3)))
    for candidate in candidates:
        start_seconds = float(candidate.get("start_seconds", 0.0))
        end_seconds = max(start_seconds, float(candidate.get("end_seconds", start_seconds)))
        try:
            frames = _sample_segment_frames(
                source,
                start_seconds=start_seconds,
                end_seconds=end_seconds,
                frame_count=frame_count,
            )
            response_text = _generate_smolvlm_rerank_response(
                runtime_objects,
                frames=frames,
                candidate=candidate,
                spec=spec,
            )
            parsed = _parse_smolvlm_rerank_response(response_text)
        except HFAdapterError:
            raise
        except Exception as exc:
            raise HFAdapterError("reranker", f"backend execution failed: {exc}") from exc
        if parsed is None:
            continue
        reranked_candidates.append(
            {
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "base_score": float(candidate.get("base_score", 0.0)),
                "rerank_score": float(parsed["rerank_score"]),
                "reason": str(parsed["reason"]).strip(),
                "reason_codes": list(parsed["reason_codes"]),
            }
        )
    return {"candidates": reranked_candidates}


def _load_transnetv2_model(spec: HFModelSpec) -> Any:
    import importlib

    torch = importlib.import_module("torch")
    module = importlib.import_module("transnetv2_pytorch")
    model_class = getattr(module, "TransNetV2", None)
    if model_class is None:
        raise HFAdapterError(
            "shot_detector",
            "missing model runtime capability: transnetv2_pytorch.TransNetV2 was not found",
        )

    runtime_options = spec.runtime_options or {}
    device = resolve_torch_device(torch, runtime_options.get("device"))
    model_kwargs: dict[str, Any] = {}
    weights_path = str(runtime_options.get("weights_path", "")).strip()
    if weights_path:
        model_kwargs["weights_path"] = weights_path

    try:
        model = model_class(device=device, **model_kwargs)
    except TypeError:
        model = model_class(**model_kwargs)
        if hasattr(model, "to"):
            model.to(device)
    return model


def _run_transnetv2_detection(model: Any, source: Path, *, threshold: float) -> tuple[list[float], list[Any]]:
    if hasattr(model, "detect_scenes"):
        result = model.detect_scenes(str(source), threshold=threshold)
        predictions = _tensor_like_to_list(result.get("single_frame_predictions", []))
        scenes = list(result.get("scenes", [])) or list(result.get("scenes_with_data", []))
        if predictions or scenes:
            return predictions, scenes

    if hasattr(model, "predict_video"):
        predict_result = model.predict_video(str(source))
        if not isinstance(predict_result, tuple) or len(predict_result) < 2:
            raise HFAdapterError(
                "shot_detector",
                "missing model runtime capability: predict_video() returned an unsupported result",
            )
        single_frame_predictions = _tensor_like_to_list(predict_result[1])
        scenes: list[Any] = []
        if hasattr(model, "predictions_to_scenes"):
            scenes = list(model.predictions_to_scenes(single_frame_predictions, threshold=threshold))
        return single_frame_predictions, scenes

    raise HFAdapterError(
        "shot_detector",
        "missing model runtime capability: TransNetV2 runtime must provide detect_scenes() or predict_video()",
    )


def _load_whisper_pipeline(spec: HFModelSpec, *, backend: str) -> Any:
    import importlib

    torch = importlib.import_module("torch")
    transformers = importlib.import_module("transformers")
    pipeline_factory = getattr(transformers, "pipeline", None)
    if pipeline_factory is None:
        raise HFAdapterError("asr", "missing model runtime capability: transformers.pipeline was not found")

    runtime_options = spec.runtime_options or {}
    device = resolve_torch_device(torch, runtime_options.get("device"))
    dtype = getattr(torch, "float16", None) if device == "cuda" else getattr(torch, "float32", None)
    pipeline_kwargs: dict[str, Any] = {
        "task": "automatic-speech-recognition",
        "model": _resolve_asr_model_id(spec, backend=backend),
        "revision": spec.revision,
    }
    if device == "cpu":
        pipeline_kwargs["device"] = -1
    else:
        pipeline_kwargs["device"] = device
    if dtype is not None:
        pipeline_kwargs["torch_dtype"] = dtype
    return pipeline_factory(**pipeline_kwargs)


def _run_pyscenedetect_detection(source: Path, *, threshold: float) -> list[tuple[float, float]]:
    import importlib

    module = importlib.import_module("scenedetect")
    scene_manager_class = getattr(module, "SceneManager", None)
    content_detector_class = getattr(module, "ContentDetector", None)
    open_video = getattr(module, "open_video", None)
    if scene_manager_class is None or content_detector_class is None or open_video is None:
        raise HFAdapterError(
            "shot_detector",
            "missing model runtime capability: scenedetect open_video/SceneManager/ContentDetector are required",
        )

    video = open_video(str(source))
    scene_manager = scene_manager_class()
    scene_manager.add_detector(content_detector_class(threshold=threshold))
    scene_manager.detect_scenes(video)
    scenes = list(scene_manager.get_scene_list())
    if not scenes:
        return [(0.0, _video_duration_seconds(source))]
    rows: list[tuple[float, float]] = []
    for start_time, end_time in scenes:
        start_seconds = _scene_timecode_to_seconds(start_time)
        end_seconds = _scene_timecode_to_seconds(end_time)
        rows.append((start_seconds, end_seconds))
    return rows


def _extract_audio_for_whisper(source: Path, spec: HFModelSpec) -> Path:
    runtime_options = spec.runtime_options or {}
    sample_rate = max(8000, int(runtime_options.get("sample_rate", 16000)))
    ffmpeg = resolve_ffmpeg_path()
    if ffmpeg is None:
        raise HFAdapterError("asr", "missing dependency or runtime capability: ffmpeg")

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
        output_path = Path(handle.name)
    command = [
        str(ffmpeg),
        "-v",
        "error",
        "-y",
        "-i",
        str(source),
        "-vn",
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        str(output_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        output_path.unlink(missing_ok=True)
        stderr = result.stderr.strip() or "unknown ffmpeg error"
        raise HFAdapterError("asr", f"backend execution failed: ffmpeg audio extraction failed: {stderr}")
    return output_path


def _load_xclip_runtime(spec: HFModelSpec) -> dict[str, Any]:
    import importlib

    torch = importlib.import_module("torch")
    transformers = importlib.import_module("transformers")
    processor_class = getattr(transformers, "AutoProcessor", None)
    model_class = getattr(transformers, "AutoModel", None)
    if processor_class is None or model_class is None:
        raise HFAdapterError(
            "semantic",
            "missing model runtime capability: transformers AutoProcessor/AutoModel are required for X-CLIP",
        )

    runtime_options = spec.runtime_options or {}
    device = resolve_torch_device(torch, runtime_options.get("device"))
    processor = processor_class.from_pretrained(spec.model_id, revision=spec.revision)
    model = model_class.from_pretrained(spec.model_id, revision=spec.revision)
    if hasattr(model, "to"):
        model.to(device)
    if hasattr(model, "eval"):
        model.eval()
    return {
        "processor": processor,
        "model": model,
        "torch": torch,
        "device": device,
    }


def _load_siglip_runtime(spec: HFModelSpec) -> dict[str, Any]:
    import importlib

    torch = importlib.import_module("torch")
    transformers = importlib.import_module("transformers")
    processor_class = getattr(transformers, "AutoProcessor", None)
    model_class = getattr(transformers, "AutoModel", None)
    if processor_class is None or model_class is None:
        raise HFAdapterError(
            "keyframes",
            "missing model runtime capability: transformers AutoProcessor/AutoModel are required for SigLIP",
        )

    runtime_options = spec.runtime_options or {}
    device = resolve_torch_device(torch, runtime_options.get("device"))
    processor = processor_class.from_pretrained(spec.model_id, revision=spec.revision)
    model = model_class.from_pretrained(spec.model_id, revision=spec.revision)
    if hasattr(model, "to"):
        model.to(device)
    if hasattr(model, "eval"):
        model.eval()
    return {
        "processor": processor,
        "model": model,
        "torch": torch,
        "device": device,
    }


def _load_smolvlm_runtime(spec: HFModelSpec) -> dict[str, Any]:
    import importlib

    torch = importlib.import_module("torch")
    transformers = importlib.import_module("transformers")
    processor_class = getattr(transformers, "AutoProcessor", None)
    model_class = getattr(transformers, "AutoModelForImageTextToText", None)
    if model_class is None:
        model_class = getattr(transformers, "AutoModelForVision2Seq", None)
    if model_class is None:
        model_class = getattr(transformers, "SmolVLMForConditionalGeneration", None)
    if processor_class is None or model_class is None:
        raise HFAdapterError(
            "reranker",
            "missing model runtime capability: SmolVLM2 requires AutoProcessor and an image-text generation model class",
        )

    runtime_options = spec.runtime_options or {}
    device = resolve_torch_device(torch, runtime_options.get("device"))
    processor = processor_class.from_pretrained(spec.model_id, revision=spec.revision)
    model = model_class.from_pretrained(spec.model_id, revision=spec.revision)
    if hasattr(model, "to"):
        model.to(device)
    if hasattr(model, "eval"):
        model.eval()
    return {
        "processor": processor,
        "model": model,
        "torch": torch,
        "device": device,
    }


def _sample_segment_frames(
    source: Path,
    *,
    start_seconds: float,
    end_seconds: float,
    frame_count: int,
) -> list[Any]:
    import cv2

    cap = cv2.VideoCapture(str(source))
    if not cap.isOpened():
        raise HFAdapterError("semantic", f"backend execution failed: could not open video {source}")

    duration = max(0.0, end_seconds - start_seconds)
    timestamps = [start_seconds] if frame_count <= 1 else [
        start_seconds + duration * (index / max(1, frame_count - 1))
        for index in range(frame_count)
    ]

    frames: list[Any] = []
    try:
        for timestamp in timestamps:
            cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, timestamp) * 1000.0)
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frames.append(frame_rgb)
    finally:
        cap.release()

    if not frames:
        raise HFAdapterError("semantic", f"backend execution failed: could not sample frames from {source}")
    return frames


def _sample_segment_midpoint_frame(
    source: Path,
    *,
    start_seconds: float,
    end_seconds: float,
) -> Any:
    import cv2

    cap = cv2.VideoCapture(str(source))
    if not cap.isOpened():
        raise HFAdapterError("keyframes", f"backend execution failed: could not open video {source}")

    target_timestamp = max(0.0, (start_seconds + end_seconds) / 2.0)
    try:
        cap.set(cv2.CAP_PROP_POS_MSEC, target_timestamp * 1000.0)
        ok, frame = cap.read()
        if (not ok or frame is None) and end_seconds > start_seconds:
            cap.set(cv2.CAP_PROP_POS_MSEC, start_seconds * 1000.0)
            ok, frame = cap.read()
        if not ok or frame is None:
            raise HFAdapterError("keyframes", f"backend execution failed: could not sample keyframe from {source}")
        return cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    finally:
        cap.release()


def _score_xclip_queries(runtime: dict[str, Any], *, frames: list[Any], queries: list[str]) -> dict[str, float]:
    processor = runtime["processor"]
    model = runtime["model"]
    torch = runtime["torch"]
    device = runtime["device"]
    try:
        inputs = processor(text=list(queries), videos=[frames], return_tensors="pt", padding=True)
    except TypeError:
        inputs = processor(text=list(queries), videos=frames, return_tensors="pt", padding=True)

    prepared_inputs = {}
    for key, value in dict(inputs).items():
        if hasattr(value, "to"):
            prepared_inputs[key] = value.to(device)
        else:
            prepared_inputs[key] = value

    with torch.no_grad():
        outputs = model(**prepared_inputs)

    logits = getattr(outputs, "logits_per_video", None)
    if logits is None:
        logits = getattr(outputs, "logits", None)
    if logits is None:
        raise HFAdapterError(
            "semantic",
            "missing model runtime capability: X-CLIP output did not provide logits_per_video",
        )

    first_row = logits[0] if getattr(logits, "ndim", 1) > 1 else logits
    probabilities = torch.softmax(first_row, dim=-1)
    values = probabilities.detach().cpu().tolist()
    return {
        str(query): float(score)
        for query, score in zip(queries, values)
    }


def _embed_siglip_frame(runtime: dict[str, Any], *, frame: Any) -> Any:
    processor = runtime["processor"]
    model = runtime["model"]
    torch = runtime["torch"]
    device = runtime["device"]

    inputs = processor(images=frame, return_tensors="pt")
    prepared_inputs = {}
    for key, value in dict(inputs).items():
        if hasattr(value, "to"):
            prepared_inputs[key] = value.to(device)
        else:
            prepared_inputs[key] = value

    with torch.no_grad():
        if hasattr(model, "get_image_features"):
            features = model.get_image_features(**prepared_inputs)
        else:
            outputs = model(**prepared_inputs)
            features = getattr(outputs, "image_embeds", None)
            if features is None:
                raise HFAdapterError(
                    "keyframes",
                    "missing model runtime capability: SigLIP output did not provide image features",
                )
    return _vector_to_unit_array(features[0] if getattr(features, "ndim", 1) > 1 else features)


def _generate_smolvlm_rerank_response(
    runtime: dict[str, Any],
    *,
    frames: list[Any],
    candidate: dict[str, Any],
    spec: HFModelSpec,
) -> str:
    processor = runtime["processor"]
    model = runtime["model"]
    torch = runtime["torch"]
    device = runtime["device"]
    runtime_options = spec.runtime_options or {}

    prompt = _build_smolvlm_rerank_prompt(candidate)
    pil_images = _frames_to_pil_images(frames)
    messages = [
        {
            "role": "user",
            "content": [{"type": "image"} for _ in pil_images] + [{"type": "text", "text": prompt}],
        }
    ]

    if not hasattr(processor, "apply_chat_template"):
        raise HFAdapterError(
            "reranker",
            "missing model runtime capability: SmolVLM2 processor must provide apply_chat_template()",
        )

    rendered_prompt = processor.apply_chat_template(messages, add_generation_prompt=True, tokenize=False)
    inputs = processor(text=rendered_prompt, images=pil_images, return_tensors="pt", padding=True)
    prepared_inputs = {}
    for key, value in dict(inputs).items():
        if hasattr(value, "to"):
            prepared_inputs[key] = value.to(device)
        else:
            prepared_inputs[key] = value

    generation_kwargs = {
        "max_new_tokens": max(32, int(runtime_options.get("max_new_tokens", 96))),
        "do_sample": False,
    }
    temperature = float(runtime_options.get("temperature", 0.0))
    if temperature > 0:
        generation_kwargs["do_sample"] = True
        generation_kwargs["temperature"] = temperature

    with torch.no_grad():
        generated_ids = model.generate(**prepared_inputs, **generation_kwargs)

    input_ids = prepared_inputs.get("input_ids")
    trimmed_ids = generated_ids
    if input_ids is not None and hasattr(generated_ids, "__getitem__"):
        prompt_token_count = int(getattr(input_ids, "shape", [0, 0])[-1])
        trimmed_ids = generated_ids[:, prompt_token_count:] if getattr(generated_ids, "ndim", 1) > 1 else generated_ids[prompt_token_count:]

    decoded = processor.batch_decode(trimmed_ids, skip_special_tokens=True)
    if not decoded:
        return ""
    return str(decoded[0]).strip()


def _build_smolvlm_rerank_prompt(candidate: dict[str, Any]) -> str:
    context = {
        "window": {
            "start_seconds": round(float(candidate.get("start_seconds", 0.0)), 4),
            "end_seconds": round(float(candidate.get("end_seconds", 0.0)), 4),
        },
        "scores": {
            "base_score": round(float(candidate.get("base_score", 0.0)), 4),
            "proposal_score": round(float(candidate.get("proposal_score", 0.0)), 4),
            "transcript_score": round(float(candidate.get("transcript_score", 0.0)), 4),
            "semantic_score": round(float(candidate.get("semantic_score", 0.0)), 4),
            "novelty_score": round(float(candidate.get("novelty_score", 0.0)), 4),
        },
    }
    allowed_codes = ", ".join(ALLOWED_RERANK_REASON_CODES)
    return (
        "You are reranking a highlight candidate from a gameplay video. "
        "Use the images and structured context to judge whether this candidate is a strong highlight. "
        "Return JSON only with keys rerank_score, reason, and reason_codes. "
        "rerank_score must be a number from 0.0 to 1.0. "
        "reason must be one short sentence. "
        f"reason_codes must be a JSON array using only these codes: {allowed_codes}. "
        f"Candidate context: {json.dumps(context, sort_keys=True)}"
    )


def _frames_to_pil_images(frames: list[Any]) -> list[Any]:
    import importlib

    image_module = importlib.import_module("PIL.Image")
    image_class = getattr(image_module, "fromarray", None)
    if image_class is None:
        raise HFAdapterError("reranker", "missing model runtime capability: PIL.Image.fromarray was not found")
    return [image_class(frame) for frame in frames]


def _parse_smolvlm_rerank_response(response_text: str) -> dict[str, Any] | None:
    payload = _extract_json_object(response_text)
    if payload is None:
        return None
    score = payload.get("rerank_score")
    if not isinstance(score, (int, float)):
        return None
    rerank_score = float(score)
    if rerank_score < 0.0 or rerank_score > 1.0:
        return None
    reason = str(payload.get("reason", "")).strip()
    reason_codes = payload.get("reason_codes", [])
    if not isinstance(reason_codes, list):
        return None
    normalized_codes: list[str] = []
    for item in reason_codes:
        code = str(item).strip()
        if not code:
            continue
        if code not in ALLOWED_RERANK_REASON_CODES:
            return None
        if code not in normalized_codes:
            normalized_codes.append(code)
    return {
        "rerank_score": round(rerank_score, 4),
        "reason": reason,
        "reason_codes": normalized_codes,
    }


def _extract_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    start_index = text.find("{")
    end_index = text.rfind("}")
    if start_index < 0 or end_index <= start_index:
        return None
    try:
        payload = json.loads(text[start_index : end_index + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _coerce_timestamp_pair(value: Any) -> tuple[float, float]:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        start_value = 0.0 if value[0] is None else float(value[0])
        end_value = start_value if value[1] is None else float(value[1])
        return start_value, max(start_value, end_value)
    return 0.0, 0.0


def _vector_to_unit_array(value: Any) -> Any:
    if hasattr(value, "detach"):
        value = value.detach()
    if hasattr(value, "cpu"):
        value = value.cpu()
    if hasattr(value, "numpy"):
        value = value.numpy()
    if hasattr(value, "tolist"):
        value = value.tolist()
    vector = [float(item) for item in _flatten_numeric_sequence(value)]
    norm = math.sqrt(sum(item * item for item in vector))
    if norm <= 0:
        return vector
    return [item / norm for item in vector]


def _shot_detector_backend(spec: HFModelSpec) -> str:
    return str((spec.runtime_options or {}).get("proposal_backend", "transnetv2")).strip().lower() or "transnetv2"


def _asr_backend(spec: HFModelSpec) -> str:
    return str((spec.runtime_options or {}).get("asr_backend", "whisper")).strip().lower() or "whisper"


def _resolve_asr_model_id(spec: HFModelSpec, *, backend: str) -> str:
    configured_model_id = str(spec.model_id).strip()
    if backend == "distil_whisper" and configured_model_id == WHISPER_DEFAULT.model_id:
        return DISTIL_WHISPER_DEFAULT.model_id
    return configured_model_id


def _scene_timecode_to_seconds(value: Any) -> float:
    if hasattr(value, "get_seconds"):
        return float(value.get_seconds())
    if isinstance(value, (int, float)):
        return float(value)
    raise HFAdapterError("shot_detector", "missing model runtime capability: scene timecode could not be converted to seconds")


def _video_duration_seconds(source: Path) -> float:
    import cv2

    capture = cv2.VideoCapture(str(source))
    if not capture.isOpened():
        raise HFAdapterError("shot_detector", f"backend execution failed: could not open video {source}")
    try:
        frame_total = float(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0)
        fps = float(capture.get(cv2.CAP_PROP_FPS) or 0.0)
    finally:
        capture.release()
    if fps <= 0:
        return 0.0
    return max(0.0, frame_total / fps)


def _derive_siglip_novelty_and_clusters(embeddings: list[Any], spec: HFModelSpec) -> tuple[list[float], list[int]]:
    if not embeddings:
        return [], []
    if len(embeddings) == 1:
        return [1.0], [1]

    matrix = [_vector_to_unit_array(embedding) for embedding in embeddings]

    novelty_scores: list[float] = []
    for index, vector in enumerate(matrix):
        similarities = [
            _dot_product(vector, other)
            for other_index, other in enumerate(matrix)
            if other_index != index
        ]
        max_similarity = max(similarities) if similarities else 0.0
        novelty_scores.append(round(max(0.0, min(1.0, 1.0 - max(0.0, max_similarity))), 4))

    runtime_options = spec.runtime_options or {}
    cluster_threshold = float(runtime_options.get("cluster_similarity_threshold", 0.92))
    cluster_ids = _greedy_cluster_assignments(matrix, threshold=cluster_threshold)
    return novelty_scores, cluster_ids


def _greedy_cluster_assignments(embedding_matrix: Any, *, threshold: float) -> list[int]:
    cluster_centroids: list[list[float]] = []
    cluster_ids: list[int] = []
    for index, vector in enumerate(embedding_matrix):
        assigned_cluster = None
        best_similarity = -1.0
        for cluster_index, centroid in enumerate(cluster_centroids):
            similarity = _dot_product(vector, centroid)
            if similarity >= threshold and similarity > best_similarity:
                assigned_cluster = cluster_index
                best_similarity = similarity
        if assigned_cluster is None:
            cluster_centroids.append(list(vector))
            cluster_ids.append(len(cluster_centroids))
            continue
        cluster_ids.append(assigned_cluster + 1)
        member_indices = [member_index for member_index, cluster_id in enumerate(cluster_ids) if cluster_id == assigned_cluster + 1]
        centroid = _mean_vectors([embedding_matrix[member_index] for member_index in member_indices])
        cluster_centroids[assigned_cluster] = _vector_to_unit_array(centroid)
    return cluster_ids


def _flatten_numeric_sequence(value: Any) -> list[float]:
    if isinstance(value, (int, float)):
        return [float(value)]
    flattened: list[float] = []
    for item in list(value or []):
        if isinstance(item, (list, tuple)):
            flattened.extend(_flatten_numeric_sequence(item))
        else:
            flattened.append(float(item))
    return flattened


def _dot_product(left: list[float], right: list[float]) -> float:
    return float(sum(left_item * right_item for left_item, right_item in zip(left, right)))


def _mean_vectors(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        return []
    width = len(vectors[0])
    totals = [0.0] * width
    for vector in vectors:
        for index, value in enumerate(vector):
            totals[index] += float(value)
    return [value / len(vectors) for value in totals]


def _tensor_like_to_list(value: Any) -> list[float]:
    if hasattr(value, "detach"):
        value = value.detach()
    if hasattr(value, "cpu"):
        value = value.cpu()
    if hasattr(value, "numpy"):
        value = value.numpy()
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, (int, float)):
        return [float(value)]
    flattened: list[float] = []
    for item in list(value or []):
        if isinstance(item, (list, tuple)):
            flattened.append(float(item[0]))
        else:
            flattened.append(float(item))
    return flattened


def _video_fps(source: Path) -> float:
    import cv2

    cap = cv2.VideoCapture(str(source))
    if not cap.isOpened():
        raise HFAdapterError("shot_detector", f"backend execution failed: could not open video {source}")
    try:
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
    finally:
        cap.release()
    return fps if fps > 0 else 1.0


def _scene_rows_to_proposals(scenes: list[Any], *, fps: float) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    for row in scenes:
        if isinstance(row, dict):
            if "start_seconds" in row or "end_seconds" in row:
                start_seconds = float(row.get("start_seconds", 0.0))
                end_seconds = max(start_seconds, float(row.get("end_seconds", start_seconds)))
            else:
                start_frame = int(row.get("start_frame", row.get("start", 0)) or 0)
                end_frame = int(row.get("end_frame", row.get("end", start_frame)) or start_frame)
                start_seconds = start_frame / fps
                end_seconds = max(start_seconds, end_frame / fps)
            proposals.append(
                {
                    "start_seconds": start_seconds,
                    "end_seconds": end_seconds,
                    "proposal_score": float(row.get("proposal_score", row.get("score", 1.0))),
                }
            )
            continue
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            start_frame = int(row[0])
            end_frame = int(row[1])
            proposals.append(
                {
                    "start_seconds": start_frame / fps,
                    "end_seconds": max(start_frame / fps, end_frame / fps),
                    "proposal_score": 1.0,
                }
            )
    return proposals


def _prediction_rows_to_boundaries(predictions: list[float], *, fps: float, threshold: float) -> list[dict[str, Any]]:
    boundaries: list[dict[str, Any]] = []
    for index, score in enumerate(predictions):
        if float(score) < threshold:
            continue
        previous_score = float(predictions[index - 1]) if index > 0 else -math.inf
        next_score = float(predictions[index + 1]) if index + 1 < len(predictions) else -math.inf
        if float(score) < previous_score or float(score) < next_score:
            continue
        boundaries.append(
            {
                "timestamp_seconds": index / fps,
                "boundary_score": float(score),
            }
        )
    return boundaries


def _boundaries_to_proposals(boundaries: list[dict[str, Any]], *, fps: float, frame_total: int) -> list[dict[str, Any]]:
    frame_points = [0]
    for row in boundaries:
        frame_points.append(max(0, int(round(float(row["timestamp_seconds"]) * fps))))
    frame_points.append(max(frame_points[-1], frame_total))
    proposals: list[dict[str, Any]] = []
    for start_frame, end_frame in zip(frame_points, frame_points[1:]):
        if end_frame <= start_frame:
            continue
        proposals.append(
            {
                "start_seconds": start_frame / fps,
                "end_seconds": end_frame / fps,
                "proposal_score": 1.0,
            }
        )
    return proposals

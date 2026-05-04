from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.hf_adapters import (
    _parse_smolvlm_rerank_response,
    _run_siglip_backend,
    _run_smolvlm_backend,
    _run_transnetv2_backend,
    _run_whisper_backend,
    _run_xclip_backend,
)
from pipeline.hf_adapters import (
    HFAdapterError,
    SigLIPAdapter,
    SmolVLM2Adapter,
    TransNetV2Adapter,
    WhisperAdapter,
    XClipAdapter,
)
from pipeline.hf_highlight import reconstruct_hf_multimodal_outputs, scan_hf_multimodal_source


def _ffmpeg_path() -> str:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    if not Path(ffmpeg).exists():
        raise RuntimeError("ffmpeg not found for hf tests")
    return ffmpeg


def _write_pgm_frame(path: Path, width: int, height: int, pixels: bytes) -> None:
    header = f"P5\n{width} {height}\n255\n".encode("ascii")
    path.write_bytes(header + pixels)


def _write_test_video(path: Path, frame_pixels: list[bytes], width: int = 64, height: int = 36, fps: int = 4) -> None:
    with tempfile.TemporaryDirectory() as frames_dir:
        frame_dir = Path(frames_dir)
        for index, pixels in enumerate(frame_pixels):
            _write_pgm_frame(frame_dir / f"frame{index:03d}.pgm", width, height, pixels)

        command = [
            _ffmpeg_path(),
            "-v",
            "error",
            "-y",
            "-framerate",
            str(fps),
            "-i",
            str(frame_dir / "frame%03d.pgm"),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ]
        subprocess.run(command, check=True, capture_output=True)


def _solid_frame(value: int, width: int = 64, height: int = 36) -> bytes:
    return bytes([value]) * (width * height)


def _base_hf_config() -> dict:
    return {
        "shortlist_count": 2,
        "generic_queries": ["highlight moment", "clutch play"],
        "transcript_keywords": ["clutch", "insane", "wow"],
        "stage_weights": {
            "proposal": 0.35,
            "transcript": 0.20,
            "semantic": 0.25,
            "novelty": 0.20,
        },
        "signal_thresholds": {
            "proposal": 0.55,
            "transcript": 0.60,
            "semantic": 0.60,
            "novelty": 0.60,
            "rerank": 0.65,
        },
        "components": {
            "shot_detector": {"enabled": True},
            "asr": {"enabled": True},
            "semantic": {"enabled": True},
            "keyframes": {"enabled": True},
            "reranker": {"enabled": True},
        },
    }


class HFAdapterTests(unittest.TestCase):
    def test_transnet_adapter_normalizes_boundaries_and_proposals(self) -> None:
        with patch(
            "pipeline.hf_adapters._run_transnetv2_backend",
            return_value={
                "boundaries": [{"timestamp_seconds": 4.1254, "boundary_score": 0.91234}],
                "proposals": [{"start_seconds": 1.0, "end_seconds": 5.0, "proposal_score": 0.81}],
            },
        ):
            result = TransNetV2Adapter({}).detect_shots(Path("/tmp/example.mp4"))

        self.assertEqual(result["stage"], "shot_detector")
        self.assertEqual(result["boundaries"][0]["timestamp_seconds"], 4.1254)
        self.assertEqual(result["boundaries"][0]["boundary_score"], 0.9123)
        self.assertEqual(result["proposals"][0]["proposal_score"], 0.81)

    def test_whisper_adapter_normalizes_transcript_segments(self) -> None:
        with patch(
            "pipeline.hf_adapters._run_whisper_backend",
            return_value={
                "transcript": "clutch wow",
                "segments": [{"start_seconds": 0.0, "end_seconds": 3.0, "text": "clutch wow"}],
            },
        ):
            result = WhisperAdapter({}).transcribe(Path("/tmp/example.mp4"))

        self.assertEqual(result["stage"], "asr")
        self.assertEqual(result["transcript"], "clutch wow")
        self.assertEqual(result["segments"][0]["text"], "clutch wow")

    def test_xclip_adapter_normalizes_query_scores(self) -> None:
        with patch(
            "pipeline.hf_adapters._run_xclip_backend",
            return_value={
                "segment_scores": [
                    {
                        "start_seconds": 0.0,
                        "end_seconds": 5.0,
                        "query_scores": {"highlight moment": 0.72},
                        "top_query": "highlight moment",
                        "semantic_score": 0.72,
                    }
                ]
            },
        ):
            result = XClipAdapter({}).score_segments(Path("/tmp/example.mp4"), [{"start_seconds": 0.0, "end_seconds": 5.0}], ["highlight moment"])

        self.assertEqual(result["stage"], "semantic")
        self.assertEqual(result["segment_scores"][0]["query_scores"]["highlight moment"], 0.72)

    def test_siglip_adapter_normalizes_keyframe_segments(self) -> None:
        with patch(
            "pipeline.hf_adapters._run_siglip_backend",
            return_value={
                "segments": [
                    {
                        "start_seconds": 0.0,
                        "end_seconds": 5.0,
                        "keyframe_timestamp_seconds": 2.5,
                        "novelty_score": 0.73,
                        "cluster_id": 2,
                    }
                ]
            },
        ):
            result = SigLIPAdapter({}).embed_segments(Path("/tmp/example.mp4"), [{"start_seconds": 0.0, "end_seconds": 5.0}])

        self.assertEqual(result["stage"], "keyframes")
        self.assertEqual(result["segments"][0]["novelty_score"], 0.73)

    def test_smolvlm_adapter_normalizes_rerank_candidates(self) -> None:
        with patch(
            "pipeline.hf_adapters._run_smolvlm_backend",
            return_value={
                "candidates": [
                    {
                        "start_seconds": 0.0,
                        "end_seconds": 5.0,
                        "base_score": 0.7,
                        "rerank_score": 0.88,
                        "reason": "crowd reaction and clutch finish",
                        "reason_codes": ["crowd_reaction", "clutch_finish"],
                    }
                ]
            },
        ):
            result = SmolVLM2Adapter({}).rerank_candidates(Path("/tmp/example.mp4"), [{"start_seconds": 0.0, "end_seconds": 5.0}])

        self.assertEqual(result["stage"], "reranker")
        self.assertEqual(result["candidates"][0]["rerank_score"], 0.88)
        self.assertEqual(result["candidates"][0]["reason_codes"], ["crowd_reaction", "clutch_finish"])

    def test_whisper_backend_reports_missing_optional_dependencies(self) -> None:
        with patch(
            "pipeline.hf_runtime.importlib.import_module",
            side_effect=ImportError("transformers missing"),
        ):
            with self.assertRaises(HFAdapterError) as exc:
                _run_whisper_backend(Path("/tmp/example.mp4"), WhisperAdapter({}).spec)

        self.assertEqual(exc.exception.stage, "asr")
        self.assertIn("missing dependency or runtime capability", exc.exception.message)
        self.assertIn("transformers", exc.exception.message)

    def test_transnet_backend_can_route_to_pyscenedetect(self) -> None:
        spec = TransNetV2Adapter({"runtime_options": {"proposal_backend": "pyscenedetect"}}).spec
        with patch(
            "pipeline.hf_adapters._run_pyscenedetect_backend",
            return_value={"boundaries": [], "proposals": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.8}]},
        ):
            result = _run_transnetv2_backend(Path("/tmp/example.mp4"), spec)

        self.assertEqual(result["proposals"][0]["proposal_score"], 0.8)

    def test_whisper_backend_can_use_distil_backend_selector(self) -> None:
        spec = WhisperAdapter({"runtime_options": {"asr_backend": "distil_whisper"}}).spec
        fake_audio_path = Path("/tmp/fake.wav")
        with (
            patch("pipeline.hf_adapters.check_hf_runtime", return_value={"ok": True, "status": "ok", "checks": {}}),
            patch("pipeline.hf_adapters._load_whisper_pipeline", return_value=lambda *args, **kwargs: {"text": "clutch", "chunks": []}) as load_pipeline,
            patch("pipeline.hf_adapters._extract_audio_for_whisper", return_value=fake_audio_path),
            patch("pathlib.Path.exists", return_value=False),
        ):
            result = _run_whisper_backend(Path("/tmp/example.mp4"), spec)

        self.assertEqual(result["transcript"], "clutch")
        self.assertEqual(load_pipeline.call_args.kwargs["backend"], "distil_whisper")

    def test_transnet_backend_converts_detect_scenes_results(self) -> None:
        fake_model = object()
        with (
            patch(
                "pipeline.hf_adapters.check_hf_runtime",
                return_value={"ok": True, "status": "ok", "checks": {}},
            ),
            patch("pipeline.hf_adapters._load_transnetv2_model", return_value=fake_model),
            patch("pipeline.hf_adapters._video_fps", return_value=4.0),
            patch(
                "pipeline.hf_adapters._run_transnetv2_detection",
                return_value=(
                    [0.1, 0.9, 0.2, 0.85],
                    [(0, 2), (2, 4)],
                ),
            ),
        ):
            result = _run_transnetv2_backend(Path("/tmp/example.mp4"), TransNetV2Adapter({}).spec)

        self.assertEqual(result["proposals"][0]["start_seconds"], 0.0)
        self.assertEqual(result["proposals"][0]["end_seconds"], 0.5)
        self.assertEqual(result["boundaries"][0]["timestamp_seconds"], 0.25)

    def test_whisper_backend_uses_pipeline_output_chunks(self) -> None:
        fake_pipeline = lambda *_args, **_kwargs: {
            "text": "clutch wow",
            "chunks": [{"timestamp": (0.25, 1.5), "text": "clutch wow"}],
        }
        with (
            patch(
                "pipeline.hf_adapters.check_hf_runtime",
                return_value={"ok": True, "status": "ok", "checks": {}},
            ),
            patch("pipeline.hf_adapters._load_whisper_pipeline", return_value=fake_pipeline),
            patch("pipeline.hf_adapters._extract_audio_for_whisper", return_value=Path("/tmp/example.wav")),
            patch("pathlib.Path.exists", return_value=True),
            patch("pathlib.Path.unlink"),
        ):
            result = _run_whisper_backend(Path("/tmp/example.mp4"), WhisperAdapter({}).spec)

        self.assertEqual(result["transcript"], "clutch wow")
        self.assertEqual(result["segments"][0]["start_seconds"], 0.25)
        self.assertEqual(result["segments"][0]["end_seconds"], 1.5)

    def test_xclip_backend_scores_only_requested_segments(self) -> None:
        sampled_windows: list[tuple[float, float]] = []

        def _recording_sampler(_source: Path, *, start_seconds: float, end_seconds: float, frame_count: int) -> list[str]:
            sampled_windows.append((start_seconds, end_seconds))
            self.assertEqual(frame_count, 8)
            return ["frame"]

        with (
            patch(
                "pipeline.hf_adapters.check_hf_runtime",
                return_value={"ok": True, "status": "ok", "checks": {}},
            ),
            patch(
                "pipeline.hf_adapters._load_xclip_runtime",
                return_value={"processor": object(), "model": object(), "torch": object(), "device": "cpu"},
            ),
            patch("pipeline.hf_adapters._sample_segment_frames", side_effect=_recording_sampler),
            patch(
                "pipeline.hf_adapters._score_xclip_queries",
                side_effect=[
                    {"highlight moment": 0.7, "clutch play": 0.3},
                    {"highlight moment": 0.2, "clutch play": 0.8},
                ],
            ),
        ):
            result = _run_xclip_backend(
                Path("/tmp/example.mp4"),
                [
                    {"start_seconds": 0.0, "end_seconds": 4.0},
                    {"start_seconds": 4.0, "end_seconds": 8.0},
                ],
                ["highlight moment", "clutch play"],
                XClipAdapter({}).spec,
            )

        self.assertEqual(sampled_windows, [(0.0, 4.0), (4.0, 8.0)])
        self.assertEqual(result["segment_scores"][0]["top_query"], "highlight moment")
        self.assertEqual(result["segment_scores"][1]["top_query"], "clutch play")

    def test_siglip_backend_reports_missing_optional_dependencies(self) -> None:
        with patch(
            "pipeline.hf_runtime.importlib.import_module",
            side_effect=ImportError("transformers missing"),
        ):
            with self.assertRaises(HFAdapterError) as exc:
                _run_siglip_backend(Path("/tmp/example.mp4"), [{"start_seconds": 0.0, "end_seconds": 5.0}], SigLIPAdapter({}).spec)

        self.assertEqual(exc.exception.stage, "keyframes")
        self.assertIn("missing dependency or runtime capability", exc.exception.message)
        self.assertIn("transformers", exc.exception.message)

    def test_siglip_backend_derives_novelty_and_clusters(self) -> None:
        sampled_windows: list[tuple[float, float]] = []
        fake_embeddings = [
            [1.0, 0.0, 0.0],
            [0.99, 0.01, 0.0],
            [0.0, 1.0, 0.0],
        ]

        def _recording_sampler(_source: Path, *, start_seconds: float, end_seconds: float) -> str:
            sampled_windows.append((start_seconds, end_seconds))
            return "frame"

        with (
            patch(
                "pipeline.hf_adapters.check_hf_runtime",
                return_value={"ok": True, "status": "ok", "checks": {}},
            ),
            patch(
                "pipeline.hf_adapters._load_siglip_runtime",
                return_value={"processor": object(), "model": object(), "torch": object(), "device": "cpu"},
            ),
            patch("pipeline.hf_adapters._sample_segment_midpoint_frame", side_effect=_recording_sampler),
            patch("pipeline.hf_adapters._embed_siglip_frame", side_effect=fake_embeddings),
        ):
            result = _run_siglip_backend(
                Path("/tmp/example.mp4"),
                [
                    {"start_seconds": 0.0, "end_seconds": 2.0},
                    {"start_seconds": 2.0, "end_seconds": 4.0},
                    {"start_seconds": 4.0, "end_seconds": 6.0},
                ],
                SigLIPAdapter({}).spec,
            )

        self.assertEqual(sampled_windows, [(0.0, 2.0), (2.0, 4.0), (4.0, 6.0)])
        self.assertEqual(result["segments"][0]["cluster_id"], 1)
        self.assertEqual(result["segments"][1]["cluster_id"], 1)
        self.assertEqual(result["segments"][2]["cluster_id"], 2)
        self.assertLess(result["segments"][0]["novelty_score"], result["segments"][2]["novelty_score"])

    def test_siglip_backend_single_segment_is_deterministic(self) -> None:
        with (
            patch(
                "pipeline.hf_adapters.check_hf_runtime",
                return_value={"ok": True, "status": "ok", "checks": {}},
            ),
            patch(
                "pipeline.hf_adapters._load_siglip_runtime",
                return_value={"processor": object(), "model": object(), "torch": object(), "device": "cpu"},
            ),
            patch("pipeline.hf_adapters._sample_segment_midpoint_frame", return_value="frame"),
            patch("pipeline.hf_adapters._embed_siglip_frame", return_value=[1.0, 0.0, 0.0]),
        ):
            result = _run_siglip_backend(
                Path("/tmp/example.mp4"),
                [{"start_seconds": 1.0, "end_seconds": 3.0}],
                SigLIPAdapter({}).spec,
            )

        self.assertEqual(result["segments"][0]["novelty_score"], 1.0)
        self.assertEqual(result["segments"][0]["cluster_id"], 1)

    def test_smolvlm_backend_reports_missing_optional_dependencies(self) -> None:
        with patch(
            "pipeline.hf_runtime.importlib.import_module",
            side_effect=ImportError("transformers missing"),
        ):
            with self.assertRaises(HFAdapterError) as exc:
                _run_smolvlm_backend(
                    Path("/tmp/example.mp4"),
                    [{"start_seconds": 0.0, "end_seconds": 5.0, "base_score": 0.7}],
                    SmolVLM2Adapter({}).spec,
                )

        self.assertEqual(exc.exception.stage, "reranker")
        self.assertIn("missing dependency or runtime capability", exc.exception.message)
        self.assertIn("transformers", exc.exception.message)

    def test_smolvlm_backend_reranks_candidates_with_controlled_codes(self) -> None:
        sampled_windows: list[tuple[float, float]] = []

        def _recording_sampler(_source: Path, *, start_seconds: float, end_seconds: float, frame_count: int) -> list[str]:
            sampled_windows.append((start_seconds, end_seconds))
            self.assertEqual(frame_count, 3)
            return ["frame-1", "frame-2", "frame-3"]

        with (
            patch(
                "pipeline.hf_adapters.check_hf_runtime",
                return_value={"ok": True, "status": "ok", "checks": {}},
            ),
            patch(
                "pipeline.hf_adapters._load_smolvlm_runtime",
                return_value={"processor": object(), "model": object(), "torch": object(), "device": "cpu"},
            ),
            patch("pipeline.hf_adapters._sample_segment_frames", side_effect=_recording_sampler),
            patch(
                "pipeline.hf_adapters._generate_smolvlm_rerank_response",
                side_effect=[
                    '{"rerank_score": 0.91, "reason": "Strong action swing.", "reason_codes": ["high_action", "clutch_moment"]}',
                    '{"rerank_score": 0.42, "reason": "Weak scene.", "reason_codes": ["low_visual_signal"]}',
                ],
            ),
        ):
            result = _run_smolvlm_backend(
                Path("/tmp/example.mp4"),
                [
                    {
                        "start_seconds": 0.0,
                        "end_seconds": 4.0,
                        "base_score": 0.6,
                        "proposal_score": 0.7,
                        "transcript_score": 0.5,
                        "semantic_score": 0.8,
                        "novelty_score": 0.4,
                    },
                    {
                        "start_seconds": 4.0,
                        "end_seconds": 8.0,
                        "base_score": 0.5,
                        "proposal_score": 0.5,
                        "transcript_score": 0.1,
                        "semantic_score": 0.3,
                        "novelty_score": 0.2,
                    },
                ],
                SmolVLM2Adapter({}).spec,
            )

        self.assertEqual(sampled_windows, [(0.0, 4.0), (4.0, 8.0)])
        self.assertEqual(result["candidates"][0]["reason_codes"], ["high_action", "clutch_moment"])
        self.assertEqual(result["candidates"][0]["rerank_score"], 0.91)
        self.assertEqual(result["candidates"][1]["reason_codes"], ["low_visual_signal"])

    def test_parse_smolvlm_rerank_response_rejects_unknown_code(self) -> None:
        parsed = _parse_smolvlm_rerank_response(
            '{"rerank_score": 0.8, "reason": "Looks good.", "reason_codes": ["unknown_code"]}'
        )
        self.assertIsNone(parsed)

    def test_parse_smolvlm_rerank_response_rejects_invalid_score(self) -> None:
        parsed = _parse_smolvlm_rerank_response(
            '{"rerank_score": "high", "reason": "Looks good.", "reason_codes": ["high_action"]}'
        )
        self.assertIsNone(parsed)

    def test_parse_smolvlm_rerank_response_rejects_malformed_payload(self) -> None:
        parsed = _parse_smolvlm_rerank_response("not json")
        self.assertIsNone(parsed)

    def test_smolvlm_backend_skips_candidates_with_invalid_model_output(self) -> None:
        with (
            patch(
                "pipeline.hf_adapters.check_hf_runtime",
                return_value={"ok": True, "status": "ok", "checks": {}},
            ),
            patch(
                "pipeline.hf_adapters._load_smolvlm_runtime",
                return_value={"processor": object(), "model": object(), "torch": object(), "device": "cpu"},
            ),
            patch("pipeline.hf_adapters._sample_segment_frames", return_value=["frame-1", "frame-2", "frame-3"]),
            patch(
                "pipeline.hf_adapters._generate_smolvlm_rerank_response",
                return_value='{"rerank_score": 0.8, "reason": "Looks good.", "reason_codes": ["unknown_code"]}',
            ),
        ):
            result = _run_smolvlm_backend(
                Path("/tmp/example.mp4"),
                [{"start_seconds": 0.0, "end_seconds": 4.0, "base_score": 0.6}],
                SmolVLM2Adapter({}).spec,
            )

        self.assertEqual(result["candidates"], [])


class HFHighlightPipelineTests(unittest.TestCase):
    def test_scan_hf_multimodal_source_emits_structured_outputs_and_signals(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            video_path = Path(handle.name)
        self.addCleanup(video_path.unlink)
        _write_test_video(video_path, [_solid_frame(80) for _ in range(16)])

        with (
            patch(
                "pipeline.hf_adapters._run_transnetv2_backend",
                return_value={
                    "boundaries": [{"timestamp_seconds": 2.0, "boundary_score": 0.9}],
                    "proposals": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.9}],
                },
            ),
            patch(
                "pipeline.hf_adapters._run_whisper_backend",
                return_value={
                    "transcript": "insane clutch wow",
                    "segments": [{"start_seconds": 0.0, "end_seconds": 5.0, "text": "insane clutch wow"}],
                },
            ),
            patch(
                "pipeline.hf_adapters._run_xclip_backend",
                return_value={
                    "segment_scores": [
                        {
                            "start_seconds": 0.0,
                            "end_seconds": 5.0,
                            "query_scores": {"highlight moment": 0.78},
                            "top_query": "highlight moment",
                            "semantic_score": 0.78,
                        }
                    ]
                },
            ),
            patch(
                "pipeline.hf_adapters._run_siglip_backend",
                return_value={
                    "segments": [
                        {
                            "start_seconds": 0.0,
                            "end_seconds": 5.0,
                            "keyframe_timestamp_seconds": 2.5,
                            "novelty_score": 0.74,
                            "cluster_id": 1,
                        }
                    ]
                },
            ),
            patch(
                "pipeline.hf_adapters._run_smolvlm_backend",
                return_value={
                    "candidates": [
                        {
                            "start_seconds": 0.0,
                            "end_seconds": 5.0,
                            "base_score": 0.6,
                            "rerank_score": 0.91,
                            "reason": "clear clutch swing",
                            "reason_codes": ["clutch_swing"],
                        }
                    ]
                },
            ),
        ):
            result = scan_hf_multimodal_source(video_path, _base_hf_config(), media_duration_seconds=8.0)

        self.assertTrue(result.signals)
        self.assertIn("structured_outputs", result.metadata)
        self.assertEqual(result.metadata["stage_statuses"]["reranker"], "ok")
        self.assertIn("duration_ms", result.metadata["stages"]["shot_detector"])
        self.assertIn("output_counts", result.metadata["stages"]["reranker"])
        outputs = result.metadata["structured_outputs"]
        self.assertEqual(outputs["segment_proposals"][0]["proposal_score"], 0.9)
        self.assertEqual(outputs["transcript_features"][0]["keyword_hits"], ["clutch", "insane", "wow"])
        self.assertEqual(outputs["semantic_scores"][0]["semantic_score"], 0.78)
        self.assertEqual(outputs["keyframe_features"][0]["novelty_score"], 0.74)
        self.assertEqual(outputs["reranked_candidates"][0]["rerank_score"], 0.91)
        self.assertIn("hf_rerank_highlight", [signal.source for signal in result.signals])

    def test_scan_hf_multimodal_source_degrades_when_stage_fails(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            video_path = Path(handle.name)
        self.addCleanup(video_path.unlink)
        _write_test_video(video_path, [_solid_frame(80) for _ in range(16)])

        with (
            patch(
                "pipeline.hf_adapters._run_transnetv2_backend",
                return_value={"boundaries": [], "proposals": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.8}]},
            ),
            patch(
                "pipeline.hf_adapters._run_whisper_backend",
                side_effect=HFAdapterError("asr", "whisper unavailable"),
            ),
            patch(
                "pipeline.hf_adapters._run_xclip_backend",
                return_value={"segment_scores": []},
            ),
            patch(
                "pipeline.hf_adapters._run_siglip_backend",
                return_value={"segments": []},
            ),
            patch(
                "pipeline.hf_adapters._run_smolvlm_backend",
                return_value={"candidates": []},
            ),
        ):
            result = scan_hf_multimodal_source(video_path, _base_hf_config(), media_duration_seconds=8.0)

        self.assertEqual(result.metadata["stage_statuses"]["asr"], "failed")
        self.assertEqual(result.metadata["stages"]["asr"]["reason"], "whisper unavailable")
        self.assertIn("duration_ms", result.metadata["stages"]["asr"])
        self.assertTrue(any(signal.source == "hf_shot_boundary" for signal in result.signals))

    def test_scan_hf_multimodal_source_emits_keyframe_novelty_signal_when_threshold_clears(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            video_path = Path(handle.name)
        self.addCleanup(video_path.unlink)
        _write_test_video(video_path, [_solid_frame(80) for _ in range(16)])

        with (
            patch(
                "pipeline.hf_adapters._run_transnetv2_backend",
                return_value={
                    "boundaries": [],
                    "proposals": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.4}],
                },
            ),
            patch("pipeline.hf_adapters._run_whisper_backend", return_value={"transcript": "", "segments": []}),
            patch("pipeline.hf_adapters._run_xclip_backend", return_value={"segment_scores": []}),
            patch(
                "pipeline.hf_adapters._run_siglip_backend",
                return_value={
                    "segments": [
                        {
                            "start_seconds": 0.0,
                            "end_seconds": 5.0,
                            "keyframe_timestamp_seconds": 2.5,
                            "novelty_score": 0.91,
                            "cluster_id": 1,
                        }
                    ]
                },
            ),
            patch("pipeline.hf_adapters._run_smolvlm_backend", return_value={"candidates": []}),
        ):
            result = scan_hf_multimodal_source(video_path, _base_hf_config(), media_duration_seconds=8.0)

        self.assertEqual(result.metadata["stage_statuses"]["keyframes"], "ok")
        self.assertEqual(result.metadata["structured_outputs"]["keyframe_features"][0]["cluster_id"], 1)

    def test_reconstruct_hf_multimodal_outputs_rebuilds_shortlist_without_inference(self) -> None:
        result = reconstruct_hf_multimodal_outputs(
            {
                "segment_proposals": [
                    {"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.9},
                    {"start_seconds": 5.0, "end_seconds": 10.0, "proposal_score": 0.5},
                ],
                "transcript_features": [
                    {"start_seconds": 0.0, "end_seconds": 5.0, "text": "clutch", "keyword_hits": ["clutch"], "salience_score": 0.7},
                    {"start_seconds": 5.0, "end_seconds": 10.0, "text": "", "keyword_hits": [], "salience_score": 0.0},
                ],
                "semantic_scores": [
                    {"start_seconds": 0.0, "end_seconds": 5.0, "semantic_score": 0.82, "top_query": "highlight moment", "query_scores": {"highlight moment": 0.82}},
                    {"start_seconds": 5.0, "end_seconds": 10.0, "semantic_score": 0.3, "top_query": "highlight moment", "query_scores": {"highlight moment": 0.3}},
                ],
                "keyframe_features": [
                    {"start_seconds": 0.0, "end_seconds": 5.0, "keyframe_timestamp_seconds": 2.5, "novelty_score": 0.72, "cluster_id": 1},
                    {"start_seconds": 5.0, "end_seconds": 10.0, "keyframe_timestamp_seconds": 7.5, "novelty_score": 0.2, "cluster_id": 1},
                ],
                "reranked_candidates": [
                    {"start_seconds": 0.0, "end_seconds": 5.0, "base_score": 0.79, "rerank_score": 0.91, "reason": "clear clutch swing", "reason_codes": ["clutch_moment"]},
                ],
            },
            {
                "duration_seconds": 10.0,
                "shortlist_count": 1,
                "stage_weights": {"proposal": 0.35, "transcript": 0.2, "semantic": 0.25, "novelty": 0.2},
                "signal_thresholds": {"proposal": 0.55, "transcript": 0.6, "semantic": 0.6, "novelty": 0.6, "rerank": 0.65},
            },
        )

        self.assertEqual(len(result["shortlisted_candidates"]), 1)
        self.assertEqual(result["shortlisted_candidates"][0]["start_seconds"], 0.0)
        self.assertEqual(result["reranked_candidates"][0]["rerank_score"], 0.91)
        self.assertTrue(any(signal.source == "hf_rerank_highlight" for signal in result["signals"]))
        self.assertIn("hf_keyframe_novelty", [signal.source for signal in result["signals"]])

    def test_scan_hf_multimodal_source_reranks_only_shortlisted_candidates(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            video_path = Path(handle.name)
        self.addCleanup(video_path.unlink)
        _write_test_video(video_path, [_solid_frame(80) for _ in range(16)])

        rerank_inputs: list[list[dict[str, float]]] = []

        def _capture_rerank(_source: Path, candidates: list[dict[str, float]], _spec: object) -> dict[str, object]:
            rerank_inputs.append(candidates)
            return {
                "candidates": [
                    {
                        "start_seconds": candidates[0]["start_seconds"],
                        "end_seconds": candidates[0]["end_seconds"],
                        "base_score": candidates[0]["base_score"],
                        "rerank_score": 0.92,
                        "reason": "Strong action.",
                        "reason_codes": ["high_action"],
                    }
                ]
            }

        config = _base_hf_config()
        config["shortlist_count"] = 2
        with (
            patch(
                "pipeline.hf_adapters._run_transnetv2_backend",
                return_value={
                    "boundaries": [],
                    "proposals": [
                        {"start_seconds": 0.0, "end_seconds": 2.0, "proposal_score": 0.9},
                        {"start_seconds": 2.0, "end_seconds": 4.0, "proposal_score": 0.7},
                        {"start_seconds": 4.0, "end_seconds": 6.0, "proposal_score": 0.4},
                    ],
                },
            ),
            patch("pipeline.hf_adapters._run_whisper_backend", return_value={"transcript": "", "segments": []}),
            patch(
                "pipeline.hf_adapters._run_xclip_backend",
                return_value={
                    "segment_scores": [
                        {
                            "start_seconds": 0.0,
                            "end_seconds": 2.0,
                            "query_scores": {"highlight moment": 0.8},
                            "top_query": "highlight moment",
                            "semantic_score": 0.8,
                        },
                        {
                            "start_seconds": 2.0,
                            "end_seconds": 4.0,
                            "query_scores": {"highlight moment": 0.6},
                            "top_query": "highlight moment",
                            "semantic_score": 0.6,
                        },
                        {
                            "start_seconds": 4.0,
                            "end_seconds": 6.0,
                            "query_scores": {"highlight moment": 0.2},
                            "top_query": "highlight moment",
                            "semantic_score": 0.2,
                        },
                    ]
                },
            ),
            patch(
                "pipeline.hf_adapters._run_siglip_backend",
                return_value={
                    "segments": [
                        {"start_seconds": 0.0, "end_seconds": 2.0, "keyframe_timestamp_seconds": 1.0, "novelty_score": 0.7, "cluster_id": 1},
                        {"start_seconds": 2.0, "end_seconds": 4.0, "keyframe_timestamp_seconds": 3.0, "novelty_score": 0.5, "cluster_id": 1},
                        {"start_seconds": 4.0, "end_seconds": 6.0, "keyframe_timestamp_seconds": 5.0, "novelty_score": 0.2, "cluster_id": 2},
                    ]
                },
            ),
            patch("pipeline.hf_adapters._run_smolvlm_backend", side_effect=_capture_rerank),
        ):
            result = scan_hf_multimodal_source(video_path, config, media_duration_seconds=8.0)

        self.assertEqual(len(rerank_inputs), 1)
        self.assertEqual(len(rerank_inputs[0]), 2)
        self.assertEqual(result.metadata["stage_statuses"]["reranker"], "ok")
        self.assertEqual(result.metadata["structured_outputs"]["reranked_candidates"][0]["rerank_score"], 0.92)
        self.assertIn("hf_rerank_highlight", [signal.source for signal in result.signals])

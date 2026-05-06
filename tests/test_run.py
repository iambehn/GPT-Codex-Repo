from __future__ import annotations

import copy
import io
import json
import math
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
import wave
from pathlib import Path
from contextlib import redirect_stdout
from unittest.mock import patch

import pipeline.proxy_review_bridge as proxy_review_bridge
from pipeline.hf_adapters import HFAdapterError
from run import (
    DEFAULT_CONFIG,
    REPO_ROOT,
    _proxy_scan_batch_report_path,
    _sidecar_path,
    run_apply_derived_row_review,
    main as run_main,
    run_adapt_game_schema,
    run_build_onboarding_draft,
    run_derive_game_detection_manifest,
    run_fill_derived_detection_rows,
    run_ingest_game_sources,
    run_audit_pipeline_contracts,
    run_apply_proxy_review,
    run_apply_onboarding_identity_review,
    run_cleanup_proxy_review,
    run_cleanup_onboarding_identity_review,
    run_create_workflow_run,
    run_onboard_game,
    run_publish_onboarding_batch,
    run_prepare_derived_row_review,
    run_query_workflow_queue,
    run_report_onboarding_batch,
    run_report_unresolved_derived_rows,
    run_publish_onboarding_draft,
    run_prepare_onboarding_identity_review,
    run_summarize_derived_row_review,
    run_validate_onboarding_publish,
    run_query_clip_registry,
    run_refresh_clip_registry,
    run_prepare_proxy_review,
    run_scan_chat_log,
    run_scan_vod,
    run_scan_vod_batch,
)


def _write_test_wave(path: Path, amplitudes: list[float], sample_rate: int = 16000, duration_seconds: float = 0.25) -> None:
    frame_total = int(sample_rate * duration_seconds)
    with wave.open(str(path), "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(sample_rate)

        for amplitude in amplitudes:
            frames = bytearray()
            for frame_index in range(frame_total):
                sample = int(32767 * amplitude * math.sin(2 * math.pi * 440 * frame_index / sample_rate))
                frames.extend(struct.pack("<h", sample))
            handle.writeframes(bytes(frames))


def _ffmpeg_path() -> str:
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    if not Path(ffmpeg).exists():
        raise RuntimeError("ffmpeg not found for run integration tests")
    return ffmpeg


def _write_pgm_frame(path: Path, width: int, height: int, pixels: bytes) -> None:
    header = f"P5\n{width} {height}\n255\n".encode("ascii")
    path.write_bytes(header + pixels)


def _solid_frame(value: int, width: int = 64, height: int = 36) -> bytes:
    return bytes([value]) * (width * height)


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


def _mux_video_and_audio(video_path: Path, audio_path: Path, output_path: Path) -> None:
    command = [
        _ffmpeg_path(),
        "-v",
        "error",
        "-y",
        "-i",
        str(video_path),
        "-i",
        str(audio_path),
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-shortest",
        str(output_path),
    ]
    subprocess.run(command, check=True, capture_output=True)


def _config_with_output_dir(output_dir: str | Path) -> dict:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["proxy_scanner"]["sidecar"]["output_dir"] = str(output_dir)
    return config


def _legacy_signals_config(output_dir: str | Path) -> dict:
    config = _config_with_output_dir(output_dir)
    config["proxy_scanner"]["signals"] = copy.deepcopy(config["proxy_scanner"]["sources"])
    del config["proxy_scanner"]["sources"]
    return config


def _write_proxy_sidecar(
    path: Path,
    *,
    game: str,
    source: Path,
    score: float,
    action: str,
    sources: list[str],
    source_families: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "proxy_scan_v1",
        "scan_id": f"{game}-{path.stem}",
        "ok": True,
        "game": game,
        "source": str(source.resolve()),
        "source_results": {
            "audio_prepass": {"status": "ok", "signal_count": int("audio_prepass" in source_families)},
            "visual_prepass": {"status": "ok", "signal_count": int("visual_prepass" in source_families)},
            "playlist_hls": {"status": "skipped", "signal_count": 0},
            "chat_velocity": {"status": "skipped", "signal_count": 0},
        },
        "config": {"proxy_scanner": {}},
        "signal_count": len(sources),
        "window_count": 1,
        "signals": [],
        "windows": [
            {
                "start_seconds": 0.0,
                "end_seconds": 10.0,
                "proxy_score": score,
                "signal_count": len(sources),
                "sources": list(sources),
                "source_families": list(source_families),
                "recommended_action": action,
                "signals": [],
                "explanation": "",
            }
        ],
        "sidecar_path": str(path.resolve()),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_gpt_review_repo(root: Path) -> None:
    (root / "processing").mkdir(parents=True, exist_ok=True)
    (root / "inbox").mkdir(parents=True, exist_ok=True)
    (root / "accepted").mkdir(parents=True, exist_ok=True)
    (root / "rejected").mkdir(parents=True, exist_ok=True)
    (root / "config.yaml").write_text(
        "\n".join(
            [
                "paths:",
                '  inbox: "inbox"',
                '  processing: "processing"',
                '  accepted: "accepted"',
                '  rejected: "rejected"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )


class RunTests(unittest.TestCase):
    def test_run_onboard_game_returns_invalid_status_for_bad_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "bad_sources.yaml"
            manifest_path.write_text("game: marvel_rivals\nsources: {}\n", encoding="utf-8")
            result = run_onboard_game("marvel_rivals", manifest_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_onboarding_manifest")

    def test_run_adapt_game_schema_returns_schema_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            with patch("run.REPO_ROOT", Path(tempdir)), patch("pipeline.game_onboarding.REPO_ROOT", Path(tempdir)):
                result = run_adapt_game_schema("marvel_rivals")
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "schema_adapted")

    def test_run_ingest_game_sources_returns_invalid_status_for_bad_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            manifest_path = Path(tempdir) / "bad_sources.yaml"
            manifest_path.write_text("game: marvel_rivals\nsources: {}\n", encoding="utf-8")
            result = run_ingest_game_sources("marvel_rivals", manifest_path)
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_onboarding_sources")

    def test_run_build_onboarding_draft_returns_invalid_status_for_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_build_onboarding_draft(Path(tempdir))
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_onboarding_draft_build")

    def test_run_report_unresolved_derived_rows_returns_invalid_status_for_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_report_unresolved_derived_rows(Path(tempdir))
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_unresolved_derived_rows_report")

    def test_run_derive_game_detection_manifest_returns_invalid_status_for_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_derive_game_detection_manifest(Path(tempdir))
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_derived_detection_manifest")

    def test_run_fill_derived_detection_rows_returns_invalid_status_for_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_fill_derived_detection_rows(
                Path(tempdir),
                detection_ids=["demo.row"],
                source_manifests=["/tmp/demo.yaml"],
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_derived_row_fill")

    def test_run_prepare_derived_row_review_returns_invalid_status_for_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_prepare_derived_row_review(Path(tempdir), detection_ids=["demo.row"])
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_derived_row_review_preparation")

    def test_run_apply_derived_row_review_returns_invalid_status_for_missing_review_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_apply_derived_row_review(Path(tempdir) / "missing.review.json")
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_derived_row_review_application")

    def test_run_summarize_derived_row_review_returns_invalid_status_for_missing_review_target(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_summarize_derived_row_review(Path(tempdir) / "missing.review.json")
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_derived_row_review_summary")

    def test_run_publish_onboarding_draft_returns_invalid_status_for_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_publish_onboarding_draft(Path(tempdir))
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_onboarding_publish")

    def test_run_validate_onboarding_publish_returns_invalid_status_for_missing_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            result = run_validate_onboarding_publish(Path(tempdir))
            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "invalid_onboarding_publish_validation")

    def test_run_report_onboarding_batch_returns_invalid_status_for_missing_root(self) -> None:
        result = run_report_onboarding_batch("/tmp/does-not-exist-onboarding-report-root")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_onboarding_batch_report")

    def test_run_prepare_onboarding_identity_review_returns_invalid_status_for_missing_draft(self) -> None:
        result = run_prepare_onboarding_identity_review("/tmp/does-not-exist-onboarding-identity-draft")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_onboarding_identity_review_preparation")

    def test_run_publish_onboarding_batch_returns_invalid_status_for_missing_root(self) -> None:
        result = run_publish_onboarding_batch("/tmp/does-not-exist-onboarding-batch-root")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_onboarding_batch_publish")

    def test_run_refresh_clip_registry_invalid_root_returns_error(self) -> None:
        result = run_refresh_clip_registry("/tmp/does-not-exist-clip-registry-root")
        self.assertFalse(result["ok"])
        self.assertIn("error", result)

    def test_run_query_clip_registry_missing_db_returns_error(self) -> None:
        result = run_query_clip_registry(registry_path="/tmp/does-not-exist-clip-registry.sqlite")
        self.assertFalse(result["ok"])
        self.assertIn("error", result)

    def test_run_scan_chat_log_returns_signals_and_windows(self) -> None:
        log_text = "\n".join(
            [
                "[00:00:05] user1: clip it",
                "[00:00:06] user2: pog",
                "[00:00:07] user3: wtf",
            ]
        )
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as handle:
            handle.write(log_text)
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        result = run_scan_chat_log(path, "marvel_rivals")
        self.assertTrue(result["ok"])
        self.assertIn("signal_count", result)
        self.assertIn("window_count", result)
        self.assertIn("game_pack", result)

    def test_run_scan_vod_combines_sources_and_writes_sidecar(self) -> None:
        playlist_text = "\n".join(
            [
                "#EXTM3U",
                "#EXT-X-VERSION:3",
                "#EXT-X-TARGETDURATION:4",
                "#EXTINF:4.0,",
                "seg001.ts",
                "#EXTINF:4.0,",
                "seg002.ts",
                "#EXT-X-DISCONTINUITY",
                "#EXTINF:8.0,",
                "seg003.ts",
                "#EXTINF:7.5,",
                "seg004.ts",
            ]
        )
        log_text = "\n".join(
            [
                "[00:00:08] user1: clip it",
                "[00:00:09] user2: pog",
                "[00:00:10] user3: wtf",
            ]
        )

        with tempfile.NamedTemporaryFile("w", suffix=".m3u8", delete=False) as playlist_handle:
            playlist_handle.write(playlist_text)
            playlist_path = Path(playlist_handle.name)
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as log_handle:
            log_handle.write(log_text)
            log_path = Path(log_handle.name)
        self.addCleanup(playlist_path.unlink)
        self.addCleanup(log_path.unlink)

        with tempfile.TemporaryDirectory() as tempdir:
            with patch("run.load_config", return_value=_config_with_output_dir(tempdir)):
                result = run_scan_vod(playlist_path, "marvel_rivals", chat_log=log_path)

            self.assertTrue(result["ok"])
            self.assertEqual(result["source_results"]["playlist_hls"]["status"], "ok")
            self.assertEqual(result["source_results"]["audio_prepass"]["status"], "skipped")
            self.assertEqual(result["source_results"]["visual_prepass"]["status"], "skipped")
            self.assertEqual(result["source_results"]["chat_velocity"]["status"], "ok")
            self.assertGreaterEqual(result["window_count"], 1)
            self.assertEqual(result["schema_version"], "proxy_scan_v1")
            self.assertIn("scan_id", result)
            self.assertIn("config", result)
            self.assertIn("sources", result["config"]["proxy_scanner"])

            sidecar_path = Path(result["sidecar_path"])
            self.assertTrue(sidecar_path.is_file())
            self.assertEqual(json.loads(sidecar_path.read_text(encoding="utf-8")), result)

    def test_cli_requires_source_manifest_for_onboard_game(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--onboard-game", "marvel_rivals"]
            with self.assertRaises(SystemExit) as exc:
                with redirect_stdout(io.StringIO()):
                    run_main()
            self.assertEqual(exc.exception.code, 2)
        finally:
            sys.argv = original_argv

    def test_cli_requires_source_manifest_for_ingest_game_sources(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--ingest-game-sources", "marvel_rivals"]
            with self.assertRaises(SystemExit) as exc:
                with redirect_stdout(io.StringIO()):
                    run_main()
            self.assertEqual(exc.exception.code, 2)
        finally:
            sys.argv = original_argv

    def test_run_scan_vod_returns_audio_results_and_sidecar(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            audio_path = Path(handle.name)
        self.addCleanup(audio_path.unlink)
        _write_test_wave(audio_path, [0.02] * 12 + [0.85] * 2 + [0.02] * 4)

        with tempfile.TemporaryDirectory() as tempdir:
            with patch("run.load_config", return_value=_config_with_output_dir(tempdir)):
                result = run_scan_vod(audio_path, "marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["source_results"]["playlist_hls"]["status"], "skipped")
            self.assertEqual(result["source_results"]["audio_prepass"]["status"], "ok")
            self.assertEqual(result["source_results"]["visual_prepass"]["status"], "skipped")
            self.assertEqual(result["source_results"]["chat_velocity"]["status"], "skipped")
            self.assertGreaterEqual(result["window_count"], 1)
            self.assertTrue(Path(result["sidecar_path"]).is_file())

    def test_run_scan_vod_failed_scan_still_writes_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            with patch("run.load_config", return_value=_config_with_output_dir(tempdir)):
                result = run_scan_vod(Path("/tmp/does-not-exist.wav"), "marvel_rivals")

            self.assertFalse(result["ok"])
            self.assertEqual(result["source_results"]["playlist_hls"]["status"], "skipped")
            self.assertEqual(result["source_results"]["audio_prepass"]["status"], "failed")
            self.assertEqual(result["source_results"]["visual_prepass"]["status"], "skipped")
            sidecar_path = Path(result["sidecar_path"])
            self.assertTrue(sidecar_path.is_file())
            self.assertEqual(json.loads(sidecar_path.read_text(encoding="utf-8")), result)

    def test_run_scan_vod_reports_visual_results_and_fused_multi_source_window(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as audio_handle:
            audio_path = Path(audio_handle.name)
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as video_handle:
            video_path = Path(video_handle.name)
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as clip_handle:
            clip_path = Path(clip_handle.name)
        self.addCleanup(audio_path.unlink)
        self.addCleanup(video_path.unlink)
        self.addCleanup(clip_path.unlink)

        _write_test_wave(audio_path, [0.02] * 8 + [0.85] * 2 + [0.02] * 6)
        frames = [_solid_frame(80) for _ in range(8)] + [_solid_frame(240)] + [_solid_frame(80) for _ in range(6)]
        _write_test_video(video_path, frames)
        _mux_video_and_audio(video_path, audio_path, clip_path)

        with tempfile.TemporaryDirectory() as tempdir:
            config = _config_with_output_dir(tempdir)
            config["proxy_scanner"]["sources"]["audio_prepass"]["z_score_threshold"] = 2.5
            config["proxy_scanner"]["sources"]["visual_prepass"]["motion_z_score_threshold"] = 6.0
            config["proxy_scanner"]["sources"]["visual_prepass"]["flash_z_score_threshold"] = 2.0
            config["proxy_scanner"]["sources"]["visual_prepass"]["rolling_baseline_frames"] = 6
            config["proxy_scanner"]["weights"]["visual_flash_spike"] = 2.8
            with patch("run.load_config", return_value=config):
                result = run_scan_vod(clip_path, "marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["source_results"]["visual_prepass"]["status"], "ok")
            self.assertEqual(result["source_results"]["audio_prepass"]["status"], "ok")
            self.assertGreaterEqual(result["signal_count"], 2)
            self.assertTrue(any(signal["source"] == "visual_flash_spike" for signal in result["signals"]))
            self.assertTrue(any("visual_flash_spike" in window["sources"] for window in result["windows"]))

    def test_run_scan_vod_includes_hf_multimodal_metadata_and_signals(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            clip_path = Path(handle.name)
        self.addCleanup(clip_path.unlink)
        _write_test_video(clip_path, [_solid_frame(80) for _ in range(16)])

        with tempfile.TemporaryDirectory() as tempdir:
            config = _config_with_output_dir(tempdir)
            config["proxy_scanner"]["sources"]["audio_prepass"]["enabled"] = False
            config["proxy_scanner"]["sources"]["visual_prepass"]["enabled"] = False
            config["proxy_scanner"]["sources"]["hf_multimodal"]["enabled"] = True
            with (
                patch("run.load_config", return_value=config),
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
                result = run_scan_vod(clip_path, "marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["source_results"]["hf_multimodal"]["status"], "ok")
            self.assertIn("metadata", result["source_results"]["hf_multimodal"])
            self.assertIn(
                "duration_ms",
                result["source_results"]["hf_multimodal"]["metadata"]["stages"]["shot_detector"],
            )
            self.assertIn(
                "output_counts",
                result["source_results"]["hf_multimodal"]["metadata"]["stages"]["reranker"],
            )
            self.assertIn(
                "hf_rerank_highlight",
                [signal["source"] for signal in result["signals"]],
            )
            self.assertTrue(
                any("hf_multimodal" in window["source_families"] for window in result["windows"])
            )

    def test_run_scan_vod_hf_multimodal_dependency_failure_keeps_sidecar_valid(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as handle:
            clip_path = Path(handle.name)
        self.addCleanup(clip_path.unlink)
        _write_test_video(clip_path, [_solid_frame(80) for _ in range(16)])

        with tempfile.TemporaryDirectory() as tempdir:
            config = _config_with_output_dir(tempdir)
            config["proxy_scanner"]["sources"]["audio_prepass"]["enabled"] = False
            config["proxy_scanner"]["sources"]["visual_prepass"]["enabled"] = False
            config["proxy_scanner"]["sources"]["hf_multimodal"]["enabled"] = True
            with (
                patch("run.load_config", return_value=config),
                patch(
                    "pipeline.hf_adapters._run_transnetv2_backend",
                    return_value={
                        "boundaries": [{"timestamp_seconds": 2.0, "boundary_score": 0.9}],
                        "proposals": [{"start_seconds": 0.0, "end_seconds": 5.0, "proposal_score": 0.9}],
                    },
                ),
                patch(
                    "pipeline.hf_adapters._run_whisper_backend",
                    side_effect=HFAdapterError("asr", "missing dependency or runtime capability: transformers"),
                ),
                patch(
                    "pipeline.hf_adapters._run_xclip_backend",
                    return_value={"segment_scores": []},
                ),
                patch(
                    "pipeline.hf_adapters._run_siglip_backend",
                    side_effect=HFAdapterError("keyframes", "requires a configured keyframe embedding backend"),
                ),
                patch(
                    "pipeline.hf_adapters._run_smolvlm_backend",
                    side_effect=HFAdapterError("reranker", "requires a configured multimodal reranker backend"),
                ),
            ):
                result = run_scan_vod(clip_path, "marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["source_results"]["hf_multimodal"]["status"], "ok")
            self.assertEqual(
                result["source_results"]["hf_multimodal"]["metadata"]["stage_statuses"]["asr"],
                "failed",
            )
            self.assertEqual(
                result["source_results"]["hf_multimodal"]["metadata"]["stages"]["asr"]["reason"],
                "missing dependency or runtime capability: transformers",
            )
            self.assertIn(
                "duration_ms",
                result["source_results"]["hf_multimodal"]["metadata"]["stages"]["asr"],
            )
            self.assertTrue(Path(result["sidecar_path"]).is_file())

    def test_disabled_source_is_skipped_cleanly(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            audio_path = Path(handle.name)
        self.addCleanup(audio_path.unlink)
        _write_test_wave(audio_path, [0.02] * 12 + [0.85] * 2 + [0.02] * 4)

        with tempfile.TemporaryDirectory() as tempdir:
            config = _config_with_output_dir(tempdir)
            config["proxy_scanner"]["sources"]["audio_prepass"]["enabled"] = False
            with patch("run.load_config", return_value=config):
                result = run_scan_vod(audio_path, "marvel_rivals")

            self.assertEqual(result["source_results"]["audio_prepass"]["status"], "skipped")
            self.assertEqual(result["source_results"]["audio_prepass"]["reason"], "disabled by config")

    def test_legacy_signals_config_still_works(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            audio_path = Path(handle.name)
        self.addCleanup(audio_path.unlink)
        _write_test_wave(audio_path, [0.02] * 12 + [0.85] * 2 + [0.02] * 4)

        with tempfile.TemporaryDirectory() as tempdir:
            with patch("run.load_config", return_value=_legacy_signals_config(tempdir)):
                result = run_scan_vod(audio_path, "marvel_rivals")

            self.assertTrue(result["ok"])
            self.assertEqual(result["source_results"]["audio_prepass"]["status"], "ok")
            self.assertIn("sources", result["config"]["proxy_scanner"])
            self.assertNotIn("signals", result["config"]["proxy_scanner"])
            self.assertEqual(result["config_warnings"][0]["status"], "legacy_proxy_signals_config")

    def test_run_audit_pipeline_contracts_reports_legacy_proxy_signals_config(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            raw_config = {"proxy_scanner": {"signals": {"audio_prepass": {"enabled": True}}}}
            with patch("run.REPO_ROOT", Path(tempdir)), patch("run._load_repo_config_file", return_value=raw_config), patch(
                "run.audit_pipeline_contracts",
                return_value={
                    "ok": True,
                    "status": "ok",
                    "game_filter": None,
                    "pack_contracts": [],
                    "legacy_usage": [
                        {
                            "status": "legacy_proxy_signals_config",
                            "surface": "config.proxy_scanner.signals",
                        }
                    ],
                    "onboarding_publish_consistency": [],
                    "runtime_contract_findings": [],
                    "fusion_contract_findings": [],
                    "recommended_cleanup_order": [],
                    "warnings": [],
                },
            ):
                result = run_audit_pipeline_contracts()
            self.assertTrue(result["ok"])
            self.assertEqual(result["legacy_usage"][0]["status"], "legacy_proxy_signals_config")

    def test_sources_take_precedence_over_legacy_signals(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            audio_path = Path(handle.name)
        self.addCleanup(audio_path.unlink)
        _write_test_wave(audio_path, [0.02] * 12 + [0.85] * 2 + [0.02] * 4)

        with tempfile.TemporaryDirectory() as tempdir:
            config = _legacy_signals_config(tempdir)
            config["proxy_scanner"]["sources"] = copy.deepcopy(DEFAULT_CONFIG["proxy_scanner"]["sources"])
            config["proxy_scanner"]["sources"]["audio_prepass"]["enabled"] = False
            config["proxy_scanner"]["signals"]["audio_prepass"]["enabled"] = True

            with patch("run.load_config", return_value=config):
                result = run_scan_vod(audio_path, "marvel_rivals")

            self.assertEqual(result["source_results"]["audio_prepass"]["status"], "skipped")
            self.assertEqual(result["source_results"]["audio_prepass"]["reason"], "disabled by config")

    def test_cli_routes_to_audit_pipeline_contracts(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--audit-pipeline-contracts", "--game", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_audit_pipeline_contracts",
                return_value={"ok": True, "status": "ok", "pack_contracts": []},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_clip_registry(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--refresh-clip-registry", "/tmp/sidecars"]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_clip_registry",
                return_value={"ok": True, "registry_path": "/tmp/registry.sqlite"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_query_clip_registry(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--query-clip-registry", "--mode", "fused-events"]
            stdout = io.StringIO()
            with patch(
                "run.run_query_clip_registry",
                return_value={"ok": True, "row_count": 10, "rows": [{"id": index} for index in range(10)]},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("rows_sample", payload)
            self.assertNotIn("rows", payload)
            self.assertEqual(payload["rows_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_query_clip_registry_with_full_json(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--query-clip-registry", "--mode", "fused-events", "--full-json"]
            stdout = io.StringIO()
            with patch(
                "run.run_query_clip_registry",
                return_value={"ok": True, "row_count": 10, "rows": [{"id": index} for index in range(10)]},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertIn("rows", payload)
            self.assertNotIn("rows_sample", payload)
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_export_v2_training_datasets(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--export-v2-training-datasets",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--output-root",
                "/tmp/v2-datasets",
                "--hook-mode",
                "natural",
                "--platform",
                "youtube",
                "--evidence-mode",
                "real_only",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_export_v2_training_datasets",
                return_value={"ok": True, "dataset_export_id": "v2-training-123", "manifest_path": "/tmp/v2-datasets/out.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                registry_path="/tmp/registry.sqlite",
                output_root="/tmp/v2-datasets",
                game="marvel_rivals",
                fixture_id=None,
                candidate_id=None,
                lifecycle_state=None,
                hook_archetype=None,
                hook_mode="natural",
                platform="youtube",
                account_id=None,
                evidence_mode="real_only",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_run_shadow_ranking_replay(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--run-shadow-ranking-replay",
                "--dataset-manifest",
                "/tmp/dataset.manifest.json",
                "--model-family",
                "deterministic_shadow_baseline",
                "--model-version",
                "v1",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_run_shadow_ranking_replay",
                return_value={"ok": True, "manifest_path": "/tmp/replay.json", "row_count": 2},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/dataset.manifest.json",
                model_path=None,
                model_family="deterministic_shadow_baseline",
                model_version="v1",
                output_path=None,
                game=None,
                fixture_id=None,
                candidate_id=None,
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_compare_shadow_ranking_replay(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--compare-shadow-ranking-replay",
                "/tmp/replay.shadow_ranking_replay.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_compare_shadow_ranking_replay",
                return_value={"ok": True, "report_path": "/tmp/comparison.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/replay.shadow_ranking_replay.json",
                output_path=None,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_train_shadow_ranking_model(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--train-shadow-ranking-model",
                "--dataset-manifest",
                "/tmp/dataset.manifest.json",
                "--model-family",
                "gradient_boosted_shadow_ranker",
                "--model-output-path",
                "/tmp/model.json",
                "--training-target",
                "approved_or_selected_probability",
                "--split-key",
                "candidate_id",
                "--train-fraction",
                "0.75",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_train_shadow_ranking_model",
                return_value={"ok": True, "manifest_path": "/tmp/model.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/dataset.manifest.json",
                model_output_path="/tmp/model.json",
                model_family="gradient_boosted_shadow_ranker",
                training_target="approved_or_selected_probability",
                split_key="candidate_id",
                train_fraction=0.75,
                game=None,
                fixture_id=None,
                candidate_id=None,
                platform=None,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_compare_shadow_model_families(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--compare-shadow-model-families",
                "/tmp/linear.shadow_ranking_experiment.json",
                "/tmp/boosted.shadow_ranking_experiment.json",
                "--training-target",
                "approved_or_selected_probability",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--output-path",
                "/tmp/families.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_compare_shadow_model_families",
                return_value={"ok": True, "manifest_path": "/tmp/families.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                ["/tmp/linear.shadow_ranking_experiment.json", "/tmp/boosted.shadow_ranking_experiment.json"],
                output_path="/tmp/families.json",
                training_target="approved_or_selected_probability",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_run_shadow_benchmark_matrix(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--run-shadow-benchmark-matrix",
                "--dataset-manifest",
                "/tmp/dataset.manifest.json",
                "--policy-path",
                "/tmp/policy.json",
                "--model-family",
                "gradient_boosted_shadow_ranker",
                "--training-target",
                "export_selection_probability",
                "--platform",
                "youtube",
                "--output-path",
                "/tmp/benchmark.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_run_shadow_benchmark_matrix",
                return_value={"ok": True, "manifest_path": "/tmp/benchmark.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/dataset.manifest.json",
                policy_path="/tmp/policy.json",
                model_family="gradient_boosted_shadow_ranker",
                training_target="export_selection_probability",
                split_key="fixture_id",
                train_fraction=0.8,
                game=None,
                platform="youtube",
                output_path="/tmp/benchmark.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_shadow_benchmark_matrix(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-shadow-benchmark-matrix",
                "/tmp/benchmark.json",
                "--training-target",
                "approved_or_selected_probability",
                "--recommendation-decision",
                "prefer_shadow",
                "--model-family",
                "linear_shadow_ranker",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_shadow_benchmark_matrix",
                return_value={"ok": True, "row_count": 1},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/benchmark.json",
                registry_path=None,
                training_target="approved_or_selected_probability",
                game=None,
                platform=None,
                recommendation_decision="prefer_shadow",
                model_family="linear_shadow_ranker",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_shadow_benchmark_matrix_with_full_json(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-shadow-benchmark-matrix",
                "/tmp/benchmark.json",
                "--full-json",
            ]
            stdout = io.StringIO()
            rows = [{"run_id": f"run-{index}"} for index in range(12)]
            with patch(
                "run.run_summarize_shadow_benchmark_matrix",
                return_value={"ok": True, "status": "ok", "row_count": 12, "rows": rows},
            ):
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["rows"], rows)
            self.assertNotIn("rows_sample", payload)
            self.assertNotIn("truncated", payload)
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_review_shadow_benchmark_results(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--review-shadow-benchmark-results",
                "/tmp/one.shadow_benchmark_matrix.json",
                "/tmp/two.shadow_benchmark_matrix.json",
                "--training-target",
                "export_selection_probability",
                "--model-family",
                "gradient_boosted_shadow_ranker",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--output-path",
                "/tmp/review.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_review_shadow_benchmark_results",
                return_value={"ok": True, "manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                ["/tmp/one.shadow_benchmark_matrix.json", "/tmp/two.shadow_benchmark_matrix.json"],
                output_path="/tmp/review.json",
                training_target="export_selection_probability",
                model_family="gradient_boosted_shadow_ranker",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_compare_shadow_benchmark_evidence_modes(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--compare-shadow-benchmark-evidence-modes",
                "/tmp/real.shadow_benchmark_review.json",
                "/tmp/synthetic.shadow_benchmark_review.json",
                "--training-target",
                "post_performance_score",
                "--platform",
                "youtube",
                "--output-path",
                "/tmp/compare.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_compare_shadow_benchmark_evidence_modes",
                return_value={"ok": True, "manifest_path": "/tmp/compare.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/real.shadow_benchmark_review.json",
                "/tmp/synthetic.shadow_benchmark_review.json",
                output_path="/tmp/compare.json",
                training_target="post_performance_score",
                game=None,
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_shadow_target_readiness(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-shadow-target-readiness",
                "/tmp/review.shadow_benchmark_review.json",
                "--training-target",
                "post_performance_score",
                "--model-family",
                "linear_shadow_ranker",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_shadow_target_readiness",
                return_value={"ok": True, "row_count": 1},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/review.shadow_benchmark_review.json",
                registry_path=None,
                training_target="post_performance_score",
                game="marvel_rivals",
                platform="youtube",
                model_family="linear_shadow_ranker",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_evaluate_shadow_ranking_model(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--evaluate-shadow-ranking-model",
                "--model-path",
                "/tmp/model.json",
                "--dataset-manifest",
                "/tmp/dataset.manifest.json",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_evaluate_shadow_ranking_model",
                return_value={"ok": True, "manifest_path": "/tmp/experiment.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                model_path="/tmp/model.json",
                dataset_manifest="/tmp/dataset.manifest.json",
                output_path=None,
                game=None,
                fixture_id=None,
                candidate_id=None,
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_evaluate_shadow_experiment_policy(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--evaluate-shadow-experiment-policy",
                "--experiment-manifest",
                "/tmp/experiment.shadow_ranking_experiment.json",
                "--policy-path",
                "/tmp/policy.shadow_evaluation_policy.json",
                "--target",
                "export_selection_probability",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_evaluate_shadow_experiment_policy",
                return_value={"ok": True, "manifest_path": "/tmp/ledger.shadow_experiment_ledger.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/experiment.shadow_ranking_experiment.json",
                policy_path="/tmp/policy.shadow_evaluation_policy.json",
                target="export_selection_probability",
                output_path=None,
                game=None,
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_shadow_experiment_ledger(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-shadow-experiment-ledger",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--target",
                "post_performance_score",
                "--training-target",
                "approved_or_selected_probability",
                "--recommendation-decision",
                "prefer_shadow",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_shadow_experiment_ledger",
                return_value={"ok": True, "target_count": 1, "targets": []},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                registry_path="/tmp/registry.sqlite",
                target="post_performance_score",
                game=None,
                platform=None,
                recommendation_decision="prefer_shadow",
                training_target="approved_or_selected_probability",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_create_workflow_run(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--create-workflow-run", "--workflow-type", "selection_queue"]
            stdout = io.StringIO()
            with patch(
                "run.run_create_workflow_run",
                return_value={"ok": True, "workflow_run_id": "workflow-123", "manifest_path": "/tmp/workflow.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "selection_queue",
                registry_path=None,
                output_path=None,
                game=None,
                fixture_id=None,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_materialize_synthetic_post_coverage(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--materialize-synthetic-post-coverage",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--synthetic-profile",
                "balanced",
                "--include-rejected",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_materialize_synthetic_post_coverage",
                return_value={"ok": True, "candidate_count": 2},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                fixture_id=None,
                platform=None,
                account_id=None,
                workflow_run_id=None,
                output_root=None,
                synthetic_profile="balanced",
                include_rejected=True,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_hook_evaluation_compacts_output_by_default(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--report-hook-evaluation",
                "/tmp/fixtures.json",
                "--baseline-sidecar-root",
                "/tmp/baseline",
                "--trial-sidecar-root",
                "/tmp/trial",
                "--registry-path",
                "/tmp/registry.sqlite",
            ]
            stdout = io.StringIO()
            fixture_rows = [{"fixture_id": f"fixture-{index}"} for index in range(12)]
            with patch(
                "run.run_report_hook_evaluation",
                return_value={
                    "ok": True,
                    "status": "ok",
                    "schema_version": "hook_evaluation_report_v1",
                    "fixture_manifest_path": "/tmp/fixtures.json",
                    "baseline_sidecar_root": "/tmp/baseline",
                    "trial_sidecar_root": "/tmp/trial",
                    "registry_path": "/tmp/registry.sqlite",
                    "trial_comparison": {
                        "comparison_row_count": 12,
                        "summary": {"matched": 10},
                        "recommendation": {"decision": "prefer_trial"},
                        "warning_count": 0,
                        "fixture_rows": fixture_rows,
                    },
                    "candidate_rollups": {"selected_or_approved": {"candidate_count": 3}},
                    "fused_hook_disagreement": {"row_count": 12},
                    "policy": {"future_gate_readiness": "insufficient_evidence"},
                    "warnings": [],
                    "report_path": "/tmp/hook-report.json",
                },
            ):
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(len(payload["fixture_rows_sample"]), 8)
            self.assertEqual(payload["fixture_rows_omitted_count"], 4)
            self.assertTrue(payload["truncated"])
            self.assertNotIn("fixture_rows", payload)
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_import_real_posted_lineage(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--import-real-posted-lineage",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--source-root",
                "/tmp/source-a",
                "--source-root",
                "/tmp/source-b",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_import_real_posted_lineage",
                return_value={"ok": True, "manifest_path": "/tmp/import.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                source_roots=["/tmp/source-a", "/tmp/source-b"],
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                output_path=None,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_validate_real_artifact_intake(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--validate-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_validate_real_artifact_intake",
                return_value={"ok": True, "manifest_path": "/tmp/validation.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_bootstrap_real_artifact_intake_bundle(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--bootstrap-real-artifact-intake-bundle",
                "--bundle-name",
                "session-001",
                "--intake-root",
                "/tmp/intake",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_bootstrap_real_artifact_intake_bundle",
                return_value={"ok": True, "bundle_root": "/tmp/intake/bundles/session-001"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "session-001",
                intake_root="/tmp/intake",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_real_artifact_intake(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-real-artifact-intake",
                "/tmp/validation.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_real_artifact_intake",
                return_value={"ok": True, "schema_version": "real_artifact_intake_summary_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/validation.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_real_artifact_intake_coverage(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--report-real-artifact-intake-coverage",
                "/tmp/validation.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_report_real_artifact_intake_coverage",
                return_value={"ok": True, "schema_version": "real_artifact_intake_coverage_report_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/validation.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_preflight_real_artifact_intake_refresh(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--preflight-real-artifact-intake-refresh",
                "/tmp/validation.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--require-resolved-dedup",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_preflight_real_artifact_intake_refresh",
                return_value={"ok": True, "schema_version": "real_artifact_intake_refresh_preflight_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/validation.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_record_real_artifact_intake_preflight_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--record-real-artifact-intake-preflight-history",
                "/tmp/validation.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--require-resolved-dedup",
                "--output-path",
                "/tmp/preflight-history.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_record_real_artifact_intake_preflight_history",
                return_value={"ok": True, "schema_version": "real_artifact_intake_refresh_preflight_history_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/validation.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
                output_path="/tmp/preflight-history.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_real_artifact_intake_preflight_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-real-artifact-intake-preflight-history",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_real_artifact_intake_preflight_history",
                return_value={"ok": True, "schema_version": "real_artifact_intake_refresh_preflight_history_summary_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_real_artifact_intake_preflight_trends(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--report-real-artifact-intake-preflight-trends",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_report_real_artifact_intake_preflight_trends",
                return_value={"ok": True, "schema_version": "real_artifact_intake_refresh_preflight_trend_report_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_record_real_artifact_intake_refresh_outcome_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--record-real-artifact-intake-refresh-outcome-history",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--require-resolved-dedup",
                "--output-root",
                "/tmp/out",
                "--output-path",
                "/tmp/history.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_record_real_artifact_intake_refresh_outcome_history",
                return_value={"ok": True, "schema_version": "real_artifact_intake_refresh_outcome_history_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
                output_root="/tmp/out",
                output_path="/tmp/history.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_real_artifact_intake_refresh_outcome_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-real-artifact-intake-refresh-outcome-history",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_real_artifact_intake_refresh_outcome_history",
                return_value={"ok": True, "schema_version": "real_artifact_intake_refresh_outcome_history_summary_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_real_artifact_intake_refresh_outcome_trends(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--report-real-artifact-intake-refresh-outcome-trends",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_report_real_artifact_intake_refresh_outcome_trends",
                return_value={"ok": True, "schema_version": "real_artifact_intake_refresh_outcome_trend_report_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_real_artifact_intake_history_comparison(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--report-real-artifact-intake-history-comparison",
                "/tmp/comparison.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_report_real_artifact_intake_history_comparison",
                return_value={"ok": True, "schema_version": "real_artifact_intake_history_comparison_report_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/comparison.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_render_real_artifact_intake_dashboard(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--render-real-artifact-intake-dashboard",
                "/tmp/comparison.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--output-path",
                "/tmp/dashboard.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_render_real_artifact_intake_dashboard",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dashboard_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/comparison.json",
                validation_manifest=None,
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
                output_path="/tmp/dashboard.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_real_artifact_intake_dashboard_registry(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-real-artifact-intake-dashboard-registry",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_real_artifact_intake_dashboard_registry",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dashboard_registry_summary_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_real_artifact_intake_comparison_targets(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-real-artifact-intake-comparison-targets",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_real_artifact_intake_comparison_targets",
                return_value={"ok": True, "schema_version": "real_artifact_intake_comparison_target_summary_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_record_real_artifact_intake_dashboard_summary_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--record-real-artifact-intake-dashboard-summary-history",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--output-path",
                "/tmp/dashboard-summary-history.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_record_real_artifact_intake_dashboard_summary_history",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dashboard_summary_history_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                registry_path="/tmp/registry.sqlite",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
                output_path="/tmp/dashboard-summary-history.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_real_artifact_intake_dashboard_summary_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-real-artifact-intake-dashboard-summary-history",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_real_artifact_intake_dashboard_summary_history",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dashboard_summary_history_summary_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_real_artifact_intake_dashboard_summary_trends(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--report-real-artifact-intake-dashboard-summary-trends",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_report_real_artifact_intake_dashboard_summary_trends",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dashboard_summary_trend_report_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_advise_real_artifact_intake_dedup(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--advise-real-artifact-intake-dedup",
                "/tmp/validation.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_advise_real_artifact_intake_dedup",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dedup_advisory_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/validation.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_materialize_real_artifact_intake_dedup_resolutions(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--materialize-real-artifact-intake-dedup-resolutions",
                "/tmp/advisory.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_materialize_real_artifact_intake_dedup_resolutions",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dedup_resolution_materialization_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/advisory.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_real_artifact_intake_dedup_resolutions(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--summarize-real-artifact-intake-dedup-resolutions",
                "/tmp/advisory.json",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_real_artifact_intake_dedup_resolutions",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dedup_resolution_summary_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/advisory.json",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_update_real_artifact_intake_dedup_resolution(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--update-real-artifact-intake-dedup-resolution",
                "/tmp/advisory.json",
                "--group-id",
                "dedup-abc123",
                "--resolution-status",
                "accepted",
                "--reviewed-by",
                "tj",
                "--notes",
                "canonical confirmed",
                "--intake-root",
                "/tmp/intake",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_update_real_artifact_intake_dedup_resolution",
                return_value={"ok": True, "schema_version": "real_artifact_intake_dedup_resolution_update_v1"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/advisory.json",
                group_id="dedup-abc123",
                status="accepted",
                reviewed_by="tj",
                notes="canonical confirmed",
                intake_root="/tmp/intake",
                game="marvel_rivals",
                platform="youtube",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_only_benchmark(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-only-benchmark",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--source-root",
                "/tmp/source-a",
                "--source-root",
                "/tmp/source-b",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/import.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_only_benchmark",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                source_roots=["/tmp/source-a", "/tmp/source-b"],
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                output_root="/tmp/real-refresh",
                output_path="/tmp/import.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_artifact_intake(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_artifact_intake",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=False,
                record_dashboard_summary_history=False,
                record_refresh_outcome_history=False,
                render_dashboard=False,
                refresh_artifact_registry=False,
                comparison_manifest=None,
                output_root="/tmp/real-refresh",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_artifact_intake_with_dedup_gate(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--require-resolved-dedup",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_artifact_intake",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=True,
                record_dashboard_summary_history=False,
                record_refresh_outcome_history=False,
                render_dashboard=False,
                refresh_artifact_registry=False,
                comparison_manifest=None,
                output_root="/tmp/real-refresh",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_artifact_intake_with_dashboard_summary_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--record-dashboard-summary-history-on-refresh",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_artifact_intake",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=False,
                record_dashboard_summary_history=True,
                record_refresh_outcome_history=False,
                render_dashboard=False,
                refresh_artifact_registry=False,
                comparison_manifest=None,
                output_root="/tmp/real-refresh",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_artifact_intake_with_refresh_outcome_history(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--record-refresh-outcome-history-on-refresh",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_artifact_intake",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=False,
                record_dashboard_summary_history=False,
                record_refresh_outcome_history=True,
                render_dashboard=False,
                refresh_artifact_registry=False,
                comparison_manifest=None,
                output_root="/tmp/real-refresh",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_artifact_intake_with_dashboard_render(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--render-dashboard-on-refresh",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_artifact_intake",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=False,
                record_dashboard_summary_history=False,
                record_refresh_outcome_history=False,
                render_dashboard=True,
                refresh_artifact_registry=False,
                comparison_manifest=None,
                output_root="/tmp/real-refresh",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_artifact_intake_with_artifact_registry_refresh(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--refresh-artifact-registry-on-refresh",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_artifact_intake",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=False,
                record_dashboard_summary_history=False,
                record_refresh_outcome_history=False,
                render_dashboard=False,
                refresh_artifact_registry=True,
                comparison_manifest=None,
                output_root="/tmp/real-refresh",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_refresh_real_artifact_intake_with_evidence_comparison(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--refresh-real-artifact-intake",
                "--intake-root",
                "/tmp/intake",
                "--registry-path",
                "/tmp/registry.sqlite",
                "--game",
                "marvel_rivals",
                "--platform",
                "youtube",
                "--compare-evidence-on-refresh",
                "/tmp/synthetic-review.json",
                "--output-root",
                "/tmp/real-refresh",
                "--output-path",
                "/tmp/validation.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_refresh_real_artifact_intake",
                return_value={"ok": True, "review_manifest_path": "/tmp/review.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                intake_root="/tmp/intake",
                registry_path="/tmp/registry.sqlite",
                game="marvel_rivals",
                platform="youtube",
                require_resolved_dedup=False,
                record_dashboard_summary_history=False,
                record_refresh_outcome_history=False,
                render_dashboard=False,
                refresh_artifact_registry=False,
                comparison_manifest="/tmp/synthetic-review.json",
                output_root="/tmp/real-refresh",
                output_path="/tmp/validation.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_query_workflow_queue(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--query-workflow-queue", "--workflow-type", "export_queue", "--limit", "5"]
            stdout = io.StringIO()
            with patch(
                "run.run_query_workflow_queue",
                return_value={"ok": True, "row_count": 10, "rows": [{"id": index} for index in range(10)]},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "export_queue",
                registry_path=None,
                game=None,
                fixture_id=None,
                limit=5,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("rows_sample", payload)
            self.assertNotIn("rows", payload)
            self.assertEqual(payload["rows_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_posted_performance(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--report-posted-performance", "--platform", "youtube"]
            stdout = io.StringIO()
            with patch(
                "run.run_report_posted_performance",
                return_value={"ok": True, "row_count": 10, "rows": [{"id": index} for index in range(10)]},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                registry_path=None,
                game=None,
                platform="youtube",
                account_id=None,
                workflow_run_id=None,
                candidate_id=None,
                fixture_id=None,
                hook_archetype=None,
                hook_mode=None,
                output_path=None,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("rows_sample", payload)
            self.assertNotIn("rows", payload)
            self.assertEqual(payload["rows_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_scan_vod_batch(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--scan-vod-batch", "/tmp/clips", "marvel_rivals", "--limit", "5"]
            stdout = io.StringIO()
            with patch(
                "run.run_scan_vod_batch",
                return_value={
                    "schema_version": "proxy_scan_batch_v1",
                    "batch_id": "batch-1",
                    "game": "marvel_rivals",
                    "root": "/tmp/clips",
                    "file_count": 10,
                    "scanned_count": 10,
                    "success_count": 10,
                    "failed_count": 0,
                    "window_count_total": 20,
                    "skip_count": 1,
                    "inspect_count": 2,
                    "download_candidate_count": 7,
                    "results": [{"source": f"/tmp/{index}.mp4"} for index in range(10)],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/clips", "marvel_rivals", pattern="*.mp4", limit=5)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["batch_id"], "batch-1")
            self.assertIn("results_sample", payload)
            self.assertNotIn("results", payload)
            self.assertEqual(payload["results_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_onboarding_batch(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--report-onboarding-batch", "/tmp/onboarding", "--game", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_report_onboarding_batch",
                return_value={
                    "ok": True,
                    "draft_count": 10,
                    "drafts": [{"draft_root": f"/tmp/draft-{index}"} for index in range(10)],
                    "summary": {"ready": 1},
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/onboarding", game="marvel_rivals", output_path=None)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("drafts_sample", payload)
            self.assertNotIn("drafts", payload)
            self.assertEqual(payload["drafts_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_publish_onboarding_batch(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--publish-onboarding-batch", "/tmp/onboarding", "--game", "marvel_rivals", "--apply"]
            stdout = io.StringIO()
            with patch(
                "run.run_publish_onboarding_batch",
                return_value={
                    "ok": True,
                    "summary": {"published": 10},
                    "published": [{"game": f"game_{index}"} for index in range(10)],
                    "ready": [],
                    "blocked": [],
                    "failed": [],
                    "skipped": [],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/onboarding", game="marvel_rivals", apply=True, output_path=None)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("published_sample", payload)
            self.assertNotIn("published", payload)
            self.assertEqual(payload["published_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_onboarding_batch_with_full_json(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--report-onboarding-batch", "/tmp/onboarding", "--full-json"]
            stdout = io.StringIO()
            with patch(
                "run.run_report_onboarding_batch",
                return_value={
                    "ok": True,
                    "draft_count": 10,
                    "drafts": [{"draft_root": f"/tmp/draft-{index}"} for index in range(10)],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/onboarding", game=None, output_path=None)
            payload = json.loads(stdout.getvalue())
            self.assertIn("drafts", payload)
            self.assertNotIn("drafts_sample", payload)
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_validate_onboarding_publish(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--validate-onboarding-publish", "/tmp/draft"]
            stdout = io.StringIO()
            with patch(
                "run.run_validate_onboarding_publish",
                return_value={
                    "ok": True,
                    "can_publish": False,
                    "readiness": "needs_binding_review",
                    "findings": [{"type": "a"} for _ in range(12)],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/draft")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("findings_sample", payload)
            self.assertNotIn("findings", payload)
            self.assertEqual(payload["findings_omitted_count"], 4)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_derive_game_detection_manifest(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--derive-game-detection-manifest", "/tmp/draft", "--output-path", "/tmp/derived.yaml"]
            stdout = io.StringIO()
            with patch(
                "run.run_derive_game_detection_manifest",
                return_value={"ok": True, "manifest_path": "/tmp/derived.yaml"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/draft", output_path="/tmp/derived.yaml")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_unresolved_derived_rows(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--report-unresolved-derived-rows", "/tmp/draft", "--output-path", "/tmp/report.json"]
            stdout = io.StringIO()
            with patch(
                "run.run_report_unresolved_derived_rows",
                return_value={
                    "ok": True,
                    "unresolved_required_count": 10,
                    "rows": [{"detection_id": f"row_{index}"} for index in range(10)],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/draft", output_path="/tmp/report.json")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("rows_sample", payload)
            self.assertNotIn("rows", payload)
            self.assertEqual(payload["rows_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_fill_derived_detection_rows(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--fill-derived-detection-rows",
                "/tmp/draft",
                "--detection-id",
                "game.row_a",
                "--detection-id",
                "game.row_b",
                "--fill-source-manifest",
                "/tmp/source-a.yaml",
                "--fill-source-manifest",
                "/tmp/source-b.yaml",
                "--output-path",
                "/tmp/fill.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_fill_derived_detection_rows",
                return_value={"ok": True, "status": "targeted_row_fill_completed"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/draft",
                detection_ids=["game.row_a", "game.row_b"],
                source_manifests=["/tmp/source-a.yaml", "/tmp/source-b.yaml"],
                output_path="/tmp/fill.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_prepare_derived_row_review(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--prepare-derived-row-review",
                "/tmp/draft",
                "--detection-id",
                "game.row_a",
                "--detection-id",
                "game.row_b",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_prepare_derived_row_review",
                return_value={"ok": True, "item_count": 2},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/draft",
                detection_ids=["game.row_a", "game.row_b"],
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_apply_derived_row_review(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--apply-derived-row-review",
                "/tmp/review-dir",
                "--accept-recommended",
                "--only-auto-populated",
                "--reject-zero-candidate",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_apply_derived_row_review",
                return_value={
                    "ok": True,
                    "applied_count": 10,
                    "skipped_count": 0,
                    "failed_count": 0,
                    "applied_reviews": [{"detection_id": f"row_{index}"} for index in range(10)],
                    "skipped_reviews": [],
                    "failed_reviews": [],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/review-dir",
                accept_recommended=True,
                only_auto_populated=True,
                reject_zero_candidate=True,
                defer_zero_candidate=False,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("applied_reviews_sample", payload)
            self.assertNotIn("applied_reviews", payload)
            self.assertEqual(payload["applied_reviews_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_derived_row_review(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--summarize-derived-row-review", "/tmp/draft"]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_derived_row_review",
                return_value={
                    "ok": True,
                    "review_file_count": 10,
                    "rows": [{"detection_id": f"row_{index}"} for index in range(10)],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/draft")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertIn("rows_sample", payload)
            self.assertNotIn("rows", payload)
            self.assertEqual(payload["rows_omitted_count"], 2)
            self.assertTrue(payload["truncated"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_summarize_derived_row_review_with_full_json(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--summarize-derived-row-review", "/tmp/draft", "--full-json"]
            stdout = io.StringIO()
            with patch(
                "run.run_summarize_derived_row_review",
                return_value={
                    "ok": True,
                    "review_file_count": 10,
                    "rows": [{"detection_id": f"row_{index}"} for index in range(10)],
                },
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/draft")
            payload = json.loads(stdout.getvalue())
            self.assertIn("rows", payload)
            self.assertNotIn("rows_sample", payload)
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_prepare_onboarding_identity_review(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--prepare-onboarding-identity-review", "/tmp/draft", "--session-name", "triage-a"]
            stdout = io.StringIO()
            with patch(
                "run.run_prepare_onboarding_identity_review",
                return_value={"ok": True, "item_count": 1},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/draft", gpt_repo=None, session_name="triage-a")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_apply_onboarding_identity_review(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--apply-onboarding-identity-review", "/tmp/session.json"]
            stdout = io.StringIO()
            with patch(
                "run.run_apply_onboarding_identity_review",
                return_value={"ok": True, "resolved_count": 1},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/session.json", gpt_repo=None)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_cleanup_onboarding_identity_review(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--cleanup-onboarding-identity-review", "/tmp/session.json"]
            stdout = io.StringIO()
            with patch(
                "run.run_cleanup_onboarding_identity_review",
                return_value={"ok": True, "cleanup_count": 2},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/session.json", gpt_repo=None)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_export_highlight_selection(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--export-highlight-selection",
                "/tmp/example.proxy_scan.json",
                "--output-path",
                "/tmp/highlights.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_export_highlight_selection",
                return_value={"ok": True, "manifest_path": "/tmp/highlights.json"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/example.proxy_scan.json", fused_sidecar=None, output_path="/tmp/highlights.json")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_export_highlight_selection_from_fused_sidecar(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--export-highlight-selection",
                "--fused-sidecar",
                "/tmp/example.fused_analysis.json",
                "--output-path",
                "/tmp/highlights.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_export_highlight_selection",
                return_value={"ok": True, "manifest_path": "/tmp/highlights.json", "selection_basis": "fused"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                None,
                fused_sidecar="/tmp/example.fused_analysis.json",
                output_path="/tmp/highlights.json",
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_launch_highlight_review_app(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--launch-highlight-review-app",
                "/tmp/sidecars",
                "--fixture-manifest",
                "/tmp/fixtures.json",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_launch_highlight_review_app",
                return_value={"ok": True, "launch_url": "http://127.0.0.1:7860"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with(
                "/tmp/sidecars",
                fixture_manifest="/tmp/fixtures.json",
                fixture_comparison_report=None,
                fixture_trial_batch_manifest=None,
                proxy_calibration_report=None,
                proxy_replay_report=None,
                runtime_calibration_report=None,
                runtime_replay_report=None,
                registry_path=None,
                output_path=None,
                launch=True,
            )
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_sidecar_path_uses_repo_output_dir_by_default(self) -> None:
        sidecar_path = _sidecar_path("/tmp/example.wav", "marvel_rivals", {"output_dir": "outputs/proxy_scans"})

        self.assertEqual(sidecar_path.parent.name, "marvel_rivals")
        self.assertEqual(sidecar_path.parent.parent, REPO_ROOT / "outputs" / "proxy_scans")

    def test_run_scan_vod_batch_walks_sorted_files_and_writes_report(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            bravo = root / "nested" / "bravo.mp4"
            alpha = root / "alpha.mp4"
            bravo.parent.mkdir(parents=True, exist_ok=True)
            alpha.write_bytes(b"alpha")
            bravo.write_bytes(b"bravo")

            responses = {
                str(alpha.resolve()): {
                    "ok": True,
                    "signal_count": 2,
                    "window_count": 1,
                    "windows": [{"proxy_score": 0.81, "recommended_action": "download_candidate"}],
                    "sidecar_path": str(root / "sidecars" / "alpha.proxy_scan.json"),
                    "source_results": {"audio_prepass": {"status": "ok", "signal_count": 2}},
                },
                str(bravo.resolve()): {
                    "ok": True,
                    "signal_count": 0,
                    "window_count": 0,
                    "windows": [],
                    "sidecar_path": str(root / "sidecars" / "bravo.proxy_scan.json"),
                    "source_results": {"audio_prepass": {"status": "ok", "signal_count": 0}},
                },
            }

            calls: list[str] = []

            def fake_scan(source: str | Path, game: str, chat_log: str | Path | None = None) -> dict:
                resolved = str(Path(source).resolve())
                calls.append(resolved)
                return responses[resolved]

            with patch("run.run_scan_vod", side_effect=fake_scan):
                result = run_scan_vod_batch(root, "marvel_rivals")

            self.assertTrue(result["report_path"])
            self.assertEqual(calls, [str(alpha.resolve()), str(bravo.resolve())])
            self.assertEqual(result["file_count"], 2)
            self.assertEqual(result["scanned_count"], 2)
            self.assertEqual(result["success_count"], 2)
            self.assertEqual(result["failed_count"], 0)
            self.assertEqual(result["window_count_total"], 1)
            self.assertEqual(result["download_candidate_count"], 1)
            self.assertEqual(result["inspect_count"], 0)
            self.assertEqual(result["skip_count"], 0)
            self.assertEqual(result["results"][1]["top_recommended_action"], "none")
            report_path = Path(result["report_path"])
            self.assertTrue(report_path.is_file())
            self.assertEqual(json.loads(report_path.read_text(encoding="utf-8")), result)

    def test_run_scan_vod_batch_pattern_and_limit_restrict_files(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "a.mp4").write_bytes(b"a")
            (root / "b.mov").write_bytes(b"b")
            (root / "c.mp4").write_bytes(b"c")

            calls: list[str] = []

            def fake_scan(source: str | Path, game: str, chat_log: str | Path | None = None) -> dict:
                calls.append(Path(source).name)
                return {
                    "ok": True,
                    "signal_count": 1,
                    "window_count": 1,
                    "windows": [{"proxy_score": 0.55, "recommended_action": "inspect"}],
                    "sidecar_path": str(root / "sidecars" / f"{Path(source).stem}.proxy_scan.json"),
                    "source_results": {},
                }

            with patch("run.run_scan_vod", side_effect=fake_scan):
                result = run_scan_vod_batch(root, "marvel_rivals", pattern="*.mp4", limit=1)

            self.assertEqual(calls, ["a.mp4"])
            self.assertEqual(result["file_count"], 1)
            self.assertEqual(result["inspect_count"], 1)

    def test_run_scan_vod_batch_keeps_scanning_after_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            alpha = root / "alpha.mp4"
            bravo = root / "bravo.mp4"
            alpha.write_bytes(b"alpha")
            bravo.write_bytes(b"bravo")

            def fake_scan(source: str | Path, game: str, chat_log: str | Path | None = None) -> dict:
                if Path(source).name == "alpha.mp4":
                    return {
                        "ok": False,
                        "signal_count": 0,
                        "window_count": 0,
                        "windows": [],
                        "sidecar_path": str(root / "sidecars" / "alpha.proxy_scan.json"),
                        "source_results": {"audio_prepass": {"status": "failed", "signal_count": 0}},
                    }
                return {
                    "ok": True,
                    "signal_count": 1,
                    "window_count": 1,
                    "windows": [{"proxy_score": 0.25, "recommended_action": "skip"}],
                    "sidecar_path": str(root / "sidecars" / "bravo.proxy_scan.json"),
                    "source_results": {"audio_prepass": {"status": "ok", "signal_count": 1}},
                }

            with patch("run.run_scan_vod", side_effect=fake_scan):
                result = run_scan_vod_batch(root, "marvel_rivals")

            self.assertEqual(result["success_count"], 1)
            self.assertEqual(result["failed_count"], 1)
            self.assertEqual(result["skip_count"], 1)
            self.assertEqual(result["results"][0]["ok"], False)

    def test_proxy_scan_batch_report_path_uses_repo_output_dir(self) -> None:
        report_path = _proxy_scan_batch_report_path(Path("/tmp/accepted"), "marvel_rivals", "*.mp4", 5)

        self.assertEqual(report_path.parent.name, "marvel_rivals")
        self.assertEqual(report_path.parent.parent, REPO_ROOT / "outputs" / "proxy_scan_batches")

    def test_prepare_proxy_review_selects_download_candidates_and_writes_gpt_queue_files(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            alpha_source = media_root / "alpha.mp4"
            bravo_source = media_root / "bravo.mp4"
            charlie_source = media_root / "charlie.mp4"
            media_root.mkdir(parents=True, exist_ok=True)
            alpha_source.write_bytes(b"alpha")
            bravo_source.write_bytes(b"bravo")
            charlie_source.write_bytes(b"charlie")

            _write_proxy_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.proxy_scan.json",
                game="marvel_rivals",
                source=alpha_source,
                score=0.81,
                action="download_candidate",
                sources=["audio_spike", "visual_flash_spike"],
                source_families=["audio_prepass", "visual_prepass"],
            )
            _write_proxy_sidecar(
                sidecar_root / "marvel_rivals" / "bravo.proxy_scan.json",
                game="marvel_rivals",
                source=bravo_source,
                score=0.77,
                action="download_candidate",
                sources=["audio_spike", "visual_motion_spike"],
                source_families=["audio_prepass", "visual_prepass"],
            )
            _write_proxy_sidecar(
                sidecar_root / "marvel_rivals" / "charlie.proxy_scan.json",
                game="marvel_rivals",
                source=charlie_source,
                score=0.54,
                action="inspect",
                sources=["audio_spike"],
                source_families=["audio_prepass"],
            )

            with patch.object(proxy_review_bridge, "REPO_ROOT", root):
                result = run_prepare_proxy_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                    session_name="bridge",
                )

            self.assertEqual(result["item_count"], 2)
            self.assertEqual(result["selection_action_filter"], "download_candidate")
            self.assertEqual(Path(result["manifest_path"]).parent.parent, root / "outputs" / "proxy_review_sessions")
            self.assertEqual(Path(result["items"][0]["source"]).name, "alpha.mp4")
            self.assertEqual(Path(result["items"][1]["source"]).name, "bravo.mp4")

            gpt_meta_path = Path(result["items"][0]["gpt_meta_path"])
            gpt_processed_path = Path(result["items"][0]["gpt_processed_path"])
            self.assertTrue(gpt_meta_path.is_file())
            self.assertTrue(gpt_processed_path.is_file())
            self.assertEqual(gpt_processed_path.read_bytes(), alpha_source.read_bytes())

            meta = json.loads(gpt_meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["processed_path"], str(gpt_processed_path))
            self.assertEqual(meta["selected_template_id"], "proxy_review_bridge")
            self.assertEqual(meta["scoring"]["clip_type"], "proxy_candidate")
            self.assertEqual(meta["scoring"]["highlight_score"], 81)
            self.assertTrue(meta["proxy_review_bridge"]["bridge_owned"])

    def test_prepare_proxy_review_can_select_from_batch_report(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            media_root.mkdir(parents=True, exist_ok=True)
            alpha_source = media_root / "alpha.mp4"
            bravo_source = media_root / "bravo.mp4"
            alpha_source.write_bytes(b"alpha")
            bravo_source.write_bytes(b"bravo")

            alpha_sidecar = sidecar_root / "marvel_rivals" / "alpha.proxy_scan.json"
            bravo_sidecar = sidecar_root / "marvel_rivals" / "bravo.proxy_scan.json"
            _write_proxy_sidecar(
                alpha_sidecar,
                game="marvel_rivals",
                source=alpha_source,
                score=0.79,
                action="download_candidate",
                sources=["audio_spike", "visual_motion_spike"],
                source_families=["audio_prepass", "visual_prepass"],
            )
            _write_proxy_sidecar(
                bravo_sidecar,
                game="marvel_rivals",
                source=bravo_source,
                score=0.66,
                action="inspect",
                sources=["audio_spike"],
                source_families=["audio_prepass"],
            )

            batch_report = root / "batch.json"
            batch_report.write_text(
                json.dumps(
                    {
                        "results": [
                            {"sidecar_path": str(bravo_sidecar), "top_recommended_action": "inspect"},
                            {"sidecar_path": str(alpha_sidecar), "top_recommended_action": "download_candidate"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(proxy_review_bridge, "REPO_ROOT", root):
                result = run_prepare_proxy_review(
                    "marvel_rivals",
                    batch_report=batch_report,
                    gpt_repo=gpt_repo,
                )

            self.assertEqual(result["selection_source"], str(batch_report.resolve()))
            self.assertEqual(result["item_count"], 1)
            self.assertEqual(Path(result["items"][0]["source"]).name, "alpha.mp4")

    def test_apply_proxy_review_updates_sidecars_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            media_root.mkdir(parents=True, exist_ok=True)
            alpha_source = media_root / "alpha.mp4"
            bravo_source = media_root / "bravo.mp4"
            charlie_source = media_root / "charlie.mp4"
            alpha_source.write_bytes(b"alpha")
            bravo_source.write_bytes(b"bravo")
            charlie_source.write_bytes(b"charlie")

            _write_proxy_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.proxy_scan.json",
                game="marvel_rivals",
                source=alpha_source,
                score=0.81,
                action="download_candidate",
                sources=["audio_spike", "visual_flash_spike"],
                source_families=["audio_prepass", "visual_prepass"],
            )
            _write_proxy_sidecar(
                sidecar_root / "marvel_rivals" / "bravo.proxy_scan.json",
                game="marvel_rivals",
                source=bravo_source,
                score=0.79,
                action="download_candidate",
                sources=["audio_spike", "visual_motion_spike"],
                source_families=["audio_prepass", "visual_prepass"],
            )
            _write_proxy_sidecar(
                sidecar_root / "marvel_rivals" / "charlie.proxy_scan.json",
                game="marvel_rivals",
                source=charlie_source,
                score=0.77,
                action="download_candidate",
                sources=["audio_spike", "visual_motion_spike"],
                source_families=["audio_prepass", "visual_prepass"],
            )

            with patch.object(proxy_review_bridge, "REPO_ROOT", root):
                prepared = run_prepare_proxy_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                )

                alpha_meta_path = Path(prepared["items"][0]["gpt_meta_path"])
                bravo_meta_path = Path(prepared["items"][1]["gpt_meta_path"])
                charlie_meta_path = Path(prepared["items"][2]["gpt_meta_path"])

                alpha_meta = json.loads(alpha_meta_path.read_text(encoding="utf-8"))
                alpha_final = gpt_repo / "accepted" / "marvel_rivals" / f"{alpha_meta['clip_id']}.mp4"
                alpha_final.parent.mkdir(parents=True, exist_ok=True)
                alpha_final.write_bytes(b"approved")
                alpha_meta["review_status"] = "accepted"
                alpha_meta["reviewed_at"] = "2026-04-29T12:00:00Z"
                alpha_meta["final_path"] = str(alpha_final)
                alpha_meta_path.write_text(json.dumps(alpha_meta, indent=2), encoding="utf-8")

                bravo_meta = json.loads(bravo_meta_path.read_text(encoding="utf-8"))
                bravo_final = gpt_repo / "rejected" / "marvel_rivals" / f"{bravo_meta['clip_id']}.mp4"
                bravo_final.parent.mkdir(parents=True, exist_ok=True)
                bravo_final.write_bytes(b"rejected")
                bravo_meta["review_status"] = "rejected"
                bravo_meta["reviewed_at"] = "2026-04-29T12:05:00Z"
                bravo_meta["final_path"] = str(bravo_final)
                bravo_meta_path.write_text(json.dumps(bravo_meta, indent=2), encoding="utf-8")

                first_apply = run_apply_proxy_review(prepared["manifest_path"])
                second_apply = run_apply_proxy_review(prepared["manifest_path"])

            self.assertTrue(first_apply["ok"])
            self.assertEqual(first_apply["approved_count"], 1)
            self.assertEqual(first_apply["rejected_count"], 1)
            self.assertEqual(first_apply["unreviewed_count"], 1)
            self.assertEqual(second_apply["approved_count"], 1)

            alpha_sidecar = json.loads((sidecar_root / "marvel_rivals" / "alpha.proxy_scan.json").read_text(encoding="utf-8"))
            bravo_sidecar = json.loads((sidecar_root / "marvel_rivals" / "bravo.proxy_scan.json").read_text(encoding="utf-8"))
            charlie_sidecar = json.loads((sidecar_root / "marvel_rivals" / "charlie.proxy_scan.json").read_text(encoding="utf-8"))

            self.assertEqual(alpha_sidecar["proxy_review"]["review_status"], "approved")
            self.assertEqual(bravo_sidecar["proxy_review"]["review_status"], "rejected")
            self.assertEqual(charlie_sidecar["proxy_review"]["review_status"], "unreviewed")
            self.assertEqual(alpha_sidecar["window_count"], 1)
            self.assertEqual(bravo_sidecar["windows"][0]["recommended_action"], "download_candidate")

    def test_cleanup_proxy_review_removes_generated_bridge_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            sidecar_root = root / "sidecars"
            media_root = root / "media"
            gpt_repo = root / "gpt"
            _write_gpt_review_repo(gpt_repo)

            media_root.mkdir(parents=True, exist_ok=True)
            alpha_source = media_root / "alpha.mp4"
            alpha_source.write_bytes(b"alpha")
            _write_proxy_sidecar(
                sidecar_root / "marvel_rivals" / "alpha.proxy_scan.json",
                game="marvel_rivals",
                source=alpha_source,
                score=0.81,
                action="download_candidate",
                sources=["audio_spike", "visual_flash_spike"],
                source_families=["audio_prepass", "visual_prepass"],
            )

            with patch.object(proxy_review_bridge, "REPO_ROOT", root):
                prepared = run_prepare_proxy_review(
                    "marvel_rivals",
                    sidecar_root=sidecar_root,
                    gpt_repo=gpt_repo,
                )
                item = prepared["items"][0]
                meta_path = Path(item["gpt_meta_path"])
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                final_path = gpt_repo / "accepted" / "marvel_rivals" / f"{meta['clip_id']}.mp4"
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(b"accepted")
                meta["final_path"] = str(final_path)
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                result = run_cleanup_proxy_review(prepared["manifest_path"])

            self.assertTrue(result["ok"])
            self.assertFalse(Path(item["gpt_processed_path"]).exists())
            self.assertFalse(Path(item["gpt_meta_path"]).exists())
            self.assertFalse(final_path.exists())

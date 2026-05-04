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
    main as run_main,
    run_adapt_game_schema,
    run_build_onboarding_draft,
    run_ingest_game_sources,
    run_audit_pipeline_contracts,
    run_apply_proxy_review,
    run_apply_onboarding_identity_review,
    run_cleanup_proxy_review,
    run_cleanup_onboarding_identity_review,
    run_onboard_game,
    run_publish_onboarding_batch,
    run_report_onboarding_batch,
    run_publish_onboarding_draft,
    run_prepare_onboarding_identity_review,
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
                return_value={"ok": True, "row_count": 1, "rows": []},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_report_onboarding_batch(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--report-onboarding-batch", "/tmp/onboarding", "--game", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_report_onboarding_batch",
                return_value={"ok": True, "draft_count": 1, "drafts": []},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/onboarding", game="marvel_rivals", output_path=None)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_publish_onboarding_batch(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--publish-onboarding-batch", "/tmp/onboarding", "--game", "marvel_rivals", "--apply"]
            stdout = io.StringIO()
            with patch(
                "run.run_publish_onboarding_batch",
                return_value={"ok": True, "summary": {"published": 1}},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/onboarding", game="marvel_rivals", apply=True, output_path=None)
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_validate_onboarding_publish(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--validate-onboarding-publish", "/tmp/draft"]
            stdout = io.StringIO()
            with patch(
                "run.run_validate_onboarding_publish",
                return_value={"ok": True, "can_publish": False, "readiness": "needs_binding_review"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once_with("/tmp/draft")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
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
            mock_run.assert_called_once_with("/tmp/example.proxy_scan.json", output_path="/tmp/highlights.json")
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

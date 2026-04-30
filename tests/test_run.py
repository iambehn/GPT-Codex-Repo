from __future__ import annotations

import copy
import json
import math
import shutil
import struct
import subprocess
import tempfile
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

import pipeline.proxy_review_bridge as proxy_review_bridge
from run import (
    DEFAULT_CONFIG,
    REPO_ROOT,
    _proxy_scan_batch_report_path,
    _sidecar_path,
    run_apply_proxy_review,
    run_cleanup_proxy_review,
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

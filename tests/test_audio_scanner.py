from __future__ import annotations

import math
import struct
import tempfile
import unittest
import wave
from pathlib import Path

from pipeline.audio_scanner import AudioScanError, scan_audio_source


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


class AudioScannerTests(unittest.TestCase):
    def test_scan_audio_source_emits_audio_spike_for_burst(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)
        _write_test_wave(path, [0.02] * 12 + [0.85] * 2 + [0.02] * 4)

        signals = scan_audio_source(
            path,
            {
                "sample_rate": 16000,
                "window_ms": 250,
                "rolling_baseline_windows": 8,
                "z_score_threshold": 2.5,
                "default_confidence": 0.72,
            },
        )

        self.assertTrue(signals)
        self.assertEqual(signals[0].source, "audio_spike")

    def test_scan_audio_source_stays_quiet_for_flat_audio(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)
        _write_test_wave(path, [0.02] * 18)

        signals = scan_audio_source(
            path,
            {
                "sample_rate": 16000,
                "window_ms": 250,
                "rolling_baseline_windows": 8,
                "z_score_threshold": 2.5,
                "default_confidence": 0.72,
            },
        )

        self.assertEqual(signals, [])

    def test_scan_audio_source_suppresses_intro_and_end_spikes(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)
        _write_test_wave(path, [0.85] * 2 + [0.02] * 12 + [0.85] * 2)

        signals = scan_audio_source(
            path,
            {
                "sample_rate": 16000,
                "window_ms": 250,
                "rolling_baseline_windows": 8,
                "z_score_threshold": 2.5,
                "default_confidence": 0.72,
                "suppress_initial_seconds": 1.0,
                "suppress_final_seconds": 1.0,
                "min_cluster_windows": 2,
                "min_peak_ratio": 3.0,
            },
            media_duration_seconds=4.0,
        )

        self.assertEqual(signals, [])

    def test_scan_audio_source_keeps_isolated_burst_only_when_peak_ratio_clears_threshold(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(path.unlink)
        _write_test_wave(path, [0.02] * 12 + [0.85] + [0.02] * 4)

        weak_gate = scan_audio_source(
            path,
            {
                "sample_rate": 16000,
                "window_ms": 250,
                "rolling_baseline_windows": 8,
                "z_score_threshold": 2.5,
                "default_confidence": 0.72,
                "suppress_initial_seconds": 0.0,
                "suppress_final_seconds": 0.0,
                "min_cluster_windows": 2,
                "min_peak_ratio": 50.0,
            },
        )
        strong_gate = scan_audio_source(
            path,
            {
                "sample_rate": 16000,
                "window_ms": 250,
                "rolling_baseline_windows": 8,
                "z_score_threshold": 2.5,
                "default_confidence": 0.72,
                "suppress_initial_seconds": 0.0,
                "suppress_final_seconds": 0.0,
                "min_cluster_windows": 2,
                "min_peak_ratio": 3.0,
            },
        )

        self.assertEqual(weak_gate, [])
        self.assertEqual(len(strong_gate), 1)
        self.assertEqual(strong_gate[0].source, "audio_spike")

    def test_scan_audio_source_raises_clean_error_for_missing_source(self) -> None:
        with self.assertRaises(AudioScanError):
            scan_audio_source(
                Path("/tmp/does-not-exist.wav"),
                {
                    "sample_rate": 16000,
                    "window_ms": 250,
                    "rolling_baseline_windows": 8,
                    "z_score_threshold": 2.5,
                    "default_confidence": 0.72,
                },
            )

from __future__ import annotations

import unittest

from pipeline.proxy_scanner import ProxySignal, build_proxy_windows


def _proxy_config(min_proxy_score: float = 0.10) -> dict[str, object]:
    return {
        "weights": {
            "chat_spike": 3.5,
            "audio_spike": 3.0,
            "visual_motion_spike": 2.8,
            "visual_flash_spike": 2.6,
        },
        "candidate_selection": {
            "dedupe_gap_seconds": 3,
            "merge_gap_seconds": 30,
            "audio_only_merge_gap_seconds": 8,
            "window_pre_seconds": 10,
            "window_post_seconds": 25,
            "audio_only_window_pre_seconds": 3,
            "audio_only_window_post_seconds": 6,
            "min_proxy_score": min_proxy_score,
            "max_windows": 20,
            "agreement_bonus_per_extra_source": 0.1,
            "max_agreement_bonus": 0.25,
        },
        "cost_gates": {
            "inspect_min_score": 0.40,
            "download_candidate_min_score": 0.75,
            "download_candidate_min_sources": 2,
        },
    }


class ProxyScannerTests(unittest.TestCase):
    def test_same_source_signals_dedupe_and_merge(self) -> None:
        signals = [
            ProxySignal("chat_spike", "chat_velocity", 10.0, 0.5, 0.7, "a"),
            ProxySignal("chat_spike", "chat_velocity", 11.0, 0.9, 0.8, "b"),
            ProxySignal("chat_spike", "chat_velocity", 40.0, 0.7, 0.8, "c"),
        ]
        windows = build_proxy_windows(signals, _proxy_config())
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].signal_count, 2)
        self.assertAlmostEqual(windows[0].start_seconds, 1.0)
        self.assertEqual(windows[0].recommended_action, "inspect")

    def test_distant_signals_stay_separate(self) -> None:
        signals = [
            ProxySignal("chat_spike", "chat_velocity", 10.0, 0.9, 0.8, "a"),
            ProxySignal("chat_spike", "chat_velocity", 100.0, 0.9, 0.8, "b"),
        ]
        windows = build_proxy_windows(signals, _proxy_config())
        self.assertEqual(len(windows), 2)

    def test_low_score_window_becomes_skip(self) -> None:
        signals = [ProxySignal("chat_spike", "chat_velocity", 10.0, 0.5, 0.7, "weak")]
        windows = build_proxy_windows(signals, _proxy_config())

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].recommended_action, "skip")

    def test_strong_multi_source_window_becomes_download_candidate(self) -> None:
        signals = [
            ProxySignal("chat_spike", "chat_velocity", 10.0, 1.0, 0.9, "chat"),
            ProxySignal("audio_spike", "audio_prepass", 11.0, 1.0, 0.9, "audio"),
        ]
        windows = build_proxy_windows(signals, _proxy_config())

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].recommended_action, "download_candidate")

    def test_audio_only_distant_spikes_do_not_merge_into_one_large_window(self) -> None:
        signals = [
            ProxySignal("audio_spike", "audio_prepass", 0.875, 1.0, 0.72, "intro-ish"),
            ProxySignal("audio_spike", "audio_prepass", 23.625, 0.61, 0.72, "late"),
        ]
        windows = build_proxy_windows(signals, _proxy_config(), media_duration_seconds=31.416667)

        self.assertEqual(len(windows), 2)
        self.assertAlmostEqual(windows[0].start_seconds, 0.0)
        self.assertAlmostEqual(windows[0].end_seconds, 6.875)
        self.assertAlmostEqual(windows[1].start_seconds, 20.625)
        self.assertAlmostEqual(windows[1].end_seconds, 29.625)

    def test_audio_only_window_end_is_clamped_to_media_duration(self) -> None:
        signals = [ProxySignal("audio_spike", "audio_prepass", 29.5, 1.0, 0.72, "tail")]
        windows = build_proxy_windows(signals, _proxy_config(), media_duration_seconds=31.416667)

        self.assertEqual(len(windows), 1)
        self.assertAlmostEqual(windows[0].start_seconds, 26.5)
        self.assertAlmostEqual(windows[0].end_seconds, 31.416667)

    def test_visual_only_same_family_signals_do_not_promote_to_download_candidate(self) -> None:
        signals = [
            ProxySignal("visual_flash_spike", "visual_prepass", 15.625, 1.0, 0.7, "flash"),
            ProxySignal("visual_motion_spike", "visual_prepass", 15.625, 1.0, 0.7, "motion"),
        ]

        windows = build_proxy_windows(signals, _proxy_config())

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].recommended_action, "inspect")
        self.assertEqual(windows[0].source_families, ["visual_prepass"])

    def test_mixed_family_signals_still_promote_to_download_candidate(self) -> None:
        signals = [
            ProxySignal("audio_spike", "audio_prepass", 15.5, 1.0, 0.72, "audio"),
            ProxySignal("visual_flash_spike", "visual_prepass", 15.625, 1.0, 0.7, "flash"),
        ]

        windows = build_proxy_windows(signals, _proxy_config())

        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].recommended_action, "download_candidate")
        self.assertEqual(sorted(windows[0].source_families), ["audio_prepass", "visual_prepass"])

    def test_borderline_mixed_family_window_stays_inspect_under_0_75_cutoff(self) -> None:
        signals = [
            ProxySignal("audio_spike", "audio_prepass", 15.5, 0.91, 0.72, "audio"),
            ProxySignal("visual_flash_spike", "visual_prepass", 15.625, 0.90, 0.70, "flash"),
        ]

        windows = build_proxy_windows(signals, _proxy_config())

        self.assertEqual(len(windows), 1)
        self.assertAlmostEqual(windows[0].proxy_score, 0.7435, places=4)
        self.assertEqual(windows[0].recommended_action, "inspect")

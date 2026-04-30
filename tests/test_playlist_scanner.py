from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pipeline.playlist_scanner import is_playlist_source, scan_playlist_source


class PlaylistScannerTests(unittest.TestCase):
    def test_scan_playlist_source_emits_duration_and_discontinuity_signals(self) -> None:
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
        with tempfile.NamedTemporaryFile("w", suffix=".m3u8", delete=False) as handle:
            handle.write(playlist_text)
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        signals = scan_playlist_source(
            path,
            {
                "duration_spike_ratio": 1.75,
                "variance_window_segments": 3,
                "default_confidence": 0.65,
                "discontinuity_confidence": 0.80,
            },
        )

        sources = [signal.source for signal in signals]
        self.assertIn("playlist_spike", sources)
        self.assertIn("playlist_discontinuity", sources)

    def test_scan_playlist_source_skips_non_playlist_source(self) -> None:
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as handle:
            handle.write("not a playlist")
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        self.assertFalse(is_playlist_source(path))
        self.assertEqual(scan_playlist_source(path, {}), [])

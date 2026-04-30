from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pipeline.chat_scanner import scan_chat_log
from pipeline.proxy_scanner import ProxySignal


class ChatScannerTests(unittest.TestCase):
    def test_scan_chat_log_emits_proxy_signals(self) -> None:
        log_text = "\n".join(
            [
                "[00:01:07] user1: POG",
                "[00:01:07] user2: CLIP IT",
                "[00:01:08] user3: POG",
                "[00:01:08] user4: WTF",
                "[00:01:08] user5: no way",
                "[00:01:09] user6: clip it",
                "[00:01:09] user7: pog",
                "[00:01:09] user8: insane",
            ]
        )
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as handle:
            handle.write(log_text)
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        signals = scan_chat_log(
            path,
            {
                "bucket_seconds": 5,
                "rolling_baseline_seconds": 300,
                "burst_threshold": 1.0,
                "default_confidence": 0.7,
            },
        )

        self.assertTrue(signals)
        self.assertIsInstance(signals[0], ProxySignal)
        self.assertEqual(signals[0].source, "chat_spike")

    def test_message_spam_counts_keyword_once_per_message(self) -> None:
        log_text = "[00:00:05] user1: POG POG POG POG"
        with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as handle:
            handle.write(log_text)
            path = Path(handle.name)
        self.addCleanup(path.unlink)

        signals = scan_chat_log(
            path,
            {
                "bucket_seconds": 5,
                "rolling_baseline_seconds": 5,
                "burst_threshold": 1.0,
                "default_confidence": 0.7,
            },
        )

        self.assertEqual(len(signals), 1)
        self.assertLessEqual(signals[0].strength, 1.0)


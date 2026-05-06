from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.proxy_registry import ProxyScanContext, ProxySourceDefinition, run_proxy_sources
from pipeline.proxy_scanner import ProxySignal


class ProxyRegistryTests(unittest.TestCase):
    def test_run_proxy_sources_enriches_signal_producer_source_ref_and_time_window(self) -> None:
        context = ProxyScanContext(
            source="/tmp/example.mp4",
            chat_log="/tmp/chat.log",
            media_duration_seconds=30.0,
        )

        def _emit(_context: ProxyScanContext, _config: dict[str, object]) -> list[ProxySignal]:
            return [ProxySignal("chat_spike", "chat_velocity", 12.5, 0.6, 0.7, "burst")]

        with patch(
            "pipeline.proxy_registry._SOURCE_REGISTRY",
            (ProxySourceDefinition(name="chat_velocity", scan=_emit),),
        ):
            signals, source_results = run_proxy_sources(context, {"sources": {"chat_velocity": {"enabled": True}}})

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].producer, "chat_velocity")
        self.assertEqual(signals[0].source_ref, "/tmp/chat.log")
        self.assertEqual(signals[0].start_timestamp, 12.5)
        self.assertEqual(signals[0].end_timestamp, 12.5)
        self.assertEqual(source_results["chat_velocity"]["status"], "ok")
        self.assertEqual(source_results["chat_velocity"]["signal_count"], 1)


if __name__ == "__main__":
    unittest.main()

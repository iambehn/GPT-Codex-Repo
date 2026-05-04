from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pipeline.runtime_ontology import RuntimeOntologyError, load_runtime_signal_event_ontology


class RuntimeOntologyTests(unittest.TestCase):
    def test_loader_rejects_malformed_ontology_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            starter = root / "starter_assets"
            starter.mkdir(parents=True, exist_ok=True)
            (starter / "runtime_signal_event_ontology.yaml").write_text("signal_types: {}\n", encoding="utf-8")
            with self.assertRaises(RuntimeOntologyError):
                load_runtime_signal_event_ontology(repo_root=root)

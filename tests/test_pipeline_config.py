from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pipeline.config import ConfigValidationError, DEFAULT_CONFIG, deep_merge, load_pipeline_config


class PipelineConfigLoaderTests(unittest.TestCase):
    def test_deep_merge_preserves_defaults(self) -> None:
        merged = deep_merge(
            DEFAULT_CONFIG,
            {
                "proxy_scanner": {
                    "sources": {
                        "audio_prepass": {
                            "enabled": False,
                        }
                    }
                }
            },
        )
        self.assertFalse(merged["proxy_scanner"]["sources"]["audio_prepass"]["enabled"])
        self.assertEqual(
            merged["proxy_scanner"]["sources"]["audio_prepass"]["sample_rate"],
            DEFAULT_CONFIG["proxy_scanner"]["sources"]["audio_prepass"]["sample_rate"],
        )

    def test_load_pipeline_config_normalizes_legacy_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            (repo_root / "config.yaml").write_text(
                "\n".join(
                    [
                        "proxy_scanner:",
                        "  signals:",
                        "    audio_prepass:",
                        "      enabled: false",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            loaded = load_pipeline_config(repo_root)

        self.assertEqual(loaded.warnings[0]["status"], "legacy_proxy_signals_config")
        self.assertIn("sources", loaded.config["proxy_scanner"])
        self.assertNotIn("signals", loaded.config["proxy_scanner"])
        self.assertFalse(loaded.config["proxy_scanner"]["sources"]["audio_prepass"]["enabled"])

    def test_explicit_sources_override_legacy_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            (repo_root / "config.yaml").write_text(
                "\n".join(
                    [
                        "proxy_scanner:",
                        "  signals:",
                        "    audio_prepass:",
                        "      enabled: true",
                        "  sources:",
                        "    audio_prepass:",
                        "      enabled: false",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            loaded = load_pipeline_config(repo_root)

        self.assertFalse(loaded.config["proxy_scanner"]["sources"]["audio_prepass"]["enabled"])

    def test_env_placeholder_is_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            (repo_root / "config.yaml").write_text(
                "\n".join(
                    [
                        "proxy_scanner:",
                        "  sidecar:",
                        "    output_dir: ${ENV:PROXY_OUTPUT_DIR}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            loaded = load_pipeline_config(repo_root, env={"PROXY_OUTPUT_DIR": "outputs/test_sidecars"})

        self.assertEqual(loaded.config["proxy_scanner"]["sidecar"]["output_dir"], "outputs/test_sidecars")

    def test_missing_env_placeholder_raises_structured_error(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            (repo_root / "config.yaml").write_text(
                "\n".join(
                    [
                        "proxy_scanner:",
                        "  sidecar:",
                        "    output_dir: ${MISSING_OUTPUT_DIR}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ConfigValidationError) as ctx:
                load_pipeline_config(repo_root, env={})

        self.assertEqual(ctx.exception.errors[0]["env_var"], "MISSING_OUTPUT_DIR")

    def test_invalid_type_raises_structured_error(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            repo_root = Path(tempdir)
            (repo_root / "config.yaml").write_text(
                "\n".join(
                    [
                        "proxy_scanner:",
                        "  candidate_selection:",
                        "    max_windows: wrong",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ConfigValidationError) as ctx:
                load_pipeline_config(repo_root)

        self.assertEqual(ctx.exception.errors[0]["path"], "proxy_scanner.candidate_selection.max_windows")
        self.assertEqual(ctx.exception.errors[0]["expected"], "int")

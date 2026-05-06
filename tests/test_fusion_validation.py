from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pipeline.fusion_validation import (
    _validate_validation_report_contract,
    _validate_replay_report_contract,
    replay_fusion_rules,
    replay_runtime_event_rules,
    replay_template_thresholds,
    validate_fusion_goldset,
)
from run import main as run_main
from run import run_replay_fusion_rules, run_replay_runtime_event_rules, run_replay_template_thresholds, run_validate_fusion_goldset


def _runtime_sidecar() -> dict[str, object]:
    return {
        "schema_version": "runtime_analysis_v1",
        "analysis_id": "runtime-1",
        "ok": True,
        "game": "marvel_rivals",
        "source": "clip-1.mp4",
        "matcher": {
            "confirmed_detections": [
                {
                    "asset_id": "marvel_rivals.double_kill.medal_icon",
                    "asset_family": "medal_icon",
                    "roi_ref": "center_badge",
                    "first_timestamp": 4.95,
                    "last_timestamp": 5.15,
                    "event_row_id": "double_kill",
                }
            ]
        },
        "events": {
            "rows": [
                {
                    "event_type": "medal_seen",
                    "timestamp": 5.05,
                    "start_timestamp": 4.95,
                    "end_timestamp": 5.15,
                    "event_row_id": "double_kill",
                }
            ]
        },
    }


def _runtime_sidecar_with_detections(
    detections: list[dict[str, object]],
    *,
    runtime_events: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": "runtime_analysis_v1",
        "analysis_id": "runtime-1",
        "ok": True,
        "game": "marvel_rivals",
        "source": "clip-1.mp4",
        "matcher": {"confirmed_detections": detections},
        "events": {
            "rows": runtime_events
            if runtime_events is not None
            else [
                {
                    "event_type": "medal_seen",
                    "timestamp": 5.05,
                    "start_timestamp": 4.95,
                    "end_timestamp": 5.15,
                    "event_row_id": "double_kill",
                }
            ]
        },
    }


def _fused_sidecar(
    *,
    gate_status: str = "confirmed",
    confidence: float = 0.95,
    end_ts: float = 6.6,
    synergy_applied: bool = False,
    minimum_required_signals_met: bool = True,
) -> dict[str, object]:
    return {
        "schema_version": "fused_analysis_v1",
        "fusion_id": "fusion-1",
        "ok": True,
        "game": "marvel_rivals",
        "source": "clip-1.mp4",
        "fused_events": [
            {
                "event_id": "event-1",
                "event_type": "medal_seen",
                "start_timestamp": 4.95,
                "end_timestamp": 5.15,
                "timestamp": 5.05,
                "gate_status": gate_status,
                "confidence": confidence,
                "final_score": confidence,
                "synergy_applied": synergy_applied,
                "synergy_multiplier": 1.15 if synergy_applied else 1.0,
                "minimum_required_signals_met": minimum_required_signals_met,
                "suggested_start_timestamp": 3.0,
                "suggested_end_timestamp": end_ts,
                "metadata": {
                    "event_row_id": "double_kill",
                    "matched_signal_types": ["medal_visibility", "chat_spike"],
                },
            }
        ],
    }


def _gold_manifest() -> dict[str, object]:
    return {
        "schema_version": "fusion_goldset_clip_v1",
        "clip_id": "clip-1",
        "game": "marvel_rivals",
        "source": "clip-1.mp4",
        "coverage_tags": ["medal_heavy", "gated_confirmation", "synergy_positive"],
        "tolerances": {
            "timestamp_tolerance_seconds": 0.25,
            "boundary_tolerance_seconds": 0.5,
        },
        "expected_detections": [
            {
                "asset_family": "medal_icon",
                "roi_ref": "center_badge",
                "event_row_id": "double_kill",
                "timestamp": 5.05,
            }
        ],
        "expected_runtime_events": [
            {
                "event_type": "medal_seen",
                "event_row_id": "double_kill",
                "timestamp": 5.05,
            }
        ],
        "expected_fused_events": [
            {
                "event_type": "medal_seen",
                "event_row_id": "double_kill",
                "gate_status": "confirmed",
                "synergy_expected": True,
                "minimum_required_signals_met": True,
                "required_signal_types": ["chat_spike", "medal_visibility"],
                "timestamp": 5.05,
            }
        ],
        "expected_boundaries": [
            {
                "event_type": "medal_seen",
                "gate_status": "confirmed",
                "event_row_id": "double_kill",
                "expected_start_timestamp": 3.0,
                "expected_end_timestamp": 6.6,
            }
        ],
    }


class FusionValidationTests(unittest.TestCase):
    def test_validate_fusion_goldset_reports_metrics_from_gold_manifest(self) -> None:
        def clip_runner(*args, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "ok": True,
                "runtime": _runtime_sidecar(),
                "fused": _fused_sidecar(synergy_applied=True),
            }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            result = validate_fusion_goldset(root, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["validated_clip_count"], 1)
        self.assertEqual(result["detection_metrics"]["recall"], 1.0)
        self.assertEqual(result["runtime_event_metrics"]["recall"], 1.0)
        self.assertEqual(result["fusion_metrics"]["recall"], 1.0)
        self.assertEqual(result["fusion_metrics"]["synergy_applied_accuracy"], 1.0)
        self.assertEqual(result["boundary_metrics"]["within_tolerance_rate"], 1.0)
        self.assertEqual(result["detection_diagnostics"]["misses_by_asset_family"], {})
        self.assertEqual(result["fusion_diagnostics"]["required_signal_coverage_failures"], {})
        self.assertEqual(result["runtime_diagnostics"]["misses_by_asset_family"], {})
        self.assertEqual(result["clip_summaries"][0]["failed_first"], "none")
        self.assertIn("medal_heavy", result["coverage_summary"]["clips_by_behavior"])
        self.assertEqual(result["release_gate_summary"]["status"], "pass")
        self.assertEqual(result["release_gate_summary"]["blocking_reasons"], [])

    def test_validate_fusion_goldset_resolves_relative_sources_through_media_root(self) -> None:
        captured: dict[str, object] = {}

        def clip_runner(source, game, **kwargs):  # type: ignore[no-untyped-def]
            captured["source"] = source
            captured["game"] = game
            captured["kwargs"] = kwargs
            return {
                "ok": True,
                "runtime": _runtime_sidecar(),
                "fused": _fused_sidecar(synergy_applied=True),
            }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media_root = root / "media"
            media_root.mkdir()
            clip_path = media_root / "clip-1.mp4"
            clip_path.write_bytes(b"clip")
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            result = validate_fusion_goldset(root, clip_runner=clip_runner, game="marvel_rivals", media_root=media_root)

        self.assertTrue(result["ok"])
        self.assertEqual(captured["source"], str(clip_path.resolve()))
        self.assertEqual(result["media_root"], str(media_root.resolve()))

    def test_validate_fusion_goldset_fails_cleanly_for_invalid_media_root(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            result = validate_fusion_goldset(
                root,
                clip_runner=lambda *args, **kwargs: {"ok": True},
                game="marvel_rivals",
                media_root=root / "missing-media",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_media_root")

    def test_validate_fusion_goldset_reports_unresolved_relative_source(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            media_root = root / "media"
            media_root.mkdir()
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            result = validate_fusion_goldset(
                root,
                clip_runner=lambda *args, **kwargs: {"ok": True},
                game="marvel_rivals",
                media_root=media_root,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["warnings"][0]["reason"], "unresolved_gold_manifest_source")

    def test_validate_fusion_goldset_rejects_malformed_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "bad.fusion_goldset.json").write_text("{bad-json", encoding="utf-8")
            result = validate_fusion_goldset(root, clip_runner=lambda *args, **kwargs: {"ok": True}, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["skipped_clip_count"], 1)
        self.assertEqual(result["warnings"][0]["reason"], "malformed_gold_manifest")

    def test_validate_fusion_goldset_rejects_unknown_ontology_fused_event_type(self) -> None:
        def clip_runner(*args, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "ok": True,
                "runtime": _runtime_sidecar(),
                "fused": _fused_sidecar(),
            }

        manifest = _gold_manifest()
        manifest["expected_fused_events"][0]["event_type"] = "unknown_event"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            result = validate_fusion_goldset(root, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["warnings"][0]["reason"], "invalid_gold_manifest_fused_event_type")

    def test_validate_fusion_goldset_release_gate_fails_on_fusion_regression(self) -> None:
        def clip_runner(*args, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "ok": True,
                "runtime": _runtime_sidecar(),
                "fused": _fused_sidecar(gate_status="ambiguous", synergy_applied=False, minimum_required_signals_met=False),
            }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            result = validate_fusion_goldset(root, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["release_gate_summary"]["status"], "fail")
        self.assertIn("fusion_metrics_below_gate", result["release_gate_summary"]["blocking_reasons"])
        self.assertIn("fusion", result["release_gate_summary"]["failing_layers"])

    def test_validate_validation_report_contract_rejects_inconsistent_counts(self) -> None:
        report = {
            "ok": True,
            "status": "ok",
            "goldset_root": "/tmp/goldset",
            "scanned_clip_count": 1,
            "validated_clip_count": 2,
            "skipped_clip_count": 0,
            "detection_metrics": {},
            "runtime_event_metrics": {},
            "fusion_metrics": {},
            "boundary_metrics": {},
            "detection_diagnostics": {},
            "runtime_diagnostics": {},
            "fusion_diagnostics": {},
            "boundary_diagnostics": {},
            "clip_summaries": [],
            "coverage_summary": {},
            "per_clip_results": [],
            "failure_buckets": {},
            "warnings": [],
            "release_gate_summary": {
                "status": "fail",
                "validated_clip_count": 2,
                "blocking_reasons": ["no_validated_clips"],
                "failed_clip_count": 0,
                "failing_layers": {},
                "coverage_gaps": [],
            },
        }

        with self.assertRaisesRegex(ValueError, "validated_clip_count exceeds scanned_clip_count"):
            _validate_validation_report_contract(report)

    def test_replay_fusion_rules_prefers_trial_when_boundary_and_gate_quality_improve(self) -> None:
        def clip_runner(source, game, trial_rules_path=None, **kwargs):  # type: ignore[no-untyped-def]
            del source, game, kwargs
            return {
                "ok": True,
                "runtime": _runtime_sidecar(),
                "fused": _fused_sidecar(
                    gate_status="confirmed" if trial_rules_path else "ambiguous",
                    confidence=0.98 if trial_rules_path else 0.7,
                    end_ts=6.6 if trial_rules_path else 5.4,
                    synergy_applied=bool(trial_rules_path),
                    minimum_required_signals_met=True,
                ),
            }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            trial_rules = root / "trial.yaml"
            trial_rules.write_text("rules: []\n", encoding="utf-8")
            result = replay_fusion_rules(root, trial_rules, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["recommendation"]["decision"], "prefer_trial")
        self.assertGreater(result["comparison"]["delta"]["gate_status_accuracy_delta"], 0.0)
        self.assertGreater(result["comparison"]["delta"]["synergy_applied_accuracy_delta"], 0.0)
        self.assertGreater(result["comparison"]["delta"]["boundary_within_tolerance_rate_delta"], 0.0)
        self.assertIs(result["recommendation"]["supporting_metrics"], result["comparison"]["delta"])

    def test_replay_template_thresholds_prefers_trial_when_matcher_improves_without_regression(self) -> None:
        def clip_runner(source, game, trial_template_overrides_path=None, **kwargs):  # type: ignore[no-untyped-def]
            del source, game, kwargs
            if trial_template_overrides_path:
                runtime = _runtime_sidecar_with_detections(
                    [
                        {
                            "asset_id": "marvel_rivals.double_kill.medal_icon",
                            "asset_family": "medal_icon",
                            "roi_ref": "center_badge",
                            "first_timestamp": 4.95,
                            "last_timestamp": 5.15,
                            "event_row_id": "double_kill",
                        }
                    ]
                )
            else:
                runtime = _runtime_sidecar_with_detections([])
            return {
                "ok": True,
                "runtime": runtime,
                "fused": _fused_sidecar(synergy_applied=True),
            }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            trial_templates = root / "trial_templates.yaml"
            trial_templates.write_text(
                "\n".join(
                    [
                        "templates:",
                        "  marvel_rivals.double_kill.medal_icon:",
                        "    threshold: 0.82",
                        "    temporal_window: 2",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = replay_template_thresholds(root, trial_templates, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["recommendation"]["decision"], "prefer_trial")
        self.assertGreater(result["comparison"]["delta"]["detection_recall_delta"], 0.0)
        self.assertGreaterEqual(result["comparison"]["trial"]["detection_metrics"]["precision"], 1.0)

    def test_replay_template_thresholds_keeps_current_when_trial_overfires(self) -> None:
        def clip_runner(source, game, trial_template_overrides_path=None, **kwargs):  # type: ignore[no-untyped-def]
            del source, game, kwargs
            if trial_template_overrides_path:
                runtime = _runtime_sidecar_with_detections(
                    [
                        {
                            "asset_id": "marvel_rivals.double_kill.medal_icon",
                            "asset_family": "medal_icon",
                            "roi_ref": "center_badge",
                            "first_timestamp": 4.95,
                            "last_timestamp": 5.15,
                            "event_row_id": "double_kill",
                        },
                        {
                            "asset_id": "marvel_rivals.noise.medal_icon",
                            "asset_family": "medal_icon",
                            "roi_ref": "center_badge",
                            "first_timestamp": 8.0,
                            "last_timestamp": 8.2,
                            "event_row_id": "noise",
                        },
                    ]
                )
                fused = {
                    **_fused_sidecar(synergy_applied=True),
                    "fused_events": [
                        _fused_sidecar(synergy_applied=True)["fused_events"][0],
                        {
                            "event_id": "event-noise",
                            "event_type": "medal_seen",
                            "start_timestamp": 8.0,
                            "end_timestamp": 8.2,
                            "timestamp": 8.1,
                            "gate_status": "confirmed",
                            "confidence": 0.88,
                            "final_score": 0.88,
                            "synergy_applied": False,
                            "synergy_multiplier": 1.0,
                            "minimum_required_signals_met": False,
                            "suggested_start_timestamp": 7.0,
                            "suggested_end_timestamp": 8.6,
                            "metadata": {
                                "event_row_id": "noise",
                                "matched_signal_types": ["medal_visibility"],
                            },
                        },
                    ],
                }
            else:
                runtime = _runtime_sidecar_with_detections(
                    [
                        {
                            "asset_id": "marvel_rivals.double_kill.medal_icon",
                            "asset_family": "medal_icon",
                            "roi_ref": "center_badge",
                            "first_timestamp": 4.95,
                            "last_timestamp": 5.15,
                            "event_row_id": "double_kill",
                        }
                    ]
                )
                fused = _fused_sidecar(synergy_applied=True)
            return {"ok": True, "runtime": runtime, "fused": fused}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            trial_templates = root / "trial_templates.yaml"
            trial_templates.write_text(
                "\n".join(
                    [
                        "templates:",
                        "  marvel_rivals.double_kill.medal_icon:",
                        "    threshold: 0.6",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            result = replay_template_thresholds(root, trial_templates, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["recommendation"]["decision"], "keep_current")
        self.assertLess(result["comparison"]["delta"]["detection_precision_delta"], 0.0)

    def test_replay_template_thresholds_rejects_invalid_trial_file(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = replay_template_thresholds(
                root,
                root / "missing-trial.yaml",
                clip_runner=lambda *args, **kwargs: {"ok": True},
                game="marvel_rivals",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_trial_templates")

    def test_replay_runtime_event_rules_prefers_trial_when_runtime_mapping_improves(self) -> None:
        def clip_runner(source, game, trial_runtime_rule_overrides_path=None, **kwargs):  # type: ignore[no-untyped-def]
            del source, game, kwargs
            if trial_runtime_rule_overrides_path:
                runtime = _runtime_sidecar_with_detections(
                    [
                        {
                            "asset_id": "marvel_rivals.double_kill.medal_icon",
                            "asset_family": "medal_icon",
                            "roi_ref": "center_badge",
                            "first_timestamp": 4.95,
                            "last_timestamp": 5.15,
                            "event_row_id": "double_kill",
                        }
                    ]
                )
            else:
                runtime = _runtime_sidecar_with_detections(
                    [
                        {
                            "asset_id": "marvel_rivals.double_kill.medal_icon",
                            "asset_family": "medal_icon",
                            "roi_ref": "center_badge",
                            "first_timestamp": 4.95,
                            "last_timestamp": 5.15,
                            "event_row_id": "double_kill",
                        }
                    ],
                    runtime_events=[],
                )
            return {"ok": True, "runtime": runtime, "fused": _fused_sidecar(synergy_applied=True)}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            trial_rules = root / "trial_runtime_rules.yaml"
            trial_rules.write_text("event_mappings:\n  medal_icon:\n    collapse_strategy: per_detection\n", encoding="utf-8")
            result = replay_runtime_event_rules(root, trial_rules, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["recommendation"]["decision"], "prefer_trial")
        self.assertGreater(result["comparison"]["delta"]["runtime_recall_delta"], 0.0)

    def test_replay_runtime_event_rules_keeps_current_when_trial_hurts_fusion(self) -> None:
        def clip_runner(source, game, trial_runtime_rule_overrides_path=None, **kwargs):  # type: ignore[no-untyped-def]
            del source, game, kwargs
            runtime = _runtime_sidecar()
            fused = _fused_sidecar(synergy_applied=True)
            if trial_runtime_rule_overrides_path:
                fused = {
                    **fused,
                    "fused_events": [],
                }
            return {"ok": True, "runtime": runtime, "fused": fused}

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            trial_rules = root / "trial_runtime_rules.yaml"
            trial_rules.write_text("event_mappings:\n  medal_icon:\n    event_timestamp_mode: end\n", encoding="utf-8")
            result = replay_runtime_event_rules(root, trial_rules, clip_runner=clip_runner, game="marvel_rivals")

        self.assertTrue(result["ok"])
        self.assertEqual(result["recommendation"]["decision"], "keep_current")
        self.assertLess(result["comparison"]["delta"]["fusion_recall_delta"], 0.0)

    def test_replay_runtime_event_rules_rejects_invalid_trial_file(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            result = replay_runtime_event_rules(
                root,
                root / "missing-runtime-trial.yaml",
                clip_runner=lambda *args, **kwargs: {"ok": True},
                game="marvel_rivals",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_trial_runtime_rules")

    def test_validate_replay_report_contract_rejects_missing_recommendation_field(self) -> None:
        report = {
            "ok": True,
            "status": "ok",
            "goldset_root": "/tmp/goldset",
            "trial_name": "trial-a",
            "comparison": {
                "current": {},
                "trial": {},
                "delta": {},
                "per_clip_comparisons": [],
            },
            "recommendation": {
                "decision": "prefer_trial",
                "reason": "improved",
                "supporting_metrics": {},
                "data_quality_notes": [],
            },
            "warnings": [],
        }

        with self.assertRaisesRegex(ValueError, "recommendation missing fields: follow_up"):
            _validate_replay_report_contract(report)

    def test_validate_fusion_goldset_writes_output_and_debug_bundle(self) -> None:
        def clip_runner(*args, **kwargs):  # type: ignore[no-untyped-def]
            return {
                "ok": True,
                "runtime": _runtime_sidecar(),
                "fused": _fused_sidecar(synergy_applied=True),
            }

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            out = root / "out"
            report_path = out / "report.json"
            debug_dir = out / "debug"
            (root / "clip-1.fusion_goldset.json").write_text(json.dumps(_gold_manifest(), indent=2), encoding="utf-8")
            result = validate_fusion_goldset(
                root,
                clip_runner=clip_runner,
                game="marvel_rivals",
                output_path=report_path,
                debug_output_dir=debug_dir,
            )

            self.assertTrue(result["ok"])
            self.assertTrue(report_path.is_file())
            self.assertTrue((debug_dir / "fusion_goldset_validation_report.json").is_file())
            self.assertTrue((debug_dir / "per_clip_results.csv").is_file())
            self.assertTrue((debug_dir / "warnings.json").is_file())

    def test_run_validate_fusion_goldset_invalid_root_returns_error(self) -> None:
        result = run_validate_fusion_goldset("/tmp/does-not-exist-fusion-goldset")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_goldset_root")

    def test_run_replay_template_thresholds_invalid_root_returns_error(self) -> None:
        result = run_replay_template_thresholds(
            "/tmp/does-not-exist-fusion-goldset",
            "/tmp/trial-templates.yaml",
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_goldset_root")

    def test_run_replay_runtime_event_rules_invalid_root_returns_error(self) -> None:
        result = run_replay_runtime_event_rules(
            "/tmp/does-not-exist-fusion-goldset",
            "/tmp/trial-runtime-rules.yaml",
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "invalid_goldset_root")

    def test_cli_routes_to_validate_fusion_goldset(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = ["run.py", "--validate-fusion-goldset", "/tmp/goldset", "--game", "marvel_rivals"]
            stdout = io.StringIO()
            with patch(
                "run.run_validate_fusion_goldset",
                return_value={"ok": True, "status": "ok", "validated_clip_count": 1},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            self.assertIsNone(mock_run.call_args.kwargs.get("media_root"))
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_replay_fusion_rules(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--replay-fusion-rules",
                "/tmp/goldset",
                "--trial-rules",
                "/tmp/trial.yaml",
                "--game",
                "marvel_rivals",
                "--media-root",
                "/tmp/media",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_replay_fusion_rules",
                return_value={"ok": True, "status": "ok", "trial_name": "trial"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            self.assertEqual(mock_run.call_args.kwargs.get("media_root"), "/tmp/media")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_replay_template_thresholds(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--replay-template-thresholds",
                "/tmp/goldset",
                "--trial-templates",
                "/tmp/trial-templates.yaml",
                "--game",
                "marvel_rivals",
                "--media-root",
                "/tmp/media",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_replay_template_thresholds",
                return_value={"ok": True, "status": "ok", "trial_name": "template-trial"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            self.assertEqual(mock_run.call_args.kwargs.get("media_root"), "/tmp/media")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

    def test_cli_routes_to_replay_runtime_event_rules(self) -> None:
        original_argv = sys.argv
        try:
            sys.argv = [
                "run.py",
                "--replay-runtime-event-rules",
                "/tmp/goldset",
                "--trial-runtime-rules",
                "/tmp/trial-runtime-rules.yaml",
                "--game",
                "marvel_rivals",
                "--media-root",
                "/tmp/media",
            ]
            stdout = io.StringIO()
            with patch(
                "run.run_replay_runtime_event_rules",
                return_value={"ok": True, "status": "ok", "trial_name": "runtime-trial"},
            ) as mock_run:
                with redirect_stdout(stdout):
                    exit_code = run_main()
            self.assertEqual(exit_code, 0)
            mock_run.assert_called_once()
            self.assertEqual(mock_run.call_args.kwargs.get("media_root"), "/tmp/media")
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
        finally:
            sys.argv = original_argv

from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from pipeline.chat_scanner import scan_chat_log
from pipeline.clip_registry import query_clip_registry, refresh_clip_registry
from pipeline.contract_audit import audit_pipeline_contracts
from pipeline.event_mapper import EventMapperError, load_runtime_rule_trial_overrides, map_roi_events
from pipeline.evaluation_fixtures import load_evaluation_fixture_manifest
from pipeline.fixture_source_manifest import load_fixture_source_manifest
from pipeline.fusion_analysis import FusionAnalysisError, fuse_analysis, load_proxy_sidecar, load_runtime_sidecar
from pipeline.fused_export import export_fused_analysis
from pipeline.fused_review_bridge import apply_fused_review, cleanup_fused_review, prepare_fused_review
from pipeline.fusion_validation import (
    load_sidecar_index,
    replay_fusion_rules,
    replay_runtime_event_rules,
    replay_template_thresholds,
    validate_fusion_goldset,
)
from pipeline.fixture_sidecar_comparison import compare_fixture_sidecars
from pipeline.game_pack import init_game_pack, list_games, load_game_pack, validate_game_pack
from pipeline.game_onboarding import (
    OnboardingSource,
    adapt_game_schema,
    build_onboarding_draft,
    ingest_onboarding_sources,
    onboard_game_from_manifest,
    publish_onboarding_draft,
)
from pipeline.onboarding_batch_publish import publish_onboarding_batch
from pipeline.onboarding_identity_review_bridge import (
    apply_onboarding_identity_review,
    cleanup_onboarding_identity_review,
    prepare_onboarding_identity_review,
)
from pipeline.onboarding_publish_readiness import validate_onboarding_publish
from pipeline.onboarding_report import summarize_onboarding_batch
from pipeline.media_probe import probe_media_duration
from pipeline.proxy_registry import ProxyScanContext, run_proxy_sources
from pipeline.proxy_calibration import calibrate_proxy_review
from pipeline.proxy_replay_viewer import render_proxy_replay_viewer
from pipeline.proxy_review_bridge import apply_proxy_review, cleanup_proxy_review, prepare_proxy_review
from pipeline.proxy_scanner import build_proxy_windows
from pipeline.proxy_tuning import replay_proxy_scoring
from pipeline.replay_viewer import render_replay_viewer
from pipeline.highlight_review_app import launch_highlight_review_app
from pipeline.highlight_selection_export import export_highlight_selection
from pipeline.roi_matcher import RoiMatcherError, match_roi_templates
from pipeline.roi_matcher import check_roi_runtime, list_pack_templates, load_template_trial_overrides, validate_published_pack
from pipeline.runtime_calibration import calibrate_runtime_review
from pipeline.runtime_export import export_runtime_analysis
from pipeline.runtime_promotion import promote_runtime_scoring
from pipeline.runtime_rollback import rollback_runtime_scoring
from pipeline.runtime_analysis import RuntimeAnalysisError, analyze_roi_runtime
from pipeline.runtime_review_bridge import apply_runtime_review, cleanup_runtime_review, prepare_runtime_review
from pipeline.runtime_tuning import replay_runtime_scoring
from pipeline.simple_yaml import load_yaml_file
from pipeline.training_export import export_training_data
from pipeline.unified_replay_viewer import render_unified_replay_viewer
from pipeline.wiki_enrichment import WikiFetchError, WikiSource, enrich_game_from_sources, enrich_game_from_wiki


REPO_ROOT = Path(__file__).resolve().parent
PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
FIXTURE_TRIAL_RUN_SCHEMA_VERSION = "fixture_trial_run_v1"
FIXTURE_TRIAL_BATCH_SCHEMA_VERSION = "fixture_trial_batch_v1"
DEFAULT_FIXTURE_TRIAL_OUTPUT_ROOT = REPO_ROOT / "outputs" / "fixture_trials"
DEFAULT_FIXTURE_TRIAL_BATCH_OUTPUT_ROOT = REPO_ROOT / "outputs" / "fixture_trial_batches"
FIXTURE_TRIAL_PRESETS = {
    "baseline": {"proposal_backend": "transnetv2", "asr_backend": "whisper"},
    "pyscenedetect": {"proposal_backend": "pyscenedetect", "asr_backend": "whisper"},
    "distil-whisper": {"proposal_backend": "transnetv2", "asr_backend": "distil_whisper"},
    "cheap-stage-combined": {"proposal_backend": "pyscenedetect", "asr_backend": "distil_whisper"},
}
DEFAULT_CONFIG = {
    "proxy_scanner": {
        "sources": {
            "chat_velocity": {
                "enabled": True,
                "bucket_seconds": 5,
                "rolling_baseline_seconds": 300,
                "burst_threshold": 3.0,
                "default_confidence": 0.70,
            },
            "playlist_hls": {
                "enabled": True,
                "duration_spike_ratio": 1.75,
                "variance_window_segments": 3,
                "default_confidence": 0.65,
                "discontinuity_confidence": 0.80,
            },
            "audio_prepass": {
                "enabled": True,
                "sample_rate": 16000,
                "window_ms": 250,
                "rolling_baseline_windows": 20,
                "z_score_threshold": 3.0,
                "default_confidence": 0.72,
                "suppress_initial_seconds": 1.0,
                "suppress_final_seconds": 1.0,
                "min_cluster_windows": 2,
                "min_peak_ratio": 3.0,
            },
            "visual_prepass": {
                "enabled": True,
                "sample_fps": 4.0,
                "default_confidence": 0.70,
                "rolling_baseline_frames": 12,
                "motion_z_score_threshold": 2.8,
                "flash_z_score_threshold": 3.2,
                "suppress_initial_seconds": 1.0,
                "suppress_final_seconds": 1.0,
                "min_cluster_frames": 2,
            },
            "hf_multimodal": {
                "enabled": False,
                "shortlist_count": 5,
                "generic_queries": [
                    "highlight moment",
                    "clutch play",
                    "high action combat",
                    "objective swing",
                ],
                "transcript_keywords": [
                    "ace",
                    "clutch",
                    "crazy",
                    "huge",
                    "insane",
                    "lets go",
                    "no way",
                    "team wipe",
                    "wow",
                ],
                "stage_weights": {
                    "proposal": 0.35,
                    "transcript": 0.20,
                    "semantic": 0.25,
                    "novelty": 0.20,
                },
                "signal_thresholds": {
                    "proposal": 0.55,
                    "transcript": 0.60,
                    "semantic": 0.60,
                    "novelty": 0.60,
                    "rerank": 0.65,
                },
                "components": {
                    "shot_detector": {
                        "enabled": True,
                        "model_id": "georgesung/shot-boundary-detection-transnet-v2",
                        "revision": "main",
                        "execution_mode": "local",
                        "runtime_options": {
                            "proposal_backend": "transnetv2",
                            "device": "auto",
                            "threshold": 0.5,
                        },
                    },
                    "asr": {
                        "enabled": True,
                        "model_id": "openai/whisper-large-v3-turbo",
                        "revision": "main",
                        "execution_mode": "local",
                        "runtime_options": {
                            "asr_backend": "whisper",
                            "device": "auto",
                            "sample_rate": 16000,
                            "chunk_length_s": 30,
                            "batch_size": 8,
                        },
                    },
                    "semantic": {
                        "enabled": True,
                        "model_id": "microsoft/xclip-base-patch32",
                        "revision": "main",
                        "execution_mode": "local",
                        "runtime_options": {
                            "device": "auto",
                            "frame_count": 8,
                        },
                    },
                    "keyframes": {
                        "enabled": True,
                        "model_id": "google/siglip-so400m-patch14-384",
                        "revision": "main",
                        "execution_mode": "local",
                        "runtime_options": {
                            "device": "auto",
                            "cluster_similarity_threshold": 0.92,
                        },
                    },
                    "reranker": {
                        "enabled": True,
                        "model_id": "HuggingFaceTB/SmolVLM2-2.2B-Instruct",
                        "revision": "main",
                        "execution_mode": "local",
                        "runtime_options": {
                            "device": "auto",
                            "frames_per_candidate": 3,
                            "max_new_tokens": 96,
                            "temperature": 0.0,
                        },
                    },
                },
            },
        },
        "weights": {
            "chat_spike": 3.5,
            "playlist_spike": 2.5,
            "playlist_discontinuity": 2.0,
            "audio_spike": 3.0,
            "visual_motion_spike": 2.8,
            "visual_flash_spike": 2.6,
            "hf_shot_boundary": 2.4,
            "hf_transcript_salience": 2.2,
            "hf_semantic_match": 2.6,
            "hf_keyframe_novelty": 2.0,
            "hf_rerank_highlight": 3.2,
        },
        "candidate_selection": {
            "dedupe_gap_seconds": 3,
            "merge_gap_seconds": 30,
            "audio_only_merge_gap_seconds": 8,
            "window_pre_seconds": 10,
            "window_post_seconds": 25,
            "audio_only_window_pre_seconds": 3,
            "audio_only_window_post_seconds": 6,
            "min_proxy_score": 0.30,
            "max_windows": 20,
            "agreement_bonus_per_extra_source": 0.10,
            "max_agreement_bonus": 0.25,
        },
        "cost_gates": {
            "inspect_min_score": 0.40,
            "download_candidate_min_score": 0.75,
            "download_candidate_min_sources": 2,
        },
        "sidecar": {
            "output_dir": "outputs/proxy_scans",
        },
    },
    "runtime_analysis": {
        "scoring": {
            "event_weights": {
                "medal_seen": 0.45,
                "ability_seen": 0.18,
                "pov_character_identified": 0.08,
            },
            "event_caps": {
                "medal_seen": 2,
                "ability_seen": 3,
                "pov_character_identified": 1,
            },
            "detection_support_weight": 0.03,
            "max_detection_support": 0.12,
            "action_thresholds": {
                "inspect": 0.25,
                "highlight_candidate": 0.60,
            },
        }
    },
}


def load_config() -> dict[str, Any]:
    path = REPO_ROOT / "config.yaml"
    if not path.exists():
        return _normalize_config(DEFAULT_CONFIG)
    loaded = load_yaml_file(path)
    return _normalize_config(_deep_merge(DEFAULT_CONFIG, loaded))


def _load_repo_config_file() -> dict[str, Any]:
    path = REPO_ROOT / "config.yaml"
    if not path.exists():
        return {}
    loaded = load_yaml_file(path)
    return loaded if isinstance(loaded, dict) else {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key in set(base) | set(override):
        base_value = base.get(key)
        override_value = override.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = _deep_merge(base_value, override_value)
        elif key in override:
            merged[key] = override_value
        else:
            merged[key] = base_value
    return merged


def _normalize_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(config)
    normalized["proxy_scanner"] = _normalize_proxy_scanner_config(normalized.get("proxy_scanner", {}))
    return normalized


def _normalize_config_with_warnings(config: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    warnings: list[dict[str, Any]] = []
    proxy_config = config.get("proxy_scanner", {}) if isinstance(config.get("proxy_scanner", {}), dict) else {}
    if isinstance(proxy_config.get("signals"), dict) and proxy_config.get("signals"):
        warnings.append(
            {
                "status": "legacy_proxy_signals_config",
                "surface": "proxy_scanner.signals",
                "message": "legacy proxy_scanner.signals config was normalized into proxy_scanner.sources",
            }
        )
    return _normalize_config(config), warnings


def _normalize_proxy_scanner_config(proxy_config: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(proxy_config)
    legacy_sources = normalized.pop("signals", {}) if isinstance(normalized.get("signals"), dict) else {}
    explicit_sources = normalized.get("sources", {}) if isinstance(normalized.get("sources"), dict) else {}
    merged_sources = _deep_merge(DEFAULT_CONFIG["proxy_scanner"]["sources"], legacy_sources)
    merged_sources = _deep_merge(merged_sources, explicit_sources)
    normalized["sources"] = merged_sources
    return normalized


def run_scan_chat_log(log_path: Path, game: str) -> dict[str, Any]:
    config, config_warnings = _normalize_config_with_warnings(load_config())
    game_pack = load_game_pack(game)
    proxy_cfg = config.get("proxy_scanner", {})
    chat_cfg = proxy_cfg.get("sources", {}).get("chat_velocity", {})
    signals = scan_chat_log(log_path, chat_cfg)
    windows = build_proxy_windows(signals, proxy_cfg, media_duration_seconds=None)
    return {
        "ok": True,
        "game": game,
        "game_pack": game_pack.summary(),
        "config_warnings": config_warnings,
        "log_path": str(log_path),
        "config": {
            "chat_velocity": chat_cfg,
            "weights": proxy_cfg.get("weights", {}),
            "candidate_selection": proxy_cfg.get("candidate_selection", {}),
        },
        "signal_count": len(signals),
        "window_count": len(windows),
        "signals": [signal.to_dict() for signal in signals],
        "windows": [window.to_dict() for window in windows],
    }


def run_scan_vod(source: str | Path, game: str, chat_log: str | Path | None = None) -> dict[str, Any]:
    config, config_warnings = _normalize_config_with_warnings(load_config())
    return _scan_vod_with_config(
        source,
        game,
        config=config,
        config_warnings=config_warnings,
        chat_log=chat_log,
    )


def _scan_vod_with_config(
    source: str | Path,
    game: str,
    *,
    config: dict[str, Any],
    config_warnings: list[dict[str, Any]] | None = None,
    chat_log: str | Path | None = None,
) -> dict[str, Any]:
    game_pack = load_game_pack(game)
    proxy_cfg = config.get("proxy_scanner", {})
    media_duration_seconds = probe_media_duration(source)
    signals, source_results = run_proxy_sources(
        ProxyScanContext(
            source=source,
            chat_log=chat_log,
            media_duration_seconds=media_duration_seconds,
        ),
        proxy_cfg,
    )
    windows = build_proxy_windows(signals, proxy_cfg, media_duration_seconds=media_duration_seconds)
    result = {
        "schema_version": PROXY_SCAN_SCHEMA_VERSION,
        "scan_id": _scan_id(game, source),
        "ok": any(result["status"] == "ok" for result in source_results.values()),
        "game": game,
        "source": str(source),
        "game_pack": game_pack.summary(),
        "config_warnings": list(config_warnings or []),
        "source_results": source_results,
        "config": {"proxy_scanner": proxy_cfg},
        "signal_count": len(signals),
        "window_count": len(windows),
        "signals": [signal.to_dict() for signal in signals],
        "windows": [window.to_dict() for window in windows],
    }
    return _write_proxy_scan_sidecar(result, proxy_cfg)


def run_export_training_data(sidecar_root: str | Path, game: str | None = None) -> dict[str, Any]:
    return export_training_data(sidecar_root, game=game)


def run_export_runtime_analysis(sidecar_root: str | Path, game: str | None = None) -> dict[str, Any]:
    config = _normalize_config(load_config())
    runtime_cfg = config.get("runtime_analysis", {})
    return export_runtime_analysis(sidecar_root, game=game, scoring_config=runtime_cfg.get("scoring", {}))


def run_export_fused_analysis(sidecar_root: str | Path, game: str | None = None) -> dict[str, Any]:
    return export_fused_analysis(sidecar_root, game=game)


def run_calibrate_proxy_review(
    sidecar_root: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    min_reviewed: int = 3,
    include_unreviewed: bool = False,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    config = _normalize_config(load_config())
    proxy_cfg = config.get("proxy_scanner", {})
    return calibrate_proxy_review(
        sidecar_root,
        game=game,
        scoring_config=proxy_cfg.get("sources", {}).get("hf_multimodal", {}),
        output_path=output_path,
        min_reviewed=min_reviewed,
        include_unreviewed=include_unreviewed,
        debug_output_dir=debug_output_dir,
    )


def run_replay_proxy_scoring(
    sidecar_root: str | Path,
    trial_proxy_config: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    min_reviewed: int = 3,
    include_unreviewed: bool = False,
    debug_output_dir: str | Path | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    config = _normalize_config(load_config())
    proxy_cfg = config.get("proxy_scanner", {})
    return replay_proxy_scoring(
        sidecar_root,
        trial_proxy_config,
        game=game,
        current_proxy_config={
            "hf_multimodal": proxy_cfg.get("sources", {}).get("hf_multimodal", {}),
            "weights": proxy_cfg.get("weights", {}),
            "candidate_selection": proxy_cfg.get("candidate_selection", {}),
            "cost_gates": proxy_cfg.get("cost_gates", {}),
        },
        output_path=output_path,
        min_reviewed=min_reviewed,
        include_unreviewed=include_unreviewed,
        debug_output_dir=debug_output_dir,
        trial_name=trial_name,
    )


def run_refresh_clip_registry(
    refresh_root: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    registry_path: str | Path | None = None,
) -> dict[str, Any]:
    return refresh_clip_registry(
        refresh_root,
        game=game,
        output_path=output_path,
        debug_output_dir=debug_output_dir,
        registry_path=registry_path,
    )


def run_query_clip_registry(
    *,
    mode: str = "fused-events",
    game: str | None = None,
    event_type: str | None = None,
    action: str | None = None,
    review_status: str | None = None,
    gate_status: str | None = None,
    fixture_id: str | None = None,
    trial_name: str | None = None,
    artifact_layer: str | None = None,
    recommendation_decision: str | None = None,
    coverage_status: str | None = None,
    has_disagreement: bool | None = None,
    limit: int | None = None,
    registry_path: str | Path | None = None,
) -> dict[str, Any]:
    return query_clip_registry(
        mode=mode,
        game=game,
        event_type=event_type,
        action=action,
        review_status=review_status,
        gate_status=gate_status,
        fixture_id=fixture_id,
        trial_name=trial_name,
        artifact_layer=artifact_layer,
        recommendation_decision=recommendation_decision,
        coverage_status=coverage_status,
        has_disagreement=has_disagreement,
        limit=limit,
        registry_path=registry_path,
    )


def run_render_replay_viewer(
    runtime_sidecar: str | Path,
    *,
    fused_sidecar: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    return render_replay_viewer(
        runtime_sidecar,
        fused_sidecar=fused_sidecar,
        output_path=output_path,
    )


def run_render_proxy_replay_viewer(
    proxy_sidecar: str | Path,
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    return render_proxy_replay_viewer(
        proxy_sidecar,
        output_path=output_path,
    )


def run_render_unified_replay_viewer(
    *,
    proxy_sidecar: str | Path | None = None,
    runtime_sidecar: str | Path | None = None,
    fused_sidecar: str | Path | None = None,
    fixture_comparison_report: str | Path | None = None,
    fixture_trial_batch_manifest: str | Path | None = None,
    proxy_calibration_report: str | Path | None = None,
    proxy_replay_report: str | Path | None = None,
    runtime_calibration_report: str | Path | None = None,
    runtime_replay_report: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    return render_unified_replay_viewer(
        proxy_sidecar=proxy_sidecar,
        runtime_sidecar=runtime_sidecar,
        fused_sidecar=fused_sidecar,
        fixture_comparison_report=fixture_comparison_report,
        fixture_trial_batch_manifest=fixture_trial_batch_manifest,
        proxy_calibration_report=proxy_calibration_report,
        proxy_replay_report=proxy_replay_report,
        runtime_calibration_report=runtime_calibration_report,
        runtime_replay_report=runtime_replay_report,
        output_path=output_path,
    )


def run_launch_highlight_review_app(
    sidecar_root: str | Path | None = None,
    *,
    fixture_manifest: str | Path | None = None,
    fixture_comparison_report: str | Path | None = None,
    fixture_trial_batch_manifest: str | Path | None = None,
    proxy_calibration_report: str | Path | None = None,
    proxy_replay_report: str | Path | None = None,
    runtime_calibration_report: str | Path | None = None,
    runtime_replay_report: str | Path | None = None,
    output_path: str | Path | None = None,
    launch: bool = True,
) -> dict[str, Any]:
    return launch_highlight_review_app(
        sidecar_root=sidecar_root,
        fixture_manifest_path=fixture_manifest,
        fixture_comparison_report=fixture_comparison_report,
        fixture_trial_batch_manifest=fixture_trial_batch_manifest,
        proxy_calibration_report=proxy_calibration_report,
        proxy_replay_report=proxy_replay_report,
        runtime_calibration_report=runtime_calibration_report,
        runtime_replay_report=runtime_replay_report,
        output_path=output_path,
        launch=launch,
    )


def run_export_highlight_selection(
    proxy_sidecar: str | Path,
    *,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    return export_highlight_selection(proxy_sidecar, output_path=output_path)


def run_compare_fixture_sidecars(
    fixture_manifest: str | Path,
    *,
    baseline_sidecar_root: str | Path,
    trial_sidecar_root: str | Path,
    artifact_layer: str = "all",
    game: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    config = _normalize_config(load_config())
    runtime_cfg = config.get("runtime_analysis", {})
    return compare_fixture_sidecars(
        fixture_manifest,
        baseline_sidecar_root=baseline_sidecar_root,
        trial_sidecar_root=trial_sidecar_root,
        artifact_layer=artifact_layer,
        game=game,
        output_path=output_path,
        runtime_scoring_config=runtime_cfg.get("scoring", {}),
    )


def run_fixture_trial(
    fixture_manifest: str | Path,
    *,
    fixture_source_manifest: str | Path,
    trial_name: str,
    output_root: str | Path | None = None,
    game: str | None = None,
    pattern: str | None = None,
    limit: int | None = None,
    proposal_backend: str | None = None,
    asr_backend: str | None = None,
    emit_runtime: bool = False,
    emit_fused: bool = False,
) -> dict[str, Any]:
    evaluation_manifest = load_evaluation_fixture_manifest(fixture_manifest)
    source_manifest = load_fixture_source_manifest(fixture_source_manifest)
    source_rows = {
        str(row["fixture_id"]): row
        for row in list(source_manifest.get("fixtures", []))
    }
    evaluation_fixture_ids = {str(row["fixture_id"]) for row in list(evaluation_manifest.get("fixtures", []))}
    base_config = _normalize_config(load_config())
    effective_overrides = _resolve_fixture_trial_overrides(
        trial_name,
        proposal_backend=proposal_backend,
        asr_backend=asr_backend,
    )
    config = _apply_fixture_trial_overrides(base_config, effective_overrides)

    root = _resolve_trial_output_root(output_root)
    trial_root = root / _fixture_trial_slug(trial_name)
    proxy_root = trial_root / "proxy"
    runtime_root = trial_root / "runtime"
    fused_root = trial_root / "fused"
    fixtures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    selected_fixtures = _select_fixture_rows(
        list(evaluation_manifest.get("fixtures", [])),
        pattern=pattern,
        limit=limit,
    )
    for source_fixture_id in sorted(source_rows):
        if source_fixture_id not in evaluation_fixture_ids:
            warnings.append(
                {
                    "fixture_id": source_fixture_id,
                    "reason": "extra_source_fixture_row",
                }
            )

    for fixture in selected_fixtures:
        fixture_id = str(fixture["fixture_id"])
        source_row = source_rows.get(fixture_id)
        if source_row is None:
            fixtures.append(
                {
                    "fixture_id": fixture_id,
                    "label": str(fixture.get("label", fixture_id)),
                    "status": "failed",
                    "failure_reason": "missing_source_fixture",
                    "error": "fixture source manifest is missing this fixture_id",
                    "layers": {},
                }
            )
            continue
        if game is not None and str(source_row.get("game", "")).strip() != game:
            continue
        fixture_layers = _effective_fixture_layers(
            fixture,
            source_row,
            emit_runtime=emit_runtime or emit_fused,
            emit_fused=emit_fused,
        )
        fixture_game = str(source_row["game"])
        source_path = str(source_row["source_path"])
        chat_log_path = source_row.get("chat_log_path")
        record = {
            "fixture_id": fixture_id,
            "label": str(fixture.get("label", fixture_id)),
            "game": fixture_game,
            "source_path": source_path,
            "chat_log_path": chat_log_path,
            "status": "ok",
            "layers": {},
        }
        proxy_filename = f"{fixture_id}.proxy_scan.json"
        fixture_config = deepcopy(config)
        fixture_config["proxy_scanner"]["sidecar"] = {
            "output_dir": str(proxy_root),
            "filename_override": proxy_filename,
        }
        proxy_result = _scan_vod_with_config(
            source_path,
            fixture_game,
            config=fixture_config,
            config_warnings=[],
            chat_log=chat_log_path,
        )
        record["layers"]["proxy"] = {
            "requested": True,
            "ok": bool(proxy_result.get("ok")),
            "status": str(proxy_result.get("status", "ok" if proxy_result.get("ok") else "failed")),
            "sidecar_path": proxy_result.get("sidecar_path"),
        }
        if not proxy_result.get("ok"):
            record["status"] = "failed"
            record["error"] = str(proxy_result.get("error") or proxy_result.get("sidecar_error") or "proxy trial failed")
            fixtures.append(record)
            continue

        runtime_result: dict[str, Any] | None = None
        if fixture_layers["runtime"]:
            runtime_output_path = runtime_root / fixture_game / f"{fixture_id}.runtime_analysis.json"
            runtime_result = run_analyze_roi_runtime(
                source_path,
                fixture_game,
                output_path=runtime_output_path,
            )
            record["layers"]["runtime"] = {
                "requested": True,
                "ok": bool(runtime_result.get("ok")),
                "status": str(runtime_result.get("status", "ok" if runtime_result.get("ok") else "failed")),
                "sidecar_path": runtime_result.get("sidecar_path"),
            }
            if not runtime_result.get("ok"):
                record["status"] = "failed"
                record["error"] = str(runtime_result.get("error") or "runtime trial failed")
                fixtures.append(record)
                continue
        else:
            record["layers"]["runtime"] = {"requested": False, "ok": False, "status": "not_requested", "sidecar_path": None}

        if fixture_layers["fused"]:
            fused_output_path = fused_root / fixture_game / f"{fixture_id}.fused_analysis.json"
            fused_result = run_fuse_clip_signals(
                source_path,
                fixture_game,
                proxy_sidecar=proxy_result.get("sidecar_path"),
                runtime_sidecar=(runtime_result or {}).get("sidecar_path"),
                output_path=fused_output_path,
            )
            record["layers"]["fused"] = {
                "requested": True,
                "ok": bool(fused_result.get("ok")),
                "status": str(fused_result.get("status", "ok" if fused_result.get("ok") else "failed")),
                "sidecar_path": fused_result.get("sidecar_path"),
            }
            if not fused_result.get("ok"):
                record["status"] = "failed"
                record["error"] = str(fused_result.get("error") or "fused trial failed")
        else:
            record["layers"]["fused"] = {"requested": False, "ok": False, "status": "not_requested", "sidecar_path": None}
        fixtures.append(record)

    manifest_path = trial_root / "fixture_trial_run_manifest.json"
    payload = {
        "ok": not any(row.get("status") == "failed" for row in fixtures),
        "status": "ok" if not any(row.get("status") == "failed" for row in fixtures) else "partial_failure",
        "schema_version": FIXTURE_TRIAL_RUN_SCHEMA_VERSION,
        "trial_name": trial_name,
        "fixture_manifest_path": str(Path(fixture_manifest).expanduser().resolve()),
        "fixture_source_manifest_path": str(Path(fixture_source_manifest).expanduser().resolve()),
        "trial_root": str(trial_root),
        "proxy_sidecar_root": str(proxy_root),
        "runtime_sidecar_root": str(runtime_root),
        "fused_sidecar_root": str(fused_root),
        "effective_overrides": effective_overrides,
        "fixture_count": len(fixtures),
        "completed_fixture_count": sum(1 for row in fixtures if row.get("status") == "ok"),
        "failed_fixture_count": sum(1 for row in fixtures if row.get("status") == "failed"),
        "fixtures": fixtures,
        "warnings": warnings,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    payload["manifest_path"] = str(manifest_path)
    return payload


def run_compare_fixture_trials(
    fixture_manifest: str | Path,
    *,
    baseline_run_root: str | Path,
    trial_run_root: str | Path,
    artifact_layer: str = "all",
    game: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    baseline_root = _resolve_path(baseline_run_root)
    trial_root = _resolve_path(trial_run_root)
    baseline_manifest = _load_fixture_trial_run_manifest_record(baseline_root)
    trial_manifest = _load_fixture_trial_run_manifest_record(trial_root)
    baseline_sidecar_root = _resolve_fixture_trial_sidecar_root(
        baseline_root,
        baseline_manifest,
        artifact_layer,
    )
    trial_sidecar_root = _resolve_fixture_trial_sidecar_root(
        trial_root,
        trial_manifest,
        artifact_layer,
    )
    result = run_compare_fixture_sidecars(
        fixture_manifest,
        baseline_sidecar_root=baseline_sidecar_root,
        trial_sidecar_root=trial_sidecar_root,
        artifact_layer=artifact_layer,
        game=game,
        output_path=output_path,
    )
    result["baseline_run_root"] = str(baseline_root)
    result["trial_run_root"] = str(trial_root)
    result["baseline_run_manifest_path"] = (
        str(baseline_manifest["manifest_path"]) if baseline_manifest is not None else None
    )
    result["trial_run_manifest_path"] = (
        str(trial_manifest["manifest_path"]) if trial_manifest is not None else None
    )
    return result


def run_fixture_trial_batch(
    fixture_manifest: str | Path,
    *,
    fixture_source_manifest: str | Path,
    trial_names: list[str] | None = None,
    batch_name: str | None = None,
    output_root: str | Path | None = None,
    game: str | None = None,
    pattern: str | None = None,
    limit: int | None = None,
    emit_runtime: bool = False,
    emit_fused: bool = False,
) -> dict[str, Any]:
    try:
        selected_trials = _resolve_fixture_trial_batch_names(trial_names)
    except ValueError as exc:
        return {
            "ok": False,
            "status": "invalid_trial_selection",
            "error": str(exc),
        }
    batch_root = _resolve_fixture_trial_batch_root(output_root, batch_name, selected_trials)
    runs_root = batch_root / "runs"
    comparisons_root = batch_root / "comparisons"
    artifact_layer = _fixture_trial_batch_artifact_layer(emit_runtime=emit_runtime, emit_fused=emit_fused)

    trial_runs: list[dict[str, Any]] = []
    trial_comparisons: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    baseline_trial_name = "baseline"
    baseline_run_root: Path | None = None

    for trial_name in selected_trials:
        result = run_fixture_trial(
            fixture_manifest,
            fixture_source_manifest=fixture_source_manifest,
            trial_name=trial_name,
            output_root=runs_root,
            game=game,
            pattern=pattern,
            limit=limit,
            proposal_backend=None,
            asr_backend=None,
            emit_runtime=emit_runtime,
            emit_fused=emit_fused,
        )
        trial_run_entry = {
            "trial_name": trial_name,
            "run_manifest_path": result.get("manifest_path"),
            "run_root": result.get("trial_root"),
            "status": result.get("status"),
            "completed_fixture_count": int(result.get("completed_fixture_count", 0)),
            "failed_fixture_count": int(result.get("failed_fixture_count", 0)),
        }
        trial_runs.append(trial_run_entry)
        if result.get("warnings"):
            warnings.extend(list(result.get("warnings", [])))
        if trial_name == baseline_trial_name:
            baseline_run_root = _resolve_path(str(result["trial_root"]))

    if baseline_run_root is None:
        return {
            "ok": False,
            "status": "missing_baseline_run",
            "error": "baseline trial did not produce a run root",
        }

    for trial_run in trial_runs:
        trial_name = str(trial_run["trial_name"])
        if trial_name == baseline_trial_name:
            continue
        if str(trial_run.get("status")) != "ok":
            trial_comparisons.append(
                {
                    "trial_name": trial_name,
                    "comparison_status": "skipped_due_to_run_failure",
                    "comparison_report_path": None,
                    "artifact_layer": artifact_layer,
                    "recommendation": {"decision": "inconclusive", "reason": "trial run did not complete successfully"},
                    "reviewed_row_count": 0,
                    "prefer_trial_count": 0,
                    "keep_current_count": 0,
                    "inconclusive_count": 0,
                }
            )
            continue
        comparison_report_path = comparisons_root / f"{_fixture_trial_slug(baseline_trial_name)}-vs-{_fixture_trial_slug(trial_name)}.json"
        comparison = run_compare_fixture_trials(
            fixture_manifest,
            baseline_run_root=baseline_run_root,
            trial_run_root=_resolve_path(str(trial_run["run_root"])),
            artifact_layer=artifact_layer,
            game=game,
            output_path=comparison_report_path,
        )
        comparison_rows = list(comparison.get("comparison", {}).get("fixture_rows", []))
        reviewed_row_count = sum(
            1
            for row in comparison_rows
            if str(row.get("review_status", "")) in {"approved", "rejected"}
            and str(row.get("coverage_status", "")) == "both"
        )
        prefer_trial_count = sum(1 for row in comparison_rows if str(row.get("recommendation_signal", "")) == "trial_better")
        keep_current_count = sum(1 for row in comparison_rows if str(row.get("recommendation_signal", "")) == "current_better")
        inconclusive_count = sum(
            1
            for row in comparison_rows
            if str(row.get("recommendation_signal", "")) not in {"trial_better", "current_better"}
        )
        trial_comparisons.append(
            {
                "trial_name": trial_name,
                "comparison_status": str(comparison.get("status", "unknown")),
                "comparison_report_path": comparison.get("report_path") or str(comparison_report_path),
                "artifact_layer": artifact_layer,
                "recommendation": dict(comparison.get("recommendation", {})),
                "reviewed_row_count": reviewed_row_count,
                "prefer_trial_count": prefer_trial_count,
                "keep_current_count": keep_current_count,
                "inconclusive_count": inconclusive_count,
            }
        )

    overall_recommendation = _fixture_trial_batch_recommendation(trial_comparisons)
    manifest_path = batch_root / "fixture_trial_batch_manifest.json"
    warnings_path = manifest_path.with_suffix(".warnings.json")
    csv_path = manifest_path.with_suffix(".csv")
    payload = {
        "ok": True,
        "status": "ok",
        "schema_version": FIXTURE_TRIAL_BATCH_SCHEMA_VERSION,
        "batch_name": batch_root.name,
        "fixture_manifest_path": str(_resolve_path(fixture_manifest)),
        "fixture_source_manifest_path": str(_resolve_path(fixture_source_manifest)),
        "selected_trials": selected_trials,
        "effective_batch_options": {
            "artifact_layer": artifact_layer,
            "game": game,
            "pattern": pattern,
            "limit": limit,
            "emit_runtime": emit_runtime,
            "emit_fused": emit_fused,
        },
        "baseline_trial_name": baseline_trial_name,
        "trial_runs": trial_runs,
        "trial_comparisons": trial_comparisons,
        "overall_recommendation": overall_recommendation,
        "warnings": warnings,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    warnings_path.write_text(json.dumps(warnings, indent=2), encoding="utf-8")
    _write_fixture_trial_batch_csv(csv_path, trial_runs, trial_comparisons)
    payload["manifest_path"] = str(manifest_path)
    payload["csv_path"] = str(csv_path)
    payload["warnings_path"] = str(warnings_path)
    payload["batch_root"] = str(batch_root)
    return payload


def run_calibrate_runtime_review(
    sidecar_root: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    min_reviewed: int = 3,
    include_unreviewed: bool = False,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    config = _normalize_config(load_config())
    runtime_cfg = config.get("runtime_analysis", {})
    return calibrate_runtime_review(
        sidecar_root,
        game=game,
        scoring_config=runtime_cfg.get("scoring", {}),
        output_path=output_path,
        min_reviewed=min_reviewed,
        include_unreviewed=include_unreviewed,
        debug_output_dir=debug_output_dir,
    )


def run_replay_runtime_scoring(
    sidecar_root: str | Path,
    trial_config: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    min_reviewed: int = 3,
    include_unreviewed: bool = False,
    debug_output_dir: str | Path | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    config = _normalize_config(load_config())
    runtime_cfg = config.get("runtime_analysis", {})
    return replay_runtime_scoring(
        sidecar_root,
        trial_config,
        game=game,
        current_scoring_config=runtime_cfg.get("scoring", {}),
        output_path=output_path,
        min_reviewed=min_reviewed,
        include_unreviewed=include_unreviewed,
        debug_output_dir=debug_output_dir,
        trial_name=trial_name,
    )


def run_promote_runtime_scoring(
    trial_config: str | Path,
    *,
    sidecar_root: str | Path | None,
    game: str | None = None,
    min_reviewed: int = 3,
    force: bool = False,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    config = _normalize_config(load_config())
    runtime_cfg = config.get("runtime_analysis", {})
    return promote_runtime_scoring(
        trial_config,
        sidecar_root=sidecar_root,
        game=game,
        current_scoring_config=runtime_cfg.get("scoring", {}),
        config_path=REPO_ROOT / "config.yaml",
        config_data=_load_repo_config_file(),
        default_config=DEFAULT_CONFIG,
        min_reviewed=min_reviewed,
        force=force,
        output_path=output_path,
        debug_output_dir=debug_output_dir,
        trial_name=trial_name,
    )


def run_rollback_runtime_scoring(
    snapshot_dir: str | Path,
    *,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    rollback_name: str | None = None,
) -> dict[str, Any]:
    config = _normalize_config(load_config())
    runtime_cfg = config.get("runtime_analysis", {})
    return rollback_runtime_scoring(
        snapshot_dir,
        config_path=REPO_ROOT / "config.yaml",
        config_data=_load_repo_config_file(),
        default_config=DEFAULT_CONFIG,
        current_scoring_config=runtime_cfg.get("scoring", {}),
        output_path=output_path,
        debug_output_dir=debug_output_dir,
        rollback_name=rollback_name,
    )


def run_prepare_proxy_review(
    game: str,
    *,
    batch_report: str | Path | None = None,
    sidecar_root: str | Path | None = None,
    action: str = "download_candidate",
    limit: int | None = None,
    gpt_repo: str | Path | None = None,
    session_name: str | None = None,
) -> dict[str, Any]:
    return prepare_proxy_review(
        game,
        batch_report=batch_report,
        sidecar_root=sidecar_root,
        action=action,
        limit=limit,
        gpt_repo=gpt_repo,
        session_name=session_name,
    )


def run_prepare_runtime_review(
    game: str,
    *,
    sidecar_root: str | Path | None = None,
    action: str | None = None,
    limit: int | None = None,
    gpt_repo: str | Path | None = None,
    session_name: str | None = None,
) -> dict[str, Any]:
    return prepare_runtime_review(
        game,
        sidecar_root=sidecar_root,
        action=action,
        limit=limit,
        gpt_repo=gpt_repo,
        session_name=session_name,
    )


def run_prepare_fused_review(
    game: str,
    *,
    sidecar_root: str | Path | None = None,
    action: str | None = None,
    limit: int | None = None,
    gpt_repo: str | Path | None = None,
    session_name: str | None = None,
    event_type: str | None = None,
) -> dict[str, Any]:
    return prepare_fused_review(
        game,
        sidecar_root=sidecar_root,
        action=action,
        limit=limit,
        gpt_repo=gpt_repo,
        session_name=session_name,
        event_type=event_type,
    )


def run_apply_proxy_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return apply_proxy_review(session_manifest, gpt_repo=gpt_repo)


def run_apply_runtime_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return apply_runtime_review(session_manifest, gpt_repo=gpt_repo)


def run_apply_fused_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return apply_fused_review(session_manifest, gpt_repo=gpt_repo)


def run_cleanup_proxy_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return cleanup_proxy_review(session_manifest, gpt_repo=gpt_repo)


def run_cleanup_runtime_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return cleanup_runtime_review(session_manifest, gpt_repo=gpt_repo)


def run_cleanup_fused_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return cleanup_fused_review(session_manifest, gpt_repo=gpt_repo)


def run_prepare_onboarding_identity_review(
    draft_root: str | Path,
    *,
    gpt_repo: str | Path | None = None,
    session_name: str | None = None,
) -> dict[str, Any]:
    try:
        return prepare_onboarding_identity_review(
            draft_root,
            gpt_repo=gpt_repo,
            session_name=session_name,
        )
    except (ValueError, KeyError, TypeError, FileNotFoundError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_identity_review_preparation",
            "draft_root": str(draft_root),
            "error": str(exc),
        }


def run_apply_onboarding_identity_review(
    session_manifest: str | Path,
    *,
    gpt_repo: str | Path | None = None,
) -> dict[str, Any]:
    try:
        return apply_onboarding_identity_review(session_manifest, gpt_repo=gpt_repo)
    except (ValueError, KeyError, TypeError, FileNotFoundError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_identity_review_apply",
            "session_manifest": str(session_manifest),
            "error": str(exc),
        }


def run_cleanup_onboarding_identity_review(
    session_manifest: str | Path,
    *,
    gpt_repo: str | Path | None = None,
) -> dict[str, Any]:
    try:
        return cleanup_onboarding_identity_review(session_manifest, gpt_repo=gpt_repo)
    except (ValueError, KeyError, TypeError, FileNotFoundError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_identity_review_cleanup",
            "session_manifest": str(session_manifest),
            "error": str(exc),
        }


def run_enrich_game_from_wiki(
    game: str,
    wiki_url: str | None = None,
    *,
    wiki_manifest: str | Path | None = None,
    wiki_sources: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    try:
        sources = _resolve_wiki_sources(wiki_url=wiki_url, wiki_manifest=wiki_manifest, wiki_sources=wiki_sources)
        if len(sources) == 1 and sources[0].role == "overview" and wiki_url and not wiki_manifest and not wiki_sources:
            return enrich_game_from_wiki(game, wiki_url)
        return enrich_game_from_sources(game, sources)
    except (ValueError, KeyError, TypeError) as exc:
        return {
            "ok": False,
            "status": "invalid_wiki_sources",
            "game": game,
            "error": str(exc),
        }
    except WikiFetchError as exc:
        return exc.to_dict()


def run_onboard_game(game: str, source_manifest: str | Path) -> dict[str, Any]:
    try:
        return onboard_game_from_manifest(game, source_manifest)
    except (ValueError, KeyError, TypeError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_manifest",
            "game": game,
            "error": str(exc),
        }
    except WikiFetchError as exc:
        return exc.to_dict()


def run_adapt_game_schema(game: str) -> dict[str, Any]:
    try:
        return adapt_game_schema(game)
    except (ValueError, KeyError, TypeError, FileNotFoundError) as exc:
        return {
            "ok": False,
            "status": "invalid_game_schema_adaptation",
            "game": game,
            "error": str(exc),
        }


def run_ingest_game_sources(schema_draft_or_game: str | Path, source_manifest: str | Path) -> dict[str, Any]:
    try:
        manifest_data = load_yaml_file(source_manifest)
        raw_sources = manifest_data.get("sources")
        if not isinstance(raw_sources, list) or not raw_sources:
            raise ValueError("source manifest must define a non-empty 'sources' list")
        sources: list[OnboardingSource] = []
        for row in raw_sources:
            if not isinstance(row, dict):
                raise ValueError("source rows must be objects with 'role' and 'url'")
            role = str(row.get("role", "")).strip()
            url = str(row.get("url", "")).strip()
            notes = str(row.get("notes", "")).strip()
            if not role or not url:
                raise ValueError("source rows must include 'role' and 'url'")
            sources.append(OnboardingSource(role=role, url=url, notes=notes))
        return ingest_onboarding_sources(schema_draft_or_game, sources)
    except (ValueError, KeyError, TypeError, FileNotFoundError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_sources",
            "target": str(schema_draft_or_game),
            "error": str(exc),
        }
    except WikiFetchError as exc:
        return exc.to_dict()


def run_build_onboarding_draft(populated_draft_root: str | Path) -> dict[str, Any]:
    try:
        return build_onboarding_draft(populated_draft_root)
    except (ValueError, KeyError, TypeError, FileNotFoundError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_draft_build",
            "draft_root": str(populated_draft_root),
            "error": str(exc),
        }


def run_publish_onboarding_draft(draft_root: str | Path) -> dict[str, Any]:
    try:
        return publish_onboarding_draft(draft_root)
    except (ValueError, KeyError, TypeError, FileNotFoundError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_publish",
            "error": str(exc),
        }
    except WikiFetchError as exc:
        return exc.to_dict()


def run_validate_onboarding_publish(draft_root: str | Path) -> dict[str, Any]:
    try:
        return validate_onboarding_publish(draft_root)
    except (ValueError, KeyError, TypeError, FileNotFoundError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_publish_validation",
            "draft_root": str(draft_root),
            "error": str(exc),
        }


def run_report_onboarding_batch(
    root: str | Path,
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    try:
        return summarize_onboarding_batch(root, game=game, output_path=output_path)
    except (ValueError, KeyError, TypeError, FileNotFoundError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_batch_report",
            "root": str(root),
            "error": str(exc),
        }


def run_publish_onboarding_batch(
    root: str | Path,
    *,
    game: str | None = None,
    apply: bool = False,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    try:
        return publish_onboarding_batch(root, game=game, apply=apply, output_path=output_path)
    except (ValueError, KeyError, TypeError, FileNotFoundError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "status": "invalid_onboarding_batch_publish",
            "root": str(root),
            "error": str(exc),
        }


def run_match_roi_templates(
    source: str | Path,
    game: str,
    *,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    output_path: str | Path | None = None,
    min_score: float | None = None,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    try:
        return match_roi_templates(
            source,
            game,
            sample_fps=sample_fps,
            limit_frames=limit_frames,
            output_path=output_path,
            min_score=min_score,
            debug_output_dir=debug_output_dir,
        )
    except RoiMatcherError as exc:
        return exc.to_dict(game=game, source=source)


def run_map_roi_events(
    source: str | Path,
    game: str,
    *,
    matcher_report: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    runtime_rule_overrides: dict[str, dict[str, Any]] | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    try:
        return map_roi_events(
            source,
            game,
            matcher_report=matcher_report,
            sample_fps=sample_fps,
            limit_frames=limit_frames,
            runtime_rule_overrides=runtime_rule_overrides,
            output_path=output_path,
            debug_output_dir=debug_output_dir,
        )
    except (EventMapperError, RoiMatcherError) as exc:
        if hasattr(exc, "to_dict"):
            return exc.to_dict(game=game, source=source)
        return {
            "ok": False,
            "status": "map_failed",
            "game": game,
            "source": str(source),
            "error": str(exc),
        }
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "status": "missing_game_pack",
            "game": game,
            "source": str(source),
            "error": str(exc),
        }


def run_analyze_roi_runtime(
    source: str | Path,
    game: str,
    *,
    matcher_report: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    template_overrides: dict[str, dict[str, Any]] | None = None,
    runtime_rule_overrides: dict[str, dict[str, Any]] | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    try:
        return analyze_roi_runtime(
            source,
            game,
            matcher_report=matcher_report,
            sample_fps=sample_fps,
            limit_frames=limit_frames,
            template_overrides=template_overrides,
            runtime_rule_overrides=runtime_rule_overrides,
            output_path=output_path,
            debug_output_dir=debug_output_dir,
        )
    except (RuntimeAnalysisError, EventMapperError, RoiMatcherError) as exc:
        if hasattr(exc, "to_dict"):
            return exc.to_dict(game=game, source=source)
        return {
            "ok": False,
            "status": "runtime_analysis_failed",
            "game": game,
            "source": str(source),
            "error": str(exc),
        }
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "status": "missing_game_pack",
            "game": game,
            "source": str(source),
            "error": str(exc),
        }


def run_fuse_clip_signals(
    source: str | Path,
    game: str,
    *,
    proxy_sidecar: str | Path | None = None,
    runtime_sidecar: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    fusion_rules_path: str | Path | None = None,
) -> dict[str, Any]:
    try:
        proxy_payload = load_proxy_sidecar(proxy_sidecar) if proxy_sidecar else run_scan_vod(source, game)
        if not proxy_sidecar and proxy_payload.get("schema_version") != PROXY_SCAN_SCHEMA_VERSION:
            return proxy_payload
        runtime_payload = (
            load_runtime_sidecar(runtime_sidecar)
            if runtime_sidecar
            else run_analyze_roi_runtime(
                source,
                game,
                sample_fps=sample_fps,
                limit_frames=limit_frames,
            )
        )
        if not runtime_sidecar and runtime_payload.get("schema_version") != "runtime_analysis_v1":
            return runtime_payload
        return fuse_analysis(
            source,
            game,
            proxy_sidecar=proxy_payload,
            runtime_sidecar=runtime_payload,
            output_path=output_path,
            debug_output_dir=debug_output_dir,
            rules_path=fusion_rules_path,
        )
    except FusionAnalysisError as exc:
        return exc.to_dict(game=game, source=source)


def run_validate_fusion_goldset(
    goldset_root: str | Path,
    *,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    proxy_sidecar_root: str | Path | None = None,
    runtime_sidecar_root: str | Path | None = None,
    fused_sidecar_root: str | Path | None = None,
) -> dict[str, Any]:
    return validate_fusion_goldset(
        goldset_root,
        clip_runner=_fusion_validation_clip_runner,
        game=game,
        media_root=media_root,
        output_path=output_path,
        debug_output_dir=debug_output_dir,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=proxy_sidecar_root,
        runtime_sidecar_root=runtime_sidecar_root,
        fused_sidecar_root=fused_sidecar_root,
    )


def run_replay_fusion_rules(
    goldset_root: str | Path,
    trial_rules: str | Path,
    *,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    proxy_sidecar_root: str | Path | None = None,
    runtime_sidecar_root: str | Path | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    return replay_fusion_rules(
        goldset_root,
        trial_rules,
        clip_runner=_fusion_validation_clip_runner,
        game=game,
        media_root=media_root,
        output_path=output_path,
        debug_output_dir=debug_output_dir,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        proxy_sidecar_root=proxy_sidecar_root,
        runtime_sidecar_root=runtime_sidecar_root,
        trial_name=trial_name,
    )


def run_replay_template_thresholds(
    goldset_root: str | Path,
    trial_templates: str | Path,
    *,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    return replay_template_thresholds(
        goldset_root,
        trial_templates,
        clip_runner=_fusion_validation_clip_runner,
        game=game,
        media_root=media_root,
        output_path=output_path,
        debug_output_dir=debug_output_dir,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        trial_name=trial_name,
    )


def run_replay_runtime_event_rules(
    goldset_root: str | Path,
    trial_runtime_rules: str | Path,
    *,
    game: str | None = None,
    media_root: str | Path | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    trial_name: str | None = None,
) -> dict[str, Any]:
    return replay_runtime_event_rules(
        goldset_root,
        trial_runtime_rules,
        clip_runner=_fusion_validation_clip_runner,
        game=game,
        media_root=media_root,
        output_path=output_path,
        debug_output_dir=debug_output_dir,
        sample_fps=sample_fps,
        limit_frames=limit_frames,
        trial_name=trial_name,
    )


def _fusion_validation_clip_runner(
    source: str | Path,
    game: str,
    *,
    sample_fps: float | None = None,
    limit_frames: int | None = None,
    proxy_sidecar_root: str | Path | None = None,
    runtime_sidecar_root: str | Path | None = None,
    fused_sidecar_root: str | Path | None = None,
    trial_rules_path: str | Path | None = None,
    trial_template_overrides_path: str | Path | None = None,
    trial_runtime_rule_overrides_path: str | Path | None = None,
) -> dict[str, Any]:
    template_overrides = None
    runtime_rule_overrides = None
    if trial_template_overrides_path is not None:
        try:
            template_overrides = load_template_trial_overrides(trial_template_overrides_path)
        except RoiMatcherError as exc:
            return {
                "ok": False,
                "status": exc.status,
                "error": exc.message,
            }
    if trial_runtime_rule_overrides_path is not None:
        try:
            runtime_rule_overrides = load_runtime_rule_trial_overrides(trial_runtime_rule_overrides_path)
        except EventMapperError as exc:
            return {
                "ok": False,
                "status": exc.status,
                "error": exc.message,
            }
    proxy_payload = _load_sidecar_from_root(proxy_sidecar_root, "*.proxy_scan.json", source, game)
    if proxy_payload is None:
        proxy_payload = run_scan_vod(source, game)
    if proxy_payload.get("schema_version") != PROXY_SCAN_SCHEMA_VERSION:
        return {"ok": False, "status": str(proxy_payload.get("status", "proxy_analysis_failed")), "proxy": proxy_payload}

    runtime_payload = _load_sidecar_from_root(runtime_sidecar_root, "*.runtime_analysis.json", source, game)
    if runtime_payload is None:
        runtime_payload = run_analyze_roi_runtime(
            source,
            game,
            sample_fps=sample_fps,
            limit_frames=limit_frames,
            template_overrides=template_overrides,
            runtime_rule_overrides=runtime_rule_overrides,
        )
    if runtime_payload.get("schema_version") != "runtime_analysis_v1":
        return {
            "ok": False,
            "status": str(runtime_payload.get("status", "runtime_analysis_failed")),
            "proxy": proxy_payload,
            "runtime": runtime_payload,
        }

    fused_payload = _load_sidecar_from_root(fused_sidecar_root, "*.fused_analysis.json", source, game)
    if fused_payload is None:
        try:
            fused_payload = fuse_analysis(
                source,
                game,
                proxy_sidecar=proxy_payload,
                runtime_sidecar=runtime_payload,
                rules_path=trial_rules_path,
            )
        except FusionAnalysisError as exc:
            return {
                "ok": False,
                "status": exc.status,
                "error": exc.message,
                "proxy": proxy_payload,
                "runtime": runtime_payload,
            }

    return {
        "ok": True,
        "status": "ok",
        "proxy": proxy_payload,
        "runtime": runtime_payload,
        "fused": fused_payload,
    }


def _load_sidecar_from_root(
    root: str | Path | None,
    glob_pattern: str,
    source: str | Path,
    game: str,
) -> dict[str, Any] | None:
    if root is None:
        return None
    index = load_sidecar_index(root, glob_pattern)
    return index.get((game, str(source)))


def run_check_roi_runtime() -> dict[str, Any]:
    return check_roi_runtime()


def run_validate_published_pack(game: str) -> dict[str, Any]:
    try:
        return validate_published_pack(game)
    except RoiMatcherError as exc:
        return exc.to_dict(game=game)


def run_audit_pipeline_contracts(
    *,
    game: str | None = None,
    output_path: str | Path | None = None,
    debug_output_dir: str | Path | None = None,
) -> dict[str, Any]:
    raw_config = _load_repo_config_file()
    result = audit_pipeline_contracts(game=game, repo_root=REPO_ROOT, config_payload=raw_config)
    if output_path is not None:
        target = Path(output_path).expanduser()
        if not target.is_absolute():
            target = (Path.cwd() / target).resolve()
        else:
            target = target.resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if debug_output_dir is not None:
        debug_root = Path(debug_output_dir).expanduser()
        if not debug_root.is_absolute():
            debug_root = (Path.cwd() / debug_root).resolve()
        else:
            debug_root = debug_root.resolve()
        debug_root.mkdir(parents=True, exist_ok=True)
        (debug_root / "pipeline_contract_audit.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def run_list_pack_templates(game: str) -> dict[str, Any]:
    try:
        return list_pack_templates(game)
    except RoiMatcherError as exc:
        return exc.to_dict(game=game)
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "status": "missing_game_pack",
            "game": game,
            "error": str(exc),
        }


def _resolve_wiki_sources(
    *,
    wiki_url: str | None,
    wiki_manifest: str | Path | None,
    wiki_sources: list[tuple[str, str]] | None,
) -> list[WikiSource]:
    sources: list[WikiSource] = []
    if wiki_url:
        sources.append(WikiSource(url=wiki_url, role="overview"))
    if wiki_manifest:
        manifest_data = load_yaml_file(Path(wiki_manifest))
        raw_sources = manifest_data.get("sources", manifest_data)
        if not isinstance(raw_sources, list):
            raise ValueError("wiki manifest must contain a list of sources or a top-level 'sources' list")
        for row in raw_sources:
            if not isinstance(row, dict):
                raise ValueError("wiki manifest sources must be objects with 'url' and 'role'")
            sources.append(WikiSource(url=str(row["url"]), role=str(row["role"])))
    for role, url in wiki_sources or []:
        sources.append(WikiSource(url=url, role=role))
    if not sources:
        raise ValueError("at least one wiki source is required")
    return sources


def run_scan_vod_batch(
    root: str | Path,
    game: str,
    pattern: str = "*.mp4",
    limit: int | None = None,
) -> dict[str, Any]:
    root_path = Path(root).expanduser().resolve()
    report_path = _proxy_scan_batch_report_path(root_path, game, pattern, limit)
    results: list[dict[str, Any]] = []
    matched_files = _collect_batch_sources(root_path, pattern, limit)

    for source_path in matched_files:
        if not source_path.is_file():
            results.append(
                {
                    "source": str(source_path),
                    "ok": False,
                    "signal_count": 0,
                    "window_count": 0,
                    "top_recommended_action": "none",
                    "top_proxy_score": None,
                    "sidecar_path": None,
                    "source_results": {},
                }
            )
            continue

        scan_result = run_scan_vod(source_path, game)
        top_window = scan_result["windows"][0] if scan_result["windows"] else None
        results.append(
            {
                "source": str(source_path),
                "ok": bool(scan_result["ok"]),
                "signal_count": int(scan_result["signal_count"]),
                "window_count": int(scan_result["window_count"]),
                "top_recommended_action": top_window["recommended_action"] if top_window else "none",
                "top_proxy_score": top_window["proxy_score"] if top_window else None,
                "sidecar_path": scan_result.get("sidecar_path"),
                "source_results": scan_result.get("source_results", {}),
            }
        )

    summary = _build_proxy_scan_batch_summary(
        root=root_path,
        game=game,
        pattern=pattern,
        limit=limit,
        results=results,
    )
    summary["report_path"] = str(report_path)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def _write_proxy_scan_sidecar(result: dict[str, Any], proxy_config: dict[str, Any]) -> dict[str, Any]:
    sidecar_path = _sidecar_path(result["source"], result["game"], proxy_config.get("sidecar", {}))
    result["sidecar_path"] = str(sidecar_path)

    try:
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    except OSError as exc:
        result["ok"] = False
        result["sidecar_error"] = str(exc)

    return result


def _sidecar_path(source: str | Path, game: str, sidecar_config: dict[str, Any]) -> Path:
    output_dir = Path(str(sidecar_config.get("output_dir", "outputs/proxy_scans")))
    if not output_dir.is_absolute():
        output_dir = REPO_ROOT / output_dir

    filename_override = str(sidecar_config.get("filename_override", "")).strip()
    if filename_override:
        return output_dir / game / filename_override

    source_slug = _source_slug(source)
    source_hash = hashlib.sha1(str(source).encode("utf-8")).hexdigest()[:12]
    filename = f"{source_slug}-{source_hash}.proxy_scan.json"
    return output_dir / game / filename


def _source_slug(source: str | Path) -> str:
    source_text = str(source)
    stem = Path(source_text).stem
    if "://" in source_text:
        path_part = source_text.split("://", 1)[1].split("?", 1)[0]
        stem = Path(path_part).stem or stem
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", stem.lower()).strip("-")
    return slug or "scan"


def _scan_id(game: str, source: str | Path) -> str:
    digest = hashlib.sha1(f"{game}\n{source}".encode("utf-8")).hexdigest()[:12]
    return f"{game}-{digest}"


def _collect_batch_sources(root: Path, pattern: str, limit: int | None) -> list[Path]:
    if not root.is_dir():
        raise ValueError(f"batch root is not a directory: {root}")

    matches = sorted(path for path in root.rglob(pattern) if path.is_file())
    if limit is not None:
        return matches[:limit]
    return matches


def _resolve_fixture_trial_overrides(
    trial_name: str,
    *,
    proposal_backend: str | None,
    asr_backend: str | None,
) -> dict[str, str]:
    preset = dict(FIXTURE_TRIAL_PRESETS.get(str(trial_name).strip(), FIXTURE_TRIAL_PRESETS["baseline"]))
    if proposal_backend:
        preset["proposal_backend"] = str(proposal_backend).strip()
    if asr_backend:
        preset["asr_backend"] = str(asr_backend).strip()
    return preset


def _apply_fixture_trial_overrides(config: dict[str, Any], overrides: dict[str, str]) -> dict[str, Any]:
    merged = deepcopy(config)
    merged["proxy_scanner"]["sources"]["hf_multimodal"]["enabled"] = True
    merged["proxy_scanner"]["sources"]["audio_prepass"]["enabled"] = False
    merged["proxy_scanner"]["sources"]["visual_prepass"]["enabled"] = False
    merged["proxy_scanner"]["sources"]["hf_multimodal"]["components"]["shot_detector"]["runtime_options"]["proposal_backend"] = overrides["proposal_backend"]
    merged["proxy_scanner"]["sources"]["hf_multimodal"]["components"]["asr"]["runtime_options"]["asr_backend"] = overrides["asr_backend"]
    return merged


def _resolve_trial_output_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return DEFAULT_FIXTURE_TRIAL_OUTPUT_ROOT
    return _resolve_path(output_root)


def _fixture_trial_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", str(value).strip().lower()).strip("-")
    return slug or "fixture-trial"


def _select_fixture_rows(
    fixtures: list[dict[str, Any]],
    *,
    pattern: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    rows = list(fixtures)
    if pattern:
        rows = [row for row in rows if fnmatch.fnmatch(str(row.get("fixture_id", "")), pattern)]
    if limit is not None:
        rows = rows[:limit]
    return rows


def _effective_fixture_layers(
    fixture: dict[str, Any],
    source_row: dict[str, Any],
    *,
    emit_runtime: bool,
    emit_fused: bool,
) -> dict[str, bool]:
    expected = dict(fixture.get("expected_artifacts", {}))
    source_layers = dict(source_row.get("produce_layers", {}))
    runtime_allowed = bool(source_layers.get("runtime", expected.get("runtime", False)))
    fused_allowed = bool(source_layers.get("fused", expected.get("fused", False)))
    return {
        "proxy": True,
        "runtime": bool(emit_runtime and runtime_allowed),
        "fused": bool(emit_fused and fused_allowed),
    }


def _maybe_load_fixture_trial_run_manifest(root: Path) -> Path | None:
    manifest_path = root / "fixture_trial_run_manifest.json"
    return manifest_path if manifest_path.exists() and manifest_path.is_file() else None


def _load_fixture_trial_run_manifest_record(root: Path) -> dict[str, Any] | None:
    manifest_path = _maybe_load_fixture_trial_run_manifest(root)
    if manifest_path is None:
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    payload["manifest_path"] = str(manifest_path)
    return payload


def _resolve_fixture_trial_sidecar_root(
    run_root: Path,
    manifest: dict[str, Any] | None,
    artifact_layer: str,
) -> Path:
    layer = str(artifact_layer or "all").strip().lower()
    if manifest is None:
        return run_root
    if layer == "proxy":
        return _resolve_path(str(manifest.get("proxy_sidecar_root", run_root / "proxy")))
    if layer == "runtime":
        return _resolve_path(str(manifest.get("runtime_sidecar_root", run_root / "runtime")))
    if layer == "fused":
        return _resolve_path(str(manifest.get("fused_sidecar_root", run_root / "fused")))
    return run_root


def _resolve_fixture_trial_batch_names(trial_names: list[str] | None) -> list[str]:
    names = [str(name).strip() for name in list(trial_names or []) if str(name).strip()]
    if not names:
        names = ["baseline", "pyscenedetect", "distil-whisper", "cheap-stage-combined"]
    invalid = [name for name in names if name not in FIXTURE_TRIAL_PRESETS]
    if invalid:
        raise ValueError(f"unknown fixture trial names: {', '.join(invalid)}")
    ordered = ["baseline"] + [name for name in names if name != "baseline"]
    deduped: list[str] = []
    for name in ordered:
        if name not in deduped:
            deduped.append(name)
    return deduped


def _resolve_fixture_trial_batch_root(
    output_root: str | Path | None,
    batch_name: str | None,
    trial_names: list[str],
) -> Path:
    root = (
        _resolve_path(output_root) / "fixture_trial_batches"
        if output_root is not None
        else DEFAULT_FIXTURE_TRIAL_BATCH_OUTPUT_ROOT
    )
    derived_name = batch_name or f"{trial_names[0]}-batch"
    return root / _fixture_trial_slug(derived_name)


def _fixture_trial_batch_artifact_layer(*, emit_runtime: bool, emit_fused: bool) -> str:
    if emit_fused:
        return "fused"
    if emit_runtime:
        return "runtime"
    return "proxy"


def _fixture_trial_batch_recommendation(trial_comparisons: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [row for row in trial_comparisons if str(row.get("comparison_status")) == "ok"]
    prefer = [row for row in successful if str(row.get("recommendation", {}).get("decision", "")) == "prefer_trial"]
    keep = [row for row in successful if str(row.get("recommendation", {}).get("decision", "")) == "keep_current"]
    if len(prefer) == 1 and not keep:
        row = prefer[0]
        return {
            "decision": "adopt_trial",
            "trial_name": row.get("trial_name"),
            "reason": "one trial clearly outperformed baseline on reviewed fixture coverage",
        }
    if keep and not prefer:
        return {
            "decision": "keep_baseline",
            "trial_name": None,
            "reason": "baseline retained better reviewed fixture behavior than the tested trials",
        }
    return {
        "decision": "inconclusive",
        "trial_name": None,
        "reason": "trial outcomes are sparse, mixed, or incomplete",
    }


def _write_fixture_trial_batch_csv(
    path: Path,
    trial_runs: list[dict[str, Any]],
    trial_comparisons: list[dict[str, Any]],
) -> None:
    run_by_name = {str(row["trial_name"]): row for row in trial_runs}
    comparison_by_name = {str(row["trial_name"]): row for row in trial_comparisons}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "trial_name",
                "run_status",
                "comparison_status",
                "recommendation_decision",
                "completed_fixture_count",
                "failed_fixture_count",
                "reviewed_row_count",
                "prefer_trial_count",
                "keep_current_count",
                "inconclusive_count",
            ],
        )
        writer.writeheader()
        for trial_name, comparison in sorted(comparison_by_name.items()):
            run = run_by_name.get(trial_name, {})
            writer.writerow(
                {
                    "trial_name": trial_name,
                    "run_status": run.get("status"),
                    "comparison_status": comparison.get("comparison_status"),
                    "recommendation_decision": comparison.get("recommendation", {}).get("decision"),
                    "completed_fixture_count": run.get("completed_fixture_count", 0),
                    "failed_fixture_count": run.get("failed_fixture_count", 0),
                    "reviewed_row_count": comparison.get("reviewed_row_count", 0),
                    "prefer_trial_count": comparison.get("prefer_trial_count", 0),
                    "keep_current_count": comparison.get("keep_current_count", 0),
                    "inconclusive_count": comparison.get("inconclusive_count", 0),
                }
            )


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path


def _build_proxy_scan_batch_summary(
    *,
    root: Path,
    game: str,
    pattern: str,
    limit: int | None,
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    success_count = sum(1 for result in results if result["ok"])
    failed_count = len(results) - success_count
    window_count_total = sum(int(result["window_count"]) for result in results)

    skip_count = 0
    inspect_count = 0
    download_candidate_count = 0
    for result in results:
        if not result["ok"]:
            continue
        action = result["top_recommended_action"]
        if action == "download_candidate":
            download_candidate_count += 1
        elif action == "inspect":
            inspect_count += 1
        elif action == "skip":
            skip_count += 1

    return {
        "schema_version": "proxy_scan_batch_v1",
        "batch_id": _proxy_scan_batch_id(root, game, pattern, limit),
        "game": game,
        "root": str(root),
        "pattern": pattern,
        "limit": limit,
        "file_count": len(results),
        "scanned_count": len(results),
        "success_count": success_count,
        "failed_count": failed_count,
        "window_count_total": window_count_total,
        "skip_count": skip_count,
        "inspect_count": inspect_count,
        "download_candidate_count": download_candidate_count,
        "results": results,
    }


def _proxy_scan_batch_report_path(root: Path, game: str, pattern: str, limit: int | None) -> Path:
    output_dir = REPO_ROOT / "outputs" / "proxy_scan_batches" / game
    filename = f"{_source_slug(root)}-{_proxy_scan_batch_hash(root, game, pattern, limit)}.proxy_scan_batch.json"
    return output_dir / filename


def _proxy_scan_batch_hash(root: Path, game: str, pattern: str, limit: int | None) -> str:
    payload = f"{root}\n{game}\n{pattern}\n{limit}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def _proxy_scan_batch_id(root: Path, game: str, pattern: str, limit: int | None) -> str:
    return f"{game}-batch-{_proxy_scan_batch_hash(root, game, pattern, limit)}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Initial runnable scaffold for the gaming clip pipeline.")
    parser.add_argument("--list-games", action="store_true", help="List available game packs.")
    parser.add_argument("--init-game", metavar="GAME", help="Copy a starter game pack into assets/games.")
    parser.add_argument("--validate-game-pack", metavar="GAME", help="Validate a game pack.")
    parser.add_argument("--chat-log", metavar="PATH", help="Optional chat log path used by --scan-vod.")
    parser.add_argument("--game", metavar="GAME", help="Optional game filter used by --export-training-data.")
    parser.add_argument("--event-type", metavar="TYPE", help="Optional event-type filter used by --prepare-fused-review.")
    parser.add_argument("--pattern", metavar="GLOB", default="*.mp4", help="Optional glob used by --scan-vod-batch.")
    parser.add_argument("--limit", metavar="N", type=int, help="Optional file limit used by scan, review, and registry query commands.")
    parser.add_argument("--action", metavar="NAME", help="Optional action filter used by review preparation and registry query commands.")
    parser.add_argument("--review-status", metavar="STATUS", help="Optional review-status filter used by --query-clip-registry.")
    parser.add_argument("--gate-status", metavar="STATUS", help="Optional gate-status filter used by --query-clip-registry.")
    parser.add_argument("--mode", metavar="NAME", help="Optional mode selector used by --query-clip-registry.")
    parser.add_argument("--fixture-id", metavar="ID", help="Optional fixture-id filter used by --query-clip-registry.")
    parser.add_argument("--recommendation-decision", metavar="NAME", help="Optional recommendation-decision filter used by --query-clip-registry.")
    parser.add_argument("--coverage-status", metavar="STATUS", help="Optional coverage-status filter used by --query-clip-registry.")
    parser.add_argument("--has-disagreement", action="store_true", help="Optional disagreement filter used by --query-clip-registry.")
    parser.add_argument("--batch-report", metavar="PATH", help="Optional batch report path used by --prepare-proxy-review.")
    parser.add_argument("--sidecar-root", metavar="PATH", help="Optional sidecar root used by --prepare-proxy-review.")
    parser.add_argument("--registry-path", metavar="PATH", help="Optional SQLite registry path used by registry commands.")
    parser.add_argument("--gpt-repo", metavar="PATH", help="Optional GPT review repo path used by proxy review bridge commands.")
    parser.add_argument("--session-name", metavar="NAME", help="Optional session name used by --prepare-proxy-review.")
    parser.add_argument(
        "--scan-chat-log",
        nargs=2,
        metavar=("LOG_PATH", "GAME"),
        help="Scan a chat log and emit proxy signals plus candidate windows.",
    )
    parser.add_argument(
        "--scan-vod",
        nargs=2,
        metavar=("SOURCE", "GAME"),
        help="Scan a VOD source with cheap proxy scanners and emit fused candidate windows.",
    )
    parser.add_argument(
        "--export-training-data",
        metavar="SIDECAR_ROOT",
        help="Export proxy scan sidecars into JSONL and CSV training datasets.",
    )
    parser.add_argument(
        "--export-runtime-analysis",
        metavar="SIDECAR_ROOT",
        help="Export runtime-analysis sidecars into scored clip, event, and detection datasets.",
    )
    parser.add_argument(
        "--export-fused-analysis",
        metavar="SIDECAR_ROOT",
        help="Export fused-analysis sidecars into candidate, fused-event, and signal-reference datasets.",
    )
    parser.add_argument(
        "--export-highlight-selection",
        metavar="PROXY_SIDECAR",
        help="Export selected highlight windows from one proxy sidecar into a canonical manifest and OTIO skeleton.",
    )
    parser.add_argument(
        "--refresh-clip-registry",
        metavar="ROOT",
        help="Refresh the local SQLite clip and event registry from sidecars and review manifests under one root.",
    )
    parser.add_argument(
        "--query-clip-registry",
        action="store_true",
        help="Query the local SQLite clip and event registry and emit structured JSON rows.",
    )
    parser.add_argument(
        "--calibrate-proxy-review",
        metavar="SIDECAR_ROOT",
        help="Analyze reviewed proxy-scan sidecars with hf_multimodal evidence and emit calibration diagnostics.",
    )
    parser.add_argument(
        "--replay-proxy-scoring",
        metavar="SIDECAR_ROOT",
        help="Replay reviewed proxy-scan sidecars with hf_multimodal evidence against one trial proxy config.",
    )
    parser.add_argument(
        "--calibrate-runtime-review",
        metavar="SIDECAR_ROOT",
        help="Analyze reviewed runtime-analysis sidecars and emit calibration diagnostics.",
    )
    parser.add_argument(
        "--replay-runtime-scoring",
        metavar="SIDECAR_ROOT",
        help="Replay reviewed runtime-analysis sidecars against one trial scoring config and compare outcomes.",
    )
    parser.add_argument(
        "--validate-fusion-goldset",
        metavar="GOLDSET_ROOT",
        help="Replay the clip-detection pipeline against labeled fusion gold manifests and emit validation diagnostics.",
    )
    parser.add_argument(
        "--replay-fusion-rules",
        metavar="GOLDSET_ROOT",
        help="Replay a fusion rule trial against labeled gold manifests and compare it with current published rules.",
    )
    parser.add_argument(
        "--replay-template-thresholds",
        metavar="GOLDSET_ROOT",
        help="Replay a template threshold trial against labeled gold manifests and compare it with current published templates.",
    )
    parser.add_argument(
        "--replay-runtime-event-rules",
        metavar="GOLDSET_ROOT",
        help="Replay runtime event rule overrides against labeled gold manifests and compare them with current published runtime rules.",
    )
    parser.add_argument(
        "--promote-runtime-scoring",
        metavar="TRIAL_CONFIG",
        help="Promote a runtime trial scoring config into the active repo config after replay validation.",
    )
    parser.add_argument(
        "--rollback-runtime-scoring",
        metavar="SNAPSHOT_DIR",
        help="Restore runtime scoring in config.yaml from a prior runtime scoring snapshot directory.",
    )
    parser.add_argument(
        "--scan-vod-batch",
        nargs=2,
        metavar=("ROOT", "GAME"),
        help="Scan a local directory of media files through the proxy pipeline and persist a batch report.",
    )
    parser.add_argument(
        "--prepare-proxy-review",
        metavar="GAME",
        help="Prepare the current proxy candidate set as a GPT-Codex review-app queue.",
    )
    parser.add_argument(
        "--prepare-runtime-review",
        metavar="GAME",
        help="Prepare the current runtime-analysis candidate set as a GPT-Codex review-app queue.",
    )
    parser.add_argument(
        "--prepare-fused-review",
        metavar="GAME",
        help="Prepare fused event segments as a GPT-Codex review-app queue.",
    )
    parser.add_argument(
        "--apply-proxy-review",
        metavar="SESSION_MANIFEST",
        help="Import GPT-Codex review decisions back into proxy sidecars.",
    )
    parser.add_argument(
        "--apply-runtime-review",
        metavar="SESSION_MANIFEST",
        help="Import GPT-Codex review decisions back into runtime-analysis sidecars.",
    )
    parser.add_argument(
        "--apply-fused-review",
        metavar="SESSION_MANIFEST",
        help="Import GPT-Codex review decisions back into fused-analysis sidecars.",
    )
    parser.add_argument(
        "--cleanup-proxy-review",
        metavar="SESSION_MANIFEST",
        help="Remove generated GPT-Codex review bridge artifacts for a session.",
    )
    parser.add_argument(
        "--cleanup-runtime-review",
        metavar="SESSION_MANIFEST",
        help="Remove generated GPT-Codex runtime review bridge artifacts for a session.",
    )
    parser.add_argument(
        "--cleanup-fused-review",
        metavar="SESSION_MANIFEST",
        help="Remove generated GPT-Codex fused review bridge artifacts for a session.",
    )
    parser.add_argument(
        "--prepare-onboarding-identity-review",
        metavar="DRAFT_ROOT",
        help="Prepare one onboarding draft's blocking identity findings as a GPT-Codex review session.",
    )
    parser.add_argument(
        "--apply-onboarding-identity-review",
        metavar="SESSION_MANIFEST",
        help="Import GPT-Codex onboarding identity review decisions back into one onboarding draft.",
    )
    parser.add_argument(
        "--cleanup-onboarding-identity-review",
        metavar="SESSION_MANIFEST",
        help="Remove generated GPT-Codex onboarding identity review bridge artifacts for a session.",
    )
    parser.add_argument(
        "--enrich-game-from-wiki",
        metavar="GAME",
        help="Build a draft-only asset enrichment bundle from an explicit source URL.",
    )
    parser.add_argument(
        "--wiki-url",
        metavar="URL",
        help="Explicit source URL used by --enrich-game-from-wiki.",
    )
    parser.add_argument(
        "--wiki-manifest",
        metavar="PATH",
        help="Path to a manifest file listing explicit wiki source URLs and roles.",
    )
    parser.add_argument(
        "--wiki-source",
        nargs=2,
        action="append",
        metavar=("ROLE", "URL"),
        help="Repeated role/URL source pair used by --enrich-game-from-wiki.",
    )
    parser.add_argument(
        "--adapt-game-schema",
        metavar="GAME",
        help="Create a saved game-specific onboarding schema draft from the repo baseline schema.",
    )
    parser.add_argument(
        "--ingest-game-sources",
        metavar="SCHEMA_DRAFT_OR_GAME",
        help="Populate an onboarding draft from explicit source inputs using a saved or newly created schema draft.",
    )
    parser.add_argument(
        "--build-onboarding-draft",
        metavar="POPULATED_DRAFT_ROOT",
        help="Generate binding candidates and QA artifacts from a populated onboarding draft.",
    )
    parser.add_argument(
        "--onboard-game",
        metavar="GAME",
        help="Build a draft onboarding bundle from an explicit onboarding source manifest.",
    )
    parser.add_argument(
        "--source-manifest",
        metavar="PATH",
        help="Path to an explicit onboarding source manifest used by --onboard-game.",
    )
    parser.add_argument(
        "--publish-onboarding-draft",
        metavar="DRAFT_ROOT",
        help="Publish accepted template bindings from an onboarding draft into assets/games/<game>/.",
    )
    parser.add_argument(
        "--publish-onboarding-batch",
        metavar="ROOT",
        help="Dry-run or apply publish across the latest onboarding draft per game under one root.",
    )
    parser.add_argument(
        "--validate-onboarding-publish",
        metavar="DRAFT_ROOT",
        help="Validate whether one onboarding draft is publish-ready and report blocking findings.",
    )
    parser.add_argument(
        "--report-onboarding-batch",
        metavar="ROOT",
        help="Summarize onboarding draft outputs under one root into a structured batch readiness report.",
    )
    parser.add_argument(
        "--fuse-clip-signals",
        nargs=2,
        metavar=("SOURCE", "GAME"),
        help="Fuse proxy and runtime signals into an auditable fused-analysis sidecar.",
    )
    parser.add_argument(
        "--match-roi-templates",
        nargs=2,
        metavar=("SOURCE", "GAME"),
        help="Run ROI template matching against a local clip using a published game pack.",
    )
    parser.add_argument(
        "--map-roi-events",
        nargs=2,
        metavar=("SOURCE", "GAME"),
        help="Map confirmed ROI template detections into standalone atomic events.",
    )
    parser.add_argument(
        "--analyze-roi-runtime",
        nargs=2,
        metavar=("SOURCE", "GAME"),
        help="Run ROI matching plus event mapping and persist a runtime-analysis sidecar.",
    )
    parser.add_argument(
        "--render-replay-viewer",
        metavar="RUNTIME_SIDECAR",
        help="Render a static HTML replay/debug viewer from one runtime-analysis sidecar.",
    )
    parser.add_argument(
        "--render-proxy-replay-viewer",
        metavar="PROXY_SIDECAR",
        help="Render a static HTML proxy replay/debug viewer from one proxy-scan sidecar.",
    )
    parser.add_argument(
        "--render-unified-replay-viewer",
        action="store_true",
        help="Render a unified static HTML replay/debug viewer from any provided proxy, runtime, and fused sidecars.",
    )
    parser.add_argument(
        "--launch-highlight-review-app",
        metavar="SIDECAR_ROOT",
        help="Launch a lightweight Gradio-backed local review app over sidecars and optional evaluation fixtures.",
    )
    parser.add_argument(
        "--compare-fixture-sidecars",
        metavar="FIXTURE_MANIFEST",
        help="Compare baseline and trial sidecar roots against one evaluation fixture manifest.",
    )
    parser.add_argument(
        "--run-fixture-trial",
        metavar="FIXTURE_MANIFEST",
        help="Run one manifest-driven fixture trial into deterministic proxy/runtime/fused sidecar roots.",
    )
    parser.add_argument(
        "--compare-fixture-trials",
        metavar="FIXTURE_MANIFEST",
        help="Compare baseline and trial fixture run roots using the existing fixture sidecar comparison flow.",
    )
    parser.add_argument(
        "--run-fixture-trial-batch",
        metavar="FIXTURE_MANIFEST",
        help="Run baseline plus one or more cheap-stage fixture trial presets, then compare them into one batch bundle.",
    )
    parser.add_argument(
        "--proxy-sidecar",
        metavar="PATH",
        help="Optional proxy sidecar path used by --fuse-clip-signals and --render-unified-replay-viewer.",
    )
    parser.add_argument(
        "--runtime-sidecar",
        metavar="PATH",
        help="Optional runtime sidecar path used by --fuse-clip-signals and --render-unified-replay-viewer.",
    )
    parser.add_argument(
        "--fused-sidecar",
        metavar="PATH",
        help="Optional fused sidecar path used by --render-replay-viewer and --render-unified-replay-viewer.",
    )
    parser.add_argument(
        "--proxy-calibration-report",
        metavar="PATH",
        help="Optional proxy calibration report used by --render-unified-replay-viewer and --launch-highlight-review-app.",
    )
    parser.add_argument(
        "--proxy-replay-report",
        metavar="PATH",
        help="Optional proxy replay report used by --render-unified-replay-viewer and --launch-highlight-review-app.",
    )
    parser.add_argument(
        "--runtime-calibration-report",
        metavar="PATH",
        help="Optional runtime calibration report used by --render-unified-replay-viewer and --launch-highlight-review-app.",
    )
    parser.add_argument(
        "--runtime-replay-report",
        metavar="PATH",
        help="Optional runtime replay report used by --render-unified-replay-viewer and --launch-highlight-review-app.",
    )
    parser.add_argument(
        "--fixture-manifest",
        metavar="PATH",
        help="Optional evaluation fixture manifest used by --launch-highlight-review-app.",
    )
    parser.add_argument(
        "--fixture-comparison-report",
        metavar="PATH",
        help="Optional fixture batch comparison report used by --render-unified-replay-viewer and --launch-highlight-review-app.",
    )
    parser.add_argument(
        "--fixture-trial-batch-manifest",
        metavar="PATH",
        help="Optional fixture trial batch manifest used by --render-unified-replay-viewer and --launch-highlight-review-app.",
    )
    parser.add_argument(
        "--baseline-sidecar-root",
        metavar="PATH",
        help="Baseline sidecar root used by --compare-fixture-sidecars.",
    )
    parser.add_argument(
        "--trial-sidecar-root",
        metavar="PATH",
        help="Trial sidecar root used by --compare-fixture-sidecars.",
    )
    parser.add_argument(
        "--trial",
        action="append",
        metavar="NAME",
        help="Repeated fixture trial preset used by --run-fixture-trial-batch.",
    )
    parser.add_argument(
        "--batch-name",
        metavar="NAME",
        help="Optional batch name used by --run-fixture-trial-batch.",
    )
    parser.add_argument(
        "--baseline-run-root",
        metavar="PATH",
        help="Baseline fixture run root used by --compare-fixture-trials.",
    )
    parser.add_argument(
        "--trial-run-root",
        metavar="PATH",
        help="Trial fixture run root used by --compare-fixture-trials.",
    )
    parser.add_argument(
        "--fixture-source-manifest",
        metavar="PATH",
        help="Fixture source manifest used by --run-fixture-trial.",
    )
    parser.add_argument(
        "--artifact-layer",
        metavar="NAME",
        default="all",
        help="Artifact layer selector for fixture comparison commands: proxy, runtime, fused, or all.",
    )
    parser.add_argument(
        "--output-root",
        metavar="PATH",
        help="Optional output root used by --run-fixture-trial.",
    )
    parser.add_argument(
        "--proposal-backend",
        metavar="NAME",
        choices=["transnetv2", "pyscenedetect"],
        help="Optional proposal backend override used by --run-fixture-trial.",
    )
    parser.add_argument(
        "--asr-backend",
        metavar="NAME",
        choices=["whisper", "distil_whisper"],
        help="Optional ASR backend override used by --run-fixture-trial.",
    )
    parser.add_argument(
        "--emit-runtime",
        action="store_true",
        help="Request runtime-analysis generation during --run-fixture-trial when the fixture allows it.",
    )
    parser.add_argument(
        "--emit-fused",
        action="store_true",
        help="Request fused-analysis generation during --run-fixture-trial when the fixture allows it.",
    )
    parser.add_argument(
        "--proxy-sidecar-root",
        metavar="PATH",
        help="Optional proxy sidecar root used by --validate-fusion-goldset and --replay-fusion-rules.",
    )
    parser.add_argument(
        "--runtime-sidecar-root",
        metavar="PATH",
        help="Optional runtime sidecar root used by --validate-fusion-goldset and --replay-fusion-rules.",
    )
    parser.add_argument(
        "--fused-sidecar-root",
        metavar="PATH",
        help="Optional fused sidecar root used by --validate-fusion-goldset.",
    )
    parser.add_argument(
        "--media-root",
        metavar="PATH",
        help="Optional clip media root used by gold-set validation and replay commands for relative gold-manifest sources.",
    )
    parser.add_argument(
        "--matcher-report",
        metavar="PATH",
        help="Optional matcher report path used by --map-roi-events and --analyze-roi-runtime.",
    )
    parser.add_argument(
        "--sample-fps",
        metavar="FPS",
        type=float,
        help="Optional sample FPS used by --match-roi-templates, --map-roi-events, and --analyze-roi-runtime.",
    )
    parser.add_argument(
        "--limit-frames",
        metavar="N",
        type=int,
        help="Optional frame limit used by --match-roi-templates, --map-roi-events, and --analyze-roi-runtime.",
    )
    parser.add_argument(
        "--output-path",
        metavar="PATH",
        help="Optional JSON output path used by runtime ROI analysis, calibration, and tuning commands.",
    )
    parser.add_argument(
        "--min-score",
        metavar="SCORE",
        type=float,
        help="Optional global minimum score used by --match-roi-templates.",
    )
    parser.add_argument(
        "--debug-output-dir",
        metavar="PATH",
        help="Optional debug artifact directory used by runtime ROI analysis, calibration, and tuning commands.",
    )
    parser.add_argument(
        "--min-reviewed",
        metavar="N",
        type=int,
        default=3,
        help="Minimum reviewed runtime sidecar count required for calibration recommendations.",
    )
    parser.add_argument(
        "--include-unreviewed",
        action="store_true",
        help="Include unlabeled runtime sidecars in calibration coverage summaries only.",
    )
    parser.add_argument(
        "--trial-proxy-config",
        metavar="PATH",
        help="Trial proxy scoring config path used by --replay-proxy-scoring.",
    )
    parser.add_argument(
        "--trial-config",
        metavar="PATH",
        help="Trial scoring config path used by --replay-runtime-scoring.",
    )
    parser.add_argument(
        "--trial-rules",
        metavar="PATH",
        help="Trial fusion rules path used by --replay-fusion-rules.",
    )
    parser.add_argument(
        "--trial-templates",
        metavar="PATH",
        help="Trial template override path used by --replay-template-thresholds.",
    )
    parser.add_argument(
        "--trial-runtime-rules",
        metavar="PATH",
        help="Trial runtime rule override path used by --replay-runtime-event-rules.",
    )
    parser.add_argument(
        "--trial-name",
        metavar="NAME",
        help="Optional trial label used by --replay-runtime-scoring.",
    )
    parser.add_argument(
        "--rollback-name",
        metavar="NAME",
        help="Optional label used when writing a fresh rollback snapshot.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow promotion to proceed even when replay validation blocks it.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply a dry-run capable batch action instead of only reporting what would happen.",
    )
    parser.add_argument(
        "--check-roi-runtime",
        action="store_true",
        help="Report whether OpenCV, NumPy, and FFmpeg are available for ROI matching.",
    )
    parser.add_argument(
        "--audit-pipeline-contracts",
        action="store_true",
        help="Audit published-pack, runtime, fusion, and config contract drift plus legacy compatibility surfaces.",
    )
    parser.add_argument(
        "--validate-published-pack",
        metavar="GAME",
        help="Validate a published game pack for ROI matcher runtime use.",
    )
    parser.add_argument(
        "--list-pack-templates",
        metavar="GAME",
        help="List published pack templates grouped by ROI for runtime inspection.",
    )
    args = parser.parse_args()

    if args.list_games:
        print(json.dumps({"ok": True, "games": list_games()}, indent=2))
        return 0

    if args.init_game:
        print(json.dumps(init_game_pack(args.init_game), indent=2))
        return 0

    if args.validate_game_pack:
        print(json.dumps(validate_game_pack(args.validate_game_pack), indent=2))
        return 0

    if args.scan_chat_log:
        log_path, game = args.scan_chat_log
        print(json.dumps(run_scan_chat_log(Path(log_path), game), indent=2))
        return 0

    if args.scan_vod:
        source, game = args.scan_vod
        print(json.dumps(run_scan_vod(source, game, chat_log=args.chat_log), indent=2))
        return 0

    if args.scan_vod_batch:
        root, game = args.scan_vod_batch
        print(json.dumps(run_scan_vod_batch(root, game, pattern=args.pattern, limit=args.limit), indent=2))
        return 0

    if args.export_training_data:
        print(json.dumps(run_export_training_data(args.export_training_data, game=args.game), indent=2))
        return 0

    if args.export_runtime_analysis:
        print(json.dumps(run_export_runtime_analysis(args.export_runtime_analysis, game=args.game), indent=2))
        return 0

    if args.export_fused_analysis:
        print(json.dumps(run_export_fused_analysis(args.export_fused_analysis, game=args.game), indent=2))
        return 0

    if args.compare_fixture_sidecars:
        if not args.baseline_sidecar_root or not args.trial_sidecar_root:
            parser.error("--compare-fixture-sidecars requires --baseline-sidecar-root and --trial-sidecar-root")
        print(
            json.dumps(
                run_compare_fixture_sidecars(
                    args.compare_fixture_sidecars,
                    baseline_sidecar_root=args.baseline_sidecar_root,
                    trial_sidecar_root=args.trial_sidecar_root,
                    artifact_layer=args.artifact_layer,
                    game=args.game,
                    output_path=args.output_path,
                ),
                indent=2,
            )
        )
        return 0

    if args.run_fixture_trial:
        if not args.fixture_source_manifest or not args.trial_name:
            parser.error("--run-fixture-trial requires --fixture-source-manifest and --trial-name")
        result = run_fixture_trial(
            args.run_fixture_trial,
            fixture_source_manifest=args.fixture_source_manifest,
            trial_name=args.trial_name,
            output_root=args.output_root,
            game=args.game,
            pattern=args.pattern,
            limit=args.limit,
            proposal_backend=args.proposal_backend,
            asr_backend=args.asr_backend,
            emit_runtime=args.emit_runtime,
            emit_fused=args.emit_fused,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.compare_fixture_trials:
        if not args.baseline_run_root or not args.trial_run_root:
            parser.error("--compare-fixture-trials requires --baseline-run-root and --trial-run-root")
        result = run_compare_fixture_trials(
            args.compare_fixture_trials,
            baseline_run_root=args.baseline_run_root,
            trial_run_root=args.trial_run_root,
            artifact_layer=args.artifact_layer,
            game=args.game,
            output_path=args.output_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.run_fixture_trial_batch:
        if not args.fixture_source_manifest:
            parser.error("--run-fixture-trial-batch requires --fixture-source-manifest")
        result = run_fixture_trial_batch(
            args.run_fixture_trial_batch,
            fixture_source_manifest=args.fixture_source_manifest,
            trial_names=args.trial,
            batch_name=args.batch_name,
            output_root=args.output_root,
            game=args.game,
            pattern=args.pattern,
            limit=args.limit,
            emit_runtime=args.emit_runtime,
            emit_fused=args.emit_fused,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.export_highlight_selection:
        result = run_export_highlight_selection(
            args.export_highlight_selection,
            output_path=args.output_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.refresh_clip_registry:
        result = run_refresh_clip_registry(
            args.refresh_clip_registry,
            game=args.game,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            registry_path=args.registry_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.query_clip_registry:
        result = run_query_clip_registry(
            mode=args.mode or "fused-events",
            game=args.game,
            event_type=args.event_type,
            action=args.action,
            review_status=args.review_status,
            gate_status=args.gate_status,
            fixture_id=args.fixture_id,
            trial_name=args.trial_name,
            artifact_layer=args.artifact_layer,
            recommendation_decision=args.recommendation_decision,
            coverage_status=args.coverage_status,
            has_disagreement=True if args.has_disagreement else None,
            limit=args.limit,
            registry_path=args.registry_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.calibrate_proxy_review:
        result = run_calibrate_proxy_review(
            args.calibrate_proxy_review,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.replay_proxy_scoring:
        if not args.trial_proxy_config:
            parser.error("--replay-proxy-scoring requires --trial-proxy-config")
        result = run_replay_proxy_scoring(
            args.replay_proxy_scoring,
            args.trial_proxy_config,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
            trial_name=args.trial_name,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.calibrate_runtime_review:
        result = run_calibrate_runtime_review(
            args.calibrate_runtime_review,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.replay_runtime_scoring:
        if not args.trial_config:
            parser.error("--replay-runtime-scoring requires --trial-config")
        result = run_replay_runtime_scoring(
            args.replay_runtime_scoring,
            args.trial_config,
            game=args.game,
            output_path=args.output_path,
            min_reviewed=args.min_reviewed,
            include_unreviewed=args.include_unreviewed,
            debug_output_dir=args.debug_output_dir,
            trial_name=args.trial_name,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.validate_fusion_goldset:
        result = run_validate_fusion_goldset(
            args.validate_fusion_goldset,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            proxy_sidecar_root=args.proxy_sidecar_root,
            runtime_sidecar_root=args.runtime_sidecar_root,
            fused_sidecar_root=args.fused_sidecar_root,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.replay_fusion_rules:
        if not args.trial_rules:
            parser.error("--replay-fusion-rules requires --trial-rules")
        result = run_replay_fusion_rules(
            args.replay_fusion_rules,
            args.trial_rules,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            proxy_sidecar_root=args.proxy_sidecar_root,
            runtime_sidecar_root=args.runtime_sidecar_root,
            trial_name=args.trial_name,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.replay_template_thresholds:
        if not args.trial_templates:
            parser.error("--replay-template-thresholds requires --trial-templates")
        result = run_replay_template_thresholds(
            args.replay_template_thresholds,
            args.trial_templates,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            trial_name=args.trial_name,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.replay_runtime_event_rules:
        if not args.trial_runtime_rules:
            parser.error("--replay-runtime-event-rules requires --trial-runtime-rules")
        result = run_replay_runtime_event_rules(
            args.replay_runtime_event_rules,
            args.trial_runtime_rules,
            game=args.game,
            media_root=args.media_root,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            trial_name=args.trial_name,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.promote_runtime_scoring:
        result = run_promote_runtime_scoring(
            args.promote_runtime_scoring,
            sidecar_root=args.sidecar_root,
            game=args.game,
            min_reviewed=args.min_reviewed,
            force=args.force,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            trial_name=args.trial_name,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.rollback_runtime_scoring:
        result = run_rollback_runtime_scoring(
            args.rollback_runtime_scoring,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            rollback_name=args.rollback_name,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.audit_pipeline_contracts:
        result = run_audit_pipeline_contracts(
            game=args.game,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.prepare_proxy_review:
        print(
            json.dumps(
                run_prepare_proxy_review(
                    args.prepare_proxy_review,
                    batch_report=args.batch_report,
                    sidecar_root=args.sidecar_root,
                    action=args.action or "download_candidate",
                    limit=args.limit,
                    gpt_repo=args.gpt_repo,
                    session_name=args.session_name,
                ),
                indent=2,
            )
        )
        return 0

    if args.prepare_runtime_review:
        print(
            json.dumps(
                run_prepare_runtime_review(
                    args.prepare_runtime_review,
                    sidecar_root=args.sidecar_root,
                    action=args.action,
                    limit=args.limit,
                    gpt_repo=args.gpt_repo,
                    session_name=args.session_name,
                ),
                indent=2,
            )
        )
        return 0

    if args.prepare_fused_review:
        print(
            json.dumps(
                run_prepare_fused_review(
                    args.prepare_fused_review,
                    sidecar_root=args.sidecar_root,
                    action=args.action,
                    limit=args.limit,
                    gpt_repo=args.gpt_repo,
                    session_name=args.session_name,
                    event_type=args.event_type,
                ),
                indent=2,
            )
        )
        return 0

    if args.render_replay_viewer:
        print(
            json.dumps(
                run_render_replay_viewer(
                    args.render_replay_viewer,
                    fused_sidecar=args.fused_sidecar,
                    output_path=args.output_path,
                ),
                indent=2,
            )
        )
        return 0

    if args.render_proxy_replay_viewer:
        print(
            json.dumps(
                run_render_proxy_replay_viewer(
                    args.render_proxy_replay_viewer,
                    output_path=args.output_path,
                ),
                indent=2,
            )
        )
        return 0

    if args.render_unified_replay_viewer:
        print(
            json.dumps(
                run_render_unified_replay_viewer(
                    proxy_sidecar=args.proxy_sidecar,
                    runtime_sidecar=args.runtime_sidecar,
                    fused_sidecar=args.fused_sidecar,
                    fixture_comparison_report=args.fixture_comparison_report,
                    fixture_trial_batch_manifest=args.fixture_trial_batch_manifest,
                    proxy_calibration_report=args.proxy_calibration_report,
                    proxy_replay_report=args.proxy_replay_report,
                    runtime_calibration_report=args.runtime_calibration_report,
                    runtime_replay_report=args.runtime_replay_report,
                    output_path=args.output_path,
                ),
                indent=2,
            )
        )
        return 0

    if args.launch_highlight_review_app:
        result = run_launch_highlight_review_app(
            args.launch_highlight_review_app,
            fixture_manifest=args.fixture_manifest,
            fixture_comparison_report=args.fixture_comparison_report,
            fixture_trial_batch_manifest=args.fixture_trial_batch_manifest,
            proxy_calibration_report=args.proxy_calibration_report,
            proxy_replay_report=args.proxy_replay_report,
            runtime_calibration_report=args.runtime_calibration_report,
            runtime_replay_report=args.runtime_replay_report,
            output_path=args.output_path,
            launch=True,
        )
        print(json.dumps({key: value for key, value in result.items() if key != "app"}, indent=2))
        return 0 if result.get("ok") else 1

    if args.apply_proxy_review:
        print(json.dumps(run_apply_proxy_review(args.apply_proxy_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.apply_runtime_review:
        print(json.dumps(run_apply_runtime_review(args.apply_runtime_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.apply_fused_review:
        print(json.dumps(run_apply_fused_review(args.apply_fused_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.cleanup_proxy_review:
        print(json.dumps(run_cleanup_proxy_review(args.cleanup_proxy_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.cleanup_runtime_review:
        print(json.dumps(run_cleanup_runtime_review(args.cleanup_runtime_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.cleanup_fused_review:
        print(json.dumps(run_cleanup_fused_review(args.cleanup_fused_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.prepare_onboarding_identity_review:
        print(
            json.dumps(
                run_prepare_onboarding_identity_review(
                    args.prepare_onboarding_identity_review,
                    gpt_repo=args.gpt_repo,
                    session_name=args.session_name,
                ),
                indent=2,
            )
        )
        return 0

    if args.apply_onboarding_identity_review:
        print(json.dumps(run_apply_onboarding_identity_review(args.apply_onboarding_identity_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.cleanup_onboarding_identity_review:
        print(json.dumps(run_cleanup_onboarding_identity_review(args.cleanup_onboarding_identity_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.enrich_game_from_wiki:
        if not args.wiki_url and not args.wiki_manifest and not args.wiki_source:
            parser.error("--enrich-game-from-wiki requires --wiki-url, --wiki-manifest, or --wiki-source")
        result = run_enrich_game_from_wiki(
            args.enrich_game_from_wiki,
            args.wiki_url,
            wiki_manifest=args.wiki_manifest,
            wiki_sources=args.wiki_source,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.adapt_game_schema:
        result = run_adapt_game_schema(args.adapt_game_schema)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.ingest_game_sources:
        if not args.source_manifest:
            parser.error("--ingest-game-sources requires --source-manifest")
        result = run_ingest_game_sources(args.ingest_game_sources, args.source_manifest)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.build_onboarding_draft:
        result = run_build_onboarding_draft(args.build_onboarding_draft)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.onboard_game:
        if not args.source_manifest:
            parser.error("--onboard-game requires --source-manifest")
        result = run_onboard_game(args.onboard_game, args.source_manifest)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.publish_onboarding_draft:
        result = run_publish_onboarding_draft(args.publish_onboarding_draft)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.publish_onboarding_batch:
        result = run_publish_onboarding_batch(
            args.publish_onboarding_batch,
            game=args.game,
            apply=args.apply,
            output_path=args.output_path,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.validate_onboarding_publish:
        result = run_validate_onboarding_publish(args.validate_onboarding_publish)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.report_onboarding_batch:
        result = run_report_onboarding_batch(args.report_onboarding_batch, game=args.game, output_path=args.output_path)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.fuse_clip_signals:
        source, game = args.fuse_clip_signals
        result = run_fuse_clip_signals(
            source,
            game,
            proxy_sidecar=args.proxy_sidecar,
            runtime_sidecar=args.runtime_sidecar,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.check_roi_runtime:
        result = run_check_roi_runtime()
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.validate_published_pack:
        result = run_validate_published_pack(args.validate_published_pack)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.list_pack_templates:
        result = run_list_pack_templates(args.list_pack_templates)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.match_roi_templates:
        source, game = args.match_roi_templates
        result = run_match_roi_templates(
            source,
            game,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            output_path=args.output_path,
            min_score=args.min_score,
            debug_output_dir=args.debug_output_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.map_roi_events:
        source, game = args.map_roi_events
        result = run_map_roi_events(
            source,
            game,
            matcher_report=args.matcher_report,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    if args.analyze_roi_runtime:
        source, game = args.analyze_roi_runtime
        result = run_analyze_roi_runtime(
            source,
            game,
            matcher_report=args.matcher_report,
            sample_fps=args.sample_fps,
            limit_frames=args.limit_frames,
            output_path=args.output_path,
            debug_output_dir=args.debug_output_dir,
        )
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok") else 1

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

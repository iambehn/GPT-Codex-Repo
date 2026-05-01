from __future__ import annotations

import argparse
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from pipeline.chat_scanner import scan_chat_log
from pipeline.event_mapper import EventMapperError, map_roi_events
from pipeline.game_pack import init_game_pack, list_games, load_game_pack, validate_game_pack
from pipeline.game_onboarding import onboard_game_from_manifest, publish_onboarding_draft
from pipeline.media_probe import probe_media_duration
from pipeline.proxy_registry import ProxyScanContext, run_proxy_sources
from pipeline.proxy_review_bridge import apply_proxy_review, cleanup_proxy_review, prepare_proxy_review
from pipeline.proxy_scanner import build_proxy_windows
from pipeline.roi_matcher import RoiMatcherError, match_roi_templates
from pipeline.roi_matcher import check_roi_runtime, list_pack_templates, validate_published_pack
from pipeline.runtime_calibration import calibrate_runtime_review
from pipeline.runtime_export import export_runtime_analysis
from pipeline.runtime_promotion import promote_runtime_scoring
from pipeline.runtime_analysis import RuntimeAnalysisError, analyze_roi_runtime
from pipeline.runtime_review_bridge import apply_runtime_review, cleanup_runtime_review, prepare_runtime_review
from pipeline.runtime_tuning import replay_runtime_scoring
from pipeline.simple_yaml import load_yaml_file
from pipeline.training_export import export_training_data
from pipeline.wiki_enrichment import WikiFetchError, WikiSource, enrich_game_from_sources, enrich_game_from_wiki


REPO_ROOT = Path(__file__).resolve().parent
PROXY_SCAN_SCHEMA_VERSION = "proxy_scan_v1"
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
        },
        "weights": {
            "chat_spike": 3.5,
            "playlist_spike": 2.5,
            "playlist_discontinuity": 2.0,
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


def _normalize_proxy_scanner_config(proxy_config: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(proxy_config)
    legacy_sources = normalized.pop("signals", {}) if isinstance(normalized.get("signals"), dict) else {}
    explicit_sources = normalized.get("sources", {}) if isinstance(normalized.get("sources"), dict) else {}
    merged_sources = _deep_merge(DEFAULT_CONFIG["proxy_scanner"]["sources"], legacy_sources)
    merged_sources = _deep_merge(merged_sources, explicit_sources)
    normalized["sources"] = merged_sources
    return normalized


def run_scan_chat_log(log_path: Path, game: str) -> dict[str, Any]:
    config = _normalize_config(load_config())
    game_pack = load_game_pack(game)
    proxy_cfg = config.get("proxy_scanner", {})
    chat_cfg = proxy_cfg.get("sources", {}).get("chat_velocity", {})
    signals = scan_chat_log(log_path, chat_cfg)
    windows = build_proxy_windows(signals, proxy_cfg, media_duration_seconds=None)
    return {
        "ok": True,
        "game": game,
        "game_pack": game_pack.summary(),
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
    config = _normalize_config(load_config())
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


def run_apply_proxy_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return apply_proxy_review(session_manifest, gpt_repo=gpt_repo)


def run_apply_runtime_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return apply_runtime_review(session_manifest, gpt_repo=gpt_repo)


def run_cleanup_proxy_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return cleanup_proxy_review(session_manifest, gpt_repo=gpt_repo)


def run_cleanup_runtime_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return cleanup_runtime_review(session_manifest, gpt_repo=gpt_repo)


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


def run_check_roi_runtime() -> dict[str, Any]:
    return check_roi_runtime()


def run_validate_published_pack(game: str) -> dict[str, Any]:
    try:
        return validate_published_pack(game)
    except RoiMatcherError as exc:
        return exc.to_dict(game=game)
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "status": "missing_game_pack",
            "game": game,
            "error": str(exc),
        }


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
    parser.add_argument("--pattern", metavar="GLOB", default="*.mp4", help="Optional glob used by --scan-vod-batch.")
    parser.add_argument("--limit", metavar="N", type=int, help="Optional file limit used by --scan-vod-batch.")
    parser.add_argument("--action", metavar="NAME", default="download_candidate", help="Optional action filter used by --prepare-proxy-review.")
    parser.add_argument("--batch-report", metavar="PATH", help="Optional batch report path used by --prepare-proxy-review.")
    parser.add_argument("--sidecar-root", metavar="PATH", help="Optional sidecar root used by --prepare-proxy-review.")
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
        "--promote-runtime-scoring",
        metavar="TRIAL_CONFIG",
        help="Promote a runtime trial scoring config into the active repo config after replay validation.",
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
        "--trial-config",
        metavar="PATH",
        help="Trial scoring config path used by --replay-runtime-scoring.",
    )
    parser.add_argument(
        "--trial-name",
        metavar="NAME",
        help="Optional trial label used by --replay-runtime-scoring.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow promotion to proceed even when replay validation blocks it.",
    )
    parser.add_argument(
        "--check-roi-runtime",
        action="store_true",
        help="Report whether OpenCV, NumPy, and FFmpeg are available for ROI matching.",
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

    if args.prepare_proxy_review:
        print(
            json.dumps(
                run_prepare_proxy_review(
                    args.prepare_proxy_review,
                    batch_report=args.batch_report,
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

    if args.apply_proxy_review:
        print(json.dumps(run_apply_proxy_review(args.apply_proxy_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.apply_runtime_review:
        print(json.dumps(run_apply_runtime_review(args.apply_runtime_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.cleanup_proxy_review:
        print(json.dumps(run_cleanup_proxy_review(args.cleanup_proxy_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.cleanup_runtime_review:
        print(json.dumps(run_cleanup_runtime_review(args.cleanup_runtime_review, gpt_repo=args.gpt_repo), indent=2))
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

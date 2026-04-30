from __future__ import annotations

import argparse
import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any

from pipeline.chat_scanner import scan_chat_log
from pipeline.game_pack import init_game_pack, list_games, load_game_pack, validate_game_pack
from pipeline.media_probe import probe_media_duration
from pipeline.proxy_registry import ProxyScanContext, run_proxy_sources
from pipeline.proxy_review_bridge import apply_proxy_review, cleanup_proxy_review, prepare_proxy_review
from pipeline.proxy_scanner import build_proxy_windows
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
    }
}


def load_config() -> dict[str, Any]:
    path = REPO_ROOT / "config.yaml"
    if not path.exists():
        return _normalize_config(DEFAULT_CONFIG)
    loaded = load_yaml_file(path)
    return _normalize_config(_deep_merge(DEFAULT_CONFIG, loaded))


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


def run_apply_proxy_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return apply_proxy_review(session_manifest, gpt_repo=gpt_repo)


def run_cleanup_proxy_review(session_manifest: str | Path, *, gpt_repo: str | Path | None = None) -> dict[str, Any]:
    return cleanup_proxy_review(session_manifest, gpt_repo=gpt_repo)


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
        "--apply-proxy-review",
        metavar="SESSION_MANIFEST",
        help="Import GPT-Codex review decisions back into proxy sidecars.",
    )
    parser.add_argument(
        "--cleanup-proxy-review",
        metavar="SESSION_MANIFEST",
        help="Remove generated GPT-Codex review bridge artifacts for a session.",
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

    if args.apply_proxy_review:
        print(json.dumps(run_apply_proxy_review(args.apply_proxy_review, gpt_repo=args.gpt_repo), indent=2))
        return 0

    if args.cleanup_proxy_review:
        print(json.dumps(run_cleanup_proxy_review(args.cleanup_proxy_review, gpt_repo=args.gpt_repo), indent=2))
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

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

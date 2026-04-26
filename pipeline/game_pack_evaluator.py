from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from pipeline.audio_detector import run_audio_detector
from pipeline.clip_judge import evaluate as evaluate_clip
from pipeline.game_pack import get_game_pack_dir, load_game_pack, validate_game_pack
from pipeline.hook_enforcer import run_hook_enforcer
from pipeline.kill_feed import run_kill_feed_parser
from pipeline.niceshot_detector import run_niceshot_detector
from pipeline.weapon_detector import run_weapon_detector
from pipeline.yolo_detector import run_yolo_detector
from utils.logger import get_logger

logger = get_logger(__name__)

_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}


def evaluate_game_pack(
    game: str,
    config: dict,
    *,
    run_detectors: bool = True,
    force: bool = True,
) -> dict[str, Any]:
    """Evaluate one game pack against its labeled gold set.

    The evaluator is intentionally non-destructive: each clip is evaluated from
    a temporary symlink/copy plus copied sidecar metadata, so detector and judge
    writes never mutate the source gold-set files.
    """
    pack_dir = get_game_pack_dir(game, config)
    manifest_path = pack_dir / "examples" / "gold_set" / "manifest.yaml"
    report_dir = pack_dir / "reports" / "evaluation"
    report_dir.mkdir(parents=True, exist_ok=True)

    validation = validate_game_pack(game, config)
    if not manifest_path.exists():
        result = _empty_result(game, manifest_path, validation, "failed")
        result["errors"].append(f"missing gold-set manifest: {manifest_path}")
        return _write_report(report_dir, result)

    manifest = _load_yaml(manifest_path)
    items = _manifest_items(manifest)
    if not items:
        result = _empty_result(game, manifest_path, validation, "failed")
        result["errors"].append("gold-set manifest has no clips")
        return _write_report(report_dir, result)

    game_pack = load_game_pack(game, config)
    cases: list[dict[str, Any]] = []
    for item in items:
        cases.append(_evaluate_item(item, game, config, game_pack, manifest_path, run_detectors, force))

    summary = _summarize_cases(cases)
    status = "ok" if not summary["failures"] else "partial"
    result = {
        "game": game,
        "status": status,
        "generated_at": _now_iso(),
        "manifest_path": str(manifest_path),
        "validation": validation,
        "run_detectors": run_detectors,
        "force": force,
        "summary": summary,
        "cases": cases,
        "errors": [],
    }
    return _write_report(report_dir, result)


def scaffold_gold_set(game: str, config: dict) -> dict[str, Any]:
    """Create the gold-set folder contract without adding real clips."""
    pack_dir = get_game_pack_dir(game, config)
    gold_dir = pack_dir / "examples" / "gold_set"
    clips_dir = gold_dir / "clips"
    sidecars_dir = gold_dir / "sidecars"
    clips_dir.mkdir(parents=True, exist_ok=True)
    sidecars_dir.mkdir(parents=True, exist_ok=True)

    for directory in (clips_dir, sidecars_dir):
        keep = directory / ".gitkeep"
        if not keep.exists():
            keep.write_text("")

    manifest = gold_dir / "manifest.yaml"
    readme = gold_dir / "README.md"
    written: list[str] = []
    if not manifest.exists():
        manifest.write_text(yaml.safe_dump(_example_manifest(game), sort_keys=False))
        written.append(str(manifest))
    if not readme.exists():
        readme.write_text(_gold_set_readme(game))
        written.append(str(readme))

    return {"gold_set_dir": str(gold_dir), "written": written}


def _evaluate_item(
    item: dict[str, Any],
    game: str,
    config: dict,
    game_pack: dict,
    manifest_path: Path,
    run_detectors: bool,
    force: bool,
) -> dict[str, Any]:
    case_id = str(item.get("id") or item.get("clip") or "unnamed")
    expected = item.get("expected") or {}
    errors: list[str] = []

    clip_path = _resolve_case_path(item.get("clip"), manifest_path)
    if clip_path is None or not clip_path.exists():
        return _case_result(case_id, expected, None, errors=[f"missing clip: {item.get('clip')}"])
    if clip_path.suffix.lower() not in _VIDEO_EXTENSIONS:
        errors.append(f"unsupported clip extension: {clip_path.suffix}")

    source_meta = _resolve_case_path(item.get("meta"), manifest_path)
    if source_meta is None:
        source_meta = clip_path.with_suffix(".meta.json")

    with tempfile.TemporaryDirectory(prefix=f"gold-{game}-") as tmp_name:
        work_clip = Path(tmp_name) / clip_path.name
        _safe_link_or_copy(clip_path, work_clip)
        work_meta = work_clip.with_suffix(".meta.json")
        if source_meta.exists():
            shutil.copy2(source_meta, work_meta)
        else:
            work_meta.write_text(json.dumps(_minimal_meta(case_id, game, work_clip), indent=2))

        _normalize_meta_paths(work_meta, case_id, game, work_clip)

        if run_detectors:
            _run_eval_detectors(work_clip, game, config, game_pack, force=force)
        else:
            run_hook_enforcer(work_clip, game, config, game_pack=game_pack, force=force)

        decision_manifest = evaluate_clip(work_clip, game_pack, config, force=force)
        meta = json.loads(work_meta.read_text())

    actual = {
        "status": (decision_manifest.get("decision") or {}).get("status"),
        "quarantine_reason": (decision_manifest.get("quarantine") or {}).get("reason"),
        "composite_score": (decision_manifest.get("decision") or {}).get("composite_score"),
        "hook_gate_passed": (decision_manifest.get("decision") or {}).get("hook_gate_passed"),
        "player_entity": (decision_manifest.get("context") or {}).get("player_entity"),
        "detected_event": (decision_manifest.get("context") or {}).get("detected_event"),
    }
    comparisons = _compare_expected(expected, actual)
    passed = not errors and all(item["passed"] for item in comparisons)
    return _case_result(
        case_id,
        expected,
        actual,
        passed=passed,
        comparisons=comparisons,
        errors=errors,
        detector_statuses=_detector_statuses(meta),
    )


def _run_eval_detectors(clip: Path, game: str, config: dict, game_pack: dict, force: bool) -> None:
    try:
        run_audio_detector(clip, game, config, force=force)
    except Exception as e:
        logger.warning(f"[game_pack_evaluator] audio detector skipped for {clip.name}: {e}")
    try:
        run_kill_feed_parser(clip, game, config, force=force)
    except Exception as e:
        logger.warning(f"[game_pack_evaluator] kill-feed detector skipped for {clip.name}: {e}")
    try:
        run_weapon_detector(clip, game, config, force=force)
    except Exception as e:
        logger.warning(f"[game_pack_evaluator] weapon detector skipped for {clip.name}: {e}")

    run_niceshot_detector(clip, game, config, game_pack=game_pack, force=force)
    run_yolo_detector(clip, game, config, game_pack=game_pack, force=force)
    run_hook_enforcer(clip, game, config, game_pack=game_pack, force=force)


def _compare_expected(expected: dict[str, Any], actual: dict[str, Any]) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []
    for key in ("status", "quarantine_reason", "hook_gate_passed", "player_entity", "detected_event"):
        if key not in expected:
            continue
        comparisons.append({
            "field": key,
            "expected": expected.get(key),
            "actual": actual.get(key),
            "passed": expected.get(key) == actual.get(key),
        })
    return comparisons


def _summarize_cases(cases: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(cases)
    passed = sum(1 for case in cases if case.get("passed"))
    by_expected: dict[str, int] = {}
    by_actual: dict[str, int] = {}
    failures: list[str] = []
    for case in cases:
        expected_status = ((case.get("expected") or {}).get("status")) or "unspecified"
        actual_status = ((case.get("actual") or {}).get("status")) or "missing"
        by_expected[expected_status] = by_expected.get(expected_status, 0) + 1
        by_actual[actual_status] = by_actual.get(actual_status, 0) + 1
        if not case.get("passed"):
            failures.append(case.get("id", "unknown"))
    return {
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": round(passed / total, 3) if total else 0.0,
        "by_expected_status": by_expected,
        "by_actual_status": by_actual,
        "failures": failures,
    }


def _case_result(
    case_id: str,
    expected: dict[str, Any],
    actual: dict[str, Any] | None,
    *,
    passed: bool = False,
    comparisons: list[dict[str, Any]] | None = None,
    errors: list[str] | None = None,
    detector_statuses: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": case_id,
        "passed": passed,
        "expected": expected,
        "actual": actual,
        "comparisons": comparisons or [],
        "detector_statuses": detector_statuses or {},
        "errors": errors or [],
    }


def _empty_result(game: str, manifest_path: Path, validation: dict[str, Any], status: str) -> dict[str, Any]:
    return {
        "game": game,
        "status": status,
        "generated_at": _now_iso(),
        "manifest_path": str(manifest_path),
        "validation": validation,
        "summary": {"total": 0, "passed": 0, "failed": 0, "pass_rate": 0.0, "failures": []},
        "cases": [],
        "errors": [],
    }


def _write_report(report_dir: Path, result: dict[str, Any]) -> dict[str, Any]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    report_path = report_dir / f"{timestamp}.json"
    latest_path = report_dir / "latest.json"
    result["report_path"] = str(report_path)
    report_path.write_text(json.dumps(result, indent=2))
    latest_path.write_text(json.dumps(result, indent=2))
    return result


def _manifest_items(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    clips = manifest.get("clips") or []
    return [item for item in clips if isinstance(item, dict)]


def _resolve_case_path(raw_path: Any, manifest_path: Path) -> Path | None:
    if not raw_path:
        return None
    path = Path(str(raw_path)).expanduser()
    if path.is_absolute():
        return path
    return (manifest_path.parent / path).resolve()


def _safe_link_or_copy(source: Path, target: Path) -> None:
    try:
        target.symlink_to(source)
    except OSError:
        shutil.copy2(source, target)


def _normalize_meta_paths(meta_path: Path, case_id: str, game: str, clip: Path) -> None:
    try:
        meta = json.loads(meta_path.read_text())
    except json.JSONDecodeError:
        meta = {}
    meta.setdefault("clip_id", case_id)
    meta.setdefault("game", game)
    meta["clip_path"] = str(clip)
    meta["meta_path"] = str(meta_path)
    meta_path.write_text(json.dumps(meta, indent=2))


def _minimal_meta(case_id: str, game: str, clip: Path) -> dict[str, Any]:
    return {"clip_id": case_id, "game": game, "clip_path": str(clip)}


def _detector_statuses(meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "niceshot": (meta.get("niceshot_detection") or {}).get("status"),
        "yolo": (meta.get("yolo_detection") or {}).get("status"),
        "hook_enforcer": (meta.get("hook_enforcer") or {}).get("status"),
        "weapon_detector": (meta.get("weapon_detection") or {}).get("method"),
        "kill_feed": (meta.get("kill_feed") or {}).get("passed"),
    }


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        return yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as e:
        return {"clips": [], "warnings": [f"malformed manifest: {e}"]}


def _example_manifest(game: str) -> dict[str, Any]:
    return {
        "game": game,
        "description": "Gold-set clips for measuring game-pack detector and judge quality.",
        "clips": [
            {
                "id": "example_accept_replace_me",
                "clip": "clips/example_accept_replace_me.mp4",
                "meta": "sidecars/example_accept_replace_me.meta.json",
                "expected": {
                    "status": "accept",
                    "hook_gate_passed": True,
                    "player_entity": "replace_with_entity_id",
                    "detected_event": "replace_with_moment_id",
                },
            }
        ],
    }


def _gold_set_readme(game: str) -> str:
    return f"""# {game} Gold Set

Use this folder for small, labeled evaluation clips before training YOLO or
tuning clip-judge weights.

Suggested starting target:
- 10 strong accepts with obvious first-second hooks.
- 10 rejects with weak action or no narrative payoff.
- 10 quarantines where context, hook, UI drift, or ROI templates are unresolved.

Manifest fields:
- `clip`: path to a short source clip, relative to this folder or absolute.
- `meta`: optional sidecar metadata fixture. If omitted, the evaluator uses the
  clip's adjacent `.meta.json` or creates a minimal temp sidecar.
- `expected.status`: `accept`, `reject`, or `quarantine`.
- `expected.quarantine_reason`: optional expected quarantine reason.
- `expected.hook_gate_passed`, `expected.player_entity`, and
  `expected.detected_event`: optional stricter checks.

Run:
`python run.py --evaluate-game-pack {game}`
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from pipeline.clip_registry import load_candidate_lifecycle_details


REPO_ROOT = Path(__file__).resolve().parent.parent
HOOK_CANDIDATE_SCHEMA_VERSION = "hook_candidate_v1"
SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION = "fused_analysis_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "hook_candidate_exports"
ELIGIBLE_LIFECYCLE_STATES = {"approved", "selected_for_export"}
HOOK_ARCHETYPES = ("clutch", "reversal", "domination", "comedy", "chaos", "fail", "flex", "other")


def derive_hook_candidates(
    fused_sidecar: str | Path,
    *,
    registry_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    sidecar_path = _resolve_path(fused_sidecar)
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SUPPORTED_FUSED_ANALYSIS_SCHEMA_VERSION:
        return {
            "ok": False,
            "status": "invalid_fused_sidecar",
            "fused_sidecar_path": str(sidecar_path),
            "error": "fused sidecar does not use fused_analysis_v1",
        }

    game = str(payload.get("game", "")).strip() or "unknown_game"
    source = str(payload.get("source", "")).strip()
    lifecycle_rows = load_candidate_lifecycle_details(
        game=game or None,
        source=source or None,
        fused_sidecar_path=sidecar_path,
        registry_path=registry_path,
    )
    lifecycle_by_event_id = {
        str(row.get("event_id") or "").strip(): row
        for row in lifecycle_rows
        if str(row.get("event_id") or "").strip()
    }

    hook_candidates: list[dict[str, Any]] = []
    ineligible_count = 0
    missing_lifecycle_count = 0
    for index, event in enumerate(list(payload.get("fused_events", []))):
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            continue
        lifecycle_row = lifecycle_by_event_id.get(event_id)
        if lifecycle_row is None:
            missing_lifecycle_count += 1
            continue
        lifecycle_state = str(lifecycle_row.get("lifecycle_state") or "").strip()
        if lifecycle_state not in ELIGIBLE_LIFECYCLE_STATES:
            ineligible_count += 1
            continue
        hook_candidates.append(_derive_hook_candidate(index=index, event=event, lifecycle_row=lifecycle_row))

    manifest = {
        "schema_version": HOOK_CANDIDATE_SCHEMA_VERSION,
        "game": game,
        "source": source,
        "fused_sidecar_path": str(sidecar_path),
        "hook_candidate_count": len(hook_candidates),
        "hook_candidates": hook_candidates,
    }
    target = _resolve_path(output_path) if output_path is not None else _default_output_path(game, source)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return {
        "ok": True,
        "status": "ok",
        "schema_version": HOOK_CANDIDATE_SCHEMA_VERSION,
        "manifest_path": str(target),
        "fused_sidecar_path": str(sidecar_path),
        "hook_candidate_count": len(hook_candidates),
        "eligible_lifecycle_states": sorted(ELIGIBLE_LIFECYCLE_STATES),
        "missing_lifecycle_count": missing_lifecycle_count,
        "ineligible_lifecycle_count": ineligible_count,
    }


def _derive_hook_candidate(*, index: int, event: dict[str, Any], lifecycle_row: dict[str, Any]) -> dict[str, Any]:
    final_score = _clamp(float(event.get("final_score", event.get("confidence", 0.0)) or 0.0))
    gate_status = str(event.get("gate_status") or "").strip() or None
    lifecycle_state = str(lifecycle_row.get("lifecycle_state") or "").strip()
    metadata = event.get("metadata", {}) if isinstance(event.get("metadata"), dict) else {}
    contributing_signal_ids = [str(value) for value in list(event.get("contributing_signals", [])) if str(value).strip()]
    event_type = str(event.get("event_type") or "").strip()
    has_entity = bool(str(metadata.get("entity_id") or "").strip())
    has_ability = bool(str(metadata.get("ability_id") or "").strip())
    signal_count = len(contributing_signal_ids)
    synergy_applied = bool(event.get("synergy_applied", False))

    intensity_score = _clamp(final_score + (0.06 if gate_status == "confirmed" else 0.0) + (0.04 if synergy_applied else 0.0))
    clarity_score = _clamp(0.4 + (0.22 if gate_status == "confirmed" else 0.0) + (0.12 if signal_count >= 2 else 0.0) + (0.1 if has_entity else 0.0))
    novelty_score = _clamp(0.35 + (0.18 if synergy_applied else 0.0) + (0.1 if "combo" in event_type else 0.0) + (0.08 if signal_count >= 3 else 0.0))
    context_sufficiency_score = _clamp(0.25 + (0.18 if has_entity else 0.0) + (0.14 if has_ability else 0.0) + (0.12 if signal_count >= 2 else 0.0) + (0.08 if lifecycle_state == "selected_for_export" else 0.0))
    payoff_readability_score = _clamp(0.3 + (0.3 * final_score) + (0.18 if gate_status == "confirmed" else 0.0) + (0.08 if "medal" in event_type or "combo" in event_type else 0.0))
    title_thumbnail_potential_score = _clamp(0.28 + (0.18 if has_entity else 0.0) + (0.14 if "combo" in event_type or "clutch" in event_type else 0.0) + (0.12 if final_score >= 0.85 else 0.0))
    sound_off_legibility_score = _clamp(0.3 + (0.2 if has_entity else 0.0) + (0.18 if gate_status == "confirmed" else 0.0) + (0.12 if payoff_readability_score >= 0.7 else 0.0))
    authenticity_risk_score = _clamp(
        0.92
        - (0.28 * clarity_score)
        - (0.22 * context_sufficiency_score)
        - (0.18 * payoff_readability_score)
        + (0.08 if signal_count < 2 else 0.0)
        + (0.06 if not has_entity else 0.0)
    )

    hook_archetype = _hook_archetype(event_type=event_type, final_score=final_score, signal_count=signal_count)
    hook_strength = _clamp(
        (0.2 * intensity_score)
        + (0.16 * clarity_score)
        + (0.12 * novelty_score)
        + (0.16 * context_sufficiency_score)
        + (0.18 * payoff_readability_score)
        + (0.18 * title_thumbnail_potential_score)
        - (0.15 * authenticity_risk_score)
    )
    hook_mode, packaging_strategy, rejection_reason = _hook_mode_and_strategy(
        hook_strength=hook_strength,
        clarity_score=clarity_score,
        context_sufficiency_score=context_sufficiency_score,
        payoff_readability_score=payoff_readability_score,
        authenticity_risk_score=authenticity_risk_score,
        hook_archetype=hook_archetype,
    )

    start_seconds = round(float(event.get("suggested_start_timestamp", event.get("start_timestamp", 0.0)) or 0.0), 4)
    end_seconds = round(max(start_seconds, float(event.get("suggested_end_timestamp", event.get("end_timestamp", start_seconds)) or start_seconds)), 4)
    candidate_id = str(lifecycle_row.get("candidate_id") or "").strip()

    return {
        "hook_id": _hook_id(candidate_id=candidate_id),
        "candidate_id": candidate_id,
        "event_id": str(event.get("event_id") or "").strip(),
        "lifecycle_state": lifecycle_state,
        "start_seconds": start_seconds,
        "end_seconds": end_seconds,
        "final_score": round(final_score, 4),
        "recommended_action": str(lifecycle_row.get("recommended_action") or event.get("recommended_action") or "").strip() or None,
        "gate_status": gate_status,
        "event_type": event_type or None,
        "hook_archetype": hook_archetype,
        "hook_strength": round(hook_strength, 4),
        "intensity_score": round(intensity_score, 4),
        "clarity_score": round(clarity_score, 4),
        "novelty_score": round(novelty_score, 4),
        "context_sufficiency_score": round(context_sufficiency_score, 4),
        "payoff_readability_score": round(payoff_readability_score, 4),
        "title_thumbnail_potential_score": round(title_thumbnail_potential_score, 4),
        "authenticity_risk_score": round(authenticity_risk_score, 4),
        "sound_off_legibility_score": round(sound_off_legibility_score, 4),
        "hook_mode": hook_mode,
        "packaging_strategy": packaging_strategy,
        "rejection_reason": rejection_reason,
        "contributing_signal_ids": contributing_signal_ids,
        "entity_id": str(metadata.get("entity_id") or "").strip() or None,
        "metadata_summary": _metadata_summary(metadata),
    }


def _hook_id(*, candidate_id: str) -> str:
    digest = hashlib.sha1(candidate_id.encode("utf-8")).hexdigest()[:16]
    return f"hook-{digest}"


def _hook_archetype(*, event_type: str, final_score: float, signal_count: int) -> str:
    text = event_type.lower()
    if "clutch" in text:
        return "clutch"
    if "reversal" in text or "swing" in text:
        return "reversal"
    if "fail" in text or "death" in text or "whiff" in text:
        return "fail"
    if "comedy" in text or "funny" in text:
        return "comedy"
    if "combo" in text:
        return "flex"
    if "medal" in text and final_score >= 0.85:
        return "domination"
    if signal_count >= 3 and final_score >= 0.8:
        return "chaos"
    return "other"


def _hook_mode_and_strategy(
    *,
    hook_strength: float,
    clarity_score: float,
    context_sufficiency_score: float,
    payoff_readability_score: float,
    authenticity_risk_score: float,
    hook_archetype: str,
) -> tuple[str, str | None, str | None]:
    if authenticity_risk_score >= 0.6:
        return "reject", None, "authenticity_risk_too_high"
    if hook_strength < 0.45 or payoff_readability_score < 0.45:
        return "reject", None, "weak_payoff_readability"
    if clarity_score >= 0.68 and context_sufficiency_score >= 0.55 and payoff_readability_score >= 0.65:
        return "natural", _natural_packaging_strategy(hook_archetype), None
    return "synthetic", "setup_then_payoff_with_context_card", None


def _natural_packaging_strategy(hook_archetype: str) -> str:
    if hook_archetype in {"clutch", "reversal", "chaos"}:
        return "cold_open_payoff_first"
    if hook_archetype in {"domination", "flex"}:
        return "tight_context_then_payoff"
    if hook_archetype == "comedy":
        return "reaction_led_open"
    if hook_archetype == "fail":
        return "instant_fail_reveal"
    return "tight_context_then_payoff"


def _metadata_summary(metadata: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in ("entity_id", "ability_id", "equipment_id", "matched_signal_types"):
        value = metadata.get(key)
        if value in (None, "", [], {}):
            continue
        summary[key] = value
    return summary


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _default_output_path(game: str, source: str) -> Path:
    stem = Path(source).stem or "hook-candidates"
    safe_stem = "".join(char if char.isalnum() else "-" for char in stem.lower()).strip("-") or "hook-candidates"
    return DEFAULT_OUTPUT_ROOT / game / f"{safe_stem}.hook_candidates.json"


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path

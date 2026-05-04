from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipeline.game_pack import list_games, load_game_pack
from pipeline.game_onboarding import _build_fusion_rules_manifest, _build_runtime_cv_rules_manifest, _load_runtime_detection_schema
from pipeline.roi_matcher import RoiMatcherError, validate_published_pack
from pipeline.simple_yaml import load_yaml_file


CANONICAL_CONTRACTS = {
    "detection_manifest": "game_detection_manifest_v1",
    "runtime_analysis": "runtime_analysis_v1",
    "fused_analysis": "fused_analysis_v1",
    "fusion_gold_manifest": "fusion_goldset_clip_v1",
    "fusion_rules": "fusion_rules_v1",
}


def audit_pipeline_contracts(
    *,
    game: str | None = None,
    repo_root: Path,
    config_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    games = [game] if game is not None else list_games()
    pack_contracts: list[dict[str, Any]] = []
    legacy_usage: list[dict[str, Any]] = []
    onboarding_publish_consistency: list[dict[str, Any]] = []
    runtime_contract_findings: list[dict[str, Any]] = []
    fusion_contract_findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for game_id in games:
        try:
            pack = load_game_pack(game_id)
        except FileNotFoundError as exc:
            warning = {"status": "missing_pack", "game": game_id, "error": str(exc)}
            warnings.append(warning)
            pack_contracts.append({"game": game_id, "ok": False, "status": "missing"})
            continue

        if pack.pack_format != "published":
            pack_contracts.append(
                {
                    "game": game_id,
                    "ok": True,
                    "status": "warning",
                    "pack_format": pack.pack_format,
                    "contract_status": "starter_only",
                    "canonical_contracts": _canonical_contract_summary(pack.files),
                    "active_legacy_modes": [],
                }
            )
            continue

        try:
            validation = validate_published_pack(game_id)
        except RoiMatcherError as exc:
            warning = {"status": exc.status, "game": game_id, "error": exc.message}
            warnings.append(warning)
            pack_contracts.append({"game": game_id, "ok": False, "status": "missing"})
            continue

        consistency = audit_published_manifest_consistency(
            repo_root / "assets" / "games" / game_id,
            repo_root=repo_root,
        )
        contract_status = _published_contract_status(validation=validation, consistency=consistency)
        contract_row = {
            "game": game_id,
            "ok": bool(validation.get("ok", False)) and not consistency["failures"],
            "status": "ok" if contract_status == "canonical" else "warning",
            "pack_format": "published",
            "contract_status": contract_status,
            "canonical_contracts": _canonical_contract_summary(pack.files),
            "active_legacy_modes": list(validation.get("legacy_modes", [])),
            "validation_status": validation.get("status"),
            "ontology_version": validation.get("ontology_version"),
            "ontology_status": validation.get("ontology_status"),
        }
        pack_contracts.append(contract_row)
        legacy_usage.extend({"game": game_id, **row} for row in validation.get("legacy_findings", []))
        onboarding_publish_consistency.append({"game": game_id, **consistency})
        runtime_contract_findings.extend({"game": game_id, **row} for row in validation.get("runtime_contract_findings", []))
        fusion_contract_findings.extend({"game": game_id, **row} for row in consistency.get("fusion_contract_findings", []))
        runtime_contract_findings.extend({"game": game_id, **row} for row in validation.get("ontology_findings", []))

    config = config_payload if isinstance(config_payload, dict) else {}
    legacy_proxy_signals = (
        config.get("proxy_scanner", {}).get("signals", {})
        if isinstance(config.get("proxy_scanner", {}), dict)
        else {}
    )
    if isinstance(legacy_proxy_signals, dict) and legacy_proxy_signals:
        legacy_usage.append(
            {
                "status": "legacy_proxy_signals_config",
                "surface": "config.proxy_scanner.signals",
                "message": "legacy proxy-scanner 'signals' config is still being normalized into proxy_scanner.sources",
            }
        )

    return {
        "ok": True,
        "status": "ok",
        "game_filter": game,
        "pack_contracts": pack_contracts,
        "legacy_usage": legacy_usage,
        "onboarding_publish_consistency": onboarding_publish_consistency,
        "runtime_contract_findings": runtime_contract_findings,
        "fusion_contract_findings": fusion_contract_findings,
        "recommended_cleanup_order": [
            "Remove target_id_source=asset_id_suffix after converting all published runtime_cv_rules to template_field semantics.",
            "Retire legacy proxy_scanner.signals config once all local config.yaml variants use proxy_scanner.sources only.",
            "Tighten publish-time consistency checks until all published packs report canonical contract status.",
        ],
        "warnings": warnings,
    }


def audit_published_manifest_consistency(published_root: str | Path, *, repo_root: Path) -> dict[str, Any]:
    root = Path(published_root).expanduser().resolve()
    detection_manifest = load_yaml_file(root / "manifests" / "detection_manifest.yaml")
    cv_templates = load_yaml_file(root / "manifests" / "cv_templates.yaml")
    runtime_rules = load_yaml_file(root / "manifests" / "runtime_cv_rules.yaml")
    fusion_rules = load_yaml_file(root / "manifests" / "fusion_rules.yaml")
    detection_schema = _load_runtime_detection_schema(repo_root=repo_root)

    detection_rows = detection_manifest.get("rows", []) if isinstance(detection_manifest, dict) else []
    template_rows = cv_templates.get("templates", []) if isinstance(cv_templates, dict) else []
    runtime_event_mappings = runtime_rules.get("event_mappings", {}) if isinstance(runtime_rules, dict) else {}
    fusion_rule_rows = fusion_rules.get("rules", []) if isinstance(fusion_rules, dict) else []
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    runtime_contract_findings: list[dict[str, Any]] = []
    fusion_contract_findings: list[dict[str, Any]] = []

    if not isinstance(detection_rows, list):
        failures.append({"status": "invalid_detection_manifest_rows"})
        detection_rows = []
    if not isinstance(template_rows, list):
        failures.append({"status": "invalid_template_rows"})
        template_rows = []
    if not isinstance(runtime_event_mappings, dict):
        failures.append({"status": "invalid_runtime_cv_rules"})
        runtime_event_mappings = {}
    if not isinstance(fusion_rule_rows, list):
        failures.append({"status": "invalid_fusion_rules"})
        fusion_rule_rows = []

    expected_runtime_rules = _build_runtime_cv_rules_manifest(detection_rows).get("event_mappings", {})
    expected_fusion_rules = _build_fusion_rules_manifest(detection_rows, detection_schema).get("rules", [])
    templates_by_asset_id = {str(row.get("asset_id", "")).strip(): row for row in template_rows if isinstance(row, dict)}

    for row in detection_rows:
        if not isinstance(row, dict):
            continue
        published_asset_id = str(row.get("published_asset_id", "")).strip()
        template_semantics = row.get("template_semantics", {}) if isinstance(row.get("template_semantics", {}), dict) else {}
        if published_asset_id:
            template_row = templates_by_asset_id.get(published_asset_id)
            if template_row is None:
                failures.append(
                    {
                        "status": "missing_template_for_detection_row",
                        "detection_id": row.get("detection_id"),
                        "published_asset_id": published_asset_id,
                    }
                )
            else:
                for field, value in template_semantics.items():
                    if template_row.get(field) != value:
                        failures.append(
                            {
                                "status": "template_semantic_mismatch",
                                "detection_id": row.get("detection_id"),
                                "published_asset_id": published_asset_id,
                                "field": field,
                                "expected": value,
                                "actual": template_row.get(field),
                            }
                        )

    runtime_rule_drift_detected = False
    for asset_family, expected_rule in expected_runtime_rules.items():
        actual_rule = runtime_event_mappings.get(asset_family)
        if _runtime_rule_matches_warn_first(expected_rule, actual_rule):
            if isinstance(actual_rule, dict) and str(actual_rule.get("target_id_source", "")).strip() == "asset_id_suffix":
                warnings.append(
                    {
                        "status": "legacy_target_id_source",
                        "asset_family": asset_family,
                    }
                )
            continue
        if actual_rule != expected_rule:
            runtime_rule_drift_detected = True
            runtime_contract_findings.append(
                {
                    "status": "runtime_rule_mismatch",
                    "asset_family": asset_family,
                    "expected": expected_rule,
                    "actual": actual_rule,
                }
            )
    extra_runtime_rule_families = sorted(asset_family for asset_family in runtime_event_mappings if asset_family not in expected_runtime_rules)
    if extra_runtime_rule_families:
        runtime_rule_drift_detected = True
    if runtime_rule_drift_detected:
        failures.append(
            {
                "status": "runtime_cv_rules_drift",
                "expected_asset_families": sorted(expected_runtime_rules.keys()),
                "actual_asset_families": sorted(runtime_event_mappings.keys()),
            }
        )

    expected_fusion_rule_ids = sorted(str(row.get("rule_id", "")).strip() for row in expected_fusion_rules if str(row.get("rule_id", "")).strip())
    actual_fusion_rule_ids = sorted(str(row.get("rule_id", "")).strip() for row in fusion_rule_rows if isinstance(row, dict) and str(row.get("rule_id", "")).strip())
    if expected_fusion_rule_ids != actual_fusion_rule_ids:
        failures.append(
            {
                "status": "fusion_rules_drift",
                "expected_rule_ids": expected_fusion_rule_ids,
                "actual_rule_ids": actual_fusion_rule_ids,
            }
        )
    expected_fusion_by_id = {str(row.get("rule_id", "")).strip(): row for row in expected_fusion_rules}
    actual_fusion_by_id = {str(row.get("rule_id", "")).strip(): row for row in fusion_rule_rows if isinstance(row, dict)}
    for rule_id, expected_rule in expected_fusion_by_id.items():
        actual_rule = actual_fusion_by_id.get(rule_id)
        if actual_rule != expected_rule:
            fusion_contract_findings.append(
                {
                    "status": "fusion_rule_mismatch",
                    "rule_id": rule_id,
                    "expected": expected_rule,
                    "actual": actual_rule,
                }
            )

    contract_status = "canonical" if not failures else "drift"
    return {
        "status": contract_status,
        "failures": failures,
        "warnings": warnings,
        "runtime_contract_findings": runtime_contract_findings,
        "fusion_contract_findings": fusion_contract_findings,
    }


def _canonical_contract_summary(files: dict[str, Any]) -> dict[str, Any]:
    detection_manifest = files.get("manifests/detection_manifest.yaml", {})
    fusion_rules = files.get("manifests/fusion_rules.yaml", {})
    return {
        "detection_manifest": detection_manifest.get("schema_version"),
        "cv_templates_present": "manifests/cv_templates.yaml" in files,
        "runtime_cv_rules_present": "manifests/runtime_cv_rules.yaml" in files,
        "fusion_rules": fusion_rules.get("schema_version", CANONICAL_CONTRACTS["fusion_rules"]),
        "runtime_analysis": CANONICAL_CONTRACTS["runtime_analysis"],
        "fused_analysis": CANONICAL_CONTRACTS["fused_analysis"],
        "fusion_gold_manifest": CANONICAL_CONTRACTS["fusion_gold_manifest"],
    }


def _published_contract_status(*, validation: dict[str, Any], consistency: dict[str, Any]) -> str:
    if validation.get("failures") or consistency.get("failures"):
        return "drifted"
    if validation.get("legacy_findings"):
        return "legacy_assisted"
    return "canonical"


def _runtime_rule_matches_warn_first(expected_rule: Any, actual_rule: Any) -> bool:
    if not isinstance(expected_rule, dict) or not isinstance(actual_rule, dict):
        return expected_rule == actual_rule
    expected_normalized = dict(expected_rule)
    actual_normalized = dict(actual_rule)
    if str(actual_normalized.get("target_id_source", "")).strip() == "asset_id_suffix":
        actual_normalized["target_id_source"] = expected_normalized.get("target_id_source")
        actual_normalized["target_value_field"] = expected_normalized.get("target_value_field")
    return actual_normalized == expected_normalized

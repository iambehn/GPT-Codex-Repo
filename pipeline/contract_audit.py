from __future__ import annotations

import ast
import json
import math
import re
from pathlib import Path
from typing import Any

from pipeline.game_pack import canonical_media_contract_summary, list_games, load_game_pack
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

PUBLISHED_PACK_DRAFT_ONLY_MANIFESTS = (
    "derived_detection_manifest.yaml",
    "game_detection_schema.yaml",
    "onboarding_state.json",
)

MANIFEST_CONTRACT_DOC_SCHEMA_TOKENS = (
    "game_detection_manifest_v1",
    "runtime_detection_schema_v1",
    "fusion_rules_v1",
)

REGISTRY_SCOPE_DOC_REQUIRED_ANCHORS = (
    "## Registry-Managed Schema Ownership",
    "## Explicit Local-Only Schema Scopes",
)

LOCAL_ONLY_SCHEMA_PREFIXES = (
    "accepted_clip_",
    "accepted_fixture_",
    "accepted_proxy_review_",
    "approval_target_dataset_",
    "derived_row_review_",
    "evaluation_fixture_manifest_",
    "fixture_source_manifest_",
    "fused_export_",
    "fusion_goldset_clip_",
    "onboarding_identity_review_session_",
    "proxy_replay_viewer_",
    "proxy_review_session_",
    "real_artifact_intake_bundle_",
    "real_artifact_intake_coverage_report_",
    "real_artifact_intake_dedup_",
    "real_artifact_intake_refresh_",
    "real_artifact_intake_summary_",
    "real_artifact_intake_validation_",
    "real_artifact_intake_history_comparison_",
    "real_artifact_intake_comparison_target_",
    "real_artifact_intake_dashboard_registry_summary_",
    "real_artifact_intake_dashboard_summary_",
    "replay_viewer_",
    "research_runtime_",
    "runtime_export_",
    "shadow_operator_run_",
    "training_export_",
    "unified_replay_viewer_",
    "v2_training_dataset_export_",
)

_SCHEMA_CONSTANT_NAME_RE = re.compile(r"^[A-Z0-9_]+SCHEMA_VERSION$")
_REVIEW_CLEAR_STATUSES = {"approved", "rejected"}

GOVERNANCE_REQUIRED_SURFACES = {
    "agents": {
        "relative_path": Path("AGENTS.md"),
        "anchors": (
            "## Decision Hierarchy",
            "## Heuristic Placement Rules",
            "## Anti-Bloat Rules",
            "## Validation Requirements",
            "## Escalation Rules",
        ),
    },
    "v2_index": {
        "relative_path": Path("docs/v2/INDEX.md"),
        "anchors": (
            "ENGINEERING_GOVERNANCE.md",
            "QUALITY_MAINTENANCE.md",
        ),
    },
    "engineering_governance": {
        "relative_path": Path("docs/v2/ENGINEERING_GOVERNANCE.md"),
        "anchors": (
            "## Decision Classes",
            "## Heuristic Placement Model",
            "## Acceptable Placement",
            "## Unacceptable Placement",
            "## Validation Spine",
        ),
    },
    "quality_maintenance": {
        "relative_path": Path("docs/v2/QUALITY_MAINTENANCE.md"),
        "anchors": (
            "## Maintenance Classes",
            "## Recurring Checks",
            "## Drift Signals",
            "## Triggered Actions",
            "## Long-Run Data-Quality Preservation",
        ),
    },
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
    manifest_authority_findings: list[dict[str, Any]] = []
    schema_documentation_findings = _audit_manifest_contract_docs(repo_root)
    registry_scope_doc_findings = _audit_registry_scope_doc(repo_root)
    registry_schema_findings = _audit_registry_schema_ownership(repo_root)
    quality_maintenance_findings = _audit_quality_maintenance(repo_root)
    governance_surfaces = _audit_governance_surfaces(repo_root)
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
        manifest_authority_findings.extend(
            {"game": game_id, **row}
            for row in _audit_published_manifest_authority(repo_root / "assets" / "games" / game_id)
        )

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
    warnings.extend(_manifest_authority_warnings(manifest_authority_findings))
    warnings.extend(_schema_documentation_warnings(schema_documentation_findings))
    warnings.extend(_registry_scope_doc_warnings(registry_scope_doc_findings))
    warnings.extend(_registry_schema_warnings(registry_schema_findings))
    warnings.extend(_quality_maintenance_warnings(quality_maintenance_findings))
    warnings.extend(_governance_warnings(governance_surfaces))

    return {
        "ok": True,
        "status": "ok",
        "game_filter": game,
        "pack_contracts": pack_contracts,
        "legacy_usage": legacy_usage,
        "onboarding_publish_consistency": onboarding_publish_consistency,
        "runtime_contract_findings": runtime_contract_findings,
        "fusion_contract_findings": fusion_contract_findings,
        "manifest_authority_findings": manifest_authority_findings,
        "schema_documentation_findings": schema_documentation_findings,
        "registry_scope_doc_findings": registry_scope_doc_findings,
        "registry_schema_findings": registry_schema_findings,
        "quality_maintenance_findings": quality_maintenance_findings,
        "governance_surfaces": governance_surfaces,
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
    summary = {
        "detection_manifest": detection_manifest.get("schema_version"),
        "cv_templates_present": "manifests/cv_templates.yaml" in files,
        "runtime_cv_rules_present": "manifests/runtime_cv_rules.yaml" in files,
        "fusion_rules": fusion_rules.get("schema_version", CANONICAL_CONTRACTS["fusion_rules"]),
        "runtime_analysis": CANONICAL_CONTRACTS["runtime_analysis"],
        "fused_analysis": CANONICAL_CONTRACTS["fused_analysis"],
        "fusion_gold_manifest": CANONICAL_CONTRACTS["fusion_gold_manifest"],
    }
    summary["canonical_media_contract"] = canonical_media_contract_summary(files, pack_format="published")
    return summary


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


def _audit_governance_surfaces(repo_root: Path) -> list[dict[str, Any]]:
    root = repo_root.expanduser().resolve()
    if not (root / "AGENTS.md").exists() and not (root / "docs" / "v2").exists():
        return []

    findings: list[dict[str, Any]] = []
    for surface_name, spec in GOVERNANCE_REQUIRED_SURFACES.items():
        target = root / spec["relative_path"]
        if not target.exists():
            findings.append(
                {
                    "surface": surface_name,
                    "path": str(spec["relative_path"]),
                    "status": "missing",
                }
            )
            continue
        content = target.read_text(encoding="utf-8")
        missing_anchors = [anchor for anchor in spec["anchors"] if anchor not in content]
        findings.append(
            {
                "surface": surface_name,
                "path": str(spec["relative_path"]),
                "status": "ok" if not missing_anchors else "incomplete",
                "missing_anchors": missing_anchors,
            }
        )
    return findings


def _governance_warnings(governance_surfaces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for row in governance_surfaces:
        status = row.get("status")
        if status == "missing":
            warnings.append(
                {
                    "status": "missing_governance_surface",
                    "surface": row.get("surface"),
                    "path": row.get("path"),
                }
            )
        elif status == "incomplete":
            warnings.append(
                {
                    "status": "incomplete_governance_surface",
                    "surface": row.get("surface"),
                    "path": row.get("path"),
                    "missing_anchors": row.get("missing_anchors", []),
                }
            )
    return warnings


def _audit_published_manifest_authority(game_root: Path) -> list[dict[str, Any]]:
    manifests_root = game_root / "manifests"
    if not manifests_root.exists():
        return []
    findings: list[dict[str, Any]] = []
    for filename in PUBLISHED_PACK_DRAFT_ONLY_MANIFESTS:
        target = manifests_root / filename
        if target.exists():
            findings.append(
                {
                    "status": "draft_only_manifest_in_published_pack",
                    "path": str(target),
                    "filename": filename,
                }
            )
    return findings


def _manifest_authority_warnings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for row in findings:
        warnings.append(
            {
                "status": row["status"],
                "game": row.get("game"),
                "path": row.get("path"),
                "filename": row.get("filename"),
            }
        )
    return warnings


def _audit_manifest_contract_docs(repo_root: Path) -> list[dict[str, Any]]:
    doc_path = repo_root.expanduser().resolve() / "docs" / "v2" / "MANIFEST_CONTRACTS.md"
    if not doc_path.exists():
        return []
    content = doc_path.read_text(encoding="utf-8")
    missing_tokens = [token for token in MANIFEST_CONTRACT_DOC_SCHEMA_TOKENS if token not in content]
    if not missing_tokens:
        return [
            {
                "surface": "manifest_contracts_doc",
                "path": str(doc_path.relative_to(repo_root.expanduser().resolve())),
                "status": "ok",
                "documented_schema_versions": list(MANIFEST_CONTRACT_DOC_SCHEMA_TOKENS),
            }
        ]
    return [
        {
            "surface": "manifest_contracts_doc",
            "path": str(doc_path.relative_to(repo_root.expanduser().resolve())),
            "status": "missing_schema_version_docs",
            "missing_schema_versions": missing_tokens,
        }
    ]


def _schema_documentation_warnings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for row in findings:
        if row.get("status") == "missing_schema_version_docs":
            warnings.append(
                {
                    "status": "undocumented_canonical_schema_version",
                    "surface": row.get("surface"),
                    "path": row.get("path"),
                    "missing_schema_versions": row.get("missing_schema_versions", []),
                }
            )
    return warnings


def _audit_registry_scope_doc(repo_root: Path) -> list[dict[str, Any]]:
    doc_path = repo_root.expanduser().resolve() / "docs" / "v2" / "REGISTRY_ORCHESTRATION_STATE.md"
    if not doc_path.exists():
        return []
    content = doc_path.read_text(encoding="utf-8")
    missing_anchors = [anchor for anchor in REGISTRY_SCOPE_DOC_REQUIRED_ANCHORS if anchor not in content]
    return [
        {
            "surface": "registry_scope_doc",
            "path": str(doc_path.relative_to(repo_root.expanduser().resolve())),
            "status": "ok" if not missing_anchors else "incomplete",
            "missing_anchors": missing_anchors,
        }
    ]


def _registry_scope_doc_warnings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for row in findings:
        if row.get("status") == "incomplete":
            warnings.append(
                {
                    "status": "incomplete_registry_scope_doc",
                    "surface": row.get("surface"),
                    "path": row.get("path"),
                    "missing_anchors": row.get("missing_anchors", []),
                }
            )
    return warnings


def _audit_registry_schema_ownership(repo_root: Path) -> list[dict[str, Any]]:
    root = repo_root.expanduser().resolve()
    pipeline_root = root / "pipeline"
    clip_registry_path = pipeline_root / "clip_registry.py"
    if not pipeline_root.exists() or not clip_registry_path.exists():
        return []

    schema_rows = _collect_pipeline_schema_constants(pipeline_root)
    if not schema_rows:
        return []
    registry_versions = _collect_registry_managed_schema_versions(clip_registry_path, schema_rows)
    by_schema_version: dict[str, list[dict[str, str]]] = {}
    for row in schema_rows:
        by_schema_version.setdefault(row["schema_version"], []).append(row)

    findings: list[dict[str, Any]] = []
    for schema_version, owners in sorted(by_schema_version.items()):
        if schema_version in registry_versions:
            continue
        if _is_explicit_local_only_schema(schema_version):
            continue
        findings.append(
            {
                "status": "unscoped_schema_version",
                "schema_version": schema_version,
                "owners": [
                    {
                        "module": owner["module"],
                        "constant_name": owner["constant_name"],
                    }
                    for owner in owners
                ],
            }
        )
    return findings


def _registry_schema_warnings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for row in findings:
        if row.get("status") == "unscoped_schema_version":
            warnings.append(
                {
                    "status": "unscoped_registry_schema_version",
                    "schema_version": row.get("schema_version"),
                    "owners": row.get("owners", []),
                }
            )
    return warnings


def _collect_pipeline_schema_constants(pipeline_root: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(pipeline_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            if not isinstance(node.value, ast.Constant) or not isinstance(node.value.value, str):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name) and _SCHEMA_CONSTANT_NAME_RE.match(target.id):
                    rows.append(
                        {
                            "module": str(path.relative_to(pipeline_root.parent)),
                            "constant_name": target.id,
                            "schema_version": node.value.value,
                        }
                    )
    return rows


def _collect_registry_managed_schema_versions(
    clip_registry_path: Path,
    schema_rows: list[dict[str, str]],
) -> set[str]:
    tree = ast.parse(clip_registry_path.read_text(encoding="utf-8"))
    registry_constant_names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and _SCHEMA_CONSTANT_NAME_RE.match(target.id):
                    registry_constant_names.add(target.id)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported_name = alias.asname or alias.name
                if _SCHEMA_CONSTANT_NAME_RE.match(imported_name):
                    registry_constant_names.add(imported_name)

    values_by_name: dict[str, set[str]] = {}
    for row in schema_rows:
        values_by_name.setdefault(row["constant_name"], set()).add(row["schema_version"])

    managed: set[str] = set()
    for name in registry_constant_names:
        managed.update(values_by_name.get(name, set()))
    return managed


def _is_explicit_local_only_schema(schema_version: str) -> bool:
    return any(schema_version.startswith(prefix) for prefix in LOCAL_ONLY_SCHEMA_PREFIXES)


def _audit_quality_maintenance(repo_root: Path) -> list[dict[str, Any]]:
    root = repo_root.expanduser().resolve()
    findings: list[dict[str, Any]] = []
    findings.extend(_audit_fixture_freshness(root))
    findings.extend(_audit_review_backlog(root))
    findings.extend(_audit_review_file_drift(root))
    return findings


def _audit_fixture_freshness(repo_root: Path) -> list[dict[str, Any]]:
    fixtures_root = repo_root / "tests" / "fixtures" / "fusion_goldsets"
    if not fixtures_root.exists():
        return []
    findings: list[dict[str, Any]] = []
    validation_surface = repo_root / "pipeline" / "fusion_validation.py"
    for game_dir in sorted(path for path in fixtures_root.iterdir() if path.is_dir()):
        fixture_files = sorted(path for path in game_dir.glob("*.json") if path.is_file())
        if not fixture_files:
            continue
        dependency_paths = [validation_surface] if validation_surface.exists() else []
        game_manifest_root = repo_root / "assets" / "games" / game_dir.name / "manifests"
        for relative_name in ("detection_manifest.yaml", "fusion_rules.yaml"):
            candidate = game_manifest_root / relative_name
            if candidate.exists():
                dependency_paths.append(candidate)
        if not dependency_paths:
            continue
        newest_dependency = max(path.stat().st_mtime for path in dependency_paths)
        newest_fixture = max(path.stat().st_mtime for path in fixture_files)
        if newest_fixture < newest_dependency:
            stale_by_days = math.ceil((newest_dependency - newest_fixture) / 86400)
            findings.append(
                {
                    "surface": "fixture_freshness",
                    "game": game_dir.name,
                    "status": "fixture_refresh_recommended",
                    "severity": "warning",
                    "fixture_count": len(fixture_files),
                    "stale_by_days": stale_by_days,
                    "latest_fixture_mtime": newest_fixture,
                    "latest_dependency_mtime": newest_dependency,
                }
            )
        else:
            findings.append(
                {
                    "surface": "fixture_freshness",
                    "game": game_dir.name,
                    "status": "fresh",
                    "severity": "informational",
                    "fixture_count": len(fixture_files),
                    "latest_fixture_mtime": newest_fixture,
                    "latest_dependency_mtime": newest_dependency,
                }
            )
    return findings


def _audit_review_backlog(repo_root: Path) -> list[dict[str, Any]]:
    assets_root = repo_root / "assets" / "games"
    if not assets_root.exists():
        return []
    findings: list[dict[str, Any]] = []
    for review_dir in sorted(assets_root.glob("*/drafts/onboarding/*/review/derived_row_reviews")):
        review_files = sorted(path for path in review_dir.glob("*.json") if path.is_file())
        if not review_files:
            continue
        try:
            relative_review_dir = review_dir.relative_to(assets_root)
            game = relative_review_dir.parts[0]
        except ValueError:
            game = None
        pending_count = 0
        status_counts: dict[str, int] = {}
        for path in review_files:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pending_count += 1
                status_counts["invalid_json"] = status_counts.get("invalid_json", 0) + 1
                continue
            review_status = str(payload.get("review_status") or "unreviewed").strip() or "unreviewed"
            status_counts[review_status] = status_counts.get(review_status, 0) + 1
            if review_status not in _REVIEW_CLEAR_STATUSES:
                pending_count += 1
        total_count = len(review_files)
        pending_ratio = pending_count / total_count if total_count else 0.0
        if pending_count == 0:
            severity = "informational"
            status = "review_backlog_clear"
        elif pending_count >= 20 or pending_ratio >= 0.2:
            severity = "blocking"
            status = "review_backlog_high"
        else:
            severity = "warning"
            status = "review_backlog_present"
        findings.append(
            {
                "surface": "review_backlog",
                "game": game,
                "draft_root": str(review_dir.parent.parent.parent),
                "review_dir": str(review_dir),
                "status": status,
                "severity": severity,
                "total_review_files": total_count,
                "pending_review_count": pending_count,
                "pending_review_ratio": round(pending_ratio, 4),
                "status_counts": status_counts,
            }
        )
    return findings


def _quality_maintenance_warnings(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for row in findings:
        severity = row.get("severity")
        if severity not in {"blocking", "warning"}:
            continue
        warning = dict(row)
        warning["status"] = f"quality_maintenance_{row.get('status')}"
        warnings.append(warning)
    return warnings


def _audit_review_file_drift(repo_root: Path) -> list[dict[str, Any]]:
    assets_root = repo_root / "assets" / "games"
    if not assets_root.exists():
        return []
    findings: list[dict[str, Any]] = []
    for draft_root in sorted(assets_root.glob("*/drafts/onboarding/*")):
        review_dir = draft_root / "review" / "derived_row_reviews"
        if not review_dir.exists():
            continue
        detection_manifest_path = draft_root / "manifests" / "derived_detection_manifest.yaml"
        if not detection_manifest_path.exists():
            continue
        detection_manifest = load_yaml_file(detection_manifest_path)
        rows = detection_manifest.get("rows", []) if isinstance(detection_manifest, dict) else []
        if not isinstance(rows, list):
            continue
        detection_ids = {
            str(row.get("detection_id") or "").strip()
            for row in rows
            if isinstance(row, dict) and str(row.get("detection_id") or "").strip()
        }
        stale_rows: list[dict[str, Any]] = []
        for review_file in sorted(review_dir.glob("*.json")):
            try:
                payload = json.loads(review_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            detection_id = str(payload.get("detection_id") or "").strip()
            if detection_id and detection_id not in detection_ids:
                stale_rows.append(
                    {
                        "detection_id": detection_id,
                        "review_file_path": str(review_file),
                        "review_status": str(payload.get("review_status") or "").strip() or None,
                        "review_decision": str(payload.get("review_decision") or "").strip() or None,
                        "candidate_option_count": int(payload.get("candidate_option_count") or 0),
                    }
                )
        if stale_rows:
            try:
                relative_draft_root = draft_root.relative_to(assets_root)
                game = relative_draft_root.parts[0]
            except ValueError:
                game = None
            findings.append(
                {
                    "surface": "review_file_drift",
                    "game": game,
                    "draft_root": str(draft_root),
                    "review_dir": str(review_dir),
                    "status": "stale_review_files_present",
                    "severity": "warning",
                    "stale_review_count": len(stale_rows),
                    "stale_rows": stale_rows,
                }
            )
    return findings

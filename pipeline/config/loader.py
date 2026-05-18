from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import os
import re
from pathlib import Path
from typing import Any, Mapping

from pipeline.simple_yaml import load_yaml_file

from .defaults import DEFAULT_CONFIG, default_config
from .schema import PipelineConfigModel, ProxyScannerConfigModel, RuntimeAnalysisConfigModel


_ENV_TOKEN_RE = re.compile(r"^\$\{(?:(?:ENV:)?)([A-Za-z_][A-Za-z0-9_]*)\}$")


class ConfigValidationError(ValueError):
    def __init__(self, message: str, *, errors: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or []


@dataclass(frozen=True)
class LoadedPipelineConfig:
    raw_config: dict[str, Any]
    merged_config: dict[str, Any]
    config: dict[str, Any]
    model: PipelineConfigModel
    warnings: list[dict[str, Any]]
    config_path: Path | None


def load_repo_config_file(repo_root: Path, config_path: Path | None = None) -> dict[str, Any]:
    target = _resolve_config_path(repo_root, config_path)
    if target is None or not target.exists():
        return {}
    loaded = load_yaml_file(target)
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ConfigValidationError(
            "config root must be a mapping",
            errors=[{"path": "", "message": "config root must be a mapping", "value_type": type(loaded).__name__}],
        )
    return loaded


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key in set(base) | set(override):
        base_value = base.get(key)
        override_value = override.get(key)
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = deep_merge(base_value, override_value)
        elif key in override:
            merged[key] = deepcopy(override_value)
        else:
            merged[key] = deepcopy(base_value)
    return merged


def normalize_proxy_scanner_config(
    proxy_config: dict[str, Any],
    *,
    default_proxy_scanner: dict[str, Any] | None = None,
) -> dict[str, Any]:
    defaults = default_proxy_scanner or DEFAULT_CONFIG["proxy_scanner"]
    normalized = deepcopy(proxy_config)
    legacy_sources = normalized.pop("signals", {}) if isinstance(normalized.get("signals"), dict) else {}
    explicit_sources = normalized.get("sources", {}) if isinstance(normalized.get("sources"), dict) else {}
    merged_sources = deep_merge(defaults["sources"], legacy_sources)
    merged_sources = deep_merge(merged_sources, explicit_sources)
    normalized["sources"] = merged_sources
    return normalized


def normalize_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = deepcopy(config)
    proxy_config = normalized.get("proxy_scanner", {})
    normalized["proxy_scanner"] = normalize_proxy_scanner_config(proxy_config if isinstance(proxy_config, dict) else {})
    return normalized


def normalize_config_with_warnings(config: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
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
    return normalize_config(config), warnings


def load_pipeline_config(
    repo_root: Path,
    *,
    config_path: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> LoadedPipelineConfig:
    resolved_path = _resolve_config_path(repo_root, config_path)
    raw_config = load_repo_config_file(repo_root, resolved_path)
    resolved_raw_config = _resolve_env_placeholders(raw_config, env or os.environ, path=[])
    normalized_raw_config, warnings = normalize_config_with_warnings(resolved_raw_config)
    merged_config = deep_merge(default_config(), normalized_raw_config)
    _validate_mapping_against_defaults(merged_config, DEFAULT_CONFIG, path=[])
    normalized_config = normalize_config(merged_config)
    model = _build_model(normalized_config)
    return LoadedPipelineConfig(
        raw_config=resolved_raw_config,
        merged_config=merged_config,
        config=normalized_config,
        model=model,
        warnings=warnings,
        config_path=resolved_path,
    )


def _resolve_config_path(repo_root: Path, config_path: Path | None) -> Path | None:
    target = config_path or (repo_root / "config.yaml")
    target = Path(target).expanduser()
    if not target.is_absolute():
        target = (repo_root / target).resolve()
    else:
        target = target.resolve()
    return target


def _resolve_env_placeholders(node: Any, env: Mapping[str, str], *, path: list[str]) -> Any:
    if isinstance(node, dict):
        return {key: _resolve_env_placeholders(value, env, path=path + [str(key)]) for key, value in node.items()}
    if isinstance(node, list):
        return [_resolve_env_placeholders(value, env, path=path + [str(index)]) for index, value in enumerate(node)]
    if isinstance(node, str):
        match = _ENV_TOKEN_RE.match(node)
        if match is None:
            return node
        env_name = match.group(1)
        if env_name not in env:
            joined = ".".join(path) or "<root>"
            raise ConfigValidationError(
                f"missing required environment variable for config placeholder at {joined}: {env_name}",
                errors=[{"path": joined, "message": "missing environment variable", "env_var": env_name}],
            )
        return env[env_name]
    return node


def _validate_mapping_against_defaults(node: Any, default_node: Any, *, path: list[str]) -> None:
    if isinstance(default_node, dict):
        if not isinstance(node, dict):
            _raise_type_error(path, expected="mapping", actual=node)
        for key, value in node.items():
            if key not in default_node:
                continue
            _validate_mapping_against_defaults(value, default_node[key], path=path + [str(key)])
        return
    if isinstance(default_node, list):
        if not isinstance(node, list):
            _raise_type_error(path, expected="list", actual=node)
        if not default_node:
            return
        template = default_node[0]
        for index, item in enumerate(node):
            _validate_mapping_against_defaults(item, template, path=path + [str(index)])
        return
    if isinstance(default_node, bool):
        if not isinstance(node, bool):
            _raise_type_error(path, expected="bool", actual=node)
        return
    if isinstance(default_node, int):
        if isinstance(node, bool) or not isinstance(node, int):
            _raise_type_error(path, expected="int", actual=node)
        return
    if isinstance(default_node, float):
        if isinstance(node, bool) or not isinstance(node, (int, float)):
            _raise_type_error(path, expected="float", actual=node)
        return
    if isinstance(default_node, str):
        if not isinstance(node, str):
            _raise_type_error(path, expected="str", actual=node)
        return


def _raise_type_error(path: list[str], *, expected: str, actual: Any) -> None:
    joined = ".".join(path) or "<root>"
    raise ConfigValidationError(
        f"invalid config type at {joined}: expected {expected}, got {type(actual).__name__}",
        errors=[{"path": joined, "expected": expected, "actual": type(actual).__name__}],
    )


def _build_model(config: dict[str, Any]) -> PipelineConfigModel:
    proxy_raw = config.get("proxy_scanner", {}) if isinstance(config.get("proxy_scanner", {}), dict) else {}
    runtime_raw = config.get("runtime_analysis", {}) if isinstance(config.get("runtime_analysis", {}), dict) else {}
    proxy_model = ProxyScannerConfigModel(
        sources=deepcopy(proxy_raw.get("sources", {})),
        weights=deepcopy(proxy_raw.get("weights", {})),
        candidate_selection=deepcopy(proxy_raw.get("candidate_selection", {})),
        cost_gates=deepcopy(proxy_raw.get("cost_gates", {})),
        sidecar=deepcopy(proxy_raw.get("sidecar", {})),
        extras={key: deepcopy(value) for key, value in proxy_raw.items() if key not in {"sources", "weights", "candidate_selection", "cost_gates", "sidecar"}},
    )
    runtime_model = RuntimeAnalysisConfigModel(
        scoring=deepcopy(runtime_raw.get("scoring", {})),
        extras={key: deepcopy(value) for key, value in runtime_raw.items() if key != "scoring"},
    )
    return PipelineConfigModel(
        proxy_scanner=proxy_model,
        runtime_analysis=runtime_model,
        extras={key: deepcopy(value) for key, value in config.items() if key not in {"proxy_scanner", "runtime_analysis"}},
    )

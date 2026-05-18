from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProxyScannerConfigModel:
    sources: dict[str, Any]
    weights: dict[str, Any]
    candidate_selection: dict[str, Any]
    cost_gates: dict[str, Any]
    sidecar: dict[str, Any]
    extras: dict[str, Any]


@dataclass(frozen=True)
class RuntimeAnalysisConfigModel:
    scoring: dict[str, Any]
    extras: dict[str, Any]


@dataclass(frozen=True)
class PipelineConfigModel:
    proxy_scanner: ProxyScannerConfigModel
    runtime_analysis: RuntimeAnalysisConfigModel
    extras: dict[str, Any]

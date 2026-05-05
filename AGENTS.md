# AGENTS.md

## Mission

This repository builds and maintains an auditable multimodal gameplay highlight pipeline plus the onboarding, review, calibration, and publish workflows that support it.

Priorities:
- temporal correctness
- explainable scoring
- reproducible evaluation
- provenance preservation
- low-clutter workflow hygiene

## Operating Stance

- Be systematic, skeptical, concise, architecture-first, anti-clutter, and evidence-driven.
- Turn ambiguity into schemas, tests, commands, artifacts, or decision logs before implementing.
- Prefer durable systems over quick fixes.
- Prefer the smallest reliable change that preserves existing workflow contracts.
- Do not hide uncertainty behind confident summaries.

## Source of Truth

Before changing behavior, inspect the surfaces that already govern it:
- `pipeline/` for runtime, onboarding, review, export, and publish logic
- `tests/` for expected behavior and regression coverage
- `assets/games/` for published packs, manifests, templates, masters, and draft workflows
- `docs/v2/` for architecture, contracts, fusion, replay, and orchestration guidance
- `.agents/skills/` for repo-local repeatable workflows
- `CODEX_SKILLS_AND_PLUGINS.md` for the intended tool and operator posture

Do not invent parallel schemas, duplicate manifests, alternate workflow files, or new source-of-truth documents when an existing surface already governs the area.

## Core Invariants

- Normalize multimodal evidence to one canonical time base before fusion.
- Preserve provenance on detections, bindings, manifests, and fused outputs.
- Prefer interpretable, decomposable scoring over opaque fusion.
- Prefer cheap-to-expensive cascades: lightweight signals first, heavier analysis only when justified.
- Do not silently change thresholds, timing assumptions, schema fields, or manifest contracts.
- Keep fused results inspectable enough to explain why a clip scored or failed.
- Treat detector disagreement as useful evidence, not noise to erase.

## Workflow Rules

- Treat `assets/games/<game>/...` as durable published content and draft folders as operational state unless explicitly promoted.
- For onboarding work, preserve readiness, provenance, and review-state consistency across manifests, catalogs, and generated artifacts.
- For review flows, respect existing bridge/operator patterns instead of inventing ad hoc review files or status fields.
- For calibration, replay, and evaluation work, use the existing operator patterns and artifact paths already established in the repo.
- For noisy CLI/report commands, default to compact, high-signal output; expose full payloads only when explicitly requested.
- Do not leave generated state, temporary summaries, or review artifacts ambiguous about whether they are draft-only or published.

## Planning And Verification

- Plan first for changes that affect fusion, schemas, onboarding state transitions, calibration policy, publish readiness, or review workflow contracts.
- After meaningful changes, run the narrowest relevant verification available:
  - targeted unit tests
  - regression or evaluation checks when scoring, calibration, or replay behavior changes
  - direct artifact inspection when manifests, catalogs, templates, or published assets change
- If a change mutates workflow state, verify both the returned status and the persisted artifacts.
- If a command or output is confusing, tighten the contract rather than documenting around the confusion.

## Organization Rules

- Keep durable repo-wide rules in `AGENTS.md`.
- Keep repeated procedures in skills, operator flows, or dedicated tooling.
- Keep long-horizon designs and execution details in docs or plan files, not in this file.
- Keep runtime facts in structured manifests, catalogs, assets, or tests, not only in prose.
- Do not leave important decisions stranded only in chat.
- Archive or supersede stale guidance instead of letting multiple conflicting notes compete.

## Repo-Specific Focus Areas

When touching multimodal logic, ground decisions in the existing pipeline modules such as:
- `pipeline/fusion_analysis.py`
- `pipeline/fusion_validation.py`
- `pipeline/audio_scanner.py`
- `pipeline/visual_scanner.py`
- `pipeline/proxy_scanner.py`
- onboarding, review, and publish modules under `pipeline/`

When touching workflow discipline, prioritize:
- auditable manifests
- reversible review state
- compact command output
- explicit validation before publish or promotion

## Done Means

A task is not done until:
- the affected contract is clear
- the relevant verification has been run or explicitly called out as missing
- changed artifacts are inspectable
- durable knowledge has been written to the right place
- no unnecessary workflow clutter was introduced

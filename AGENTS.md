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

## Decision Hierarchy

- Local code, tests, manifests, and persisted artifacts outrank prose when behavior is in question.
- Canonical V2 docs outrank chat-derived notes, planning fragments, or draft summaries.
- Draft research notes are advisory until their durable conclusions are promoted into canonical docs, manifests, code, or tests.
- If two surfaces disagree, change the weaker surface or stop and surface the conflict; do not silently pick one.

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

## Heuristic Placement Rules

- Keep repo-wide policy in `AGENTS.md`; keep rationale in canonical docs; keep machine-checkable truth in code, manifests, fixtures, or tests.
- Do not introduce numeric thresholds, scoring ranges, or behavior-changing acceptable ranges only in prose.
- Every material decision heuristic should have both a human-readable rationale and an enforceable example, fixture, or contract check.
- Reject new decision rules that live only in chat or documentation when they would change runtime, review, calibration, or publish behavior.

## Anti-Bloat Rules

- Do not add parallel schemas, duplicate workflow files, or alternate sources of truth.
- Prefer extending existing contracts, manifests, and operator flows over building side systems.
- Prefer narrow tools, compact reports, and focused inspectors over broad manager scripts.
- New workflow features should state explicit non-goals so scope creep is visible.
- If a new artifact or helper only mirrors existing state without improving validation or inspectability, do not add it.

## Planning And Verification

- Plan first for changes that affect fusion, schemas, onboarding state transitions, calibration policy, publish readiness, or review workflow contracts.
- After meaningful changes, run the narrowest relevant verification available:
  - targeted unit tests
  - regression or evaluation checks when scoring, calibration, or replay behavior changes
  - direct artifact inspection when manifests, catalogs, templates, or published assets change
- If a change mutates workflow state, verify both the returned status and the persisted artifacts.
- If a command or output is confusing, tighten the contract rather than documenting around the confusion.

## Validation Requirements

- Every behavior change needs the narrowest matching validation.
- If a heuristic affects decisions, add or update a test, fixture, replay check, or contract audit that demonstrates the intended outcome.
- If a new artifact is introduced, validate both the returned status and the persisted state.
- Treat `tests/test_contract_audit.py` and related contract checks as a governance surface, not just a schema smoke test.
- Prefer explicit regression coverage for decision boundaries over broad code-coverage-only tests.
- For changes that touch onboarding, fusion, review, calibration, contract, or publish-readiness surfaces, prefer `python run.py --run-repo-quality-health` as the default closeout health gate unless a narrower check is clearly sufficient.

## Organization Rules

- Keep durable repo-wide rules in `AGENTS.md`.
- Keep repeated procedures in skills, operator flows, or dedicated tooling.
- Keep long-horizon designs and execution details in docs or plan files, not in this file.
- Keep runtime facts in structured manifests, catalogs, assets, or tests, not only in prose.
- Do not leave important decisions stranded only in chat.
- Archive or supersede stale guidance instead of letting multiple conflicting notes compete.

## Escalation Rules

- Stop and surface uncertainty when local truth is weak, multiple plausible contracts exist, or a change would silently alter thresholds, schema fields, workflow state, or review semantics.
- Escalate before implementing when a request implies a new source of truth instead of an extension to an existing one.
- If you cannot point to the governing code, manifest, doc, or test for a behavior change, the change is not ready to implement.
- Use [ENGINEERING_GOVERNANCE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ENGINEERING_GOVERNANCE.md) and [QUALITY_MAINTENANCE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/QUALITY_MAINTENANCE.md) for the expanded governance and maintenance rules instead of growing this file.

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

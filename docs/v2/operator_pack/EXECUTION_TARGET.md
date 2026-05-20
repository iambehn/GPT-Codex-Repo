# Execution Target

Status: active-draft
Version: 0.1
Last updated: 2026-05-20

## Objective

Pin one concrete, runnable happy path for the gameplay highlight pipeline and prevent broad architecture drift while that path is being proven.

## Current Execution Doctrine

- Build one thin vertical slice before broadening scope.
- Prefer one supported game over cross-game abstractions.
- Prefer one sample input over generalized input support.
- Use existing repo surfaces before inventing new schemas or artifacts.
- Treat fixture-only success as bootstrap, not final proof, unless explicitly approved.
- Stop on explicit blockers instead of patching around them.

## Current Phase Gate

Current phase: `Phase 0 - repo inventory and operator-pack activation`

Phase 1 implementation work should not start until the P0 blockers in [OPEN_QUESTIONS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/OPEN_QUESTIONS.md) are either resolved or explicitly deferred with approval.

## First Supported Game

Status: unresolved

Current blocker:

- no canonical Phase 0 inventory has yet pinned the first supported game

Resolution rule:

- choose one game only after inventorying existing game packs, manifests, assets, and workflow support

## Input Mode

Status: unresolved

Allowed values:

- `real_sample`
- `fixture_bootstrap`

Rules:

- `fixture_bootstrap` may be used to unblock Phase 1 setup
- `fixture_bootstrap` does not complete the happy path by itself
- real input success is required for happy-path completion unless explicitly waived

## Sample Input

Status: unresolved

Current blocker:

- no approved sample clip or canonical fixture bootstrap path is pinned yet

## Happy-Path Stages

1. Config load
2. Input resolution
3. Sidecar and artifact generation
4. Candidate and review surface
5. Calibration and replay path
6. Local export-readiness bundle
7. Repo-quality health gate

## Required Artifacts

The exact command and artifact owners are part of Phase 0 inventory. Until then, the minimum expected stage outputs are:

| Stage | Minimum artifact expectation | Validation target |
| --- | --- | --- |
| Config load | resolved config object or explicit validation error | config parses and command bootstraps |
| Input resolution | approved input path or explicit blocker | input exists and chosen mode is explicit |
| Sidecar/artifact generation | at least one stage-appropriate analysis or sidecar artifact | downstream review or replay surface can consume it |
| Candidate/review surface | inspectable candidate or review payload | reviewer can inspect status and evidence |
| Calibration/replay | replay or calibration artifact with inspectable result | result is parseable and meaningful |
| Local export-readiness | local bundle plus publish blockers | bundle is inspectable and `platform_action_taken` remains false |
| Repo-quality health | green `run.py` health result | maintenance and decision regression both pass |

## Stop Condition

The happy path is complete when all of the following are true:

- one supported game is pinned
- one approved input mode is pinned
- one sample input or approved fixture bootstrap path is pinned
- one config/load path runs
- required artifacts are produced and parseable
- one candidate/review path is executable
- one calibration/replay path is executable
- one local export-readiness result is inspectable
- `python run.py --run-repo-quality-health` returns green

## Non-Goals

- new games
- new agent frameworks
- paid APIs
- platform posting or scheduling
- cloud or distributed infrastructure
- broad UI redesigns
- major schema rewrites
- learned ranking or large-scale model-selection work

## Escalation Triggers

Stop and escalate if any of these are true:

- first supported game cannot be determined
- sample input cannot be determined
- required assets are missing for the chosen game
- schema or status ownership is ambiguous
- the health gate does not exist or cannot be explained
- a stage only works on fixtures and fails on real input
- artifacts exist but are semantically unusable downstream
- local export-readiness is confused with external publish approval

## Known Repo Fact

The repo-quality health command already exists:

- `python run.py --run-repo-quality-health`

Current known coverage from prior validation:

- maintenance findings summary
- decision-regression suite summary
- green result requires both maintenance and decision regression to be clean enough for the gate

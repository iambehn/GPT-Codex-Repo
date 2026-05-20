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

Current phase: `Phase 0 - inventory complete, blocked on canonical sample input`

Phase 1 implementation work should not start until the P0 blockers in [OPEN_QUESTIONS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/OPEN_QUESTIONS.md) are either resolved or explicitly deferred.

## Phase 0 Inventory Snapshot

### Runnable Command Surfaces

| Surface | Representative commands | Status | Notes |
| --- | --- | --- | --- |
| Game intake and pack validation | `python run.py --list-games`, `python run.py --validate-game-pack <game>` | `works` | `call_of_duty` and `marvel_rivals` validate from `assets`; `valorant` is starter-only |
| Maintenance and health | `python run.py --run-repo-quality-health`, `python run.py --inspect-quality-maintenance-findings` | `works` | health gate is the best-proven execution surface today |
| Onboarding review and publish-readiness | `python run.py --summarize-derived-row-review <draft_root>`, `python run.py --validate-onboarding-publish <draft_root>` | `works` | verified on `call_of_duty` onboarding draft `20260505T213332Z` |
| Review bridges and replay viewers | `--prepare-proxy-review`, `--prepare-runtime-review`, `--prepare-fused-review`, `--render-unified-replay-viewer`, `--launch-highlight-review-app` | `exists` | routed through `pipeline/commands/review_calibration.py`; not executed in this Phase 0 slice |
| Calibration and replay | `--calibrate-proxy-review`, `--replay-proxy-scoring`, `--calibrate-runtime-review`, `--replay-runtime-scoring`, `--promote-runtime-scoring`, `--rollback-runtime-scoring` | `exists` | surfaces are present; semantic execution not tested in this slice |
| Export and posting artifacts | `--export-highlight-selection`, `--create-highlight-export-batch`, `--record-post-ledger`, `--record-posted-metrics-snapshot`, `--report-posted-performance` | `exists` | owner surfaces are clear; no command execution in this slice |
| Workflow and registry state | `--refresh-clip-registry`, `--query-clip-registry`, `--query-workflow-queue`, `--transition-candidate-lifecycle` | `exists` | present and registry-backed; not executed in this slice |
| Shadow and training | `--export-v2-training-datasets`, `--run-shadow-ranking-replay`, `--train-shadow-ranking-model`, `--run-shadow-operator` | `exists` | present but intentionally out of the first happy path until needed |

### Game Support Snapshot

| Game | Source | Status | Key evidence |
| --- | --- | --- | --- |
| `call_of_duty` | `assets/games` | `recommended` | published pack validates, canonical media contract is `canonical`, abilities layer present, `112` templates, `112` published assets |
| `marvel_rivals` | `assets/games` | `available but weaker` | published pack validates, but canonical media contract is `partial` because the abilities layer is not yet canonical |
| `valorant` | `starter_assets` | `not ready` | starter-only seed, missing published-pack files, not suitable for the first runnable path |

### Input And Fixture Snapshot

| Input source | Status | Notes |
| --- | --- | --- |
| Real sample media under `assets/`, `tests/`, or `starter_assets/` | `missing` | no canonical local `.mp4` or equivalent sample was found in those repo surfaces |
| Review, calibration, and readiness fixtures under `tests/fixtures/` | `available` | current fixture families cover onboarding review, publish readiness, review bridges, fusion boundaries, runtime promotion, and detector calibration |
| Unrelated local media outside canonical repo surfaces | `ignore` | `.firecrawl/` and vendored `.venv/` media exist but are not approved happy-path inputs |

## First Supported Game

Status: provisional

Current recommendation:

- use `call_of_duty` as the first supported game for the happy path

Rationale:

- its published pack validates from `assets`
- its canonical media contract is `canonical`
- its published asset and template coverage is materially larger than `marvel_rivals`
- its onboarding and publish-readiness surfaces are already runnable locally

## Input Mode

Status: provisional

Allowed values:

- `real_sample`
- `fixture_bootstrap`

Current recommendation:

- use `fixture_bootstrap` only to unblock Phase 1 setup if needed
- keep real-sample proof as a hard requirement for final happy-path completion

Rules:

- `fixture_bootstrap` may be used to unblock Phase 1 setup
- `fixture_bootstrap` does not complete the happy path by itself
- real input success is required for happy-path completion unless explicitly waived

## Sample Input

Status: unresolved

Current blocker:

- no approved real sample clip was found in canonical repo input surfaces

Current bootstrap options:

- onboarding review and publish-readiness surfaces on `assets/games/call_of_duty/drafts/onboarding/20260505T213332Z`
- decision-regression fixtures under `tests/fixtures/`

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
- current suite count: `9`
- current test count: `18`
- green result requires both maintenance and decision regression to be clean enough for the gate

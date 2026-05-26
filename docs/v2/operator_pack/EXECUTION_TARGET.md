# Execution Target

Status: active-draft
Version: 1.1
Last updated: 2026-05-27

## Objective

Pin one concrete, runnable happy path for the gameplay highlight pipeline, then allow broad local-only autonomy to continue improving the pipeline without drifting into external-risk or out-of-scope work.

## Current Execution Doctrine

- Build one thin vertical slice before broadening scope.
- Prefer one supported game over cross-game abstractions.
- Prefer one sample input over generalized input support.
- Use existing repo surfaces before inventing new schemas or artifacts.
- Treat fixture-only success as bootstrap, not final proof, unless explicitly approved.
- Stop on explicit blockers instead of patching around them.
- Once a local-only path is proven, Codex may choose the next local-only target without waiting for explicit user selection.
- The standing user delegation authorizes long-running local-only execution without routine approval checkpoints.

## Current Phase Gate

Current phase: `Phase 7 - broad local autonomy on canonical local test path`

Current execution rule:

- use the bounded `call_of_duty` real-media path as the canonical operator sample for local pipeline testing
- treat bootstrap GPT review decisions as test-only, not as human editorial approval
- treat the current local export artifact as canonically user-authorized for local testing after the adopted fused-review decisions were re-applied
- treat the current runtime calibration artifact as canonically user-authorized for local testing after the adopted runtime-review decisions were re-applied
- do not treat any artifact from this path as publish-cleared external approval
- keep the hardening regressions green:
  - local export remains local-only until posting
  - runtime-only reviewed artifacts are not export-ready without fused selection
  - a synthetic bounded local-test regression proves runtime calibration pass and local export can coexist in one isolated workspace
  - a hook or export regression proves hook-rejected candidates can still export while hook remains advisory in V1
- treat the current `call_of_duty` sample as mechanically proven but editorially weak:
  - the hook artifact currently lands at `hook_mode: reject`
  - the registry hook-quality rollup currently lands at `editorial_viability_status: mechanics_only`
  - hook artifacts remain advisory in V1, so this does not block local export
  - later editorial scouting showed the active published `call_of_duty` pack currently has no promoted medal coverage, so further sample scouting alone is not the highest-leverage next step
- treat the current `call_of_duty` medal slice as structurally promoted but not yet clip-validated:
  - the filled medal packet was strong enough to produce a manual curated source bundle at `assets/games/call_of_duty/drafts/wiki_curated/20260526T233955Z`
  - that bundle bridged into onboarding draft `assets/games/call_of_duty/drafts/onboarding/20260526T234014Z`
  - all `15` promoted medal rows were accepted and published into the canonical `call_of_duty` pack
  - the published pack now contains `15` medals, `15` compatibility events, `127` templates, `3` runtime rules, and `4` fusion rules
  - post-promotion rerun on the same four-sample measurement set still produced `0 / 4` medal-driven outcomes and `0 / 4` hook candidates
  - frame probes show that at least one multiplayer-style sample exposes text or reward-banner signals such as `UAV`, `DOUBLE KI`, and `7TH KO` more clearly than native medal badge icons
  - the next useful improvement now depends on deciding whether the current clip family should be targeted through medal-icon detection, text or banner detection, or a medal-visible replacement sample set
- allow Codex to choose the next local-only target without waiting for explicit user selection when the work stays inside the gameplay highlight pipeline mission
- apply the standing user delegation:
  - choose and execute next local-only tasks without asking first
  - batch multiple milestones into one work block
  - treat docs, tests, regressions, refactors, workflow hardening, pack-coverage work, and narrow behavior fixes inside existing workflow families as pre-approved
- do not require a user approval checkpoint for small local-only changes such as regressions, docs clarifications, focused refactors, workflow hardening, quality or inspection improvements, or narrow behavior fixes inside existing contracts
- use milestone-style reporting by default:
  - report meaningful milestone completion
  - report real blockers
  - report material contract decisions
  - report branch or commit boundaries worth surfacing
  - do not stop for routine next-step permission

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
| Downloaded bounded public test media under `outputs/public_gameplay_mining/call_of_duty_test_sources/` | `canonical for local testing only` | ratified as the operator sample for local pipeline validation; still not publish-cleared content |
| Review, calibration, and readiness fixtures under `tests/fixtures/` | `available` | current fixture families cover onboarding review, publish readiness, review bridges, fusion boundaries, runtime promotion, and detector calibration |
| Unrelated local media outside canonical repo surfaces | `ignore` | `.firecrawl/` and vendored `.venv/` media exist but are not approved happy-path inputs |

## First Supported Game

Status: local-test-pinned

Current recommendation:

- use `call_of_duty` as the first supported game for the happy path

Rationale:

- its published pack validates from `assets`
- its canonical media contract is `canonical`
- its published asset and template coverage is materially larger than `marvel_rivals`
- its onboarding and publish-readiness surfaces are already runnable locally

## Input Mode

Status: bootstrap-pinned

Allowed values:

- `real_sample`
- `fixture_bootstrap`

Current recommendation:

- use `real_sample` for the bounded public-test-media proof path
- use `fixture_bootstrap` only where media is not required
- keep a canonical gameplay clip as the stronger follow-up input

Rules:

- `fixture_bootstrap` may be used to unblock non-media stages
- `fixture_bootstrap` does not complete the happy path by itself
- downloaded public test media may prove execution, but it does not imply publish-cleared input or human approval

## Sample Input

Status: bootstrap-pinned

Current bootstrap real-sample path:

- game: `call_of_duty`
- input mode: `real_sample`
- sample path: `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
- provenance: downloaded bounded public test media ratified as the canonical local operator sample

Fallback non-media bootstrap path:

- game: `call_of_duty`
- input mode: `fixture_bootstrap`
- sample path: `assets/games/call_of_duty/drafts/onboarding/20260505T213332Z`

## Current Bootstrap Command Path

The current bootstrap path is intentionally artifact-driven. It proves the existing review and readiness surfaces without pretending that media-backed sidecar generation is already working.

| Stage | Command | Current result | Meaning |
| --- | --- | --- | --- |
| Config load bootstrap | `python run.py --list-games --config config.yaml` | `ok: true` with `call_of_duty`, `marvel_rivals`, `valorant` | config path loads and CLI bootstraps |
| Candidate and review bootstrap | `python run.py --summarize-derived-row-review assets/games/call_of_duty/drafts/onboarding/20260505T213332Z` | `review_file_count: 122`, `pending_count: 0`, `decision_ready_count: 122`, `applied_count: 122` | review payloads are present, applied, and inspectable |
| Calibration and replay bootstrap | `python run.py --run-decision-regression-goldsets` | `suite_count: 9`, `total_tests: 18`, `ok: true` | current decision surfaces are regression-backed |
| Local readiness bootstrap | `python run.py --validate-onboarding-publish assets/games/call_of_duty/drafts/onboarding/20260505T213332Z` | `phase_status: bindings_pending`, `readiness: needs_population_review`, `can_publish: false` | blockers are inspectable and local-only |

Current limitation:

- the current local test sample is canonical for operator validation, not for external publishing
- the current real-media path still uses bootstrap GPT review decisions until human review replaces them

## Bounded Real-Media Proof Path

The current bounded real-media proof uses one downloaded public `call_of_duty` test clip segment and one isolated registry path. It is suitable for local pipeline validation only.

| Stage | Command | Current result | Meaning |
| --- | --- | --- | --- |
| Runtime sidecar generation | `python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --sample-fps 1 --limit-frames 30` | `ok: true`, `status: ok`, `event_count: 3` | media-backed `runtime_analysis_v1` sidecar is produced |
| Runtime review bridge | `python run.py --prepare-runtime-review call_of_duty --action all_non_skip --session-name bootstrap-real-cod` plus `python run.py --apply-runtime-review ...` | `approved_count: 2`, `rejected_count: 2` | reviewed runtime sidecars exist and persist review state |
| Runtime calibration | `python run.py --calibrate-runtime-review outputs/runtime_analysis/call_of_duty --game call_of_duty` | `status: ok`, `reviewed_sidecar_count: 4`, `release_gate_summary.status: pass` | runtime calibration consumes reviewed sidecars successfully |
| Fusion | `python run.py --fuse-clip-signals outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --runtime-sidecar outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s-c40d17236088.runtime_analysis.json --output-path outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json` | `ok: true`, `status: ok`, `fused_event_count: 3` | fused candidate surface is produced from real media |
| Fused review bridge | `python run.py --prepare-fused-review call_of_duty --sidecar-root outputs/fused_analysis/call_of_duty --action highlight_candidate --session-name bootstrap-real-cod-fused` plus `python run.py --apply-fused-review ...` | `approved_count: 1`, `rejected_count: 1` | fused review state propagates into candidate lifecycle |
| Local export boundary | `python run.py --export-highlight-selection --fused-sidecar outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json --output-path outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.highlight_selection.json` then `python run.py --create-workflow-run --workflow-type export_queue --registry-path outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite --game call_of_duty --output-path outputs/workflow_runs/call_of_duty/bootstrap-real-cod.export_queue.workflow_run.json` then `python run.py --create-highlight-export-batch --registry-path outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite --workflow-run-id workflow-11aea2937311834b --output-path outputs/highlight_exports/call_of_duty/bootstrap-real-cod.highlight_export_batch.json` | `highlight_export_batch_v1` created with `export_count: 1` | local export artifact exists without any posted-ledger mutation and is now backed by user-adopted fused-review decisions |

Why this is the current preferred continuation:

- `--analyze-roi-runtime` is the narrowest existing command that goes directly from media input to a `runtime_analysis_v1` sidecar
- `--prepare-runtime-review` already defaults to `outputs/runtime_analysis/<game>` and consumes those sidecars without needing a parallel workflow
- `--calibrate-runtime-review` is the first replay or calibration surface that uses reviewed runtime sidecars without requiring a separate trial config
- runtime analysis alone is not enough for the local export surface; the export path needs fused candidates plus lifecycle propagation
- `--create-highlight-export-batch` is the current local export surface; publication starts later at `--record-post-ledger`

## Happy-Path Stages

1. Config load
2. Input resolution
3. Sidecar and artifact generation
4. Candidate and review surface
5. Calibration and replay path
6. Local export-readiness bundle
7. Repo-quality health gate

Current bootstrap stage status:

| Stage | Status | Notes |
| --- | --- | --- |
| Config load | `proved` | `--list-games --config config.yaml` succeeds |
| Input resolution | `proved for bounded real-media bootstrap` | bounded public test clip path is pinned; canonical production sample is still unresolved |
| Sidecar and artifact generation | `proved for bounded real media` | runtime and fused sidecars were produced from one real clip segment |
| Candidate and review surface | `proved for bounded real media` | runtime and fused review bridges both persisted review state |
| Calibration and replay path | `canonically user-authorized for local testing` | runtime calibration passed on user-authorized reviewed real-media sidecars |
| Local export-readiness bundle | `canonically user-authorized for local testing` | `highlight_export_batch_v1` exists locally with one exported candidate and no post ledger; fused-review provenance now reflects user-adopted decisions |
| Editorial hook viability | `proved mechanics_only on current sample` | current hook artifact lands at `hook_mode: reject`, and current `hook-quality-rollups` query lands at `editorial_viability_status: mechanics_only` |
| Repo-quality health gate | `proved` | known green from prior validation |

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

Current governance limit:

- the current path is a canonical local test path on bounded public media
- the local export artifact is backed by user-adopted fused-review decisions
- the runtime calibration artifact is backed by user-adopted runtime-review decisions across its four reviewed runtime sidecars
- the current exported candidate is still editorially weak by hook-layer standards
- the current `call_of_duty` published pack has moved past the original medal-coverage gap, but not past the current clip-side signal gap:
  - published asset families now include `99` `hero_portrait`, `13` `equipment_icon`, and `15` `medal_icon`
  - the packet-derived medal slice has been published canonically
  - the current blocker is no longer onboarding workflow shape or medal-source promotion mechanics
  - the current blocker is that the measured clip set does not yet show clear medal-icon-driven runtime evidence even after promotion
  - the next useful discriminator is whether the intended clip family actually contains native medal badges or mostly text or reward-banner surfaces
- the standing user delegation authorizes longer autonomous local-only work blocks inside the existing pipeline mission
- Codex may autonomously choose the next local-only target, including pipeline quality hardening, editorial or hook-layer improvement, new local test samples, additional local-only execution targets within the existing pipeline mission, docs clarification, and focused refactors that improve inspectability, validation, or workflow clarity
- Codex must stop only when a hard-stop category is active:
  - the action would affect external systems
  - the action is destructive or hard to reverse
  - a real source-of-truth conflict exists
  - the requested change would materially expand project scope
  - local truth is too weak to continue responsibly

## Non-Goals

- new games
- new agent frameworks
- paid APIs
- platform posting or scheduling
- cloud or distributed infrastructure
- broad UI redesigns
- major schema rewrites
- learned ranking or large-scale model-selection work

## Hard-Stop Categories

Stop and escalate if any of these are true:

- the action would affect external systems:
  - posting
  - scheduling
  - external accounts
  - purchases or paid APIs
- the action is destructive or hard to reverse:
  - deleting source media
  - wiping state
  - irreversible migrations
- a real source-of-truth conflict exists:
  - code versus canonical docs
  - two competing schemas
  - conflicting lifecycle or status ownership
- the requested change would materially expand project scope:
  - a new product surface outside the gameplay highlight pipeline
  - a new external platform workflow
  - a new long-horizon subsystem not justified by the current pipeline mission
- local truth is too weak to continue responsibly:
  - artifact meaning is ambiguous
  - test behavior contradicts docs and code
  - multiple plausible contracts exist and none clearly governs

## Known Repo Fact

The repo-quality health command already exists:

- `python run.py --run-repo-quality-health`

Current known coverage from prior validation:

- maintenance findings summary
- decision-regression suite summary
- current suite count: `9`
- current test count: `18`
- green result requires both maintenance and decision regression to be clean enough for the gate

# Codex Backlog

Status: active-draft
Version: 0.7
Last updated: 2026-05-21

This backlog is phase-gated. Codex should work in order and stop when a phase blocker is hit.

## Phase 0: Repo Inventory And Operator Pack

### Task 0.1: Inventory runnable repo surfaces

Status:

- completed

Objective:

- identify existing commands and surfaces for config, onboarding, scanning, fusion, review bridge, calibration/replay, export-readiness, and repo health

Acceptance criteria:

- command or surface table exists
- each surface is marked `exists`, `works`, `fails`, `missing`, or `unknown`
- health command status is explicit
- no feature implementation occurs in this task

### Task 0.2: Inventory game support and recommend first game

Status:

- completed
- current recommendation: `call_of_duty`

Objective:

- determine which game has the most complete current support

Acceptance criteria:

- game configs and assets are listed
- required and missing assets are explicit
- recommended first game or blocker is stated

### Task 0.3: Inventory sample inputs and fixtures

Status:

- completed
- result: no canonical real sample media found; fixture bootstrap remains available but insufficient for final proof

Objective:

- determine whether a real sample clip or canonical fixture bootstrap path exists

Acceptance criteria:

- sample and fixture table exists
- `input_mode` recommendation is stated
- fixture-only risk is explicitly flagged if applicable

### Task 0.4: Inventory artifact, schema, and status ownership

Status:

- completed at the operator-pack level
- no immediate schema ownership conflict was found for candidate, review-session, lifecycle, or export artifact families
- local export boundary is now clarified as `highlight_export_batch_v1` without any posted-ledger artifact

Objective:

- identify existing artifacts, sidecars, candidate/review schemas, status enums, and ownership conflicts

Acceptance criteria:

- artifact list exists
- schema and status owners are listed
- conflicts are flagged
- no new schema is introduced in this task

### Task 0.5: Draft operator pack v0.1

Status:

- completed

Objective:

- activate and update the operator pack files for execution control

Deliverables:

- [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md)
- [CODEX_BACKLOG.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/CODEX_BACKLOG.md)
- [PIPELINE_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/PIPELINE_CONTRACTS.md)
- [OPEN_QUESTIONS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/OPEN_QUESTIONS.md)
- [FAILURE_TAXONOMY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/FAILURE_TAXONOMY.md)

Acceptance criteria:

- each file has status and version headers
- P0 blockers are explicit
- Phase 1 task shape is concrete enough to execute once P0 completes

## Phase 1: Config And Input Path

Goal:

- pin one game, one input mode, and one sample input path

Status:

- completed for bounded real-media bootstrap execution
- pinned game: `call_of_duty`
- pinned bootstrap real-sample path: `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
- fallback non-media bootstrap path remains `assets/games/call_of_duty/drafts/onboarding/20260505T213332Z`

Exit criteria:

- chosen config path loads
- chosen input path resolves or blocker is explicit
- no new schemas are introduced

Current limitation:

- the pinned sample is canonical for local testing only, not for external publishing

Current decision:

- use the bounded real-media path as the canonical local test sample
- keep the fixture bootstrap path available for non-media fallback
- do not treat bootstrap GPT review decisions as human approval unless the user explicitly adopts them for local testing

Recommended next task:

- rerun or re-annotate the same path with human review labels if the resulting calibration or export artifacts need canonical status

## Phase 2: Sidecar And Artifact Generation

Goal:

- produce the minimum stage artifacts required to move into review

Status:

- completed for bounded real-media bootstrap
- proof artifacts:
  - `runtime_analysis_v1` at `outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s-c40d17236088.runtime_analysis.json`
  - `fused_analysis_v1` at `outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json`

Exit criteria:

- artifact exists
- artifact parses
- required fields are present
- downstream review or replay surface can consume it

## Phase 3: Candidate And Review Surface

Goal:

- make one inspectable candidate or review path executable for the chosen happy path

Status:

- completed for bounded real-media bootstrap execution
- runtime review proof: one session applied with `approved_count: 2`, `rejected_count: 2`
- fused review proof: one session applied with `approved_count: 1`, `rejected_count: 1`
- resulting lifecycle state includes one `selected_for_export` candidate in the isolated registry

Exit criteria:

- evidence and status are inspectable
- review semantics are explicit
- no schema ownership conflict is active

## Phase 4: Calibration And Replay Path

Goal:

- make one calibration or replay path executable for the chosen happy path

Status:

- completed for bounded real-media canonical local testing
- runtime calibration proof artifact: `outputs/runtime_calibration/call_of_duty/bootstrap-real-cod.runtime_calibration.json`
- current proof result: `status: ok`, `reviewed_sidecar_count: 4`, `approved_count: 2`, `rejected_count: 2`, `release_gate_summary.status: pass`
- current decision provenance: user-adopted runtime-review decisions re-applied on `2026-05-20T23:53:19Z`

Exit criteria:

- replay or calibration artifact exists
- result is parseable and meaningful
- warnings and failure states are visible

## Phase 5: Local Export-Readiness

Goal:

- produce a local inspectable export-readiness result

Status:

- completed for bounded real-media bootstrap execution
- current proof artifact: `outputs/highlight_exports/call_of_duty/bootstrap-real-cod.highlight_export_batch.json`
- current proof result: `schema_version: highlight_export_batch_v1`, `export_count: 1`
- isolated registry result after refresh: approved candidate advanced to `lifecycle_state: exported` with `post_ledger_path: null`
- canonical local-test decision state now comes from user-adopted fused-review decisions
- note: this remains local-only and is not external posting approval

Exit criteria:

- bundle exists locally
- metadata and blockers are inspectable
- `platform_action_taken: false`

## Phase 6: Repo-Quality Health

Goal:

- prove the chosen path does not break the enforced quality surfaces

Status:

- already green on the current branch
- keep rerunning after any behavior change that touches the happy path or its governing surfaces

Exit criteria:

- targeted verification is green
- `python run.py --run-repo-quality-health` is green

## Phase 7: Hardening

Goal:

- reduce known failure risk without broadening project scope

Status:

- semantic hardening added for the current bootstrap path
- sample-governance decision is resolved
- local export canonicalization is resolved through user-adopted fused review
- runtime calibration canonicalization is resolved through user-adopted runtime review
- no active blocker remains for the bounded local test path

Allowed work:

- fix blockers discovered on the chosen path
- improve validation where false passes are likely
- document new execution truths in the operator pack

Current recommended hardening:

- keep the new local export-boundary regressions green:
  - local export remains local-only until posting
  - runtime-only reviewed artifacts remain non-exportable without fused selection
- use the canonicalization handoff:
  - `docs/handoffs/2026-05-21-call-of-duty-canonical-human-review-handoff.md`

Current stop rule:

- do not broaden implementation beyond the canonical local test path until a new execution target is explicitly chosen

Deferred until after the happy path:

- multi-game support
- platform posting
- new frameworks
- broad UI work
- large observability or dashboard systems

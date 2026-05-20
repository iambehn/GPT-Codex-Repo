# Codex Backlog

Status: active-draft
Version: 0.1
Last updated: 2026-05-20

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
- local export-readiness completion semantics remain an open follow-up

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

Exit criteria:

- chosen config path loads
- chosen input path resolves or blocker is explicit
- no new schemas are introduced

Current blocker:

- no canonical real sample input is available yet

Recommended next Phase 1 task:

- either provide a real `call_of_duty` sample clip path
- or explicitly proceed with a documented `fixture_bootstrap` step while keeping media-backed sidecar generation blocked

## Phase 2: Sidecar And Artifact Generation

Goal:

- produce the minimum stage artifacts required to move into review

Exit criteria:

- artifact exists
- artifact parses
- required fields are present
- downstream review or replay surface can consume it

## Phase 3: Candidate And Review Surface

Goal:

- make one inspectable candidate or review path executable for the chosen happy path

Exit criteria:

- evidence and status are inspectable
- review semantics are explicit
- no schema ownership conflict is active

## Phase 4: Calibration And Replay Path

Goal:

- make one calibration or replay path executable for the chosen happy path

Exit criteria:

- replay or calibration artifact exists
- result is parseable and meaningful
- warnings and failure states are visible

## Phase 5: Local Export-Readiness

Goal:

- produce a local inspectable export-readiness result

Exit criteria:

- bundle exists locally
- metadata and blockers are inspectable
- `platform_action_taken: false`

## Phase 6: Repo-Quality Health

Goal:

- prove the chosen path does not break the enforced quality surfaces

Exit criteria:

- targeted verification is green
- `python run.py --run-repo-quality-health` is green

## Phase 7: Hardening

Goal:

- reduce known failure risk without broadening project scope

Allowed work:

- fix blockers discovered on the chosen path
- improve validation where false passes are likely
- document new execution truths in the operator pack

Deferred until after the happy path:

- multi-game support
- platform posting
- new frameworks
- broad UI work
- large observability or dashboard systems

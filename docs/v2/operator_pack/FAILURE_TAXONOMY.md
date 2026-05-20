# Failure Taxonomy

Status: active
Version: 0.1
Last updated: 2026-05-20

This file records the seed failures that should shape Phase 0 inventory and the first happy-path build loop.

## RISK-CODEX-001: Happy-path overfitting

Meaning:

- the pipeline is patched to pass one sample and fails on the next realistic input

Required response:

- record the overfit condition
- add or update a validation path that proves the behavior is not sample-specific

## RISK-CODEX-002: Fixture-only false progress

Meaning:

- fixture inputs pass, but real input fails or has never been attempted

Required response:

- mark fixture-only success as bootstrap
- do not declare happy-path completion from fixtures alone

## RISK-CODEX-003: Artifact existence false pass

Meaning:

- an artifact file exists but lacks required fields, provenance, timestamps, or downstream usability

Required response:

- validate at least `L2`
- prefer `L3` or `L4` for stage completion claims

## RISK-CODEX-004: Green health, broken semantics

Meaning:

- repo-quality health is green while the chosen happy path is still semantically broken

Required response:

- treat the happy-path semantic failure as blocking
- add targeted verification rather than assuming the gate is sufficient

## RISK-CODEX-005: Operator file drift

Meaning:

- execution-control docs diverge from actual repo behavior

Required response:

- update operator files in the same slice as the behavior change
- do not leave changed execution rules only in chat

## RISK-CODEX-006: Schema ownership conflict

Meaning:

- a task encounters multiple plausible schema or status owners

Required response:

- stop and escalate
- do not patch around the conflict by creating a side schema

## RISK-CODEX-007: Export-ready confused with publish-ready

Meaning:

- a local bundle or readiness artifact is mistaken for approval to post externally

Required response:

- keep export-readiness local-only
- keep `platform_action_taken: false`
- treat posting as out of scope until explicitly approved

## RISK-CODEX-008: Codex runaway past blocker

Meaning:

- Codex keeps building around missing assets, missing decisions, or ambiguous contracts instead of stopping

Required response:

- stop on blocker
- write the blocker into [OPEN_QUESTIONS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/OPEN_QUESTIONS.md)
- do not compensate by widening architecture scope

## Named Failure Seeds

Use these exact seed names when a compact label is useful:

- `fixture_only_success`
- `artifact_exists_but_invalid`
- `health_green_semantic_fail`
- `operator_file_stale`
- `schema_ownership_conflict`
- `missing_required_game_asset`
- `review_runs_on_empty_data`
- `export_ready_not_publish_ready`

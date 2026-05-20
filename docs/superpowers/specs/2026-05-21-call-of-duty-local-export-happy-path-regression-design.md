## Goal

Add one focused regression that hardens the proven `call_of_duty` local export happy path without changing pipeline behavior, schemas, command surfaces, or operator workflow shape.

## Scope

In scope:
- one additive regression test on the existing export and registry contract
- isolated registry coverage for the local-only export boundary
- assertions for exported lifecycle state and absence of posting state

Out of scope:
- new CLI commands
- new inspectors or reports
- schema changes
- review-bridge behavior changes
- posting workflow changes
- real-media fixture ingestion inside the test

## Problem

The repo now has a manually proven bounded `call_of_duty` happy path that reaches:
- fused candidate approval
- isolated registry refresh
- workflow-run creation
- local highlight export batch creation

That path is documented in the operator pack, but there is not yet one explicit regression that asserts the exact local-only semantics of that boundary:
- a selected candidate becomes `exported`
- an export artifact path is recorded
- no post ledger is present before any posting step

Existing tests already cover the individual export and registry surfaces. The gap is a narrow semantic assertion for the local export boundary that was just proven operationally.

## Target Surface

Extend:
- `tests/test_highlight_export_batch.py`

Reuse existing helper coverage and existing registry queries instead of introducing a new test module.

## Recommended Approach

Use one new regression test that:
1. creates a fused sidecar with an approved fused review
2. refreshes an isolated temporary registry
3. exports highlight selection
4. derives hook candidates
5. creates an export workflow run
6. creates a highlight export batch
7. refreshes the isolated registry again
8. queries candidate lifecycles for the exported row

This keeps the change inside the already-governing export/registry contract and avoids adding a second happy-path harness.

## Assertions

The new test should assert:
- `highlight_export_batch_v1` is written
- `export_count == 1`
- exactly one candidate lifecycle row reaches `exported`
- the exported row has a non-empty `export_artifact_path`
- the exported row has `post_ledger_path is None`
- the selected candidate remains queryable through the existing lifecycle surface

## Constraints

- use a temporary isolated registry, not the repo’s operational registry
- do not depend on downloaded media
- do not add a synthetic posting step
- do not assert unrelated operator-pack wording or documentation text
- do not add a second source of truth for export readiness

## Backward Compatibility

- additive test coverage only
- no runtime behavior change
- no artifact contract change
- no command routing change

## Verification

Run:
- `python -m unittest tests.test_highlight_export_batch`
- `python run.py --run-repo-quality-health`

If the existing file already has a better placement point during implementation, keep the same assertions but stay on the narrowest existing test surface.

## Recommended Implementation Order

1. Add the new regression to `tests/test_highlight_export_batch.py`
2. Run the targeted export-batch test module
3. Run the repo-quality health gate
4. Update the operator pack only if execution truth changes, which is not expected for this slice

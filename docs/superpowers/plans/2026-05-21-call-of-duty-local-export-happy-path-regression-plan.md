# Call Of Duty Local Export Happy Path Regression Plan

## Goal

Add one additive regression that hardens the proven `call_of_duty` local export happy path at the exact local-only export boundary.

## Scope

In scope:
- one new regression in `tests/test_highlight_export_batch.py`
- isolated temporary registry coverage
- assertions for exported lifecycle state, export artifact linkage, and no posting linkage before posting

Out of scope:
- runtime or registry behavior changes
- new helper modules
- new CLI commands
- posting-path assertions beyond confirming local-only state

## Implementation Steps

1. Extend the existing export-batch test module.
- Add one new test in `tests/test_highlight_export_batch.py`
- Reuse the module’s existing fused-sidecar helper and tempdir setup

2. Model the already-proven local export chain.
- write one approved `call_of_duty` fused sidecar
- refresh an isolated temporary registry
- export the highlight selection manifest
- derive hook candidates
- create an export workflow run
- create the highlight export batch
- refresh the registry again after export

3. Assert the local-only export boundary.
- export manifest schema is `highlight_export_batch_v1`
- export count is `1`
- one candidate lifecycle row reaches `exported`
- exported row has a non-empty `export_artifact_path`
- exported row still has `post_ledger_path is None`
- the same candidate remains queryable through the existing `highlight-exports` registry surface

4. Keep the change contract-only.
- no posting ledger creation
- no synthetic posted metrics
- no changes to written manifest schemas
- no changes to runtime code

## Verification

Run:

```bash
source .venv/bin/activate && python -m unittest tests.test_highlight_export_batch
source .venv/bin/activate && python run.py --run-repo-quality-health
```

## Risks And Guards

- Risk: the new test duplicates broader posted-flow coverage.
  - Guard: stop the test before any posting step and assert the pre-post boundary only.

- Risk: the regression accidentally depends on repo-global state.
  - Guard: use a temporary isolated registry and tempdir-owned artifacts only.

- Risk: the exported candidate is asserted only through the file manifest and not the registry.
  - Guard: assert both the written export manifest and the registry query surface.

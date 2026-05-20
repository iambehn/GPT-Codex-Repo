# Call Of Duty Runtime-Only Not Export-Ready Regression Plan

## Goal

Add one additive regression that proves runtime-only reviewed artifacts are not enough to make a candidate export-ready. Local export must still require fused selection and lifecycle propagation.

## Scope

In scope:
- one new regression on the existing export-batch test surface
- isolated temporary-registry coverage
- assertions for empty export readiness before any fused selection exists

Out of scope:
- runtime-analysis behavior changes
- runtime-review bridge changes
- calibration changes
- fused-selection logic changes
- new helpers or command surfaces

## Implementation Steps

1. Extend `tests/test_highlight_export_batch.py`.
- add a local helper for a minimal reviewed `runtime_analysis_v1` sidecar if needed
- keep the change inside the existing export-batch module

2. Model the runtime-only precondition.
- write one approved runtime sidecar for `call_of_duty`
- refresh an isolated temporary registry
- do not write any fused sidecar
- do not write any highlight selection manifest

3. Assert the absence of export readiness.
- query `candidate-lifecycles` with `lifecycle_state="selected_for_export"` and assert `row_count == 0`
- query `query_workflow_queue("export_queue", ...)` and assert `row_count == 0`
- call `create_highlight_export_batch(...)` and assert:
  - `ok is False`
  - `status == "no_selected_candidates"`

4. Keep the slice contract-only.
- no posting step
- no synthetic metrics
- no operator-pack changes unless execution truth changes unexpectedly

## Verification

Run:

```bash
source .venv/bin/activate && python -m unittest tests.test_highlight_export_batch
source .venv/bin/activate && python run.py --run-repo-quality-health
```

## Risks And Guards

- Risk: the regression accidentally encodes a broader assumption about runtime-sidecar ingestion.
  - Guard: assert only the export boundary, not the full runtime-review lifecycle.

- Risk: the test duplicates broader workflow-queue coverage.
  - Guard: stop before any fused artifact or highlight-selection manifest exists.

- Risk: the runtime-only case is expressed through a new helper that drifts from existing test fixtures.
  - Guard: keep the helper minimal and local to the existing test module only if the current helpers are insufficient.

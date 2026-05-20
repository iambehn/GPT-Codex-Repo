## Goal

Add one focused regression that proves reviewed runtime artifacts and runtime calibration alone do not make a candidate export-ready. Local export must continue to require fused selection plus lifecycle propagation.

## Scope

In scope:
- one additive regression on the existing registry and export contract
- isolated temporary-registry coverage
- assertions for the absence of export readiness before fused selection exists

Out of scope:
- runtime-analysis schema changes
- runtime-review schema changes
- calibration behavior changes
- fused workflow behavior changes
- new CLI commands, inspectors, or reports

## Problem

The bounded `call_of_duty` happy-path proof established a non-obvious but important rule:

- runtime analysis can succeed
- runtime review can succeed
- runtime calibration can succeed
- and local export can still remain impossible until fused selection and lifecycle propagation happen

That rule is documented in the operator pack, but there is not yet one narrow regression that enforces it directly.

Without that regression, future changes could accidentally make runtime-only state look export-ready even though the current workflow contract requires selected fused candidates before export.

## Target Surface

Extend one existing test module:
- preferred: `tests/test_highlight_export_batch.py`
- acceptable fallback: `tests/test_clip_registry.py`

Prefer the narrowest existing surface that can assert both:
- the export queue stays empty
- the export batch tool returns `no_selected_candidates`

## Recommended Approach

Use one new regression test that:
1. creates a reviewed runtime sidecar in a temporary root
2. refreshes an isolated temporary registry
3. optionally creates a runtime review session artifact only if needed for registry recognition
4. does not create a fused sidecar
5. does not create a highlight selection manifest
6. queries the export queue
7. attempts to create a highlight export batch

This keeps the test aligned to the real workflow contract without inventing a new happy-path harness.

## Assertions

The new test should assert:
- no candidate lifecycle rows reach `selected_for_export`
- `query_workflow_queue("export_queue", ...)` returns `row_count == 0`
- `create_highlight_export_batch(...)` returns:
  - `ok: False`
  - `status: "no_selected_candidates"`
- no posting or export artifact linkage appears

## Constraints

- use a temporary isolated registry, not any operational registry
- do not synthesize fused selection in this slice
- do not mutate runtime or fused pipeline behavior
- do not introduce new helper modules unless the existing test surfaces are truly insufficient
- do not broaden the assertion into full review-bridge or calibration smoke coverage

## Backward Compatibility

- additive regression coverage only
- no runtime behavior change
- no artifact contract change
- no command routing change

## Verification

Run:
- `python -m unittest tests.test_highlight_export_batch`
- `python run.py --run-repo-quality-health`

If the implementation lands in a different existing test module, keep the same behavior assertions and run the narrowest relevant test target instead.

## Recommended Implementation Order

1. Add the regression on the narrowest existing export or registry test surface
2. Run the targeted test module
3. Run the repo-quality health gate
4. Update the operator pack only if execution truth changes, which is not expected in this slice

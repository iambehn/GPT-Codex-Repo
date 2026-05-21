# Call Of Duty Bounded Local Test Smoke Regression Plan

## Goal

Add one additive regression that proves the bounded `call_of_duty` local-test path can pass runtime calibration and reach the local export boundary in the same isolated workspace.

## Scope

In scope:
- one new regression in `tests/test_highlight_export_batch.py`
- synthetic runtime and fused sidecars in one temp root
- assertions for calibration pass, export success, and no posting state

Out of scope:
- runtime scoring changes
- registry behavior changes
- new test modules or helper packages
- posting or metrics assertions

## Implementation Steps

1. Extend the existing export-batch test module.
- add small runtime event and detection helpers if needed
- extend the existing runtime-sidecar helper so the test can control event and detection content

2. Model the runtime calibration half of the path.
- create four reviewed `call_of_duty` runtime sidecars
- make two score as approved highlight candidates
- make two score as rejected inspect or skip candidates
- run `run_calibrate_runtime_review(...)` against the temp runtime root

3. Model the local export half of the path.
- create one approved fused sidecar for the same temp workspace
- refresh an isolated registry
- export highlight selection
- derive hook candidates
- create an export workflow run
- create a highlight export batch
- refresh the registry after export

4. Assert the combined state machine.
- runtime calibration is `ok`
- `release_gate_summary.status == "pass"`
- reviewed split is `2 approved / 2 rejected`
- export manifest schema is `highlight_export_batch_v1`
- export count is `1`
- one candidate lifecycle row reaches `exported`
- exported row has a non-empty `export_artifact_path`
- exported row still has `post_ledger_path is None`

## Verification

Run:

```bash
source .venv/bin/activate && python -m unittest tests.test_highlight_export_batch
source .venv/bin/activate && python run.py --run-repo-quality-health
```

## Risks And Guards

- Risk: the test duplicates existing calibration coverage without proving the combined path.
  - Guard: require both calibration pass and export boundary assertions in one workspace.

- Risk: the calibration half depends on event shapes that are too minimal.
  - Guard: reuse the same event and detection shapes already proven in `tests/test_runtime_calibration.py`.

- Risk: the export half accidentally drifts into posting-path coverage.
  - Guard: stop at `exported` and assert `post_ledger_path is None`.

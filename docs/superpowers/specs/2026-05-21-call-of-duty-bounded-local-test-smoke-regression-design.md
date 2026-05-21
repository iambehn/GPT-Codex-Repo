## Goal

Add one focused synthetic regression that hardens the bounded `call_of_duty` local-test happy path by proving the same isolated workspace can both pass runtime calibration and reach the local export boundary without any posting step.

## Scope

In scope:
- one additive regression on the existing calibration, registry, and export contracts
- synthetic tempdir-owned runtime and fused sidecars only
- assertions for runtime calibration pass plus local-only export state

Out of scope:
- new CLI commands
- new helper modules
- real-media ingestion in the test
- review-bridge behavior changes
- posting or metrics workflow changes
- schema changes

## Problem

The repo now has a manually proven bounded `call_of_duty` local-test path that reaches:
- reviewed runtime sidecars
- a passing runtime calibration artifact
- approved fused selection
- local export batch creation without any posted-ledger state

Current regression coverage proves the export boundary and the runtime-only failure case separately, but it does not yet prove that both halves of the bounded path can coexist in one isolated workspace:
- runtime calibration passes with a reviewed `2 approved / 2 rejected` runtime set
- fused selection still advances one candidate to `exported`
- no posting state is created implicitly

That is the exact combined state machine that was proven manually and is now part of the operator doctrine.

## Target Surface

Extend:
- `tests/test_highlight_export_batch.py`

Reuse its tempdir, sidecar, registry, and export helpers rather than creating a new smoke harness.

## Recommended Approach

Use one new regression test that:
1. creates four reviewed runtime sidecars in a temp runtime root
2. runs runtime calibration on that runtime root and asserts the gate passes
3. creates one approved fused sidecar for the same temporary workspace
4. refreshes an isolated registry
5. exports highlight selection
6. derives hook candidates
7. creates an export workflow run
8. creates a highlight export batch
9. refreshes the registry again and inspects the exported candidate lifecycle

This keeps the change inside the existing governing contracts and avoids introducing a separate end-to-end tool or fixture family.

## Assertions

The new test should assert:
- runtime calibration returns `ok`
- runtime calibration returns `release_gate_summary.status == "pass"`
- runtime calibration returns `reviewed_sidecar_count == 4`
- runtime calibration returns `approved_count == 2`
- runtime calibration returns `rejected_count == 2`
- `highlight_export_batch_v1` is written
- `export_count == 1`
- exactly one candidate lifecycle row reaches `exported`
- the exported row has a non-empty `export_artifact_path`
- the exported row has `post_ledger_path is None`

## Constraints

- use only temporary isolated artifacts
- do not depend on downloaded public media
- do not use the GPT review bridge in the test
- do not add a synthetic posting step
- do not assert operator-pack prose
- do not add a second source of truth for happy-path completion

## Backward Compatibility

- additive test coverage only
- no runtime behavior change
- no artifact contract change
- no command routing change

## Verification

Run:
- `python -m unittest tests.test_highlight_export_batch`
- `python run.py --run-repo-quality-health`

## Recommended Implementation Order

1. Extend `tests/test_highlight_export_batch.py`
2. Add the minimal runtime sidecar helper inputs needed to produce a passing calibration split
3. Run the targeted test module
4. Run the repo-quality health gate
5. Update operator docs only if the governing execution truth changes

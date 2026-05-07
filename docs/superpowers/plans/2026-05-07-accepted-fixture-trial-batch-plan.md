# Accepted Fixture Trial Batch Implementation Plan

## Scope

Implement the accepted fixture-trial batch wrapper approved in:

- [docs/superpowers/specs/2026-05-07-accepted-fixture-trial-batch-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-fixture-trial-batch-design.md)

This subproject adds one thin orchestration layer over the existing
fixture-trial execution path. It consumes a `fixture_source_manifest_v1`
artifact, runs fixture-trial once per row, and records one batch result
artifact.

It does not add detector logic, new sidecar schemas, review state, or registry
mutation.

## Desired End State

After this work:

- the repo can run fixture-trial across an adapted accepted source manifest
- the result is captured in one `accepted_fixture_trial_batch_v1` artifact
- per-fixture success/failure and output paths are preserved
- the CLI can run this batch wrapper with compact output by default
- one real `marvel_rivals` accepted batch run has been executed and inspected

## Implementation Strategy

Implement this as one orchestration module plus one CLI route:

1. build the batch runner module
2. add focused unit coverage with mocked fixture-trial execution
3. wire the CLI route and compact output
4. run focused verification
5. run one real batch using the current accepted source manifest

Keep the wrapper narrow. Reuse the existing fixture source-manifest loader and
the existing fixture-trial execution path instead of introducing new execution
contracts.

## Slice 1: Batch Runner Module

Add:

- `pipeline/accepted_fixture_trial_batch.py`

Implement:

- schema constant:
  - `accepted_fixture_trial_batch_v1`
- batch entrypoint that:
  - loads a `fixture_source_manifest_v1`
  - iterates fixtures in source order
  - calls the existing fixture-trial runner once per fixture
  - captures per-fixture outcomes
  - writes one batch manifest

### Top-Level Output Fields

- `ok`
- `status`
- `schema_version`
- `batch_id`
- `created_at`
- `source_manifest_path`
- `source_manifest_id`
- `game`
- `fixture_count`
- `success_count`
- `failed_count`
- `results`
- `manifest_path`

### Per-Result Fields

- `fixture_id`
- `game`
- `source_path`
- `status`
- `error`
- `proxy_sidecar_path`
- `runtime_sidecar_path`
- `fused_sidecar_path`
- `trial_result_path`

### Status Rules

- `ok`
  - all fixtures succeeded
- `partial`
  - at least one success and at least one failure
- `failed`
  - all fixtures failed after a valid batch started

Source-manifest validation failures should return structured non-execution
errors rather than a fixture-level batch result.

## Slice 2: Unit Tests

Add:

- `tests/test_accepted_fixture_trial_batch.py`

Cover:

- success path when all fixture-trial calls succeed
- partial batch when one fixture fails
- failed batch when all fixtures fail
- source order preserved in output results
- invalid source manifest rejected before execution begins
- output manifest written correctly

Patch the existing fixture-trial runner rather than invoking real detector
execution.

## Slice 3: CLI Route

Update:

- `run.py`
- `tests/test_run.py`

Add:

- import for the batch runner
- route helper:
  - `run_accepted_fixture_trial_batch(...)`
- parser flag:
  - `--run-accepted-fixture-trial-batch`
- parser input:
  - `--fixture-source-manifest`
- compact output branch
- `main()` dispatch branch

### Compact Output Contract

- `ok`
- `status`
- `schema_version`
- `batch_id`
- `fixture_count`
- `success_count`
- `failed_count`
- `manifest_path`
- sampled results if needed

### CLI Tests

Add focused route coverage for:

- compact output by default
- full-json passthrough
- expected argument forwarding

## Slice 4: Focused Verification

Run:

```bash
python3 -m py_compile \
  pipeline/accepted_fixture_trial_batch.py \
  tests/test_accepted_fixture_trial_batch.py \
  run.py \
  tests/test_run.py
```

Run:

```bash
python3 -m unittest \
  tests.test_accepted_fixture_trial_batch \
  tests.test_run.RunTests.test_cli_routes_to_run_accepted_fixture_trial_batch_compacts_output_by_default \
  tests.test_run.RunTests.test_cli_routes_to_run_accepted_fixture_trial_batch_with_full_json
```

## Slice 5: Real Batch Run

Use:

- [accepted-source-manifest-50f2384aaf9f.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_source_manifests/marvel_rivals/accepted-source-manifest-50f2384aaf9f.json)

Run:

```bash
python3 run.py \
  --run-accepted-fixture-trial-batch \
  --fixture-source-manifest \
  /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_source_manifests/marvel_rivals/accepted-source-manifest-50f2384aaf9f.json \
  --full-json
```

Inspect:

- batch status
- success/failure counts
- per-fixture emitted paths
- whether proxy/runtime/fused sidecars appear as expected

## Commit Strategy

Prefer one coherent implementation commit containing:

- batch runner module
- batch runner tests
- CLI route
- CLI tests

Generated `outputs/` artifacts remain operational state and should not be
included in the implementation commit unless explicitly requested.

## Risks

### Risk: Batch wrapper diverges from single-fixture behavior

Mitigation:

- delegate execution to the existing fixture-trial path
- keep orchestration separate from trial logic

### Risk: One fixture failure aborts the batch

Mitigation:

- catch and record per-fixture failures
- continue processing later fixtures

### Risk: Output payload becomes noisy

Mitigation:

- use the existing compact CLI pattern
- keep only the highest-signal per-fixture fields in the compact summary

## Out of Scope Follow-Up

After this batch wrapper exists, the next design cycle can decide how to turn
the generated sidecars into:

- review preparation
- review application
- registry refresh

Those steps should remain separate subprojects rather than being bundled into
this orchestration wrapper.

# Accepted Fixture Trial Batch Design

## Summary

Add a thin batch wrapper that consumes an adapted
`fixture_source_manifest_v1` artifact and runs the existing fixture-trial path
once per fixture row.

This wrapper is the next operational step after:

- accepted clip inventory
- accepted clip intake manifest
- accepted intake source-manifest adapter

It does not change detector logic, sidecar schemas, review flows, or registry
state. It only orchestrates existing fixture-trial execution across the accepted
source manifest and writes one structured batch result artifact.

## Problem

The repo can now materialize an accepted source manifest:

- [accepted-source-manifest-50f2384aaf9f.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_source_manifests/marvel_rivals/accepted-source-manifest-50f2384aaf9f.json)

But there is no durable workflow boundary that says:

- run fixture-trial across this accepted source set
- collect outcomes in one auditable artifact
- preserve success/failure and sidecar paths per clip

Using shell loops or one-off manual invocation would be operationally brittle
and would not produce a structured batch record.

## Goals

- Reuse the existing fixture-trial implementation per fixture row.
- Add one batch wrapper over the existing fixture source-manifest contract.
- Preserve ordered per-fixture outcomes in one batch artifact.
- Record output paths for generated proxy/runtime/fused sidecars and trial
  result artifacts.
- Keep this step operationally narrow and free of review or registry side
  effects.

## Non-Goals

- No new detector logic.
- No new sidecar schema.
- No review preparation.
- No registry refresh.
- No promotion, scoring, or approval semantics.
- No rewriting of single-fixture `--run-fixture-trial`.

## Approaches Considered

### Option 1: Manifest-driven wrapper over fixture-trial

Add a batch module that:

- loads one `fixture_source_manifest_v1`
- iterates fixtures in order
- calls the existing fixture-trial path per row
- writes one structured batch manifest

Pros:

- reuses the current execution path
- clean audit trail
- small scope

Cons:

- adds one orchestration artifact

### Option 2: Widen `--run-fixture-trial`

Teach the single-fixture command to accept a manifest and internally loop.

Rejected because it mixes a low-level primitive with batch orchestration and
widens the command contract.

### Option 3: One-off script or shell loop

Rejected because it is not durable enough and produces no structured batch
record.

## Selected Design

Implement Option 1.

Add a thin wrapper:

- `pipeline/accepted_fixture_trial_batch.py`

It will accept one adapted source manifest, run existing fixture-trial logic per
fixture, and write one batch result manifest under `outputs/`.

## Inputs

- one `fixture_source_manifest_v1` artifact produced from the accepted-intake
  adapter
- optional output root/path overrides
- optional execution flags that are already supported by the fixture-trial path
  only if they can be forwarded without changing semantics

The first version should default to the same sidecar-generation behavior the
existing fixture-trial path already uses.

## Output Contract

Schema:

- `accepted_fixture_trial_batch_v1`

Default output root:

- `outputs/accepted_fixture_trials/<game>/`

### Top-Level Fields

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

## Execution Rules

1. Load and validate the input source manifest.
2. Process fixtures in manifest order.
3. Call existing fixture-trial logic once per fixture.
4. Record success or failure per fixture without losing later fixtures because
   of one failure.
5. Return:
   - `status: ok` when all fixtures succeed
   - `status: partial` when at least one succeeds and at least one fails
   - `status: failed` when all fixtures fail

## Provenance

The batch artifact must preserve:

- source manifest path
- source manifest id
- fixture ids from the source manifest
- emitted sidecar/result paths per fixture

## CLI Surface

Add one command in `run.py`:

- `--run-accepted-fixture-trial-batch`

Inputs:

- `--fixture-source-manifest <PATH>`
- optional `--output-root <PATH>`
- optional `--output-path <PATH>`

Output:

- compact JSON by default
- full payload with `--full-json`

Compact summary fields:

- `ok`
- `status`
- `schema_version`
- `batch_id`
- `fixture_count`
- `success_count`
- `failed_count`
- `manifest_path`
- sample of per-fixture results if useful

## Error Handling

Return structured failures for:

- missing source manifest
- invalid manifest schema
- malformed fixture rows
- execution exceptions from fixture-trial per row

One fixture failure must not abort the whole batch unless the source manifest
itself is invalid and execution cannot start.

## Testing

Add focused tests for:

- valid source manifest -> batch success
- partial success when one fixture fails
- failed batch when all fixtures fail
- source manifest order preserved in results
- invalid source manifest rejected
- CLI compact output
- CLI full-json passthrough

Tests should patch the existing fixture-trial runner rather than executing the
real detectors.

## Verification

After implementation:

- run targeted unit tests for the batch module and CLI route
- run one real batch using:
  - [accepted-source-manifest-50f2384aaf9f.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_source_manifests/marvel_rivals/accepted-source-manifest-50f2384aaf9f.json)
- inspect:
  - batch status
  - fixture counts
  - per-fixture result paths
  - whether sidecars are emitted as expected

## Expected Outcome

This wrapper creates the first durable sidecar-generation boundary for accepted
clips:

- source manifest stays the existing downstream contract
- fixture-trial stays the existing per-clip execution primitive
- batch wrapper adds auditable orchestration across the accepted source set

That is the smallest reliable step toward generating reviewable pipeline
artifacts from the accepted `marvel_rivals` clips.

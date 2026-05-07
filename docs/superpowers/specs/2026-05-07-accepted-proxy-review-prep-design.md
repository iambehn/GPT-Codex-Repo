# Accepted Proxy Review Prep Design

## Summary

Add a thin wrapper that consumes an `accepted_fixture_trial_batch_v1` artifact,
collects successful proxy sidecars, and routes them into the existing proxy
review preparation workflow.

This is the first review-oriented step in the accepted-clip pipeline:

- accepted inventory
- accepted intake manifest
- accepted source-manifest adapter
- accepted fixture-trial batch
- accepted proxy review prep

The wrapper does not create a new review schema and does not apply review
decisions. It only orchestrates accepted proxy sidecars into the existing proxy
review preparation flow and writes one provenance-preserving artifact.

## Problem

The repo can now generate proxy sidecars for all accepted `marvel_rivals`
fixtures through:

- [accepted_fixture_trial_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_fixture_trials/marvel_rivals/34e70b59308f/accepted_fixture_trial_batch.json)

But there is no accepted-specific orchestration layer that:

- discovers those successful proxy sidecars from the batch artifact
- routes them into the existing proxy review preparation workflow
- preserves fixture-level provenance back to the accepted batch run

Without this wrapper, proxy review prep would require manual path assembly and
would lose a clean operational boundary.

## Goals

- Reuse the existing proxy review preparation flow.
- Consume one accepted fixture-trial batch manifest as the source of sidecars.
- Prepare review input only for successful rows that emitted proxy sidecars.
- Preserve accepted fixture ids and source batch provenance.
- Emit one accepted review-prep artifact under `outputs/`.

## Non-Goals

- No new review schema.
- No review application or decision writing.
- No registry mutation.
- No runtime/fused review prep in this step.
- No new sidecar generation.

## Approaches Considered

### Option 1: Accepted proxy review-prep wrapper

Add:

- `pipeline/accepted_proxy_review_prep.py`

This wrapper:

- validates one accepted batch manifest
- extracts successful proxy sidecar paths
- invokes the existing proxy review-prep path
- records per-fixture preparation outcomes

Pros:

- preserves existing review workflow contracts
- keeps accepted orchestration explicit
- easy to test

Cons:

- adds one extra orchestration artifact

### Option 2: Direct use of existing proxy review prep

Rejected because it pushes manual sidecar discovery and provenance handling onto
the operator.

### Option 3: New accepted-specific review schema

Rejected because it widens scope before proving the existing proxy review flow
is insufficient.

## Selected Design

Implement Option 1.

The accepted proxy review-prep wrapper will accept one
`accepted_fixture_trial_batch_v1` artifact and produce one
accepted-specific orchestration artifact while reusing the existing proxy review
preparation implementation underneath.

## Input

- one `accepted_fixture_trial_batch_v1` manifest

The wrapper should only consider rows where:

- `status == "ok"`
- `proxy_sidecar_path` is present

## Output Contract

Schema:

- `accepted_proxy_review_prep_v1`

Default output root:

- `outputs/accepted_proxy_reviews/<game>/`

### Top-Level Fields

- `ok`
- `status`
- `schema_version`
- `review_prep_id`
- `created_at`
- `source_batch_manifest_path`
- `source_batch_id`
- `game`
- `fixture_count`
- `prepared_count`
- `skipped_count`
- `results`
- `manifest_path`

### Per-Result Fields

- `fixture_id`
- `proxy_sidecar_path`
- `status`
- `error`
- `prepared_review_path`

## Execution Rules

1. Load and validate the accepted batch manifest.
2. Filter to successful rows with a non-empty `proxy_sidecar_path`.
3. Invoke the existing proxy review preparation workflow on those sidecars.
4. Record one result row per accepted fixture considered by the wrapper.
5. Return:
   - `status: ok` when all eligible fixtures prepare successfully
   - `status: partial` when at least one prepares and at least one fails/skips
   - `status: failed` when no eligible fixtures prepare successfully

## Provenance

The wrapper must preserve:

- source accepted batch manifest path
- source batch id
- accepted fixture ids
- proxy sidecar path per prepared row
- prepared review artifact path per row

## CLI Surface

Add one command in `run.py`:

- `--prepare-accepted-proxy-review`

Inputs:

- `--accepted-fixture-trial-batch-manifest <PATH>`
- optional `--output-root <PATH>`
- optional `--output-path <PATH>`

Output:

- compact JSON by default
- full payload with `--full-json`

Compact summary fields:

- `ok`
- `status`
- `schema_version`
- `review_prep_id`
- `fixture_count`
- `prepared_count`
- `skipped_count`
- `manifest_path`

## Error Handling

Return structured failures for:

- missing batch manifest
- invalid batch schema
- malformed result rows
- no successful proxy sidecars found
- proxy review-prep failures

Suggested statuses:

- `invalid_accepted_fixture_trial_batch`
- `no_proxy_sidecars`
- `ok`
- `partial`
- `failed`

## Testing

Add focused tests for:

- successful review prep across eligible proxy sidecars
- skipped rows when batch entries failed or lacked proxy sidecars
- partial result when one prep call fails
- invalid batch schema rejected
- no eligible proxy sidecars handled explicitly
- CLI compact output
- CLI full-json passthrough

Tests should patch the existing proxy review-prep function rather than invoking
the real review stack.

## Verification

After implementation:

- run targeted unit tests for the wrapper and CLI route
- run the wrapper against:
  - [accepted_fixture_trial_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_fixture_trials/marvel_rivals/34e70b59308f/accepted_fixture_trial_batch.json)
- inspect:
  - prepared fixture count
  - prepared review artifact paths
  - preserved fixture ids and proxy sidecar paths

## Expected Outcome

This wrapper creates the first durable bridge from accepted clip sidecars into
the repo’s existing human review workflow:

- accepted batch remains the source of successful sidecars
- proxy review prep remains the existing review entrypoint
- accepted wrapper adds only the orchestration and provenance layer

That is the smallest reliable step toward explicit human review of accepted clip
sidecars without widening downstream review contracts.

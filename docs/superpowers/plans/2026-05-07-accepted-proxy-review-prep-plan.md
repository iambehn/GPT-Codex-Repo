# Accepted Proxy Review Prep Implementation Plan

## Scope

Implement the accepted proxy review-prep wrapper approved in:

- [docs/superpowers/specs/2026-05-07-accepted-proxy-review-prep-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-proxy-review-prep-design.md)

This subproject adds one thin orchestration layer over the existing proxy
review preparation flow. It consumes an `accepted_fixture_trial_batch_v1`
artifact, selects successful proxy sidecars, routes those sidecars into the
existing proxy review bridge, and records one provenance-preserving wrapper
artifact.

It does not add a new review schema, does not apply review decisions, and does
not mutate the registry.

## Desired End State

After this work:

- the repo can prepare proxy review from one accepted fixture-trial batch
- the existing proxy review bridge remains the underlying execution path
- one `accepted_proxy_review_prep_v1` artifact preserves fixture-level
  provenance back to the accepted batch
- the CLI can run this wrapper with compact output by default
- one real `marvel_rivals` accepted proxy review-prep run has been executed and
  inspected

## Implementation Strategy

Implement this as one orchestration module plus one CLI route:

1. build the wrapper module
2. add focused unit coverage with a patched bridge function
3. wire the CLI route and compact output
4. run focused verification
5. run one real wrapper invocation on the current accepted batch

Keep the wrapper narrow. Reuse the existing proxy review-prep bridge instead of
re-implementing sidecar discovery, queue materialization, or review session
writing.

## Slice 1: Wrapper Module

Add:

- `pipeline/accepted_proxy_review_prep.py`

Implement:

- schema constant:
  - `accepted_proxy_review_prep_v1`
- wrapper entrypoint that:
  - loads an `accepted_fixture_trial_batch_v1` artifact
  - filters to rows with `status == "ok"` and non-empty `proxy_sidecar_path`
  - translates those rows into the minimal batch-report shape expected by
    `pipeline.proxy_review_bridge.prepare_proxy_review(...)`
  - invokes the existing proxy review bridge
  - maps returned prepared review items back to accepted fixture ids
  - writes one accepted review-prep manifest

### Top-Level Output Fields

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
- `proxy_review_session_manifest_path`
- `proxy_review_session_id`
- `results`
- `manifest_path`

### Per-Result Fields

- `fixture_id`
- `proxy_sidecar_path`
- `status`
- `error`
- `prepared_review_path`

### Status Rules

- `ok`
  - all eligible proxy sidecars prepare successfully
- `partial`
  - at least one eligible row prepares and at least one eligible row fails or
    is skipped
- `failed`
  - no eligible row prepares successfully after valid execution starts
- `no_proxy_sidecars`
  - the batch is valid but contains no eligible proxy sidecars

Batch validation failures should return structured non-execution errors rather
than a wrapper artifact with execution counts.

## Slice 2: Unit Tests

Add:

- `tests/test_accepted_proxy_review_prep.py`

Cover:

- success path when all eligible proxy sidecars prepare successfully
- skipped rows when batch entries failed or lacked a proxy sidecar
- partial result when one eligible sidecar fails to map into a prepared item
- invalid batch schema rejected before bridge execution begins
- explicit `no_proxy_sidecars` result when the batch has no eligible rows
- output manifest written correctly

Patch the bridge function rather than invoking the real GPT review repo.

## Slice 3: CLI Route

Update:

- `run.py`
- `tests/test_run.py`

Add:

- import for the wrapper
- route helper:
  - `run_prepare_accepted_proxy_review(...)`
- parser flag:
  - `--prepare-accepted-proxy-review`
- parser input:
  - `--accepted-fixture-trial-batch-manifest`
- compact output branch
- `main()` dispatch branch

### Compact Output Contract

- `ok`
- `status`
- `schema_version`
- `review_prep_id`
- `fixture_count`
- `prepared_count`
- `skipped_count`
- `proxy_review_session_manifest_path`
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
  pipeline/accepted_proxy_review_prep.py \
  tests/test_accepted_proxy_review_prep.py \
  run.py \
  tests/test_run.py
```

Run:

```bash
python3 -m unittest \
  tests.test_accepted_proxy_review_prep \
  tests.test_run.RunTests.test_cli_routes_to_prepare_accepted_proxy_review_compacts_output_by_default \
  tests.test_run.RunTests.test_cli_routes_to_prepare_accepted_proxy_review_with_full_json
```

## Slice 5: Real Wrapper Run

Use:

- [accepted_fixture_trial_batch.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_fixture_trials/marvel_rivals/34e70b59308f/accepted_fixture_trial_batch.json)

Run:

```bash
python3 run.py \
  --prepare-accepted-proxy-review \
  --accepted-fixture-trial-batch-manifest \
  /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_fixture_trials/marvel_rivals/34e70b59308f/accepted_fixture_trial_batch.json \
  --full-json
```

Inspect:

- wrapper status
- prepared/skipped counts
- proxy review session manifest path
- per-fixture prepared review paths

## Commit Strategy

Prefer one coherent implementation commit containing:

- accepted proxy review-prep module
- accepted proxy review-prep tests
- CLI route
- CLI tests

Generated `outputs/` artifacts remain operational state and should not be
included in the implementation commit unless explicitly requested.

## Risks

### Risk: Wrapper redefines proxy review behavior

Mitigation:

- translate accepted batch rows into the bridge's existing batch-report input
  shape
- keep queue materialization inside `prepare_proxy_review(...)`

### Risk: Mapping accepted fixture ids back to prepared review items becomes brittle

Mitigation:

- build the bridge batch-report rows directly from accepted batch rows
- map prepared items back by resolved proxy sidecar path

### Risk: Missing or failed rows appear as silent success

Mitigation:

- compute prepared and skipped counts explicitly
- emit one result row per eligible accepted fixture

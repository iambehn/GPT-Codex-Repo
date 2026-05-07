# Accepted Intake Source Manifest Adapter Implementation Plan

## Scope

Implement the accepted-intake source-manifest adapter approved in:

- [docs/superpowers/specs/2026-05-07-accepted-intake-source-manifest-adapter-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-intake-source-manifest-adapter-design.md)

This subproject adds one narrow translation step from:

- `accepted_clip_intake_manifest_v1`

to:

- the existing downstream source-manifest shape

It does not generate sidecars, start review, or mutate registry state.

## Desired End State

After this work:

- the repo can adapt one accepted-intake manifest into one downstream
  source-manifest artifact
- the output preserves provenance back to the accepted-intake manifest
- the output uses canonical clip paths only
- the CLI exposes the adapter with compact output by default
- one real adapted source manifest has been built from the current
  `marvel_rivals` accepted-intake manifest

## Implementation Strategy

Build this as one adapter module plus one CLI route:

1. implement the adapter module
2. add focused unit tests
3. wire the CLI route and compact output
4. run focused verification
5. adapt the real intake manifest and inspect the result

Do not widen downstream source-manifest consumers in this subproject.

## Slice 1: Adapter Module

Add:

- `pipeline/accepted_clip_source_manifest_adapter.py`

Implement:

- adapter entrypoint to load one accepted-intake manifest
- strict schema validation for `accepted_clip_intake_manifest_v1`
- deterministic adapted manifest id
- row translation from intake rows into the existing downstream source-manifest
  shape
- JSON artifact writing under `outputs/accepted_clip_source_manifests/<game>/`

### Module Responsibilities

- validate accepted-intake manifest structure
- preserve row order
- derive one stable downstream row id per intake row from `clip_id`
- copy only canonical source path
- preserve accepted-intake provenance at top level and per row where supported
- emit one standalone adapted manifest

### Explicit Non-Responsibilities

- no media inspection
- no duplicate regrouping
- no sidecar generation
- no review state
- no registry mutation

## Slice 2: Unit Tests

Add:

- `tests/test_accepted_clip_source_manifest_adapter.py`

Cover:

- valid intake manifest adapts successfully
- output row count matches input row count
- output rows use only canonical clip paths
- stable row id mapping from `clip_id`
- top-level provenance fields are preserved
- invalid intake schema is rejected
- no-rows manifest returns explicit `no_rows` status

Use synthetic intake manifests in temporary directories.

## Slice 3: CLI Route

Update:

- `run.py`
- `tests/test_run.py`

Add:

- import for the adapter module
- route helper:
  - `run_adapt_accepted_clip_intake_to_source_manifest(...)`
- parser flag:
  - `--adapt-accepted-clip-intake-to-source-manifest`
- parser input:
  - `--accepted-clip-intake-manifest`
- compact output branch
- `main()` dispatch branch

### Compact Output Contract

- `ok`
- `status`
- adapted manifest id
- `row_count`
- output manifest path
- source accepted-intake manifest id

### CLI Tests

Add focused route coverage for:

- compact output by default
- full-json passthrough
- expected argument forwarding

## Slice 4: Focused Verification

Run:

```bash
python3 -m py_compile \
  pipeline/accepted_clip_source_manifest_adapter.py \
  tests/test_accepted_clip_source_manifest_adapter.py \
  run.py \
  tests/test_run.py
```

Run:

```bash
python3 -m unittest \
  tests.test_accepted_clip_source_manifest_adapter \
  tests.test_run.RunTests.test_cli_routes_to_adapt_accepted_clip_intake_to_source_manifest_compacts_output_by_default \
  tests.test_run.RunTests.test_cli_routes_to_adapt_accepted_clip_intake_to_source_manifest_with_full_json
```

## Slice 5: Real Artifact Adaptation

Use:

- [accepted-clip-intake-15e8592f6d81.manifest.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_intake/marvel_rivals/accepted-clip-intake-15e8592f6d81.manifest.json)

Run:

```bash
python3 run.py \
  --adapt-accepted-clip-intake-to-source-manifest \
  --accepted-clip-intake-manifest \
  /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_intake/marvel_rivals/accepted-clip-intake-15e8592f6d81.manifest.json \
  --full-json
```

Inspect:

- row count matches intake manifest
- only canonical paths appear
- accepted-intake provenance is present
- no duplicate-variant fields are reintroduced

## Commit Strategy

Prefer one coherent implementation commit containing:

- adapter module
- adapter tests
- CLI route
- CLI tests

Generated output artifacts under `outputs/` should remain operational state and
stay out of the implementation commit unless explicitly requested.

## Risks

### Risk: Wrong downstream source-manifest shape

Mitigation:

- inspect the existing source-manifest contract before implementation
- map only fields that the downstream shape already supports

### Risk: Provenance dropped during translation

Mitigation:

- include source accepted-intake manifest path and id at top level
- derive stable row ids from accepted `clip_id`
- add direct test coverage for provenance fields

### Risk: Duplicate semantics leak back in

Mitigation:

- adapt only from canonical intake rows
- explicitly whitelist copied row fields

## Out of Scope Follow-Up

Once this adapter exists, the next design cycle can decide how to consume the
adapted source manifest to produce reviewable pipeline inputs:

- sidecar generation
- review bridge preparation
- registry refresh

Those workflow changes should be designed separately rather than bundled into
this adapter.

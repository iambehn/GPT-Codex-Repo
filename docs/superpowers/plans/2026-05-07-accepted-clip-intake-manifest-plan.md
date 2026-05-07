# Accepted Clip Intake Manifest Implementation Plan

## Scope

Implement the standalone accepted-clip intake manifest workflow approved in:

- [docs/superpowers/specs/2026-05-07-accepted-clip-intake-manifest-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-clip-intake-manifest-design.md)

The deliverable is one builder plus one CLI route that converts an
`accepted_clip_inventory_v1` artifact into one frozen
`accepted_clip_intake_manifest_v1` artifact.

## Desired End State

After this work:

- the repo can materialize an accepted intake manifest from an accepted
  inventory manifest
- the output lives under `outputs/accepted_clip_intake/<game>/`
- the manifest contains only canonical clip rows
- the CLI emits compact output by default and full JSON on request
- one real `marvel_rivals` intake artifact has been built from the existing
  accepted inventory

## Implementation Strategy

Build this as a narrow, isolated workflow:

1. implement the builder module
2. add direct unit coverage
3. add the CLI route and compact-output support
4. run focused verification
5. build one real artifact from the existing accepted inventory and inspect it

Do not add sidecar generation, review semantics, or registry mutation in this
subproject.

## Slice 1: Builder Module

Add:

- `pipeline/accepted_clip_intake_manifest.py`

Implement:

- `ACCEPTED_CLIP_INTAKE_MANIFEST_SCHEMA_VERSION = "accepted_clip_intake_manifest_v1"`
- `build_accepted_clip_intake_manifest(...)`
- validation for:
  - readable manifest file
  - correct inventory schema version
  - required top-level inventory fields
  - row presence for success path
- deterministic `intake_manifest_id`
- default output path logic
- JSON + CSV writing

### Builder Inputs

- `accepted_inventory_manifest`
- optional `output_root`
- optional `output_path`

### Builder Output

Top level:

- `ok`
- `status`
- `schema_version`
- `intake_manifest_id`
- `created_at`
- `source_inventory_manifest_path`
- `source_inventory_id`
- `game`
- `row_count`
- `ingestion_ready_count`
- `rows`
- `manifest_path`
- `csv_path`

Rows copied from inventory with canonical-only fields:

- `clip_id`
- `game`
- `canonical_clip_path`
- `meta_path`
- `quality_tag`
- `downloaded_at`
- `duration_seconds`
- `has_audio`
- `resolution_width`
- `resolution_height`
- `fps`
- `has_meta`
- `ingestion_ready`

### Builder Rules

- preserve row order from the source inventory
- include all inventory rows in v1
- do not copy `variant_paths`, `variant_filenames`, `variant_count`,
  `preferred_source_reason`, or `naming_pattern`
- allow `has_meta == false`

## Slice 2: Unit Tests

Add:

- `tests/test_accepted_clip_intake_manifest.py`

Cover:

- valid inventory manifest builds intake manifest successfully
- all canonical rows are copied
- duplicate/variant fields are not present in intake rows
- invalid inventory schema returns structured failure
- empty inventory rows return explicit `no_rows` status
- output paths are created and JSON/CSV are written

Use temporary files and a synthetic inventory payload rather than depending on
the real `outputs/` artifact.

## Slice 3: CLI Route

Update:

- `run.py`
- `tests/test_run.py`

Add:

- import for `build_accepted_clip_intake_manifest`
- route helper:
  - `run_build_accepted_clip_intake_manifest(...)`
- parser flag:
  - `--build-accepted-clip-intake-manifest`
- parser input:
  - `--accepted-inventory-manifest`
- compact output branch in `_compact_cli_payload(...)`
- dispatch branch in `main()`

### Compact Output Contract

- `ok`
- `status`
- `schema_version`
- `intake_manifest_id`
- `row_count`
- `ingestion_ready_count`
- `manifest_path`
- `csv_path`
- `source_inventory_id`

### CLI Tests

Add focused route coverage for:

- compact output by default
- full-json passthrough
- expected argument forwarding

## Slice 4: Focused Verification

Run:

```bash
python3 -m py_compile \
  pipeline/accepted_clip_intake_manifest.py \
  tests/test_accepted_clip_intake_manifest.py \
  run.py \
  tests/test_run.py
```

Run:

```bash
python3 -m unittest \
  tests.test_accepted_clip_intake_manifest \
  tests.test_run.RunTests.test_cli_routes_to_build_accepted_clip_intake_manifest_compacts_output_by_default \
  tests.test_run.RunTests.test_cli_routes_to_build_accepted_clip_intake_manifest_with_full_json
```

## Slice 5: Real Artifact Build

Use the existing accepted inventory:

- [accepted-clip-inventory-0f2774939cb0.manifest.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_inventory/marvel_rivals/accepted-clip-inventory-0f2774939cb0.manifest.json)

Run:

```bash
python3 run.py \
  --build-accepted-clip-intake-manifest \
  --accepted-inventory-manifest \
  /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_inventory/marvel_rivals/accepted-clip-inventory-0f2774939cb0.manifest.json \
  --full-json
```

Inspect:

- `row_count == 18`
- `ingestion_ready_count`
- rows contain only canonical clip fields
- no duplicate variant fields remain

## Commit Strategy

Prefer one coherent implementation commit once all slices pass:

- builder module
- builder tests
- CLI route
- CLI tests

If the real artifact build writes only operational output under `outputs/`, do
not include those generated artifacts in the implementation commit unless the
user explicitly asks for that.

## Risks

### Risk: Inventory schema drift

Mitigation:

- validate `accepted_clip_inventory_v1` explicitly
- fail structurally if required fields are absent

### Risk: Accidental reintroduction of duplicate semantics

Mitigation:

- explicitly whitelist copied row fields
- test that variant fields are absent in intake rows

### Risk: CLI surface sprawl

Mitigation:

- reuse the existing compact-output pattern already used by the approval-target
  commands
- keep the route narrow and artifact-first

## Out of Scope Follow-Up

After this plan lands, the next subproject can consume the intake manifest to:

- create source manifests or pipeline-ready clip inputs
- run sidecar generation
- capture review outcomes
- refresh the registry

Those steps should be designed separately rather than folded into this change.

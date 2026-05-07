# Accepted Clip Intake Manifest Design

## Summary

Add a dedicated accepted-clip intake manifest builder that converts one
`accepted_clip_inventory_v1` artifact into one frozen
`accepted_clip_intake_manifest_v1` artifact under `outputs/`.

This manifest is the next operational boundary after accepted-clip inventory:

- inventory normalizes files and duplicate groups
- intake manifest freezes one canonical ingestion set
- later review and registry work consume that frozen set

The first version is intentionally narrow. It does not generate sidecars, does
not capture review outcomes, and does not refresh the registry. It only
materializes a durable intake artifact for the accepted `marvel_rivals` clips.

## Problem

The repo now has a canonical accepted-clip inventory for
`/Users/tj/GPT-Codex-Repo/accepted/marvel_rivals`, but it does not yet have a
stable artifact that says:

- which exact canonical clips are in scope for ingestion
- which inventory build they came from
- what minimum media facts and metadata will be carried forward

Relying directly on the inventory manifest is the wrong boundary because:

- inventory rebuilds could change row ordering or future normalization rules
- later review or registry workflows need a frozen intake set, not a live
  inventory dependency
- accepted-clip intake is not the same contract as fixture source manifests or
  training exports

## Goals

- Freeze one canonical accepted-clip ingestion set into a standalone manifest.
- Use the existing accepted inventory as the source of truth for row selection.
- Preserve only the canonical clip path per row, not duplicate variant paths.
- Include all canonical `marvel_rivals` rows from the selected inventory in v1.
- Write both JSON and CSV artifacts under `outputs/accepted_clip_intake/`.
- Keep the artifact narrow enough that later ingestion/review steps can build on
  it without reinterpreting accepted inventory semantics.

## Non-Goals

- No sidecar generation.
- No proxy/runtime/fused analysis.
- No review bridge preparation or review state.
- No registry refresh.
- No attempt to merge accepted intake into `fixture_source_manifest_v1`.
- No duplicate variant preservation in the intake rows.

## Recommended Approach

### Option 1: Standalone accepted-intake manifest

Create a dedicated builder and artifact schema:

- module: `pipeline/accepted_clip_intake_manifest.py`
- schema: `accepted_clip_intake_manifest_v1`
- output root: `outputs/accepted_clip_intake/<game>/`

This keeps accepted intake as its own operational artifact while still using the
accepted inventory as the upstream selection surface.

### Option 2: Reuse fixture source manifest directly

Emit a `fixture_source_manifest_v1`-shaped artifact from the accepted
inventory.

Rejected because it collapses two different workflow contracts:

- accepted clip intake for future review/registry work
- fixture source manifests for fixture/evaluation workflows

### Option 3: Pointer manifest back to accepted inventory

Emit a tiny artifact containing only inventory id and selected clip ids.

Rejected because it is too brittle. Later inventory rebuilds would remain part
of the effective runtime dependency for ingestion.

## Selected Design

Implement Option 1.

The builder will load one `accepted_clip_inventory_v1` manifest, validate it,
and emit a standalone `accepted_clip_intake_manifest_v1` artifact containing
one row per canonical accepted clip.

For v1:

- include all canonical rows from the inventory
- include only `canonical_clip_path`
- do not carry duplicate `variant_paths`
- do not exclude rows only because `has_meta` is false

## Artifact Contract

### Top-Level Fields

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

### Row Fields

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

## Selection Rules

Given one accepted inventory manifest:

1. Validate `schema_version == accepted_clip_inventory_v1`.
2. Validate that `rows` is a non-empty list for the normal success path.
3. For each inventory row:
   - copy the canonical fields only
   - do not copy `variant_paths` or `variant_filenames`
4. Preserve row order from the source inventory.
5. Include all rows in the intake manifest, even if `has_meta == false`.
6. Count `ingestion_ready_count` from the copied `ingestion_ready` flag.

## Output Paths

Default JSON path:

- `outputs/accepted_clip_intake/<game>/accepted-clip-intake-<id>.manifest.json`

Default CSV path:

- same basename with `.csv`

The id should be deterministic from:

- source inventory manifest path
- source inventory id
- game
- ordered clip ids

## CLI Surface

Add one CLI command in `run.py`:

- `--build-accepted-clip-intake-manifest`

Inputs:

- `--accepted-inventory-manifest <PATH>`
- optional `--output-root <PATH>`
- optional `--output-path <PATH>`

Output:

- compact JSON summary by default
- full payload with `--full-json`

Compact summary fields:

- `ok`
- `status`
- `schema_version`
- `intake_manifest_id`
- `row_count`
- `ingestion_ready_count`
- `manifest_path`
- `csv_path`
- `source_inventory_id`

## Error Handling

Return structured failures for:

- missing inventory manifest path
- unreadable manifest
- invalid schema version
- malformed manifest payload
- missing required top-level fields

Suggested statuses:

- `invalid_inventory_manifest`
- `invalid_inventory_schema`
- `no_rows`
- `ok`

No exceptions should leak through the CLI route for expected validation
failures.

## Testing

Add focused tests for:

- valid inventory -> intake manifest success
- copied rows exclude variant fields
- all canonical rows included
- invalid schema rejected
- empty rows rejected or reported with explicit `no_rows` status
- CLI compact output
- CLI full-json passthrough

## Verification

After implementation:

- run targeted unit tests for the new builder and CLI route
- materialize one real intake manifest from:
  - `outputs/accepted_clip_inventory/marvel_rivals/accepted-clip-inventory-0f2774939cb0.manifest.json`
- inspect:
  - row count
  - ingestion-ready count
  - canonical clip paths only
  - no duplicate variant fields in rows

## Expected Outcome

This change creates the next durable workflow boundary:

- accepted inventory answers “what files normalize together?”
- accepted intake manifest answers “what exact canonical clips are in scope for
  ingestion?”

That gives the repo a frozen operational artifact for the next step: converting
accepted clips into reviewable pipeline inputs without depending on a mutable
inventory artifact at runtime.

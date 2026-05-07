# Accepted Clip Inventory Design

## Summary

This spec defines a canonical-primary operational inventory for accepted clips, starting with `marvel_rivals` clips under:

- `/Users/tj/GPT-Codex-Repo/accepted/marvel_rivals`

The inventory is an operational normalization artifact, not a training dataset and not an ingestion run. Its purpose is to convert a messy accepted-media directory into a stable source-of-truth inventory with one canonical row per clip id and explicit attachment of alternate filename variants.

## Goals

- Build one operational inventory artifact under `outputs/`.
- Normalize accepted clips by canonical clip id.
- Preserve every file variant while choosing one canonical primary.
- Link any matching `.meta.json` file when present.
- Expose whether each canonical row is structurally ready for later ingestion.

## Non-Goals

- Do not ingest clips into the pipeline yet.
- Do not create fused sidecars, lifecycle rows, or approval labels.
- Do not treat accepted clips as direct training data.
- Do not mutate or rename the source accepted directory.

## Why This Exists

The accepted `marvel_rivals` folder is useful as an upstream source for expanding the approval-target dataset, but it is not yet shaped for reliable ingestion:

- there are repeated filename variants for the same clip id
- only a minority of clips have matching `.meta.json` files
- the metadata is media-oriented, not approval-oriented

Before any review, sidecar generation, or registry refresh work, the repo needs a canonical operational inventory that answers:

- what unique clips exist?
- which file path is the canonical primary for each clip?
- what other variants point at the same clip id?
- which clips have linked metadata?

## Artifact Contract

Add a new operational artifact schema:

- `accepted_clip_inventory_v1`

Store it under `outputs/`, for example:

- `outputs/accepted_clip_inventory/marvel_rivals/...`

Top-level fields:

- `ok`
- `status`
- `schema_version`
- `inventory_id`
- `created_at`
- `source_root`
- `game`
- `row_count`
- `canonical_clip_count`
- `meta_linked_count`
- `duplicate_group_count`
- `rows`
- `manifest_path`
- `csv_path`

Statuses:

- `ok`
- `invalid_source_root`
- `no_clips_found`

## Row Contract

Each row represents one canonical clip id group.

Required fields:

- `clip_id`
- `game`
- `canonical_clip_path`
- `meta_path`
- `quality_tag`
- `downloaded_at`
- `duration_seconds`
- `has_audio`
- `variant_paths`
- `variant_count`
- `has_meta`
- `naming_pattern`
- `ingestion_ready`

Suggested additional audit fields:

- `canonical_filename`
- `variant_filenames`
- `preferred_source_reason`
- `resolution_width`
- `resolution_height`
- `fps`

## Canonicalization Rules

The inventory is canonical-primary, not destructive deduplication.

Clip id resolution:

- derive `clip_id` from the trailing numeric suffix when possible
- if the filename contains a stable human-readable prefix plus `_NNNNNNNNNN`, keep the full semantic clip id text before the suffix
- if a file cannot be resolved to a clip id, mark it non-ingestion-ready and keep it as a singleton row

Primary selection order:

1. unprefixed filename with matching `.meta.json`
2. unprefixed filename without metadata
3. shortest stable filename variant
4. lexical fallback

Variant handling:

- keep all other matching files in `variant_paths`
- do not create separate primary rows for the same clip id

## Metadata Linkage Rules

Metadata linkage is by canonical clip id / base filename match.

When metadata exists:

- populate `meta_path`
- copy through stable media facts such as:
  - `quality_tag`
  - `downloaded_at`
  - `duration_seconds`
  - `has_audio`
  - `resolution_width`
  - `resolution_height`
  - `fps`

When metadata does not exist:

- `meta_path: null`
- `has_meta: false`

The inventory should not attempt to infer approval semantics from the metadata.

## Ingestion Ready

`ingestion_ready` is a structural flag only.

Set `ingestion_ready: true` when:

- canonical clip path exists
- clip id was resolved

Set `ingestion_ready: false` when:

- no canonical clip path exists
- clip id could not be resolved

Metadata presence is helpful but not required for `ingestion_ready`.

## Output Format

Write:

- JSON manifest
- CSV summary

CSV should flatten the main row fields and serialize `variant_paths` as JSON text or a delimiter-joined string.

## Module Layout

Add:

- `pipeline/accepted_clip_inventory.py`

Primary function:

- `build_accepted_clip_inventory(...)`

Suggested helpers:

- `_resolve_clip_id(...)`
- `_group_variants(...)`
- `_choose_canonical_primary(...)`
- `_link_meta(...)`
- `_inventory_row(...)`
- `_inventory_id(...)`
- `_default_output_path(...)`
- `_write_csv(...)`

## CLI Surface

Add a new command to `run.py`:

- `--build-accepted-clip-inventory`

Required inputs:

- `--source-root`
- `--game`

Optional inputs:

- `--output-root`
- `--output-path`

Compact output should show:

- `status`
- `row_count`
- `canonical_clip_count`
- `meta_linked_count`
- `duplicate_group_count`
- `manifest_path`

`--full-json` returns the full manifest.

## Testing

Add focused tests for:

- canonical grouping of prefixed and unprefixed variants
- primary selection prefers unprefixed file with metadata
- metadata linkage copies media facts correctly
- rows without metadata still appear
- no-clips-found status
- CLI route and compact output behavior

## Expected Outcome

After implementation, the repo will have a stable operational inventory for accepted `marvel_rivals` clips.

That inventory will be the correct next source artifact for any later ingestion, review, sidecar generation, or approval-target expansion work.

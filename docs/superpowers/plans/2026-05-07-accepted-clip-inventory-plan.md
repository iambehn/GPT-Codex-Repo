# Accepted Clip Inventory Plan

## Goal

Implement a canonical-primary operational inventory for accepted clips under `outputs/`, starting with the `marvel_rivals` accepted folder at:

- `/Users/tj/GPT-Codex-Repo/accepted/marvel_rivals`

The inventory should normalize variant filenames by clip id, choose one canonical primary, link matching metadata, and expose a structurally ingestion-ready source artifact for later review or pipeline work.

## Scope

In scope:

- new inventory module
- JSON manifest writing
- CSV summary writing
- canonical grouping by clip id
- metadata linkage
- CLI command
- compact CLI output
- focused module and CLI tests

Out of scope:

- ingesting clips into the pipeline
- generating fused sidecars
- generating review outcomes
- registry refresh
- approval-target dataset construction from these clips

## Files

Add:

- `pipeline/accepted_clip_inventory.py`
- `tests/test_accepted_clip_inventory.py`

Update:

- `run.py`
- `tests/test_run.py`

## Artifact Contract

Implement `accepted_clip_inventory_v1` with:

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

Each row is one canonical clip group.

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

Additional audit fields:

- `canonical_filename`
- `variant_filenames`
- `preferred_source_reason`
- `resolution_width`
- `resolution_height`
- `fps`

## Canonicalization Rules

Implement the approved priority order:

1. unprefixed filename with matching metadata
2. unprefixed filename without metadata
3. shortest stable filename variant
4. lexical fallback

Grouping:

- derive `clip_id` from the trailing numeric suffix when possible
- group all variants sharing the same resolved clip id
- keep all other files in `variant_paths`
- do not drop or rename source files

If a clip id cannot be resolved:

- keep the file as a singleton row
- mark `ingestion_ready: false`

## Metadata Linkage

Link metadata by base clip id / filename match.

When metadata exists:

- set `meta_path`
- copy through:
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

Do not interpret metadata as approval supervision.

## Naming Pattern

Add a simple naming-pattern classifier for operator readability, for example:

- `unprefixed`
- `dated_prefixed`
- `mixed_variants`
- `unresolved`

The exact classifier can stay small, but it should let operators quickly see whether a row is a clean original or a mixed duplicate group.

## Ingestion Ready

`ingestion_ready` is structural only.

Set `true` when:

- canonical clip path exists
- clip id resolved

Set `false` when:

- clip id unresolved
- canonical file missing

Metadata is not required for `ingestion_ready`.

## Module Design

Create `pipeline/accepted_clip_inventory.py` with:

- `ACCEPTED_CLIP_INVENTORY_SCHEMA_VERSION = "accepted_clip_inventory_v1"`
- `DEFAULT_OUTPUT_ROOT`
- `build_accepted_clip_inventory(...)`

Suggested helpers:

- `_resolve_source_root(...)`
- `_inventory_rows(...)`
- `_resolve_clip_id(...)`
- `_group_variants(...)`
- `_choose_canonical_primary(...)`
- `_link_meta(...)`
- `_naming_pattern(...)`
- `_inventory_row(...)`
- `_inventory_id(...)`
- `_default_output_path(...)`
- `_write_csv(...)`

## CLI Surface

Add:

- `--build-accepted-clip-inventory`

Required:

- `--source-root`
- `--game`

Optional:

- `--output-root`
- `--output-path`

Route through:

- `run_build_accepted_clip_inventory(...)`

Compact output should include:

- `status`
- `row_count`
- `canonical_clip_count`
- `meta_linked_count`
- `duplicate_group_count`
- `manifest_path`

`--full-json` returns the full inventory payload.

## Implementation Steps

### 1. Add the inventory module

Create `pipeline/accepted_clip_inventory.py` and implement:

- source-root validation
- clip discovery
- clip id resolution
- duplicate grouping
- canonical primary selection
- metadata linkage
- JSON/CSV artifact writing

### 2. Add module tests

Create `tests/test_accepted_clip_inventory.py` covering:

- canonical grouping of prefixed + unprefixed variants
- primary selection prefers unprefixed file with metadata
- metadata linkage copies media facts correctly
- rows without metadata remain present
- unresolved clip id yields `ingestion_ready: false`
- empty source root yields `no_clips_found`

Use temporary directories with small fake files rather than touching the real accepted folder in tests.

### 3. Add CLI route

Update `run.py` to:

- import the module
- add `--build-accepted-clip-inventory`
- add `run_build_accepted_clip_inventory(...)`
- route through `_print_cli_result(...)`

### 4. Add compact CLI rendering

Add a compact renderer keyed by `command_name="build_accepted_clip_inventory"`.

### 5. Add CLI tests

Update `tests/test_run.py` for:

- compact default output
- `--full-json` passthrough
- required-argument behavior

### 6. Run one real inventory build

After tests pass, run the new command on:

- `/Users/tj/GPT-Codex-Repo/accepted/marvel_rivals`

Verify:

- row count
- duplicate group count
- metadata link count
- representative canonical-primary choices

## Verification

Run:

```bash
python3 -m py_compile \
  pipeline/accepted_clip_inventory.py \
  tests/test_accepted_clip_inventory.py \
  run.py \
  tests/test_run.py
```

Then:

```bash
python3 -m unittest \
  tests.test_accepted_clip_inventory \
  tests.test_run.RunTests.test_cli_routes_to_build_accepted_clip_inventory_compacts_output_by_default \
  tests.test_run.RunTests.test_cli_routes_to_build_accepted_clip_inventory_with_full_json
```

Then the real run:

```bash
python3 run.py \
  --build-accepted-clip-inventory \
  --source-root /Users/tj/GPT-Codex-Repo/accepted/marvel_rivals \
  --game marvel_rivals \
  --output-root outputs/accepted_clip_inventory \
  --full-json
```

## Sequencing

Implement in this order:

1. inventory module
2. module tests
3. CLI route
4. compact output
5. CLI tests
6. real inventory build

This keeps the operational artifact contract stable before touching the real source folder.

## Expected Outcome

After implementation, the repo will have a canonical operational inventory for accepted `marvel_rivals` clips under `outputs/`.

That inventory becomes the next correct source artifact for any later ingestion, review, sidecar generation, or approval-target expansion work.

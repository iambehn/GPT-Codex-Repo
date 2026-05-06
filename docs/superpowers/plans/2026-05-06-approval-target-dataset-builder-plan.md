# Approval Target Dataset Builder Plan

## Goal

Implement a dedicated registry-backed dataset builder for `candidate_approval_probability` that:

- emits a separate `approval_target_dataset_v1` artifact
- uses only clip registry rows as the source of truth
- applies strict approval-label semantics
- reports whether the resulting dataset is training-ready
- exposes a compact CLI command in `run.py`

## Scope

In scope:

- new pipeline module
- new CLI command
- artifact writing
- compact CLI output
- focused unit and CLI tests

Out of scope:

- changes to generic `v2_training_export.py`
- automatic shadow training or operator execution from this builder
- registry schema changes
- downstream post-performance or export-selection targets

## Files

Add:

- `pipeline/approval_target_dataset.py`
- `tests/test_approval_target_dataset.py`

Update:

- `run.py`
- `tests/test_run.py`

## Artifact Contract

Implement `approval_target_dataset_v1` with:

- `ok`
- `status`
- `schema_version`
- `dataset_id`
- `created_at`
- `registry_path`
- `filters`
- `row_count`
- `positive_count`
- `negative_count`
- `training_ready`
- `readiness_reason`
- `rows`
- `warnings`
- `manifest_path`
- optional `csv_path`

Statuses:

- `ok`
- `no_rows`
- `invalid_registry`

Readiness reasons:

- `ready`
- `no_positive_labels`
- `no_negative_labels`
- `no_rows`

## Label Rules

Implement the approved design exactly:

1. `review_outcome`
- `approved` => positive
- `rejected` => negative

2. lifecycle fallback only when review outcome is absent
- positive:
  - `approved`
  - `selected_for_export`
- negative:
  - `rejected`
  - `invalidated`
  - `superseded`

3. exclude ambiguous rows
- `exported` or `posted` without explicit review outcome do not create labels
- rows without a clean label are omitted from the dataset

## Implementation Steps

### 1. Add the builder module

Create `pipeline/approval_target_dataset.py` with:

- `APPROVAL_TARGET_DATASET_SCHEMA_VERSION = "approval_target_dataset_v1"`
- `DEFAULT_OUTPUT_ROOT`
- `build_approval_target_dataset(...)`

Suggested helpers:

- `_resolve_registry_rows(...)`
- `_approval_label_from_registry_row(...)`
- `_approval_dataset_row(...)`
- `_approval_dataset_id(...)`
- `_approval_dataset_summary(...)`
- `_default_output_path(...)`
- `_write_csv(...)`

### 2. Query source rows from the registry

Use:

- `query_clip_registry(mode="candidate-lifecycles", ...)`

Pass through supported filters:

- `registry_path`
- `game`
- optional `platform`
- optional `evidence_mode`

Do not read source sidecars directly.

### 3. Build labeled rows

For each candidate lifecycle row:

- derive `approval_label`
- derive `label_source`
- emit the compact row contract
- drop ambiguous rows

Preserve enough audit context to explain why the row is present:

- lifecycle state
- review outcome
- selected highlight fields if present
- evidence mode

### 4. Summarize dataset readiness

After row construction:

- count positives
- count negatives
- set `training_ready`
- set `readiness_reason`

Return:

- `status: no_rows` when zero labeled rows exist
- `status: ok` otherwise, even if `training_ready` is false

### 5. Add CLI entrypoint

In `run.py`:

- add `--build-approval-target-dataset`
- require:
  - `--registry-path`
  - `--game`
- allow:
  - `--platform`
  - `--evidence-mode`
  - `--output-root`
  - `--output-path`

Route to:

- `run_build_approval_target_dataset(...)`

### 6. Add compact CLI rendering

Default compact output should show:

- `status`
- `row_count`
- `positive_count`
- `negative_count`
- `training_ready`
- `readiness_reason`
- `manifest_path`

Keep `--full-json` support for the full artifact payload.

## Tests

### Module tests

Add `tests/test_approval_target_dataset.py` covering:

- approved review => positive
- rejected review => negative
- lifecycle fallback with missing review outcome
- posted/exported without explicit review outcome => excluded
- both-class dataset => `training_ready: true`
- one-class dataset => `training_ready: false`
- zero labeled rows => `status: no_rows`

Prefer small temporary SQLite-backed fixtures built through existing registry helpers where practical.

### CLI tests

Add `tests/test_run.py` coverage for:

- CLI route to `--build-approval-target-dataset`
- compact output by default
- full-json passthrough
- required-argument errors

## Verification

Run:

```bash
python3 -m py_compile \
  pipeline/approval_target_dataset.py \
  tests/test_approval_target_dataset.py \
  run.py \
  tests/test_run.py
```

Then:

```bash
python3 -m unittest \
  tests.test_approval_target_dataset \
  tests.test_run.RunTests.test_cli_routes_to_build_approval_target_dataset \
  tests.test_run.RunTests.test_cli_routes_to_build_approval_target_dataset_with_full_json
```

If the builder reuses registry helpers in a way that risks collateral breakage, also rerun:

```bash
python3 -m unittest tests.test_shadow_model_training tests.test_shadow_operator_workflow
```

## Sequencing

Implement in this order:

1. builder module
2. module tests
3. CLI route
4. compact output
5. CLI tests

This keeps the artifact contract stable before wiring the operator-facing entrypoint.

## Expected Outcome

After implementation, the repo will have a target-correct dataset artifact for `candidate_approval_probability`.

That should let shadow training answer the approval-ranking question on a dataset built from approval-stage evidence, instead of inferring labels from post/export-heavy slices.

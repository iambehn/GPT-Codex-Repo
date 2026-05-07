# Approval Target Dataset Adapter Plan

## Goal

Implement a narrow adapter from `approval_target_dataset_v1` into a minimal `v2_training_dataset_export_v1`-compatible artifact so the existing shadow training, replay, benchmark, and operator stack can consume approval-target datasets without internal contract changes.

## Scope

In scope:

- new adapter module
- artifact writing for adapted V2-compatible manifests
- new CLI command
- compact CLI output
- focused module and CLI tests
- narrow compatibility verification against the current shadow training path

Out of scope:

- native `approval_target_dataset_v1` support in shadow training or replay
- changes to `pipeline/shadow_model_training.py`
- changes to `pipeline/shadow_ranking_replay.py`
- changes to shadow benchmark/operator semantics
- changes to approval-target label construction rules

## Files

Add:

- `pipeline/approval_target_dataset_adapter.py`

Update:

- `run.py`
- `tests/test_run.py`

Add tests:

- `tests/test_approval_target_dataset_adapter.py`

## Source Contract

Input:

- one `approval_target_dataset_v1` manifest

Required source fields:

- `schema_version`
- `dataset_id`
- `filters`
- `rows`

Each source row must provide:

- `candidate_id`
- `game`
- `source`
- `fixture_id`
- `event_id`
- `review_outcome`
- `lifecycle_state`
- `approval_label`
- `label_source`
- `final_score`
- `fused_confidence`
- `hook_strength`
- `hook_mode`
- `hook_archetype`
- `selected_highlight_event_type`
- `selected_highlight_fusion_id`
- `evidence_mode`
- `fused_sidecar_path`
- `highlight_selection_manifest_path`

## Output Contract

Emit one `v2_training_dataset_export_v1`-compatible artifact with:

- `candidates` rows only
- empty `hooks`, `outcomes`, and `performance` views
- consistent manifest counts and row counts

Required top-level manifest fields:

- `schema_version: "v2_training_dataset_export_v1"`
- `dataset_export_id`
- `generated_at`
- `filters`
- `dataset_views`
- `row_count`
- `coverage_counts`
- `warning_count`
- `warnings`

Add explicit provenance:

- `source_approval_target_manifest_path`
- `source_approval_target_dataset_id`
- `source_approval_target_schema_version`

## Candidate Row Mapping

Each approval-target row becomes one adapted candidate row.

Carry through directly:

- `candidate_id`
- `game`
- `source`
- `fixture_id`
- `event_id`
- `fused_sidecar_path`
- `highlight_selection_manifest_path`
- `lifecycle_state`
- `review_outcome`
- `final_score`
- `selected_highlight_event_type`
- `selected_highlight_fusion_id`
- `evidence_mode`

Bridge as metadata for auditability:

- `approval_label`
- `label_source`
- `fused_confidence`
- `hook_strength`
- `hook_mode`
- `hook_archetype`

Emit compatibility defaults:

- `hook_candidate_present: false`
- `export_present: false`
- `post_present: false`
- `metrics_present: false`
- `coverage_tier: "reviewed"`
- `latest_post_performance_coverage_tier: "no_post_record"`
- `latest_post_performance_label_eligible: false`
- `latest_post_performance_recoverable: false`
- `latest_post_performance_target_score: null`
- `latest_post_performance_target_bucket: null`
- `latest_post_performance_label_reason: "no_post_record"`
- `preferred_hook_mode_natural: 0.0`

The adapter does not emit `label_positive` or `label_score`; existing shadow code derives those from `review_outcome` and `lifecycle_state`.

## Module Design

Create `pipeline/approval_target_dataset_adapter.py` with:

- `APPROVAL_TARGET_DATASET_SCHEMA_VERSION = "approval_target_dataset_v1"`
- `V2_TRAINING_EXPORT_SCHEMA_VERSION = "v2_training_dataset_export_v1"`
- `DEFAULT_OUTPUT_ROOT`
- `adapt_approval_target_dataset(...)`

Suggested helpers:

- `_resolve_manifest_path(...)`
- `_load_approval_target_manifest(...)`
- `_validate_approval_target_manifest(...)`
- `_adapt_candidate_row(...)`
- `_adapted_dataset_id(...)`
- `_build_manifest(...)`
- `_dataset_paths(...)`
- `_write_dataset_artifacts(...)`
- `_write_jsonl_view(...)`
- `_write_csv_view(...)`

## CLI Surface

Add:

- `--adapt-approval-target-dataset`

Required input:

- one approval-target manifest path

Optional:

- `--output-root`
- `--output-path`

Route through:

- `run_adapt_approval_target_dataset(...)`

Compact output should include:

- `status`
- `dataset_export_id`
- `row_count`
- `manifest_path`
- `source_approval_target_manifest_path`

`--full-json` returns the full adapter result.

## Failure Modes

Return explicit statuses for:

- `invalid_approval_target_manifest`
- `unsupported_approval_target_manifest`
- `ok`

Invalid/malformed source artifacts must fail before writing output.

## Implementation Steps

### 1. Add the adapter module

Create `pipeline/approval_target_dataset_adapter.py` and implement:

- source manifest loading
- source validation
- row adaptation
- minimal V2 artifact writing

### 2. Add module tests

Create `tests/test_approval_target_dataset_adapter.py` covering:

- valid approval-target manifest adapts successfully
- positive and negative approval rows are preserved
- empty `hooks`, `outcomes`, and `performance` views are written
- malformed source manifest is rejected
- provenance fields are present on the adapted manifest

Prefer fixture generation from temporary approval-target manifests rather than relying on large repo artifacts.

### 3. Add the CLI route

Update `run.py` to:

- import the adapter
- add `--adapt-approval-target-dataset`
- add `run_adapt_approval_target_dataset(...)`
- route through `_print_cli_result(...)`

### 4. Add compact CLI rendering

Add a compact renderer keyed by `command_name="adapt_approval_target_dataset"`.

### 5. Add CLI tests

Update `tests/test_run.py` for:

- compact default output
- `--full-json` passthrough
- required-argument behavior

### 6. Run narrow compatibility verification

After the adapter works, run one focused check:

- adapt the known good approval-target dataset
- feed the adapted manifest into `train_shadow_ranking_model(...)` for `approved_or_selected_probability`

The goal is to prove compatibility without widening the scope into operator reruns yet.

## Verification

Run:

```bash
python3 -m py_compile \
  pipeline/approval_target_dataset_adapter.py \
  tests/test_approval_target_dataset_adapter.py \
  run.py \
  tests/test_run.py
```

Then:

```bash
python3 -m unittest \
  tests.test_approval_target_dataset_adapter \
  tests.test_run.RunTests.test_cli_routes_to_adapt_approval_target_dataset_compacts_output_by_default \
  tests.test_run.RunTests.test_cli_routes_to_adapt_approval_target_dataset_with_full_json
```

Then the narrow compatibility check:

```bash
python3 -m unittest \
  tests.test_shadow_model_training
```

If needed, add one direct compatibility test that trains on an adapted manifest rather than rerunning broader operator flows immediately.

## Sequencing

Implement in this order:

1. adapter module
2. module tests
3. CLI route
4. compact output
5. CLI tests
6. narrow shadow-training compatibility verification

This keeps the compatibility contract stable before widening to full operator use.

## Expected Outcome

After implementation:

1. build `approval_target_dataset_v1`
2. adapt it into `v2_training_dataset_export_v1`
3. run the current shadow stack unchanged on that adapted artifact

That gives the repo a low-risk path from approval-target slicing to real model evaluation.

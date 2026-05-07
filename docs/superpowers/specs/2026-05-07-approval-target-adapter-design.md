# Approval Target Dataset Adapter Design

## Summary

This spec defines a narrow adapter from `approval_target_dataset_v1` into a minimal `v2_training_dataset_export_v1`-compatible artifact.

The approval-target builder already solves the slice-construction problem for `candidate_approval_probability`. The remaining gap is compatibility with the current shadow training, replay, benchmark, and operator stack, which expects the generic V2 training dataset schema. The adapter bridges that gap without changing shadow training or replay behavior.

## Goals

- Keep the shadow stack unchanged.
- Convert `approval_target_dataset_v1` into a training-shape artifact the existing shadow modules already accept.
- Preserve approval-target label semantics exactly as built by the dedicated dataset builder.
- Keep the adapter explicit and auditable rather than hiding conversion inside the operator flow.

## Non-Goals

- Do not add native `approval_target_dataset_v1` support to shadow training or replay in this subproject.
- Do not change `pipeline/shadow_model_training.py`, `pipeline/shadow_ranking_replay.py`, or operator workflows to consume a new schema directly.
- Do not add hooks, outcomes, or performance semantics that were not present in the approval-target source artifact.
- Do not reinterpret labels during adaptation.

## Why An Adapter

The approval-target dataset is intentionally separate from the generic V2 export because it enforces stricter semantics:

- explicit approval labels
- exclusion of ambiguous posted/exported rows without explicit review outcome
- training-readiness summary based on class balance

That separation is useful. It should not be collapsed immediately by teaching every shadow module about a second dataset schema. A dedicated adapter keeps the semantic boundary while letting the current shadow stack run on a compatible artifact with minimal risk.

## Artifact Contract

Input:

- `approval_target_dataset_v1`

Output:

- `v2_training_dataset_export_v1`

The output is intentionally minimal:

- `candidates` view contains adapted approval-target rows
- `hooks` view is empty
- `outcomes` view is empty
- `performance` view is empty

The output must still satisfy the current manifest contract expected by:

- `train_shadow_ranking_model(...)`
- `evaluate_shadow_ranking_model(...)`
- `run_shadow_ranking_replay(...)`
- `run_shadow_benchmark_matrix(...)`
- `run_shadow_operator_workflow(...)`

## Candidate Row Mapping

Each approval-target row becomes one V2 candidate row.

Required mapped fields:

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

Required shadow-training compatibility fields:

- `hook_candidate_present: false`
- `export_present: false`
- `post_present: false`
- `metrics_present: false`
- `coverage_tier: "reviewed"`
- `latest_post_performance_coverage_tier: "no_post_record"`
- `latest_post_performance_label_eligible: false`
- `latest_post_performance_recoverable: false`
- `preferred_hook_mode_natural: 0.0`

Approval-label bridge fields:

- `approval_label` from the source row is carried through as metadata
- `label_source` from the source row is carried through as metadata

The adapter does not need to emit `label_positive` or `label_score` directly. Existing shadow code computes those from `review_outcome`, `lifecycle_state`, and related fields.

## Manifest Contract

The adapted manifest should:

- use `schema_version: "v2_training_dataset_export_v1"`
- include a distinct `dataset_export_id`
- include `filters` copied from the approval-target artifact
- include a provenance field pointing back to the source approval-target manifest
- set dataset view row counts correctly
- set coverage counts consistently:
  - `candidate_count`
  - `hook_count: 0`
  - `outcome_count: 0`
  - `performance_count: 0`

Add explicit provenance fields:

- `source_approval_target_manifest_path`
- `source_approval_target_dataset_id`
- `source_approval_target_schema_version`

## Module Layout

Add:

- `pipeline/approval_target_dataset_adapter.py`

Primary function:

- `adapt_approval_target_dataset(...)`

Suggested helpers:

- `_load_approval_target_manifest(...)`
- `_adapt_candidate_row(...)`
- `_build_adapted_manifest(...)`
- `_adapted_dataset_id(...)`
- `_default_output_path(...)`
- `_write_jsonl_view(...)`
- `_write_csv_view(...)`

## CLI Surface

Add a new command to `run.py`:

- `--adapt-approval-target-dataset`

Required input:

- approval-target manifest path

Optional inputs:

- `--output-root`
- `--output-path`

Default compact output:

- `status`
- `dataset_export_id`
- `row_count`
- `manifest_path`
- `source_approval_target_manifest_path`

`--full-json` should return the full adapted manifest payload.

## Failure Modes

Return explicit statuses for:

- `invalid_approval_target_manifest`
- `unsupported_approval_target_manifest`
- `ok`

The adapter should not silently proceed on malformed or unsupported source artifacts.

## Testing

Add focused tests for:

- adapting a valid approval-target manifest into a minimal V2-compatible export
- preserving positive and negative approval examples through mapped candidate rows
- emitting empty `hooks`, `outcomes`, and `performance` views
- rejecting malformed source manifests
- CLI route and compact output behavior

Also run a narrow compatibility check:

- feed the adapted manifest into the existing shadow training path for `approved_or_selected_probability`

## Expected Outcome

After this change, the approval-target builder becomes operationally useful for the current shadow stack:

1. build approval-target dataset
2. adapt it into V2 training shape
3. run the existing shadow workflow without changing shadow training internals

This keeps the semantic improvement isolated while minimizing downstream risk.

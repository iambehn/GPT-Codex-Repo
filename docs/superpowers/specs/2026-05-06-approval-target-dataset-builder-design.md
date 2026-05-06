# Approval Target Dataset Builder Design

## Summary

This spec defines a dedicated builder for `candidate_approval_probability` datasets.

The current generic V2 exports are useful for broad multimodal training and downstream analytics, but they mix approval-stage and downstream-post-selection populations. That makes them unreliable as the source artifact for approval-target shadow training. The dedicated builder creates a clean approval-target dataset from clip registry rows only, with explicit class-balance reporting and no dependence on downstream posted/exported rows as the primary label signal.

## Goals

- Build a dataset artifact specifically for `candidate_approval_probability`.
- Source only from clip registry rows.
- Prefer explicit review outcome over downstream lifecycle artifacts.
- Preserve a clean positive/negative class boundary.
- Report whether the built dataset is training-ready.
- Keep this builder separate from generic `v2_training_export.py`.

## Non-Goals

- Do not replace or modify the generic V2 training export as the canonical broad export.
- Do not include post-performance or export-selection targets in this artifact.
- Do not read fused sidecars, selection manifests, or posted artifacts directly; registry rows are the source of truth.
- Do not auto-train or auto-run shadow operator flows from this builder.

## Why A Dedicated Builder

The current approval-target experiments showed two different failure modes:

- some slices included enough clear approved/rejected candidates to produce a plausible `prefer_shadow` signal
- other slices were dominated by downstream exported/posted rows, which collapsed the approval target or made it too close to justify promotion

This is not only sample variance. It is a mismatch between the target semantics and the source slice. Approval-target training should be built from approval-relevant registry evidence, not inferred indirectly from downstream post-heavy populations.

## Artifact Contract

Add a new artifact schema:

- `approval_target_dataset_v1`

Top-level fields:

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

`training_ready` should mean:

- at least one positive label
- at least one negative label

`readiness_reason` should be explicit:

- `ready`
- `no_positive_labels`
- `no_negative_labels`
- `no_rows`

## Row Contract

Each row should be a registry-derived approval example with fields sufficient for later shadow training and audit.

Required row fields:

- `candidate_id`
- `game`
- `source`
- `fixture_id`
- `event_id`
- `review_outcome`
- `lifecycle_state`
- `approval_label`
- `final_score`
- `fused_confidence`
- `hook_strength`
- `hook_mode`
- `hook_archetype`
- `selected_highlight_event_type`
- `selected_highlight_fusion_id`
- `evidence_mode`
- `label_source`

`approval_label` values:

- `1.0`
- `0.0`

`label_source` values:

- `review_outcome`
- `lifecycle_state`

## Labeling Rules

The builder is target-specific, so label rules must be strict and easy to audit.

Priority order:

1. explicit review outcome
- `approved` => positive
- `rejected` => negative

2. lifecycle fallback only when review outcome is absent
- positive lifecycle states:
  - `approved`
  - `selected_for_export`
- negative lifecycle states:
  - `rejected`
  - `invalidated`
  - `superseded`

3. exclude ambiguous rows
- rows whose label cannot be determined by the above rules are omitted
- rows that are only `exported` or `posted` without explicit review outcome are omitted from this builder

This is the key semantic difference from the broad V2 export: downstream posted/exported presence is not enough to define an approval label here.

## Source Query

Source from existing registry surfaces only.

Primary source:

- `query_clip_registry(mode="candidate-lifecycles", ...)`

Optional filters:

- `game`
- `platform`
- `review_status`
- `evidence_mode`

The builder should reuse the existing registry row shape rather than create a parallel ingestion path.

## CLI Surface

Add a new command to `run.py`:

- `--build-approval-target-dataset`

Required inputs:

- `--registry-path`
- `--game`

Optional inputs:

- `--platform`
- `--evidence-mode`
- `--output-root`
- `--output-path`

Return compact output by default:

- status
- row_count
- positive_count
- negative_count
- training_ready
- manifest_path
- small sample if useful

`--full-json` should return the full artifact.

## Module Layout

Add:

- `pipeline/approval_target_dataset.py`

Primary function:

- `build_approval_target_dataset(...)`

Suggested helpers:

- `_approval_label_from_registry_row(...)`
- `_approval_dataset_row(...)`
- `_approval_dataset_id(...)`
- `_approval_dataset_summary(...)`
- `_default_output_path(...)`

## Failure Modes

Return explicit statuses for:

- `invalid_registry`
- `no_rows`
- `ok`

Important distinction:

- `no_rows` means the builder could not construct any approval-labeled rows
- `ok` with `training_ready: false` means rows exist, but only one class exists

## Verification

Add focused tests for:

- approved review => positive
- rejected review => negative
- lifecycle fallback when review outcome is missing
- posted/exported without explicit review outcome => excluded
- training-ready summary when both classes are present
- not-ready summary when only one class is present
- CLI route and compact output behavior

## Expected Outcome

This builder gives the repo a target-correct input artifact for `candidate_approval_probability`.

That should let us answer the right question:

- does the shadow model help rank approval-stage candidates?

instead of conflating that with:

- does the shadow model help within an already posted/exported slice?

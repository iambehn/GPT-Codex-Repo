# Bounded Editorial Replay Minimum Artifact Contract v0

Date: 2026-06-13
Status: completed
Scope: bounded `call_of_duty` proof-path contract definition only

## Objective

Define the minimum artifact contract required to make bounded editorial replay deterministic for the `call_of_duty` proof path, without widening scope beyond the existing review bridge and export workflow surfaces.

This document is a contract-definition artifact only.

Non-goals:

- implementing replay
- changing review policy
- changing lifecycle policy
- changing export policy
- generalizing beyond the bounded `call_of_duty` proof path

## Executive Result

The minimum replayable contract needs **three durable objects** in repo-local state:

1. a stable editorial object identity
2. a repo-local editorial decision record
3. a repo-local export-ready snapshot record

Without those three objects, the current system can preserve historical outcomes but cannot deterministically reapply them to fresh reruns.

## Design Constraints

This contract must preserve the existing architecture:

- runtime analysis sidecars remain the signal-generation surface
- fused analysis sidecars remain the fused candidate surface
- review bridges remain the review-acquisition surface
- highlight selection manifests remain the selection surface
- workflow runs and export batches remain the export-history surface

This contract must **not** require:

- a new ontology layer
- a new review policy
- a new export policy
- a generalized multi-game identity system

## Minimum Contract

### Contract Object A: Stable Editorial Identity Record

Purpose:

- identify the same editorial object across reruns, even when sidecar paths change

Required because:

- current candidate identity includes `fused_sidecar_path`
- reruns therefore generate new candidate ids for the same underlying event

Minimum fields:

- `editorial_object_id`
- `game`
- `source`
- `event_id`
- `event_type`
- `identity_basis`
- `identity_version`

Minimum identity rule for the bounded proof path:

- identity must not depend on `fused_sidecar_path`
- identity may depend on:
  - `game`
  - `source`
  - canonical event identity for the fused event

Notes:

- this is the smallest contract change that breaks the current sidecar-bound identity failure
- candidate ids may still exist operationally, but replay must anchor to `editorial_object_id`

### Contract Object B: Editorial Decision Record

Purpose:

- preserve runtime-review or fused-review decisions in a repo-local, replayable form

Required because:

- current review sessions depend on external `GPT-Codex-Repo` metadata
- current applied review state only exists as sidecar-local mutation

Minimum fields:

- `decision_record_id`
- `editorial_object_id`
- `review_surface`
  - `runtime`
  - `fused`
- `review_status`
  - `approved`
  - `rejected`
  - `deferred`
- `reviewed_at`
- `review_session_id`
- `decision_reason`
- `source_sidecar_schema_version`
- `source_sidecar_game`
- `source_sidecar_source`

Surface-specific minimum fields:

- for runtime decisions:
  - `runtime_event_types`
  - `bridge_score`
  - `bridge_recommended_action`
- for fused decisions:
  - `event_id`
  - `event_type`
  - `final_score`
  - `recommended_action`
  - `gate_status`
  - `suggested_start_timestamp`
  - `suggested_end_timestamp`

Optional but recommended:

- `gpt_meta_path`
- `gpt_processed_path`
- `gpt_final_path`

Important rule:

- those GPT paths are provenance only
- replay must not depend on them

### Contract Object C: Export-Ready Snapshot Record

Purpose:

- preserve the exact historical moment when an approved editorial object was considered exportable

Required because:

- current export regeneration only consumes present `selected_for_export` lifecycle state
- once the candidate advances to `exported`, the export queue becomes empty

Minimum fields:

- `export_ready_snapshot_id`
- `editorial_object_id`
- `selection_manifest_path`
- `workflow_run_id`
- `selection_basis`
- `approved_review_status`
- `selected_at`
- `start_seconds`
- `end_seconds`
- `final_score`
- `fused_sidecar_path_at_selection`

Optional but recommended:

- `hook_id`
- `hook_mode`
- `hook_archetype`
- `packaging_strategy`

Important rule:

- this object represents an exportable historical state
- it must remain replayable even after the lifecycle later advances to `exported`

## Minimum Replay Rules

### Rule 1: Review replay rule

Given:

- a fresh runtime or fused sidecar
- a stable `editorial_object_id`
- a matching repo-local editorial decision record

The system must be able to:

- locate the fresh editorial object
- reapply the preserved review decision
- restore equivalent reviewed state without reading external GPT metadata

### Rule 2: Export replay rule

Given:

- an `export_ready_snapshot_record`
- a matching `editorial_object_id`
- a preserved selection window

The system must be able to:

- reconstruct one export-ready item set
- generate a workflow-run-equivalent artifact
- generate an export-batch-equivalent artifact

without requiring the current lifecycle row to still equal `selected_for_export`

### Rule 3: Historical-state preservation rule

Lifecycle may continue to advance:

- `approved -> selected_for_export -> exported -> posted`

But replayability of a past export-ready state must not be destroyed by later lifecycle advancement.

## Minimum Attachment Points

The minimum contract can stay inside existing surfaces if attached here:

### Existing surface: review session manifests

Add or preserve enough data to emit:

- `editorial_object_id`
- repo-local decision-record content

### Existing surface: reviewed sidecars

Continue to store applied review state for inspection, but treat sidecar mutation as:

- inspection surface
- not canonical replay surface

### Existing surface: selection manifests

Preserve enough data to emit:

- `editorial_object_id`
- selection window
- export-ready snapshot data

### Existing surface: workflow run or export batch history

Preserve historical export-ready item set as:

- replayable snapshot
- not just one-time queue output

## What Is Minimally Sufficient

For the bounded proof path, the system does **not** need:

- generalized cross-game replay
- a full editorial planner
- a new registry family
- a second review app
- automatic recovery for every historical artifact family

It only needs:

- one stable editorial identity
- one repo-local decision record
- one preserved export-ready snapshot

That is the smallest contract set that closes both observed gaps:

- editorial replay gap
- export regeneration gap

## Contract Checklist

Replay is minimally supportable only if all of these are true:

- the same fused event gets the same `editorial_object_id` across reruns
- a repo-local decision record exists for each preserved review outcome
- that decision record is sufficient without external GPT metadata
- a preserved export-ready snapshot exists for approved selected objects
- export replay can consume that snapshot even if current lifecycle is already `exported`

If any of those are false, replay remains incomplete.

## Recommended Next Step

Use this contract as the boundary for the next design step.

The next useful artifact should be a narrow design for how to materialize these three objects inside current repo surfaces, without adding a parallel architecture.

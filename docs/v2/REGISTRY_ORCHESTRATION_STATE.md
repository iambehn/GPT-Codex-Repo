# Registry / Orchestration / State

This document is the canonical V2 home for persistent state and workflow sequencing.

## Canonical Scope

Use this doc for:

- queryable artifact-state goals
- candidate and review lifecycle states
- registry boundaries vs sidecar boundaries
- orchestration hardening policy

Do not use this doc for:

- detector scoring details
- editorial hook taxonomy
- downstream analytics interpretation

## Current V2 Position

The repo has moved beyond pure file coordination.

Stable policies:

- sidecars remain the detailed evidence artifacts
- a registry should index and query those artifacts rather than replace them
- candidate state should be explicit
- orchestration should automate clear state transitions, not infer them from directory scanning
- retries, failure recovery, and idempotency belong after state is explicit

Minimum lifecycle direction:

- pending review
- approved
- rejected
- exported
- posted

Registry-first expectations:

- query clips, runs, reviews, comparisons, and artifact lineage without bespoke file scans
- preserve provenance back to source sidecars
- avoid hidden or implicit workflow state

## How To Think About The Registry

The registry is not the evidence layer. It is the workflow-state layer.

That distinction is the key to understanding this part of the system:

- sidecars and manifests hold detailed evidence
- the registry answers workflow questions across those artifacts

Examples of workflow questions are:

- what is waiting for review?
- what was approved?
- what became exportable?
- what was exported?
- what was posted?
- which artifacts support that state?

If sidecars answer "what did we see?", the registry answers "where is this candidate in the workflow right now?"

## What The Registry Is Protecting Us From

Without an explicit registry, the repo would have to infer state from file presence and directory scans.

That fails quickly in a system like this because:

- multiple artifacts can exist for the same source clip
- review outcomes and lifecycle state are not the same thing
- exported is not the same as posted
- old workflow runs should not re-own current queue state

So the registry protects against a common class of silent failure:

- the files exist
- the state looks plausible
- but the workflow meaning is wrong

## Current Concrete Example

The bounded `call_of_duty` local-test path is a clean example.

By the time the path reaches local export, several different truths coexist:

- runtime review has already happened
- fused review has already happened
- a highlight-selection manifest exists
- an export batch exists
- no posted ledger exists yet

Those statements should not be guessed from "what files are around."

The registry makes them queryable as workflow state:

- a candidate can be `selected_for_export`
- then `exported`
- but still have `post_ledger_path = null`

That is the important local-only boundary. The export artifact exists, but no external-action record exists yet.

## Why Lifecycle State And Review State Must Stay Separate

This is one of the easiest conceptual mistakes in the project.

Review state answers:

- did someone approve or reject this evidence-backed candidate?

Lifecycle state answers:

- what workflow stage has this candidate reached?

Those are related, but not interchangeable.

For example:

- a fused event may be approved in review
- yet still not be exported if selection, hook packaging, or queue generation never advances it

Or:

- an exported candidate may still not be posted
- because posting is a different state transition with different artifacts

If those concepts get collapsed together, debugging becomes much harder.

## Where Complex Problems Usually Hide Here

### 1. Artifact state and workflow state drift apart

Example:
- a manifest exists
- but the candidate lifecycle never advanced

Typical causes:
- registry refresh logic
- missing linkage fields
- stale workflow-run assumptions

### 2. Review decisions exist, but queue state is wrong

Example:
- review says approved
- export queue is empty

Typical causes:
- selection manifest not generated
- lifecycle transition not propagated
- queue query filtered on the wrong state

### 3. Export happened, but posting assumptions leak backward

Example:
- local export artifact exists
- downstream logic accidentally treats it as externally posted

Typical cause:
- export and posting boundaries were not kept explicit enough

### 4. Historical artifacts get mistaken for current intent

Example:
- an old workflow manifest exists
- but current queue ownership should come from lifecycle state, not that historical file

This is why lifecycle-first queue semantics matter.

## Practical Mental Model

Use this short model:

- sidecars are detailed evidence
- manifests are explicit artifact outputs
- the registry is the queryable join layer across those artifacts
- lifecycle state tells you what should happen next
- workflow runs record batch provenance, not absolute truth by themselves

If that separation stays clear, the orchestration layer remains understandable even as the repo accumulates more artifact types.

## Current Artifact Chain

The current operational chain should be treated as explicit and queryable:

- fused candidate lifecycle
- workflow run manifest
- highlight-selection manifest
- hook candidate manifest
- highlight export batch
- posted highlight ledger
- posted metrics snapshot

The registry is responsible for indexing and joining that chain. The artifacts remain the detailed evidence source of truth.

## Registry-Managed Schema Ownership

Registry-managed artifact schemas should remain centralized through `pipeline/clip_registry.py`.

That means:

- a schema version that the registry ingests, joins, or exposes as a query surface should be represented in the registry contract layer
- introducing a new registry-managed artifact should update the registry code and matching tests rather than relying on ad hoc file scans
- central registry ownership is for cross-workflow, queryable state, not every helper artifact in the repo

Examples of registry-managed families include:

- core sidecars and review sessions
- workflow and export artifacts
- hook and comparison artifacts
- shadow-evaluation artifacts that are queryable across runs
- dashboard-level intake artifacts that are intentionally exposed through registry analytics

## Explicit Local-Only Schema Scopes

Not every schema version belongs in the central registry.

These local-only families are expected to stay outside `pipeline/clip_registry.py` unless their role changes:

- `accepted_clip_`
- `accepted_fixture_`
- `accepted_proxy_review_`
- `approval_target_dataset_`
- `derived_row_review_`
- `evaluation_fixture_manifest_`
- `fixture_source_manifest_`
- `fused_export_`
- `fusion_goldset_clip_`
- `onboarding_identity_review_session_`
- `proxy_replay_viewer_`
- `proxy_review_session_`
- `real_artifact_intake_bundle_`
- `real_artifact_intake_coverage_report_`
- `real_artifact_intake_dedup_`
- `real_artifact_intake_refresh_`
- `real_artifact_intake_summary_`
- `real_artifact_intake_validation_`
- `real_artifact_intake_history_comparison_`
- `real_artifact_intake_comparison_target_`
- `real_artifact_intake_dashboard_registry_summary_`
- `real_artifact_intake_dashboard_summary_`
- `replay_viewer_`
- `research_runtime_`
- `runtime_export_`
- `shadow_operator_run_`
- `training_export_`
- `unified_replay_viewer_`
- `v2_training_dataset_export_`

These are intentionally local because they are:

- operator helpers
- onboarding or review prep artifacts
- export-only outputs
- research-runtime artifacts
- maintenance or reporting artifacts that do not define the central lifecycle/query contract

If one of these families becomes a cross-workflow query surface, promote it deliberately into the registry contract instead of relying on drift.

## Query Semantics

Current registry query behavior is intentionally split into two shapes:

- row-oriented modes return concrete artifact or lineage rows
- aggregate modes return summarized analytics payloads

The main examples are:

- `candidate-lifecycles`, `workflow-runs`, `highlight-exports`, `post-ledger-records`, and `posted-metrics` are row-oriented
- `posted-performance-rollups` is aggregate

Operators should treat workflow queues as lifecycle-first views:

- current lifecycle state decides what needs action next
- older workflow manifests provide batch provenance, not queue ownership
- repeated refresh should preserve idempotent state rather than re-derive action from historical runs

## What Belongs Elsewhere

- Review semantics belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md).
- Distribution-side post records belong in [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md).
- Source-enrichment candidate scoring belongs in [SOURCE_ENRICHMENT_IDENTITY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/SOURCE_ENRICHMENT_IDENTITY.md).

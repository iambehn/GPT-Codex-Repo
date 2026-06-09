# Failure Taxonomy v0

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Define the first canonical failure taxonomy for the pipeline redesign control plane.

This artifact states:

- what classes of failure currently exist
- how failures relate to states, transitions, and inspection outcomes
- which failures should be treated as requirement, execution, inspection, control, or lifecycle problems
- how failure reporting can stay actionable without yet expanding into qualification or routing logic

## Scope

This spec defines:

- failure family categories
- failure record shape
- initial actionable failure classes
- mapping guidance from inspection and transition outcomes into failure categories

This spec does not define:

- routing consequences
- qualification updates
- retry policy
- resource scheduling
- planning prioritization

## Guardrails

- A failure is not just “something went wrong.” It is a categorized explanation for why the intended target state was not reached or not accepted.
- Failure taxonomy must attach to canonical states, canonical transitions, and inspection outcomes already defined in:
  - [2026-06-10-state-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-state-catalog-v0.md)
  - [2026-06-10-transition-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-transition-catalog-v0.md)
  - [2026-06-10-inspection-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-inspection-architecture-v0.md)
- v0 should prefer compact, actionable failure classes over exhaustive edge-case enumeration.
- Failure taxonomy should explain failure; it should not silently encode routing or qualification policy.

## Why This Artifact Exists

The current control-plane stack now has:

- target states
- allowed transitions
- inspection rules for successful target-state arrival

The next missing layer is the explicit explanation of why intended target-state arrival did not happen or was not accepted.

Without that layer:

- blocked and rejected outcomes remain too vague
- repeated friction cannot be compared cleanly across transitions
- later qualification and support-asset work will not have a stable failure surface to attach to

## Core Principle

For v0, a failure should answer:

- what target state was intended
- what transition or inspection context was active
- why target-state arrival failed, was rejected, or could not be judged

## Failure Record Shape

Each failure record in v0 should be expressible with:

- `failure_family`
- `failure_code`
- `failure_name`
- `applies_to_layer`
- `typical_target_states`
- `typical_transitions`
- `typical_inspection_outcomes`
- `notes`

## Failure Families

Use these families in v0:

- `requirement`
- `source`
- `execution`
- `inspection`
- `support`
- `control`
- `lifecycle`

### `requirement`

The requested or expected result is too ambiguous, mismatched, or unstable for the intended target state to be reached cleanly.

### `source`

The source artifact or input condition is not usable enough for the intended transition.

### `execution`

The work to create the target-state artifact was attempted but did not successfully produce a valid result.

### `inspection`

The target state may have been attempted, but inspection did not accept it or could not validate it.

### `support`

The transition depends on missing support assets, tooling, export support, or structured assistance that was not available.

### `control`

Progress halted because the system lacked required authorization, external input, or decision closure.

### `lifecycle`

The artifact did not fail as an execution attempt, but it was intentionally retired, superseded, or removed from active progression.

## Initial Failure Classes

| failure_family | failure_code | failure_name | applies_to_layer | typical_target_states | typical_transitions | typical_inspection_outcomes | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `requirement` | `REQ-001` | ambiguous_deliverable_intent | work_order | `review_pack_ready`, `approved_clips_ready`, `platform_package_ready` | `TRANS-001`, `TRANS-006`, `TRANS-007` | `blocked`, `reject` | Use when the intended target state is not well-defined enough to judge success. |
| `requirement` | `REQ-002` | unstable_acceptance_criteria | inspection | `review_pack_approved`, `completed_local`, `completed_posted` | `TRANS-002`, `TRANS-011`, `TRANS-012` | `reject`, `rework_required`, `blocked` | Use when inspection standards shift or are too unclear to validate target-state arrival reliably. |
| `source` | `SRC-001` | unusable_source_input | state_or_transition | `review_pack_ready`, `invalid_source` | `TRANS-001` | `blocked`, `archive` | Use when the source media or source context is not fit for the intended next state. |
| `source` | `SRC-002` | unsupported_source_path | transition | `review_pack_ready`, `blocked` | `TRANS-001`, future `invalid_source` coverage | `blocked`, `archive` | Use when the requested source-to-state path is not supported by current workflow assumptions. |
| `execution` | `EXE-001` | artifact_not_produced | transition | `review_pack_ready`, `approved_clips_ready`, `platform_package_ready` | `TRANS-001`, `TRANS-006`, `TRANS-007` | `blocked`, `archive` | Use when the transition was attempted but the expected artifact did not materialize. |
| `execution` | `EXE-002` | artifact_produced_but_not_viable | transition | `review_pack_ready`, `approved_clips_ready`, `platform_package_ready` | `TRANS-001`, `TRANS-006`, `TRANS-007` | `rework_required`, `reject` | Use when an artifact exists but is not good enough to justify the claimed target state. |
| `inspection` | `INSP-001` | target_state_not_accepted | inspection | `review_pack_approved`, `approved_clips_ready`, `completed_local`, `completed_posted` | `TRANS-002`, `TRANS-006`, `TRANS-011`, `TRANS-012` | `reject`, `rework_required` | Use when inspection concludes the intended target state was not actually reached. |
| `inspection` | `INSP-002` | evidence_insufficient_for_acceptance | inspection | `review_pack_ready`, `review_pack_approved`, `approved_clips_ready`, `platform_package_ready` | `TRANS-001`, `TRANS-002`, `TRANS-006`, `TRANS-007` | `blocked`, `reject` | Use when evidence exists but is not strong enough to validate the target state. |
| `support` | `SUP-001` | missing_support_asset | transition | `review_pack_ready`, `approved_clips_ready`, `platform_package_ready`, `blocked` | `TRANS-001`, `TRANS-006`, `TRANS-007`, `TRANS-004`, `TRANS-008` | `blocked` | Use when transition success depends on a missing prompt, export path, template, pack, or helper asset. |
| `support` | `SUP-002` | missing_export_or_delivery_support | transition | `platform_package_ready`, `completed_local`, `completed_posted`, `blocked` | `TRANS-007`, `TRANS-011`, `TRANS-012` | `blocked`, `rework_required` | Use when a package exists but cannot complete delivery/posting due to missing support capability. |
| `control` | `CTL-001` | authorization_missing | control_or_inspection | `blocked`, `completed_local`, `completed_posted` | `TRANS-004`, `TRANS-008`, `TRANS-011`, `TRANS-012` | `blocked` | Use when progress depends on a manager or privileged decision that has not been obtained. |
| `control` | `CTL-002` | decision_closure_missing | inspection | `review_pack_approved`, `review_pack_rejected`, `blocked` | `TRANS-002`, `TRANS-003`, `TRANS-004`, `TRANS-008` | `blocked` | Use when the artifact is reviewable but no bounded accept/reject/rework decision is reached. |
| `lifecycle` | `LFC-001` | intentionally_archived | lifecycle | `archived` | `TRANS-005`, `TRANS-009`, `TRANS-010`, `TRANS-013`, `TRANS-014` | `archive` | Use when an artifact is intentionally retired rather than failing as an execution attempt. |
| `lifecycle` | `LFC-002` | superseded_by_rework_or_replacement | lifecycle | `archived`, `review_pack_ready` | `TRANS-010`, `TRANS-015` | `archive`, `rework_required` | Use when an earlier artifact is retired because a replacement or revised artifact path becomes the intended route forward. |

## Mapping Rules From Inspection Outcomes

Use these rules in v0:

- `pass`
  - not a failure record
- `reject`
  - usually maps to `inspection` or `requirement`
- `rework_required`
  - usually maps to `execution` or `inspection`
- `blocked`
  - usually maps to `support`, `control`, `source`, or `requirement`
- `archive`
  - usually maps to `lifecycle`, unless archiving is being used to hide an unresolved execution or inspection failure

## Mapping Rules From Transition Types

- `production`
  - most often maps to `source`, `execution`, `support`, or `requirement`
- `inspection`
  - most often maps to `inspection`, `requirement`, or `control`
- `control`
  - most often maps to `control` or `support`
- `archive`
  - most often maps to `lifecycle`

## Reject Versus Rework Guidance

For v0, preserve this distinction:

- `reject`
  - target state not reached and the current attempt should not continue as-is
- `rework_required`
  - target state not reached, but the artifact remains viable after bounded correction

This distinction should be used consistently by inspection and should later help determine whether repeated failure patterns are capability problems, requirement problems, or ordinary refinement loops.

## Non-Goals

- This spec does not define retry routing.
- This spec does not define qualification promotion or demotion rules.
- This spec does not assign numeric failure severity.
- This spec does not yet split every high-level failure into deep operational subcodes.
- This spec does not determine whether a failure justifies archiving, blocking, or rework; that remains a later routing and governance question.

## Immediate Follow-On Questions

The next review of this artifact should focus on:

1. whether the failure families are the right top-level grouping for the current control plane
2. whether any current transition or inspection outcomes are missing an actionable failure class
3. whether `support` and `control` are drawn cleanly enough apart
4. whether the taxonomy is compact enough for v0 while still being operationally useful

# Bounded Archetype-Routing Remediation Design

Date: 2026-06-14
Status: designed
Scope: bounded archetype-routing enhancement only

## Objective

Improve archetype specificity for retained synthetic candidates by extending the
existing `_hook_archetype(...)` assignment point with bounded event-family and
retained-context cues that already exist on the hook row.

This design targets the current post-synthetic-packaging bottleneck:

- some retained synthetic candidates still remain `hook_archetype = other`

This design does not attempt to change top-level hook policy. It only makes the
existing archetype assignment surface more specific for the currently validated
bounded cases.

## Problem Statement

The bounded synthetic-packaging validation established:

- `hook_mode` remains correctly bounded at `synthetic`
- packaging strategy is no longer one coarse fallback for every synthetic case
- the strongest repeated unresolved subtype is:
  - `archetype_salvageable`

The active bounded unresolved cases are:

1. `call_of_duty-second-bounded-clip`
2. `marvel_rivals-transfer-kjera`

Both cases:

- are approved/exported
- are context-backed enough to avoid rejection
- are specific enough to route into `archetype_salvageable`
- still remain:
  - `hook_archetype = other`

That means the next bottleneck is not hook-mode policy. It is archetype
specificity inside the existing archetype assignment surface.

## Design Classification

This is an `Archetype Routing Enhancement`.

It is not:

- a hook-policy change
- a review change
- a replayability change
- a selection change
- an export-timing change

## Policy Boundary

Keep these unchanged:

- `reject` gate
- `synthetic` gate
- `natural` gate
- top-level `hook_mode` definitions

Do not change:

- review semantics
- replayability contracts
- selection scope
- export eligibility
- publish-readiness posture

If cues are insufficient, preserve:

- `hook_archetype = other`

Do not force specificity when the retained evidence does not support it.

## Invariants

### Main Assignment Invariance

Archetype refinement must happen at the existing main assignment point:

- `_hook_archetype(...)`

Do not create:

- a second archetype pipeline
- a late rescue pass
- a parallel post-classification override path

### Hook-Mode Invariance

Archetype refinement may change:

- `hook_archetype`
- downstream packaging strategy selected from the existing routing logic

It may not change:

- `hook_mode`
- approval status
- lifecycle state
- export eligibility

### Explainability Invariance

Every archetype change must be explainable from retained row evidence.

Required explainability fields for validation output:

- `old_archetype`
- `new_archetype`
- `cue_match`
- `archetype_rationale`

Examples:

- `ability_seen + equipment_visibility + equipment_id`
- `team_wipe_seen + team_wipe_visibility`

### Additive Contract Invariance

This remediation must remain additive.

Allowed changes:

- extend `_hook_archetype(...)` inputs
- add compact explanation fields such as:
  - `archetype_cue_match`
  - `archetype_rationale`

Disallowed changes:

- new top-level hook states
- new score systems
- broad new archetype taxonomy

## Current Archetype Surface

The current archetype assignment is narrow and mostly text-driven:

- `clutch` if `"clutch"` appears in `event_type`
- `reversal` if `"reversal"` or `"swing"` appears in `event_type`
- `fail` if `"fail"`, `"death"`, or `"whiff"` appears in `event_type`
- `comedy` if `"comedy"` or `"funny"` appears in `event_type`
- `flex` if `"combo"` appears in `event_type`
- `domination` if `"medal"` appears and `final_score >= 0.85`
- `chaos` if `signal_count >= 3 and final_score >= 0.8`
- otherwise `other`

The hook row already carries richer evidence that this function does not yet
consume:

- `context_pre_signal_types`
- `context_post_signal_types`
- `metadata_summary.matched_signal_types`
- `metadata_summary.equipment_id`

## Recommended Approach

Approach:

- `main assignment extension`

Reason:

- the failure lives inside the existing archetype assignment surface
- the smallest durable fix is to extend `_hook_archetype(...)`
- this avoids a second archetype path and stays below the hook-policy line

## Bounded Inputs To Consume

Extend `_hook_archetype(...)` to accept the following existing cues:

- `context_pre_signal_types`
- `context_post_signal_types`
- `matched_signal_types`
- `equipment_id`

These are already present or derivable from the current hook row and metadata
surface. No new retrieval layer is needed.

## Bounded Branches

Only add the currently validated missing branches needed for the approved
`call_of_duty` and `marvel_rivals` set.

### Branch 1: `call_of_duty` utility/equipment shape

Current bounded shape:

- `event_type = ability_seen`
- `matched_signal_types` contains `equipment_visibility`
- `equipment_id` is present

Current problem:

- `ability_seen` is too generic
- the richer semantic clue lives in the matched signal family and equipment
  identity
- current assignment falls through to `other`

Proposed bounded rule:

When:

- `event_type == ability_seen`
- and `matched_signal_types` contains `equipment_visibility`
- and `equipment_id` is present

Assign:

- `hook_archetype = chaos`

Rationale:

- this bounded approved synthetic shape is better represented as a contextual
  utility-chaos moment than as generic `other`
- this uses the closest existing archetype rather than inventing a new one

Expected bounded target:

- `call_of_duty-second-bounded-clip`

### Branch 2: `marvel_rivals` team-wipe shape

Current bounded shape:

- `event_type = team_wipe_seen`
- `matched_signal_types` contains `team_wipe_visibility`
- retained post-context may contain `round_state_visibility`

Current problem:

- `team_wipe_seen` is already a meaningful event family
- but the current mapping table has no branch for it
- current assignment falls through to `other`

Proposed bounded rule:

When:

- `event_type == team_wipe_seen`
- and `matched_signal_types` contains `team_wipe_visibility`

Assign:

- `hook_archetype = chaos`

Rationale:

- within the current bounded taxonomy, `chaos` is the closest existing
  archetype for a validated multi-agent wipe moment
- this avoids adding a new taxonomy branch in this slice

Expected bounded target:

- `marvel_rivals-transfer-kjera`

## Fallback Rule

If none of the bounded cue families match:

- preserve current logic
- preserve `hook_archetype = other` when evidence is weak, mixed, or too coarse

This is required to prevent forced specificity.

## Explanation Surface

Add bounded explanation output so validation can show why an archetype changed.

Required additive fields:

- `archetype_cue_match`
- `archetype_rationale`

Example values:

- `ability_seen + equipment_visibility + equipment_id`
- `team_wipe_seen + team_wipe_visibility`

This explanation surface is observational and should not change hook-policy
behavior.

## Before/After Validation Table

The validation report should record, for each approved export:

- `fixture_id`
- `hook_mode`
- `old_archetype`
- `new_archetype`
- `cue_match`
- `archetype_rationale`
- `packaging_strategy`
- `approval_or_export_status_unchanged`

Expected bounded outcomes:

| Fixture | Hook Mode | Old Archetype | New Archetype | Cue Match | Packaging Strategy | Approval/Export Unchanged |
| --- | --- | --- | --- | --- | --- | --- |
| `call_of_duty-bootstrap-real-cod` | `synthetic` | `chaos` | `chaos` | `none` | unchanged | `yes` |
| `call_of_duty-second-bounded-clip` | `synthetic` | `other` | `chaos` | `ability_seen + equipment_visibility + equipment_id` | may change through existing packaging routing | `yes` |
| `marvel_rivals-transfer-absolute-cinema` | `synthetic` | `chaos` | `chaos` | `none` | unchanged | `yes` |
| `marvel_rivals-transfer-kjera` | `synthetic` | `other` | `chaos` | `team_wipe_seen + team_wipe_visibility` | may change through existing packaging routing | `yes` |

## Validation Plan

Validate only on the current approved bounded export set:

- `call_of_duty`
- `marvel_rivals`

Validation method:

1. regenerate hook/export artifacts for the same bounded cases
2. compare before vs after on:
   - `hook_mode`
   - `old_archetype`
   - `new_archetype`
   - `cue_match`
   - `archetype_rationale`
   - `packaging_strategy`
   - `approval_or_export_status_unchanged`

Success criteria:

- the two current `archetype_salvageable` cases no longer remain
  `hook_archetype = other`
- `hook_mode` stays unchanged for all validated cases
- approval/export status stays unchanged for all validated cases
- no previously specific archetype becomes less specific
- no case is forced into a specific archetype when cues are insufficient

## Risks And Guards

### Risk: Archetype drift becomes hidden hook-policy change

Guard:

- do not change `hook_mode` thresholds
- validate that `hook_mode` remains unchanged for the bounded set

### Risk: Ad hoc branches become a second taxonomy system

Guard:

- add only the currently validated missing branches
- do not create new top-level archetypes in this slice

### Risk: Weak cues force false specificity

Guard:

- preserve `other` when bounded cue requirements are not met

### Risk: Packaging is used to justify archetype changes

Guard:

- archetype assignment must be explainable directly from retained row evidence
- packaging strategy is downstream evidence, not a routing cue

## Recommended Next Goal

```text
/goal Write the bounded archetype-routing remediation implementation plan for the approved call_of_duty and marvel_rivals export set, using main assignment extension only. Preserve hook_mode thresholds, review semantics, replayability, selection scope, and fallback-to-other behavior when cues are insufficient.
```

## Conclusion

The next justified bounded remediation is not a hook-policy revision.

It is a narrow improvement to archetype specificity at the existing assignment
point.

The smallest additive fix is:

- extend `_hook_archetype(...)` with bounded retained-context and event-family
  cues already present on the hook row

while preserving:

- existing hook-mode policy
- review semantics
- replayability
- selection scope
- fallback to `other` when evidence is insufficient

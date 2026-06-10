# Inspection Architecture v0

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Define the first canonical inspection architecture for the pipeline redesign control plane.

This artifact states:

- what must be inspected
- who inspects it
- what evidence proves successful target-state arrival
- what pass/fail outcomes exist
- which transitions require explicit inspection before downstream progression

## Scope

This spec defines:

- inspection roles
- inspection object model
- inspection outcome vocabulary
- evidence expectations for inspected transitions
- which current transitions require explicit inspection

This spec does not define:

- failure taxonomy
- qualification thresholds
- routing policy
- inventory policy
- planning logic

## Guardrails

- Inspection validates whether a claimed target state was actually reached.
- Inspection is not the same thing as routing.
- Inspection does not assign qualification; it only records outcome evidence.
- Inspection should reference canonical states from [2026-06-10-state-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-state-catalog-v0.md) and canonical transitions from [2026-06-10-transition-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-transition-catalog-v0.md).
- v0 should stay compact enough that later failure and qualification layers can attach without redefining inspection objects.

## Why This Artifact Exists

The state catalog defines target conditions.

The transition catalog defines allowed state changes.

The next missing layer is the explicit model for deciding whether the target state was actually reached with enough evidence to allow downstream progression.

Without that layer:

- target-state arrival remains interpretive
- rejection versus rework remains inconsistent
- downstream progression risks being based on artifacts that exist but were not actually accepted

## Core Principle

For v0, an inspected transition should be understood as:

- transition execution produces candidate evidence
- inspection evaluates that evidence against the intended target state
- inspection emits a bounded outcome
- only then should the target state be treated as valid for downstream progression

## Inspection Roles

Use these role families in v0:

- `system_validator`
  - deterministic or structured validation of artifact readiness
- `human_editor`
  - editorial review where taste, judgment, or selection quality matters
- `manager_approver`
  - privileged approval or release decision

These are role families, not staffing or scheduling policies.

## Inspection Object Model

Each inspection event in v0 should declare:

- `inspection_id`
- `inspection_name`
- `inspection_role`
- `applies_to_transition`
- `input_state`
- `target_state`
- `evidence_required`
- `pass_outcome`
- `fail_outcomes`
- `notes`

## Inspection Outcome Vocabulary

Use this outcome vocabulary in v0:

- `pass`
  - target state is accepted as reached
- `reject`
  - target state is not accepted
- `rework_required`
  - target state is not accepted but the artifact may be revised and reintroduced
- `blocked`
  - target state cannot be judged or progressed due to a missing dependency, missing authority, or missing evidence
- `archive`
  - the artifact should not continue in active progression

This vocabulary defines inspection outcomes only. It does not define routing consequences beyond the current transition/state foundation.

## Transition Inspection Matrix

The following transitions require explicit inspection in v0.

| inspection_id | inspection_name | inspection_role | applies_to_transition | input_state | target_state | evidence_required | pass_outcome | fail_outcomes | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INSP-001 | review_pack_readiness_check | `system_validator` | `TRANS-001` | `raw_vod` | `review_pack_ready` | review-pack artifact exists, is attributable to the order, and is complete enough to review | `pass` | `blocked`, `archive` | This validates readiness, not editorial quality. |
| INSP-002 | review_pack_acceptance | `human_editor` | `TRANS-002` | `review_pack_ready` | `review_pack_approved` | review-pack artifact plus decision record showing the pack is accepted as valid downstream input | `pass` | `reject`, `blocked`, `archive` | This is the first substantive editorial gate. |
| INSP-003 | review_pack_rejection_review | `human_editor` | `TRANS-003` | `review_pack_ready` | `review_pack_rejected` | review-pack artifact plus decision record showing the pack is explicitly not accepted | `pass` | `blocked`, `archive` | Rejection is itself an inspected state change. |
| INSP-004 | approved_clip_set_validation | `human_editor` | `TRANS-006` | `review_pack_approved` | `approved_clips_ready` | bounded approved-clip set exists, is attributable, and matches the accepted review outcome | `pass` | `rework_required`, `blocked`, `archive` | This confirms the approved clips are real downstream inputs, not just an intent. |
| INSP-005 | platform_package_readiness_review | `system_validator` | `TRANS-007` | `approved_clips_ready` | `platform_package_ready` | platform package artifact exists, is attributable, and satisfies required local packaging checks | `pass` | `rework_required`, `blocked`, `archive` | This is readiness inspection before final delivery/posting inspection. |
| INSP-006 | final_local_delivery_inspection | `manager_approver` | `TRANS-011` | `platform_package_ready` | `completed_local` | local/export artifact exists and satisfies the intended local completion contract | `pass` | `rework_required`, `blocked`, `archive` | Use when the terminal obligation is local completion only. |
| INSP-007 | final_publish_inspection | `manager_approver` | `TRANS-012` | `platform_package_ready` | `completed_posted` | posting action succeeded, downstream proof exists, and the intended posted completion contract is satisfied | `pass` | `rework_required`, `blocked`, `archive` | Use when the terminal obligation includes external posting. |
| INSP-008 | review_pack_mixed_status_assessment | `human_editor` | `TRANS-016` | `review_pack_ready` | `review_pack_mixed_status` | aggregate review artifact has recorded member-level outcomes showing a mix of resolved and unresolved required members | `pass` | `blocked`, `archive` | This captures mixed aggregate condition without overclaiming downstream readiness. |
| INSP-009 | review_pack_needs_rework_assessment | `human_editor` | `TRANS-017` | `review_pack_mixed_status` | `review_pack_needs_rework` | aggregate review artifact plus decision record showing it remains active but is not yet acceptable downstream without bounded correction | `pass` | `blocked`, `archive` | This is an inspected non-terminal readiness state, not a terminal rejection. |
| INSP-010 | corrected_review_pack_readiness_check | `system_validator` | `TRANS-018` | `review_pack_needs_rework` | `review_pack_ready` | corrected aggregate review artifact exists, is attributable, and is complete enough for re-review | `pass` | `blocked`, `archive` | This validates that bounded correction returned the aggregate artifact to a reviewable state. |

## Authorization Versus Inspection

Not every transition in v0 requires a substantive inspection.

For now:

- `production` transitions that establish a reviewable or terminal state generally require explicit inspection
- `inspection` transitions are themselves inspection-backed state changes
- `control` transitions usually require confirmation or authorization, not substantive quality review
- `archive` transitions usually require authorization, not substantive quality review

This distinction prevents v0 from treating every control or archive action as if it were a quality inspection.

## Minimal Evidence Rules

For v0, inspection evidence should follow these rules:

1. Evidence must be attributable to the active order or transition context.
2. Evidence must be specific enough to justify the claimed target state.
3. An inspection outcome without evidence should not be treated as sufficient target-state arrival.
4. For editorial acceptance, a decision record is part of the evidence.
5. For terminal states, the completion artifact or posted proof is part of the evidence.

## Transitions Without Explicit Substantive Inspection

These transitions may proceed with authorization or recorded control intent rather than a substantive quality inspection in v0:

- `TRANS-004`
- `TRANS-005`
- `TRANS-008`
- `TRANS-009`
- `TRANS-010`
- `TRANS-013`
- `TRANS-014`
- `TRANS-015`

This does not mean they are unaudited. It means their v0 requirement is recorded control or lifecycle intent, not a separate acceptance review of target-state quality.

## Non-Goals

- This spec does not define detailed failure classes.
- This spec does not define qualification updates from inspection outcomes.
- This spec does not define unblock routing.
- This spec does not define which actor is staffed or scheduled to perform inspection.
- This spec does not change the transition catalog or state catalog beyond clarifying which transitions require inspection.

## Immediate Follow-On Questions

The next review of this artifact should focus on:

1. whether any inspected transitions are missing from v0
2. whether any inspection roles should be tightened or simplified
3. whether `system_validator` versus `human_editor` is drawn in the right place for approved-clip and package readiness checks
4. whether final local completion and final posted completion require distinct evidence surfaces
5. whether aggregate mixed-status review evidence is strong enough to justify state changes without introducing member-level sub-inspections yet

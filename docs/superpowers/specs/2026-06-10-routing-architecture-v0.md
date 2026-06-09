# Routing Architecture v0

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Define the first canonical routing architecture for the pipeline redesign control plane.

This artifact states:

- what transition or state follows each major transition outcome
- how `blocked`, `reject`, `rework_required`, `archive`, and terminal outcomes are routed
- which routing decisions are explicit in v0
- which routing decisions remain intentionally deferred

## Scope

This spec defines:

- routing principles
- routing objects
- canonical outcome-to-next-step guidance
- major routing paths across the current transition set
- explicit deferrals

This spec does not define:

- qualification thresholds
- inventory policy
- planning prioritization
- staffing or capacity allocation

## Guardrails

- Routing decides what happens next after a transition or inspection outcome.
- Routing is not the same as execution, inspection, or failure classification.
- Routing must attach to canonical states, canonical transitions, inspection outcomes, and failure classes already defined in:
  - [2026-06-10-state-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-state-catalog-v0.md)
  - [2026-06-10-transition-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-transition-catalog-v0.md)
  - [2026-06-10-inspection-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-inspection-architecture-v0.md)
  - [2026-06-10-failure-taxonomy-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-failure-taxonomy-v0.md)
- v0 should route clearly enough to remove ambiguity, but must not silently encode planning or qualification policy.

## Why This Artifact Exists

The control-plane stack now has:

- states
- transitions
- inspection outcomes
- failure explanations

The next missing layer is explicit next-step logic.

Without routing:

- `reject` and `rework_required` remain descriptive rather than operational
- `blocked` remains a state without a clear control-plane posture
- archive and terminal outcomes remain under-specified in terms of progression stopping rules

## Core Principle

For v0, routing should answer:

- given this transition and this outcome, what is the next valid control-plane move?

That move may be:

- proceed to a next transition
- enter or remain in a control state
- rework toward a prior reviewable state
- archive
- stop at a terminal production outcome

## Routing Object Model

Each routing rule in v0 should be expressible with:

- `route_id`
- `route_context`
- `trigger_transition`
- `trigger_outcome`
- `result_type`
- `next_transition_or_state`
- `notes`

## Result Types

Use these in v0:

- `advance`
- `rework`
- `control_hold`
- `archive`
- `terminal_stop`
- `deferred_definition`

Meaning:

- `advance`
  - continue into the next intended productive transition
- `rework`
  - route back to a prior productive state for bounded correction
- `control_hold`
  - remain blocked pending missing support, authority, or closure
- `archive`
  - retire from active progression
- `terminal_stop`
  - no further production routing is expected
- `deferred_definition`
  - a routing concept is acknowledged but intentionally not fully defined in v0

## Canonical Outcome Routing Rules

For v0, use these default interpretations:

- `pass`
  - route by advancing into the next allowed productive transition, or stop if the resulting state is terminal
- `reject`
  - do not continue the current attempt as-is; route either to a rejected state, archive, or another explicitly defined non-advance path
- `rework_required`
  - route to a bounded correction path if one exists
- `blocked`
  - route to `blocked` as a non-terminal control hold
- `archive`
  - route to `archived`

## Major Routing Table

| route_id | route_context | trigger_transition | trigger_outcome | result_type | next_transition_or_state | notes |
| --- | --- | --- | --- | --- | --- | --- |
| ROUTE-001 | successful source-to-review-pack progression | `TRANS-001` | `pass` | `advance` | `TRANS-002` or `TRANS-003` | Inspection determines whether the review-ready artifact is accepted or rejected. |
| ROUTE-002 | source-to-review-pack blocked | `TRANS-001` or `INSP-001` | `blocked` | `control_hold` | `blocked` | Remain in non-terminal control hold until routing architecture later defines state-specific unblock rules. |
| ROUTE-003 | source-to-review-pack archive outcome | `TRANS-001` or `INSP-001` | `archive` | `archive` | `archived` | Use when source or attempted output should not remain active. |
| ROUTE-004 | review-pack accepted | `TRANS-002` | `pass` | `advance` | `TRANS-006` | Accepted review pack becomes downstream input for approved-clip production. |
| ROUTE-005 | review-pack acceptance blocked | `TRANS-002` or `INSP-002` | `blocked` | `control_hold` | `blocked` | Use when the artifact is reviewable but no closure or required authority exists. |
| ROUTE-006 | review-pack acceptance rejected as target-state failure | `INSP-002` | `reject` | `rework` | `review_pack_rejected` via `TRANS-003` | In v0, rejection of review-pack acceptance is expressed by routing through the explicit rejection transition rather than bypassing it. This is a non-advance rejection path, not an archive decision. |
| ROUTE-007 | review-pack rejected and retired | `TRANS-003` | `pass` | `advance` | `TRANS-010` or `TRANS-015` | Rejection state may archive or rework depending on later bounded decision. |
| ROUTE-008 | review-pack rejected to rework | `TRANS-015` | `pass` | `rework` | `review_pack_ready` | Rework returns the artifact to a reviewable state rather than advancing directly downstream. |
| ROUTE-009 | approved clips successfully produced | `TRANS-006` | `pass` | `advance` | `TRANS-007` | Valid approved clip set advances to package creation. |
| ROUTE-010 | approved clips require bounded correction | `INSP-004` | `rework_required` | `deferred_definition` | bounded rework path not yet separately modeled | v0 acknowledges rework need but does not yet split an explicit approved-clip rework transition. |
| ROUTE-011 | approved clips blocked | `TRANS-006` or `INSP-004` | `blocked` | `control_hold` | `blocked` | Hold until missing support, authority, or closure is resolved. |
| ROUTE-012 | approved clips archived | `TRANS-009` or `INSP-004` | `archive` | `archive` | `archived` | Use when downstream progression is intentionally stopped. |
| ROUTE-013 | platform package successfully produced | `TRANS-007` | `pass` | `advance` | `TRANS-011` or `TRANS-012` | Route depends on whether the terminal obligation is local completion or posted completion. |
| ROUTE-014 | package readiness requires bounded correction | `INSP-005` | `rework_required` | `deferred_definition` | bounded packaging rework path not yet separately modeled | v0 records the need for correction without defining a separate rework transition. |
| ROUTE-015 | package readiness blocked | `TRANS-007` or `INSP-005` | `blocked` | `control_hold` | `blocked` | Use when delivery/posting preparation cannot proceed cleanly. |
| ROUTE-016 | local completion achieved | `TRANS-011` | `pass` | `terminal_stop` | `completed_local` | Terminal production outcome for no-posting-obligation path. |
| ROUTE-017 | posted completion achieved | `TRANS-012` | `pass` | `terminal_stop` | `completed_posted` | Terminal production outcome for posted path. |
| ROUTE-018 | final completion requires bounded correction | `INSP-006` or `INSP-007` | `rework_required` | `deferred_definition` | return-to-package correction path not yet separately modeled | v0 acknowledges correction need but does not yet define separate terminal rework transitions. |
| ROUTE-019 | final completion blocked | `TRANS-011`, `TRANS-012`, `INSP-006`, or `INSP-007` | `blocked` | `control_hold` | `blocked` | Use when completion is prevented by missing authorization, posting proof, or external dependency. |
| ROUTE-020 | any archive transition | `TRANS-005`, `TRANS-009`, `TRANS-010`, `TRANS-013`, `TRANS-014` | `pass` | `archive` | `archived` | Archive transitions are explicit retirement moves, not productive advancement. |

## Blocked Routing Rule

For v0:

- `blocked` is a non-terminal control state
- entering `blocked` means the artifact remains real but cannot progress
- re-entry from `blocked` is intentionally deferred to a later routing refinement

This keeps routing v0 from inventing generic unblock loops before state-specific unblock rules exist.

## Reject Versus Rework Routing Rule

For v0:

- `reject`
  - the current attempt does not continue as-is
  - routing should move into a rejected state, archive path, or other explicitly non-advance path
- `rework_required`
  - routing should prefer a bounded correction path if one exists
  - if no explicit rework transition exists yet, record a deferred routing definition rather than silently improvising one

## Terminal Routing Rule

For v0:

- `completed_local`
  - terminal production outcome unless an explicit lifecycle/archive transition is requested
- `completed_posted`
  - terminal production outcome unless an explicit lifecycle/archive transition is requested
- `archived`
  - terminal control outcome for active progression

## Explicit Deferrals

The following routing decisions remain intentionally deferred in v0:

- state-specific unblock paths from `blocked`
- explicit transition coverage for `invalid_source`
- explicit approved-clip rework transition beyond current inspection/failure signaling
- explicit package rework transition beyond current inspection/failure signaling
- explicit final-delivery or final-posting rework transition beyond current inspection/failure signaling

These are deferred because v0 needs routing clarity without overbuilding correction branches before qualification and support-asset layers exist.

## Non-Goals

- This spec does not define qualification-based routing.
- This spec does not define inventory-aware routing.
- This spec does not define scheduling or queue priority.
- This spec does not define staffing or capacity selection.
- This spec does not redefine failure families or inspection roles.

## Immediate Follow-On Questions

The next review of this artifact should focus on:

1. whether any routing rows are still too implicit
2. whether the deferred rework paths should remain deferred or be promoted into explicit transitions
3. whether `blocked` should gain state-specific re-entry rules before qualification work begins
4. whether archive routing is sufficiently distinct from reject and rework behavior

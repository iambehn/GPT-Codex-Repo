# Attribution Architecture v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Define the first compact architecture for capturing subject identity at transition execution time so transition outcome records can support later qualification, failure analysis, and routing analysis without fabricated attribution.

This artifact states:

- what attribution is in this system
- what attribution facts must be captured when a transition outcome is recorded
- how attribution quality should be represented conservatively
- how attribution maps into the existing event-capture and ledger surfaces

## Scope

This spec defines:

- a compact attribution model
- bounded attribution vocabularies
- attribution confidence rules
- attribution capture rules at transition execution time
- the relationship between attribution and the existing event-capture and ledger schemas

This spec does not define:

- qualification promotion or demotion policy
- planning or scheduling policy
- staffing policy
- inventory or resource allocation
- a second history schema parallel to the ledger

## Guardrails

- Attribution exists to record who or what performed, inspected, or authorized a transition outcome.
- Attribution must preserve uncertainty explicitly instead of forcing false precision.
- Attribution must be captured at execution time when possible, not reconstructed later by default.
- Attribution quality must be visible to downstream consumers.
- Attribution architecture must extend the existing event-capture and ledger baseline rather than replace it.

This artifact must stay consistent with:

- [2026-06-11-operational-observation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-operational-observation-report-v0.md)
- [2026-06-11-transition-event-capture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-event-capture-v0.md)
- [2026-06-11-transition-outcome-ledger-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-v0.md)
- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)

## Why This Artifact Exists

The operational observation report showed:

- the control plane survives
- the evidence plane survives
- the weakest field is not transition outcome itself
- the weakest field is subject attribution quality at execution time

Observed result:

- `118` recoverable transition-outcome candidates
- `0` explicit strong subject-attributed rows
- `6` deterministic or strong inferred rows
- `112` weak or `unknown_subject` rows

That means the next missing layer is not more qualification logic.

It is the compact architecture that answers:

- who performed the transition
- how that identity was obtained
- how certain the system is
- what basis supports the attribution

## Position In The Stack

This artifact sits inside the capture-to-ledger boundary:

- `Source`
- `State`
- `Transition`
- `Inspection`
- `Transition Event Capture`
- `Attribution`
- `Transition Outcome Ledger`
- `Failure`
- `Routing`
- `Qualification`

Interpretation:

- execution creates outcomes
- capture records them
- attribution strengthens or weakens the evidence quality of those recorded outcomes
- qualification consumes the resulting history

## Core Principle

Attribution Architecture v0 should answer one question conservatively:

- who or what is being credited with this transition outcome?

For v0:

- if the subject is explicit, record it
- if the subject is deterministic, record it and say why
- if the subject is only weakly inferable, record that weakness explicitly
- if the subject is not knowable at execution time, use `unknown_subject`

The architecture should prefer incomplete truth over fabricated certainty.

## Attribution Model

Each emitted transition outcome should be evaluated along four dimensions:

- `subject_identity`
- `attribution_source`
- `attribution_confidence`
- `attribution_basis`

These dimensions define attribution quality.

They do not create a second event schema.

## Dimension 1: Subject Identity

`subject_identity` is the attributed actor or actor class for the transition outcome.

Use the existing qualification-aligned subject vocabulary:

- `codex_structured_worker`
- `system_validator`
- `human_editor`
- `manager_approver`
- `unknown_subject`

Meaning:

- `codex_structured_worker`
  - machine-executed or machine-assisted production step under the structured worker surface
- `system_validator`
  - deterministic validation or inspection step owned by the system
- `human_editor`
  - editorial or review judgment performed by a human reviewer
- `manager_approver`
  - explicit governance, authorization, or approval decision
- `unknown_subject`
  - best available fallback when attribution is not sufficiently knowable at capture time

## Dimension 2: Attribution Source

`attribution_source` describes where the subject identity came from.

Bounded v0 values:

- `explicit_actor_argument`
- `explicit_review_record`
- `explicit_approval_record`
- `deterministic_system_step`
- `workflow_owned_default`
- `unknown_at_capture`

Meaning:

- `explicit_actor_argument`
  - the execution surface received a concrete actor identity as part of the write or command
- `explicit_review_record`
  - the execution surface persisted an explicit reviewer identity with the decision
- `explicit_approval_record`
  - the execution surface persisted an explicit approver identity with the decision
- `deterministic_system_step`
  - the transition can be attributed to a system-owned step without ambiguity
- `workflow_owned_default`
  - ownership is being inferred from the workflow surface rather than explicit event data
- `unknown_at_capture`
  - the capture surface could not determine a defensible source

## Dimension 3: Attribution Confidence

`attribution_confidence` is the system's bounded assessment of attribution strength.

Bounded v0 values:

- `strong`
- `medium`
- `weak`
- `unknown`

Meaning:

- `strong`
  - explicit subject identity was recorded directly by the execution surface
- `medium`
  - subject identity is deterministic or strongly constrained by the execution surface, but not explicitly named in the event record
- `weak`
  - subject identity is only loosely inferred from workflow ownership or adjacent notes
- `unknown`
  - no defensible subject identity is available at capture time

## Dimension 4: Attribution Basis

`attribution_basis` is the compact explanation for why the claimed subject identity and confidence are considered valid.

This should stay short and inspectable.

Examples:

- `review decision row includes reviewer identity`
- `publish-readiness validator is system-owned`
- `workflow owner inferred from draft family`
- `no actor persisted in current execution surface`

## Confidence Rules

Use the following v0 confidence rules:

### `strong`

Allowed only when:

- a concrete subject is recorded directly by the execution surface
- the attribution source is explicit
- the evidence reference points to the same persisted surface that carries the actor context

### `medium`

Allowed when:

- the subject is not directly named in the event row
- but the execution surface is deterministic enough to narrow attribution safely

Examples:

- a deterministic validator step
- a system-owned source-intake adapter

### `weak`

Use when:

- the subject is being inferred from workflow context
- adjacent notes imply likely ownership
- but the execution record itself does not carry enough direct evidence

### `unknown`

Use when:

- there is not enough information to make even a weak attribution claim

In that case:

- `subject_identity = unknown_subject`
- `attribution_source = unknown_at_capture`

## Preferred Attribution Order

Capture-time attribution should prefer sources in this order:

1. explicit actor identity passed by the execution surface
2. explicit review record
3. explicit approval record
4. deterministic system-owned step
5. workflow-owned default
6. `unknown_subject`

This is intentionally conservative.

The system should move down the order only when the stronger option is unavailable.

## Mapping To Existing Event-Capture Baseline

Attribution Architecture v0 preserves the existing event-capture schema as the baseline.

Current required event-capture fields already cover the minimum viable attribution contract:

- `subject`
- `subject_attribution_basis`

Current recommended event-capture fields already provide useful execution context:

- `capture_source`
- `evidence_reference`
- `workflow_ref`

For v0, attribution dimensions map into those surfaces as follows:

| attribution dimension | baseline event-capture mapping |
| --- | --- |
| `subject_identity` | `subject` |
| `attribution_source` | bounded prefix or normalized leading token in `subject_attribution_basis` |
| `attribution_confidence` | derived from `subject` plus `subject_attribution_basis` at interpretation time |
| `attribution_basis` | remainder of `subject_attribution_basis` and supporting `notes` |

Interpretation:

- no required event-capture schema expansion is needed for v0
- the existing capture envelope is sufficient if the attribution values are written more rigorously

## Mapping To Existing Ledger Baseline

Attribution Architecture v0 also preserves the existing ledger schema as the baseline.

Current ledger fields already provide the minimum viable attribution surface:

- `subject`
- `subject_attribution_basis`
- `evidence_reference`
- `notes`

For v0:

- ledger rows should keep the compact baseline schema
- attribution confidence should be reconstructable from the recorded fields
- if later operational use shows repeated ambiguity, a future schema revision may add explicit `attribution_confidence`

That future expansion is not required now.

## Execution-Time Capture Rules

### Rule 1: attribution is captured when the event is emitted

Do not defer subject attribution to later normalization when the execution surface knows enough to record it now.

### Rule 2: attribution is part of event quality, not a blocker for event existence

Missing attribution must not suppress the event.

Instead:

- emit the event
- record `unknown_subject`
- mark the attribution source as `unknown_at_capture`

### Rule 3: explicit beats inferred

If the execution surface holds direct actor data, use it even when a deterministic fallback exists.

### Rule 4: deterministic beats workflow-owned default

If the step is clearly system-owned, do not downgrade it to workflow ownership inference.

### Rule 5: workflow ownership alone is weak evidence

Workflow ownership should not be treated as strong attribution unless the workflow itself is the direct acting surface.

## Attribution By Transition Class

### Production transitions

Preferred attribution subjects:

- `codex_structured_worker`
- `human_editor`
- `unknown_subject`

Typical confidence posture:

- `medium` when the step is clearly system-owned
- `weak` when only workflow ownership is known

### Inspection transitions

Preferred attribution subjects:

- `human_editor`
- `system_validator`
- `manager_approver`
- `unknown_subject`

Typical confidence posture:

- `strong` only when the inspection surface persists the reviewer or approver
- otherwise `medium` for deterministic system checks or `weak` for implicit review ownership

### Control transitions

Preferred attribution subjects:

- `manager_approver`
- `system_validator`
- `unknown_subject`

Typical confidence posture:

- `strong` when the control decision is explicitly persisted with an actor
- otherwise `medium` only for deterministic system-owned holds

### Source-intake transitions

Preferred attribution subjects:

- `codex_structured_worker`
- `system_validator`
- `unknown_subject`

Typical confidence posture:

- `medium` for deterministic source fetch or validation steps
- `weak` when source handling is only implied by workflow surface state

## Current Evidence-Based Diagnosis

Using the operational observation report as evidence:

- source fetch outcomes are the best current example of deterministic system-owned attribution
- publish-readiness outcomes are also structurally attributable to deterministic validation
- accepted binding successes currently remain weakly attributable because the workflow artifacts record timestamps and notes but not explicit actor identity

That means the highest-leverage capture improvement is:

- explicit actor persistence on accepted binding and review-surface writes

Not:

- changes to the control-plane state model
- changes to qualification policy

## Non-Goals

Do not add in v0:

- qualification promotion thresholds
- planning or staffing interpretation
- inventory semantics
- cost or duration analytics
- identity resolution across external account systems
- probabilistic identity matching beyond the bounded confidence vocabulary above

## Explicit Deferrals

These remain deferred beyond Attribution Architecture v0:

- explicit persisted `attribution_confidence` field
  - future event-capture or ledger revision if operational use proves it necessary
- external identity registry or account mapping
  - future identity or operator model
- planner use of attribution history
  - future planning doctrine
- attribution-weighted qualification promotion
  - future qualification evidence policy

## Minimum v0 Outcome

Attribution Architecture v0 is successful if it makes these points explicit:

- attribution is a first-class part of transition outcome quality
- attribution quality is separate from transition outcome existence
- `unknown_subject` is a valid and necessary fallback
- strong attribution requires explicit execution-surface evidence
- medium and weak attribution must remain visible to downstream consumers
- no required capture or ledger schema rewrite is needed to begin operating with stronger attribution discipline

## Recommended Next Pressure Test

Apply this attribution model to one execution surface that currently records weakly attributable success, such as:

- accepted binding writes
- review decision writes
- publish-readiness result writes

and ask:

- can the execution surface persist explicit actor identity?
- can it emit `subject_attribution_basis` from a bounded vocabulary rather than free-form notes?
- does attribution quality move from `weak` to `strong` without changing the control-plane model?

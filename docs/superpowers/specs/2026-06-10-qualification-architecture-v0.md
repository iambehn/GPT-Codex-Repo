# Qualification Architecture v0

Date: 2026-06-10
Status: draft
Owner: Codex

## Objective

Define the first canonical qualification architecture for the pipeline redesign control plane.

This artifact states:

- who or what is trusted to perform each major transition
- what qualification levels exist
- what evidence supports qualification
- how inspection and failure history affect trust

## Scope

This spec defines:

- qualification subjects
- qualification ladder
- qualification evidence model
- transition-level qualification matrix
- promotion and demotion logic at a high level

This spec does not define:

- staffing or hiring policy
- inventory policy
- planning or scheduling policy
- resource allocation across many workers

## Guardrails

- Qualification is about trust to perform a transition reliably enough for the current control plane, not about broad capability claims.
- Qualification must attach to canonical transitions and their inspection/failure history already defined in:
  - [2026-06-10-transition-catalog-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-transition-catalog-v0.md)
  - [2026-06-10-inspection-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-inspection-architecture-v0.md)
  - [2026-06-10-failure-taxonomy-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-failure-taxonomy-v0.md)
  - [2026-06-10-routing-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-routing-architecture-v0.md)
- Qualification must remain separate from routing, inspection, and failure explanation.
- v0 should prefer compact trust rules over speculative automation or broad workforce modeling.

## Why This Artifact Exists

The control-plane stack now has:

- states
- transitions
- inspection rules
- failure explanations
- routing rules

The next missing layer is explicit trust:

- who is currently trusted to execute each transition
- under what evidence
- with what limits

Without that layer:

- “can perform” remains too vague
- repeated inspection and failure outcomes do not change the trust model
- later planning cannot distinguish trusted lanes from experimental ones

## Core Principle

For v0, qualification should answer:

- for this transition, which subject is trusted to perform it at what level, based on what evidence?

## Qualification Subjects

Use these subject families in v0:

- `codex_structured_worker`
- `system_validator`
- `human_editor`
- `manager_approver`

Meaning:

- `codex_structured_worker`
  - structured execution subject for repeatable artifact-production or rework transitions
- `system_validator`
  - deterministic or structured checking subject
- `human_editor`
  - editorial judgment subject
- `manager_approver`
  - privileged authorization and final acceptance subject

These are trust subjects, not staffing commitments.

## Qualification Ladder

Use this ladder in v0:

- `Q0`
  - not qualified
- `Q1`
  - trial only
- `Q2`
  - qualified with strict guardrails
- `Q3`
  - qualified for normal production
- `Q4`
  - qualified for scaled core production

## Qualification Meaning

### `Q0`

No current trust for normal execution of the transition.

### `Q1`

Possible only as a trial, with active oversight or bounded experimental use.

### `Q2`

Trusted only under explicit constraints and guardrails.

### `Q3`

Trusted for routine production use under the current architecture.

### `Q4`

Trusted as a stable, scale-ready core transition subject.

## v0 Placement Rule

Unless the repo has explicit repeat evidence attached to a given subject-transition pair, v0 placements should be treated conservatively:

- `Q1`
  - plausible or trial-only
- `Q2`
  - demonstrated in limited examples with guardrails
- `Q3`
  - reserved for repeatable transitions without unusual rescue
- `Q4`
  - reserved for trusted core transitions with strong repeat evidence

Where that evidence is not yet collected in the control-plane artifacts, v0 should prefer lower current levels over aspirational current-state claims.

## Qualification Evidence Model

Qualification in v0 should be supported by:

- transition success history
- inspection pass/fail history
- failure-pattern history
- rework frequency
- block frequency where relevant

Evidence should answer:

- does this subject repeatedly reach the intended target state?
- does inspection accept the results?
- do failures cluster in a way that narrows trust?
- is the transition stable enough for broader use or only for guarded use?

## Qualification Record Shape

Each qualification record in v0 should be expressible with:

- `qualification_subject`
- `transition_id`
- `current_level`
- `target_level`
- `subject_attribution_basis`
- `supporting_evidence`
- `guardrails`
- `demotion_signals`
- `notes`

## High-Level Qualification Rules

For v0:

- successful transition execution without repeated negative inspection outcomes increases trust
- repeated `rework_required`, `reject`, or `blocked` outcomes narrow trust
- repeated lifecycle retirement by itself does not necessarily imply low execution trust
- repeated support failures may indicate missing capability support rather than unqualified execution subject
- repeated control failures may indicate governance friction rather than unqualified execution subject

This keeps qualification from over-blaming the wrong layer.

## Subject Attribution Rule

For v0 and the current aggregate mixed-status patch:

- qualification evidence must identify which subject actually executed or authorized the transition being judged
- for aggregate review transitions, qualification evidence must distinguish:
  - artifact-production subject
  - member-status decision subject where applicable
  - non-terminal readiness or authorization subject where applicable

If subject attribution is missing, the transition may still be recorded operationally, but qualification should remain conservative and should not be promoted based on unattributed outcomes.

## Transition Qualification Matrix

| transition_id | transition_name | primary_subject | current_level | target_level | subject_attribution_basis | supporting_evidence | guardrails | demotion_signals | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `TRANS-001` | `raw_vod_to_review_pack_ready` | `codex_structured_worker` | `Q1` | `Q2` | attributable production artifact plus readiness inspection context | review-pack artifact creation plus readiness inspection outcomes | supported source assumptions, bounded review-pack contract | repeated source failures, repeated blocked readiness, repeated unusable artifacts | Still early; trust depends heavily on source and support conditions. |
| `TRANS-002` | `review_pack_ready_to_review_pack_approved` | `human_editor` | `Q2` | `Q3` | explicit approval decision record tied to the reviewing subject | explicit editorial acceptance outcomes | bounded review criteria and decision ownership | repeated decision-closure failures, repeated unstable acceptance criteria | Current placement is conservative until repeat acceptance evidence is recorded for this exact transition. |
| `TRANS-003` | `review_pack_ready_to_review_pack_rejected` | `human_editor` | `Q2` | `Q3` | explicit rejection decision record tied to the reviewing subject | explicit editorial rejection outcomes | clear rejection reasoning and bounded review surface | repeated ambiguous rejection decisions | Rejection remains a valid inspected transition, but current trust stays below routine-production level until repeat evidence is explicit. |
| `TRANS-004` | `review_pack_ready_to_blocked` | `manager_approver` | `Q1` | `Q2` | explicit block authorization tied to the responsible control subject | consistent control-hold decisions with clear block reasons | use only when block condition is explicit | repeated vague or unnecessary blocks | This is a control transition, so trust is about governance clarity rather than broad execution authority. |
| `TRANS-005` | `review_pack_ready_to_archived` | `manager_approver` | `Q1` | `Q2` | explicit archive authorization tied to the responsible control subject | explicit archive decisions with rationale | archive only when continuation is not justified | repeated premature archive decisions | Archive trust is lifecycle trust, not production trust. |
| `TRANS-006` | `review_pack_approved_to_approved_clips_ready` | `codex_structured_worker` | `Q1` | `Q2` | attributable production artifact plus downstream validation context | approved-clip production plus human-editor validation outcomes | bounded approved-clip expectations, supported editorial context | repeated rework_required, repeated target-state rejection | Trust still depends on editorial validation rather than pure automation. |
| `TRANS-007` | `approved_clips_ready_to_platform_package_ready` | `codex_structured_worker` | `Q2` | `Q3` | attributable package artifact plus readiness validation context | package creation plus readiness validation outcomes | supported packaging templates and bounded platform scope | repeated packaging rework, repeated support failures | More structurally stable than upstream transitions, but still not evidenced strongly enough for a higher current level. |
| `TRANS-008` | `approved_clips_ready_to_blocked` | `manager_approver` | `Q1` | `Q2` | explicit block authorization tied to the responsible control subject | explicit block decisions tied to package path | bounded use of block state | repeated vague or overused block decisions | Another control transition where trust is governance trust, kept conservative in v0. |
| `TRANS-009` | `approved_clips_ready_to_archived` | `manager_approver` | `Q1` | `Q2` | explicit archive authorization tied to the responsible control subject | explicit archive decisions with rationale | archive only when downstream progression is not justified | repeated premature archive decisions | Lifecycle trust, not production trust. |
| `TRANS-010` | `review_pack_rejected_to_archived` | `manager_approver` | `Q1` | `Q2` | explicit archive authorization following a recorded rejection state | archive decisions following explicit rejection | preserve distinction between rejection and retirement | repeated archival where rework should have been used | Trust depends on clean separation of reject vs archive. |
| `TRANS-011` | `platform_package_ready_to_completed_local` | `manager_approver` | `Q2` | `Q3` | explicit final acceptance decision tied to the approving subject | final local-delivery inspection outcomes | bounded local completion contract | repeated blocked completion or unstable acceptance criteria | Terminal acceptance transition with high governance weight, but current placement remains below routine-production trust until repeat evidence is attached. |
| `TRANS-012` | `platform_package_ready_to_completed_posted` | `manager_approver` | `Q2` | `Q3` | explicit final acceptance decision plus posted proof tied to the approving subject | final publish inspection outcomes plus posted proof | explicit posting proof and approval boundary | repeated blocked publish completions, repeated insufficient evidence | External posting adds more failure surface than local completion. |
| `TRANS-013` | `completed_local_to_archived` | `manager_approver` | `Q2` | `Q3` | explicit lifecycle retirement authorization tied to the responsible control subject | consistent lifecycle retirement handling | archive only after local completion is explicit | repeated premature or unclear retirement | Stable lifecycle action once terminal state is reached, but v0 keeps current trust conservative. |
| `TRANS-014` | `completed_posted_to_archived` | `manager_approver` | `Q2` | `Q3` | explicit lifecycle retirement authorization tied to the responsible control subject | consistent lifecycle retirement handling | archive only after posted completion is explicit | repeated premature or unclear retirement | Same as local archival, but current trust remains below core until repeat evidence is explicit. |
| `TRANS-015` | `review_pack_rejected_to_review_pack_ready` | `codex_structured_worker` | `Q1` | `Q2` | attributable corrected artifact plus renewed readiness inspection context | revised review-pack production plus later readiness inspection | bounded correction only; do not broaden to open-ended reinterpretation | repeated rework loops, repeated rejection after rework | Rework trust should remain narrower than first-pass production trust. |
| `TRANS-016` | `review_pack_ready_to_review_pack_mixed_status` | `human_editor` | `Q1` | `Q2` | explicit mixed-status decision record tied to the reviewing subject and summarized member-level outcome evidence | explicit mixed-status review outcomes with attributable member-level status summary | use only when aggregate evidence clearly shows both resolved and unresolved required members | repeated unattributed mixed-status decisions, repeated ambiguous aggregate review outcomes | Aggregate mixed-status is an inspected judgment and should stay conservative until subject attribution is consistently explicit. |
| `TRANS-017` | `review_pack_mixed_status_to_review_pack_needs_rework` | `human_editor` | `Q1` | `Q2` | explicit needs-rework decision record tied to the reviewing subject with bounded unresolved-member basis | explicit needs-rework decisions on active aggregate review artifacts | bounded correction scope and explicit member-level unresolved basis | repeated vague needs-rework outcomes, repeated closure failures | This transition depends on clear attribution of the readiness judgment, not just artifact existence. |
| `TRANS-018` | `review_pack_needs_rework_to_review_pack_ready` | `codex_structured_worker` | `Q1` | `Q2` | attributable corrected aggregate artifact plus renewed readiness inspection context | corrected aggregate review-pack artifact plus renewed readiness inspection outcomes | bounded correction only; do not treat as open-ended reinterpretation | repeated rework loops, repeated return to mixed-status without progress | Rework remains conservative until corrected aggregate artifacts repeatedly pass with explicit attribution. |

## Promotion Guidance

For v0:

- promote when the subject repeatedly reaches the target state and inspection consistently passes
- promote more slowly when success depends on strong guardrails
- do not promote based only on artifact existence without inspection support

## Demotion Guidance

For v0:

- demote when repeated `reject` or `rework_required` outcomes show the target state is not being reached reliably
- demote when repeated `blocked` outcomes are caused by poor subject decisions rather than missing support or missing authority
- do not automatically demote for failures clearly attributable to support or control layers outside the subject’s responsibility

## Support Versus Qualification Rule

If failures repeatedly indicate:

- `support`

then the likely next action is support improvement, not automatic qualification demotion.

If failures repeatedly indicate:

- `inspection`
- `execution`
- `requirement`

then trust in the subject’s transition performance should narrow or remain guarded.

## Control Versus Qualification Rule

If failures repeatedly indicate:

- `control`

then the likely next action is governance or closure improvement, not automatic demotion of a production subject.

This prevents v0 from blaming the wrong trust subject for managerial or authorization friction.

## Non-Goals

- This spec does not define staffing headcount or assignment policy.
- This spec does not define inventory-aware trust.
- This spec does not define planning priority or market selection.
- This spec does not set numeric thresholds for promotion or demotion yet.
- This spec does not claim any broad “qualified for gaming editing” capability.

## Immediate Follow-On Questions

The next review of this artifact should focus on:

1. whether the qualification subjects are the right abstraction layer
2. whether any transition is assigned to the wrong primary subject
3. whether the initial `Q1`/`Q2`/`Q3` placements are too optimistic or too conservative
4. whether support and control failures are sufficiently separated from execution trust

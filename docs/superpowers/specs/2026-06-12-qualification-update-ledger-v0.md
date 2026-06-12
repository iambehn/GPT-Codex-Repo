# Qualification Update Ledger v0

Date: 2026-06-12
Status: Draft
Scope: Canonical governance-history record for qualification-state changes

## 1. Objective

Define the first canonical ledger for qualification-state updates.

This artifact specifies:

- the record shape for qualification-state changes
- the governance metadata required for each update
- the audit-trail requirements for update history
- the retention rule for prior qualification-state changes
- the linkage between a qualification update and its promotion-review packet

This artifact preserves:

- the committed qualification architecture
- the calibrated qualification thresholds
- the qualification promotion review packet
- the transition outcome ledger

It does not redefine:

- promotion thresholds
- transition semantics
- outcome-event recording
- planning
- inventory
- staffing
- scheduling

## 2. Why This Artifact Exists

The system has now produced its first real qualification decision:

- `system_validator x SITRANS-001`
- `Q1 -> Q2`
- `approve_promotion`

That means the architecture now has:

- outcome history
- promotion candidates
- promotion review packets
- promotion decisions

What is still missing is durable governance memory of the trust change itself.

Without that:

- qualification state changes remain stranded in reports
- later reviewers cannot easily inspect trust-history evolution
- approved updates have no canonical persisted record

## 3. Core Principle

The qualification update ledger records:

- what trust changed
- why it changed
- who approved or rejected the change
- when the change became effective

It does not record:

- the underlying transition outcomes themselves
- the thresholds themselves
- the full review packet content

Those remain separate surfaces.

## 4. Position In The Stack

This artifact sits after promotion review and before later trust-history consumers:

- `Transition Outcome Ledger`
- `Qualification Candidate`
- `Promotion Review Packet`
- `Promotion Decision`
- `Qualification Update Ledger`

Interpretation:

- the outcome ledger answers:
  - what happened operationally
- the promotion review packet answers:
  - should trust change
- the qualification update ledger answers:
  - what trust actually changed

## 5. Guardrails

- The qualification update ledger must remain separate from the transition outcome ledger.
- One update row must correspond to one reviewed governance decision about one `subject x transition` pair.
- The ledger records effective trust-state changes and explicit non-changes; it must not infer them silently.
- The ledger must reference the promotion-review packet rather than duplicate the full packet body.
- Update history must be append-only.
- Historical qualification updates must remain inspectable after later promotions or demotions occur.

## 6. Evidence Surface

Primary baseline surfaces:

- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)
- [2026-06-11-qualification-threshold-calibration-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-qualification-threshold-calibration-report-v0.md)
- [2026-06-12-qualification-promotion-review-packet-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-12-qualification-promotion-review-packet-v0.md)
- [2026-06-12-first-qualification-promotion-review-decision-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-12-first-qualification-promotion-review-decision-report-v0.md)
- [2026-06-11-transition-outcome-ledger-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-v0.md)

Primary evidence surface for v0:

- approved promotion decision:
  - `system_validator x SITRANS-001`
  - `Q1 -> Q2`

## 7. Ledger Shape

Use one compact JSON artifact aligned with repo ledger conventions.

Top-level fields:

- `schema_version`
- `generated_at`
- `ledger_id`
- `row_count`
- `rows`

Suggested schema version:

- `qualification_update_ledger_v1`

## 8. Update Row Contract

Each row represents one explicit governance decision about one `subject x transition` record.

Required fields:

- `update_id`
- `timestamp`
- `qualification_subject`
- `transition_id`
- `old_level`
- `new_level`
- `review_decision`
- `review_packet_reference`
- `reviewer_subject`
- `effective_timestamp`
- `decision_reason`
- `notes`

Recommended additional fields:

- `threshold_version`
- `candidate_reference`
- `update_reason_code`
- `prior_update_reference`

## 9. Required Row Field Meaning

### `update_id`

Unique identifier for this qualification-state update row.

### `timestamp`

UTC timestamp when the update row was recorded.

### `qualification_subject`

The governed qualification subject whose trust state is under update.

Examples:

- `system_validator`
- `codex_structured_worker`
- `human_editor`
- `manager_approver`

### `transition_id`

Canonical transition identifier for the trust update.

Examples:

- `SITRANS-001`
- `TRANS-002`
- `TRANS-016`

### `old_level`

Qualification level before the reviewed decision.

Examples:

- `Q0`
- `Q1`
- `Q2`

### `new_level`

Qualification level after the reviewed decision.

Examples:

- `Q1`
- `Q2`
- `Q3`

For non-promotions:

- `new_level` may equal `old_level`

This allows the ledger to preserve explicit reviewed non-changes.

### `review_decision`

The governance decision outcome from the promotion review process.

Use this bounded set in v0:

- `approve_promotion`
- `reject_promotion`
- `defer_pending_more_evidence`

### `review_packet_reference`

Canonical reference to the review packet or decision report that justified the update row.

This is the primary audit linkage.

### `reviewer_subject`

The subject that made the governance decision.

Example:

- `codex_review_operator`

### `effective_timestamp`

UTC timestamp at which the qualification-state change became effective.

For approved promotions:

- this marks when the new level became active

For rejected or deferred updates:

- this may equal the review timestamp because the non-change itself became effective then

### `decision_reason`

Compact human-readable explanation of why the decision was made.

### `notes`

Additional context that does not belong in the core bounded fields.

## 10. Recommended Additional Fields

### `threshold_version`

Records which threshold calibration version governed the decision.

### `candidate_reference`

Reference to the candidate-report surface that first identified the update candidate.

### `update_reason_code`

Suggested bounded values:

- `threshold_met`
- `threshold_not_met`
- `attribution_insufficient`
- `literal_evidence_insufficient`
- `inspection_contradiction`
- `stability_insufficient`
- `governance_incomplete`

### `prior_update_reference`

Reference to the previous update row for the same `subject x transition` pair.

This supports trust-history chains without duplicating full past state.

## 11. Decision Semantics

### 11.1 Approved promotion

When:

- `review_decision = approve_promotion`

Then:

- `new_level` must differ from `old_level`
- `effective_timestamp` must be present
- `review_packet_reference` must point to the approving packet or decision report

### 11.2 Rejected promotion

When:

- `review_decision = reject_promotion`

Then:

- `new_level` should equal `old_level`
- the ledger still records the rejected governance event

Reason:

- reviewed non-changes are part of governance history

### 11.3 Deferred promotion

When:

- `review_decision = defer_pending_more_evidence`

Then:

- `new_level` should equal `old_level`
- the ledger still records the defer event

Reason:

- deferral is also a governance decision and should not be lost

## 12. Audit-Trail Requirements

Every update row must make it possible to answer:

- which `subject x transition` pair changed
- what the old level was
- what the new level became
- who made the decision
- which packet justified the decision
- when the change became effective
- why the change was approved, rejected, or deferred

If an update row cannot answer those questions directly:

- it is incomplete

## 13. Retention Rules

Update history retention in v0:

- append-only
- never overwrite prior rows
- never collapse multiple governance decisions into one current-state row

This means:

- later promotions do not delete earlier promotions
- demotions do not rewrite earlier approvals
- defer and reject events remain visible as governance history

Retention rule for current-state reconstruction:

- current qualification state for a `subject x transition` pair should be derived from the latest effective ledger row for that pair
- historical state should be reconstructable by replaying prior rows in timestamp order

## 14. Linkage Rules

The qualification update ledger must link outward, not absorb everything inward.

Required linkages:

- to the promotion review packet
- to the promotion decision report where used
- optionally to the candidate report

It must not duplicate:

- the full supporting event list from the outcome ledger
- the full packet body
- the full threshold calibration text

Those remain separate durable surfaces.

## 15. Example v0 Row

```json
{
  "update_id": "qualification-update-system-validator-sitrans-001-20260612",
  "timestamp": "2026-06-12T19:10:00Z",
  "qualification_subject": "system_validator",
  "transition_id": "SITRANS-001",
  "old_level": "Q1",
  "new_level": "Q2",
  "review_decision": "approve_promotion",
  "review_packet_reference": "docs/superpowers/specs/2026-06-12-first-qualification-promotion-review-decision-report-v0.md",
  "reviewer_subject": "codex_review_operator",
  "effective_timestamp": "2026-06-12T19:10:00Z",
  "decision_reason": "Candidate exceeded calibrated Q2 threshold with 14 literal native success events across 3 workflow runs.",
  "notes": "Promotion limited to deterministic source-fetch success only.",
  "threshold_version": "qualification-threshold-calibration-v0",
  "candidate_reference": "docs/superpowers/specs/2026-06-12-qualification-promotion-candidate-report-v0.md",
  "update_reason_code": "threshold_met",
  "prior_update_reference": ""
}
```

## 16. Main Result

The qualification update ledger is now required because:

- the system has already produced a real promotion candidate
- the governance layer has already made a real promotion decision
- the architecture now needs durable trust-history memory

This ledger is the missing persistence layer for:

- approved trust changes
- reviewed non-changes
- later trust-history inspection

## 17. Recommended Next Step

Use this ledger design to record the first approved update:

- `system_validator x SITRANS-001`
- `Q1 -> Q2`

That is the narrowest real case for validating:

- update-row shape
- packet linkage
- auditability of trust-state change history

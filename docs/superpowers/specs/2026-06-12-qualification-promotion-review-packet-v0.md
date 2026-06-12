# Qualification Promotion Review Packet v0

Date: 2026-06-12
Status: Draft
Scope: Minimum evidence package and decision process required to move a `subject x transition` pair from promotion candidate status to an approved qualification update

## 1. Objective

Define the first canonical review packet for qualification promotion decisions.

This artifact specifies:

- the minimum evidence package required for promotion review
- the review criteria that determine whether a candidate is approvable
- the fields that record a promotion or rejection decision
- the rejection criteria that block promotion even when a candidate exists
- the audit trail required for later inspection

This artifact preserves:

- the committed qualification architecture
- the calibrated `Q1` / `Q2` / `Q3` thresholds
- the outcome-ledger and event-capture contracts

It does not change:

- qualification thresholds
- transition semantics
- planning
- inventory
- staffing
- scheduling

## 2. Why This Artifact Exists

The repo now has:

- qualification thresholds
- observed promotion candidates

That means the system can now answer:

- which `subject x transition` pairs appear promotable

But it does not yet define:

- what exact packet a reviewer should inspect
- what fields must be recorded to approve or reject a promotion
- how to audit why a promotion decision was made

This artifact fills that governance gap.

## 3. Evidence Surface

Primary evidence surfaces for v0:

- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)
- [2026-06-11-qualification-threshold-calibration-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-qualification-threshold-calibration-report-v0.md)
- [2026-06-12-qualification-promotion-candidate-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-12-qualification-promotion-candidate-report-v0.md)

Observed v0 candidate examples:

- `system_validator x SITRANS-001`
  - live `Q2` candidate
- `system_validator x TRANS-002`
  - live `Q1` candidate
- replayed `TRANS-016`
  - operationally successful but not promotable due to `unknown_subject`

These examples anchor the packet design.

## 4. Core Rule

A promotion candidate is not a promotion.

Promotion requires:

- threshold satisfaction
- explicit packet review
- explicit decision recording

This keeps qualification state downstream of evidence governance rather than automatic count-based updates.

## 5. Minimum Review Packet

Each packet must describe exactly one:

- `qualification_subject`
- `transition_id`
- `current_level`
- `proposed_level`

And must include the following minimum sections.

### 5.1 Candidate identity

Required:

- `qualification_subject`
- `transition_id`
- `current_level`
- `proposed_level`
- `review_packet_id`
- `generated_at`

Purpose:

- identifies the exact promotion question under review

### 5.2 Threshold satisfaction summary

Required:

- `threshold_version`
- `thresholds_checked`
- `qualifying_event_count`
- `workflow_run_count`
- `meets_threshold`
- `threshold_notes`

Purpose:

- shows whether the candidate actually satisfies the calibrated promotion threshold for the proposed level

### 5.3 Evidence bundle summary

Required:

- `evidence_window`
- `native_event_count`
- `attribution_distribution`
- `inspection_outcome_distribution`
- `failure_family_distribution`
- `rescue_frequency`

Purpose:

- summarizes the shape of the supporting evidence before the reviewer inspects individual rows

### 5.4 Event references

Required:

- `supporting_event_references`
  - list of literal qualifying event references
- `excluded_event_references`
  - list of nearby rows excluded from qualification counting
- `exclusion_reasons`

Purpose:

- preserves which rows counted and which did not

### 5.5 Risk and demotion context

Required:

- `guardrails`
- `demotion_signals`
- `known_failure_clusters`
- `support_or_control_confounders`

Purpose:

- forces the review to consider whether success evidence is narrow, brittle, or misattributed to the wrong layer

### 5.6 Reviewer decision block

Required:

- `review_decision`
- `reviewer_subject`
- `reviewed_at`
- `decision_reason`

Allowed decisions in v0:

- `approve_promotion`
- `reject_promotion`
- `defer_pending_more_evidence`

Purpose:

- makes the promotion decision explicit and attributable

## 6. Review Criteria

A promotion should be approved only when all of the following are true.

### 6.1 Literal threshold criterion

The packet must show:

- the proposed level threshold is met
- using only literal event evidence for the exact `subject x transition`

If the packet depends on semantic similarity rather than literal transition evidence:

- reject or defer

### 6.2 Attribution criterion

The counted qualifying events must satisfy:

- non-`unknown_subject` attribution
- attribution quality consistent with the proposed level

Implication:

- deterministic `medium` attribution can support `Q1` and some `Q2` decisions
- human-judgment promotions should remain more conservative where only medium evidence exists

### 6.3 Inspection criterion

The counted evidence must show:

- inspection outcomes consistent with success for that transition
- no unresolved negative-inspection pattern that materially contradicts the proposed promotion

If the evidence set is dominated by rescue or negative inspected outcomes:

- reject or defer

### 6.4 Failure-context criterion

The packet must distinguish:

- execution trust problems
- support problems
- control/governance problems

If failures are clustering in support or control rather than execution:

- promotion may still be valid

If failures are clustering in execution, inspection, or requirement:

- promotion should usually be rejected or deferred

### 6.5 Stability criterion

The evidence must be stable enough for the proposed level.

That means:

- `Q1`
  - one attributable literal success may be enough
- `Q2`
  - repeated success across multiple runs must be visible
- `Q3`
  - multi-run routine stability must be visible

## 7. Rejection Criteria

The reviewer should reject promotion when any of the following hold.

### 7.1 Threshold failure

- the candidate does not actually meet the calibrated threshold

### 7.2 Attribution failure

- counted rows depend on `unknown_subject`
- or the attribution quality is lower than the threshold requires

### 7.3 Literal-evidence failure

- the packet depends on semantic analogues instead of literal transition evidence

### 7.4 Inspection contradiction

- counted rows are labeled as success, but inspection or failure patterns materially contradict that reading

### 7.5 Narrow or brittle evidence

- the evidence set is technically sufficient by count but too concentrated in one brittle scenario
- repeated unusual rescue is masking instability

### 7.6 Governance incompleteness

- `guardrails`
- `demotion_signals`
- or decision reasoning are absent or weak enough that the promotion would not be auditable

## 8. Defer Criteria

The reviewer should prefer `defer_pending_more_evidence` when:

- the candidate clearly satisfies `Q1` but is close to `Q2`
- evidence is directionally strong but one required run or event is still missing
- the candidate is operationally promising but the packet still has attribution or inspection ambiguity

Deferral is different from rejection:

- rejection means the current packet does not justify promotion
- deferral means the packet is structurally valid but still underpowered

## 9. Promotion Decision Fields

Each reviewed packet must record:

- `review_packet_id`
- `qualification_subject`
- `transition_id`
- `current_level`
- `proposed_level`
- `review_decision`
- `reviewer_subject`
- `reviewed_at`
- `decision_reason`
- `supporting_event_references`
- `excluded_event_references`
- `guardrails`
- `demotion_signals`
- `notes`

If `review_decision = approve_promotion`, also require:

- `approved_new_level`
- `effective_at`

If `review_decision = reject_promotion`, also require:

- `rejection_reason_code`

Suggested v0 rejection codes:

- `threshold_not_met`
- `attribution_insufficient`
- `literal_evidence_insufficient`
- `inspection_contradiction`
- `stability_insufficient`
- `governance_incomplete`

## 10. Audit Trail Rule

The packet must be auditable without archaeology.

That means a later reviewer should be able to answer:

- what candidate was reviewed
- what threshold was being tested
- which events counted
- which events were excluded
- who approved or rejected the promotion
- why that decision was made

If the packet cannot answer those questions directly:

- it is incomplete

## 11. Packet Shape

Each packet in v0 should be expressible with:

- `review_packet_id`
- `generated_at`
- `qualification_subject`
- `transition_id`
- `current_level`
- `proposed_level`
- `threshold_version`
- `threshold_summary`
- `evidence_bundle_summary`
- `supporting_event_references`
- `excluded_event_references`
- `guardrails`
- `demotion_signals`
- `review_decision`
- `reviewer_subject`
- `reviewed_at`
- `decision_reason`
- `rejection_reason_code`
- `notes`

## 12. How The Observed Candidates Fit

### 12.1 `system_validator x SITRANS-001`

This candidate should produce a packet that:

- proposes promotion from current baseline to `Q2`
- cites `14` qualifying native success rows across `3` runs
- records deterministic `medium` attribution as acceptable for this subject family
- explains why the single excluded `empty_source` row does not count

### 12.2 `system_validator x TRANS-002`

This candidate should produce a packet that:

- proposes promotion to `Q1`
- explicitly records that `Q2` is not yet satisfied
- uses `defer_pending_more_evidence` rather than `approve_promotion` for `Q2`

### 12.3 Replayed `TRANS-016`

This candidate should produce a packet that:

- records large operational success volume
- rejects promotion due to `attribution_insufficient`

This is important because it proves:

- a candidate can be operationally successful
- yet still non-promotable

## 13. Main Result

The qualification system now needs a review packet layer because:

- evidence thresholds are calibrated
- promotion candidates now exist
- promotion must still be explicitly governed

This packet is the minimal governance object that turns:

- candidate status

into:

- approved qualification update
- rejected promotion
- or deferred pending more evidence

## 14. Recommended Next Step

Use this packet design on the live `system_validator x SITRANS-001` candidate first.

That is the narrowest real case where the system can test:

- threshold satisfaction
- explicit review
- explicit promotion or rejection recording

without inventing broader qualification-update automation yet

# Operational Observation Report v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Run one real workflow through the current intake, control-plane, event-capture, and outcome-ledger architecture and observe what the system can actually record today.

This report shows:

- transition frequencies
- inspection outcomes
- failure distributions
- subject attribution quality
- qualification-evidence density
- which events were captured automatically
- which events still required manual reconstruction
- which architectural assumptions failed in operation

This report preserves the current architecture as the baseline.

It does not redesign any layer unless the observed behavior requires it.

## Observation Window

Observed workflow:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

Why this workflow was used:

- it is a real persisted onboarding workflow
- it contains source, candidate, binding, QA, and readiness artifacts
- it already served as the baseline control-plane validation workflow
- it exposes both successful and non-terminal unresolved outcomes

Observation surfaces used:

- `catalog/source_fetch_log.csv`
- `catalog/bindings.csv`
- `catalog/qa_queue.csv`
- `manifests/onboarding_state.json`
- `manifests/assets_manifest.json`
- `pipeline.onboarding_publish_readiness.validate_onboarding_publish(...)`

## Operational Baseline Under Test

Architecture baseline exercised in this observation:

- Source Intake
- State
- Transition
- Inspection
- Event Capture
- Outcome Ledger
- Failure Attribution
- Routing
- Qualification

Important constraint:

- transition event capture and outcome ledger are designed, but not yet implemented as automatic runtime writes in this workflow family

So this observation measures:

- what current artifacts already capture directly
- what can be normalized into ledger-grade events now
- what still requires later capture instrumentation

## Workflow Snapshot

Current observed status:

- `phase_status = bindings_pending`
- `readiness = needs_binding_review`
- `can_publish = false`

Observed counts:

- source rows fetched: `5`
- candidate rows: `362`
- binding rows: `510`
- accepted bindings: `112`
- required detection rows: `377`
- completeness findings: `265`
- binding findings: `680`
- QA rows: `925`

Relevant quality signals:

- accepted binding rows with `derived_row_reviewed_at`: `112 / 112`
- accepted binding rows marked `auto-applied recommended candidate`: `112 / 112`
- accepted binding `name_match_quality`:
  - `exact = 103`
  - `strong = 9`
- accepted binding `candidate_quality`:
  - `high = 110`
  - `medium = 2`

## Observed Transition Outcome Set

This observation does not attempt to reconstruct every possible row-level transition.

It focuses on the transition outcomes that are operationally visible enough to matter now.

### Observed event classes

| observed event class | closest transition | count | evidence source | capture quality |
| --- | --- | --- | --- | --- |
| source fetch success | `SITRANS-001` | `5` | `source_fetch_log.csv` | partial |
| accepted binding success | semantic fit to `TRANS-006` | `112` | `bindings.csv` | medium |
| publish-readiness non-terminal failure | semantic fit to `TRANS-017` | `1` | `validate_onboarding_publish(...)` plus persisted artifacts | reconstructed |

Total observed transition-outcome candidates in this observation:

- `118`

## Transition Frequencies

### 1. Source-intake progression

Observed:

- `5` source rows with `status = fetched`

Interpretation:

- the workflow cleanly reached `source_fetched`
- no population-review source failures were active in this workflow

Closest frequency summary:

- `SITRANS-001 source_declared -> source_fetched = 5`

### 2. Partial downstream success

Observed:

- `112` accepted binding rows

Interpretation:

- the workflow produced meaningful successful sub-results
- but those successes are row-level, not yet clean whole-artifact transition closures

Closest frequency summary:

- semantic `TRANS-006`-like accepted progression = `112`

### 3. Inspected non-terminal failure

Observed:

- one workflow-level publish-readiness result:
  - `needs_binding_review`

Interpretation:

- the workflow did not terminate
- it remained active and recoverable

Closest frequency summary:

- semantic `TRANS-017`-like needs-rework progression = `1`

## Inspection Outcomes

Observed inspection outcomes:

- `pass`
  - source fetch success rows
  - accepted binding rows
- `rework_required`
  - workflow-level publish-readiness result `needs_binding_review`

No observed outcomes in this workflow:

- `reject`
- `blocked`
- `archive`

Operational interpretation:

- the workflow is not failing as a hard rejection lane
- it is producing useful partial success while remaining non-terminal

## Failure Distributions

### Workflow-level distribution

Observed readiness findings:

- `population_findings = 0`
- `binding_findings = 680`
- `completeness_findings = 265`
- `structural_findings = 0`
- `provenance_findings = 0`

### QA distribution

Top QA statuses:

- `pending_review = 398`
- `needs_binding_review = 261`
- `needs_better_reference = 144`
- `accepted = 112`
- `needs_candidate_review = 10`

Top QA item types:

- `binding_candidate = 510`
- `manual_crop_required = 250`
- `missing_binding = 144`
- `duplicate_candidate_cluster = 10`
- `conflicting_binding_candidates = 10`
- `weak_name_match = 1`

### Closest failure-family interpretation

Dominant observed families:

- `inspection`
  - `needs_binding_review`
  - unresolved required rows at publish-readiness
- `support`
  - `missing_binding`
  - `needs_better_reference`
  - `manual_crop_required`
- `requirement`
  - some unresolved completeness and review-surface constraints

Operational conclusion:

- the failure distribution is concentrated in recoverable binding and completeness pressure
- not in source failure
- not in structural corruption

## Subject Attribution Quality

### Direct explicit attribution

Observed:

- `0 / 118` events with explicit subject identity recorded in the workflow artifacts

Reason:

- source fetch rows do not record actor identity
- accepted binding rows record reviewed timestamps and auto-applied notes, but not a concrete actor field
- publish-readiness result is deterministic but not persisted as a subject-attributed event row

### Deterministic or strong inferred attribution

Observed:

- `6 / 118` events can be attributed only by deterministic system ownership or strong inference:
  - `5` source fetch successes
  - `1` publish-readiness inspection outcome

These are operationally useful but still weaker than explicit actor capture.

### Weak or unknown attribution

Observed:

- `112 / 118` events remain weakly attributed

Reason:

- accepted binding rows are evidence-rich on timestamps and outcomes
- but actor identity remains absent

Operational conclusion:

- subject attribution quality is the weakest part of the observation plane

## Qualification-Evidence Density

For this observation, use three evidence-density classes:

- `strong`
  - explicit subject + timestamp + evidence reference
- `medium`
  - timestamp + evidence reference, but weak or unknown subject attribution
- `weak`
  - evidence reference exists, but timestamp or subject attribution is missing or inferred

### Density results

- `strong = 0`
- `medium = 112`
- `weak = 6`

Interpretation:

- the workflow produces enough evidence to support historical outcome reconstruction
- it does not yet produce enough strong evidence to support aggressive qualification promotion

This aligns with the qualification validation result that:

- `Q1` is defensible
- many `Q2` claims remain normative placeholders

## Automatic Capture Versus Manual Reconstruction

### Automatically available today from existing artifacts

Already persisted without new architecture work:

- source fetch success status
- accepted binding status
- accepted binding reviewed timestamps
- accepted binding review notes
- QA status and item-type distributions
- workflow-level readiness outcome

### Still requiring manual or semantic reconstruction

Not yet emitted as first-class transition event rows:

- canonical `transition_id`
- canonical `input_state`
- canonical `output_state`
- normalized `inspection_result`
- normalized `failure_family`
- subject attribution basis

Operational conclusion:

- current repo artifacts already capture many event ingredients
- but the event-capture layer is not yet writing ledger-grade rows directly

## Architectural Assumptions That Failed

### 1. Outcome history is not yet automatically emitted

Failed assumption:

- meaningful workflow outcomes are already stored as canonical transition event rows

Observed reality:

- they are still distributed across logs, CSV rows, notes, and validator outputs

### 2. Subject attribution is not implicit enough to trust

Failed assumption:

- review notes and workflow ownership are enough to support qualification-grade attribution

Observed reality:

- outcome timing is often known
- actor identity is often not

### 3. One workflow can show strong partial success without strong qualification evidence

Failed assumption:

- many accepted outcomes naturally imply strong qualification evidence

Observed reality:

- accepted outcomes exist
- but attribution and literal transition identity remain too thin for stronger trust claims

## Architectural Assumptions That Held

### 1. The control plane still describes the workflow well enough

Observed:

- source progression, partial success, and non-terminal inspection failure all map usefully into the current architecture

### 2. The evidence plane is worth building

Observed:

- current artifacts already contain enough ingredients to justify direct event capture
- the missing value is normalization and emission, not a new ontology

### 3. Append-only history remains the right rule

Observed:

- partial successes and later negative workflow-level outcomes must both remain visible
- a single final status would hide too much of the real trajectory

## Operational Verdict

The architecture can operate at an observational level, but not yet at a fully automated evidence-capture level.

What is already operationally real:

- source-intake evidence
- partial accepted outcomes
- workflow-level inspected failure outcomes
- recoverable failure distributions

What is not yet operationally real:

- automatic transition event capture
- ledger-native outcome rows
- strong subject attribution

## Bottom Line

The current architecture survives end-to-end operational observation on one real workflow.

The next bottleneck is no longer architecture design.

It is implementation of:

- transition event capture
- direct ledger emission
- stronger subject attribution at execution time

No redesign is justified from this observation pass.

# Source Fetch Instrumentation Readiness Report v0

Date: 2026-06-11
Status: draft
Owner: Codex

## Objective

Prepare the source-fetch instrumentation slice without implementing it.

This report identifies:

- the current source-fetch write path
- the safest implementation seam
- the canonical transition mapping
- the inspection and outcome mapping
- expected attribution quality
- duplicate-protection strategy
- required emitted fields
- validation targets

It stops before implementation.

## Scope

This report prepares only the source-fetch instrumentation slice.

It does not:

- implement workflow changes
- redefine control-plane or source-intake states
- redefine qualification policy
- instrument `qa_queue`
- expand into planning, inventory, routing policy, or staffing

## Evidence Surfaces

- [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- [pipeline/onboarding_publish_readiness.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/onboarding_publish_readiness.py)
- [tests/test_game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/tests/test_game_onboarding.py)
- [tests/test_onboarding_report.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/tests/test_onboarding_report.py)
- [2026-06-11-source-intake-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-source-intake-architecture-v0.md)
- [2026-06-11-transition-event-capture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-event-capture-v0.md)
- [2026-06-11-transition-outcome-ledger-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-validation-report-v0.md)
- [2026-06-11-attribution-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-validation-report-v0.md)
- [2026-06-11-remaining-instrumentation-prioritization-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-remaining-instrumentation-prioritization-report-v0.md)

## Current Write Path

## Source-fetch outcome creation

The current source-fetch outcome rows are created in:

- [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- functions:
  - `_source_log_row(...)`
  - `_source_failure_row(...)`

Current emitted outcome classes include:

- `status = fetched`
- `status = empty_source`
- `status = fetch_failed`

Current persisted fields include:

- `source_page_url`
- `source_role`
- `source_title`
- `status`
- `content_type`
- `page_type`
- for failure rows:
  - `failure_category`
  - `error`
  - `hint`

## Merge and persistence seam

The current source-fetch rows are merged in:

- `_merge_source_fetch_log_rows(...)`

They are then persisted through existing onboarding artifact writers into:

- `catalog/source_fetch_log.csv`

The main persistence paths today are:

- initial onboarding ingestion flow in `ingest_onboarding_sources(...)`
- targeted refill flow in `fill_detection_rows_from_sources(...)`

## Current limitation

The source-fetch surface already proves the fetch outcome, but it still does not emit:

- `event_id`
- `timestamp`
- `subject`
- `subject_kind`
- `subject_attribution_basis`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`

This is why source fetch still counts as partially recoverable deterministic evidence rather than direct event-grade evidence.

## Canonical Transition Mapping

## Main positive fetch outcome

For the first bounded slice, the canonical success mapping should be:

- `transition_id = SITRANS-001`
- `input_state = source_declared`
- `output_state = source_fetched`
- `inspection_result = pass`
- `failure_family = none`
- `rescue_required = false`

Reason:

- this mapping is already the committed source-intake transition for successful fetch or local presence
- the ledger validation already used this as the closest reconstructed source-fetch event
- it is explicitly earlier than population readiness

## Important non-overclaim rule

Fetch success must not be mapped to:

- `source_population_ready`

Reason:

- `source_fetched` only means the source is reachable or locally present
- it does not mean coverage, identity stability, or downstream entry-path sufficiency has been proven
- `source_population_ready` is a later inspected source-intake outcome

This is the main semantic guardrail for the slice.

## Negative fetch outcome handling

The first source-fetch slice should remain bounded to:

- fetch success
- fetch failure

For fetch failure, the source-intake architecture does not yet require a committed event mapping in this report, but the emitted fields should still be shaped to fit the current Event Capture / Outcome Ledger contracts.

Recommended first-pass failure posture:

- preserve failure-grade event fields on `fetch_failed` rows
- keep any stronger source-invalid transition mapping deferred until implementation review confirms the right control transition fit

This keeps the first slice honest and avoids overclaiming invalidation semantics too early.

## Inspection / Outcome Mapping

For the bounded first slice:

| source-fetch row status | canonical transition | input_state | output_state | inspection_result | failure_family | rescue_required |
| --- | --- | --- | --- | --- | --- | --- |
| `fetched` | `SITRANS-001` | `source_declared` | `source_fetched` | `pass` | `none` | `false` |
| `empty_source` | defer exact canonical mapping in first slice unless implementation proves it is safely representable | `source_declared` | not yet fixed | not yet fixed | not yet fixed | not yet fixed |
| `fetch_failed` | defer exact canonical mapping in first slice unless implementation proves it is safely representable | `source_declared` | not yet fixed | not yet fixed | not yet fixed | not yet fixed |

Recommended first implementation pressure:

- success rows first
- failure rows only if the implementation can represent them without inventing stronger invalidation semantics than the current source-intake architecture supports

## Expected Attribution Quality

Expected subject:

- `system_validator`

Expected attribution source:

- deterministic workflow-owned system step

Expected attribution basis:

- `deterministic_system_step`

Expected attribution quality:

- `medium`

Reason:

- attribution validation already classified source-fetch outcomes as realistic deterministic medium-attribution events
- the workflow already structurally proves system ownership
- the missing piece is explicit emission, not attribution theory

## Duplicate-Protection Strategy

The current merge path already performs semantic dedupe in `_merge_source_fetch_log_rows(...)` using:

- `source_page_url`
- `source_role`
- `status`
- `failure_category`
- `error`

This is the right starting point for the event id strategy.

Recommended duplicate rule:

- same source page
- same source role
- same outcome status
- same failure signature when present
= same semantic fetch outcome

Recommended event-id basis:

- draft root
- source page url
- source role
- outcome status
- failure signature when present

Why:

- unchanged reruns should not emit duplicate event rows
- materially different fetch outcomes for the same source should produce a new event
- the event id should be stable without depending on mutable timestamps

## Required Emitted Fields

Minimum required fields for the source-fetch slice:

- `event_id`
- `timestamp`
- `subject`
- `subject_kind`
- `subject_attribution_basis`
- `transition_id`
- `input_state`
- `output_state`
- `inspection_result`
- `failure_family`
- `rescue_required`
- `evidence_reference`

Recommended supporting fields:

- `capture_source = source_intake_adapter`
- `notes`

## Preferred Evidence Reference

The preferred evidence surface remains:

- `catalog/source_fetch_log.csv`

The evidence reference should point to the persisted fetch row or an immediately adjacent source-fetch event surface, not to a later reconstruction report.

The key requirement is:

- the event must be provable from execution-time persisted workflow state

## Safe Implementation Seam

The safest implementation seam is:

- source-fetch row construction plus merge

Concretely:

- `_source_log_row(...)`
- `_source_failure_row(...)`
- `_merge_source_fetch_log_rows(...)`

Why this seam is safer than later artifact writers:

- it is the first place where the outcome semantics are concrete
- it already has the source role, source url, and outcome classification
- it is naturally earlier than population review and publish-readiness logic

This reduces the risk of accidentally overclaiming later source-intake states.

## Validation Targets

The first implementation should be validated with narrow tests for:

1. source-fetch success emission
- `fetched` row emits:
  - `SITRANS-001`
  - `source_declared -> source_fetched`
  - `inspection_result = pass`
  - `failure_family = none`

2. deterministic attribution
- emitted rows carry:
  - `subject = system_validator`
  - `subject_kind = deterministic`
  - `subject_attribution_basis = deterministic_system_step`

3. duplicate safety
- unchanged reruns of the same fetch outcome do not duplicate event rows

4. changed outcome distinction
- if the same source later produces a materially different outcome signature, a new event can be emitted

5. non-overclaim guard
- success rows do not emit `source_population_ready`
- success rows do not emit control-plane entry transitions

6. compatibility
- emitted rows remain directly compatible with Event Capture v0 and Outcome Ledger v0 without semantic reconstruction

## Readiness Verdict

This slice is ready for bounded implementation.

Why:

- the write path is identifiable safely
- the canonical success mapping is already committed
- attribution quality is expected and already validated conceptually
- duplicate protection can extend the existing semantic merge key
- the main semantic risk is explicit and controllable

## Main Guardrail

The main guardrail for implementation is:

- fetch success must remain `source_fetched`
- it must not be promoted to `source_population_ready`

That distinction preserves the committed source-intake architecture.

## Recommended First Implementation Scope

Recommended bounded scope:

- instrument `fetched` source rows first
- include deterministic attribution and event-grade fields
- treat failure-row mapping as optional in the first pass unless the exact control semantics are made explicit during implementation review

## Non-Goals

Do not add in the first source-fetch slice:

- population-ready emission
- control-plane entry transitions
- generic `qa_queue` instrumentation
- planning, inventory, routing policy, or qualification promotion policy

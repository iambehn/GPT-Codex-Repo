# Evidence Density Improvement Report v0

Date: 2026-06-11
Status: Baseline comparison complete
Scope: Accepted binding instrumentation, publish-readiness instrumentation, source-fetch instrumentation

## 1. Objective

Measure evidence-density improvement after the first three workflow instrumentation slices by comparing:

- transition coverage
- attribution strength
- `unknown_subject` exposure
- qualification-evidence strength

against the pre-instrumentation onboarding workflow baseline.

This report does not introduce new instrumentation. It compares:

- the historical pre-instrumentation workflow observation
- the current post-instrumentation generator capability for the same workflow-family event classes

## 2. Evidence Surface

### Baseline workflow

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

### Baseline reports

- [2026-06-11-operational-observation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-operational-observation-report-v0.md)
- [2026-06-11-attribution-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-attribution-validation-report-v0.md)
- [2026-06-11-transition-outcome-ledger-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-transition-outcome-ledger-validation-report-v0.md)

### Implemented instrumentation slices

- accepted binding writes in [pipeline/derived_row_review.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/derived_row_review.py)
- publish-readiness outcomes in [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)
- source-fetch outcomes in [pipeline/game_onboarding.py](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/game_onboarding.py)

### Validation references

- [2026-06-11-instrumentation-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-instrumentation-validation-report-v0.md)
- [2026-06-11-publish-readiness-instrumentation-readiness-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-publish-readiness-instrumentation-readiness-report-v0.md)
- [2026-06-11-source-fetch-instrumentation-readiness-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-source-fetch-instrumentation-readiness-report-v0.md)

## 3. Method

Use the same observed event classes from the operational baseline:

- `112` accepted binding outcomes
- `5` successful source-fetch outcomes
- `1` publish-readiness outcome

Total comparison set:

- `118` transition-outcome events

Important constraint:

- the historical workflow artifacts predate instrumentation
- the "after" state therefore measures native emission capability of the current generators for the same event classes
- it does not claim that the old workflow artifacts were retroactively rewritten

## 4. Baseline Before Instrumentation

From the operational observation report:

- transition-outcome candidates recoverable: `118`
- direct explicit attribution: `0 / 118`
- deterministic or strong inferred attribution: `6 / 118`
- weak or unknown attribution: `112 / 118`

Qualification-evidence density:

- `strong = 0`
- `medium = 112`
- `weak = 6`

Interpretation:

- the workflow already preserved enough evidence to reconstruct outcomes
- but most accepted binding rows remained attribution-thin
- the evidence plane was structurally valid, but generator quality was weak

## 5. Post-Instrumentation Generator Capability

### Accepted binding writes

Current emitted shape:

- explicit reviewer identity when present
- deterministic `codex_structured_worker` identity for auto-applied accepted reviews
- canonical transition and inspection fields
- event identifier
- evidence reference

Validated evidence quality:

- manual accepted review: `weak -> strong`
- auto-applied accepted review: `weak -> medium`

The observed baseline workflow used auto-applied accepted reviews for all `112 / 112` accepted bindings, so the relevant after-state for that workflow family is:

- `112` accepted binding events at `medium`

### Publish-readiness outcomes

Current emitted shape:

- append-only JSONL event rows
- deterministic `system_validator` attribution
- canonical mappings:
  - `ready_to_publish -> TRANS-002 -> pass`
  - `needs_binding_review -> TRANS-017 -> rework_required`

Relevant after-state for the observed workflow family:

- `1` publish-readiness event at `medium`

### Source-fetch outcomes

Current emitted shape:

- event-grade rows directly on `catalog/source_fetch_log.csv`
- deterministic `system_validator` attribution
- canonical success mapping:
  - `SITRANS-001`
  - `source_declared -> source_fetched`
  - `inspection_result = pass`
  - `failure_family = none`

Guardrail preserved:

- `source_fetched` is not promoted to `source_population_ready`

Relevant after-state for the observed workflow family:

- `5` successful source-fetch events at `medium`

## 6. Before / After Comparison

### Transition coverage

Use "native transition coverage" to mean:

- the workflow surface emits event-grade canonical transition evidence directly
- without retrospective semantic reconstruction

| metric | before | after |
| --- | --- | --- |
| native coverage for accepted binding outcomes | `0 / 112` | `112 / 112` |
| native coverage for publish-readiness outcomes | `0 / 1` | `1 / 1` |
| native coverage for source-fetch success outcomes | `0 / 5` | `5 / 5` |
| native coverage for comparison set | `0 / 118` | `118 / 118` |

### Attribution strength

| attribution class | before | after |
| --- | --- | --- |
| strong | `0` | `0` |
| medium | `6` | `118` |
| weak or unknown | `112` | `0` |

Interpretation:

- the three implemented slices eliminate weak attribution for the measured event classes
- they do not yet produce strong attribution for this specific historical workflow family because its accepted bindings were auto-applied, not manually reviewed

### `unknown_subject` exposure

Operationally useful comparison:

- before: `112 / 118` events had weak or effectively unknown subject attribution
- after: `0 / 118` events require `unknown_subject` for the instrumented comparison set

Reason:

- accepted binding auto-apply now emits `codex_structured_worker`
- publish readiness emits `system_validator`
- source fetch emits `system_validator`

### Qualification-evidence strength

For consistency with the operational observation report, keep the same density classes:

- `strong`: explicit subject + timestamp + evidence reference
- `medium`: timestamp + evidence reference, but subject is deterministic system-owned rather than explicit human identity
- `weak`: evidence reference exists, but subject or timestamp remains missing or inferred

| qualification-evidence class | before | after |
| --- | --- | --- |
| strong | `0` | `0` |
| medium | `112` | `118` |
| weak | `6` | `0` |

Interpretation:

- current instrumentation converts the entire measured comparison set into at least medium-grade qualification evidence
- strong qualification evidence still depends on explicit human review identity, which did not occur in the baseline workflow family

## 7. Surface-by-Surface Evidence Gain

| surface | baseline condition | current emitted condition | evidence gain |
| --- | --- | --- | --- |
| accepted binding writes | review outcome recoverable, actor missing | deterministic or explicit actor plus canonical event fields | large |
| publish-readiness outcomes | readiness outcome inferable, not emitted as canonical event | append-only canonical readiness event | large |
| source-fetch outcomes | source fetch success visible, transition meaning reconstructed | canonical source-intake transition evidence on log rows | medium to large |

## 8. Main Result

The three implemented instrumentation slices materially improve evidence density.

Most important changes:

- transition coverage for the measured comparison set moves from reconstructed to native emission
- attribution quality moves from `112` weak/unknown events to `118` medium events
- `unknown_subject` dependency is eliminated for the measured event classes
- qualification evidence moves from mixed `medium + weak` to uniformly `medium`

This is the first point where the evidence plane is supported by multiple live generator surfaces across:

- human-mediated review execution
- system-mediated publish readiness
- source-intake execution

## 9. Limits

- this report measures post-instrumentation generator capability against a pre-instrumentation historical workflow family
- it does not claim that the historical artifacts themselves now contain native event rows
- the measured after-state for accepted bindings reflects the observed baseline pattern of auto-applied accepted reviews, not the stronger manual-review path
- `source_fetch` failure rows remain intentionally conservative and do not claim source invalidation

## 10. Recommended Next Decision

The evidence plane now has three implemented generator surfaces.

The next decision should be based on which path yields more value:

- instrument another workflow surface such as a narrow `qa_queue` sub-surface
- or use the improved evidence density to begin qualification calibration against newer instrumented workflows

Current recommendation:

- prefer a fresh operational observation or qualification-calibration pass before widening instrumentation scope again

# Qualification Promotion Candidate Report v0

Date: 2026-06-12
Status: Observation pass complete
Scope: Evaluate whether newer instrumented workflow runs now satisfy calibrated `Q1` or `Q2` thresholds for specific `subject x transition` records

## 1. Objective

Run an observation pass on newer instrumented workflows and determine whether any literal `subject x transition` records now satisfy the calibrated `Q1` or `Q2` promotion thresholds.

This report preserves:

- the committed qualification architecture
- the qualification-threshold calibration
- the existing event-capture and outcome-ledger contracts

It does not add:

- new instrumentation
- new ontology layers

## 2. Calibration Baseline

Thresholds under test come from:

- [2026-06-10-qualification-architecture-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-10-qualification-architecture-v0.md)
- [2026-06-11-qualification-threshold-calibration-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-11-qualification-threshold-calibration-report-v0.md)

Bounded interpretation:

- `Q1`
  - at least `1` attributable literal transition success
- `Q2`
  - at least `3` qualifying successful events
  - across at least `2` distinct workflow runs
  - with attribution at `medium` or `strong`

## 3. Observation Surface

This pass used two observation types.

### 3.1 Fresh source-intake replays

Fresh onboarding runs executed through current source-fetch instrumentation:

- [20260612T180917Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/call_of_duty/drafts/onboarding/20260612T180917Z)
- [20260612T180939Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/call_of_duty/drafts/onboarding/20260612T180939Z)
- [20260612T181004Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/marvel_rivals/drafts/onboarding/20260612T181004Z)

These runs were created from source sets replayed out of earlier onboarding source catalogs, but executed with current code.

### 3.2 Review-surface observation copies

Copied review-ready drafts reprocessed with current review-application and publish-readiness logic:

- [review_cod_a](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/_qualification_observation/drafts/review_cod_a)
- [review_cod_b](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/_qualification_observation/drafts/review_cod_b)
- [review_marvel_a](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/_qualification_observation/drafts/review_marvel_a)

These copies preserve the historical review decisions while allowing current instrumentation logic to emit present-day evidence fields.

## 4. Method

For each observed run, count only events that are:

- literal committed transitions
- attributed to a non-`unknown_subject` subject
- emitted natively by the current workflow surface
- successful according to the transition semantics

This pass counted the following success shapes:

- `system_validator + SITRANS-001 + pass`
- `system_validator + TRANS-002 + pass`
- `system_validator + TRANS-017 + rework_required`
- `codex_structured_worker + TRANS-016 + pass`
- `human_editor + TRANS-016 + pass`

Important exclusion rule:

- events replayed as `unknown_subject` remain operationally useful
- they do not count toward qualification promotion

## 5. Observed Run Results

### 5.1 Fresh source-intake runs

Observed canonical source-fetch success events:

- `20260612T180917Z`
  - `5` qualifying `SITRANS-001` success rows
- `20260612T180939Z`
  - `5` qualifying `SITRANS-001` success rows
  - `1` extra `empty_source` row with no `transition_id`; excluded
- `20260612T181004Z`
  - `4` qualifying `SITRANS-001` success rows

Total qualifying source-fetch success events:

- `14`

### 5.2 Review-surface copies

Observed accepted binding replay:

- `review_cod_a`
  - `112` accepted binding rows
  - all re-emitted as:
    - `transition_id = TRANS-016`
    - `inspection_result = pass`
    - `subject = unknown_subject`
- `review_cod_b`
  - `112` accepted binding rows
  - same attribution outcome: `unknown_subject`
- `review_marvel_a`
  - `47` accepted binding rows
  - same attribution outcome: `unknown_subject`

Observed publish-readiness events:

- `review_cod_a`
  - no canonical readiness event emitted
  - final readiness remained `needs_population_review`
- `review_cod_b`
  - `1` canonical `TRANS-002 / pass` event
- `review_marvel_a`
  - `1` canonical `TRANS-002 / pass` event

Observed `TRANS-017` events:

- none in this pass

## 6. Candidate Evaluation

### Summary Table

| subject | transition_id | qualifying events | workflow runs | threshold result | verdict |
| --- | --- | --- | --- | --- | --- |
| `system_validator` | `SITRANS-001` | `14` | `3` | `Q1` and `Q2` satisfied | promotion candidate |
| `system_validator` | `TRANS-002` | `2` | `2` | `Q1` satisfied, `Q2` not satisfied | early candidate |
| `unknown_subject` | `TRANS-016` | `271` operational rows | `3` | not promotion-eligible | blocked by attribution |

## 7. Detailed Findings

### 7.1 `system_validator x SITRANS-001`

Observed evidence:

- `14` native success events
- `3` distinct workflow runs
- all counted events emitted:
  - `transition_id = SITRANS-001`
  - `input_state = source_declared`
  - `output_state = source_fetched`
  - `inspection_result = pass`
  - `failure_family = none`
  - deterministic `system_validator` attribution

Threshold result:

- satisfies `Q1`
- satisfies `Q2`

Interpretation:

- this is the clearest current promotion candidate in the observed evidence set
- the source-intake success path now has enough literal multi-run evidence for guarded qualification promotion

### 7.2 `system_validator x TRANS-002`

Observed evidence:

- `2` native success events
- `2` distinct workflow runs
- both counted events emitted:
  - `transition_id = TRANS-002`
  - `inspection_result = pass`
  - deterministic `system_validator` attribution

Threshold result:

- satisfies `Q1`
- does not satisfy `Q2`

Reason:

- it is short by one qualifying event

Interpretation:

- publish-readiness approval is now evidence-backed enough for trial trust
- it is not yet repeated enough for guarded production trust

### 7.3 `TRANS-016` replay on accepted binding surfaces

Observed evidence:

- `271` accepted binding rows across `3` observed review-copy runs
- all rows emitted:
  - `transition_id = TRANS-016`
  - `inspection_result = pass`
- but subject replay remained:
  - `subject = unknown_subject`
  - `subject_attribution_basis = unknown_at_capture`

Threshold result:

- does not satisfy `Q1`
- does not satisfy `Q2`

Reason:

- calibration requires non-`unknown_subject` attribution for promotion

Interpretation:

- the accepted-binding instrumentation is working for native event emission
- but replaying older already-applied review files does not recover promotable subject identity
- this is an evidence-history limitation, not a generator failure in current code

## 8. Main Result

This observation pass found:

- one clear `Q2` candidate
- one `Q1`-only candidate
- one large operational evidence class that remains blocked from qualification promotion by historical attribution loss

Concrete outcome:

- `system_validator x SITRANS-001`
  - qualifies as a live `Q2` promotion candidate
- `system_validator x TRANS-002`
  - qualifies as a live `Q1` promotion candidate
- accepted-binding `TRANS-016` evidence
  - remains below `Q1` for promotion when replayed from older unattributed review payloads

## 9. Why This Matters

This is the first pass that shows the calibrated qualification thresholds can now accept or reject promotion candidates using emitted workflow evidence rather than architectural judgment alone.

The key distinction is:

- source-intake success now produces promotable deterministic evidence
- publish-readiness success now produces promotable but still thin evidence
- historical accepted-binding replays still expose the cost of missing subject identity

## 10. Limits

- this pass mixes:
  - fresh current-code source-intake runs
  - current-code replay of older review-ready draft copies
- the accepted-binding observation copies preserve historical review decisions that lacked explicit subject identity
- no `TRANS-017` candidate was observed in this pass
- this report is bounded to `Q1` / `Q2` evaluation and does not make `Q3` claims

## 11. Recommended Next Step

Do not widen instrumentation first.

Preferred next move:

- commit this report
- then run another observation pass on newly executed review workflows where accepted bindings are applied under current code from the start, so `TRANS-016` can be evaluated with promotable subject identity rather than historical replay loss

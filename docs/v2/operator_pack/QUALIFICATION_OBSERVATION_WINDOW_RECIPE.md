# Qualification Observation Window Recipe

## Purpose

This is the operator procedure for deciding when a new qualification-observation
window is ready and how to generate the next observation report without changing
architecture, thresholds, governance, or ontology.

It exists to support the active program:

- `Qualification Observation`

It does not exist to:

- redesign qualification policy
- expand instrumentation scope
- add planning, inventory, scheduling, or resource layers

## Governing Doctrine

Use this recipe under the active doctrine:

```text
Observe
↓
Measure
↓
Compare
↓
Intervene Only On Repeated Failure
```

And preserve the intervention rule:

```text
One Window
=
Signal

Two Consecutive Windows
=
Pattern

Repeated Pattern
=
Candidate Intervention
```

## Fixed Comparison Set

Every observation window compares the same metrics:

- `candidate_count`
- `promotion_conversion_rate`
- `qualification_updates`
- `evidence_density`
- `attribution_quality`
- `unknown_subject`

Do not add new comparison metrics unless repeated observation failures show that
the current set cannot explain system behavior.

## Current Baseline Artifacts

Use these artifacts as the comparison baseline until superseded by later
observation reports:

- [2026-06-13-qualification-observation-report-v1.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-qualification-observation-report-v1.md)
- [2026-06-13-qualification-observation-report-v2.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-qualification-observation-report-v2.md)

Current observation state:

- `v1 = Baseline`
- `v2 = First Delta`
- `v3 = Not Ready` until new history exists after:
  - [20260612T190811Z](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/assets/games/_qualification_observation/windows/20260612T190811Z)

## Window Readiness Checklist

Do not generate a new observation report unless at least one of the following
is true after the most recent observation window:

1. a new source-intake onboarding run exists
2. a new review-surface replay or real review workflow run exists
3. a new promotion candidate appears
4. a new promotion decision appears
5. a new qualification update record appears

If none of those are true:

- the next report is not ready
- do not create a new observation report

### Quick readiness checks

Check for new observation windows:

```bash
find assets/games/_qualification_observation/windows -maxdepth 1 -mindepth 1 -type d | sort
```

Check for newer onboarding drafts:

```bash
find assets/games/call_of_duty/drafts/onboarding -maxdepth 1 -mindepth 1 -type d | sort | tail -n 8
find assets/games/marvel_rivals/drafts/onboarding -maxdepth 1 -mindepth 1 -type d | sort | tail -n 8
```

Check for new qualification-governance artifacts:

```bash
rg -n "approve_promotion|defer_pending_more_evidence|reject_promotion|qualification-update" docs/superpowers/specs docs/v2/operator_pack/CODEX_RUN_LOG.md
```

## Window Generation Procedure

Only run this procedure when the readiness checklist passes.

### 1. Create a fresh observation window root

Use:

```text
assets/games/_qualification_observation/windows/<timestamp>/
```

Inside it, create:

- `source_manifests/`
- `drafts/`

### 2. Generate fresh source-intake observation runs

For the current onboarding workflow family:

1. reconstruct or provide source manifests
2. run:

```bash
python3 run.py --onboard-game <game> --source-manifest <manifest> --full-json
```

3. capture the emitted draft roots

Validate the resulting drafts by inspecting:

- `catalog/source_fetch_log.csv`
- `manifests/onboarding_state.json`

### 3. Generate fresh review-surface observation copies

Use original pre-observation onboarding drafts as the copy source, not prior
observation copies.

Procedure:

1. copy the original draft into the new window under `drafts/`
2. rewrite embedded absolute `draft_root` and `review_file_path` references
3. apply reviews with:

```bash
python3 run.py --apply-derived-row-review <review_dir> --accept-recommended --full-json
```

4. validate readiness with:

```bash
python3 run.py --validate-onboarding-publish <draft_root> --full-json
```

Validate the resulting copies by inspecting:

- `catalog/bindings.csv`
- `manifests/publish_readiness_events.jsonl` when present

### 4. Measure the fixed comparison set

Compute:

- `candidate_count`
- `promotion_conversion_rate`
- `qualification_updates`
- `evidence_density`
- `attribution_quality`
- `unknown_subject`

Interpretation rules:

- treat already approved candidates as resolved, not still live
- count only promotion-eligible evidence toward qualification growth
- keep operational-but-unpromotable evidence separate

### 5. Compare against prior windows

Use:

- `v1` as baseline
- `v2` as the first delta

For `v3` and later, answer:

```text
What changed since the previous observation window?
```

Do not write another snapshot-only report.

## Report Structure

The next observation report should use:

### Section 0 - Executive Delta

Show:

- `candidate_count`
- `promotion_conversion_rate`
- `qualification_updates`
- `evidence_density`
- `attribution_quality`

With:

- `↑`
- `↓`
- `→`

### Section A - Candidate Generation

Compare:

- `candidate_count`
- `candidate_frequency`
- `candidate_growth_rate`

### Section B - Promotion Decisions

Compare:

- `approved`
- `deferred`
- `rejected`
- `promotion_conversion_rate`

### Section C - Qualification Updates

Compare:

- `Q0 -> Q1`
- `Q1 -> Q2`
- `Q2 -> Q3`
- `Q3 -> Q4`

### Section D - Evidence Density

Compare:

- `strong`
- `medium`
- `weak`

### Section E - Attribution Quality

Compare:

- `strong attribution`
- `medium attribution`
- `weak attribution`
- `unknown_subject`

### Section F - Failure Review

Evaluate:

- `Candidate Starvation`
- `Candidate Flooding`
- `Promotion Stagnation`
- `Qualification Inflation`

Use only:

- `Observed`
- `Not Observed`
- `Inconclusive`

## Decision Rules After Measurement

### When to hold

Hold if:

- only one comparative interval shows a signal
- the leading constraint has not repeated
- the new window can be explained by a resolved prior candidate rather than a new failure mode

### When to escalate

Escalate to intervention analysis only if:

- the same constraint repeats across consecutive windows
- the repeated pattern survives direct comparison against the fixed metric set

Example:

```text
unknown_subject
=
unchanged again

evidence_density
=
unchanged again

qualification_updates
=
unchanged again
```

Then:

- attribution quality graduates from leading hypothesis to credible repeated constraint

## Non-Goals

This recipe does not authorize:

- architecture extension
- threshold changes
- governance redesign
- instrumentation expansion
- planning/inventory/scheduling/resource work

Those only become eligible after repeated observation failure.

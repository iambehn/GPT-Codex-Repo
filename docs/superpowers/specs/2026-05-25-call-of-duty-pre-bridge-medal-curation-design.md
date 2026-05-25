# `call_of_duty` Pre-Bridge Medal Curation Design

Date: 2026-05-25
Status: approved-design

## Summary

Add one explicit pre-bridge curation step for `call_of_duty` wiki medal sources.

The current real `call_of_duty` wiki-to-onboarding bridge works structurally, but the source bundle is polluted by contracts, intel mission prose, calling cards, weapon blueprints, logos, and map or season branding. That pollution creates a large onboarding review queue that is not a good operator target.

The chosen approach is:

- keep the raw wiki draft unchanged
- add a new curation step that writes a sibling curated wiki draft bundle
- keep the existing wiki-to-onboarding bridge focused on shape conversion
- use the curated bundle as the bridge input

This keeps the workflow auditable and isolates source triage policy from onboarding conversion.

## Objectives

- reduce obvious non-HUD medal noise before rows become onboarding targets
- preserve the raw wiki draft bundle for auditability
- keep the existing onboarding and publish workflow unchanged after the bridge boundary
- make curation decisions inspectable and reversible
- narrow the first curation slice to a multikill-medal subset

## Non-Goals

- no direct publish from wiki draft shape
- no change to `publish_onboarding_draft(...)`
- no change to the published-pack folder layout
- no change to runtime thresholds or fusion heuristics
- no attempt to fully solve all `call_of_duty` medal curation in one pass
- no multi-game generalization beyond what falls out naturally from the command shape

## Current Problem

The real bridged draft at:

- `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

is structurally valid, but source quality is poor.

Observed results:

- `medal_icon` derived rows: `265`
- rows with candidate bindings: `121`
- rows without candidates: `144`
- `manual_crop_required` QA rows: `250`

Observed noise classes:

- contract descriptions
- intel mission prose
- calling cards
- weapon blueprints
- logos
- map and season branding

This means the next blocker is no longer onboarding workflow shape. It is upstream source curation quality.

## Chosen Architecture

### Boundary

Add a new sibling-draft workflow:

1. raw wiki draft bundle
2. curated wiki draft bundle
3. existing wiki-to-onboarding bridge
4. existing onboarding review and publish flow

The bridge remains unchanged in purpose:

- it converts a wiki-style bundle into canonical onboarding-draft shape

The new curation layer takes responsibility for:

- deciding which wiki medal or event rows are plausible HUD medals worth promoting into onboarding review

### Why This Boundary

- raw scrape output stays preserved
- curation becomes independently inspectable
- bridge semantics remain focused and easier to reason about
- operator debugging becomes simpler because “source noise” and “workflow shape” stop being mixed together

## New Command Surface

### Python API

```python
curate_wiki_medal_draft(
    wiki_draft_root,
    *,
    output_path=None,
    game=None,
    profile="multikill",
    repo_root=None,
) -> dict
```

### CLI

```bash
python run.py --curate-wiki-medal-draft <wiki_draft_root> [--output-path <draft_root>] [--curation-profile multikill]
```

### Default Behavior

- infer `game` from the wiki draft bundle
- default profile is `multikill`
- write a sibling bundle under `assets/games/<game>/drafts/wiki_curated/<timestamp>/` unless `--output-path` is provided

## Input Surfaces

The curation command reads the existing stable wiki-bundle surfaces:

- `assets_manifest.json`
- `catalog/assets.csv`
- `catalog/events_or_medals.csv`
- `catalog/source_fetch_log.csv`
- `catalog/qa_queue.csv`

It should not depend on raw `.draft.yaml` files.

## Output Bundle Shape

The curated output remains a wiki-style draft bundle, not an onboarding draft.

Recommended output files:

- `assets_manifest.json`
- `catalog/assets.csv`
- `catalog/events_or_medals.csv`
- `catalog/source_fetch_log.csv`
- `catalog/qa_queue.csv`
- `catalog/curation_decisions.csv`
- `catalog/curation_summary.json`

The curated bundle must remain consumable by:

- `bridge_wiki_draft_to_onboarding(...)`

without requiring a second bridge contract.

## Curation Metadata

Each evaluated row should have a decision outcome:

- `kept`
- `dropped`

Each decision should carry a basis:

- `heuristic_keep`
- `heuristic_drop`
- `override_keep`
- `override_drop`

Dropped rows should record one explicit reason, for example:

- `contract_text`
- `intel_mission_text`
- `calling_card`
- `weapon_blueprint`
- `logo_or_branding`
- `map_or_season_label`
- `weak_medal_signal`
- `outside_profile_scope`

## Source-Of-Truth Strategy

Chosen strategy: `hybrid`

The curation layer uses:

- repo heuristics as the default filter
- optional override lists for keep or drop exceptions

Overrides should win over heuristics.

## First-Pass Policy

Chosen policy:

- `keep by default`

But the first slice is intentionally scoped to a:

- `multikill` profile

So the effective first-pass behavior is:

- keep rows that look like likely multikill medals
- drop obvious non-medal or out-of-profile rows
- allow overrides to rescue or suppress exceptions

## First Curation Profile: `multikill`

### Keep Signals

Rows are strong keep candidates when they show one or more of:

- display names with multikill vocabulary such as:
  - `double kill`
  - `triple kill`
  - `quad kill`
  - `multi kill`
  - `collateral`
  - `fury kill`
- compact badge-like names instead of sentence-like prose
- section headings that look medal-like rather than encyclopedic
- asset rows whose display names plausibly refer to streak or multikill rewards

### Drop Signals

Rows are strong drop candidates when they show one or more of:

- section headings such as:
  - `Contracts[]`
  - `Intel Missions[]`
- display names indicating non-HUD content such as:
  - `Calling Card`
  - `Weapon Blueprint`
  - `Logo`
- map, season, or franchise branding terms
- long descriptive sentence-like prose rather than badge-like labels
- rows clearly outside the first `multikill` profile

### Overrides

Optional override files may match on:

- exact `event_id`
- exact `asset_id`
- normalized display name

Two override sets are supported:

- keep overrides
- drop overrides

Overrides win over heuristics.

## Expected Artifacts

The curated bundle should:

- contain materially fewer retained rows than the raw wiki bundle
- preserve the original fetch-log context
- preserve QA rows for retained candidate assets
- expose clear kept/dropped counts and reasons

The bridge output created from the curated bundle should:

- remain structurally valid
- produce a materially smaller onboarding review queue
- contain a more realistic set of medal-derived detection rows

## Downstream Workflow

The intended operator flow becomes:

1. `python run.py --curate-wiki-medal-draft <wiki_draft_root>`
2. `python run.py --bridge-wiki-draft-to-onboarding <curated_wiki_draft_root>`
3. existing onboarding review and publish commands

No change is required to:

- `build_onboarding_draft(...)`
- `derive_game_detection_manifest(...)`
- `publish_onboarding_draft(...)`

## Tests

### 1. Curation Contract Test

Prove that:

- raw wiki bundle in
- curated sibling bundle out
- decision logs exist
- kept and dropped counts are explicit

### 2. Heuristic Classification Test

Prove that obvious noise classes drop:

- contracts
- intel mission prose
- calling cards
- weapon blueprints
- logos

And plausible multikill medal rows keep.

### 3. Override Test

Prove that:

- keep override rescues an otherwise dropped row
- drop override removes an otherwise kept row

### 4. Bridge Compatibility Test

Prove that:

- the curated bundle remains consumable by `bridge_wiki_draft_to_onboarding(...)`

### 5. End-To-End Reduction Proof

Start from a minimal wiki fixture containing both good medal rows and noisy rows.

Prove that:

- the curated bundle is smaller
- the bridged onboarding draft contains fewer medal rows than the raw-bridge version
- the retained rows align with the curation profile

## Rollout Plan

1. implement the curation command and decision logs
2. add focused tests for heuristics and overrides
3. run the curation command on the real `call_of_duty` wiki draft
4. compare raw vs curated row counts
5. bridge the curated bundle into onboarding
6. inspect whether the resulting onboarding draft is a realistic review target

## Risks

### False Drops

A strict first-pass profile may remove real medals that matter later.

Mitigation:

- keep overrides
- narrow first scope only to multikill medals
- preserve the raw wiki bundle untouched

### False Keeps

Weak heuristics may still allow non-HUD rows through.

Mitigation:

- explicit decision logs
- drop overrides
- inspect bridged onboarding counts before treating the curated bundle as successful

### Scope Drift

Curation could become a second onboarding workflow.

Mitigation:

- keep the curated output in wiki-style draft shape
- keep the bridge as the only shape-conversion step
- do not add direct publish semantics to the curated bundle

## Success Criteria

This design is successful when:

- the raw wiki draft remains unchanged
- the curated wiki bundle is inspectable and reproducible
- the bridge can consume the curated bundle without contract changes
- the resulting onboarding draft is materially smaller and less noisy
- the next review target becomes plausible for human or operator review rather than being dominated by obvious non-medal rows

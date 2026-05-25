# Call Of Duty Pre-Bridge Medal Curation

Date: 2026-05-25
Status: proposed
Scope: `call_of_duty` local-only onboarding support

## Objective

Add a pre-bridge curation step for the noisy `call_of_duty` wiki medal draft so that obvious non-HUD source material is removed before rows become onboarding targets.

The immediate goal is not to solve all medal coverage for `call_of_duty`. The immediate goal is to produce a smaller, cleaner wiki-derived source bundle that the existing wiki-to-onboarding bridge can consume without flooding onboarding review with contracts, calling cards, blueprints, logos, and map or season branding.

## Current Problem

The real wiki-to-onboarding bridge now works on the live `call_of_duty` draft:

- raw wiki draft: `assets/games/call_of_duty/drafts/wiki/20260430T015758Z`
- bridged onboarding draft: `assets/games/call_of_duty/drafts/onboarding/20260524T225117Z`

That proved the workflow gap is closed, but it also exposed a source-quality problem:

- raw wiki asset rows: `250`
- bridged derived `medal_icon` rows: `265`
- derived rows with candidate bindings: `121`
- derived rows with no candidates: `144`
- manual crop blockers: `250`

The bridged draft is structurally valid, but a large part of its derived medal surface is polluted by non-HUD material such as:

- contracts and contract descriptions
- intel mission prose
- calling cards
- weapon blueprints
- franchise logos
- map and season branding

That means the next high-value boundary is source curation, not more bridge work.

## Non-Goals

- do not change `bridge_wiki_draft_to_onboarding(...)` semantics in this slice
- do not change the onboarding draft contract
- do not publish directly from `drafts/wiki/...`
- do not attempt full `call_of_duty` medal coverage in one pass
- do not introduce new runtime thresholds or fusion rules
- do not broaden beyond `call_of_duty`

## Design Summary

Add a new pre-bridge curation command that reads a raw wiki draft and writes a sibling curated wiki bundle.

The curated wiki bundle remains draft state. It is not published truth. It becomes a cleaner input to the existing bridge.

The first curation profile is intentionally narrow:

- game: `call_of_duty`
- curation strategy: hybrid
- default retention policy: keep by default at the general policy level
- first active promotion subset: multikill medals

In practice, because the first profile is intentionally narrow, the first implementation keeps rows that look like likely multikill medals and drops obvious non-medal noise unless override rules say otherwise.

## Architecture

### New API

Add a new API surface:

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

### New CLI Surface

Add a new CLI command:

```bash
python run.py --curate-wiki-medal-draft <wiki_draft_root> [--output-path <draft_root>] [--curation-profile multikill]
```

### Workflow Placement

The intended operator flow becomes:

1. `python run.py --curate-wiki-medal-draft <raw_wiki_draft_root>`
2. `python run.py --bridge-wiki-draft-to-onboarding <curated_wiki_draft_root>`
3. `python run.py --derive-game-detection-manifest <bridged_draft_root>`
4. existing derived-row review and apply flow
5. existing publish-readiness and publish flow

### Why A Sibling Curated Wiki Bundle

This boundary is preferred over integrating curation directly into the bridge because it:

- preserves the raw wiki draft for auditability
- keeps source triage policy separate from onboarding shape conversion
- makes curation inspectable on its own
- allows raw vs curated comparisons before bridge execution

## Input Surfaces

The curation command reads the same stable wiki-bundle surfaces as the bridge:

- `assets_manifest.json`
- `catalog/assets.csv`
- `catalog/events_or_medals.csv`
- `catalog/source_fetch_log.csv`
- `catalog/qa_queue.csv`

The curation command should not depend on raw `.draft.yaml` files.

## Output Bundle Shape

The curated bundle should live under:

- `assets/games/call_of_duty/drafts/wiki_curated/<timestamp>/`

unless `--output-path` is explicitly provided.

The output should preserve the same practical wiki-bundle surfaces:

- `assets_manifest.json`
- `catalog/assets.csv`
- `catalog/events_or_medals.csv`
- `catalog/source_fetch_log.csv`
- `catalog/qa_queue.csv`

Add curation-specific audit artifacts:

- `catalog/curation_decisions.csv`
- `catalog/curation_summary.json`

## Curation Decision Contract

Each source row evaluated by curation should resolve to:

- `kept`
- `dropped`

Each decision should record a basis:

- `heuristic_keep`
- `heuristic_drop`
- `override_keep`
- `override_drop`

Each dropped row should record a short reason category such as:

- `contract_text`
- `intel_mission_text`
- `calling_card`
- `weapon_blueprint`
- `logo_or_branding`
- `map_or_season_label`
- `weak_medal_signal`
- `outside_profile`

## First-Pass Profile

### Profile Name

- `multikill`

### Purpose

Reduce the first `call_of_duty` medal-promotion slice to the rows most likely to improve highlight quality quickly.

### Why Multikill First

Multikill medals are the clearest first editorial target because they are:

- more likely to correlate with actual highlight payoff
- easier to reason about than broad victory or contract-style event families
- less ambiguous than encyclopedia-derived text rows

## Hybrid Curation Model

The first pass uses a hybrid model:

1. repo heuristic filter
2. optional override keeps
3. optional override drops

Overrides always win over heuristics.

## Heuristic Rules

### Keep Signals

Keep rows when they strongly resemble multikill medal material, for example:

- display name contains strong multikill vocabulary such as:
  - `double kill`
  - `triple kill`
  - `quad kill`
  - `multi kill`
  - `collateral`
  - `fury kill`
- row appears compact and badge-like rather than sentence-like
- paired asset naming and event naming both support a likely HUD medal interpretation

### Drop Signals

Drop rows when they strongly resemble non-medal material, for example:

- section headings such as:
  - `Contracts[]`
  - `Intel Missions[]`
- display names containing:
  - `Calling Card`
  - `Weapon Blueprint`
  - `Logo`
- obvious franchise, map, or season labels
- long sentence-style prose that reads like encyclopedia explanation instead of HUD medal naming
- rows outside the active `multikill` profile unless rescued by overrides

## Override Inputs

The first implementation may support optional override files colocated with the curated run or passed later through a narrow extension.

Override keys may include:

- exact `event_id`
- exact `asset_id`
- normalized display name

The design does not require a complicated override schema in the first slice, only a stable path for exact keep or drop exceptions.

## Data Flow

1. Load raw wiki draft surfaces.
2. Normalize row identities needed for comparison.
3. Evaluate heuristic keep/drop decision for each wiki event and asset row.
4. Apply override decisions if present.
5. Emit curated row sets.
6. Emit curation decision ledger and summary.
7. Write a sibling curated wiki bundle.
8. Allow the existing bridge to consume that bundle unchanged.

## Failure Handling

### Hard Failures

The command should fail when:

- the input wiki draft does not exist
- required CSV or JSON surfaces are missing
- required surfaces are malformed
- the game cannot be determined safely

### Soft Failures

The command should still write a curated bundle when:

- some rows are ambiguous but safely droppable
- some rows have weak evidence but remain inside keep-by-default logic
- overrides are absent

### Summary Reporting

The return payload should include at minimum:

- `ok`
- `status`
- `game`
- `profile`
- `raw_asset_count`
- `raw_event_count`
- `kept_asset_count`
- `kept_event_count`
- `dropped_asset_count`
- `dropped_event_count`
- `output_root`
- artifact paths for the curated bundle and curation ledgers

## Validation

### Contract Test

Prove that:

- a raw wiki draft goes in
- a sibling curated wiki bundle comes out
- decision logs and summary artifacts exist

### Heuristic Test

Prove that:

- contracts drop
- intel mission prose drops
- calling cards drop
- weapon blueprints drop
- logos drop
- likely multikill medal rows keep

### Override Test

Prove that:

- override keep rescues an otherwise dropped row
- override drop removes an otherwise kept row

### Bridge Compatibility Test

Prove that the curated bundle is consumable by:

- `bridge_wiki_draft_to_onboarding(...)`

without modifying bridge semantics.

### Reduction Proof

Prove that a representative fixture produces fewer onboarding targets after curation than before curation.

## Rollout Plan

1. implement the curation command and tests
2. run it on the real `call_of_duty` wiki draft
3. compare raw vs curated counts
4. bridge the curated bundle
5. inspect whether the resulting onboarding draft becomes a realistic review target

## Expected Outcome

Success for this slice means:

- raw wiki source preservation remains intact
- curated wiki output is inspectable and auditable
- the bridge consumes the curated output without workflow changes
- the first `call_of_duty` medal review queue becomes materially smaller and cleaner than the current raw-bridge result

## Open Follow-On Work

This design intentionally leaves later questions open for later slices:

- broader `call_of_duty` medal families beyond multikill
- whether curation should eventually support more than one profile
- whether curated wiki bundles should later become a standard operator surface across games
- whether some of the heuristics should later be driven by researcher packets rather than repo-only rules

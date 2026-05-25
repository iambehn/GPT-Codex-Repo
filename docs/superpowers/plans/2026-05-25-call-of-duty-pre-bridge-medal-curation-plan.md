# Call Of Duty Pre-Bridge Medal Curation Plan

Date: 2026-05-25
Status: active
Spec: `docs/superpowers/specs/2026-05-25-call-of-duty-pre-bridge-medal-curation-design.md`

## Goal

Implement a local-only pre-bridge curation step that reads a raw `call_of_duty` wiki medal draft, writes a sibling curated wiki bundle, and reduces obvious non-HUD noise before the existing wiki-to-onboarding bridge runs.

## Constraints

- do not change the onboarding draft contract
- do not change `bridge_wiki_draft_to_onboarding(...)` semantics
- keep the raw wiki draft intact
- keep the first profile narrow: `multikill`
- stay inside existing onboarding and CLI workflow families

## Work Items

### 1. Add the curation API and helpers

Implement in `pipeline/game_onboarding.py`:

- `curate_wiki_medal_draft(...)`
- output-root resolver for `drafts/wiki_curated/...`
- row loading helpers for wiki bundle CSV or JSON surfaces
- curation decision helpers
- curated bundle writer

Expected output:

- curated sibling wiki bundle
- `catalog/curation_decisions.csv`
- `catalog/curation_summary.json`

### 2. Add first-pass `multikill` curation heuristics

Implement a narrow profile that:

- keeps likely multikill medal rows
- drops obvious contracts, intel prose, calling cards, weapon blueprints, logos, and map or season branding
- records decision basis and reason category

The first slice does not need a broad profile system beyond the minimum required to support `multikill`.

### 3. Preserve bridge compatibility

Ensure the curated bundle still exposes the wiki surfaces that the bridge already reads:

- `assets_manifest.json`
- `catalog/assets.csv`
- `catalog/events_or_medals.csv`
- `catalog/source_fetch_log.csv`
- `catalog/qa_queue.csv`

The bridge should work against the curated bundle without modification to its external contract.

### 4. Add CLI routing

Add:

- `run_curate_wiki_medal_draft(...)` wrapper in `run.py`
- `--curate-wiki-medal-draft`
- `--curation-profile`
- onboarding command dispatch route in `pipeline/commands/onboarding_analysis.py`

### 5. Add focused tests

Add a test module covering:

- curated sibling bundle creation
- heuristic keeps and drops
- decision ledger generation
- bridge compatibility
- reduced onboarding noise relative to the raw fixture

Add CLI and wrapper tests alongside the existing onboarding command tests.

### 6. Run the real local proof

After tests pass:

1. run curation on the real `call_of_duty` wiki draft
2. bridge the curated output
3. compare raw vs curated counts
4. record the resulting execution truth in the operator docs if it materially changes the next target

## Validation

- `python3 -m py_compile` on touched files
- focused unit tests for the new curation surface
- existing bridge tests
- `python run.py --run-repo-quality-health`
- real local execution:
  - curate raw wiki draft
  - bridge curated wiki draft
  - inspect resulting counts

## Expected Outcome

Success means:

- the raw wiki draft remains unchanged
- the curated sibling bundle exists and is inspectable
- the bridge accepts the curated bundle
- the curated onboarding draft is materially smaller and cleaner than the raw-bridge result

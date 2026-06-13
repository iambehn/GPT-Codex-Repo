# Bounded Archetype-Routing Remediation Plan

## Goal

Implement the smallest additive archetype-routing change that improves
specificity for retained synthetic candidates by extending the existing
`_hook_archetype(...)` assignment point with bounded event-family and
retained-context cues, without changing the top-level `reject` /
`synthetic` / `natural` decision surface.

## Scope

In scope:

- main assignment extension only in:
  - `pipeline/hook_candidate_export.py`
- additive cue consumption from existing hook-row evidence:
  - `context_pre_signal_types`
  - `context_post_signal_types`
  - `metadata_summary.matched_signal_types`
  - `metadata_summary.equipment_id`
- two validated bounded routing branches only:
  - `ability_seen + equipment_visibility + equipment_id`
  - `team_wipe_seen + team_wipe_visibility`
- additive explanation fields:
  - `archetype_cue_match`
  - `archetype_rationale`
- bounded validation on the current approved `call_of_duty` and
  `marvel_rivals` export set
- focused regression coverage proving that archetype refinement does not change:
  - `hook_mode`
  - approval status
  - export eligibility

Out of scope:

- `reject` threshold changes
- `synthetic` threshold changes
- `natural` threshold changes
- second archetype pipeline or rescue pass
- replayability changes
- review-semantic changes
- selection-scope changes
- export-timing changes
- publish-readiness work
- taxonomy expansion beyond the existing archetype set

## Implementation Steps

1. Extend the main archetype assignment surface.

- update `_hook_archetype(...)` in:
  - `pipeline/hook_candidate_export.py`
- preserve it as the only assignment point
- add new inputs for bounded retained-context and metadata cues
- keep the current text-driven logic as the base path

2. Thread bounded cues into hook derivation.

- pass existing retained cues already available during hook derivation:
  - `context_pre_signal_types`
  - `context_post_signal_types`
  - `matched_signal_types`
  - `equipment_id`
- avoid adding new retrieval or scoring layers

3. Add only the validated missing branches.

- branch A:
  - `event_type == ability_seen`
  - `matched_signal_types` contains `equipment_visibility`
  - `equipment_id` present
  - assign `hook_archetype = chaos`
- branch B:
  - `event_type == team_wipe_seen`
  - `matched_signal_types` contains `team_wipe_visibility`
  - assign `hook_archetype = chaos`
- preserve current branches for:
  - `clutch`
  - `reversal`
  - `fail`
  - `comedy`
  - `flex`
  - `domination`
  - generic `chaos`

4. Preserve fallback behavior.

- if bounded cues are weak, mixed, or absent:
  - preserve the current logic
  - preserve fallback to `hook_archetype = other`
- do not force specificity

5. Add explanation fields additively.

- write:
  - `archetype_cue_match`
  - `archetype_rationale`
- keep them compact and directly derived from retained row evidence
- do not use them as a second policy surface

6. Propagate additive fields through downstream surfaces.

- ensure hook-manifest rows persist the explanation fields
- if needed, propagate them through export rows for bounded validation
- avoid changing consumer behavior beyond additive visibility

7. Add focused regression coverage.

- extend:
  - `tests/test_hook_candidate_export.py`
- cover:
  - `call_of_duty` bounded equipment branch
  - `marvel_rivals` bounded team-wipe branch
  - unchanged `hook_mode`
  - unchanged natural/reject behavior
  - preserved fallback to `other` when cues are insufficient
- extend downstream assertions in:
  - `tests/test_highlight_export_batch.py`
  only if needed to prove additive propagation of explanation fields

8. Run bounded operational validation.

- reuse the current approved bounded export set
- regenerate hook/export artifacts for:
  - `call_of_duty`
  - `marvel_rivals`
- compare before vs after on:
  - `hook_mode`
  - `old_archetype`
  - `new_archetype`
  - `archetype_cue_match`
  - `archetype_rationale`
  - `packaging_strategy`
  - unchanged approval/export status

9. Write bounded validation outputs.

- produce:
  - one bounded archetype-routing validation report
  - one structured before/after comparison ledger
- keep the validation question narrow:
  - did the bounded `other -> specific archetype` transitions happen
    without changing hook policy?

## Verification

Implementation verification:

```bash
python3 -m py_compile \
  pipeline/hook_candidate_export.py \
  tests/test_hook_candidate_export.py \
  tests/test_highlight_export_batch.py
source .venv/bin/activate && python -m unittest \
  tests.test_hook_candidate_export \
  tests.test_highlight_export_batch
```

Repo closeout:

```bash
source .venv/bin/activate && python run.py --run-repo-quality-health
```

Operational validation should confirm:

- `call_of_duty-second-bounded-clip` moves:
  - `other -> chaos`
- `marvel_rivals-transfer-kjera` moves:
  - `other -> chaos`
- `call_of_duty-bootstrap-real-cod` stays unchanged
- `marvel_rivals-transfer-absolute-cinema` stays unchanged
- `hook_mode` remains unchanged for all validated cases
- approval/export status remains unchanged for all validated cases

## Risks And Guards

- Risk: archetype refinement becomes hidden hook-policy change.
  - Guard: assert `hook_mode` does not change in tests and bounded validation.

- Risk: the new branches create a second archetype system.
  - Guard: keep all changes inside `_hook_archetype(...)` and do not add a late
    rescue pass.

- Risk: weak cues force false specificity.
  - Guard: preserve fallback to `other` when bounded cue requirements are not
    met.

- Risk: packaging strategy gets used as an archetype cue.
  - Guard: restrict cue inputs to retained row evidence only.

- Risk: branch growth turns into taxonomy creep.
  - Guard: add only the two currently validated missing branches in this slice.

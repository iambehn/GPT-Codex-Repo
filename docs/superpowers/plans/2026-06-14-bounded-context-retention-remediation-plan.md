# Bounded Context-Retention Remediation Plan

## Goal

Implement the smallest export-shape change that improves hook-layer viability
for already approved exported highlights by adding bounded, signal-aware
 context retention around the approved anchor moment.

## Scope

In scope:
- export-shape derivation for approved exported highlights only
- dual-window persistence:
  - `anchor_window`
  - `context_window`
- bounded signal-aware pre-roll and post-roll expansion
- additive export metadata fields for context-expansion auditability
- bounded validation against the currently approved `call_of_duty` and
  `marvel_rivals` export set
- focused regression coverage for anchor preservation, cap enforcement, and
  metadata persistence

Out of scope:
- replayability contract changes
- review-bridge semantic changes
- selection approval logic changes
- hook-threshold tuning
- publish-readiness work
- multi-game widening beyond the current approved bounded set

## Implementation Steps

1. Add a bounded export-context derivation helper.
- introduce one narrow helper in the existing export surface, likely near:
  - `pipeline/highlight_export_batch.py`
  - or a closely related existing export helper module if one already fits
- accept:
  - anchor start/end
  - game id
  - selected candidate context
  - nearby runtime/fused evidence already available from existing artifacts
- return:
  - `anchor_start_seconds`
  - `anchor_end_seconds`
  - `context_start_seconds`
  - `context_end_seconds`
  - `context_expansion_seconds`
  - `context_expansion_policy`
  - `context_expansion_reasons`
  - `context_signal_count`
  - `context_pre_signal_types`
  - `context_post_signal_types`

2. Keep anchor invariance explicit in implementation.
- preserve the existing approved anchor event as the canonical reviewed object
- do not mutate or reinterpret the reviewed anchor timing
- derive `context_window` separately from `anchor_window`
- ensure exported media timing uses `context_window` while metadata preserves
  both

3. Implement bounded signal-aware timing expansion.
- add fixed per-game caps:
  - `call_of_duty`
    - `1.5s` pre-roll
    - `2.0s` post-roll
  - `marvel_rivals`
    - `2.0s` pre-roll
    - `2.5s` post-roll
- bias pre-roll toward nearby setup signals:
  - entity mention
  - ability/action signal
  - preceding fused/runtime cluster
- bias post-roll toward nearby payoff signals:
  - follow-through confirmation
  - reaction/completion signal
  - adjacent clarifying second signal
- apply fallback padding when no qualifying signals exist:
  - `0.5s` pre-roll
  - `0.75s` post-roll

4. Persist the additive export metadata contract.
- extend the export artifact row shape without removing existing fields
- add:
  - `anchor_start_seconds`
  - `anchor_end_seconds`
  - `context_start_seconds`
  - `context_end_seconds`
  - `context_expansion_seconds`
  - `context_expansion_policy`
  - `context_expansion_reasons`
  - `context_signal_count`
  - `context_pre_signal_types`
  - `context_post_signal_types`
- keep existing consumers compatible by preserving current exported timing
  fields and aligning them to `context_window`

5. Keep the selection-export boundary clean.
- inspect whether `pipeline/highlight_selection_export.py` needs additive
  metadata propagation for downstream export use
- only extend that surface if required to avoid recomputing or losing anchor
  context
- do not widen selection semantics or approval semantics in this slice

6. Add focused regression coverage.
- extend the existing highlight export test surface, likely:
  - `tests/test_highlight_export_batch.py`
- cover:
  - anchor preservation
  - context window existence
  - cap enforcement
  - fallback padding on sparse signals
  - metadata persistence
  - no omission of the anchor window from exported shape
- add a regression that proves current replay-related export surfaces still work
  with the additive fields present

7. Add bounded before/after validation tooling or fixture flow.
- produce a comparison path for:
  - baseline export shape
  - context-retained export shape
- run the same downstream hook/export evaluation on both
- compare deltas on:
  - `context_sufficiency_score`
  - `title_thumbnail_potential_score`
  - `authenticity_risk_score`
  - `hook_mode`
  - `hook_archetype`
  - `packaging_strategy`
  - `rejection_reason`

8. Write bounded validation artifacts.
- produce:
  - one before/after comparison report
  - one structured comparison ledger
- keep the validation set limited to the currently approved bounded exports for:
  - `call_of_duty`
  - `marvel_rivals`

## Verification

Implementation verification:

```bash
python3 -m py_compile \
  pipeline/highlight_export_batch.py \
  pipeline/highlight_selection_export.py \
  tests/test_highlight_export_batch.py
source .venv/bin/activate && python -m unittest tests.test_highlight_export_batch
```

Bounded validation verification:

```bash
source .venv/bin/activate && python run.py --run-repo-quality-health
```

Operational validation should confirm:
- anchor set unchanged
- context windows remain inside per-game caps
- replay/export surfaces do not regress
- at least bounded metric comparison artifacts are produced successfully

## Risks And Guards

- Risk: context expansion silently becomes reselection.
  - Guard: preserve explicit `anchor_*` fields and enforce hard caps.

- Risk: export timing changes break replay or audit interpretation.
  - Guard: keep anchor invariance explicit and additive; do not alter reviewed
    object identity or replay contract objects.

- Risk: metric improvement is faked by larger windows rather than better shape.
  - Guard: compare baseline vs contextualized exports on the same approved
    anchors and flag cap-hugging behavior as failure interpretation.

- Risk: additive fields break existing export consumers.
  - Guard: preserve existing fields and extend the row shape compatibly.

- Risk: this slice drifts into hook-threshold tuning or packaging-policy work.
  - Guard: treat unchanged hook thresholds as a hard non-goal and stop at
    before/after evaluation.

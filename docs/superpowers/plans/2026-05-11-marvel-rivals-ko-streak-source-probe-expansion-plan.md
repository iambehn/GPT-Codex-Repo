## Goal

Improve local KO-streak confirmation recall by expanding exporter probe centers around mined timestamps for source clips only, while preserving the existing published KO-streak detection contract.

## Scope

In scope:

- `tools/marvel_rivals_ko_streak_exporter.py`
- additive exporter `summary.json` metadata
- bounded rerun against the current deduped miner summary

Out of scope:

- miner ranking changes
- threshold or temporal-window changes
- ROI changes
- runtime/fusion schema changes
- broader bridge-artifact probing

## Implementation Steps

### 1. Add source-only offset schedule configuration

Extend `tools/marvel_rivals_ko_streak_exporter.py` with a source-only offset-center schedule.

Concrete changes:

- add a new CLI argument:
  - `--source-offset-seconds`
  - default values:
    - `0.0`
    - `-0.5`
    - `0.5`
    - `-1.0`
    - `1.0`
- normalize and store the parsed offset list in config output

Behavior:

- only clips classified as `source` receive this expanded offset schedule
- non-source clip kinds continue to probe only the original mined seed center in this slice

### 2. Carry source-artifact metadata into exporter grouping

The exporter currently consumes the miner summary but should now respect:

- `source_artifact_kind`
- `source_family_key`

Concrete changes:

- include the miner’s source-artifact fields in the seed-group data structure
- preserve them in clip-level summary output

This keeps the source-only branching explicit and auditable.

### 3. Expand one seed into multiple offset-center probes for source clips

Keep `_probe_clip(...)` as the per-center probe primitive. Change the caller so that one source seed can produce multiple probe attempts at nearby centers.

Concrete changes:

- for each source seed:
  - generate derived probe rows centered at:
    - seed timestamp + each configured offset
  - clamp negative timestamps to `0.0`
  - call `_probe_clip(...)` once per offset center
- for non-source seeds:
  - call `_probe_clip(...)` once at the original center only

Expected result:

- source clips get broader local temporal coverage without changing confirmation rules

### 4. Extend clip-level winner resolution to include offset attempts

The exporter already resolves one winning probe per clip across seed rows. Extend that logic so offset attempts for a given seed are also folded into the same winner selection.

Rules stay the same:

1. highest `peak_score`
2. highest `supporting_frames`
3. earliest `first_timestamp`
4. deterministic event id tie-break

Expected result:

- still only one exported proof package per confirmed clip
- wider temporal coverage can improve recall without creating duplicate exports

### 5. Add additive summary metadata

Extend exporter `summary.json` with:

- `source_offset_probe_enabled: true`
- `source_offset_schedule_seconds`
- `offset_probe_attempt_count`

Per-attempt metadata should include:

- `offset_seconds`
- `probe_center_timestamp_seconds`
- `confirmed`

Per-clip summary should also report:

- how many offset attempts were made
- whether the winning confirmation came from an offset-center probe different from the original mined timestamp

### 6. Preserve downstream artifact contracts

Do not change:

- proof segment generation
- runtime sidecar generation
- fused sidecar generation
- clip-level export dedupe behavior

Expected result:

- downstream lifecycle and registry paths remain unchanged

## Verification Plan

### A. Static verification

Run:

```bash
python3 -m py_compile tools/marvel_rivals_ko_streak_exporter.py
```

### B. Operational verification

Run the updated exporter against the current deduped miner summary:

```bash
source .venv/bin/activate
python3 tools/marvel_rivals_ko_streak_exporter.py \
  --summary-path /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_analysis/marvel_rivals/medal_candidate_miner_20260510T220055Z/summary.json \
  --top-unique-clips 20 \
  --max-seeds-per-clip 3
```

Verify:

1. summary contains:
   - `source_offset_probe_enabled`
   - `source_offset_schedule_seconds`
   - `offset_probe_attempt_count`
2. source clips show multiple offset attempts
3. non-source clips do not receive expanded offset probing
4. exporter still emits at most one proof package per confirmed clip
5. compare `confirmed_unique_clip_count` against the previous deduped exporter run

### C. Interpretation

If confirmation count increases:

- treat it as real recall improvement under the unchanged published contract

If confirmation count does not increase:

- treat the result as evidence that source-timestamp drift is not the main remaining bottleneck
- use that result to justify shifting to another recall lever later

## Risks And Mitigations

### Risk: exporter runtime grows too much

Mitigation:

- source clips only
- small fixed offset schedule
- preserve bounded `top_unique_clips`

### Risk: multiple near-identical confirmations per clip

Mitigation:

- keep existing clip-level winner selection as final arbiter

### Risk: offset probing obscures auditability

Mitigation:

- record offset values and probe centers explicitly in summary metadata

## Done Criteria

This slice is done when:

- source-only offset probing is implemented
- additive summary metadata is present
- exporter still emits one clip-level winner at most
- bounded verification has been run
- published runtime detection contracts remain unchanged

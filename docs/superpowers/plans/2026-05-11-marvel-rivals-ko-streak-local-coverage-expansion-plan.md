## Goal

Expand local Marvel Rivals KO-streak confirmation coverage by allowing the exporter to probe multiple mined seeds per clip, while preserving the current published detection contract.

## Scope

In scope:

- `tools/marvel_rivals_ko_streak_exporter.py`
- additive exporter `summary.json` metadata
- bounded operational verification against an existing miner summary

Out of scope:

- `tools/marvel_rivals_medal_candidate_miner.py`
- published template thresholds
- ROI changes
- runtime or fusion contract changes
- `run.py` integration

## Implementation Steps

### 1. Extend seed selection from one row per clip to N rows per clip

Update `tools/marvel_rivals_ko_streak_exporter.py` so seed loading groups miner results by clip and retains multiple ranked rows per clip.

Concrete changes:

- add a new CLI argument:
  - `--max-seeds-per-clip`
  - default: `3`
- replace `_load_seed_rows(...)` with a clip-grouped selector that:
  - reads `results` from the miner summary
  - groups rows by `clip_name`
  - preserves miner ordering within each clip
  - keeps up to `max_seeds_per_clip` rows per clip
  - still respects `top_unique_clips` as the number of clip groups selected

Expected result:

- exporter receives a clip-to-seeds mapping instead of a flat one-seed-per-clip list

### 2. Probe all retained seeds for each clip

Keep `_probe_clip(...)` unchanged as the per-seed probe primitive. Change the caller to invoke it once per retained seed for a clip.

Concrete changes:

- for each selected clip:
  - iterate over that clip’s retained seed rows
  - call `_probe_clip(...)` for each seed row
  - collect all per-seed probe results

Expected result:

- exporter can recover confirmations missed by the first seed row

### 3. Add clip-level winner resolution

After probing all seeds for a clip, choose at most one exported winner for that clip.

Concrete changes:

- add a helper such as `_best_confirmed_probe(...)`
- consider only seed probes with at least one confirmed candidate
- rank by:
  1. highest confirmed candidate `peak_score`
  2. highest `supporting_frames`
  3. earliest `first_timestamp`
  4. deterministic asset/event id tie-break
- export only the clip-level winner

Expected result:

- one proof package per confirmed clip
- no duplicate clip exports from multiple confirming seeds

### 4. Add additive exporter summary metadata

Update exporter `summary.json` to expose the wider coverage behavior clearly.

Top-level additions:

- `max_seeds_per_clip`
- `seed_rows_considered`
- `clip_probe_attempt_count`
- `confirmed_probe_count`
- `confirmed_unique_clip_count`

Per-clip additions:

- attempted seed timestamps
- attempted seed template ids or asset ids
- whether each seed confirmed
- which seed index / timestamp produced the exported winner when applicable

Expected result:

- operator can tell whether the widened coverage actually exercised more seeds

### 5. Preserve existing export artifacts and downstream compatibility

Do not change:

- proof segment generation
- runtime sidecar generation
- fused sidecar generation
- exported artifact directory layout

Expected result:

- downstream lifecycle and registry flows continue to work without modification

## Verification Plan

### A. Static verification

Run:

```bash
python3 -m py_compile tools/marvel_rivals_ko_streak_exporter.py
```

### B. Operational verification

Run the updated exporter against an existing miner summary that already contains multiple rows per clip:

```bash
source .venv/bin/activate
python3 tools/marvel_rivals_ko_streak_exporter.py \
  --summary-path /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_analysis/marvel_rivals/medal_candidate_miner_20260510T203313Z/summary.json \
  --top-unique-clips 12 \
  --max-seeds-per-clip 3
```

Verify:

1. `summary.json` includes the new top-level fields
2. at least one clip shows more than one attempted seed
3. no clip produces more than one exported proof package
4. `confirmed_unique_clip_count` is reported explicitly
5. existing runtime/fused sidecars are still produced for confirmed clips

### C. Compare against previous behavior

Compare the new exporter summary against the previous one-seed-per-clip run.

Success criteria:

- coverage metadata proves multiple seeds were attempted for at least one clip
- exporter remains stable
- if confirmed clip count increases, treat that as a real gain
- if confirmed clip count stays the same, treat the slice as validated evidence recovery rather than a failed change

## Risks And Mitigations

### Risk: exporter runtime grows too much

Mitigation:

- keep default `max_seeds_per_clip=3`
- keep `top_unique_clips` bounded
- keep current short probe windows

### Risk: duplicate or noisy exports from one clip

Mitigation:

- force one clip-level winner
- keep ranking deterministic

### Risk: summary becomes ambiguous

Mitigation:

- separate attempted seeds, confirmed probes, and exported winners explicitly

## Done Criteria

This slice is done when:

- exporter supports multiple retained seeds per clip
- only one exported winner can be produced per clip
- additive summary metadata is present
- bounded operational verification has been run
- no published detection contracts were changed

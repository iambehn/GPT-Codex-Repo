## Summary

Expand local Marvel Rivals KO-streak evidence discovery by increasing exporter coverage per clip without changing the published runtime detection contract.

The current published KO-streak prompt path is technically valid and already yields one confirmed local `5_player_ko_streak` candidate. The next step is not threshold relaxation. The next step is broader, denser probing around mined local timestamps so the existing contract can recover more confirmed local clips if they exist.

## Current State

The current one-off operator flow has two stages:

1. `tools/marvel_rivals_medal_candidate_miner.py`
   - mines accepted and rejected local clips
   - ranks likely KO-streak prompt frames
   - retains a bounded number of top rows

2. `tools/marvel_rivals_ko_streak_exporter.py`
   - loads a miner `summary.json`
   - selects only one seed row per unique clip
   - probes a short window around that seed
   - confirms candidates only when the current published template contract is satisfied
   - exports proof segments plus runtime and fused sidecars for confirmed clips

This exporter shape is conservative and correct, but it leaves coverage on the table:

- a clip can contain multiple strong mined timestamps
- only the first mined timestamp per clip is currently considered
- if that first timestamp misses the true prompt timing, the clip is lost even when another mined row for the same clip would have confirmed

## Goal

Increase the number of confirmed local KO-streak clips by widening per-clip probe coverage while preserving all current detection and confirmation rules.

Success means:

- more local clips can be confirmed under the existing published `5_player_ko_streak` and `10_player_ko_streak` contract
- no threshold, ROI, temporal window, or fusion policy changes are required
- proof segments and downstream runtime/fused artifacts remain auditable and bounded

## Non-Goals

This slice does not:

- relax published KO-streak thresholds
- change the `medal_area` ROI
- add new KO-streak ontology rows
- change runtime or fusion scoring behavior
- replace the miner ranking model
- turn the one-off operator tools into durable CLI product surfaces under `run.py`

## Approach Options

### Option 1: Wider miner only

Scan more clips and keep the exporter unchanged.

Pros:
- smallest implementation

Cons:
- still limited to one seed per clip
- wastes mined evidence when a clip has multiple plausible timestamps

### Option 2: Wider corpus plus denser per-clip seed coverage

Keep the published contract unchanged, but let the exporter evaluate multiple mined seeds per clip before deciding whether the clip confirms.

Pros:
- best chance of recovering additional positives without changing scoring contracts
- directly targets the current missed-coverage failure mode

Cons:
- slightly higher exporter runtime
- requires clip-local dedupe so the same clip is not exported repeatedly

### Option 3: Contract relaxation

Lower thresholds or temporal windows again.

Pros:
- fastest path to more positives

Cons:
- highest false-positive risk
- weakens the published runtime contract instead of improving evidence recovery

## Decision

Adopt Option 2.

The exporter will probe multiple mined seeds per unique clip, under the exact same published KO-streak contract, and export at most one best confirmed candidate per clip.

## Proposed Design

### Miner

`tools/marvel_rivals_medal_candidate_miner.py` remains functionally unchanged in this slice.

It already emits enough ranked rows per clip to support denser exporter probing. No miner contract change is required.

### Exporter

`tools/marvel_rivals_ko_streak_exporter.py` will change in three ways.

#### 1. Multiple seeds per clip

Instead of selecting exactly one seed row per unique clip from the miner summary, the exporter will:

- group rows by `clip_name`
- retain up to `N` top seeds per clip
- probe each seed independently

Default:
- `max_seeds_per_clip = 3`

This default is intentionally small. The goal is better recovery, not unbounded expansion.

#### 2. Clip-local confirmed winner selection

After probing multiple seeds for one clip, the exporter will:

- collect all confirmed probe results for that clip
- choose one best confirmed candidate
- export only that one clip-level winner

Ranking order:

1. highest `peak_score`
2. highest `supporting_frames`
3. earliest `first_timestamp`
4. deterministic asset/event id tie-break

This keeps one exported proof package per clip and avoids artifact clutter.

#### 3. Summary visibility

The exporter `summary.json` will explicitly report:

- `max_seeds_per_clip`
- `seed_rows_considered`
- `clip_probe_attempt_count`
- `confirmed_probe_count`
- `confirmed_unique_clip_count`
- `export_count`

Per-clip rows will also record:

- which mined seed timestamps were attempted
- which seed produced the confirmed winner when applicable

## Output Contract

Existing artifact types remain unchanged:

- proof segments
- runtime sidecars
- fused sidecars
- top-level exporter `summary.json`

The only contract change is additive metadata in the exporter summary. Existing downstream consumers should continue to work because:

- exported runtime/fused artifacts keep the same schema
- one exported proof package still maps to one confirmed clip

## Error Handling

Per-seed probe failures must not abort the clip or the whole run.

Rules:

- failed seed probes are recorded in the clip summary
- a clip only fails completely if all attempted seeds fail to probe cleanly
- no export occurs for a clip unless at least one seed confirms under the current published contract

## Verification

Verification for this slice will be operational and bounded:

1. run the updated exporter against the latest KO-streak miner summary
2. verify it attempts multiple seeds for at least one clip
3. verify the summary reports the new seed-attempt metadata
4. verify exported proof packages remain at most one per confirmed clip
5. compare `confirmed_unique_clip_count` against the previous exporter behavior on the same miner summary

If the count does not improve, that is still a valid result. The value of the change is that missed confirmations from one-seed-per-clip bias are eliminated.

## Risks

### Runtime inflation

More seeds per clip means longer exporter runtime.

Mitigation:
- keep a small default `max_seeds_per_clip`
- preserve bounded `top_unique_clips`
- preserve short probe windows

### Duplicate exports from one clip

Multiple confirming seeds could create duplicate artifacts.

Mitigation:
- choose one best confirmed candidate per clip
- export only the clip-level winner

### Misleading improvement claims

The widened coverage may still produce the same single confirmed clip.

Mitigation:
- treat the result as evidence recovery, not guaranteed recall improvement
- report confirmed unique clips explicitly in the summary

## Resulting Operator Posture

After this change, the KO-streak local evidence workflow becomes:

1. run the miner
2. run the exporter on the miner summary
3. let the exporter probe multiple ranked timestamps per clip
4. export only clip-level confirmed winners
5. use the resulting runtime/fused artifacts for lifecycle advancement and downstream registry refresh

This is the smallest reliable coverage expansion that preserves the current published runtime contract.

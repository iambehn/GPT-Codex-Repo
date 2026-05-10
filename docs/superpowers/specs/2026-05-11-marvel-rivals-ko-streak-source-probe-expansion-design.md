## Summary

Expand local KO-streak confirmation recall by widening exporter probe coverage around mined timestamps for source clips, without changing the published KO-streak detection contract.

The miner now produces cleaner source-family representatives. The next bottleneck is that most deduped source clips reach the exporter with only one mined seed row. If that one timestamp is slightly offset from the real on-screen prompt, the clip fails confirmation even when the prompt exists nearby.

## Problem

Current exporter behavior:

- probe a narrow symmetric window around each mined seed timestamp
- sample at a fixed cadence
- require the existing published threshold and temporal-window contract

This is correct, but too brittle for short-lived center-top prompt surfaces:

- a mined timestamp can be slightly early or late
- a prompt can persist just outside the current small probe window
- a fixed cadence can miss a prompt transition even when the clip is otherwise a strong candidate

The evidence from current deduped clip summaries shows that many source clips now have:

- exactly one attempted seed
- no confirmation

That means the next recall gain should come from better local probing around that source seed, not from changing thresholds.

## Goal

Increase the chance that a real KO-streak prompt near a mined source timestamp is confirmed under the existing published contract.

Success means:

- more source clips can confirm without any threshold relaxation
- published runtime and fusion contracts remain unchanged
- exporter summaries remain auditable and bounded

## Non-Goals

This slice does not:

- lower KO-streak thresholds
- change `temporal_window`
- change `medal_area`
- add new KO-streak assets
- change miner ranking
- reintroduce duplicate bridge artifacts into the retained miner summary

## Approach Options

### Option 1: Larger symmetric probe window only

Increase the current per-seed probe window from the current narrow range to a wider symmetric range.

Pros:
- smallest implementation

Cons:
- still assumes the mined timestamp is centered correctly
- can waste work probing equally on both sides when prompt timing is skewed

### Option 2: Multi-offset source probe schedule

For source clips, probe several nearby time centers around the mined timestamp using a bounded offset schedule, while keeping the current confirmation contract unchanged.

Pros:
- directly addresses timestamp offset error
- keeps runtime contract unchanged
- best expected recall gain for the current failure mode

Cons:
- slightly more exporter runtime
- requires clip-level dedupe of offset attempts

### Option 3: Higher probe sample FPS

Increase `sample_fps` globally for all exporter probes.

Pros:
- better temporal resolution

Cons:
- higher cost everywhere
- does not fix center-timestamp offset by itself

## Decision

Adopt Option 2.

For source clips, the exporter will probe a bounded set of nearby offset centers around the mined timestamp and keep one best confirmed result per clip. The underlying threshold and temporal-window contract stays unchanged.

## Proposed Design

### Source-only offset expansion

Apply the wider probe policy only to `source` clips first.

Do not apply it to:

- `proxy_review`
- `fused_review`
- `unknown`

Reason:

- the source-family dedupe already prefers source clips
- source clips are the main target for unique local evidence expansion
- bridge-derived artifacts should not absorb more probe budget in this slice

### Offset schedule

For each mined source seed, probe multiple nearby centers rather than only the exact mined timestamp.

Initial offset schedule:

- `0.0`
- `-0.5`
- `+0.5`
- `-1.0`
- `+1.0`

All offsets are in seconds relative to the mined seed timestamp.

Each offset center still uses the existing bounded per-center probe window. This means the change expands probe coverage by moving the center, not by weakening confirmation.

### Clip-level winner selection

The exporter already chooses one confirmed winner per clip across multiple seed rows. Extend that same logic so it also chooses one winner across multiple offset-center probe attempts.

Ranking order remains:

1. highest `peak_score`
2. highest `supporting_frames`
3. earliest `first_timestamp`
4. deterministic event id tie-break

### Summary visibility

Add additive exporter summary metadata:

- `source_offset_probe_enabled: true`
- `source_offset_schedule_seconds`
- `offset_probe_attempt_count`

Per-clip attempted seed metadata should also include:

- `offset_seconds`
- whether that offset attempt confirmed

This keeps the recall-expansion behavior inspectable.

## Output Contract

Existing exported artifacts remain unchanged:

- proof segments
- runtime sidecars
- fused sidecars
- top-level `summary.json`

Contract changes are additive summary metadata only.

## Error Handling

Offset attempts are independent.

Rules:

- a failed offset probe does not fail the seed
- a failed seed does not fail the clip if another offset or seed confirms
- one clip still produces at most one exported proof package

## Verification

Run the updated exporter against the current deduped miner summary.

Verify:

1. summary exposes source-offset metadata
2. source clips show multiple offset attempts even when they have only one seed row
3. non-source clips do not receive expanded offset probing in this slice
4. exporter still emits at most one export per confirmed clip
5. compare `confirmed_unique_clip_count` to the pre-change deduped exporter run

If the count does not improve, that still establishes whether source-timestamp drift is or is not the next real bottleneck.

## Risks

### Risk: runtime inflation

Mitigation:

- source clips only
- small fixed offset schedule
- keep the existing bounded probe window

### Risk: duplicate confirmations inside one clip

Mitigation:

- existing clip-level winner selection remains the final arbiter

### Risk: false confidence from wider temporal search

Mitigation:

- confirmation still requires the same published threshold and temporal-window contract
- only the probe centers change

## Resulting Operator Posture

After this change:

1. miner yields deduped source-family representatives
2. exporter probes source seeds at several nearby offset centers
3. exporter keeps one best confirmed clip-level winner
4. confirmed local KO-streak evidence can expand without weakening the published runtime contract

This is the smallest reliable next step for recall after source-family dedupe.

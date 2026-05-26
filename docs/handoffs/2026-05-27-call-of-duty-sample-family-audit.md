# Call Of Duty Sample Family Audit

Date: 2026-05-27
Status: active

## Objective

Reduce one of the remaining medal-packet uncertainties:

- are the current `call_of_duty` samples title-consistent enough to justify a `BOCW`-first medal seed, or are they a mixed family that needs a more careful source strategy

## Samples Inspected

1. `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
2. `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
3. `outputs/public_gameplay_mining/call_of_duty_measurement_sources/v-SzAArdAfY.60s-70s.mp4`
4. `outputs/public_gameplay_mining/call_of_duty_measurement_sources/Qop1sH70nHI.60s-70s.mp4`

Representative extracted frames live under:

- `outputs/measurement/call_of_duty/frame_audit/`

## Findings

### 1. The sample set is mixed, not title-pure

Observed frame characteristics:

- `SVbTc2AZzYw-60s-70s`
  - parachute drop over a large urban Warzone-style map
  - not a clean multiplayer medal-popup sample
- `Qop1sH70nHI-60s-70s`
  - indoor Warzone-style looting and contract HUD
  - clearly not a tight medal-popup validation sample
- `v-SzAArdAfY-60s-70s`
  - scavenger contract and Warzone-style HUD
  - again Warzone-family, not clean multiplayer medal validation
- `_PL_5qWwKtY`
  - compact multiplayer-style kill sequence with a visible `UAV` reward banner and `4TH KILL`
  - the strongest current sample for medal-family reasoning

### 2. `BOCW` is a usable research seed, but not yet a proven title match

The filled medal packet recommends `Black Ops Cold War` medals because that source slice cleanly contains:

- multikill medals
- streak medals
- payoff or victory medals

That is still a reasonable seed for a first curated medal family, but the current sample set does **not** prove that `BOCW` is the correct title-specific visual match for the measured clips.

### 3. The first validation loop should prefer family fit over title perfection

Because the current blocker is total lack of medal coverage, the next practical question is:

- can a curated medal subset produce any real `medal_visibility` evidence on the current samples

That means the next source packet should still focus on:

- true gameplay HUD medal icons
- multikill and streak medals first

But it should explicitly note that title matching remains provisional until live visual comparison is done.

## Practical Implication

Do not over-commit to `BOCW` as a final title-truth claim yet.

Use this more precise rule instead:

- `BOCW` medal pages are an acceptable first strong-candidate seed because they provide a compact multikill/streak/victory set
- the first implementation loop should treat them as onboarding candidates, not final detector truth
- after import, validate against the current sample set with visual debug output
- if the medal shapes do not visually align, pivot to a different title-specific medal family without changing the overall bridge/onboarding workflow

## Recommended Next Step

When the researcher packet is finalized, carry forward this refinement:

- keep the `BOCW` seed recommendation as `strong_candidate`, not `authoritative`
- require visual comparison against the current local samples before treating the title match as settled

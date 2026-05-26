# Call Of Duty Medal Packet Promotion Results

Date: 2026-05-27
Status: active

## Objective

Use the filled `2026-05-26-call-of-duty-medal-packet` to promote a first narrow `call_of_duty` medal slice into the published pack, then rerun the existing four-sample measurement set.

## Packet-Derived Source Slice

Operational source bundle created:

- `assets/games/call_of_duty/drafts/wiki_curated/20260526T233955Z`

Bundle shape:

- existing wiki-draft contract
- `15` medal rows
- `15` source fetch rows
- `0` manual-crop blockers

Source strategy used:

- `BOCW` file-page assets for streak and payoff medals where the file page exposed a real `static.wikia` image URL
- `BO3` category-image assets as a fallback for multikill medals because the current fetch path did not expose direct `BOCW` multikill image URLs

Promoted medal names:

- multikill:
  - `Double Kill`
  - `Triple Kill`
  - `Fury Kill`
  - `Frenzy Kill`
  - `Mega Kill`
  - `Ultra Kill`
  - `Kill Chain`
- elimination-streak:
  - `Bloodthirsty`
  - `Merciless`
  - `Ruthless`
  - `Relentless`
  - `Brutal`
  - `Nuclear`
  - `Unstoppable`
- payoff or victory:
  - `Victor`

## Onboarding And Publish Results

Bridge result:

- input bundle: `assets/games/call_of_duty/drafts/wiki_curated/20260526T233955Z`
- onboarding draft: `assets/games/call_of_duty/drafts/onboarding/20260526T234014Z`
- bridge status: `bindings_pending`
- `wiki_medal_candidates: 15`

Derived-row review result:

- `15` medal rows prepared
- all `15` rows were `auto_accept_eligible`
- `python run.py --apply-derived-row-review ... --accept-recommended`
  - `applied_count: 15`

Publish-readiness result:

- readiness: `ready_to_publish`
- `accepted_bindings: 127`
- `unresolved_required_count: 0`

Publish result:

- published pack status: `ok`
- published template count: `127`
- published medal count: `15`
- published event count: `15`
- published runtime rule count: `3`
- published fusion rule count: `4`

The previous structural blocker is resolved:

- the packet can be turned into a repo-native curated source bundle
- the curated source bundle can be bridged into onboarding
- the resulting draft can be reviewed and published through the standard workflow

## Post-Promotion Measurement

Fresh measurement root:

- `outputs/measurement/call_of_duty_post_medals/`

Reused sample set:

1. `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
2. `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
3. `outputs/public_gameplay_mining/call_of_duty_measurement_sources/v-SzAArdAfY.60s-70s.mp4`
4. `outputs/public_gameplay_mining/call_of_duty_measurement_sources/Qop1sH70nHI.60s-70s.mp4`

Commands reused per sample:

```bash
python run.py --analyze-roi-runtime <SOURCE> call_of_duty --sample-fps 1 --limit-frames 30 --output-path <RUNTIME_JSON>
python run.py --fuse-clip-signals <SOURCE> call_of_duty --runtime-sidecar <RUNTIME_JSON> --output-path <FUSED_JSON>
python run.py --export-highlight-selection --fused-sidecar <FUSED_JSON> --output-path <SELECTION_JSON>
python run.py --refresh-clip-registry <ROOT> --registry-path <REGISTRY_SQLITE>
python run.py --derive-hook-candidates <FUSED_JSON> --registry-path <REGISTRY_SQLITE> --output-path <HOOK_JSON>
```

Measured result:

- `2 / 4` samples still produced no runtime events
- `2 / 4` samples still produced only `equipment_icon` runtime events
- `0 / 4` samples produced `medal_icon` runtime events
- `0 / 4` samples produced hook candidates

No medal-driven improvement was observed on this sample set after promotion.

## Frame-Level Interpretation

Frame probes now answer one of the remaining practical questions.

Observed on `_PL_5qWwKtY`:

- a `UAV` reward banner is clearly visible around `11.0s`
- yellow multikill text such as `DOUBLE KI` is visible around `20.5s`
- kill-counter text such as `7TH KO` is visible in the same sequence

Observed on `SVbTc2AZzYw.60s-70s`:

- early frames are parachute-drop Warzone footage with no visible medal badge area

Interpretation:

- the current promoted `medal_icon` slice is structurally valid
- the current measured sample set does not show clear evidence that native medal badge art is present in the expected ROI
- at least one multiplayer-style sample appears to expose text or banner signals more clearly than icon badges

## Current Blocker

The main blocker has moved again.

It is no longer:

- raw wiki source noise
- onboarding draft shape
- bridge mechanics
- derived-row review mechanics
- publish-readiness mechanics

It is now:

- clip-side visual mismatch between the promoted `medal_icon` assumption and the actual visible signals in the measured sample set

## Recommended Next Step

Do not spend the next slice promoting more medal icons blindly.

The next high-value input should answer one of these paths explicitly:

1. gather a `call_of_duty` sample set that visibly contains native medal badge icons in the HUD ROI expected by the pack
2. produce a new researcher packet for `call_of_duty` text or banner-based multikill and streak signals, for example:
   - `DOUBLE KILL`
   - `TRIPLE KILL`
   - `7TH KILL`
   - `UAV`
   - similar reward or streak banners
3. compare both paths and decide which surface should be the first-class target for the current clip family

## Practical Conclusion

The medal packet was still useful.

It proved:

- the standard onboarding and publish workflow can now carry a narrow medal slice end to end
- the published `call_of_duty` pack can now hold medal coverage canonically

But the packet did not yet solve the current measurement problem because the current clips appear to be a better fit for text or banner signal work than icon-badge detection.

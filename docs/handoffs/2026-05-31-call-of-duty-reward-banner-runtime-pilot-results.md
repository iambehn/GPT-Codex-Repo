# Call of Duty Reward Banner Runtime Pilot Results

## Scope

Implemented the first published-pack `call_of_duty` `reward_banner` pilot using a local `UAV` banner crop and mapped it through:

- `asset_family: reward_banner`
- `signal_type: hud_visibility`
- `event_type: high_action_sequence`

## Pack Changes

Published-pack additions:

- ROI: `reward_banner`
- Template asset: `call_of_duty.uav_reward_banner.reward_banner`
- Runtime rule family: `reward_banner`
- Detection row: `call_of_duty.uav_reward_banner.reward_banner`
- Published asset, candidate, and binding rows in `assets_manifest.json`
- Ontology event seed: `uav_reward_banner`

Threshold tuning:

- initial threshold `0.94` failed on sampled runtime frames
- lowered threshold to `0.90`

## Validation

- `python3 run.py --validate-game-pack call_of_duty`
- `python3 run.py --run-repo-quality-health`

Both passed after the published-pack edits.

## Runtime Findings

### 3-second focused probe

Source:

- `outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_9p5s_12p5s.mp4`

Result:

- matcher detections: `4`
- confirmed reward-banner detections: `3`
- runtime signals: `3`
- runtime events: `3`
- new event type emitted: `high_action_sequence`

Interpretation:

- the first non-equipment runtime signal path now works
- the `UAV` banner can survive the repo runtime sampling path at `sample_fps=2`

### 16-second broader probe

Source:

- `outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_8s_24s.mp4`

Result:

- matcher detections: `32`
- confirmed detections: `5`
- reward-banner confirmed detections: `3`
- equipment confirmed detections: `2`
- runtime event mix:
  - `high_action_sequence: 3`
  - `ability_seen: 2`

Interpretation:

- the `reward_banner` family survives beyond the minimal slice
- the clip is no longer equipment-only under this pilot

## Failure Analysis

Why the first attempt failed:

- direct frame matching against the known probe frame was valid
- the miss came from runtime sampled-frame scores landing around `0.87` to `0.905`
- the original `0.94` threshold was too strict for the sampled runtime path

Notable residual issue:

- one broader-probe reward-banner event landed at approximately `14.5s`
- this is likely a false positive or over-broad banner similarity case
- the next slice should tighten false-positive control before expanding the family

## Recommended Next Step

Do not add more `reward_banner` templates yet.

Take one narrow quality-control slice first:

1. inspect the `14.5s` false-positive frame
2. decide whether to tighten threshold, ROI width, or introduce a mask
3. rerun the same two local probes

## Artifacts

- runtime sidecars:
  - `outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_9p5s_12p5s.runtime.json`
  - `outputs/measurement/call_of_duty_reward_banner_pilot/_PL_5qWwKtY_8s_24s.runtime.json`
- debug bundles:
  - `outputs/measurement/call_of_duty_reward_banner_pilot/debug_3s/`
  - `outputs/measurement/call_of_duty_reward_banner_pilot/debug_8s_24s/`

# Call Of Duty Reward Banner Candidate Probes

Date: 2026-06-01
Scope: test whether the published masked `UAV` reward-banner family can be reproduced on additional public `call_of_duty` gameplay clips.

## Objective

Find at least one additional clip family that shows the same `UAV`-style reward banner used by the current published `call_of_duty` pilot.

## Candidate Sources

Downloaded candidates:

- `https://www.youtube.com/watch?v=ZrWvsu5wjuM`
  - title at download time: `FLAWLESS 71 Gunstreak w/ Bullfrog [71-0] | Black Ops Cold War Multiplayer (No Commentary)`
  - local media: `outputs/public_gameplay_mining/call_of_duty_reward_banner_candidates/ZrWvsu5wjuM.webm`

- `https://www.youtube.com/watch?v=qryfXU7w2IQ`
  - title at download time: `76-0 KILLSTREAK in Black Ops Cold War! (WORLD RECORD)`
  - local media: `outputs/public_gameplay_mining/call_of_duty_reward_banner_candidates/qryfXU7w2IQ.webm`

To keep the scout bounded, each source was converted into a 120-second MP4 probe:

- `outputs/measurement/call_of_duty_reward_banner_candidate_probes/ZrWvsu5wjuM.0s-120s.mp4`
- `outputs/measurement/call_of_duty_reward_banner_candidate_probes/qryfXU7w2IQ.0s-120s.mp4`

## Method

I did not change the published pack.

Instead of broadening the detector, I directly scanned the published `UAV` template over the published `reward_banner` ROI:

- ROI from `assets/games/call_of_duty/hud.yaml`
  - `x_pct: 0.28`
  - `y_pct: 0.16`
  - `w_pct: 0.44`
  - `h_pct: 0.20`
- template:
  - `assets/games/call_of_duty/templates/reward_banners/uav.png`
- mask:
  - `assets/games/call_of_duty/templates/reward_banners/uav.mask.png`

Sampling posture:

- one frame per second
- first 120 seconds only
- score method matched the current masked template shape

The current published threshold for the family is:

- `0.91`

## Results

### 1. `ZrWvsu5wjuM.0s-120s.mp4`

Top observed scores:

- `99.10s` -> `0.84834`
- `69.07s` -> `0.84608`
- `68.07s` -> `0.84542`
- `20.02s` -> `0.84374`

Interpretation:

- no sampled frame approached the published `0.91` threshold
- this probe does not provide evidence for the current `UAV` reward-banner family

### 2. `qryfXU7w2IQ.0s-120s.mp4`

Top observed scores:

- `104.10s` -> `0.84728`
- `103.10s` -> `0.82599`
- `3.00s` -> `0.82182`
- `18.02s` -> `0.82106`

Interpretation:

- no sampled frame approached the published `0.91` threshold
- this probe also does not provide evidence for the current `UAV` reward-banner family

## Decision

These two additional `Black Ops Cold War` high-streak probes do **not** reproduce the current published `UAV` reward-banner family.

Current read:

- the family is still proven only on `_PL_5qWwKtY`
- the new scout evidence does not support expanding `reward_banner` templates yet
- the family may be:
  - clip-specific
  - title-family-specific in a narrower way than expected
  - or influenced by post-production presentation in `_PL_5qWwKtY`

## Recommended Next Step

Do not add another reward-banner asset yet.

The next useful branch should be one of:

1. explicitly source more clips from the same presentation family as `_PL_5qWwKtY`
2. inspect whether the `_PL_5qWwKtY` banner is native HUD or a `medal.tv`-style overlay artifact
3. pivot to a different text family with stronger repeatability evidence

Current recommendation:

- treat `reward_banner` as a narrow validated pilot, not a generalized family
- prioritize source-family verification before detector expansion

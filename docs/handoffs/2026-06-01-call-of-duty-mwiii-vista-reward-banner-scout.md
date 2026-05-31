# Call Of Duty MWIII Vista Reward Banner Scout

Date: 2026-06-01
Scope: test the published masked `UAV` reward-banner pilot against additional clips from the same inferred title/HUD family as `_PL_5qWwKtY`.

## Objective

After identifying `_PL_5qWwKtY` as likely `Modern Warfare III (2023)` on the `Vista` map, test whether the current `UAV` reward-banner family reproduces inside the same title/HUD family.

## Title-Family Basis

The earlier source-truth check showed:

- `_PL_5qWwKtY` contains the location label `VISTA`
- public `Call of Duty` reference material places `Vista` in `Modern Warfare III` multiplayer
- the upper-middle `UAV` banner still looks native, but earlier failed reproductions used mismatched `Black Ops Cold War` HUD families

## Same-Family Candidate Sources

Downloaded sources:

- `https://www.youtube.com/watch?v=CXh9c8AUoZw`
  - title at download time: `Vista Gameplay Call of Duty: Modern Warfare 3 Multiplayer Team Deathmatch (No Commentary)`
  - local media: `outputs/public_gameplay_mining/call_of_duty_reward_banner_mwiii_candidates/CXh9c8AUoZw.webm`

- `https://www.youtube.com/watch?v=gcAGS3R2t2o`
  - title at download time: `Call of Duty Modern Warfare 3 Vista Multiplayer Gameplay PS5 4K (No Commentary)`
  - local media: `outputs/public_gameplay_mining/call_of_duty_reward_banner_mwiii_candidates/gcAGS3R2t2o.webm`

Bounded local probes:

- `outputs/measurement/call_of_duty_reward_banner_mwiii_probes/CXh9c8AUoZw.0s-120s.mp4`
- `outputs/measurement/call_of_duty_reward_banner_mwiii_probes/gcAGS3R2t2o.0s-120s.mp4`

## Method

I kept the published pack unchanged and repeated the same direct masked scan posture:

- template:
  - `assets/games/call_of_duty/templates/reward_banners/uav.png`
- mask:
  - `assets/games/call_of_duty/templates/reward_banners/uav.mask.png`
- ROI:
  - `reward_banner` from `assets/games/call_of_duty/hud.yaml`
- sampling:
  - first 120 seconds only
  - one frame per second

Current published family threshold:

- `0.91`

## Results

### 1. `CXh9c8AUoZw.0s-120s.mp4`

Top observed scores:

- `60.06s` -> `0.85886`
- `46.05s` -> `0.84971`
- `21.02s` -> `0.84962`
- `18.02s` -> `0.84961`

Interpretation:

- same-family candidate still fails to approach the current published threshold
- no evidence that this clip contains the exact current `UAV` reward-banner asset

### 2. `gcAGS3R2t2o.0s-120s.mp4`

Top observed scores:

- `94.09s` -> `0.85461`
- `42.04s` -> `0.85214`
- `74.07s` -> `0.85069`
- `107.11s` -> `0.85028`

Interpretation:

- second same-family candidate also fails to approach the current published threshold
- again, no evidence for direct reuse of the current `UAV` banner asset

## Decision

This same-family scout is negative.

That materially tightens the conclusion:

- the current published `UAV` reward-banner pilot is not just blocked on wrong title family
- even inside likely `MWIII` `Vista` gameplay, the exact current asset does not reproduce
- the family still behaves like a narrow one-clip pilot

## Recommended Next Step

Do not add another reward-banner asset from this family yet.

The strongest next branches are now:

1. inspect whether the current published `UAV` template is too clip-specific in styling, scale, or post-capture processing to generalize
2. source clips from the exact same capture/presentation provenance as `_PL_5qWwKtY`, if available
3. stop expanding `reward_banner` and pivot to a different native surface with stronger repeatability evidence

Current recommendation:

- cap the current `reward_banner` work as a validated narrow pilot
- do not broaden the family further without new evidence

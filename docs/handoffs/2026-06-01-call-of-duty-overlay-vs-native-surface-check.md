# Call Of Duty Overlay Vs Native Surface Check

Date: 2026-06-01
Scope: determine whether the visible `_PL_5qWwKtY` text/banner surfaces are native game HUD or overlay/editorial presentation.

## Objective

Resolve the current ambiguity around `_PL_5qWwKtY`:

- should the center-top `UAV` banner remain a published-pack candidate surface
- should the lower-center kill-count and multikill text remain in scope for pack work

## Evidence

Primary source frames:

- `outputs/inspection/call_of_duty_overlay_check/pl_11_full.png`
- `outputs/inspection/call_of_duty_overlay_check/pl_11_reward.png`
- `outputs/inspection/call_of_duty_overlay_check/pl_11_ability.png`
- `outputs/inspection/call_of_duty_overlay_check/pl_20p5_full.png`
- `outputs/inspection/call_of_duty_overlay_check/pl_20p5_reward.png`
- `outputs/inspection/call_of_duty_overlay_check/pl_20p5_ability.png`

Comparison frames from the two additional public `Black Ops Cold War` probes:

- `outputs/inspection/call_of_duty_overlay_check/zr_99p1_full.png`
- `outputs/inspection/call_of_duty_overlay_check/zr_99p1_reward.png`
- `outputs/inspection/call_of_duty_overlay_check/zr_99p1_ability.png`
- `outputs/inspection/call_of_duty_overlay_check/qr_104p1_full.png`
- `outputs/inspection/call_of_duty_overlay_check/qr_104p1_reward.png`
- `outputs/inspection/call_of_duty_overlay_check/qr_104p1_ability.png`

## Findings

### 1. The center-top `UAV` banner looks native

At `11.0s` in `_PL_5qWwKtY`:

- the `UAV` banner is centered in the upper-middle HUD region
- it uses a cohesive streak-style panel with:
  - icon on the left
  - dark title bar
  - blue accent line
  - `PRESS 3 TO USE` instruction
- it is spatially separated from the `medal.tv` watermark in the lower-left corner

Current read:

- this banner behaves like native game UI, not like a `medal.tv` post-production overlay

### 2. The lower-center count text behaves like an editorial or overlay family

At `_PL_5qWwKtY`:

- `11.0s` lower-center crop shows:
  - `4TH KILL`
  - killer/victim labeling above it
- `20.5s` lower-center crop shows:
  - `7TH KO`
  - red horizontal treatment behind the text

These surfaces:

- sit in the same clip that visibly carries the `medal.tv` watermark
- do not match the published `reward_banner` region
- do not resemble the native upper-middle `UAV` streak panel
- look more like editorial summary/count overlays than stable native HUD widgets

Current read:

- `kill_count_text` and similar lower-center count surfaces should **not** be treated as immediate pack-surface candidates from this clip family

### 3. The failed reproduction clips were from a mismatched HUD family

The additional probes used for reproduction were:

- `ZrWvsu5wjuM`
- `qryfXU7w2IQ`

The extracted frames show a materially different presentation family:

- one probe shows a top-right objective/status card
- another shows a bright yellow `WAR MACHINE READY` banner
- neither shows the same center-top black/blue `UAV` panel shape
- one probe also includes streamer/webcam presentation not present in the native `_PL_5qWwKtY` `UAV` crop

Current read:

- the scout failure does **not** prove that the `_PL_5qWwKtY` `UAV` banner is fake
- it does show that the two candidate clips were the wrong HUD family for reproducing this exact asset

## Decision

Refined source-truth decision:

- keep `reward_banner` as a legitimate native-HUD pilot surface
- treat `_PL_5qWwKtY` lower-center count text as overlay/editorial-adjacent, not as current pack truth
- reinterpret the earlier candidate-probe failure as a title-family mismatch, not a direct refutation of the native `UAV` banner

## Recommended Next Step

Do not pivot the pack toward lower-center count overlays from `_PL_5qWwKtY`.

Do one of these instead:

1. source more clips from the same HUD/title family as `_PL_5qWwKtY`
2. explicitly scope `reward_banner` validation by title family rather than generic `call_of_duty`

Current recommendation:

- keep the published `UAV` reward-banner pilot
- stop treating the lower-center text in `_PL_5qWwKtY` as evidence for a general native text family
- make the next acquisition pass target the same HUD family as `_PL_5qWwKtY`, not generic `Black Ops Cold War` streak clips

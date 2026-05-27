# Call Of Duty Text Banner Packet Brief

## Objective

Produce a signal-specific promotion packet for `call_of_duty` text or reward-banner surfaces that are visibly present in the current measured sample set and may be a better first-class detection target than medal icons for those clips.

## Current Repo Truth

Current published-pack truth:

- published `call_of_duty` pack now has:
  - `hero_portrait` coverage
  - `equipment_icon` coverage
  - promoted `medal_icon` coverage for `15` medals
- the medal packet was promoted through the standard onboarding workflow and published canonically

Current measured behavior after medal promotion:

- post-promotion rerun on the same four public samples still produced:
  - `2 / 4` samples with no runtime events
  - `2 / 4` samples with equipment-only events
  - `0 / 4` medal-driven outcomes
  - `0 / 4` hook candidates

Current frame-level truth:

- at least one multiplayer-style sample visibly shows:
  - `UAV`
  - `DOUBLE KI`
  - `7TH KO`
- the probed frames do not show a clear native medal badge icon in the expected ROI
- local starter evidence now exists at:
  - `outputs/measurement/call_of_duty_text_banner_probes/uav_11.0_full.png`
  - `outputs/measurement/call_of_duty_text_banner_probes/uav_banner_11.0_crop.png`
  - `outputs/measurement/call_of_duty_text_banner_probes/killcount_4th_11.0_crop.png`
  - `outputs/measurement/call_of_duty_text_banner_probes/doubleki_20.5_full.png`
  - `outputs/measurement/call_of_duty_text_banner_probes/doublekill_20.5_crop.png`
  - `outputs/measurement/call_of_duty_text_banner_probes/killcount_7th_20.5_crop.png`

This means the current blocker is no longer medal onboarding mechanics. It is clip-side signal mismatch.

## Decision Target

Identify the first `call_of_duty` text or reward-banner subset worth promoting into onboarding or another sanctioned signal-ingestion path for the current measured sample family.

Priority surface:

- multikill text
- kill-count or streak text
- reward banners

## Required Scope

Focus only on signals that are clearly visible in live gameplay frames and likely to be detectable through:

- OCR
- compact text-region template matching
- lightweight reward-banner detection

Prefer signals like:

- `DOUBLE KILL`
- `TRIPLE KILL`
- `FURY KILL`
- `7TH KILL`
- `8TH KILL`
- `UAV`
- similar reward or streak banners shown during active play

Avoid broad research into:

- cosmetic overlays
- post-production subtitles unrelated to game HUD
- generic scoreboard text
- killfeed names unless the signal is explicitly killfeed-driven

Use this handoff as the local starting point before gathering outside references:

- `docs/handoffs/2026-05-27-call-of-duty-text-banner-frame-probes.md`

## What The Packet Must Contain

For each proposed signal, include:

- exact displayed text
- signal family:
  - `multikill_text`
  - `kill_count_text`
  - `reward_banner`
  - or another compact label if needed
- source clip or source page
- screenshot or crop
- where it appears on screen
- whether it is native game UI or post-production overlay
- expected duration or visibility window
- likely extraction method:
  - OCR
  - template
  - mixed

Also include:

- explicit exclusions
- ambiguous cases
- title-family notes if the signal appears tied to one `call_of_duty` title or mode

## Acceptance Target

The packet is good enough when Codex can use it to:

1. decide whether the next detection slice should target text or banner signals
2. define a narrow first signal family
3. choose sample clips that visibly contain that signal
4. route the result into an existing onboarding or runtime-compatible workflow without guessing

## Researcher Reminder

Prefer:

- live gameplay HUD frames
- tight crops
- visible text with timestamps
- title-family notes
- evidence about whether the signal is native HUD or post-production

Avoid:

- generic wiki pages without screenshots
- source sets that mention medals but do not show the actual visible signal
- broad gameplay compilations without timestamps

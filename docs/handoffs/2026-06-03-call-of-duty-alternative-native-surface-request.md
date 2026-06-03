# Call Of Duty Alternative Native Surface Request

Date: 2026-06-03
Scope: request the next concrete research packet after capping the `reward_banner` family as a narrow pilot.

## Decision Target

Identify the next `call_of_duty` native HUD surface family that is:

- visibly repeatable across clips
- more repeatable than the current `UAV` reward-banner pilot
- more trustworthy than the lower-center overlay-adjacent count text
- suitable for a narrow first runtime slice

## Current Repo Truth

- the current published `UAV` `reward_banner` family is:
  - native-looking
  - clean off-target
  - reproducible only on `_PL_5qWwKtY`
  - not reproducible on same-family `MWIII Vista` no-commentary probes
- the lower-center count text in `_PL_5qWwKtY` behaves like an editorial or overlay family and should not be promoted as pack truth
- the same-family `MWIII Vista` scout did not reveal a strong local-only alternative native surface that is both highlight-salient and visibly repeated

Relevant evidence:

- `docs/handoffs/2026-06-03-call-of-duty-reward-banner-template-specificity-diagnosis.md`
- `docs/handoffs/2026-06-01-call-of-duty-overlay-vs-native-surface-check.md`
- `docs/handoffs/2026-06-01-call-of-duty-mwiii-vista-reward-banner-scout.md`

## What The Research Packet Must Provide

The next packet should be a real `Runtime Signal Packet` and must identify one recommended native surface family.

For each candidate family, include:

- exact signal name
- exact clip URL or local path
- exact timestamps
- screenshots and tight crops
- native HUD vs overlay classification
- title-family notes
- likely extraction posture:
  - `template_first`
  - `mixed_ocr_template`
  - `ocr_first`
  - `unknown`
- explicit false positives
- explicit exclusions
- concrete recommendation for which family Codex should implement first

## Candidate Families To Investigate

Priority order:

1. native scorestreak-ready or streak-earned banners that are **not** the current clip-specific `UAV` panel
2. native objective or mode-state banners only if they are actually repeatable and editorially meaningful
3. native top-right or upper-middle event/status cards if they recur across clips and are not streamer or edit overlays

Exclude:

- lower-center count overlays from `_PL_5qWwKtY`
- webcam, creator branding, or `medal.tv` overlays
- intro cards, killcams, scoreboard transitions, or lobby/setup panels unless the packet argues they are useful and repeatable in gameplay

## Local Scout Result To Respect

The local `MWIII Vista` scout showed:

- lots of stable HUD and objective clutter
- no clearly repeated alternative surface that is both native and highlight-salient enough to implement directly from local inspection

So the next packet should not just list visible UI. It must rank surfaces by:

1. native legitimacy
2. repeatability
3. extraction feasibility
4. usefulness for highlight detection

## Acceptance Target

The packet is good enough when Codex can:

- choose exactly one alternative native surface family
- explain why it is better than continuing `reward_banner`
- implement a narrow runtime pilot without inventing the signal family or evidence base

## Current Recommendation

Do not continue tuning `reward_banner`.

Request:

- `ALTERNATIVE_NATIVE_SURFACE_PACKET`

with the explicit goal of finding a more repeatable native `call_of_duty` HUD surface than the current one-clip `UAV` pilot.

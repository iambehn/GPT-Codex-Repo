# Call Of Duty Top-Right Event Card Scout

Date: 2026-06-03
Scope: local scout of the packet-selected `killfeed_events` family using the current `call_of_duty` sample family and same-title `MWIII Vista` probes.

## Decision Target

Determine whether the next repeatable native `call_of_duty` surface should actually be treated as:

- `killfeed_events`
- or `objective_event_notifications` / top-right event-status cards

## Current Repo Truth

- the active packet recommended `killfeed_events` first
- the published `call_of_duty` pack already contains a top-right ROI named `kill_feed`
- no published `killfeed_events` family exists yet
- the current blocker is no longer family search in the abstract
- the current blocker is correct family classification and extraction posture for the top-right repeated surface

## Local Evidence

### `_PL_5qWwKtY`

Artifacts:

- contact sheet:
  - `outputs/inspection/call_of_duty_killfeed_scout/_PL_5qWwKtY.kill_feed.contact.png`
- exact stills:
  - `outputs/inspection/call_of_duty_killfeed_scout/stills/_PL_5qWwKtY.12s.png`
  - `outputs/inspection/call_of_duty_killfeed_scout/stills/_PL_5qWwKtY.13s.png`
  - `outputs/inspection/call_of_duty_killfeed_scout/stills/_PL_5qWwKtY.14s.png`

Observed:

- the top-right ROI contains repeated native event cards around `12s-14s`
- visible card content includes:
  - streak count
  - multikill or streak-style labels
- this does **not** look like a simple elimination killfeed row
- separate elimination-style text remains visible elsewhere in the full frame and is not what the current top-right ROI is capturing

Assessment:

- native HUD: yes
- repeatability in clip: yes
- family label: top-right event-status card, not clean elimination killfeed
- extraction posture: `mixed_ocr_template`

### `gcAGS3R2t2o.0s-120s`

Artifacts:

- contact sheet:
  - `outputs/inspection/call_of_duty_killfeed_scout/gcAGS3R2t2o_0s-120s.kill_feed.contact.png`
- exact stills:
  - `outputs/inspection/call_of_duty_killfeed_scout/stills/gcAGS3R2t2o_0s-120s.25s.png`
  - `outputs/inspection/call_of_duty_killfeed_scout/stills/gcAGS3R2t2o_0s-120s.27s.png`
  - `outputs/inspection/call_of_duty_killfeed_scout/stills/gcAGS3R2t2o_0s-120s.29s.png`

Observed:

- the same top-right ROI contains repeated native cards around `27s-29s`
- visible card text includes:
  - player name
  - `SECURED A`
- this aligns more naturally with an objective or mode-state notification family than with elimination killfeed

Assessment:

- native HUD: yes
- repeatability in clip: yes
- cross-clip support: yes
- family label: `objective_event_notifications`
- extraction posture: `mixed_ocr_template`

### Other current measurement clips

Artifacts:

- `outputs/inspection/call_of_duty_killfeed_scout/SVbTc2AZzYw_60s-70s.kill_feed.contact.png`
- `outputs/inspection/call_of_duty_killfeed_scout/v-SzAArdAfY_60s-70s.kill_feed.contact.png`
- `outputs/inspection/call_of_duty_killfeed_scout/Qop1sH70nHI_60s-70s.kill_feed.contact.png`

Observed:

- sparse or no convincing top-right event-card visibility at `1 fps` in these short windows
- these clips do not currently justify a pack mutation by themselves

Assessment:

- useful negatives for repeatability pressure
- not strong evidence against the family because the windows are short and sparse

## Findings

### What survived the scout

- the top-right ROI is still the strongest next native-surface region after `reward_banner`
- native event cards do repeat across at least two clips
- same-title support exists from `MWIII Vista`

### What changed

- the packet's `killfeed_events` label does not survive local inspection cleanly
- the repeated visible family is better described as:
  - `objective_event_notifications`
  - or a broader `top_right_event_status_cards` family

### Extraction posture

Recommended posture:

- `mixed_ocr_template`

Reason:

- placement and card shape look structurally stable enough for ROI-based extraction
- key text payloads are variable:
  - player names
  - objective labels
  - state words
- pure template matching is likely too brittle across different card payloads

## Recommendation

Do not continue with `killfeed_events` as the working label for the next runtime slice.

Use:

- `objective_event_notifications`

as the next candidate family, with a possible broader implementation label of:

- `top_right_event_status_cards`

if the runtime layer needs one neutral family name first.

## Acceptance Target

This scout is good enough if Codex can now:

- stop treating the next family as generic killfeed
- choose `objective_event_notifications` / top-right event cards as the next local candidate
- draft the next runtime ticket without inventing the family semantics from scratch

## What Codex Should Do Next

- draft the narrow runtime pilot around the existing top-right ROI
- treat extraction as `mixed_ocr_template`
- use the visible repeated examples from `_PL_5qWwKtY` and `gcAGS3R2t2o` as the first evidence base

## What Codex Should Not Do Next

- do not promote a `killfeed_events` family name that mislabels the visible repeated surface
- do not expand `reward_banner`
- do not treat the sparse short-window negatives as enough to reject the top-right event-card family outright

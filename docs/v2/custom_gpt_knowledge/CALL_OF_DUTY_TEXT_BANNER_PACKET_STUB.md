# Call Of Duty Text Banner Packet Stub

Use this as the starting document for the next `call_of_duty` text or reward-banner research packet.

## Metadata

```yaml
packet_id: 2026-05-27-call-of-duty-text-banner-packet
status: draft
owner: Pipeline Research Strategist
decision_target: Identify the first visible text or reward-banner signal subset worth promoting for call_of_duty.
pipeline_layer: onboarding
game: call_of_duty
priority: P1
confidence: exploratory
supersedes:
```

## 1. Decision Target

Identify the first `call_of_duty` text or reward-banner subset worth promoting for the current measured sample family.

Priority surface:

- multikill text
- kill-count or streak text
- reward banners

## 2. Current Repo Truth

Current published-pack truth:

- published `call_of_duty` pack now includes promoted `medal_icon` coverage for `15` medals

Current measured behavior after medal promotion:

- post-promotion rerun on the same four public samples still produced:
  - `2 / 4` samples with no runtime events
  - `2 / 4` samples with equipment-only events
  - `0 / 4` medal-driven outcomes
  - `0 / 4` hook candidates

Current frame-level truth:

- `_PL_5qWwKtY` visibly shows:
  - `UAV`
  - `DOUBLE KI`
  - `7TH KO`
- the probed frames do not show a clear native medal badge icon in the expected ROI

Relevant repo evidence:

- `docs/handoffs/2026-05-27-call-of-duty-medal-packet-promotion-results.md`
- `docs/handoffs/2026-05-27-call-of-duty-sample-family-audit.md`
- `docs/handoffs/2026-05-27-call-of-duty-text-banner-frame-probes.md`

## 3. Evidence Bundle

Starter local evidence:

```yaml
- evidence_id: EV-COD-TEXT-001
  type: clip_timestamp
  path_or_url: outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4 @ 11.0s
  why_it_matters: Multiplayer-style frame with a clear UAV reward banner and visible 4TH KILL text.
  trust_level: strong_candidate
- evidence_id: EV-COD-TEXT-002
  type: screenshot
  path_or_url: outputs/measurement/call_of_duty_text_banner_probes/uav_11.0_full.png
  why_it_matters: Full frame showing live gameplay context for the UAV banner and kill-count text.
  trust_level: strong_candidate
- evidence_id: EV-COD-TEXT-003
  type: crop
  path_or_url: outputs/measurement/call_of_duty_text_banner_probes/uav_banner_11.0_crop.png
  why_it_matters: Tight crop of the UAV reward banner with readable supporting text.
  trust_level: strong_candidate
- evidence_id: EV-COD-TEXT-004
  type: crop
  path_or_url: outputs/measurement/call_of_duty_text_banner_probes/killcount_4th_11.0_crop.png
  why_it_matters: Tight crop of the 4TH KILL counter in the same frame as the UAV reward banner.
  trust_level: strong_candidate
- evidence_id: EV-COD-TEXT-005
  type: clip_timestamp
  path_or_url: outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4 @ 20.5s
  why_it_matters: Multiplayer-style frame with visible yellow multikill text and a red kill-count counter.
  trust_level: strong_candidate
- evidence_id: EV-COD-TEXT-006
  type: screenshot
  path_or_url: outputs/measurement/call_of_duty_text_banner_probes/doubleki_20.5_full.png
  why_it_matters: Full frame showing live gameplay context for the yellow multikill text and red kill-count counter.
  trust_level: strong_candidate
- evidence_id: EV-COD-TEXT-007
  type: crop
  path_or_url: outputs/measurement/call_of_duty_text_banner_probes/doublekill_20.5_crop.png
  why_it_matters: Tight crop of the visible DOUBLE KI multikill text; useful as a partial-read local seed.
  trust_level: exploratory
- evidence_id: EV-COD-TEXT-008
  type: crop
  path_or_url: outputs/measurement/call_of_duty_text_banner_probes/killcount_7th_20.5_crop.png
  why_it_matters: Tight crop of the visible 7TH KO kill-count text; useful as a partial-read local seed.
  trust_level: exploratory
```

## 4. Structured Findings

### Target Signals To Keep

- `UAV`
- `4TH KILL`
- `DOUBLE KILL` or visible truncated variant `DOUBLE KI`
- `7TH KILL` or visible truncated variant `7TH KO`

### Category Mapping

```text
multikill_text:
- DOUBLE KILL
- visible local seed: DOUBLE KI

kill_count_text:
- 4TH KILL
- 7TH KILL
- visible local seed: 7TH KO

reward_banner:
- UAV
```

### Explicit Exclusions

- cosmetic overlays
- generic scoreboard text
- unrelated subtitles
- broad killfeed names unless the signal is explicitly killfeed-driven

### Ambiguous Candidates

- `DOUBLE KI`: likely a truncated live `DOUBLE KILL` read, but not yet a clean full-string capture
- `7TH KO`: likely a truncated or stylized kill-count read, but not yet a clean title-verified full-string capture

### Negative Examples / False Positives

- `medal.tv` watermark and branding in the clip frame
- post-production overlays that might mimic in-game text
- generic scoreboard or location labels such as `PLAZA` and `TOWER`

### Source Quality Notes

- current local evidence is strong enough to justify a text/banner packet
- current local evidence is not strong enough to settle title-family truth or native-versus-overlay status on its own
- outside packet work should focus on validating the visible signal family, not re-proving that the text exists in the local clip

## 5. Recommendation

State the exact action Codex should take next.

Recommendation:

- start with a narrow `reward_banner + kill_count_text` packet using `UAV`, `4TH KILL`, and `7TH KILL` as the cleanest visible local seeds
- treat `DOUBLE KILL` as a secondary `multikill_text` candidate until a cleaner full-string frame is found
- require the researcher to classify each signal as native UI, post-production overlay, or unresolved

## 6. Acceptance Target

The packet is good enough when Codex can use it to:

1. choose a first text or banner signal family
2. identify sample clips that visibly contain it
3. decide whether the signal should route through OCR, template matching, or a mixed path

Concrete expected outcome:

- Codex can choose whether the next implementation slice should target `reward_banner`, `kill_count_text`, or `multikill_text`
- the next packet contains at least one clip-backed candidate for each promoted family
- the packet explicitly resolves or escalates the native-versus-overlay ambiguity

## 7. Open Uncertainties

```yaml
- question: Are the visible `_PL_5qWwKtY` text surfaces native game UI or post-production overlays added by medal.tv?
  blocks_implementation: true
  recommended_next_step: Validate against outside gameplay references or a second local clip showing the same signal family without branded post-production.
- question: Is `7TH KO` a truncated `7TH KILL`, a title-specific abbreviation, or a different overlay surface?
  blocks_implementation: false
  recommended_next_step: Find adjacent frames or outside references with a cleaner full-string read.
- question: Does the current sample set contain enough clean `multikill_text` evidence to prioritize OCR or template work next?
  blocks_implementation: false
  recommended_next_step: Prefer `UAV` and `kill_count_text` as the first validation slice unless the researcher finds better clean multikill frames.
```

## Researcher Reminder

Prefer:

- live gameplay HUD frames
- tight crops
- visible text with timestamps
- evidence about whether the signal is native game UI or post-production

Avoid:

- generic wiki pages without screenshots
- broad gameplay compilations without timestamps
- source sets that mention medals but do not show the actual visible signal

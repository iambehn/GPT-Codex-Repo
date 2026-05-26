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

## 3. Evidence Bundle

Add one entry per source item.

```yaml
- evidence_id:
  type: screenshot | crop | source_page | clip_timestamp | table | note
  path_or_url:
  why_it_matters:
  trust_level: authoritative | strong_candidate | exploratory
```

## 4. Structured Findings

### Target Signals To Keep

- 

### Category Mapping

```text
multikill_text:
- 

kill_count_text:
- 

reward_banner:
- 
```

### Explicit Exclusions

- cosmetic overlays
- generic scoreboard text
- unrelated subtitles
- broad killfeed names unless the signal is explicitly killfeed-driven

### Ambiguous Candidates

- 

### Negative Examples / False Positives

- 

### Source Quality Notes

- 

## 5. Recommendation

State the exact action Codex should take next.

Recommendation:

- 

## 6. Acceptance Target

The packet is good enough when Codex can use it to:

1. choose a first text or banner signal family
2. identify sample clips that visibly contain it
3. decide whether the signal should route through OCR, template matching, or a mixed path

Concrete expected outcome:

- 

## 7. Open Uncertainties

```yaml
- question:
  blocks_implementation: true | false
  recommended_next_step:
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

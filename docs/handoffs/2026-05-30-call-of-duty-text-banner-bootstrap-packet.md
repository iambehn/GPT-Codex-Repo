# Call of Duty Text/Banner Bootstrap Packet

Status: draft_filled
Packet type: runtime signal packet
Packet id: `2026-05-30-call-of-duty-text-banner-bootstrap-packet`
Owner: Codex
Decision target: choose the first `call_of_duty` native text/banner runtime family for implementation without waiting on a stronger external packet
Pipeline layer: runtime
Game: `call_of_duty`
Priority: `P0`
Confidence: `partial / implementation-usable`
Supersedes: none

## Decision Target

Choose the first `call_of_duty` text/banner family to implement next from the current local evidence:

- `reward_banner`
- `kill_count_text`
- `multikill_text`

## Current Repo Truth

- the published `call_of_duty` pack now includes promoted `medal_icon` coverage
- post-promotion measurement still produced:
  - `2 / 4` samples with no runtime events
  - `2 / 4` samples with equipment-only runtime events
  - `0 / 4` medal-driven outcomes
  - `0 / 4` hook candidates
- local frame probes already established one promising clip:
  - `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
- known local probe anchors:
  - `11.0s`: `UAV`, `PRESS 3 TO USE`, `4TH KILL`
  - `20.5s`: `DOUBLE KI`, `7TH KO`
- the current runtime path is template-ROI based and has no text/banner family yet
- the likely runtime extension surfaces are already mapped in repo docs:
  - `starter_assets/runtime_detection_schema.yaml`
  - `starter_assets/call_of_duty/game_detection_schema_overrides.yaml`
  - `assets/games/call_of_duty/hud.yaml`
  - published runtime rules and fusion rules

Relevant repo evidence:

- [2026-05-27-call-of-duty-text-banner-frame-probes.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-27-call-of-duty-text-banner-frame-probes.md)
- [2026-05-27-call-of-duty-text-banner-runtime-recon.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-27-call-of-duty-text-banner-runtime-recon.md)
- [2026-05-25-call-of-duty-cross-clip-measurement-audit.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-05-25-call-of-duty-cross-clip-measurement-audit.md)

## Evidence Bundle

- `evidence_id`: `EV-COD-TEXT-001`
  - `type`: `local_clip_anchor`
  - `path_or_url`: `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
  - `timestamp`: `11.0s`
  - `why_it_matters`: the clip shows an upper-middle reward-style surface with readable `UAV` text plus `PRESS 3 TO USE`; this is the strongest current candidate for a stable banner family
  - `signal_family`: `reward_banner`
  - `ui_classification`: `unresolved`
  - `extraction_posture`: `template_first`
  - `trust_level`: `strong_local`

- `evidence_id`: `EV-COD-TEXT-002`
  - `type`: `local_clip_anchor`
  - `path_or_url`: `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
  - `timestamp`: `11.0s`
  - `why_it_matters`: the same frame region also shows `4TH KILL`, which supports a lower-center kill-count family, but the text appears more variable and less banner-like than `UAV`
  - `signal_family`: `kill_count_text`
  - `ui_classification`: `unresolved`
  - `extraction_posture`: `mixed_ocr_template`
  - `trust_level`: `strong_local`

- `evidence_id`: `EV-COD-TEXT-003`
  - `type`: `local_clip_anchor`
  - `path_or_url`: `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`
  - `timestamp`: `20.5s`
  - `why_it_matters`: the clip shows partial strings `DOUBLE KI` and `7TH KO`, which are relevant multikill/kill-count hints, but the truncation makes them a weaker first implementation family
  - `signal_family`: `multikill_text`
  - `ui_classification`: `unresolved`
  - `extraction_posture`: `mixed_ocr_template`
  - `trust_level`: `strong_local`

- `evidence_id`: `EV-COD-TEXT-004`
  - `type`: `source_page`
  - `path_or_url`: `https://callofduty.fandom.com/wiki/Medal`
  - `why_it_matters`: useful for confirming medal naming and category structure, but not strong evidence for the current text/banner runtime path
  - `signal_family`: `n/a`
  - `ui_classification`: `n/a`
  - `extraction_posture`: `n/a`
  - `trust_level`: `secondary_support`

## Structured Findings

### Target Families To Keep

Primary keep:

- `reward_banner`
  - visible candidate text: `UAV`
  - nearby supporting text: `PRESS 3 TO USE`
  - current recommendation: first implementation family
  - reason: the banner appears more self-contained and more likely to support a template-compatible pilot than the truncated kill-count strings

Secondary keep:

- `kill_count_text`
  - visible candidate text: `4TH KILL`
  - reason: likely meaningful, but text shape and ordinal variation make it a riskier first template family

- `multikill_text`
  - visible candidate text: `DOUBLE KI`
  - reason: semantically strong, but currently only observed as a partial string in the local probe and therefore weaker than `reward_banner` for a first narrow runtime slice

### UI Classification

Current classification:

- `reward_banner`: `unresolved`
- `kill_count_text`: `unresolved`
- `multikill_text`: `unresolved`

Reason:

- current local evidence shows the surfaces clearly enough to justify a runtime pilot
- current web search did not provide reliable public frame-verified proof that distinguishes native HUD from post-production overlay for these exact strings

### Extraction Posture

- `reward_banner`: `template_first`
- `kill_count_text`: `mixed_ocr_template`
- `multikill_text`: `mixed_ocr_template`

Reason:

- the existing runtime family infrastructure is template-ROI based
- `reward_banner` is the best candidate for a narrow first slice that preserves the current runtime posture
- `kill_count_text` and `multikill_text` appear more text-variant-heavy and may require OCR or mixed handling sooner

### Explicit Exclusions

- medal badge icons as the first runtime family in this slice
- broad OCR expansion across the whole screen
- killfeed parsing in this slice
- hook/editorial redesign in this slice
- conclusions that depend on treating the web search as frame-verified signal truth

### False Positives And Risks

- post-production overlays that mimic in-game banner text
- scorestreak or UI prompts that are readable but not semantically useful for highlight detection
- text truncation from compression or crop mismatch
- title-family mismatch between the current clips and any externally found examples

### Source Quality Notes

- local probe evidence is currently stronger than the web search for the exact implementation question
- the web search was useful for medal terminology but did not quickly yield a strong public packet with timestamps, crops, and native-UI classification for the text/banner path
- this packet is therefore implementation-usable but not strong enough to claim broad external validation

## Recommendation

Implement `reward_banner` first.

Exact next action:

- add one provisional `reward_banner` ROI to the `call_of_duty` HUD surface
- add one narrow runtime family for that ROI with a template-compatible pilot posture
- rerun the existing measured `call_of_duty` sample set
- inspect whether the runtime sidecars now capture meaningful banner evidence

What not to do next:

- do not add multiple text families at once
- do not broaden to OCR-first across the whole frame
- do not revisit medal onboarding mechanics

## Acceptance Target

This packet is good enough when Codex can:

- choose `reward_banner` as the first implementation family without guessing
- add one narrow ROI and one runtime family
- rerun the existing measured sample set
- classify the outcome as one of:
  - usable new runtime evidence
  - ROI mismatch
  - text instability
  - overlay contamination

Concrete expected outcome:

- a first `reward_banner` runtime family exists in the repo
- validation stays green
- rerun output shows either:
  - at least one new runtime signal family beyond equipment-only behavior
  - or a more explicit failure mode than the current generic non-detection result

## Open Uncertainties

- `question`: are the visible banner surfaces native HUD or post-production overlay?
  - `blocks_implementation`: false
  - `recommended_next_step`: implement the narrow pilot and inspect whether the behavior generalizes across the current measured sample set

- `question`: is `reward_banner` actually more stable than `kill_count_text` once ROI crops are extracted programmatically?
  - `blocks_implementation`: false
  - `recommended_next_step`: inspect debug crops from the first pilot run before broadening

- `question`: do the existing four measured clips contain enough banner-positive moments to validate the family decisively?
  - `blocks_implementation`: false
  - `recommended_next_step`: rerun on the current set first, then fall back to the replacement-samples packet only if the result remains ambiguous

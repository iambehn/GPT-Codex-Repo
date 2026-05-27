# Call Of Duty Text Banner Research Request

Date: 2026-05-27
Status: active

## Objective

Produce a decision-ready packet for one next repo action:

- choose the first `call_of_duty` text or banner signal family for implementation

Candidate families:

- `reward_banner`
- `kill_count_text`
- `multikill_text`

## Packet Contract

Follow the repo packet contract:

- one exact decision target
- current repo truth
- evidence bundle
- structured findings
- recommendation
- acceptance target
- open uncertainties

Do not return broad research. Return a decision-ready packet with exact evidence, exclusions, and observable repo-change targets.

Canonical packet docs:

- `docs/v2/RESEARCHER_INPUT_CONTRACT.md`
- `docs/v2/RESEARCH_PACKET_TEMPLATE.md`
- `docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md`

## Primary Sample

Inspect first:

- `outputs/public_gameplay_mining/call_of_duty_editorial_candidates/_PL_5qWwKtY.mp4`

Known local probe anchors:

- `11.0s`
  - `UAV`
  - `PRESS 3 TO USE`
  - `4TH KILL`
- `20.5s`
  - `DOUBLE KI`
  - `7TH KO`

Comparison sample:

- `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`

Use the comparison sample only to confirm the “no useful banner surface” case.

## Scope

Prioritize native HUD reward/banner text over medal badge icons.

For every kept signal, classify UI status as one of:

- `native_ui`
- `overlay`
- `unresolved`

For every kept signal, classify extraction posture as one of:

- `ocr_first`
- `template_first`
- `mixed_ocr_template`
- `unknown`

## Evidence Requirements

For each evidence item, include:

- exact local path or URL
- exact timestamp
- crop or screenshot filename
- why it matters
- signal family
- UI classification
- extraction posture

## ROI Posture

Challenge current ROI assumptions. Treat these as provisional local seeds:

- center-top or upper-middle reward banner region:
  - `UAV`
- lower-center kill-count region:
  - `4TH KILL`
  - `7TH KO`

Supporting local evidence:

- `docs/handoffs/2026-05-27-call-of-duty-text-banner-frame-probes.md`
- `docs/handoffs/2026-05-27-call-of-duty-text-banner-runtime-recon.md`

## Output Targets

Fill:

- `docs/v2/custom_gpt_knowledge/CALL_OF_DUTY_TEXT_BANNER_PACKET_STUB.md`

Also produce:

- one durable completed handoff or packet file

## Acceptance Target

The packet is good enough when Codex can:

- choose one first signal family
- implement the next slice without guessing
- use at least one clip-backed candidate with timestamp and crop
- distinguish `native_ui` vs `overlay` vs `unresolved`
- see explicit false positives and exclusions

## Fallback

If the text/banner route fails, the next packet should be:

- `medal-visible replacement samples`

Do not fall back directly to runtime extraction work before the signal family question is resolved.

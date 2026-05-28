# Call Of Duty Runtime Signal Packet Request

Date: 2026-05-28
Status: queued

## Objective

Produce the next-layer runtime signal packet for the first winning `call_of_duty` signal family.

This packet should only be completed after one of these upstream packets resolves the signal family:

- `docs/handoffs/2026-05-27-call-of-duty-text-banner-research-request.md`
- `docs/handoffs/2026-05-27-call-of-duty-medal-visible-replacement-samples-request.md`

## Decision Target

Given the chosen first signal family, determine the minimum runtime extraction contract needed to implement that family without guessing.

Examples of winning families:

- `reward_banner`
- `kill_count_text`
- `multikill_text`
- `medal_visibility` via replacement samples

## Packet Contract

Follow:

- `docs/v2/RESEARCHER_INPUT_CONTRACT.md`
- `docs/v2/RESEARCH_PACKET_TEMPLATE.md`

Required sections:

- one exact decision target
- current repo truth
- evidence bundle
- structured findings
- recommendation
- acceptance target
- open uncertainties

## Required Inputs

The packet must begin by naming the already-chosen winning family and the packet that established it.

Required upstream references:

- winning family name
- source packet path
- chosen sample clip set
- exact timestamps that visibly contain the signal

## Required Evidence

For each kept signal instance, include:

- exact clip path or URL
- exact timestamp or time window
- screenshot or crop filename
- screen region description
- whether the signal is icon, text, or banner
- expected visibility duration
- likely negatives or confusing neighbors
- extraction posture:
  - `ocr_first`
  - `template_first`
  - `mixed_ocr_template`
  - `unknown`

## Required Findings

The packet should answer:

- where on screen the signal appears
- how stable its position is across clips
- how long it stays visible
- what common false positives exist
- whether current ROI assumptions are compatible, need adjustment, or should be replaced
- whether the first runtime slice should be:
  - ROI-template only
  - OCR-only
  - mixed

## Output Target

Produce one durable handoff or packet file that Codex can use to implement the runtime slice directly.

## Acceptance Target

The packet is good enough when Codex can:

- define the first runtime extraction surface without guessing
- choose the first ROI or ROI candidates
- choose the first extraction posture
- identify likely negatives worth testing
- proceed to implementation and targeted regression work

## Fallback

If the runtime packet cannot establish a stable extraction surface, escalate back to:

- better sample validation
- narrower signal-family selection

Do not jump directly to fusion or editorial work if the runtime surface is still ambiguous.

# Call Of Duty Editorial Packet Request

Date: 2026-05-28
Status: queued

## Objective

Produce the editorial packet for the chosen first `call_of_duty` signal family after runtime and fusion are clear enough to judge opener quality.

## Decision Target

Determine whether the chosen signal family improves opener quality, is only mechanically useful, or should stay rejected for editorial packaging.

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

The packet must name:

- the chosen signal family
- the runtime packet that established it
- the fusion packet, if one exists
- the sample clips or timestamps used for editorial examples

## Required Evidence

For each editorial example, include:

- exact clip path or URL
- exact timestamp window
- why the opening moment works or fails
- whether the signal is:
  - a strong natural opener
  - acceptable only with synthetic framing
  - mechanically real but editorially weak
  - misleading or reject-worthy

## Required Findings

The packet should answer:

- what counts as a strong opener for this signal family
- what counts as merely mechanically valid
- what synthetic framing is still truthful
- what should still be rejected
- what common misleading edit patterns should be avoided

## Output Target

Produce one durable handoff or packet file that Codex can use to:

- classify the family as editorially useful or mechanics-only
- improve hook evaluation or packaging logic if justified

## Acceptance Target

The packet is good enough when Codex can:

- decide whether the chosen family should remain advisory only or be treated as editorially useful
- identify at least one strong opener pattern and one reject pattern
- improve editorial evaluation without guessing

## Fallback

If the signal family is mechanically useful but editorially weak, say so explicitly.

Do not force a positive editorial recommendation just because the runtime path works.

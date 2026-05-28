# Call Of Duty Fusion Packet Request

Date: 2026-05-28
Status: queued

## Objective

Produce the validation packet that defines how the chosen first `call_of_duty` signal family should participate in fused event claims.

This packet should only be completed after the runtime signal family is chosen and the first runtime extraction posture is clear.

## Decision Target

Determine what the chosen signal family should corroborate, what should count as a meaningful fused event, and what should not fuse.

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
- the runtime packet that established its extraction posture
- the sample clips or timestamps used as supporting evidence

## Required Evidence

For each proposed fused use case, include:

- exact clip path or URL
- exact timestamps
- the primary signal instance
- any corroborating signals visible in the same window
- why the combination is meaningful
- why nearby non-meaningful combinations should not count

Potential corroborators may include:

- equipment visibility
- hero or character identity
- killfeed or combat-feed context
- reward banners
- kill-count text
- medal visibility if replacement samples win later

## Required Findings

The packet should answer:

- what should fuse with the chosen family
- what timing window should be expected
- what minimum corroboration should be required
- what “looks related but should not count” cases exist
- whether the first fused event should be:
  - atomic only
  - corroborated only
  - both

## Output Target

Produce one durable handoff or packet file that Codex can use to add or adjust the first fusion rule set.

## Acceptance Target

The packet is good enough when Codex can:

- define the first fused event claim for the chosen family
- choose what corroboration is required
- choose what should stay non-fused
- implement the first fusion rule or fusion-regression slice without guessing

## Fallback

If the packet cannot define meaningful corroboration, the signal family may still be useful as an atomic runtime surface.

In that case, say so explicitly instead of forcing a weak fused event.

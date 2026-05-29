# Implementation Ticket Template

Use this template when the researcher GPT needs to frame implementation-facing work clearly, but the work is not yet ready to become a direct Codex handoff.

This artifact is narrower than a broad research note and earlier than a Codex-ready handoff.

## Template

```text
title:
  Short implementation-facing task name.

goal:
  One paragraph describing the concrete behavior, workflow, or artifact outcome to produce.

why_now:
  Why this work matters now in the current repo sequence or blocker chain.

current_repo_truth:
  The governing repo surfaces, existing behavior, and current blocker state.

scope:
  The exact behavior or workflow slice this ticket should cover.

non_goals:
  What this ticket should explicitly not attempt.

candidate_approach:
  The currently recommended implementation direction at a behavior level.

dependencies:
  Required inputs, packets, sample sets, artifacts, or prior decisions.

acceptance_criteria:
  Observable conditions that make the ticket complete enough to promote into a direct Codex handoff or implementation pass.

verification_shape:
  The likely tests, commands, artifact inspections, or review surfaces that should validate the work.

open_questions:
  Only the unresolved questions that still matter for promotion.
```

## Rules

- use this when the next work item is implementation-facing but repo-context translation or boundary stabilization is still incomplete
- keep the scope narrow enough to promote into one later Codex handoff
- describe behavior and constraints, not a full file-by-file patch plan
- keep `open_questions` limited to promotion-relevant uncertainty
- if the work is already implementation-ready, use the Codex handoff template instead
- if the work is still exploratory, use a research note or decision-ready packet instead

## Example Skeleton

```text
title:
  Add first call_of_duty reward-banner runtime family

goal:
  Add one narrow runtime family that can test whether reward-banner text is a more productive detection surface than medal icons on the current measured call_of_duty clips.

why_now:
  Medal onboarding is structurally complete, but the measured clips still do not yield medal-driven outcomes. Local frame probes suggest visible reward-banner text may be a better first discriminator.

current_repo_truth:
  - call_of_duty pack already publishes medal_icon coverage
  - current measured clips still produce no medal-driven runtime outcomes
  - text/banner packet is being used to decide the first viable text-facing signal family

scope:
  - one first runtime family only
  - one provisional ROI shape
  - one validation loop on the existing measured samples

non_goals:
  - OCR platform expansion
  - broad multi-family text detection
  - editorial hook redesign

candidate_approach:
  - prefer a template-compatible pilot if the packet evidence supports one stable banner surface
  - avoid broad OCR-first expansion unless the packet shows the text is too variable for template matching

dependencies:
  - completed text/banner signal packet
  - local frame probes and crops
  - current call_of_duty runtime schema surfaces

acceptance_criteria:
  - a direct Codex handoff can name the first signal family, ROI surface, likely extraction posture, and validation path without guessing

verification_shape:
  - targeted repo health check
  - runtime artifact inspection on the existing measured clips

open_questions:
  - whether the visible surface is native UI or post-production overlay
  - whether one stable banner surface exists across more than one sample
```

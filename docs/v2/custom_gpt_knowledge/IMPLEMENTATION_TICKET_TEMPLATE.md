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
  Add first call_of_duty top-right anchor runtime pilot

goal:
  Add one narrow runtime pilot that can test whether a discriminative top-right native event-card anchor is valid on the current measured call_of_duty clips.

why_now:
  The bounded local call_of_duty path is mechanically proven, the reward_banner pilot is capped, and the first top-right shell anchor has already been falsified. The next useful implementation-facing work depends on one narrower anchor strategy.

current_repo_truth:
  - call_of_duty pack already contains the capped reward_banner pilot
  - the first shell anchor matched positives and negatives in the same confidence band
  - the active blocker is anchor specificity, not runtime mechanics

scope:
  - one first runtime family only
  - one narrower anchor class only
  - one validation loop on the existing measured samples and current negative set

non_goals:
  - shell-anchor threshold retuning
  - broad OCR expansion
  - multi-family top-right parsing
  - editorial hook redesign

candidate_approach:
  - prefer one anchor-specific pilot if the packet evidence supports a discriminative icon, emblem, or text fragment
  - avoid broad OCR-first expansion unless the packet shows it is stronger than the current weak local OCR evidence

dependencies:
  - completed top-right anchor packet
  - local top-right diagnostics, appendix, and dispatch
  - current call_of_duty runtime schema surfaces

acceptance_criteria:
  - a direct Codex handoff can name one anchor class, one validation path, and one explicit exclusion posture without guessing

verification_shape:
  - targeted repo health check
  - runtime artifact inspection on the existing measured clips

open_questions:
  - whether one anchor candidate is actually more discriminative than generic top-right HUD chrome
  - whether the packet recommends implementation or explicit family retirement
```

# Project Researcher Unblock Handoff

## Purpose

This document is the direct handoff for the project researcher.

Its purpose is not to restate project architecture.

Its purpose is to answer one question:

> What should the researcher produce that would remove the current blocker for Codex?

## Current Blocker

The main blocked branch is:

- `call_of_duty`
- top-right native event-card family
- missing discriminative anchor

Codex is not blocked on:

- runtime plumbing
- published-pack mechanics
- review workflow mechanics
- more local shell-threshold tuning

Codex is blocked on:

- missing external evidence strong enough to choose one next anchor candidate

## What Has Already Been Exhausted

The following lines of local work are already exhausted or explicitly weak:

### 1. `reward_banner` expansion

Status:

- capped as a narrow pilot

Meaning:

- do not send back more `reward_banner` tuning ideas unless there is materially new evidence

### 2. Shell-anchor strategy

Status:

- implemented
- measured
- retired

Meaning:

- do not recommend another shell-level template
- do not recommend threshold tuning on the shell

### 3. OCR-first escalation

Status:

- currently under-justified by local evidence

Meaning:

- do not recommend “try OCR” unless the recommendation is clip-backed and clearly stronger than the current weak OCR evidence

### 4. Generic same-title clip sourcing

Status:

- already tried
- mostly produced wrong-surface top-right content

Meaning:

- do not recommend generic high-kill or same-title sourcing unless the proposed clips visibly contain the target event-card family itself

## What The Researcher Should Read First

Read these in order:

1. [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md)
2. [2026-06-04-call-of-duty-top-right-anchor-dispatch.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-dispatch.md)
3. [2026-06-04-call-of-duty-top-right-anchor-packet-request.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-packet-request.md)
4. [2026-06-04-call-of-duty-top-right-anchor-local-diagnostics.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-local-diagnostics.md)
5. [2026-06-04-call-of-duty-top-right-anchor-appendix.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-appendix.md)

If those do not yield a concrete anchor recommendation, the correct output may be retirement of the family, not more open-ended brainstorming.

## What Output Will Actually Help

The useful researcher output is one decision-ready packet.

It should choose exactly one of these outcomes:

1. recommend one specific narrower anchor candidate for the top-right family
2. retire the top-right family explicitly

The output should not stay undecided.

## Required Packet Contents

If recommending an anchor, include:

- exact clip URLs or local paths
- exact timestamps
- crop or frame references
- native HUD vs overlay classification
- why this anchor is more discriminative than:
  - the failed shell anchor
  - weak OCR evidence
  - wrong-surface Vista candidates
- explicit false positives or exclusions
- implementation posture:
  - `template_first`
  - `mixed_ocr_template`
  - or `retire_family`

If retiring the family, include:

- why the remaining evidence is too weak
- what fallback branch should replace it
- what kind of clips or surface would be needed to reopen it later

## What Output Will Not Help

Do not send back:

- another dashboard
- another coordination artifact
- a list of possibilities without a recommendation
- shell-anchor retry ideas
- generic OCR suggestions
- generic same-title clip suggestions without visible event-card evidence
- a broad theory of the HUD family without a next implementation decision

## Success Condition

The handoff succeeds only if Codex can take one next action without guessing:

1. implement one new narrow anchor pilot
2. explicitly retire the family and pivot

Anything weaker than that preserves the blocker.

## Current Secondary Context

While this external blocker remains open, Codex has continued local hardening work and has now exhausted the meaningful local `clip_registry` ingestion debt.

That means:

- there is no longer a comparable local hardening slice competing with this blocker
- the top-right anchor evidence problem is now one of the highest-value unresolved external inputs

## Bottom Line

The researcher should help by producing one packet that removes ambiguity.

The packet must answer:

> What exact discriminative anchor should Codex try next for the top-right native event-card family, or should that family be retired?

That is the current unblock target.

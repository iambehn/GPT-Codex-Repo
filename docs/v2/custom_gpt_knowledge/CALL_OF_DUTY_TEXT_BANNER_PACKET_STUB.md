# Call Of Duty Text Banner Packet Stub

This file is no longer the active default packet for `call_of_duty`.

Use it only if new evidence explicitly reopens a text or reward-banner branch.

Current active branch:

- top-right native event-card anchor selection
- see [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md)

## Metadata

```yaml
packet_id: 2026-05-27-call-of-duty-text-banner-packet
status: dormant_branch
owner: Pipeline Research Strategist
decision_target: Reopen text or reward-banner work only if materially new evidence appears.
pipeline_layer: onboarding
game: call_of_duty
priority: deferred
confidence: historical_stub
superseded_by:
  - 2026-06-05-call-of-duty-top-right-anchor-researcher-brief
```

## 1. Decision Target

If this branch is reopened, the packet must answer one narrower question:

> Does new evidence justify reopening `kill_count_text`, `multikill_text`, or `reward_banner` work after the current top-right anchor branch?

Do not use this stub to propose a first implementation slice. That decision is already stale.

## 2. Current Repo Truth

Current active truth:

- the published `call_of_duty` medal promotion did not produce clip-visible medal outcomes on the current measured sample family
- the published masked `UAV` `reward_banner` pilot is clean on `_PL_5qWwKtY`
- that `reward_banner` pilot did not generalize across broader cross-sample checks or same-family `MWIII Vista` probes
- `reward_banner` is now capped as a narrow validated pilot, not an expanding family
- the first top-right event-card shell anchor was implemented, measured, and rolled back
- the current active blocker is a discriminative top-right anchor candidate, not a text-banner-first promotion choice

This means:

- do not recommend `reward_banner` as the next default implementation family
- do not treat the older `_PL_5qWwKtY` text/banner evidence as enough to restart that branch by itself

Relevant current docs:

- [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md)
- [2026-06-04-call-of-duty-top-right-anchor-packet-request.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-packet-request.md)
- [2026-06-04-call-of-duty-top-right-anchor-dispatch.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-dispatch.md)
- [2026-06-05-pipeline-current-state-refresher.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-pipeline-current-state-refresher.md)

## 3. When This Stub Is Valid Again

Reopen this branch only if at least one of these is true:

- a new clip family shows repeatable native `reward_banner` behavior beyond `_PL_5qWwKtY`
- a new clip family shows clear native `kill_count_text` or `multikill_text` with better visibility than the current top-right family
- the top-right anchor branch is explicitly retired and text or banner evidence becomes the next strongest native surface

Without one of those conditions, use the top-right anchor brief instead.

## 4. Required Evidence If Reopened

Any reopened packet must include:

- exact clip URLs or local paths
- exact timestamps
- crop or still references
- native UI vs overlay classification
- same-family repetition evidence
- explicit exclusions
- why the reopened text/banner branch is now better than:
  - the capped `reward_banner` pilot
  - the failed top-right shell anchor
  - the current weak OCR evidence

## 5. What Not To Recommend

Do not recommend:

- `reward_banner` as the default next slice without new repeatability evidence
- broad OCR expansion from the existing local `_PL_5qWwKtY` crops alone
- overlay-adjacent lower-center text
- a generic “text signals might work” packet without one chosen branch and one chosen reason

## 6. Acceptance Target

This stub is only useful if a reopened packet lets Codex do one of these without guessing:

1. reopen one text/banner family with new evidence-backed justification
2. keep the branch retired because the evidence is still weaker than the current top-right anchor branch

## 7. Bottom Line

This is now a dormant branch.

The active `call_of_duty` packet need is:

- one discriminative top-right native event-card anchor
- or explicit retirement of that family

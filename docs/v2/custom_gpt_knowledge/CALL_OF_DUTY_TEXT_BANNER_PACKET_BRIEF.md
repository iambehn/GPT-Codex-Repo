# Call Of Duty Text Banner Packet Brief

## Status

Historical branch, not the active default.

Current active `call_of_duty` research blocker:

- discriminative top-right native event-card anchor selection

Use instead:

- [2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-05-call-of-duty-top-right-anchor-researcher-brief.md)

## Objective

Only use this brief if new evidence explicitly reopens text or reward-banner work.

If reopened, the task is not:

- "find any text-like signal"

The task is:

- determine whether a newly evidenced text or banner family is now stronger than the active top-right anchor branch

## Current Repo Truth

Current `call_of_duty` runtime truth:

- medal promotion is structurally complete but still weak on the measured sample family
- `reward_banner` produced one clean `UAV` pilot
- that pilot did not generalize across broader cross-sample checks or same-family `MWIII Vista` probes
- `reward_banner` is capped as a narrow validated pilot
- the first top-right shell anchor was falsified because positives and negatives matched in the same confidence band
- the current live blocker is anchor specificity inside the top-right family

Therefore:

- do not ask Codex to reopen `reward_banner` by default
- do not ask for a generic text/banner packet unless new evidence clearly beats the current top-right branch

## Valid Reopen Triggers

This brief becomes active again only if one of these happens:

- a new clip family shows repeatable native `reward_banner` visibility
- a new clip family shows clearer native `kill_count_text` or `multikill_text` than the current top-right family
- the top-right family is retired and a text/banner branch becomes the strongest next native surface

## Required Packet Content If Reopened

Any reopened text/banner packet must include:

- exact clip URLs or local paths
- exact timestamps
- stills or crops
- native HUD vs overlay classification
- repeatability evidence across more than one clip or family window
- explicit exclusions
- a direct comparison against:
  - the capped `reward_banner` pilot
  - the failed top-right shell anchor
  - the weak current OCR evidence

## What Not To Do

Do not produce:

- a first-slice recommendation for `reward_banner`
- broad OCR-first recommendations from the old `_PL_5qWwKtY` evidence alone
- lower-center overlay-adjacent text recommendations
- broad theory without one explicit next branch

## Acceptance Target

This brief is successful only if Codex can use the resulting packet to:

1. reopen one text/banner family with clear evidence-backed justification
2. or keep the branch retired without ambiguity

## Bottom Line

Today, this is not the live packet path.

The live packet path is:

- one narrower top-right anchor candidate
- or explicit retirement of the top-right family

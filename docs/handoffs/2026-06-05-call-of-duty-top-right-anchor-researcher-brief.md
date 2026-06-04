# Call of Duty Top-Right Anchor Researcher Brief

## Objective

Give the project researcher one complete, implementation-relevant explanation of the blocked `call_of_duty` top-right event-card problem so they can investigate how to help unblock it.

This brief is not a new framework or queue surface.

It is a direct blocker explanation for one next research action:

- identify one discriminative anchor candidate for the top-right native event-card family
- or retire that family explicitly

## Current Blocker

The active blocker is no longer:

- whether the top-right family exists
- whether it is native HUD
- whether the runtime pack can support another template family mechanically

The active blocker is:

- anchor specificity

More precisely:

- the top-right family is still plausible as a native HUD surface
- the first shell-anchor pilot was implemented, measured, and rolled back
- the shell anchor matched positives and negatives in the same confidence band
- bounded local OCR probing is too weak to justify an OCR-first move

Codex cannot safely continue implementation until the researcher identifies:

- one narrower anchor inside the top-right card region that is more specific than the shell itself
- or a strong reason to retire the family

## What Has Already Been Proven

### 1. `reward_banner` is not the next expansion path

What was learned:

- `reward_banner` produced one clean `UAV` pilot
- it did not generalize across same-family `MWIII Vista` probes
- it is now treated as a narrow validated pilot, not a reusable family

Implication:

- do not spend more local work on `reward_banner` tuning unless new evidence appears

### 2. The top-right family is the next plausible native surface

What was learned:

- repeated native-looking cards are visible in the top-right ROI
- the local scout falsified the simple `killfeed_events` label
- the region behaves more like:
  - `objective_event_notifications`
  - or broader `top_right_event_status_cards`

Same-family support windows:

- `_PL_5qWwKtY @ 12s-14s`
- `gcAGS3R2t2o @ 27s-29s`

Negative pressure set:

- `SVbTc2AZzYw.60s-70s`
- `v-SzAArdAfY.60s-70s`
- `Qop1sH70nHI.60s-70s`

### 3. The first shell-anchor pilot failed decisively

What was implemented:

- a narrow top-right shell-anchor pilot using the existing ROI

Measured result:

- positives matched around `0.95194` to `0.95271`
- negatives matched around `0.95325` to `0.95557`

Interpretation:

- there is no usable threshold gap
- the shell is generic HUD chrome, not a discriminative identity

Implication:

- do not keep threshold tuning
- do not republish the shell anchor
- do not derive another shell-level template from the same logic

### 4. Current local OCR evidence is weak

What was learned:

- `_PL_5qWwKtY` yields only partial OCR recovery
- `gcAGS3R2t2o` yields no useful OCR recovery on the tested card crops
- negatives yield OCR noise

Interpretation:

- OCR is not fully ruled out
- but the current clips do not justify a broad OCR-first move

Implication:

- any text-fragment recommendation now needs to beat weak local OCR evidence explicitly

### 5. Generic same-title high-kill Vista sourcing did not help

What was learned:

- additional `MWIII Vista` probes from `vistastructions` were same-title but wrong-surface candidates
- the top-right ROI mostly showed:
  - loadout labels
  - player-name overlays
  - plain environment

Implication:

- generic same-title or high-kill sourcing is not enough
- future sourcing must visibly contain the event-card family itself

## What Has Been Ruled Out

The researcher should treat these as already falsified or already weak:

- shell-derived anchors
- generic killfeed semantics
- broad OCR rollout without stronger evidence
- lower-center overlay-adjacent text
- generic high-kill Vista sourcing that does not visibly show the top-right family

## Exact Problem To Solve

The researcher needs to answer:

> Is there one narrower anchor inside the top-right native event-card region that is discriminative enough to support a narrow runtime pilot?

Acceptable anchor classes:

- icon or emblem block
- compact left-side badge cluster
- stable text fragment, but only if it is justified against the current weak OCR evidence

Unacceptable output:

- a broad theory of the family
- a list of possibilities without a recommendation
- another shell-level template idea
- a generic “try OCR” recommendation without clip-backed justification

## What The Researcher Should Inspect

Primary docs:

- [2026-06-04-call-of-duty-top-right-anchor-dispatch.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-dispatch.md)
- [2026-06-04-call-of-duty-top-right-anchor-packet-request.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-packet-request.md)
- [2026-06-04-call-of-duty-top-right-anchor-local-diagnostics.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-local-diagnostics.md)
- [2026-06-04-call-of-duty-top-right-anchor-appendix.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-top-right-anchor-appendix.md)

Key visual artifacts:

- `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/top_right_roi_montage.png`
- `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/matched_patch_montage.png`
- `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_pl_12.75.png`
- `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/pos_gc_28.0.png`
- `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_sv_4.75.png`
- `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_vs_4.75.png`
- `outputs/inspection/call_of_duty_top_right_anchor_diagnostics/neg_qop_4.75.png`

Supporting exclusion evidence:

- [2026-06-04-call-of-duty-vista-anchor-candidate-scout.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/handoffs/2026-06-04-call-of-duty-vista-anchor-candidate-scout.md)

## What A Useful Researcher Output Must Contain

The next packet should recommend one anchor strategy only.

It must include:

- exact clip URLs or paths
- exact timestamps
- crop or still references
- native UI vs overlay classification
- explicit exclusions
- why the recommended anchor is more discriminative than:
  - the failed shell anchor
  - the weak OCR path
  - wrong-surface same-title candidates

It must separate:

- recommended anchor
- ambiguous anchors
- false-positive-prone anchors
- unusable anchors

## Success Condition

The research output is good enough when Codex can do exactly one of these without guessing:

1. implement one narrower top-right anchor pilot
2. retire the top-right family and pivot cleanly to the fallback sourcing branch

## Failure Modes To Avoid

The researcher should avoid output that:

- restates the blocker without choosing
- recommends OCR because it “might help”
- recommends the shell again with threshold changes
- proposes sourcing that does not visibly show the top-right card family
- mixes multiple anchor candidates without a decision

## Bottom Line

Codex is not blocked on runtime mechanics.

Codex is blocked on one missing decision:

- what exact discriminative anchor inside the top-right event-card family should be tried next, if any

Until that anchor is identified, further local implementation becomes speculative and low-yield.

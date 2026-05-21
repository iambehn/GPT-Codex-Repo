# Detection / Runtime / Fusion

This document is the canonical V2 home for upstream highlight-detection logic.

## Canonical Scope

Use this doc for:

- proposal generation policy
- runtime signal extraction and normalization
- fusion and synergy rules
- shortlist and reranking boundaries
- detector expansion policy

Do not use this doc for:

- editorial hook strategy
- post-routing and account analytics
- identity-resolution or leaderboard sourcing

## Current V2 Position

The repo remains proposal-first and shortlist-first.

Stable policies:

- proxy or cheap proposal passes should reduce the candidate surface early
- runtime signals should be normalized before fusion
- fusion should preserve provenance and contributing evidence
- interaction and synergy are valid scoring constructs, but should remain explainable
- reranking stays top-N only
- heavy VLM work should never become a full-video default path

## How To Think About The Upstream Stack

This layer is where the pipeline turns raw media into candidate highlight claims.

The important idea is that upstream is not one step. It is three different kinds of decision:

1. proposal generation
- where might something interesting be happening?

2. runtime extraction and normalization
- what concrete signals do we think are present, and when?

3. fusion and candidate scoring
- do those signals combine into a highlight candidate that is strong enough to review?

If those decisions stay separated, debugging stays tractable. If they blur together, the system becomes much harder to reason about because every failure starts looking like “the detector was bad.”

## What Proposal Generation Is Actually Doing

Proposal generation is a narrowing step, not a truth step.

Its job is not to decide that a highlight exists. Its job is to cheaply decide where more expensive analysis is justified.

That means proposal generation should optimize for:

- reducing surface area
- preserving potential recall
- avoiding expensive full-video processing by default

It should not be treated as the final authority on candidate quality.

In practice:

- proxy passes are allowed to be rougher
- missing some precision here is acceptable if later stages can correct it
- but if proposal generation is too noisy or too narrow, the downstream system either becomes expensive or loses meaningful events before runtime and fusion ever see them

## What Runtime Extraction Is Actually Doing

Runtime extraction is where the system moves from “something may be happening” to “here are specific game signals with timestamps and provenance.”

This layer is trying to answer:

- what did we detect?
- where did we detect it?
- when did we detect it?
- how confident are we?
- what sidecar evidence can explain it later?

Runtime extraction is not only about detection quality. It is also about signal shape.

A runtime signal is only useful downstream if it is:

- time-aligned
- attributable to a detector or asset family
- preserved with enough context to survive fusion, review, and replay

That is why normalization matters as much as raw matching.

## What Fusion Is Actually Doing

Fusion is the first place the pipeline is allowed to say something like:

- this looks like a real highlight candidate

Fusion does that by combining normalized evidence into a claim that is stronger than any one signal alone.

That claim needs to stay explainable.

Good fusion means:

- the contributing signals remain visible
- the interaction logic is inspectable
- the score can be decomposed
- disagreement between signals is preserved instead of hidden

Bad fusion means:

- a high final score with unclear evidence
- opaque synergy effects
- timing assumptions that are hard to inspect
- candidate claims that later review cannot meaningfully explain

## Current Concrete Example

The bounded `call_of_duty` local-test path is again the best concrete example.

Upstream, it proved this progression:

1. media input existed
2. runtime analysis produced `runtime_analysis_v1`
3. runtime evidence was reviewed and calibrated
4. fusion produced `fused_analysis_v1`
5. fused review produced approved or rejected candidate outcomes

The conceptual point is:

- runtime analysis told us what the system believed it saw
- fusion turned that evidence into candidate highlight events
- fused review decided whether those candidate claims were acceptable

That is why runtime success alone was not enough for local export. The pipeline needed a fused candidate layer, not just runtime evidence.

## Where Complex Problems Usually Hide Here

### 1. Proposal generation is too broad or too narrow

Symptoms:

- too many candidate windows
- too little downstream signal density
- expensive analysis on unpromising media
- meaningful moments never reaching runtime or fusion

This is a narrowing-policy problem, not always a detector problem.

### 2. Runtime extraction looks plausible, but fusion is wrong

Symptoms:

- detections exist
- events look individually reasonable
- fused candidates are poor or missing

Typical causes:

- normalization mismatch
- timing-window mismatch
- insufficient provenance carried into fusion
- interaction logic overweighting or underweighting certain evidence

### 3. Fusion scores are high, but the candidate is not actually convincing

Symptoms:

- strong `final_score`
- weak human confidence
- review keeps rejecting high-scoring candidates

Typical causes:

- explainability gap
- synergy logic masking weak base evidence
- detector-specific false positives being amplified by fusion

### 4. Upstream artifacts exist, but downstream semantics are still broken

Symptoms:

- runtime and fused sidecars parse
- review and export behavior still look wrong

Typical causes:

- upstream artifacts technically satisfy schema shape
- but the evidence they contain is not meaningful enough for later stages

That is why upstream validation has to be semantic, not just file-based.

## Practical Mental Model

Use this short model:

- proposals decide where to look harder
- runtime decides what signals seem to exist
- normalization makes those signals comparable
- fusion decides whether the signal set supports a candidate claim
- review decides whether that claim is acceptable

If those functions stay mentally separate, upstream bugs become much easier to diagnose.

## Shared Signal Contract

V2 should treat normalized signals as one internal contract across proxy, runtime CV, audio, and fused layers.

Minimum expectations:

- one canonical time anchor per signal
- explicit modality and detector identity
- confidence preserved without hidden rescaling
- provenance back to the originating artifact or sidecar
- enough payload to explain why a downstream fused event exists

Fusion hardening rules:

- keep base evidence separate from interaction or confirmation logic
- preserve disagreement cases instead of collapsing them into opaque scores
- prefer cheap-to-expensive gating before broader inference
- do not introduce hidden timing or threshold changes without reviewable evidence

Detector expansion policy:

- add heavier detector families only when replay and reviewed evidence show unresolved recall gaps
- preserve the cheap-signal-first funnel even when heavier detectors are added
- treat model changes as trials with comparison and review evidence, not intuition

## What Belongs Elsewhere

- Hook packaging and opener logic belong in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
- Trial results and calibration outcomes belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) and the experiment ledger.
- Learned ranking adoption decisions belong in ADRs plus experiment records.

## Current Repo Anchors

- current roadmap summary of this layer: [FUTURE_FEATURES_ROADMAP.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md)
- research shortlist: [HIGHLIGHT_DETECTION_RESEARCH_SHORTLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/HIGHLIGHT_DETECTION_RESEARCH_SHORTLIST.md)

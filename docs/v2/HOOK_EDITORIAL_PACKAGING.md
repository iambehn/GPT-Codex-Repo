# Hook / Editorial Packaging

This document is the canonical V2 home for hook logic and editorial packaging rules.

## Canonical Scope

Use this doc for:

- hook candidate artifact policy
- truthful opener and packaging logic
- natural vs synthetic hook distinctions
- measurable editorial fields and review expectations

Do not use this doc for:

- raw detector semantics
- upstream identity sourcing
- broad post-analytics strategy

## Current V2 Position

Hook logic is now a first-class V2 concern.

Stable policies:

- hook quality should become a measurable artifact, not an informal editing instinct
- event mapping tells us what happened
- hook packaging decides whether the opening makes the event legible, compelling, and truthful
- clips without a natural or defensible synthetic hook should be rejected rather than over-salvaged
- hook-related review and comparison data should stay connected to the same artifact lineage as upstream evidence
- hook artifacts remain advisory in V1; they do not change lifecycle gates by themselves

The intended artifact direction is:

- explicit hook candidate metadata
- hook archetype classification
- packaging strategy fields
- sound-off legibility and authenticity-risk style measurements

## How To Think About The Hook Layer

This layer exists because “a real event happened” is not the same thing as “the resulting short-form clip opens well.”

Upstream detection and fusion are trying to answer:

- did something highlight-worthy happen?

The hook and packaging layer is trying to answer:

- can we present that event in a way that is legible, compelling, and still truthful?

That means this layer is not mainly about event truth. It is about editorial usability.

Use this mental split:

- upstream says whether a candidate claim is real enough to keep
- hook logic says whether the viewer can understand why it matters quickly enough
- packaging says how to present that candidate without falsifying what happened

## What A Hook Is Actually Doing

A hook is the opening framing that makes the candidate event understandable and watchable in short-form context.

That can mean:

- a natural opening already exists in the clip
- a defensible synthetic opener is needed
- no honest opener exists, so the clip should be rejected

The hook layer is therefore making an editorial viability decision, not a detector-quality decision.

Its job is to decide things like:

- does the viewer understand the event quickly enough?
- is the opening emotionally or informationally legible?
- is the opener truthful to the event?
- does the packaging overstate, obscure, or distort what actually happened?

## Natural vs Synthetic Hooks

This is the most important distinction in the layer.

Natural hook:
- the source clip already contains an opening that makes the event legible

Synthetic hook:
- the source clip does not open strongly enough on its own
- but a defensible editorial opener can be added without becoming misleading

Rejected hook:
- no honest opening is available
- or the packaging required to make the clip work would distort the underlying event too much

That distinction matters because it prevents the project from quietly turning into “salvage every candidate no matter what.”

## What Packaging Is Actually Doing

Packaging is where the clip becomes a communication object rather than only an evidence object.

Packaging choices include:

- opener structure
- title or caption framing
- sound-off legibility
- authenticity-risk constraints
- whether the clip should be treated as single-event, explanatory, reaction-led, or something else

Packaging should never redefine upstream event truth.

Its job is to communicate the candidate, not to invent a better event than the one the system actually found.

## Current Concrete Example

The bounded `call_of_duty` local-test path did not go deep into hook evaluation, but it still illustrates the layer boundary.

That path proved:

- runtime evidence can be reviewed
- fused candidate claims can be reviewed
- a local export artifact can be created

The current concrete hook artifact for that sample currently says:

- `hook_mode: reject`
- `rejection_reason: authenticity_risk_too_high`
- `hook_strength: 0.4168`
- `authenticity_risk_score: 0.7688`

So what the path did **not** prove is:

- that the exported clip has a strong short-form opening
- that the opener is the best available opener
- that the packaging is the most compelling editorial treatment

That is why the hook layer exists as a separate concern even after a candidate is already exportable.

The important V1 nuance is:

- the candidate is mechanically exportable
- but the current hook artifact still judges it editorially weak
- this is allowed right now because hook artifacts are still advisory rather than lifecycle-gating

## Where Complex Problems Usually Hide Here

### 1. The event is real, but the clip still feels weak

Typical cause:

- the pipeline found a valid event
- but the opening does not explain why the viewer should care quickly enough

This is not necessarily an upstream failure.

### 2. The hook is compelling, but not truthful enough

Typical cause:

- packaging improved attention
- but crossed into distortion, overstatement, or ambiguous causality

This is the central risk of synthetic hooks.

### 3. The system rejects too many candidates that could be packaged well

Typical cause:

- hook criteria are too strict
- natural and synthetic hook modes are not distinguished carefully enough
- editorial viability is being conflated with raw event intensity

### 4. Good hooks do not correlate with downstream selection or export

Typical cause:

- hook artifacts exist, but they are not connected clearly enough to the lifecycle or review evidence
- hook quality is being measured, but not in a way that later decisions can inspect

## Practical Mental Model

Use this short model:

- detection finds possible events
- fusion claims a candidate is worth attention
- hook logic decides whether the audience can understand that candidate fast enough
- packaging decides how to present it without breaking truthfulness

If that separation stays clear, editorial problems stop being misdiagnosed as detector problems.

## Hook Evaluation V1

The repo now exposes a unified hook evaluation artifact:

- `hook_evaluation_report_v1`

It combines:

- fixture/trial hook comparisons
- approved or export-selected candidate rollups from the registry
- fused-vs-hook disagreement counts
- editorial viability classification for the current evaluated state:
  - `editorially_viable`
  - `mixed`
  - `mechanics_only`
  - `not_exported`
  - `no_candidates`
- explicit advisory policy and future gate readiness status

Primary operator entrypoint:

```bash
python3 run.py \
  --report-hook-evaluation /path/to/fixtures.json \
  --baseline-sidecar-root /path/to/baseline \
  --trial-sidecar-root /path/to/trial \
  --registry-path /path/to/registry.sqlite \
  --game <game>
```

Registry-backed query surfaces:

- `hook-evaluation-reports`
- `hook-quality-rollups`

These exist to answer two separate questions:

- did the trial hook strategy improve the reviewed fixtures
- what editorial hook patterns are being approved and exported right now

## What Belongs Elsewhere

- Runtime/fusion scoring mechanics belong in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Calibration and comparison of hook strategies belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) and experiment records.
- Post-level performance interpretation belongs in [DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md).

# Architecture / Operating Model

This document is the canonical V2 home for the pipeline operating model.

## Canonical Scope

Use this doc for:

- staged pipeline boundaries
- cheap-signal-first compute policy
- artifact roles and handoff points
- separation between detection, scoring, editorial packaging, and downstream operations

Do not use this doc for:

- detailed experiment results
- one-off trial outcomes
- subsystem-specific implementation checklists

## Current V2 Position

The pipeline stays staged and manifest-driven.

Core operating rules:

- cheap global passes happen first
- expensive inference happens only on narrowed candidate sets
- sidecars remain the detailed evidence layer
- review, replay, and calibration are decision surfaces, not afterthoughts
- downstream posting and analytics do not redefine upstream event truth

The core boundary set is:

1. Ingest and proposal generation
2. Runtime signal extraction and normalization
3. Fusion and candidate scoring
4. Reranking and candidate packaging
5. Review, export, posting, and metrics

## How To Think About The Pipeline

The pipeline is not one big "find highlights" function. It is a ladder of narrower decisions.

Each stage is trying to answer a different question:

1. Ingest and proposal generation
- What media are we analyzing?
- Do we have enough evidence to justify spending more compute on any part of it?

2. Runtime signal extraction and normalization
- What concrete game signals appear to be present?
- Can we express those signals on one consistent time base with preserved provenance?

3. Fusion and candidate scoring
- Do the normalized signals support a meaningful highlight candidate?
- Is the candidate explainable enough to review instead of just "high-scoring"?

4. Selection and export packaging
- Which reviewed candidates become local highlight artifacts?
- What exact artifact chain proves that decision?

5. Posting and downstream metrics
- What was exported?
- What was actually posted?
- How did it perform?

That separation matters because most hard bugs in this repo are not syntax bugs. They are decision-boundary bugs where one stage appears to succeed, but the next stage cannot use the result correctly.

## Current Concrete Example

The bounded `call_of_duty` local-test path is the best current example of the system.

It proves this chain:

1. Media input
- one bounded local test clip

2. Runtime evidence
- `runtime_analysis_v1`
- this is where ROI matching and event extraction become inspectable sidecar evidence
- matcher debug coordinates are expressed in pack-normalized frame space; read `matcher.frame_dimensions` and `matcher.frame_coordinate_space` before treating `frame_match_x` or `frame_match_y` as pixel locations

3. Runtime review
- `runtime_review_session_v1`
- this is where uncertain or important runtime outcomes become explicit review decisions instead of hidden heuristics

4. Runtime calibration
- runtime calibration report
- this does not create highlights; it checks whether the scoring behavior is defensible against reviewed runtime examples

5. Fused evidence
- `fused_analysis_v1`
- this is where runtime evidence becomes candidate highlight events with preserved contributing-signal provenance

6. Fused review
- `fused_review_session_v1`
- this is where candidate highlight events become approved or rejected review outcomes

7. Selection and lifecycle propagation
- `highlight_selection_v1`
- `workflow_run_v1`
- candidate lifecycle state in the registry

8. Local export artifact
- `highlight_export_batch_v1`
- this proves a candidate became exportable and was materialized locally

9. Not yet posting
- no `posted_highlight_ledger_v1`
- this is the important local-only boundary

The key conceptual lesson is that runtime calibration and local export are related, but they do different jobs:

- runtime calibration tells us whether runtime scoring aligns with reviewed examples
- fused review and export tell us whether a reviewed candidate can actually move through the selection and lifecycle chain

## What The Current Proof Does Not Mean

The current bounded `call_of_duty` proof does **not** mean:

- the repo is done
- every game is equally ready
- the pipeline is publish-cleared
- the sample media is cleared for external use
- the current thresholds are globally correct
- runtime success alone is enough for export

It only proves that one concrete path can move from media input to a local export artifact while keeping the repo-quality gate green.

## Where Complex Problems Usually Hide

When the pipeline becomes hard to debug, the issue is usually in one of these overlap zones:

### 1. Evidence exists, but downstream meaning is wrong

Example:
- a sidecar parses
- events exist
- but review or export behavior is semantically wrong

This usually means the bug is in:
- field interpretation
- status propagation
- lifecycle joins
- timing alignment

Not in "whether the file exists."

### 2. Runtime and fused stages disagree

Example:
- runtime review looks reasonable
- fused candidate behavior looks wrong

This usually means the bug is in:
- normalization
- fusion gating
- provenance propagation
- candidate packaging assumptions

Not necessarily in the detector itself.

### 3. Review state and lifecycle state drift apart

Example:
- something is approved in review
- but it never becomes exportable

This usually means the bug is in:
- registry refresh logic
- selection manifest linkage
- workflow queue generation
- lifecycle transition rules

### 4. The health gate is green while the path is still wrong

This is the most dangerous class.

It means:
- contracts may still be too weak
- the tested boundary is narrower than the real failure
- we need a more semantic regression, not more general confidence

## How To Debug Conceptually

When something fails, start with the decision boundary, not the code file.

Ask:

1. What question was this stage supposed to answer?
2. What artifact proves its answer?
3. Can the next stage consume that artifact?
4. If not, is the problem:
- missing evidence
- wrong status
- wrong timing
- wrong lifecycle propagation
- wrong interpretation of a valid artifact
5. Is the failure isolated to one layer, or does it show disagreement between layers?

That is why this repo uses:
- sidecars for detailed evidence
- review sessions for explicit decisions
- calibration and replay for decision checking
- registry state for cross-artifact lifecycle joins
- repo-quality health for governance-level drift detection

## Practical Mental Model

Use this short model when thinking about the project:

- sidecars are evidence
- review artifacts are decisions
- calibration artifacts are checks on decision quality
- fused artifacts are candidate highlight claims
- registry rows are workflow state
- export artifacts are local outputs
- posted ledgers are external-action records

If those layers stay mentally separate, the project remains understandable even when the implementation gets large.

## What Belongs Elsewhere

- Game-pack and manifest field definitions belong in [MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md).
- Runtime, fusion, and reranking mechanics belong in [DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md).
- Hook packaging logic belongs in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
- Registry and orchestration state rules belong in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).

## Current Repo Anchors

- canonical roadmap: [FUTURE_FEATURES_ROADMAP.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/FUTURE_FEATURES_ROADMAP.md)
- V1 baseline summary: [V1_NOTES_CONDENSED_SUMMARY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/V1_NOTES_CONDENSED_SUMMARY.md)

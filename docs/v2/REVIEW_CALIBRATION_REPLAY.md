# Review / Calibration / Replay

This document is the canonical V2 home for review-backed decision loops.

## Canonical Scope

Use this doc for:

- review surfaces and statuses
- replay and comparison policy
- fixture-driven evaluation workflow
- calibration posture and recommendation rules

Do not use this doc for:

- long-term storage schema design
- platform-posting operations
- source-enrichment ranking logic

## Current V2 Position

The repo already has strong replay and comparison infrastructure. V2 should treat it as a core operating surface.

Stable policies:

- review-backed comparisons outrank intuition
- sidecars are source artifacts for replay and comparison
- fixture-based baseline vs trial workflows should stay deterministic
- recommendation artifacts should remain human-decision aids, not auto-promotion logic
- viewer and review surfaces should make disagreement cases easy to inspect
- hook evaluation remains advisory in V1 even when its comparisons are queryable and registry-backed
- non-trivial detector, fusion, schema, and publish-workflow changes should pass through review-backed validation before promotion

Measured outcomes belong in experiment ledgers, not here.

This doc should stay focused on:

- what comparison and replay are for
- what artifacts they consume
- what recommendation states mean
- how review outcomes feed later decisions

## How To Think About Review, Calibration, And Replay

This layer exists because a scoring pipeline can be internally consistent and still be wrong.

Detection, runtime scoring, fusion, and packaging all produce candidate claims. Review, calibration, and replay answer a different class of question:

- did the system make a defensible decision?
- can we explain why that decision happened?
- if we change the system, did the decision quality improve or regress?

That makes this layer a decision-checking surface, not a content-generation surface.

Use this mental split:

- sidecars say what the system observed
- review says whether a human or authorized operator accepts the decision implied by that evidence
- calibration says whether the scoring behavior aligns with reviewed outcomes
- replay says whether a proposed change behaves better, worse, or differently on the same evidence

## What Review Is Actually Doing

Review is not just a UI step or a manual approval ritual.

It has three jobs:

1. convert ambiguous or high-impact machine outcomes into explicit decisions
2. preserve those decisions in reusable artifact form
3. create the evidence base that later calibration, replay, and promotion decisions depend on

Without review, the pipeline can still emit scores. What it cannot do reliably is distinguish:

- a strong decision from a merely high score
- a detector failure from a policy failure
- a useful threshold from an accidental one

That is why review-backed artifacts matter more than one-off observations in chat or shell output.

## What Calibration Is Actually Doing

Calibration does not create highlight candidates.

It asks whether the current scoring rules behave sensibly against reviewed examples.

In the current bounded `call_of_duty` path, runtime calibration answers a question like:

- given a small reviewed set of runtime sidecars, do approved items land on the approved side of the decision boundary and rejected items land on the rejected side?

That makes calibration a quality check on decision behavior, not an execution step that directly produces publishable content.

The practical implication is important:

- a passing calibration report does not mean the export path works
- a working export path does not mean the calibration logic is trustworthy

Those are adjacent but different proofs.

## What Replay Is Actually Doing

Replay is how the repo answers "what would change if we changed this rule or model?" without guessing.

Replay matters whenever:

- a threshold is adjusted
- a scoring weight changes
- a detector is replaced
- a fusion policy is modified
- a packaging rule starts selecting different candidates

Without replay, changes get judged by memory and intuition. With replay, they get judged against the same reviewed evidence.

That is the core discipline:

- same evidence
- old behavior vs new behavior
- explicit recommendation artifact
- human-readable difference, not just a new score

## Current Concrete Example

The bounded `call_of_duty` local-test path now shows the intended separation clearly:

1. runtime analysis sidecars were produced from media
2. runtime review decisions were applied
3. runtime calibration passed on a reviewed `2 approved / 2 rejected` set
4. fused review decisions were applied separately
5. local export succeeded separately

That sequence matters because it proves:

- runtime review and calibration are about decision quality on runtime evidence
- fused review and export are about candidate advancement through the highlight workflow

The same clip can touch both surfaces, but the surfaces do different conceptual work.

## Where Complex Problems Usually Hide Here

The hardest failures in this layer are usually not parsing failures. They are meaning failures.

### 1. Review looks correct, but calibration is misleading

Typical cause:
- too few reviewed examples
- class imbalance
- reviewed set not representative of the actual candidate mix

### 2. Calibration passes, but downstream candidate quality is still bad

Typical cause:
- runtime scoring is acceptable, but fusion or packaging is the real problem
- approved runtime evidence does not automatically imply good fused candidate behavior

### 3. Replay shows difference, but the meaning of the difference is unclear

Typical cause:
- metrics moved, but the changed examples are not inspectable enough
- artifact comparison exists, but the decision rationale is too opaque

### 4. Review outcomes are not reusable

Typical cause:
- review state was applied, but not preserved in a form calibration or replay can consume later
- the system treated review as a one-time approval instead of reusable evidence

## Practical Mental Model

Use this short model:

- review decides whether evidence-backed outcomes are acceptable
- calibration checks whether scoring aligns with those reviewed outcomes
- replay checks how proposed changes behave against the same evidence
- promotion should happen only after those surfaces agree strongly enough

If those functions get blurred together, the pipeline becomes much harder to diagnose.

## Detector Calibration Operator Note

For detector-calibration follow-up generation, the tool-returned JSON is the operator contract.

Use:
- `output_path` as the canonical emitted artifact path
- `emitted_manifest` from `tools/detector_calibration_followup_manifest.py`
- `emitted_report` from `tools/detector_calibration_followup_report.py`

Do not rediscover the latest follow-up manifest or report with shell globs or wildcard-based `ls` flows just to inspect the strongest generated row.

Preferred operator pattern:
1. run the tool
2. read the returned `output_path`
3. inspect the returned `emitted_*` summary block for the strongest row
4. open the written artifact only when deeper row inspection is actually needed

## Release-Gate Role

Replay, calibration, and review-backed comparison are V2 release gates, not just debugging helpers.

That means:

- regressions should be surfaced before publish or promotion
- recommendation artifacts should stay stable enough to compare baseline vs trial behavior
- reviewer approvals and rejections should remain reusable evidence for later tuning
- compact operator reporting should still expose enough state to debug a blocked release

## Shadow Target-Specific Promotion

Shadow promotion decisions are target-specific.

- `approved_or_selected_probability` can be promoted from a focused real-only `full` operator run when:
  - model training and evaluation succeed
  - benchmark review marks the target ready for next iteration
  - governance coverage is sufficient and policy recommends `prefer_shadow`
- explicit review outcomes outrank downstream lifecycle state for this target
- approval-target datasets must contain both positive and negative labels after target construction
- sparse post-performance coverage does not block this target by itself
- `post_performance_score` remains a separate target and should stay blocked until usable post-performance labels exist

Warnings should stay target-relevant. Approved-target and export-target runs should not inherit `sparse_post_performance_target` warnings from inactive heads.

### Approved-Target Operator Runbook

Use this flow when validating `candidate_approval_probability` from the shadow stack against a real-only dataset.

Command:

```bash
python3 run.py \
  --run-shadow-operator \
  --mode full \
  --dataset-manifest /absolute/path/to/v2-training.manifest.json \
  --policy-path /absolute/path/to/default.shadow_evaluation_policy.json \
  --training-target approved_or_selected_probability \
  --target candidate_approval_probability \
  --split-key candidate_id \
  --train-fraction 0.75 \
  --output-root /absolute/path/to/output-root
```

Promotion-ready result for this target means all of the following hold in the resulting `shadow_operator_run_v1` artifact:

- `status: ok`
- `final_recommendation.decision: prefer_shadow`
- `final_summary.warning_count: 0`
- `step_results.train_model.warning_count: 0`
- `step_results.run_benchmark_matrix.warning_count: 0`
- benchmark review reports the target ready for next iteration
- governance reports sufficient coverage for `candidate_approval_probability`

This runbook is intentionally narrow. It does not imply that `post_performance_score` is ready, and it should not be used as a proxy for broader multi-target promotion.

Interpretation note:

- the `outputs/real_only_refresh/2026-05-05/no_platform_dataset/...` `prefer_shadow` result should be treated as exploratory-only evidence
- it remains useful for hypothesis generation, but not for promotion on its own
- the stronger promotion read is the corrected fixture-slice run, which currently lands at `keep_current` with `primary_metric_delta: 0.0`

## Hook Comparison Integration

Hook evaluation now sits inside the same review-backed comparison loop as other fixture and trial work.

Relevant artifacts:

- `hook_candidate_comparison_v1`
- `hook_evaluation_report_v1`

The comparison flow is:

1. compare baseline and trial hook sidecars on the same fixture manifest
2. summarize recommendation state from matched rows
3. join registry-backed approved/exported hook rollups
4. preserve disagreement cases where fused quality and hook quality diverge

This is intentionally not promotion logic. The V1 report is for:

- measuring whether a hook trial improved editorial quality
- exposing which hook modes and archetypes are actually surviving selection and export
- showing whether disagreement patterns are strong enough to justify future gate review

## What Belongs Elsewhere

- Registry design belongs in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Learned-model adoption evidence belongs in experiment records and ADRs.
- Hook-specific packaging rules belong in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).

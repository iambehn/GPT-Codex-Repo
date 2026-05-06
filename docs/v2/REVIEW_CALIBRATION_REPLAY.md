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

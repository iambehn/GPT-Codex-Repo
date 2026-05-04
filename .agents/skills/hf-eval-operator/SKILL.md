---
name: hf-eval-operator
description: |
  Standardized operator workflow for hf_multimodal calibration and replay in this repo.
  Use when running or interpreting --calibrate-proxy-review, --replay-proxy-scoring,
  --calibrate-runtime-review, or --replay-runtime-scoring, and when recommending config
  moves from reviewed sidecars.
---

# HF Eval Operator

Use this skill for evaluation and tuning work around the repo's staged Hugging Face pipeline.

## When To Use

Use this skill when the task involves:
- running `python3 run.py --calibrate-proxy-review ...`
- running `python3 run.py --replay-proxy-scoring ... --trial-proxy-config ...`
- running `python3 run.py --calibrate-runtime-review ...`
- running `python3 run.py --replay-runtime-scoring ... --trial-config ...`
- reading calibration or replay reports and recommending config changes
- comparing current vs trial routing on reviewed sidecars

Do not use this skill for:
- model implementation work in `pipeline/hf_adapters.py`
- review-bridge prep/apply/cleanup flows
- published pack or onboarding work

## Core Workflow

1. Confirm the evaluation surface:
   - proxy sidecars for `hf_multimodal` tuning
   - runtime sidecars for ROI/runtime scoring tuning
2. Prefer reviewed sidecars over unlabeled data.
3. Run calibration first to understand current separation and data quality.
4. Run replay second to compare one explicit trial config.
5. Recommend one of:
   - `prefer_trial`
   - `keep_current`
   - `inconclusive`
6. Explain the decision using report fields, not intuition.

## Command Templates

Proxy calibration:

```bash
python3 run.py --calibrate-proxy-review <SIDECAR_ROOT> --game <GAME> --min-reviewed <N> --output-path <REPORT_JSON> --debug-output-dir <DEBUG_DIR>
```

Proxy replay:

```bash
python3 run.py --replay-proxy-scoring <SIDECAR_ROOT> --trial-proxy-config <TRIAL_PATH> --game <GAME> --min-reviewed <N> --output-path <REPORT_JSON> --debug-output-dir <DEBUG_DIR> --trial-name <NAME>
```

Runtime calibration:

```bash
python3 run.py --calibrate-runtime-review <SIDECAR_ROOT> --game <GAME> --min-reviewed <N> --output-path <REPORT_JSON> --debug-output-dir <DEBUG_DIR>
```

Runtime replay:

```bash
python3 run.py --replay-runtime-scoring <SIDECAR_ROOT> --trial-config <TRIAL_PATH> --game <GAME> --min-reviewed <N> --output-path <REPORT_JSON> --debug-output-dir <DEBUG_DIR> --trial-name <NAME>
```

## Report Fields To Read

Proxy calibration:
- `reviewed_sidecar_count`
- `approved_count`
- `rejected_count`
- `diagnostics.score_distribution`
- `diagnostics.action_outcomes`
- `diagnostics.threshold_diagnostics`
- `diagnostics.signal_incidence`
- `diagnostics.stage_coverage.stage_status_counts`
- `diagnostics.stage_coverage.stage_latency_ms`
- `recommendations.threshold_observations`
- `recommendations.stage_weight_observations`
- `recommendations.data_quality_notes`

Proxy replay:
- `comparison.action_quality`
- `comparison.score_separation`
- `comparison.targeted_errors`
- `comparison.clip_movements`
- `comparison.stage_contribution_deltas`
- `recommendation.decision`
- `recommendation.reason`

Runtime calibration and replay:
- use the same pattern, but read runtime event/detection fields instead of HF stage fields

## Decision Heuristics

Use `prefer_trial` when:
- approved routing improves without worsening rejected routing, or
- rejected false positives drop without harming approved clips

Use `keep_current` when:
- approved routing gets worse, or
- rejected false positives get worse, or
- the score gap regresses materially

Use `inconclusive` when:
- reviewed coverage is below threshold
- approved/rejected populations are badly imbalanced
- current and trial outcomes are effectively tied

Do not recommend config changes from unlabeled coverage alone.

## Common Failure Handling

If calibration returns insufficient review data:
- report the missing coverage clearly
- do not recommend threshold or weight changes yet
- recommend creating more reviewed sidecars through the existing review bridge

If replay rejects the trial config:
- confirm the correct trial path
- confirm only replayable keys are present:
  - proxy: `shortlist_count`, `stage_weights`, `signal_thresholds`
  - runtime: runtime scoring fields only

If sidecars are skipped:
- inspect warnings for:
  - malformed JSON
  - schema mismatch
  - wrong game filter
  - unreviewed sidecars
  - non-HF source on proxy calibration

## Output Standard

When summarizing an eval/replay run, include:
- current objective
- sidecar population and review counts
- strongest approved vs rejected separation signal
- whether the trial changed routing
- final recommendation and why

Keep recommendations narrow. Suggest one trial change at a time unless the report clearly supports a coupled adjustment.

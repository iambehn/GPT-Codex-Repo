# Shadow Approved-Target Warning Plan

## Goal

Remove irrelevant post-performance sparsity warnings from approved-target operator runs and document how to interpret promotion readiness for `approved_or_selected_probability`.

## Steps

1. Update warning emission in `pipeline/shadow_model_training.py`
   - gate `sparse_post_performance_target` on `training_target == "post_performance_score"`

2. Add focused regressions
   - `tests/test_shadow_model_training.py`
   - `tests/test_shadow_operator_workflow.py`

3. Add durable operator guidance
   - update `docs/v2/REVIEW_CALIBRATION_REPLAY.md` with the approved-target promotion rule and the distinction from `post_performance_score`

4. Verify
   - targeted unit tests for model training and operator workflow
   - direct inspection of the new rule text in docs


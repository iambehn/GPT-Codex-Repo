# Shadow Approved-Target Warning Design

## Summary

Focused shadow runs for `approved_or_selected_probability` produced valid `prefer_shadow` decisions, but the operator artifacts still carried `sparse_post_performance_target` warnings from shared dataset rows. Those warnings were accurate about dataset sparsity, but irrelevant to the active target and confusing in promotion-facing output.

This change keeps the approved-target promotion rule explicit and narrows warning emission so only target-relevant sparsity warnings appear in approved-target runs.

## Scope

- Document the target-specific operator interpretation for `approved_or_selected_probability`.
- Suppress `sparse_post_performance_target` warnings when the active training target does not depend on post-performance labels.
- Preserve existing post-performance warning behavior for `post_performance_score` runs.

## Design

### Warning Contract

`pipeline/shadow_model_training.py` currently computes all three target heads for each candidate row and emits `sparse_post_performance_target` whenever the post-performance head is absent. That leaks post-performance sparsity into approved-target and export-target runs.

The warning should become target-aware:

- emit `sparse_post_performance_target` only when `training_target == "post_performance_score"`
- keep `skipped_row_without_target` unchanged for rows that cannot participate in the active target
- preserve downstream aggregation behavior in benchmark and operator artifacts by fixing the source warning list, not by hiding warnings later

### Operator Interpretation

For real-only datasets, `approved_or_selected_probability` may be promotable even when post-performance coverage is sparse.

The operator guidance should state:

- `approved_or_selected_probability` can be promoted from a focused `full` run when:
  - train/evaluate succeeds
  - benchmark review marks the target ready
  - governance coverage is sufficient
  - the policy recommendation is `prefer_shadow`
- `post_performance_score` remains blocked until usable post-performance labels exist

## Verification

- add a model-training regression proving approved-target training does not emit `sparse_post_performance_target`
- add an operator-workflow regression proving a focused approved-target `full` run does not surface irrelevant warnings in train or benchmark steps
- update durable docs with the target-specific promotion rule


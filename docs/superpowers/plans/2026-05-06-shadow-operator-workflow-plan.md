# Shadow Operator Workflow Implementation Plan

## Scope

Implement one standardized shadow operator wrapper that supports:

- `train`
- `benchmark`
- `govern`
- `full`

The implementation must reuse existing shadow modules, write one `shadow_operator_run_v1` summary artifact per run, and expose a compact default CLI surface through `run.py`.

This plan follows the approved spec in:

- `docs/superpowers/specs/2026-05-06-shadow-operator-workflow-design.md`

## Deliverables

### New module

- `pipeline/shadow_operator_workflow.py`

### CLI integration

- `run.py`
- `tests/test_run.py`

### New tests

- `tests/test_shadow_operator_workflow.py`

### New artifact schema

- `shadow_operator_run_v1`

## Implementation Strategy

Build this in four slices, with the public interface and summary artifact shape stabilized first.

### Slice 1: Core workflow module and artifact contract

Add `pipeline/shadow_operator_workflow.py` with:

- mode validation
- input normalization
- operator run id generation
- summary artifact writing
- final status derivation
- final recommendation derivation

Core public API:

- `run_shadow_operator_workflow(...)`

Supporting internal helpers:

- `_normalize_mode(...)`
- `_required_inputs_for_mode(...)`
- `_operator_run_id(...)`
- `_step_result(...)`
- `_final_status(...)`
- `_final_recommendation(...)`
- `_write_operator_artifact(...)`

Initial behavior:

- validate mode
- validate required inputs for the selected mode
- write a valid `shadow_operator_run_v1` artifact even on failure

Tests:

- invalid mode
- missing required input by mode
- `ok` / `partial` / `failed` final status rules
- artifact schema fields present

### Slice 2: `train` and `benchmark` mode execution

Implement real orchestration for:

- `train`
- `benchmark`

`train` step flow:

1. `train_shadow_ranking_model(...)`
2. `evaluate_shadow_ranking_model(...)`

`benchmark` step flow:

1. `run_shadow_benchmark_matrix(...)`
2. `review_shadow_benchmark_results(...)`
3. optional `compare_shadow_benchmark_evidence_modes(...)`

Rules:

- each step writes a normalized `step_result`
- each successful step contributes artifact paths into `produced_artifacts`
- final recommendation should come from the strongest downstream artifact available:
  - experiment comparison for `train`
  - benchmark review for `benchmark`

Tests:

- `train` happy path
- `benchmark` happy path
- failure in first required step
- failure in second required step produces `partial`
- optional evidence comparison omission does not fail benchmark mode

### Slice 3: `govern` and `full` mode execution

Implement:

- `govern`
- `full`

`govern` step flow:

1. `evaluate_shadow_experiment_policy(...)`
2. optional `summarize_shadow_experiment_ledger(...)`

Standalone `govern` requires an explicit experiment manifest. `full` derives that manifest from its `train` stage output.

`full` step flow:

1. `train`
2. `benchmark`
3. `govern`

Rules:

- `full` threads artifact outputs between stages
- downstream stages only run if upstream required dependencies succeeded
- final recommendation in `full` should prefer:
  1. governance recommendation
  2. benchmark review conclusion
  3. train comparison recommendation

Tests:

- `govern` happy path
- `full` happy path
- `full` short-circuits after failed required stage
- `full` still writes `shadow_operator_run_v1` on intermediate failure

### Slice 4: CLI and output integration

Wire the workflow into `run.py`.

Add CLI entrypoint:

- `--run-shadow-operator`

Add/validate args:

- `--mode`
- `--dataset-manifest`
- `--experiment-manifest`
- `--model-path`
- `--model-family`
- `--model-version`
- `--training-target`
- `--target`
- `--policy-path`
- `--game`
- `--platform`
- `--output-root`
- `--output-path`
- `--split-key`
- `--train-fraction`
- `--full-json`

Behavior:

- route through `_print_cli_result(...)`
- compact output by default
- full artifact payload on `--full-json`

Compact payload should include:

- `ok`
- `status`
- `schema_version`
- `operator_run_id`
- `mode`
- `manifest_path`
- `final_summary`
- `final_recommendation`
- `produced_artifacts`
- sampled `step_results`

Tests:

- CLI routing
- compact default output
- `--full-json` passthrough
- mode-specific required argument validation

## Detailed Task Breakdown

### 1. Add workflow module skeleton

- create `pipeline/shadow_operator_workflow.py`
- define schema version constant
- define mode constants
- define normalized artifact writer

Acceptance:

- module imports cleanly
- empty/failure artifact generation works

### 2. Define operator artifact contract

- formalize `shadow_operator_run_v1` fields
- ensure every code path returns a dict matching that contract

Acceptance:

- tests assert stable top-level shape
- failure paths include `step_results`

### 3. Implement `train` mode

- call training/evaluation pipeline
- normalize outputs into step summaries
- derive final recommendation from comparison report

Acceptance:

- train mode produces model + experiment + replay/comparison artifact references

### 4. Implement `benchmark` mode

- call benchmark matrix
- call benchmark review
- optionally call evidence comparison when both real/synthetic manifests are supplied

Acceptance:

- benchmark mode produces benchmark + review references
- optional comparison is truly optional

### 5. Implement `govern` mode

- call experiment policy evaluation
- optionally call ledger summary when registry path is supplied

Acceptance:

- govern mode produces governed ledger reference
- summary is included when available

### 6. Implement `full` mode

- orchestrate `train` -> `benchmark` -> `govern`
- thread artifact outputs between phases

Acceptance:

- full mode can run end to end with one command
- partial failure is explicit and non-destructive

### 7. Wire CLI

- add parser flags
- add command route
- add compact output support

Acceptance:

- command is discoverable in `--help`
- stdout follows compact/full-json policy

### 8. Add tests

- module tests in `tests/test_shadow_operator_workflow.py`
- CLI tests in `tests/test_run.py`

Acceptance:

- focused workflow tests pass
- CLI route/output tests pass

## Test Plan

### Focused unit/integration suite

Run at minimum:

```bash
python3 -m py_compile \
  pipeline/shadow_operator_workflow.py \
  run.py \
  tests/test_shadow_operator_workflow.py \
  tests/test_run.py
```

```bash
python3 -m unittest \
  tests.test_shadow_operator_workflow \
  tests.test_run
```

### Recommended adjacent sweep

Because this wrapper composes existing shadow surfaces, re-run:

```bash
python3 -m unittest \
  tests.test_shadow_model_training \
  tests.test_shadow_ranking_replay \
  tests.test_shadow_benchmark_matrix \
  tests.test_shadow_benchmark_review \
  tests.test_shadow_benchmark_evidence_comparison \
  tests.test_shadow_evaluation_policy
```

## Risks

### Risk 1: Wrapper duplicates business logic

Mitigation:

- keep the wrapper as orchestration only
- derive summaries from returned artifacts instead of recomputing model logic

### Risk 2: `full` mode becomes brittle

Mitigation:

- require explicit step dependency checks
- stop after failed required step
- persist partial artifact state

### Risk 3: Output contract drifts into another oversized CLI payload

Mitigation:

- make compact output part of the implementation, not a follow-up
- add direct tests for compact vs `--full-json`

### Risk 4: Mode input requirements become confusing

Mitigation:

- centralize required-argument validation by mode
- keep help text short and explicit

## Out of Scope Follow-Ups

These are intentionally deferred:

- registry ingestion for `shadow_operator_run_v1`
- scheduled or queued execution
- UI/dashboard for operator runs
- policy changes for promotion/gating
- adding new model families or benchmark targets

## Recommended Commit Structure

1. `Add shadow operator workflow module`
2. `Implement shadow operator train and benchmark modes`
3. `Implement shadow operator govern and full modes`
4. `Add shadow operator CLI and compact output`

This keeps the work reviewable and makes regressions easier to isolate.

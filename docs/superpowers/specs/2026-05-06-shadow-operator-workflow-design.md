# Shadow Operator Workflow Design

## Summary

This spec defines one standardized operator wrapper for the shadow model and benchmark stack. The wrapper should orchestrate the existing shadow commands, support multiple modes from day one, and write one summary artifact per run.

The goal is not to replace the current artifacts. The goal is to make the workflow operable without manual command choreography while preserving the existing artifact-first design.

## Goals

- Provide one operator-facing CLI entrypoint for shadow train, benchmark, govern, and full runs.
- Reuse existing shadow modules instead of introducing a parallel orchestration subsystem.
- Write one summary artifact that links all produced artifacts, key metrics, and final recommendation state.
- Keep default CLI output compact.
- Preserve step-level failure visibility.

## Non-Goals

- No new model families, training logic, or policy logic.
- No registry ingestion in V1.
- No changes to the underlying replay, benchmark, or governance artifact schemas unless required for wrapper interoperability.
- No workflow scheduling, queue ownership, or background execution.

## Operator Problem

The repo now has usable shadow primitives:

- `train_shadow_ranking_model(...)`
- `evaluate_shadow_ranking_model(...)`
- `run_shadow_benchmark_matrix(...)`
- `review_shadow_benchmark_results(...)`
- `compare_shadow_benchmark_evidence_modes(...)`
- `evaluate_shadow_experiment_policy(...)`
- `summarize_shadow_experiment_ledger(...)`

But the operator still has to know the right sequence, remember which output feeds the next step, and inspect several manifests to understand the final state. That is too much manual coordination for repeated evaluation work.

## Proposed Entry Point

Add one CLI wrapper in `run.py`:

- `--run-shadow-operator`

Required selector:

- `--mode train|benchmark|govern|full`

The wrapper should reuse existing CLI arguments where possible:

- `--dataset-manifest`
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

This keeps the wrapper aligned with the existing command vocabulary and avoids a second set of operator-only flags.

## Modes

### `train`

Runs:

1. `train_shadow_ranking_model(...)`
2. `evaluate_shadow_ranking_model(...)`
3. implicit replay comparison through the experiment output

Expected primary output:

- model manifest
- experiment manifest
- replay manifest
- replay comparison report

### `benchmark`

Runs:

1. `run_shadow_benchmark_matrix(...)`
2. `review_shadow_benchmark_results(...)`
3. optional `compare_shadow_benchmark_evidence_modes(...)` when both real and synthetic comparison inputs are available

Expected primary output:

- benchmark matrix manifest
- benchmark review manifest
- optional evidence comparison manifest

### `govern`

Runs:

1. `evaluate_shadow_experiment_policy(...)`
2. `summarize_shadow_experiment_ledger(...)` when registry-backed summary context is available

Expected primary output:

- governed ledger manifest
- optional ledger summary payload

### `full`

Runs the standard operator progression:

1. train
2. benchmark
3. govern

This mode is orchestration only. It does not invent new shadow behavior. It just threads outputs between the existing steps and records one top-level summary.

## Summary Artifact

Add one new artifact schema:

- `shadow_operator_run_v1`

Recommended fields:

- `ok`
- `status`
- `schema_version`
- `operator_run_id`
- `created_at`
- `mode`
- `inputs`
- `filters`
- `step_results`
- `produced_artifacts`
- `final_summary`
- `final_recommendation`

### `inputs`

Stores normalized operator inputs:

- dataset manifest
- model path
- model family
- model version
- training target
- evaluation target
- policy path
- game
- platform
- split key
- train fraction

### `step_results`

One object per executed step:

- `step_name`
- `status`
- `error`
- `artifact_path`
- `summary`

`summary` should be small and machine-usable. It should contain the few metrics needed for operator review, not the full nested payload.

### `produced_artifacts`

Explicit links to generated outputs, for example:

- model manifest
- experiment manifest
- replay manifest
- replay comparison report
- benchmark matrix manifest
- benchmark review manifest
- evidence comparison manifest
- governed ledger manifest

### `final_summary`

One compact rollup intended for both CLI output and follow-on automation:

- executed step count
- successful step count
- failed step count
- warning count
- primary model family
- primary training target
- primary evaluation target
- benchmark readiness classification when available
- governance recommendation decision when available

### `final_recommendation`

One normalized final operator conclusion:

- `decision`
- `reason`
- `supporting_artifacts`
- `follow_up`

The wrapper should not invent policy. It should derive this conclusion from the strongest downstream artifact available for the selected mode.

## Failure Model

The wrapper must not hide partial execution.

Final statuses:

- `ok`
- `partial`
- `failed`

Rules:

- `ok`: all required steps for the selected mode succeeded
- `partial`: at least one earlier step succeeded and at least one later required step failed
- `failed`: no required step produced usable output

Each failed step must preserve:

- original step status
- error message
- artifact path if a partial artifact exists

The wrapper should stop on the first failed required dependency in a chain, but still write the summary artifact.

## Output Behavior

The wrapper command should use the repo’s compact CLI pattern by default:

- counts
- key artifact paths
- final recommendation
- small samples only when needed

`--full-json` should emit the full operator summary artifact payload.

This keeps the new operator flow consistent with the recent CLI-output cleanup work.

## Implementation Approach

### Option A: Thin wrapper around existing functions

- call existing `run_*` helpers in `run.py`
- normalize their outputs into one summary artifact

Pros:

- lowest implementation risk
- minimal new logic
- easy to keep aligned with current CLI behavior

Cons:

- `run.py` grows further unless the orchestration helper is extracted

### Option B: New orchestration module plus `run.py` route

- add `pipeline/shadow_operator_workflow.py`
- `run.py` becomes a thin route layer

Pros:

- cleaner long-term boundary
- easier direct test coverage

Cons:

- slightly more new structure up front

### Option C: Registry-backed orchestration from day one

- wrapper writes summary plus registry rows

Pros:

- best long-term queryability

Cons:

- too much scope for V1
- mixes operator ergonomics with new persistence contracts

### Recommendation

Choose Option B.

The wrapper is meaningful enough to deserve its own module, and it avoids turning `run.py` into the only place where the workflow exists. This keeps the orchestration boundary testable while still reusing current pipeline functions.

## Module Design

Add:

- `pipeline/shadow_operator_workflow.py`

Core public function:

- `run_shadow_operator_workflow(...)`

Responsibilities:

- validate mode and required inputs
- execute the selected step sequence
- record per-step status and summaries
- derive final status and final recommendation
- write `shadow_operator_run_v1`

`run.py` responsibilities:

- parse `--run-shadow-operator`
- validate obvious CLI requirements
- call `run_shadow_operator_workflow(...)`
- print compact or full JSON

## Testing

### Unit tests

- mode validation and required-argument validation
- final status derivation:
  - `ok`
  - `partial`
  - `failed`
- final recommendation derivation for each mode

### Integration tests

- `train` mode writes summary and references model/experiment/replay artifacts
- `benchmark` mode writes summary and references benchmark/review artifacts
- `govern` mode writes summary and references ledger artifacts
- `full` mode chains all required steps and records downstream artifacts
- failure in an intermediate step still produces `shadow_operator_run_v1`

### CLI tests

- route coverage in `tests/test_run.py`
- compact default output
- `--full-json` passthrough

## Open Decisions Resolved

- Multiple modes are supported from day one.
- The primary operator surface is a wrapper command, not a runbook-only flow.
- V1 writes a summary artifact but does not ingest it into the registry.
- The wrapper should live in a dedicated pipeline module, not only in `run.py`.

## Recommended First Implementation Slice

Implement in this order:

1. `pipeline/shadow_operator_workflow.py` with mode validation and summary artifact writing
2. support `train` and `benchmark` modes first inside the module
3. add `govern`
4. add `full`
5. wire `run.py`
6. add compact CLI output and tests

This sequencing reduces risk while still landing the multi-mode contract in one feature branch.

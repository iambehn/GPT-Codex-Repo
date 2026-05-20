# Call Of Duty Canonical Human Review Handoff

## Purpose

Status:

- partially completed on 2026-05-21
- fused-review decisions for the canonical `60s-70s` sample were adopted by the user and re-applied
- local export is now canonically human-reviewed for local testing
- runtime calibration is still broader and remains bootstrap-derived

The `call_of_duty` bounded clip at:

- `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`

is now the canonical operator sample for local pipeline testing.

What is still not canonical:

- runtime review labels
- fused review labels
- the runtime calibration artifact derived from bootstrap GPT review
- the local export artifact derived from bootstrap GPT review

Those remain test-only until a human reviewer replaces the bootstrap GPT decisions.

## Current Canonical-Test Artifacts

### Runtime Review Session

- manifest:
  - `outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json`
- session id:
  - `call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4`
- item count:
  - `4`
- counts:
  - `approved_count: 2`
  - `rejected_count: 2`
- GPT repo:
  - `/Users/tj/GPT-Codex-Repo`

Relevant canonical-sample runtime item:

- source:
  - `outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4`
- sidecar:
  - `outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s-c40d17236088.runtime_analysis.json`
- GPT meta:
  - `/Users/tj/GPT-Codex-Repo/inbox/call_of_duty/runtime-review-fbfbc59cafa4-001-svbtc2azzyw-60s-70s.meta.json`

Important limitation:

- the current runtime calibration artifact depends on all four reviewed runtime items in this session, not only the canonical `60s-70s` sample

### Fused Review Session

- manifest:
  - `outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json`
- session id:
  - `call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c`
- item count:
  - `2`
- counts:
  - `approved_count: 1`
  - `rejected_count: 1`
- GPT repo:
  - `/Users/tj/GPT-Codex-Repo`

Relevant fused-review GPT meta files:

- `/Users/tj/GPT-Codex-Repo/inbox/call_of_duty/fused-review-19e9d6dcf48c-000-svbtc2azzyw-60s-70s.meta.json`
- `/Users/tj/GPT-Codex-Repo/inbox/call_of_duty/fused-review-19e9d6dcf48c-001-svbtc2azzyw-60s-70s.meta.json`

Shared fused sidecar:

- `outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json`

### Downstream Bootstrap Outputs

Current runtime calibration artifact:

- `outputs/runtime_calibration/call_of_duty/bootstrap-real-cod.runtime_calibration.json`

Current local export artifact:

- `outputs/highlight_exports/call_of_duty/bootstrap-real-cod.highlight_export_batch.json`

Current isolated registry:

- `outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite`

## Human Review Rule

Human review replaces bootstrap GPT review by editing the existing GPT meta files and then re-running the corresponding `apply` command.

This is safe in the current implementation:

- `apply_runtime_review(...)` reloads the GPT meta file and overwrites the sidecar `runtime_review` block
- `apply_fused_review(...)` reloads the GPT meta file and overwrites the sidecar `fused_review.events[...]` entry

Cleanup is not rollback and should not be used as rollback.

## Minimum Manual Review Path

If the immediate goal is to make the local export artifact canonical:

1. Human-review the two fused-review meta files for the `60s-70s` sample.
2. Re-apply the fused review session.
3. Rebuild the downstream local export artifacts.

Completion note:

- this path is now complete for the current canonical sample
- the fused sidecar review timestamps were updated to `2026-05-20T23:44:27Z`
- the exported candidate remained in `lifecycle_state: exported`, so no new export-queue item was created
- the existing local export artifact remains the canonical local export output

If the immediate goal is to make the runtime calibration artifact canonical:

1. Human-review the relevant runtime-review meta files.
2. Re-apply the runtime review session.
3. Re-run runtime calibration.

## Recommended Commands

### Re-apply Human-Reviewed Runtime Decisions

```bash
source .venv/bin/activate
python run.py --apply-runtime-review \
  outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json \
  --gpt-repo /Users/tj/GPT-Codex-Repo
```

### Re-apply Human-Reviewed Fused Decisions

```bash
source .venv/bin/activate
python run.py --apply-fused-review \
  outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json \
  --gpt-repo /Users/tj/GPT-Codex-Repo
```

### Rebuild Canonical Runtime Calibration

```bash
source .venv/bin/activate
python run.py --calibrate-runtime-review \
  outputs/runtime_analysis/call_of_duty \
  --game call_of_duty \
  --output-path outputs/runtime_calibration/call_of_duty/bootstrap-real-cod.runtime_calibration.json \
  --debug-output-dir outputs/runtime_calibration/call_of_duty/bootstrap-real-cod.debug
```

### Rebuild Canonical Local Export Path

```bash
source .venv/bin/activate
python run.py --export-highlight-selection \
  --fused-sidecar outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json \
  --output-path outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.highlight_selection.json

source .venv/bin/activate
python run.py --refresh-clip-registry . \
  --registry-path outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite

source .venv/bin/activate
python run.py --create-workflow-run \
  --workflow-type export_queue \
  --registry-path outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite \
  --game call_of_duty \
  --output-path outputs/workflow_runs/call_of_duty/bootstrap-real-cod.export_queue.workflow_run.json

source .venv/bin/activate
python run.py --create-highlight-export-batch \
  --registry-path outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite \
  --workflow-run-id workflow-11aea2937311834b \
  --output-path outputs/highlight_exports/call_of_duty/bootstrap-real-cod.highlight_export_batch.json
```

## Important Nuance

The current runtime calibration artifact is broader than the canonical sample. It reflects four reviewed runtime sidecars:

- `SVbTc2AZzYw.mp4`
- `SVbTc2AZzYw.60s-70s.mp4`
- `SVbTc2AZzYw.10s.mp4`
- `SVbTc2AZzYw.40s-50s.mp4`

So:

- the current fused/export path can be canonicalized by human review on the canonical `60s-70s` sample
- the current calibration artifact only becomes canonical if the runtime review decisions it depends on are also human-reviewed

If a narrower calibration artifact is preferred later, generate a narrower reviewed runtime set first rather than treating the existing four-item calibration result as sample-specific.

## Completion Condition

The local `call_of_duty` path should be considered canonically human-reviewed only when:

- the relevant GPT meta files have human decisions
- the corresponding `apply` command has been rerun
- downstream calibration and/or export artifacts have been regenerated from those human-reviewed decisions
- `python run.py --run-repo-quality-health` is still green

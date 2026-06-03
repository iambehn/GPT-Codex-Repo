# Pipeline Contracts

Status: active-draft
Version: 0.3
Last updated: 2026-05-21

This is the minimal contract layer for the first happy path. It does not replace subsystem docs under `docs/v2/`; it pins the minimum execution expectations needed to keep Codex work narrow and auditable.

## Validation Levels

- `L0`: file exists
- `L1`: file parses
- `L2`: required fields are present
- `L3`: downstream stage can consume it
- `L4`: artifact is human-inspectable and semantically meaningful

## Cross-Stage Rules

- Do not treat `L0` success as stage completion.
- Every produced artifact should retain provenance when the owning surface supports it.
- Status fields must be owned by an existing repo surface, not invented ad hoc in a task slice.
- If an artifact can be produced but not consumed downstream, the stage is not complete.

## Minimal Stage Contract Matrix

| Stage | Inputs | Outputs | Minimum required fields or properties | Validation | Owning repo surface |
| --- | --- | --- | --- | --- | --- |
| Config load | `--config` path or default config root | validated config object or explicit error | parseable config, normalized values, explicit validation failure on bad input | `L1-L2` | `pipeline/config/` and `run.py` bootstrap |
| Input resolution | chosen game, chosen input mode, chosen input path or fixture path | explicit resolved input context | game, input mode, source path or explicit blocker | `L1-L2` | Phase 0 inventory must pin owner |
| Sidecar/artifact generation | input context plus runtime commands | stage artifact or sidecar | parseable payload, stage identity, enough fields for downstream review/replay consumption | `L1-L3` | Phase 0 inventory must pin exact surface per artifact |
| Candidate/review surface | sidecar or stage artifact | inspectable review or candidate payload | status, evidence context, stable record identity, downstream-readable structure | `L1-L4` | review bridge and highlight review surfaces |
| Calibration/replay | reviewable artifact plus calibration or replay command | replay or calibration result | parseable result, explicit success/failure, inspectable warnings or deltas | `L1-L4` | replay/calibration surfaces under `pipeline/` and `tools/` |
| Local export-readiness | reviewed or chosen artifact state | local readiness bundle | blockers, review state, local output location, `platform_action_taken: false` | `L1-L4` | Phase 0 inventory must pin exact owner |
| Repo-quality health | current repo state | health summary | maintenance result, decision regression result, overall gate result | `L1-L4` | `python run.py --run-repo-quality-health` |

## Generic Artifact Expectations

The exact schema is owned by the relevant repo surface. Until Phase 0 inventory pins each owner, the following generic expectations apply whenever the owning format permits them:

- stable record or artifact identity
- source or input provenance
- explicit stage or artifact type
- schema version or equivalent contract identifier
- review, lifecycle, or decision status where applicable
- timestamps when the stage is time-sensitive
- enough evidence context for a human to understand what the artifact represents

## Current Bootstrap Command Mapping

This is the current command-to-artifact contract for the bootstrap path. It does not replace the final real-media happy path.

| Stage | Command | Artifact or output surface | Required fields or properties now verified | Validation level |
| --- | --- | --- | --- | --- |
| Config load | `python run.py --list-games --config config.yaml` | CLI JSON result with game list | `ok`, non-empty `games` list | `L1-L2` |
| Input resolution | pinned draft root `assets/games/call_of_duty/drafts/onboarding/20260505T213332Z` | explicit bootstrap source path | chosen game, input mode, source path | `L1-L2` |
| Candidate/review bootstrap | `python run.py --summarize-derived-row-review assets/games/call_of_duty/drafts/onboarding/20260505T213332Z` | derived-row review summary | `status`, `review_file_count`, `pending_count`, `decision_ready_count`, `applied_count` | `L1-L4` |
| Calibration/replay bootstrap | `python run.py --run-decision-regression-goldsets` | decision-regression summary | `suite_count`, `total_tests`, `ok`, per-suite status | `L1-L4` |
| Local readiness bootstrap | `python run.py --validate-onboarding-publish assets/games/call_of_duty/drafts/onboarding/20260505T213332Z` | onboarding publish-readiness summary | `phase_status`, `can_publish`, `readiness`, `counts`, inspectable findings | `L1-L4` |

Current bounded real-media proof:

- `runtime_analysis_v1`: `outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s-c40d17236088.runtime_analysis.json`
- `fused_analysis_v1`: `outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json`
- `highlight_selection_v1`: `outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.highlight_selection.json`
- `workflow_run_v1`: `outputs/workflow_runs/call_of_duty/bootstrap-real-cod.export_queue.workflow_run.json`
- `highlight_export_batch_v1`: `outputs/highlight_exports/call_of_duty/bootstrap-real-cod.highlight_export_batch.json`

Current limitation:

- this proof uses downloaded public test media and bootstrap GPT review labels; it is an execution proof, not a publish-cleared production contract

Runtime-sidecar coordinate note:

- `runtime_analysis_v1` matcher coordinates are reported in pack-normalized frame space, not source-video pixel space
- read `matcher.frame_dimensions` and `matcher.frame_coordinate_space` before interpreting `frame_match_x` or `frame_match_y`
- downstream summaries that preserve matcher geometry should keep those fields instead of assuming source-resolution pixels

## Planned Real-Media Command Mapping

These commands are the current preferred continuation and now have one bounded real-media proof.

| Stage | Command | Expected artifact or output surface | Current execution truth |
| --- | --- | --- | --- |
| Sidecar generation | `python run.py --analyze-roi-runtime <SOURCE> call_of_duty` | `runtime_analysis_v1` sidecar under `outputs/runtime_analysis/call_of_duty/` unless overridden | proved on bounded public test media |
| Review path | `python run.py --prepare-runtime-review call_of_duty` plus `python run.py --apply-runtime-review <SESSION_MANIFEST>` | `runtime_review_session_v1` plus persisted `runtime_review` blocks on sidecars | proved on bounded public test media |
| Replay or calibration | `python run.py --calibrate-runtime-review outputs/runtime_analysis/call_of_duty --game call_of_duty` | runtime calibration report over reviewed sidecars | proved on bounded public test media |
| Fusion path | `python run.py --fuse-clip-signals <SOURCE> call_of_duty --runtime-sidecar <RUNTIME_SIDECAR>` | `fused_analysis_v1` sidecar under `outputs/fused_analysis/call_of_duty/` unless overridden | required because runtime sidecars alone do not feed local export |
| Fused review path | `python run.py --prepare-fused-review call_of_duty ...` plus `python run.py --apply-fused-review <SESSION_MANIFEST>` | `fused_review_session_v1` plus persisted `fused_review.events` | proved on bounded public test media and propagates lifecycle state |
| Local export surface | `python run.py --export-highlight-selection --fused-sidecar <FUSED_SIDECAR>` then `python run.py --create-workflow-run --workflow-type export_queue ...` then `python run.py --create-highlight-export-batch ...` | `highlight_selection_v1`, `workflow_run_v1`, then `highlight_export_batch_v1` | proved on bounded public test media |

## Phase 0 Ownership Snapshot

| Surface | Current owner |
| --- | --- |
| Published game-pack manifests and completeness rules | [docs/v2/MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md), `pipeline/game_pack.py`, `assets/games/<game>/manifests/` |
| Candidate lifecycle, export status, post status, and queryable cross-workflow state | [docs/v2/REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md), `pipeline/clip_registry.py` |
| Proxy review session schema `proxy_review_session_v1` | `pipeline/proxy_review_bridge.py` |
| Runtime review session schema `runtime_review_session_v1` | `pipeline/runtime_review_bridge.py` |
| Fused review session schema `fused_review_session_v1` | `pipeline/fused_review_bridge.py` |
| Onboarding identity review session schema `onboarding_identity_review_session_v1` | `pipeline/onboarding_identity_review_bridge.py` |
| Derived row review schema `derived_row_review_v1` | `pipeline/derived_row_review.py` |
| Local export batch schema `highlight_export_batch_v1` | `pipeline/highlight_export_batch.py` |
| Local posted-ledger schema `posted_highlight_ledger_v1` | `pipeline/highlight_export_batch.py` |
| Local posted-metrics schema `posted_highlight_metrics_snapshot_v1` | `pipeline/highlight_export_batch.py` |
| Onboarding publish-readiness semantics | `pipeline/onboarding_publish_readiness.py` and onboarding draft manifests |

Current ownership conclusion:

- no immediate cross-surface schema conflict was found in the candidate, review-session, lifecycle, or export artifact families reviewed in Phase 0
- the local export boundary is `highlight_export_batch_v1`
- the publication boundary begins at `posted_highlight_ledger_v1`
- the repo does not currently expose an explicit `platform_action_taken: false` field; the operative contract is that a local export batch exists without any posted ledger yet
- runtime analysis alone is not the local export boundary; the current export path requires fused selection plus lifecycle propagation
- the remaining ambiguity is not artifact ownership; it is whether the current bootstrap sample and review labels should be promoted into a canonical operator sample

## Semantic Success Rule

A stage counts as complete only when:

- its command ran
- its output exists
- the output parses
- required fields are present
- the next stage can consume it
- the output is inspectable enough to explain success or failure

## Known Health Gate Contract

The repo-quality health gate is already a known execution-control surface:

- command: `python run.py --run-repo-quality-health`
- current known meaning:
  - maintenance must be healthy enough for the gate
  - decision-regression suites must pass

# Pipeline Contracts

Status: active-draft
Version: 0.2
Last updated: 2026-05-20

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

Current deferred gap:

- no command-backed sidecar-generation proof is pinned yet because there is still no canonical real-media input

## Planned Real-Media Command Mapping

These commands are the current preferred continuation once a canonical `call_of_duty` sample clip exists.

| Stage | Command | Expected artifact or output surface | Reason this is the preferred next proof |
| --- | --- | --- | --- |
| Sidecar generation | `python run.py --analyze-roi-runtime <SOURCE> call_of_duty` | `runtime_analysis_v1` sidecar under `outputs/runtime_analysis/call_of_duty/` unless overridden | narrowest existing media-to-sidecar runtime path |
| Review path | `python run.py --prepare-runtime-review call_of_duty` | `runtime_review_session_v1` manifest under `outputs/runtime_review_sessions/call_of_duty/` | consumes the default runtime sidecar root directly |
| Replay or calibration | `python run.py --calibrate-runtime-review outputs/runtime_analysis/call_of_duty --game call_of_duty` | runtime calibration report over reviewed sidecars | first runtime proof that does not require a separate trial config |

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
- the remaining ambiguity is not schema naming; it is which real-media export surface should replace onboarding publish-readiness as the final Phase 5 completion artifact

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

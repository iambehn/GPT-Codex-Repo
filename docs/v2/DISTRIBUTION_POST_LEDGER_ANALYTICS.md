# Distribution / Post Ledger / Analytics

This document is the canonical V2 home for downstream posting and performance state.

## Canonical Scope

Use this doc for:

- post ledger boundaries
- downstream routing and posting policy
- performance metrics and feedback surfaces
- relationship between approved clips and posts

Do not use this doc for:

- upstream runtime event truth
- source-enrichment matching logic
- raw hook taxonomy design

## Current V2 Position

Distribution is downstream of a stronger candidate pipeline.

Stable policies:

- approved clip and candidate artifacts remain upstream authority
- one clip may map to many posts
- post records need their own lifecycle, metrics, failures, and moderation context
- vendor tooling is acceptable early for publishing and inbox operations
- analytics should feed learning loops without redefining upstream evidence contracts
- downstream reporting should preserve candidate, hook, export, and post lineage as one inspectable chain

Minimum downstream concepts:

- post ledger
- post metrics
- comment or moderation state
- experiment linkage
- destination-aware routing once candidate trust is high enough

## How To Think About The Downstream Layer

This layer exists because “we exported a clip” is not the same thing as “we posted it” and not the same thing as “we learned anything from its performance.”

Downstream is trying to answer three different kinds of question:

1. export state
- what local artifact did we create?

2. posting state
- what was actually sent to an external platform?

3. performance state
- what happened after posting?

Those distinctions matter because external action, platform-side state, and post-performance evidence should not redefine upstream event truth.

## What The Post Ledger Is Actually Doing

The post ledger is the first explicit record that something moved from local pipeline state into an external platform action.

That means the post ledger is not just another export artifact.

It answers:

- what export was posted?
- to which platform or account?
- when was it posted?
- what external record should we treat as the downstream source of truth?

Without that boundary, the system can accidentally treat local export as if it were already published.

## What Metrics Are Actually Doing

Metrics are not approval signals for event truth.

They are downstream observations about how a posted piece of content performed after the fact.

Metrics can help answer:

- did this packaging strategy tend to work?
- did this hook mode underperform?
- do certain event types travel well to a platform?

But metrics should not be allowed to silently rewrite:

- what happened in the clip
- whether the review decision was correct
- whether a candidate was truthfully represented

That is why analytics are downstream learning surfaces rather than upstream evidence surfaces.

## Current Concrete Example

The bounded `call_of_duty` local-test path proved the boundary cleanly:

- a local export batch exists
- the candidate lifecycle can reach `exported`
- `post_ledger_path` remains `null`

That is the important local-only state.

It proves:

- the system can materialize a local output artifact
- but no external-action record exists yet

This is exactly why the downstream layer needs its own explicit artifacts instead of letting export state stand in for posting state.

## Where Complex Problems Usually Hide Here

### 1. Export and posting get conflated

Symptoms:

- a clip is treated as posted when only a local export exists
- downstream reporting appears populated without any true external action

Typical cause:

- the post-ledger boundary was not kept explicit enough

### 2. Performance evidence gets overtrusted

Symptoms:

- strong or weak metrics start getting treated as if they proved upstream event quality

Typical cause:

- the learning surface is being confused with the evidence surface

### 3. One candidate maps to multiple downstream artifacts and lineage becomes unclear

Symptoms:

- exports, posts, and metrics exist
- but it is hard to tell which downstream rows belong to which upstream candidate or hook treatment

Typical cause:

- lineage joins are incomplete
- downstream records are not preserving the right export or candidate references

### 4. Real-world platform state leaks backward into workflow assumptions

Symptoms:

- a moderation issue, posting failure, or platform-specific quirk starts changing how upstream evidence is interpreted

Typical cause:

- downstream operational problems are being allowed to redefine upstream truth instead of being recorded as downstream state

## Practical Mental Model

Use this short model:

- export artifacts say what we created locally
- post ledgers say what we actually sent outward
- metrics say what happened afterward
- analytics should teach later strategy without rewriting earlier evidence

If those functions stay separate, the project can learn from downstream performance without corrupting upstream decision logic.

## Canonical Downstream Chain

The current downstream artifact sequence is:

1. approved or selected fused candidate
2. hook candidate
3. highlight-selection manifest
4. highlight export batch
5. posted highlight ledger
6. posted metrics snapshot

One candidate may fan out into multiple exports, posts, and metrics captures. Those downstream records must stay traceable without changing upstream candidate truth.

## Query And Reporting Expectations

Current operator surfaces are registry- and report-first:

- `post-ledger-records` exposes explicit posted rows
- `posted-metrics` exposes append-only metrics snapshots with latest-snapshot and coverage context
- `posted-performance-rollups` exposes aggregate buckets by platform, game, hook archetype, hook mode, and packaging strategy

Analytics are descriptive only in the current horizon:

- no lifecycle advancement from metrics
- no ranking or packaging changes driven by metrics
- no requirement for a dashboard before the artifact and registry model is stable

## Example Operational Flow

The compact expected operator flow is:

- review and approve a fused candidate
- select it for export
- materialize an export batch
- record a post ledger after publication
- append metrics snapshots over time
- query row-level lineage or aggregate rollups from the registry

## Real Artifact Intake

The canonical local intake root for real downstream evidence is:

- `outputs/real_artifact_intake/`

Expected bundle layout:

- `outputs/real_artifact_intake/bundles/<bundle_name>/fused/*.fused_analysis.json`
- optional `hooks/*.hook_candidates.json`
- optional `selection/*.highlight_selection.json`
- `exports/*.highlight_export_batch.json`
- `posted/*.posted_highlight_ledger.json`
- `metrics/*.posted_highlight_metrics_snapshot.json`

Minimum viable real benchmark bundle:

- one fused artifact
- one export batch
- one post ledger
- one metrics snapshot

Synthetic files must not be mixed into these bundles.

Operator flow:

- run `python3 run.py --bootstrap-real-artifact-intake-bundle --bundle-name <bundle_name>` when starting a new real bundle
- fill in `outputs/real_artifact_intake/bundles/<bundle_name>/bundle.manifest.json`
- place real artifacts into `outputs/real_artifact_intake/bundles/<bundle_name>/`
- run `python3 run.py --validate-real-artifact-intake --game <game> --platform <platform>`
- run `python3 run.py --summarize-real-artifact-intake --game <game> --platform <platform>` to inspect bundle readiness and dominant gaps
- run `python3 run.py --report-real-artifact-intake-coverage --game <game> --platform <platform>` to inspect cross-bundle overlap and eligible-label contribution before refresh
- run `python3 run.py --preflight-real-artifact-intake-refresh --game <game> --platform <platform>` for a compact go/no-go refresh check
- run `python3 run.py --record-real-artifact-intake-preflight-history --game <game> --platform <platform>` to persist that preflight result into local history
- run `python3 run.py --summarize-real-artifact-intake-preflight-history --game <game> --platform <platform>` to inspect readiness drift over time
- run `python3 run.py --report-real-artifact-intake-preflight-trends --game <game> --platform <platform>` to inspect whether real-only readiness is improving or stuck over time
- run `python3 run.py --record-real-artifact-intake-refresh-outcome-history --game <game> --platform <platform>` to persist one full real-only refresh outcome and review result
- run `python3 run.py --summarize-real-artifact-intake-refresh-outcome-history --game <game> --platform <platform>` to inspect recorded real-only refresh outcomes
- run `python3 run.py --report-real-artifact-intake-refresh-outcome-trends --game <game> --platform <platform>` to inspect whether real-only benchmark outcomes are improving over time
- run `python3 run.py --report-real-artifact-intake-history-comparison <comparison_manifest> --game <game> --platform <platform>` to join intake health trends, real-only refresh trends, and saved real-vs-synthetic benchmark disagreement
- run `python3 run.py --render-real-artifact-intake-dashboard <comparison_manifest> --game <game> --platform <platform>` to render one compact current-state dashboard artifact for the intake root
- run `python3 run.py --advise-real-artifact-intake-dedup --game <game> --platform <platform>` to emit canonical-bundle cleanup recommendations for duplicate downstream lineage
- run `python3 run.py --materialize-real-artifact-intake-dedup-resolutions --game <game> --platform <platform>` to create one pending resolution file per advisory group
- run `python3 run.py --update-real-artifact-intake-dedup-resolution --group-id <group_id> --resolution-status accepted|ignored --reviewed-by <operator> --notes <text> --game <game> --platform <platform>` to record the operator decision for a duplicate group
- run `python3 run.py --summarize-real-artifact-intake-dedup-resolutions --game <game> --platform <platform>` to see which duplicate groups are still unresolved
- fix or add bundles if the intake is not yet benchmark-ready
- run `python3 run.py --refresh-real-artifact-intake --game <game> --platform <platform>`
- add `--require-resolved-dedup` to the refresh command when you want unresolved duplicate groups to block the real-only refresh instead of surfacing as a warning
- add `--compare-evidence-on-refresh <synthetic_review_or_benchmark_manifest> --record-dashboard-summary-history-on-refresh --record-refresh-outcome-history-on-refresh --render-dashboard-on-refresh --refresh-artifact-registry-on-refresh` when you want one refresh command to also emit the latest real-vs-synthetic comparison, render the dashboard, record both history layers, and make the resulting reports immediately queryable from the intake-root registry
- optionally compare the resulting real-only review against the synthetic-augmented track with `--compare-shadow-benchmark-evidence-modes`

Bundle manifest linting:

- `bundle.manifest.json` dates must use `YYYY-MM-DD`
- manifest `game` and `platform` must match the validation filters
- declared `expected_artifact_types` must match what is actually staged in the bundle

Key readiness states:

- `benchmark_ready`: real bundle is usable for standalone real-only refresh
- `lineage_complete_without_required_metadata`: evidence is present, but `bundle.manifest.json` is missing or incomplete
- `lineage_complete_without_eligible_metrics`: files are present, but no usable real post-performance labels exist yet
- `partial_lineage`: some required benchmark files are missing
- `downstream_only`: downstream evidence exists without fused candidate lineage

## What Belongs Elsewhere

- Candidate review state belongs in [REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md).
- Hook packaging logic belongs in [HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md).
- Calibration and replay outcomes belong in [REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md) and the experiment ledger.

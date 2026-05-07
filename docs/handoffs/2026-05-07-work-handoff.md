# 2026-05-07 Work Handoff

## Repo State

- Repo: `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo`
- Branch: `codex-v2-registry-expansion`
- Worktree status at handoff: clean

Recent commits, newest first:

- `1765349` `Add accepted proxy review prep wrapper`
- `7a50d93` `Add accepted proxy review prep plan`
- `e35fd9b` `Add accepted proxy review prep design spec`
- `fc0a83a` `Add accepted fixture trial batch runner`
- `3e04218` `Add accepted fixture trial batch plan`
- `ed89364` `Add accepted fixture trial batch design spec`
- `d4fb016` `Add accepted intake source manifest adapter`
- `19a4a16` `Add accepted intake source manifest adapter plan`
- `2396f90` `Add accepted intake source manifest adapter design spec`
- `89a6a16` `Add accepted clip intake manifest builder`
- `208a425` `Add accepted clip intake manifest plan`
- `da805bb` `Add accepted clip intake manifest design spec`
- `1c2ad80` `Add accepted clip inventory builder`
- `d2804a9` `Add approval target dataset pipeline`

## What Was Built In This Session

Two threads of work were completed in parallel:

1. `candidate_approval_probability` dataset hardening and evaluation
2. accepted-clip ingestion pipeline for `marvel_rivals`, from local accepted clips to prepared proxy review items

The approval-target work established the correct evaluation boundary for approval labels. The accepted-ingestion work built a new manifest-driven pipeline that turns canonical accepted clips into proxy-review-ready GPT review items without inventing a separate review system.

## Approval-Target Dataset Path

### Outcome

The current best real-only approval-target dataset is valid, but small. It does **not** justify promotion of the shadow approach for `candidate_approval_probability`.

### Key Artifacts

- approval-target dataset:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/approval_target_datasets/marvel_rivals/approval-target-986caef92773.manifest.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/approval_target_datasets/marvel_rivals/approval-target-986caef92773.manifest.json)
- adapted V2-style dataset:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/approval_target_dataset_adapted/marvel_rivals/v2-training-ca90212a351b.manifest.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/approval_target_dataset_adapted/marvel_rivals/v2-training-ca90212a351b.manifest.json)
- real evaluation artifact:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/shadow_ranking_experiments/approval_target_real_only/experiment.shadow_ranking_experiment.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/shadow_ranking_experiments/approval_target_real_only/experiment.shadow_ranking_experiment.json)

### Current Reading

- adapted dataset size: `4`
- label balance: `2` positive / `2` negative
- evidence mode: `real_only`
- shadow training succeeded cleanly
- evaluation result: `inconclusive`
- comparison summary:
  - `shadow_topk_hits: 2`
  - `heuristic_topk_hits: 2`
  - `shadow_ranking_gain == heuristic_ranking_gain`

### Important Contract Decisions Already Made

- explicit review outcomes outrank lifecycle state for approval labeling
- all-positive approval slices now fail early with `insufficient_target_label_balance`
- `no_platform` exploratory results are documented as **not promotion evidence**

### Practical Conclusion

Do not spend more time on approval-target model/ranker plumbing until more real reviewed approval labels exist. The current blocker is data volume, not code.

## Accepted-Clip Ingestion Pipeline

### Objective

Turn local accepted `marvel_rivals` clips in `/Users/tj/GPT-Codex-Repo/accepted/marvel_rivals` into reviewable pipeline artifacts with provenance preserved at each step.

### Implemented Pipeline

1. accepted clip inventory
2. accepted clip intake manifest
3. accepted intake -> `fixture_source_manifest_v1` adapter
4. accepted fixture-trial batch runner
5. accepted proxy review-prep wrapper over the existing proxy review bridge

This path reuses existing repo contracts instead of inventing a parallel review flow.

### Real Artifacts

- accepted inventory:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_inventory/marvel_rivals/accepted-clip-inventory-0f2774939cb0.manifest.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_inventory/marvel_rivals/accepted-clip-inventory-0f2774939cb0.manifest.json)
- accepted intake manifest:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_intake/marvel_rivals/accepted-clip-intake-15e8592f6d81.manifest.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_intake/marvel_rivals/accepted-clip-intake-15e8592f6d81.manifest.json)
- accepted source manifest:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_source_manifests/marvel_rivals/accepted-source-manifest-50f2384aaf9f.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_source_manifests/marvel_rivals/accepted-source-manifest-50f2384aaf9f.json)
- accepted fixture-trial batch:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_fixture_trials/marvel_rivals/34e70b59308f/accepted_fixture_trial_batch.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_fixture_trials/marvel_rivals/34e70b59308f/accepted_fixture_trial_batch.json)
- accepted proxy review-prep wrapper:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_proxy_reviews/marvel_rivals/3737fe3ef78b/accepted_proxy_review_prep.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_proxy_reviews/marvel_rivals/3737fe3ef78b/accepted_proxy_review_prep.json)
- underlying proxy review session:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/proxy_review_sessions/marvel_rivals/marvel_rivals-proxy-review-accepted-3737fe3ef78b-1317da0a9b85.proxy_review_session.json`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/proxy_review_sessions/marvel_rivals/marvel_rivals-proxy-review-accepted-3737fe3ef78b-1317da0a9b85.proxy_review_session.json)

### Real Results

- accepted inventory:
  - `34` source clips
  - `18` canonical inventory rows
  - `5` meta-linked rows
  - `13` duplicate groups
- fixture-trial batch:
  - `fixture_count: 18`
  - `success_count: 18`
  - `failed_count: 0`
- accepted proxy review prep:
  - `status: ok`
  - `fixture_count: 18`
  - `prepared_count: 18`
  - `skipped_count: 0`

### Important Implementation Notes

#### Accepted fixture-trial batch

- module:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/accepted_fixture_trial_batch.py`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/accepted_fixture_trial_batch.py)
- CLI:
  - `python3 run.py --run-accepted-fixture-trial-batch --fixture-source-manifest ...`
- important bug fix already landed:
  - default pattern `*.mp4` now uses filename-style matching via `fnmatch` instead of substring matching

#### Accepted proxy review prep

- module:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/accepted_proxy_review_prep.py`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/accepted_proxy_review_prep.py)
- CLI:
  - `python3 run.py --prepare-accepted-proxy-review --accepted-fixture-trial-batch-manifest ...`
- underlying bridge compatibility enhancement:
  - [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/proxy_review_bridge.py`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/pipeline/proxy_review_bridge.py)
  - batch-report rows can now carry explicit candidate fields (`source`, `top_proxy_score`, etc.)
  - this matters because accepted proxy sidecars may have empty `windows`, so reparsing sidecars alone would otherwise prepare zero review items

## Current Blocker

The pipeline is now blocked on **human proxy review**, not code.

Prepared review items exist in the GPT review repo:

- directory:
  - `/Users/tj/GPT-Codex-Repo/inbox/marvel_rivals/`

Current state:

- all `18` GPT `.meta.json` review files are prepared
- review status is still `unreviewed`
- therefore nothing should be applied back into sidecars yet

## Exact Next Step

### Human action required

Open the GPT review items in:

- `/Users/tj/GPT-Codex-Repo/inbox/marvel_rivals/`

and set each prepared proxy review item to either:

- `approved`
- `rejected`

using the existing GPT-Codex review workflow.

### After review decisions exist

Run:

```bash
python3 run.py \
  --apply-proxy-review \
  /Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/proxy_review_sessions/marvel_rivals/marvel_rivals-proxy-review-accepted-3737fe3ef78b-1317da0a9b85.proxy_review_session.json \
  --gpt-repo /Users/tj/GPT-Codex-Repo
```

Then inspect the mutated sidecars and refresh registry state.

## Recommended Immediate Follow-Up After Review Apply

1. audit proxy sidecar mutations
2. refresh clip registry from the accepted sidecars
3. rebuild the approval-target dataset from the refreshed registry
4. rerun the approval-target builder -> adapter -> shadow evaluation path

The expected reason for doing this is to increase the real reviewed approval-label pool beyond the current `4` useful rows.

## Suggested Next Architecture Work

Once the review/apply step is complete, the strongest next contract work is:

1. structured review-label contract
2. explicit temporal provenance hardening
3. clip lifecycle / state machine
4. failure taxonomy

Rationale:

- the repo now has a real accepted review path, so structured reviewer labels should be captured as training data rather than only free-form review status
- temporal provenance and lifecycle contracts are the next highest leverage improvements across fusion, replay, registry, and export

## Relevant Design And Plan Docs Created In This Session

Accepted pipeline docs:

- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-06-approval-target-dataset-builder-design.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-06-approval-target-dataset-builder-design.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-06-approval-target-dataset-builder-plan.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-06-approval-target-dataset-builder-plan.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-approval-target-adapter-design.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-approval-target-adapter-design.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-approval-target-adapter-plan.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-approval-target-adapter-plan.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-clip-inventory-design.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-clip-inventory-design.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-clip-inventory-plan.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-clip-inventory-plan.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-clip-intake-manifest-design.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-clip-intake-manifest-design.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-clip-intake-manifest-plan.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-clip-intake-manifest-plan.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-intake-source-manifest-adapter-design.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-intake-source-manifest-adapter-design.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-intake-source-manifest-adapter-plan.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-intake-source-manifest-adapter-plan.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-fixture-trial-batch-design.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-fixture-trial-batch-design.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-fixture-trial-batch-plan.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-fixture-trial-batch-plan.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-proxy-review-prep-design.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-07-accepted-proxy-review-prep-design.md)
- [`/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-proxy-review-prep-plan.md`](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/plans/2026-05-07-accepted-proxy-review-prep-plan.md)

## If Another Codex Instance Picks This Up

Start by reading, in this order:

1. this handoff file
2. the accepted proxy review-prep manifest
3. the proxy review session manifest
4. the accepted fixture-trial batch manifest
5. the approval-target dataset + adapted manifest + shadow experiment result

Then decide whether:

- human proxy review decisions are already present in `/Users/tj/GPT-Codex-Repo/inbox/marvel_rivals/`
- if yes, apply them immediately
- if not, stop and wait for review input rather than fabricating labels

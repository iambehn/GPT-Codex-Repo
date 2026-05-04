---
name: review-bridge-operator
description: |
  Standardized operator workflow for proxy, runtime, fused, and onboarding identity
  review bridges in this repo. Use when preparing, applying, or cleaning up review
  sessions and when auditing the resulting draft or sidecar mutations.
---

# Review Bridge Operator

Use this skill for any GPT-Codex review bridge workflow in this repo.

## When To Use

Use this skill when the task involves:
- `--prepare-proxy-review`
- `--apply-proxy-review`
- `--cleanup-proxy-review`
- `--prepare-runtime-review`
- `--apply-runtime-review`
- `--cleanup-runtime-review`
- `--prepare-fused-review`
- `--apply-fused-review`
- `--cleanup-fused-review`
- `--prepare-onboarding-identity-review`
- `--apply-onboarding-identity-review`
- `--cleanup-onboarding-identity-review`

Do not use this skill for:
- calibration/replay
- HF model/runtime implementation
- unrelated onboarding draft generation

## Bridge Selection

Use `proxy review` when:
- reviewing clip-level proxy candidates before deeper analysis

Use `runtime review` when:
- reviewing runtime-analysis clip decisions driven by ROI/event scoring

Use `fused review` when:
- reviewing fused event candidates after proxy + runtime fusion

Use `onboarding identity review` when:
- resolving publish-blocking onboarding identity findings in one draft

## Command Templates

Proxy:

```bash
python3 run.py --prepare-proxy-review <GAME> --sidecar-root <ROOT> --action <ACTION> --limit <N> --gpt-repo <PATH> --session-name <NAME>
python3 run.py --apply-proxy-review <SESSION_MANIFEST> --gpt-repo <PATH>
python3 run.py --cleanup-proxy-review <SESSION_MANIFEST> --gpt-repo <PATH>
```

Runtime:

```bash
python3 run.py --prepare-runtime-review <GAME> --sidecar-root <ROOT> --action <ACTION> --limit <N> --gpt-repo <PATH> --session-name <NAME>
python3 run.py --apply-runtime-review <SESSION_MANIFEST> --gpt-repo <PATH>
python3 run.py --cleanup-runtime-review <SESSION_MANIFEST> --gpt-repo <PATH>
```

Fused:

```bash
python3 run.py --prepare-fused-review <GAME> --sidecar-root <ROOT> --action <ACTION> --event-type <TYPE> --limit <N> --gpt-repo <PATH> --session-name <NAME>
python3 run.py --apply-fused-review <SESSION_MANIFEST> --gpt-repo <PATH>
python3 run.py --cleanup-fused-review <SESSION_MANIFEST> --gpt-repo <PATH>
```

Onboarding identity:

```bash
python3 run.py --prepare-onboarding-identity-review <DRAFT_ROOT> --gpt-repo <PATH> --session-name <NAME>
python3 run.py --apply-onboarding-identity-review <SESSION_MANIFEST> --gpt-repo <PATH>
python3 run.py --cleanup-onboarding-identity-review <SESSION_MANIFEST> --gpt-repo <PATH>
```

## Required Inputs

Before preparing a session, confirm:
- the target game or draft root
- the correct sidecar root or draft root
- the intended review surface
- the `gpt_repo` if the default should not be used
- any filter that materially changes the candidate set:
  - `action`
  - `event_type`
  - `limit`

## Expected Artifacts

Common outputs:
- one session manifest under `outputs/..._review_sessions/...`
- GPT review items and metadata under the configured GPT repo

Apply results should update:
- proxy sidecars via `proxy_review`
- runtime sidecars via `runtime_review`
- fused sidecars via `fused_review`
- onboarding drafts via row metadata and QA cleanup/audit rows

Cleanup should remove bridge-owned generated artifacts but leave accepted draft or sidecar mutations intact.

## Apply-Time Audit Checks

After `apply`, confirm:
- manifest counts look reasonable:
  - approved
  - rejected
  - unreviewed
  - resolved/deferred/failed for onboarding identity
- the target sidecar or draft changed in the expected place
- no unrelated review surface was mutated
- review status fields and timestamps were written

For onboarding identity review specifically, also confirm:
- resolved blocking identity QA rows were removed
- review audit metadata was written to the row
- readiness changed only if the decision was actually resolving

## Cleanup Checks

After `cleanup`, confirm:
- bridge-owned GPT processing artifacts are removed
- session manifest records cleanup metadata
- reviewed sidecars or drafts remain intact

Do not use cleanup as rollback. Cleanup is artifact removal only.

## Failure Handling

If prepare finds no candidates:
- report the exact selection filter used
- recommend the next wider filter rather than guessing

If apply reports invalid review decisions:
- inspect the per-item apply status
- do not assume session-wide failure
- summarize valid items applied vs failed items left unchanged

If gpt repo mismatch occurs:
- use the manifest’s recorded `gpt_repo`
- do not force-apply against a different repo path

## Output Standard

When summarizing a review-bridge operation, include:
- which bridge ran
- selection inputs
- manifest path
- candidate/item count
- apply or cleanup counts
- any follow-up audit concern

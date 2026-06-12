# Call of Duty Editorial Replay Procedure

Status: active
Version: 0.1
Last updated: 2026-06-13

## Purpose

This is the bounded operator procedure for validating repo-local editorial
replay and historical export regeneration on the canonical `call_of_duty`
proof path.

It exists to answer:

- can review state be replayed from repo-local artifacts alone?
- can a historical export-ready moment be regenerated after lifecycle has
  advanced to `exported`?

It does not exist to:

- widen replay beyond `call_of_duty`
- claim publish readiness
- validate posting or external distribution
- generalize to multi-game replay
- redesign review or export policy

## Governing Scope

Use this procedure only for the bounded `call_of_duty` proof path already
documented in:

- [EXECUTION_TARGET.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/operator_pack/EXECUTION_TARGET.md)
- [2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md)

Preserve these limits:

- local-only outputs
- no publish-cleared claims
- no widening beyond the current `call_of_duty` proof path

## Required Inputs

### Canonical review-session artifacts

- runtime review session:
  - [call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json)
- fused review session:
  - [call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json)

### Canonical proof-path sidecars

- runtime sidecar:
  - [svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json)
- fused sidecar:
  - [svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json)
- canonical reviewed fused sidecar:
  - [svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json)

### Canonical registry and historical workflow id

- registry:
  - [bootstrap-real-cod.registry.sqlite](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite)
- historical workflow run:
  - `workflow-11aea2937311834b`

### Runtime requirements

- use the repo-local interpreter:

```bash
./.venv/bin/python
```

Do not use system `python3` for the media-backed path. The proof path depends on
repo-local packages such as `cv2`.

## Replay Objects

This procedure assumes the bounded replay contract persists three repo-local
object families:

1. stable editorial identity records
2. editorial decision records
3. export-ready snapshot records

Primary storage roots:

- identities:
  - [outputs/editorial_replay/identities/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/identities/call_of_duty)
- decisions:
  - [outputs/editorial_replay/decisions/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/decisions/call_of_duty)
- snapshots:
  - [outputs/editorial_replay/snapshots/call_of_duty](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/editorial_replay/snapshots/call_of_duty)

## Procedure

### 1. Re-apply canonical review sessions

Purpose:

- ensure current canonical review sessions have persisted replay artifacts
- upgrade stale editorial ids if source normalization changed

Commands:

```bash
./.venv/bin/python run.py --apply-runtime-review outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json --full-json
./.venv/bin/python run.py --apply-fused-review outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json --full-json
```

Expected outputs:

- runtime session reports nonzero `approved_count` and `rejected_count`
- fused session reports nonzero `approved_count` and `rejected_count`
- session items contain:
  - `editorial_object_id`
  - `editorial_identity_path`
  - `editorial_decision_path`

Fail if:

- either apply command returns `ok = false`
- replay artifact paths are absent from applied session items

### 2. Reconfirm mechanical proof-path surfaces

Purpose:

- verify runtime, fusion, and selection behavior still works after replay
  changes

Commands:

```bash
./.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --sample-fps 1 --limit-frames 30 --output-path outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json --full-json
./.venv/bin/python run.py --fuse-clip-signals outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --proxy-sidecar outputs/proxy_scans/call_of_duty/svbtc2azzyw-60s-70s-c40d17236088.proxy_scan.json --runtime-sidecar outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json --output-path outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json --full-json
./.venv/bin/python run.py --export-highlight-selection --fused-sidecar outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json --output-path outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.highlight_selection.json --full-json
./.venv/bin/python run.py --export-highlight-selection --fused-sidecar outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json --output-path outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613-from-fresh-fused.highlight_selection.json --full-json
```

Expected outputs:

- runtime sidecar:
  - `ok = true`
  - `event_count = 3`
- fused sidecar:
  - `ok = true`
  - `fused_event_count = 3`
- both selection exports:
  - `selected_highlight_count = 3`

Fail if:

- runtime or fused generation fails
- selected highlight counts fall below the expected bounded sample behavior

### 3. Create an isolated replay-validation root

Purpose:

- prove replay works from repo-local preserved artifacts, not from live sidecar
  state or external GPT metadata

Use:

```text
outputs/replay_validation/call_of_duty/<timestamp>/
```

Expected subtrees:

- `outputs/editorial_replay/identities/call_of_duty/`
- `outputs/editorial_replay/decisions/call_of_duty/`
- `sidecars/`
- `exports/`

### 4. Copy replay artifacts and intentionally break GPT paths

Purpose:

- verify replay does not require external GPT or human metadata as a canonical
  source of truth

Procedure:

1. copy repo-local `call_of_duty` identity and decision artifacts into the new
   validation root
2. rewrite copied decision payload fields:
   - `gpt_meta_path`
   - `gpt_processed_path`
   - `gpt_final_path`
   to nonexistent paths such as:

```text
/tmp/nonexistent/replay/meta.json
/tmp/nonexistent/replay/processed.mp4
/tmp/nonexistent/replay/final.mp4
```

Expected result:

- replay should still succeed from the copied repo-local decision artifacts

Fail if:

- replay requires those external paths to exist

### 5. Replay runtime and fused review state on fresh sidecar copies

Purpose:

- prove editorial replay is deterministic from repo-local artifacts alone

Procedure:

1. copy the fresh runtime and fused sidecars into the isolated validation root
2. remove:
   - `runtime_review`
   - `fused_review`
3. ensure copied sidecars point their `sidecar_path` to the isolated copies
4. call:
   - `replay_runtime_editorial_decision(...)`
   - `replay_fused_editorial_decisions(...)`
   against the copied sidecars using the isolated validation root as
   `repo_root`

Expected outputs:

- runtime replay:
  - `ok = true`
  - `review_app = repo_local_editorial_replay`
  - `review_status` restored
- fused replay:
  - `ok = true`
  - `applied_count > 0`
  - fused event review statuses restored

Fail if:

- replay returns:
  - `no_runtime_decision_record`
  - `no_fused_decision_records`
- restored review blocks are absent from copied sidecars

### 6. Replay historical export regeneration from preserved snapshot

Purpose:

- prove export replay can reconstruct the historical export-ready moment even
  after lifecycle has advanced to `exported`

Procedure:

1. copy the canonical registry into the isolated validation root
2. generate an export batch against:
   - `workflow-11aea2937311834b`
3. write output into the isolated validation root

Recommended command shape:

```bash
./.venv/bin/python run.py --create-highlight-export-batch --registry-path <isolated-registry-copy> --workflow-run-id workflow-11aea2937311834b --output-path <isolated-export-manifest> --full-json
```

Expected outputs:

- export batch:
  - `ok = true`
  - `export_count = 1`
  - `replayed_from_export_ready_snapshot = true`

Fail if:

- export batch returns:
  - `status = no_selected_candidates`
- replay is forced to depend on current lifecycle `selected_for_export` state

## Pass / Fail Summary

### Pass

This bounded replay procedure passes only if all of the following are true:

1. canonical review re-apply succeeds
2. runtime rerun still succeeds
3. fused rerun still succeeds
4. selection export still succeeds from:
   - canonical reviewed fused sidecar
   - fresh fused sidecar
5. runtime replay succeeds from copied repo-local decision artifacts
6. fused replay succeeds from copied repo-local decision artifacts
7. replay still succeeds after copied GPT paths are intentionally broken
8. historical export regeneration succeeds from preserved snapshot with:
   - `replayed_from_export_ready_snapshot = true`

### Fail

Treat the procedure as failed if any of the following occur:

- canonical review re-apply fails
- runtime or fused proof-path regression fails
- replay depends on external GPT metadata existence
- replayed sidecars do not restore review state
- export replay returns `no_selected_candidates`
- replay requires widening scope beyond the bounded `call_of_duty` proof path

## Expected Artifacts

Successful execution should leave:

- updated canonical review session manifests with replay-artifact paths
- repo-local replay contract artifacts under:
  - `outputs/editorial_replay/identities/call_of_duty/`
  - `outputs/editorial_replay/decisions/call_of_duty/`
  - `outputs/editorial_replay/snapshots/call_of_duty/`
- isolated replay-validation root under:
  - `outputs/replay_validation/call_of_duty/<timestamp>/`
- replayed export batch manifest under the isolated root
- a durable validation report in:
  - `docs/superpowers/specs/`

## Replay-Validation Outputs To Record

Record these outputs in the validation report:

- runtime replay status
- fused replay status
- restored runtime review status
- restored fused event review statuses
- whether replay succeeded with broken GPT paths
- export replay status
- whether export replay used preserved snapshot fallback
- runtime event count
- fused event count
- selection export counts from:
  - canonical reviewed fused sidecar
  - fresh fused sidecar

## Current Bounded Reference Result

The current passing reference result is:

- [2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-06-13-call-of-duty-bounded-replay-validation-report-v0.md)

Use it as the comparison baseline for future bounded replay reruns.

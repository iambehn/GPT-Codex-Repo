# Call of Duty Bounded Replay Validation Report v0

Date: 2026-06-13
Status: completed
Scope: bounded operational replay validation only

## Objective

Re-run the bounded `call_of_duty` proof-path validation against real repo artifacts after the replay-contract implementation, and determine whether editorial replay and export regeneration now work operationally.

Non-goals:

- multi-game validation
- publish or posting readiness
- production-readiness claims
- threshold or policy changes

## Validation Gates

1. editorial replay works from repo-local artifacts
2. export regeneration works from preserved snapshot
3. no external GPT or human metadata dependency remains for replay
4. prior runtime, fusion, and selection behavior still works
5. outputs remain local-only and not publish-cleared

## Commands Executed

### Canonical review re-apply after replay-contract update

```bash
./.venv/bin/python run.py --apply-runtime-review outputs/runtime_review_sessions/call_of_duty/call_of_duty-runtime-review-bootstrap-real-cod-fbfbc59cafa4.runtime_review_session.json --full-json
./.venv/bin/python run.py --apply-fused-review outputs/fused_review_sessions/call_of_duty/call_of_duty-fused-review-bootstrap-real-cod-fused-19e9d6dcf48c.fused_review_session.json --full-json
```

### Regression rerun of prior mechanical proof path surfaces

```bash
./.venv/bin/python run.py --analyze-roi-runtime outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --sample-fps 1 --limit-frames 30 --output-path outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json --full-json
./.venv/bin/python run.py --fuse-clip-signals outputs/public_gameplay_mining/call_of_duty_test_sources/SVbTc2AZzYw.60s-70s.mp4 call_of_duty --proxy-sidecar outputs/proxy_scans/call_of_duty/svbtc2azzyw-60s-70s-c40d17236088.proxy_scan.json --runtime-sidecar outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json --output-path outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json --full-json
./.venv/bin/python run.py --export-highlight-selection --fused-sidecar outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.bootstrap-real-cod.fused_analysis.json --output-path outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.highlight_selection.json --full-json
./.venv/bin/python run.py --export-highlight-selection --fused-sidecar outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json --output-path outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613-from-fresh-fused.highlight_selection.json --full-json
```

### Isolated repo-local replay validation

Mechanism:

- copied repo-local editorial decision and identity artifacts into:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/replay_validation/call_of_duty/20260612T214215Z`
- rewrote copied `gpt_meta_path`, `gpt_processed_path`, and `gpt_final_path` to nonexistent `/tmp/nonexistent/...` paths
- replayed runtime and fused review state against fresh sidecar copies using only the copied repo-local replay artifacts

### Historical export regeneration from preserved snapshot

Mechanism:

- copied:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/happy_path/call_of_duty/bootstrap-real-cod.registry.sqlite`
- generated export batch against historical workflow id:
  - `workflow-11aea2937311834b`
- output manifest:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/replay_validation/call_of_duty/20260612T214215Z/exports/bootstrap-real-cod.replayed.highlight_export_batch.json`

## Results

### 1. Editorial replay from repo-local artifacts

Runtime replay:

- status:
  - `ok`
- replay source:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/replay_validation/call_of_duty/20260612T214215Z/sidecars/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json`
- replayed review status:
  - `approved`
- replay app:
  - `repo_local_editorial_replay`

Fused replay:

- status:
  - `ok`
- applied event count:
  - `2`
- replay source:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/replay_validation/call_of_duty/20260612T214215Z/sidecars/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json`
- replayed event statuses:
  - `equipment_visibility_atomic-363ba2756b53 = rejected`
  - `equipment_visibility_atomic-e57f5f737fcb = approved`

Assessment:

- editorial replay now works from repo-local artifacts

### 2. External GPT metadata dependency

Observed:

- replay succeeded even though copied decision artifacts pointed at nonexistent:
  - `/tmp/nonexistent/replay/meta.json`
  - `/tmp/nonexistent/replay/processed.mp4`
  - `/tmp/nonexistent/replay/final.mp4`

Assessment:

- replay no longer depends on external GPT metadata as a required source of truth
- GPT-path fields are still preserved as historical references, but they are no longer required for deterministic replay

### 3. Export regeneration from preserved snapshot

Result:

- export batch status:
  - `ok`
- manifest:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/replay_validation/call_of_duty/20260612T214215Z/exports/bootstrap-real-cod.replayed.highlight_export_batch.json`
- `export_count = 1`
- `replayed_from_export_ready_snapshot = true`

Assessment:

- historical export regeneration now works from the preserved export-ready snapshot even though the current lifecycle state has already advanced past `selected_for_export`

### 4. Prior runtime, fusion, and selection behavior

Runtime rerun:

- sidecar:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/runtime_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.runtime_analysis.json`
- result:
  - `ok = true`
  - `event_count = 3`

Fusion rerun:

- sidecar:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/fused_analysis/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.fused_analysis.json`
- result:
  - `ok = true`
  - `fused_event_count = 3`

Selection export from canonical reviewed fused sidecar:

- manifest:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613.highlight_selection.json`
- result:
  - `selected_highlight_count = 3`

Selection export from fresh fused sidecar:

- manifest:
  - `/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/highlight_selection_exports/call_of_duty/svbtc2azzyw-60s-70s.validation-20260613-from-fresh-fused.highlight_selection.json`
- result:
  - `selected_highlight_count = 3`

Assessment:

- prior runtime, fusion, and selection behavior still works

### 5. Local-only boundary

Observed:

- replay validation produced:
  - repo-local sidecar mutations
  - repo-local selection exports
  - repo-local export batch artifacts
- no posted-highlight ledger or publish mutation was created

Assessment:

- outputs remain local-only
- nothing in this validation is publish-cleared

## Failure Fixed

The blocking replay bug was:

- editorial identity generation was source-path-sensitive
- equivalent clip sources represented as absolute paths versus repo-relative paths produced different `editorial_object_id` values

Resolution:

- canonical source normalization now maps repo-local clip sources to one stable repo-relative identity basis
- review re-apply now self-heals stale editorial ids on historical session manifests during persistence

## Conclusion

Classification:

- bounded replay validation = `passes`

Current status by stage:

- runtime analysis:
  - `proven`
- fusion analysis:
  - `proven`
- selection export:
  - `proven`
- repo-local editorial replay:
  - `proven`
- historical export regeneration from preserved snapshot:
  - `proven`
- publish workflow:
  - `not evaluated`

The bounded `call_of_duty` proof path is now mechanically reproducible and operationally replayable within the repo-local replay contract. This remains a local validation result only, not a publish-readiness or generalized multi-game result.

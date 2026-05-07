# Highlight Review App Proxy Session Implementation Plan

## Scope

Implement the approved proxy-review-session extension for the existing
highlight review app:

- [docs/superpowers/specs/2026-05-08-highlight-review-app-proxy-session-design.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/superpowers/specs/2026-05-08-highlight-review-app-proxy-session-design.md)

This subproject adds one new review-app input surface for
`proxy_review_session_v1` manifests and allows queue-list + detail-view review
of prepared proxy items.

It does not replace the proxy review bridge and does not mutate repo sidecars
directly. The existing `--apply-proxy-review` step remains unchanged.

## Desired End State

After this work:

- the existing highlight review app can load a proxy review session manifest
- prepared proxy items appear as first-class review records
- the app exposes a queue list with a detail view
- operators can set `approved`, `rejected`, or `unreviewed`
- those decisions are written back to the existing `gpt_meta_path`
- the CLI route remains `--launch-highlight-review-app`
- one real prepared `marvel_rivals` proxy review session can be opened through
  the app

## Implementation Strategy

Implement this as one extension to the review app and one CLI wiring update:

1. extend record loading for proxy review session items
2. add metadata and transcript discovery helpers
3. add in-app writeback actions
4. update the app UI to support queue list + detail view for session items
5. wire the new CLI argument
6. add focused tests

Keep the design narrow. Reuse the existing app and the existing bridge-owned GPT
metadata rather than inventing a new review persistence layer.

## Slice 1: Record Loading

Update:

- `pipeline/highlight_review_app.py`

Add support for:

- `proxy_review_session_manifest: str | Path | None = None`

Extend `load_highlight_review_records(...)` to:

- load `proxy_review_session_v1`
- project its items into app records with:
  - `kind: proxy_review_session_item`
  - `record_id`
  - `label`
  - `game`
  - `source`
  - `processed_clip_path`
  - `source_clip_path`
  - `proxy_sidecar_path`
  - `gpt_meta_path`
  - `gpt_processed_path`
  - `review_status`
  - `bridge_score`
  - `bridge_sources`
  - `bridge_source_families`
  - `transcript_srt_path`
  - `transcript_whisper_json_path`
  - `transcript_available`
  - `session_id`

### Record Rules

- session records should be loaded in manifest item order
- malformed items should be ignored or surfaced as degraded records, but must
  not crash the app
- the app should be able to mix fixture rows, sidecar rows, and proxy session
  rows in one selector list

## Slice 2: Transcript And Metadata Discovery

Add helper logic that:

- reads `gpt_meta_path`
- discovers transcript companions for the source clip:
  - `.srt`
  - `.whisper.json`
- preserves raw bridge metadata for the detail payload/debug view

The app should not parse transcripts deeply in v1. Presence plus paths is
sufficient.

## Slice 3: Review Writeback Actions

Extend `launch_highlight_review_app(...)` to expose review actions for
`proxy_review_session_item` rows:

- `approve`
- `reject`
- `leave unreviewed`

Implement writeback to `gpt_meta_path` only:

- `review_status`
- `reviewed_at`

### Writeback Policy

- `approved` / `rejected`
  - set `review_status`
  - set `reviewed_at`
- `unreviewed`
  - set `review_status` to `unreviewed`
  - clear `reviewed_at`

After writeback:

- refresh the selected row summary in-app
- preserve all unrelated metadata fields

## Slice 4: App UI Update

Keep using the existing Gradio-based app.

Add:

- queue list / selector support for proxy review session items
- detail panel rendering for those rows
- writeback controls only when the selected row kind is
  `proxy_review_session_item`

### Detail Panel Content

- processed clip path
- source clip path
- current review status
- proxy bridge score
- source sidecar path
- bridge sources
- bridge source families
- transcript availability and paths
- raw metadata/debug payload

The fixture and sidecar paths already supported by the app must remain intact.

## Slice 5: CLI Route

Update:

- `run.py`
- `tests/test_run.py`

Add one optional CLI input:

- `--proxy-review-session-manifest <PATH>`

Route it through the existing:

- `--launch-highlight-review-app`

No new dedicated app command is needed.

## Slice 6: Tests

Update or add:

- `tests/test_highlight_review_app.py`
- `tests/test_run.py`

Cover:

- loading `proxy_review_session_v1` items into records
- surfacing bridge metadata and transcript availability
- writeback of `approved`, `rejected`, and `unreviewed`
- launch path accepting the new CLI argument
- existing fixture/sidecar review app behavior remains intact

Use fake Gradio objects as current tests already do.

## Focused Verification

Run:

```bash
python3 -m py_compile \
  pipeline/highlight_review_app.py \
  tests/test_highlight_review_app.py \
  run.py \
  tests/test_run.py
```

Run:

```bash
python3 -m unittest \
  tests.test_highlight_review_app \
  tests.test_run.RunTests.test_cli_routes_to_launch_highlight_review_app
```

Add any new focused app-route tests needed for the new
`--proxy-review-session-manifest` argument.

## Real Validation

After implementation, validate against the real session:

- [marvel_rivals-proxy-review-accepted-3737fe3ef78b-1317da0a9b85.proxy_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/proxy_review_sessions/marvel_rivals/marvel_rivals-proxy-review-accepted-3737fe3ef78b-1317da0a9b85.proxy_review_session.json)

Expected outcome:

- the app launches
- queue items appear
- selecting an item shows clip and bridge context
- changing review status updates the matching `gpt_meta_path`

## Commit Strategy

Prefer one coherent implementation commit containing:

- review-app record loading changes
- review writeback helpers
- CLI argument wiring
- app tests and CLI tests

Generated review-session state in the GPT repo should not be committed.

## Risks

### Risk: Review app grows a second persistence model

Mitigation:

- write decisions only to `gpt_meta_path`
- keep `--apply-proxy-review` unchanged

### Risk: Session-item UI breaks existing fixture or sidecar review

Mitigation:

- isolate `kind == proxy_review_session_item` logic
- retain existing render paths for fixtures and sidecars

### Risk: Transcript presence complicates the first version

Mitigation:

- treat transcript support as path discovery only in v1
- do not parse or render transcript internals beyond basic payload exposure

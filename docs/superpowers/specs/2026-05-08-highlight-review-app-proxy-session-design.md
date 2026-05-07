# Highlight Review App Proxy Session Design

## Summary

Extend the existing highlight review app to load and review
`proxy_review_session_v1` manifests directly.

The app should become the primary operator surface for reviewing prepared proxy
review items, instead of requiring manual edits to GPT `.meta.json` files.

This slice must reuse the existing proxy review bridge contract:

- review decisions are still written into the bridge-owned `gpt_meta_path`
- `--apply-proxy-review` remains the only step that mutates repo sidecars

No new review truth source should be introduced.

## Problem

The accepted-clip ingestion pipeline now prepares real proxy review sessions for
`marvel_rivals`, and the current blocker is human review of the prepared GPT
queue items.

Today that review requires:

- locating the `.mp4` in `/Users/tj/GPT-Codex-Repo/processing/...`
- locating the matching `proxy-review-*.meta.json` in
  `/Users/tj/GPT-Codex-Repo/inbox/...`
- manually editing `review_status`

That workflow is repetitive, error-prone, and not acceptable as a recurring
operator path.

The repo already has:

- an existing proxy review bridge
- an existing highlight review app
- an existing apply step

So the right move is to extend the review app to support prepared proxy review
sessions directly.

## Goals

- Review prepared proxy session items inside the existing highlight review app.
- Support one queue list with one detail view.
- Show the clip, bridge context, and transcript availability.
- Allow explicit decisions:
  - `approved`
  - `rejected`
  - `unreviewed`
- Write decisions back to the existing `gpt_meta_path`.
- Keep `--apply-proxy-review` unchanged.

## Non-Goals

- No new review schema.
- No direct mutation of repo sidecars from the app.
- No runtime or fused review controls in this slice.
- No multi-session batch management.
- No registry mutation.

## Selected Design

Extend the existing highlight review app to accept an optional
`proxy_review_session_manifest` input and represent those queued review items as
first-class records inside the existing app.

The app should present:

- a queue list of review items
- a detail view for the selected item
- write-back controls for `review_status`

This is preferred over a separate micro-app because it preserves one operator
surface and reuses the app/viewer stack already present in the repo.

## Input

One optional `proxy_review_session_v1` manifest.

Example real artifact:

- [marvel_rivals-proxy-review-accepted-3737fe3ef78b-1317da0a9b85.proxy_review_session.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/proxy_review_sessions/marvel_rivals/marvel_rivals-proxy-review-accepted-3737fe3ef78b-1317da0a9b85.proxy_review_session.json)

## Record Model Extension

Add one new review-app record kind:

- `proxy_review_session_item`

Each session item record should carry:

- `record_id`
- `label`
- `kind`
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

## UI Shape

Use a queue list plus detail view.

### Queue List

The list should show at minimum:

- clip label
- current review status
- proxy score if available

### Detail View

The selected item view should show:

- processed clip path
- source clip path
- current review status
- proxy bridge context:
  - score
  - source sidecar path
  - bridge sources
  - bridge source families
- transcript availability:
  - `.srt`
  - `.whisper.json`
- raw metadata payload for audit/debug

## Review Actions

Provide explicit actions:

- `approve`
- `reject`
- `leave unreviewed`

Each action updates the bridge-owned `gpt_meta_path` only.

Expected writeback:

- `review_status`
- `reviewed_at`

`reviewed_at` should be set when the status becomes `approved` or `rejected`.
If the user sets the item back to `unreviewed`, the app may either:

- clear `reviewed_at`, or
- leave it intact and rely on `review_status`

For v1, the cleaner rule is:

- `approved` / `rejected`: write `reviewed_at`
- `unreviewed`: clear `reviewed_at`

## CLI Surface

Extend the existing highlight review app entrypoint in `run.py`.

Add:

- `--proxy-review-session-manifest <PATH>`

This should route into the existing:

- `--launch-highlight-review-app`

No new standalone app command is needed for v1.

## Data Flow

1. load highlight review app inputs
2. if `proxy_review_session_manifest` is provided:
   - load `proxy_review_session_v1`
   - project items into review-app records
3. merge those records into the selector choices
4. on selection:
   - render detail view from the selected record
5. on decision action:
   - mutate the corresponding `gpt_meta_path`
   - refresh the visible review status in-app

## Error Handling

Return structured failures for:

- invalid proxy review session manifest
- unreadable `gpt_meta_path`
- malformed session item
- writeback failure when updating review status

The app should still launch if at least one valid record exists.

Invalid session rows should be surfaced in the payload/debug view rather than
crashing the session.

## Testing

Add focused tests for:

- loading `proxy_review_session_v1` items into review records
- correctly surfacing review status and bridge metadata
- transcript path discovery when `.srt` / `.whisper.json` exist
- writeback of `approved`, `rejected`, and `unreviewed`
- launch path with the new session-manifest argument

Tests should not depend on real Gradio runtime.

## Scope Boundary

This slice does not redesign the whole review app. It adds one clean extension
point for proxy review session items and keeps the existing bridge/apply flow
intact.

If runtime/fused review later need the same operator affordances, their support
should follow this same pattern rather than introducing a separate app.

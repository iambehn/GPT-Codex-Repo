# Accepted Intake Source Manifest Adapter Design

## Summary

Add a narrow adapter that converts one
`accepted_clip_intake_manifest_v1` artifact into one existing source-manifest
artifact for downstream workflows.

This keeps the workflow layered:

- accepted inventory normalizes canonical media
- accepted intake freezes the chosen ingestion set
- source-manifest adapter translates that frozen set into an existing downstream
  manifest contract

The adapter does not generate sidecars, does not start review, and does not
touch the registry. It only performs schema translation into the existing
source-manifest shape.

## Problem

The repo now has a stable accepted-intake artifact:

- [accepted-clip-intake-15e8592f6d81.manifest.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_intake/marvel_rivals/accepted-clip-intake-15e8592f6d81.manifest.json)

But downstream workflows already expect existing source-manifest contracts. We
need a narrow translation step so those workflows can consume accepted clips
without:

- widening multiple downstream interfaces
- teaching `run.py` to do ad hoc manifest translation
- collapsing accepted-intake semantics into every consumer

## Goals

- Convert one `accepted_clip_intake_manifest_v1` into one downstream
  source-manifest artifact.
- Reuse the existing source-manifest workflow shape rather than inventing a new
  downstream contract.
- Preserve provenance back to the accepted-intake manifest.
- Keep the adapter artifact deterministic and auditable.
- Avoid any side effects beyond writing the adapted manifest.

## Non-Goals

- No sidecar generation.
- No fixture execution.
- No review-state creation.
- No registry mutation.
- No direct changes to downstream source-manifest consumers.

## Approaches Considered

### Option 1: Standalone adapter module

Add:

- `pipeline/accepted_clip_source_manifest_adapter.py`

This module takes one accepted-intake manifest and emits one source-manifest
artifact in the existing downstream shape.

Pros:

- clean boundary
- easy to test
- keeps accepted intake and source-manifest semantics distinct

Cons:

- one extra artifact step

### Option 2: Teach downstream consumers to accept accepted-intake manifests

Rejected because it broadens interfaces and couples downstream workflows to a
new upstream artifact type.

### Option 3: Inline translation in `run.py`

Rejected because it creates an untestable workflow-specific boundary and hides
manifest logic inside command routing.

## Selected Design

Implement Option 1.

The adapter will:

1. load and validate one `accepted_clip_intake_manifest_v1`
2. translate each intake row into one downstream source row
3. emit one standalone adapted source-manifest artifact under `outputs/`

## Output Contract

Use the existing source-manifest shape already consumed by downstream workflow
code.

The adapter artifact should not invent a parallel downstream schema unless the
existing consumers require a wrapper field or schema version marker.

### Top-Level Fields

- existing source-manifest top-level fields
- provenance back to:
  - source accepted-intake manifest path
  - source accepted-intake manifest id

### Row Fields

Each output row should contain enough for downstream source-manifest consumers:

- stable source id derived from `clip_id`
- `game`
- source media path from `canonical_clip_path`
- optional metadata/provenance fields derived from intake rows when compatible
  with the existing source-manifest contract

## Row Mapping Rules

From intake row:

- `clip_id` -> stable downstream row id
- `game` -> `game`
- `canonical_clip_path` -> downstream source media path
- `meta_path` -> optional provenance field if the existing contract allows it
- quality/media facts stay only if they fit the existing source-manifest shape;
  otherwise they are not forced into the downstream manifest

The adapter must not reintroduce duplicate variant fields or any non-canonical
paths.

## Provenance

The adapted manifest must preserve enough provenance to answer:

- which accepted-intake artifact produced this source manifest
- which accepted clip row produced each source row

At minimum:

- source accepted-intake manifest path
- source accepted-intake manifest id
- row ids derived from accepted `clip_id`

## Output Paths

Default output root:

- `outputs/accepted_clip_source_manifests/<game>/`

Suggested filename:

- `accepted-source-manifest-<id>.json`

The id should be deterministic from:

- accepted-intake manifest path
- accepted-intake manifest id
- ordered clip ids

## CLI Surface

Add one command in `run.py`:

- `--adapt-accepted-clip-intake-to-source-manifest`

Inputs:

- `--accepted-clip-intake-manifest <PATH>`
- optional `--output-root <PATH>`
- optional `--output-path <PATH>`

Output:

- compact JSON by default
- full payload with `--full-json`

Compact summary fields:

- `ok`
- `status`
- adapted manifest id
- row count
- output manifest path
- source accepted-intake manifest id

## Error Handling

Return structured failures for:

- missing intake manifest
- unreadable JSON
- invalid schema version
- malformed rows
- no rows

Suggested statuses:

- `invalid_accepted_intake_manifest`
- `invalid_accepted_intake_schema`
- `no_rows`
- `ok`

## Testing

Add focused tests for:

- valid intake manifest -> adapted source manifest success
- output rows use only canonical source paths
- stable id mapping from `clip_id`
- provenance fields preserved
- invalid schema rejected
- no-rows manifest handled explicitly
- CLI compact output
- CLI full-json passthrough

## Verification

After implementation:

- run targeted unit tests for adapter and CLI route
- adapt:
  - [accepted-clip-intake-15e8592f6d81.manifest.json](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/outputs/accepted_clip_intake/marvel_rivals/accepted-clip-intake-15e8592f6d81.manifest.json)
- inspect:
  - row count matches intake manifest
  - canonical paths only
  - provenance fields present
  - no duplicate-variant data reappears

## Expected Outcome

This adapter creates the next stable boundary without changing downstream
workflow contracts:

- accepted intake remains the frozen operational selection
- downstream workflows continue reading source manifests
- one small adapter bridges the two

That is the smallest reliable way to move accepted clips toward reviewable
pipeline inputs while preserving manifest discipline.

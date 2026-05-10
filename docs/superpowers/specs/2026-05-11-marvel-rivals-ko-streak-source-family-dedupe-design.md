## Summary

Reduce duplicate local KO-streak miner results by deduping related artifacts that represent the same underlying gameplay source.

The current miner treats original clips, proxy-review bridge clips, and fused-review bridge clips as independent media. That is correct at the file level, but it is too noisy for evidence discovery. Multiple derived artifacts from the same source moment can consume top-ranked slots and reduce unique local coverage.

## Problem

The latest full-corpus miner summary shows this pattern clearly:

- original source clip appears in the top results
- one or more `proxy-review-*` clips for the same source also appear
- one or more `fused-review-*` clips for the same source also appear

Example cluster:

- `ABSOLUTE CINEMA_3522796292.mp4`
- `proxy-review-1317da0a9b85-000-absolute-cinema-3522796292.mp4`
- `fused-review-83fb6f99a464-000-absolute-cinema-3522796292.mp4`
- `fused-review-83fb6f99a464-001-absolute-cinema-3522796292.mp4`
- `fused-review-83fb6f99a464-002-absolute-cinema-3522796292.mp4`

These rows inflate apparent coverage without increasing unique gameplay evidence.

## Goal

Prefer one representative miner candidate per underlying source family so top-ranked miner outputs cover more unique local gameplay clips.

Success means:

- duplicated bridge/media variants no longer crowd out unique source clips
- the miner still preserves the strongest candidate from each source family
- downstream exporter input is higher-signal and less repetitive

## Non-Goals

This slice does not:

- dedupe final runtime or fused sidecars
- change published KO-streak thresholds
- change exporter confirmation logic
- remove bridge artifacts from disk
- redesign review bridge preparation

## Approach Options

### Option 1: Exporter-only dedupe

Keep the miner unchanged and dedupe only after the exporter loads the summary.

Pros:
- smallest exporter-only change

Cons:
- miner artifacts and saved ROI/match images remain noisy
- top-ranked summary still misrepresents unique coverage

### Option 2: Miner-level source-family dedupe

Collapse related artifacts before final global ranking output.

Pros:
- fixes the problem at the earliest useful stage
- improves miner summary, ROI images, and exporter input together
- recommended

Cons:
- requires explicit source-family derivation heuristics

### Option 3: Hard path exclusions

Exclude `proxy-review-*` and `fused-review-*` clips entirely.

Pros:
- simplest conceptual rule

Cons:
- too blunt
- bridge clips can still be useful fallback evidence when the original source clip is absent from the scanned bucket

## Decision

Adopt Option 2.

Add miner-level source-family dedupe that preserves the strongest row per source family while allowing standalone clips with no derived relationship to pass through unchanged.

## Proposed Design

### Source family identifier

Introduce a source-family key derived from the clip filename.

Initial normalization rules:

- plain gameplay clip:
  - family key is the normalized stem of the clip itself
- `proxy-review-*` clip:
  - family key is the normalized trailing source portion after the generated review prefix
- `fused-review-*` clip:
  - family key is the normalized trailing source portion after the generated review prefix and index

Normalization should:

- lowercase
- replace runs of non-alphanumeric characters with single separators
- preserve the stable content-bearing clip tail

The purpose is not to perfectly reconstruct the original filename. The purpose is to group obvious derived variants with their source family consistently.

### Deduplication stage

Apply source-family dedupe after:

- per-clip scoring
- same-clip timestamp-window dedupe
- per-clip `top_per_clip` trimming

Apply source-family dedupe before:

- final global ranking
- ROI/match image materialization
- final `top_global` truncation

This keeps the strongest candidate from each artifact family while still honoring the current clip-local logic.

### Winner selection within a source family

When multiple rows map to the same source family, keep one winner using this ranking:

1. highest `score`
2. earliest `timestamp_seconds`
3. prefer non-derived source clip over proxy/fused review variants
4. deterministic clip-path tie-break

The explicit preference for the original source clip matters. If the original clip and a proxy/fused derivative score similarly, the original is the better representative for unique local evidence.

### Summary visibility

Add additive miner summary metadata:

- `source_family_dedupe_enabled: true`
- `source_family_result_count`
- `source_family_drop_count`

Per-result rows should also carry:

- `source_family_key`
- `source_artifact_kind`

Allowed `source_artifact_kind` values for this slice:

- `source`
- `proxy_review`
- `fused_review`
- `unknown`

This keeps the dedupe behavior inspectable.

## Output Contract

Existing miner outputs remain the same shape:

- `summary.json`
- ROI images
- match images

Contract changes are additive:

- new per-row metadata
- new top-level summary counters

No downstream consumer should break because rows still contain the existing fields used by the exporter.

## Error Handling

If a clip name cannot be confidently parsed into a derived family:

- classify it as `unknown`
- fall back to using its own normalized stem as the source-family key

This avoids brittle parse failures.

## Verification

Verification for this slice should compare the latest full-corpus miner pass before and after dedupe.

Checks:

1. top results no longer include multiple bridge variants of the same source family
2. `source_family_drop_count` is non-zero on the current full-corpus batch
3. `source_family_result_count` is lower than or equal to the pre-dedupe retained count
4. unique source-family coverage in the top results improves
5. exporter can still consume the new miner summary unchanged

## Risks

### Risk: over-grouping unrelated clips

Mitigation:

- keep normalization conservative
- only parse obvious `proxy-review-*` and `fused-review-*` prefixes
- otherwise fall back to the clip’s own normalized stem

### Risk: losing a stronger derived artifact

Mitigation:

- winner ranking is score-first
- original-source preference only resolves close ties after score and timestamp ordering

### Risk: hiding useful bridge evidence

Mitigation:

- dedupe only affects final retained miner rows
- raw per-clip scanning still happens for all clips
- bridge clips remain eligible when they are the strongest family representative

## Resulting Operator Posture

After this change:

1. miner scans the same local corpus
2. miner keeps per-clip best rows as before
3. miner collapses duplicate bridge/source variants into one source-family representative
4. exporter receives a cleaner summary with better unique gameplay coverage

This is the smallest reliable change that improves unique local KO-streak evidence discovery without weakening the published runtime contract.

## Goal

Improve unique local KO-streak evidence coverage by deduping related source, proxy-review, and fused-review artifacts at the miner stage.

## Scope

In scope:

- `tools/marvel_rivals_medal_candidate_miner.py`
- additive miner `summary.json` metadata
- bounded rerun of the miner and exporter for operational verification

Out of scope:

- exporter confirmation logic
- published runtime thresholds
- ROI changes
- runtime/fusion schema changes
- removal of any bridge artifacts from disk

## Implementation Steps

### 1. Add source-family derivation helpers

Extend `tools/marvel_rivals_medal_candidate_miner.py` with helpers that derive:

- `source_family_key`
- `source_artifact_kind`

Rules:

- plain gameplay clip:
  - `source_artifact_kind = "source"`
  - family key derived from normalized clip stem
- `proxy-review-*` clip:
  - `source_artifact_kind = "proxy_review"`
  - family key derived from the trailing source portion after the generated prefix
- `fused-review-*` clip:
  - `source_artifact_kind = "fused_review"`
  - family key derived from the trailing source portion after the generated prefix/index
- fallback:
  - `source_artifact_kind = "unknown"`
  - family key derived from normalized clip stem

Normalization should be conservative:

- lowercase
- collapse non-alphanumeric runs to separators
- preserve the stable content-bearing tail

### 2. Attach source-family metadata to retained rows

Every retained miner row should include:

- `source_family_key`
- `source_artifact_kind`

This must happen before final global sorting and image materialization so downstream consumers can inspect the dedupe basis.

### 3. Add source-family dedupe stage

Insert a new dedupe stage after:

- per-clip scan
- same-clip timestamp dedupe
- per-clip `top_per_clip` trimming

Insert it before:

- final global `top_global` truncation
- image materialization
- final summary write

Implementation behavior:

- group retained rows by `source_family_key`
- keep one winning row per family

Winner ranking:

1. highest `score`
2. earliest `timestamp_seconds`
3. prefer `source` over `proxy_review` over `fused_review` over `unknown`
4. deterministic `clip_path` tie-break

### 4. Add additive summary metadata

Extend `summary.json` with:

- `source_family_dedupe_enabled: true`
- `source_family_result_count`
- `source_family_drop_count`

Existing fields stay intact.

### 5. Preserve current image/export compatibility

Do not change:

- ROI image generation
- match image generation
- existing row fields consumed by the exporter

The exporter should continue to accept the summary without modification.

## Verification Plan

### A. Static verification

Run:

```bash
python3 -m py_compile tools/marvel_rivals_medal_candidate_miner.py
```

### B. Operational miner verification

Rerun the full-corpus miner:

```bash
source .venv/bin/activate
python3 tools/marvel_rivals_medal_candidate_miner.py --max-duration-seconds 30 --top-per-clip 2
```

Verify:

1. `summary.json` contains:
   - `source_family_dedupe_enabled`
   - `source_family_result_count`
   - `source_family_drop_count`
2. retained rows contain:
   - `source_family_key`
   - `source_artifact_kind`
3. obvious duplicate families such as:
   - `ABSOLUTE CINEMA`
   - its `proxy-review-*`
   - its `fused-review-*`
   no longer occupy separate final top-ranked rows

### C. Exporter compatibility verification

Run the existing widened exporter against the new miner summary:

```bash
source .venv/bin/activate
python3 tools/marvel_rivals_ko_streak_exporter.py \
  --summary-path <new-summary-path> \
  --top-unique-clips 20 \
  --max-seeds-per-clip 3
```

Verify:

1. exporter still runs without code changes
2. `unique_clip_count` in the miner summary improves or duplicate-family concentration drops materially
3. if confirmed local clip count stays the same, confirm that the searched set now reflects more unique underlying source families

## Risks And Mitigations

### Risk: bad family parsing

Mitigation:

- keep family parsing narrow and explicit for known bridge prefixes
- fall back to the clip’s own normalized stem

### Risk: strongest row comes from a derived clip and gets replaced incorrectly

Mitigation:

- ranking is score-first
- source preference applies only after score and timestamp ordering

### Risk: exporter loses useful seeds

Mitigation:

- dedupe happens after per-clip retention, not before scanning
- one strongest representative per source family is kept

## Done Criteria

This slice is done when:

- miner emits source-family metadata
- miner collapses obvious bridge/source duplicates
- summary reports source-family dedupe counters
- exporter still consumes the deduped summary
- operational rerun shows cleaner unique coverage even if confirmed clip count does not increase

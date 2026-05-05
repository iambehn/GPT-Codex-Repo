# Version 2 Roadmap

This document is the canonical V2 roadmap for the repo. It supersedes the prior split between `FUTURE_FEATURES_ROADMAP.md` and `V2_KICKOFF.md`.

Its job is to hold three things in one place:

- the V2 operating model
- the V2 execution sequence
- the V2 knowledge-management rules that determine where decisions, experiments, and overlapping ideas belong

This roadmap should stay concise and operational. Raw notes remain useful inputs, but they are not equal-weight planning authorities.

This roadmap builds on the V1 baseline summarized in [V1_NOTES_CONDENSED_SUMMARY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/V1_NOTES_CONDENSED_SUMMARY.md).

---

## Operating Model

V2 keeps the current architectural backbone:

- manifest-driven, staged pipeline
- cheap-signal-first gating before expensive inference
- deterministic or explainable intermediate artifacts
- review, replay, and calibration loops as decision surfaces
- heavy multimodal reasoning only on narrowed candidate sets

The main V2 policy is:

- improve operational clarity, queryable state, and evidence quality before expanding detector breadth, dashboard scope, or ML replacement

The system should continue to separate:

- detection and proposal generation
- normalized signal mapping and fusion
- candidate ranking and reranking
- editorial packaging and hook logic
- review, export, posting, and downstream analytics

Source enrichment also stays separate:

- leaderboards, wikis, and identity resolution are scored enrichment layers
- they are not assumed truth sources and they are not runtime dependencies for highlight detection

---

## Baseline And Next Phases

### Completed Baseline

These should be treated as established foundations, not active V2 sequencing work:

- manifest-driven game packs and published runtime contracts
- ROI-first template matching and runtime signal normalization
- deterministic runtime event mapping
- proxy-first upstream filtering
- multi-signal fusion with temporal gates and synergy
- gold-set validation, replay, calibration, and contract-audit flows
- unified replay/debug and report-aware inspection
- fixture-sidecar comparison, fixture trial runner, and trial batch operator flow
- persistent registry and queryable artifact state
- fused-event candidate lifecycle registry
- fused highlight-selection export linkage
- hook candidate sidecars with registry, viewer, and review-app integration

Baseline complete does not mean contract-hardening complete.

The repo already has meaningful implementation coverage in manifests, runtime normalization, fusion, replay, and export lineage. The active V2 problem is to harden those foundations into blocking publish rules, shared contracts, measurable release gates, and downstream traceability.

### Current Pipeline Stability

The pipeline should currently be treated as:

- stable in upstream detection, replay, registry, lifecycle, export-selection, and hook-artifact foundations
- not yet complete in orchestration, post/export progression, hook evaluation, and downstream analytics feedback loops

The following are explicitly partial, not done:

- hook-aware calibration and replay
- export/post lifecycle progression as a full operating loop
- registry-backed operator workflow beyond query/state infrastructure
- analytics wired back to measurable candidate, hook, and export lineage

### Active Execution Phases

Only the items below should be treated as the active V2 implementation order:

1. Manifest and publish hardening
2. Runtime and fusion contract hardening
3. Review, calibration, and replay release gating
4. Candidate, export, and downstream analytics integration

### Deferred Phases

These matter, but they should sequence after the active phases above:

- source-enrichment scoring expansion when it is justified by downstream usage
- training-readiness and structured label capture for later learned systems
- shadow learned ranking and fusion
- heavier detector additions only where replay and gold sets show real recall gaps

### Ordering Rule

Keep this rule explicit:

- no broader ML replacement, large dashboard work, or analytics-heavy expansion before orchestration, hook evaluation, export/post state, and downstream lineage are explicit and stable

---

## Cross-Cutting V2 Notes

These points are worth carrying into planning because they sharpen the current roadmap without replacing its sequencing model.

### Canonical Media Contract

One open design question remains important enough to stay visible in the roadmap:

- the repo still needs one canonical source/media contract for a VOD or fixture run
- that contract should make raw source identity, downloaded or mezzanine asset identity, proxy identity, hashing, retry semantics, and storage lifecycle explicit
- more tooling around proposals, reranking, and live-mode experiments should stay secondary to this contract

### Storage And Interchange Direction

The most consistent storage direction from the Drive notes is:

- JSON for manifests, sidecars, ledgers, and service payloads
- Parquet as the preferred future format for feature tables, candidate tables, and experiment ledgers when table volume justifies it
- OTIO as the canonical editorial graph
- FCPXML, AAF, EDL, or render-service payloads only as boundary exports
- CSV only for reviewer exports, audits, and lightweight interchange

### Batch Before Live

The default operating posture should remain:

- VOD-first and batch-first for analysis, benchmarking, and regression safety
- local or object-backed media as the active processing source of truth
- Google Drive for notes, docs, and review-support artifacts, not as the hot-path media store
- live or near-live mode only after the batch path is operationally stable

### Stage Metrics To Keep In View

The roadmap should keep these evaluation lenses visible as future implementation and experiment work is planned:

- cheap proposal recall at budget
- mid-stage precision lift
- top-N hit rate for ranked candidate sets
- boundary accuracy or temporal overlap quality
- editor acceptance rate
- quarantine rate and dedupe failure rate
- time to first good cut
- manual adjustments per accepted clip
- export failure rate
- downstream engagement or retention proxy families by clip family and game

---

## Execution Phases

### Phase 1. Manifest And Publish Hardening

Goal:

- make game-pack publication fully manifest-driven, draft-safe, and operationally blocking when required evidence is missing

Deliverables:

- explicit baseline-to-derived manifest flow for onboarding drafts
- blocking publish validation for unresolved required rows
- explicit required vs optional row semantics, including unsupported-family handling
- persisted-state checks that keep draft readiness, review application, and published outputs aligned

What becomes queryable or inspectable:

- which derived rows are required, optional, resolved, or still unresolved
- why a draft is publish-blocked and which rows or families need action
- whether persisted draft state agrees with publish-readiness results

Acceptance criteria:

- publish fails whenever required rows remain unresolved
- unsupported families are explicitly marked optional rather than silently ignored
- review-apply paths cannot leave readiness, manifest state, and published outputs in conflict

Out of scope:

- broader detector expansion
- downstream post or analytics behavior

### Phase 2. Runtime And Fusion Contract Hardening

Goal:

- make proxy, runtime CV, audio, and fused layers converge on one auditable signal contract with explainable gating and timing behavior

Deliverables:

- one canonical normalized signal contract across modalities
- explicit provenance and confidence requirements for all normalized signals
- tightened fusion rules that preserve decomposability and cheap-to-expensive gating
- fixture coverage for timing, disagreement, and gating regressions

What becomes queryable or inspectable:

- how a fused event was formed from upstream signal families
- where timing windows, thresholds, or confirmation logic changed
- whether disagreement cases are preserved instead of collapsed into opaque scores

Acceptance criteria:

- normalized signals preserve time anchor, modality, detector identity, confidence, and provenance
- fused outputs remain inspectable enough to explain why a candidate scored or failed
- heavy analysis remains shortlist-only and does not become a full-path default

Out of scope:

- editorial packaging or hook-specific policy
- downstream posting and metrics

### Phase 3. Review, Calibration, And Replay Release Gating

Goal:

- make review-backed comparison and calibration the release gate for detector, fusion, schema, and publish-workflow changes

Deliverables:

- explicit per-stage acceptance checks for manifests, runtime behavior, fusion quality, and review load
- replay and calibration artifacts wired into a stable operator workflow
- reusable reviewer decision surfaces that can later reduce ambiguity and manual load

What becomes queryable or inspectable:

- recommendation and disagreement states across baseline vs trial artifacts
- reviewer approvals and rejections as reusable evidence
- regressions that would block promotion before they reach publish or downstream selection

Acceptance criteria:

- non-trivial detector, fusion, schema, or publish changes flow through measurable review-backed validation
- replay and calibration outputs remain stable and inspectable
- recommendation artifacts remain human-decision aids rather than silent auto-promotion logic

Out of scope:

- broader lifecycle automation
- analytics-driven ranking changes

### Phase 4. Candidate, Export, And Downstream Analytics Integration

Goal:

- harden candidate progression, export artifacts, post-ledger lineage, and downstream analytics as one traceable chain

Deliverables:

- explicit candidate-to-export-to-post lineage across artifact families
- export/post integration that preserves upstream authority boundaries
- downstream performance views grouped by candidate, hook, export path, and game context

What becomes queryable or inspectable:

- which candidates were selected, exported, posted, and later measured
- where a candidate fell out of the chain between approval and distribution
- downstream performance context without redefining upstream candidate truth

Acceptance criteria:

- performance can be traced back to candidate, hook, and export lineage
- analytics stay downstream and do not redefine upstream evidence truth
- the repo has enough structured evidence to support later learning-oriented work

Out of scope:

- shadow learned ranking implementation
- broad analytics platform replacement

---

## Documentation Model

V2 needs an explicit source-of-truth structure so the note corpus stops acting like a flat pile of equal-weight documents.

### Canonical Documentation Spine

The intended doc model is:

- one canonical roadmap: this file
- one source-of-truth index: `docs/v2/INDEX.md`
- subsystem docs for stable logic:
  - architecture / operating model
  - manifest contracts / game packs
  - detection / runtime / fusion
  - hook / editorial packaging
  - review / calibration / replay
  - registry / orchestration / state
  - source enrichment / identity resolution
  - distribution / post ledger / analytics
- ADR-style decision records for major choices
- experiment and calibration ledger for measured outcomes
- raw-note catalog for traceability back to Drive notes

### Category Rules

Future V2 content should be organized under these categories:

- `Architecture / Operating Model`
- `Manifest Contracts / Game Packs`
- `Detection / Runtime / Fusion`
- `Hook / Editorial Packaging`
- `Review / Calibration / Replay`
- `Registry / Orchestration / State`
- `Source Enrichment / Identity Resolution`
- `Distribution / Post Ledger / Analytics`
- `Experiments / Trials / Decisions`
- `Archive / Raw Notes`

Each category should have one canonical home for stable logic. Supporting artifacts should point back to that canonical location rather than restating it in full.

### Overlap Policy

Overlapping ideas are expected. They should be handled like this:

- canonical logic is written once
- other docs reference the canonical explanation
- local docs may include short contextual summaries
- full duplicated policy is discouraged
- decisions go in ADRs
- measured outcomes go in experiment ledgers
- exploratory thinking stays in raw notes or archive materials

This is the rule for recurring topics like:

- interaction and synergy logic
- hook reasoning
- registry-before-automation decisions
- enrichment confidence models
- post-performance interpretation

---

## Doc Spine Links

The roadmap should stay linked to the existing V2 doc spine rather than restating subsystem design:

- architecture / operating model: [docs/v2/ARCHITECTURE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/ARCHITECTURE.md)
- manifest contracts / game packs: [docs/v2/MANIFEST_CONTRACTS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/MANIFEST_CONTRACTS.md)
- detection / runtime / fusion: [docs/v2/DETECTION_RUNTIME_FUSION.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DETECTION_RUNTIME_FUSION.md)
- registry / orchestration / state: [docs/v2/REGISTRY_ORCHESTRATION_STATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REGISTRY_ORCHESTRATION_STATE.md)
- hook / editorial packaging: [docs/v2/HOOK_EDITORIAL_PACKAGING.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/HOOK_EDITORIAL_PACKAGING.md)
- review / calibration / replay: [docs/v2/REVIEW_CALIBRATION_REPLAY.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/REVIEW_CALIBRATION_REPLAY.md)
- distribution / post ledger / analytics: [docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/DISTRIBUTION_POST_LEDGER_ANALYTICS.md)

Future work products should follow this split:

- roadmap: sequencing and phase ownership
- subsystem docs: stable design rules
- ADRs: major decisions
- experiment ledger: measured trial outcomes

---

## Potential Tools To Consider

These are planning candidates, not default commitments. They should be pulled in only where they materially strengthen one of the active or deferred roadmap tracks.

### Repo-Local Skills And Process Tools

- `media-probe`: explicit probe, hash, proxy, and ingest normalization workflow
- `candidate-proposer`: cheap proposal generation and proposal-manifest materialization
- `multimodal-ranker`: shortlist-only semantic reranking over narrowed candidate sets
- `benchmark-harness`: fixture comparison, regression, and gated benchmark execution
- `timeline-interop`: OTIO-first assembly plus downstream export adapters
- `render-regression`: preview or render diff checks for editorial output changes
- `license-guard`: dependency and model-license provenance checks before adoption
- GitHub review helpers: CI/debug and review-thread resolution once repo workflows need more operator support
- browser or review-UI testing helpers: only when reviewer-facing UI surfaces become materially broader

### External Runtime And Detection Candidates

- FFmpeg and ffprobe remain core dependencies for ingest, probe, proxy generation, and render plumbing
- GStreamer is a live-mode candidate, not the default VOD backbone
- PySceneDetect remains a cheap baseline gate for shot or scene proposals
- TransNetV2 is a high-value trial candidate for learned boundary detection
- Whisper remains a strong default ASR backbone
- pyannote.audio is a strong follow-on candidate for diarization and speaker-structure signals
- AudioSet-derived audio-event models are worth considering for richer non-speech excitement signals
- SigLIP is a strong candidate for frame-level semantic relevance, dedupe, and novelty signals
- X-CLIP is a strong candidate for clip-level semantic scoring in shortlist reranking
- managed cloud video APIs should be treated as comparison baselines or niche enrichments, not default core dependencies

### Experiment And Governance Tooling

- MLflow is a strong future candidate for experiment lineage, dataset logging, champion or challenger tracking, and traceable benchmark runs
- MLflow tracing is especially relevant for multi-step Codex-assisted workflows once the artifact contracts are stable enough to make traces useful
- any future experiment tooling should log params, metrics, artifacts, config snapshots, and dataset identity rather than creating opaque “winning runs”

---

## Success Conditions

This V2 roadmap is working if:

- there is one active sequencing authority in the repo
- completed baseline is clearly separated from active next phases
- partial operating loops are named explicitly instead of drifting between done and future
- another engineer can decide what to build next without reconstructing dependencies from chat history
- future implementation suggestions can be traced back to one explicit phase rather than improvised sequencing

## Assumptions

- the canonical V2 roadmap already existed, but its first-wave phase language was stale relative to implemented work
- recent registry, lifecycle, export, and hook work should be treated as completed baseline for planning purposes
- the current useful planning horizon ends at post ledger and analytics, not shadow learning or broader ML replacement
- the immediate planning problem is sequencing clarity, not feature ideation

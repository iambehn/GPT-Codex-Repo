# Version 2 Roadmap

This document is the canonical V2 roadmap for the repo. It supersedes the prior split between `FUTURE_FEATURES_ROADMAP.md` and `V2_KICKOFF.md`.

Its job is to hold three things in one place:

- the V2 operating model
- the V2 implementation sequence
- the V2 knowledge-management rules that determine where decisions, experiments, and overlapping ideas belong

This document should stay concise and operational. Raw notes remain useful inputs, but they are no longer equal-weight planning authorities.

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

## Baseline vs Active V2

### Already Baseline

These should be treated as established foundations, not re-planned V2 work:

- manifest-driven game packs and published runtime contracts
- ROI-first template matching and runtime signal normalization
- deterministic runtime event mapping
- proxy-first upstream filtering
- multi-signal fusion with temporal gates and synergy
- gold-set validation, replay, calibration, and contract-audit flows
- fused candidate export and review preparation
- fixture-sidecar comparison, trial runner, and batch-operator workflow
- unified replay/debug viewing across proxy, runtime, and fused artifacts

### First-Wave V2

Only the items below should be treated as the active V2 implementation order:

1. Canonical documentation and source-of-truth setup
2. Unified replay/debug and report-aware inspection hardening
3. Persistent registry and queryable artifact state
4. Candidate and review lifecycle hardening
5. Hook candidate artifact family and measurable editorial logic
6. Fixture-driven trial, comparison, and calibration operating-loop hardening
7. Orchestration and state-machine hardening
8. Source-enrichment pilot as a scored subsystem
9. Distribution, post ledger, and analytics wiring
10. Training-readiness and shadow learned ranking/fusion

Important ordering rule:

- no major detector expansion, broad dashboarding, or larger ML replacement work before replay, state, and review evidence are stable

### Mid-Horizon

These matter, but they should follow the first-wave work above:

- structured training-row export and label capture
- shadow learned fusion or ranking experiments
- destination-aware routing and action scoring
- broader experiment indexing and multi-run analytics
- selective heavier detector additions where replay and gold sets show real recall gaps

### Research / Later

These stay explicitly exploratory until earlier V2 work is stable:

- end-to-end learned replacement of major pipeline stages
- broad analytics platform work before registry/state maturity
- large orchestrator or platform replacement
- generalized metadata ingest systems without proven downstream value
- aggressive creative-automation layers that outrun review and evidence quality

---

## Documentation Model

V2 now needs an explicit source-of-truth structure so the note corpus stops acting like a flat pile of equal-weight documents.

### Canonical Documentation Spine

The intended doc model is:

- one canonical roadmap: this file
- one future source-of-truth index: `docs/v2/INDEX.md`
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

---

## Implementation Sequence

### 1. Canonical Documentation And Source-of-Truth Setup

Purpose:

- stop further roadmap and note drift
- establish one doc spine before more V2 work accumulates

Required outcomes:

- canonical V2 roadmap is in place
- future subsystem docs have defined landing zones
- decisions, experiments, and archive material are separated by purpose

### 2. Unified Replay / Debug And Inspection Hardening

Purpose:

- make one clip and its artifacts easier to inspect than raw sidecar reading

Required outcomes:

- stronger provenance display
- clearer baseline/trial/report overlays
- easier debugging of review disagreement and calibration failures

Why early:

- this still has the highest leverage over nearly every later decision

### 3. Persistent Registry And Queryable Artifact State

Purpose:

- move beyond file-only state into queryable operational state

Minimum intended scope:

- clips
- artifacts and runs
- reviews
- candidate state
- comparison and replay outcomes
- export/post state when those layers arrive

Why now:

- stateful operations, reporting, and orchestration need a durable index over the current artifact system

### 4. Candidate / Review Lifecycle Hardening

Purpose:

- turn candidates into an explicit operating queue instead of a loose artifact set

Minimum intended states:

- pending review
- approved
- rejected
- exported
- posted

Why now:

- better lifecycle handling improves labels, reduces operator glue, and prepares downstream export/post flows

### 5. Hook Candidate Artifact Family

Purpose:

- make editorial packaging and narrative-hook reasoning measurable instead of anecdotal

Minimum intended capability:

- explicit hook candidate sidecar or equivalent artifact family
- hook-related review fields
- hook-aware comparison and calibration surfaces

Why now:

- the newer V2 notes make hook quality part of pipeline value, not a purely downstream edit choice

### 6. Fixture-Driven Trial / Comparison / Calibration Hardening

Purpose:

- tighten the operator loop around baseline vs trial judgment

Required outcomes:

- less manual glue across runs, comparisons, viewer inspection, and review
- cleaner recommendation artifacts
- clearer experiment traceability

### 7. Orchestration And State-Machine Hardening

Purpose:

- improve retries, idempotency, recovery, and sequencing after state models are explicit

Why later than registry:

- orchestration should automate clear state, not automate ambiguity

### 8. Source-Enrichment Pilot

Purpose:

- test leaderboard, wiki, and identity-resolution enrichment as a scored subsystem

Policy:

- official-source-first where possible
- tracker and public-profile data treated as enrichment, not truth
- weak matches quarantined for review
- success measured by yield and review burden, not scrape volume

### 9. Distribution, Post Ledger, And Analytics Wiring

Purpose:

- connect approved candidates to auditable posting and performance state

Policy:

- candidate pipeline remains upstream authority
- post records and metrics remain downstream ledgers
- vendor tooling is acceptable early for posting and community operations

### 10. Training-Readiness And Shadow Learned Ranking/Fusion

Purpose:

- prepare for learned systems without replacing the heuristic pipeline prematurely

Policy:

- use review-backed artifacts and fixture evidence first
- learned systems begin in shadow or trial mode
- adoption depends on explicit offline and operator-visible gains

---

## Knowledge-Management Rules

The V2 note set is now dense enough that documentation is part of the technical operating model.

Use these rules going forward:

- roadmap files define sequence and policy, not exhaustive theory
- subsystem docs define stable current logic
- ADRs record why important choices were made
- experiment ledgers record measured outcomes and recommendations
- raw notes preserve research depth and ideation
- no major planning topic should require rereading the full note archive to understand current repo direction

The practical test is simple:

- a new note about hook logic should have one obvious canonical destination
- a calibration result should have a ledger home separate from design prose
- a major sequencing decision should have an ADR home
- overlapping ideas should be reference-linked, not rewritten in full across multiple docs

---

## Success Signals

This V2 roadmap is working if:

- there is one active roadmap authority in the repo
- implementation order is clearer than the prior split between roadmap and kickoff docs
- note overlap no longer forces repeated full rereads
- future docs can be placed by category without reopening organizational debates
- replay, state, review, and hook work become easier to sequence without ambiguity

---

## Assumptions

- the newer V2 note cluster is authoritative enough to replace the prior split roadmap setup
- the documentation problem is now part of the technical problem because note density affects implementation clarity
- raw Drive notes should remain preserved, but should stop functioning as day-to-day source-of-truth planning documents
- V2 should continue optimizing for operational clarity, queryable state, and evidence loops before broader detector or model expansion

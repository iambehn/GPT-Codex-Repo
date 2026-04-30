# Methodology Current-Pipeline Action Roadmap

This roadmap translates [Methodology.md](/Users/tj/Downloads/Methodology.md) into a repo-facing plan for the current gaming pipeline subset.

It stays focused on the core question the note is actually answering:

- how we measure signals
- how we score them
- how we calibrate them
- how we validate them
- how we treat scraping as signal extraction

It is not a broad theory document for future podcast, UGC, or creative-editing systems. It complements the existing blueprint and roadmap docs rather than replacing them.

---

## Already Covered

These ideas are already represented at a high level in the existing blueprint and roadmap set:

- layered detector flow
- confidence-driven judgment
- quarantine as a recovery path
- proxy-first cheap filtering
- atomic events as a later semantic layer

The methodology roadmap should build on those ideas instead of restating them.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. Signal Definitions And Manifests

Turn signal design into a concrete backlog for defining signals as first-class measured quantities.

Required roadmap behavior:

- each important signal should be defined in terms of:
  - what it measures
  - what it is a proxy for
  - known false positives
  - expected range
  - expected base rate
- detector outputs should converge toward a normalized manifest shape with:
  - timestamp
  - source
  - confidence
  - evidence summary
  - optional noise or quality hints

This workstream should connect to the proxy and atomic-event roadmaps rather than redefine them.

### B. Noise Rejection And Event Aggregation

Translate the note’s practical advice on noise into a deterministic backlog.

Required backlog items:

- confidence floors
- temporal smoothing and persistence checks
- context gates
- co-occurrence logging for correlated signals
- a distinct event aggregation layer between raw detections and final judgment

This roadmap should explicitly state that raw detector hits are not the judge interface. Composite events are.

### C. Calibration Layer

Turn the note’s calibration ideas into a concrete current-pipeline plan.

Required backlog items:

- per-game weight normalization
- acceptance and quarantine threshold tuning
- detector co-dependence review so correlated signals are not double-counted
- confidence calibration checks so score meaning stays consistent across games

Calibration should be framed as a tuning layer over existing heuristics, not as a new ML ranking system.

### D. Evaluation And Gold Sets

Make evaluation a first-class planned subsystem.

Required backlog items:

- a gold-set structure for manually verified clips
- an evaluation script or module separate from the runtime judge
- metrics for:
  - precision and recall
  - false positives and false negatives
  - threshold tradeoffs
  - calibration quality
- regression testing against known clips after detector or weight changes

This should be treated as the mandatory feedback loop for future proxy, event, and judge work.

### E. Scraping As Signal Extraction

Translate the note’s scraping section into an active backlog for signal quality.

Required backlog items:

- treat OCR and scraped fields as measured signals, not just convenience data
- attach confidence or completeness metadata to scraped outputs
- define schema-drift or malformed-output warnings
- connect scraped signals back into the same scoring and validation framework as visual and audio signals

This workstream should stay limited to extraction quality and validation, not full scraping architecture.

---

## Deferred

These ideas from the note are valid, but should stay out of the first methodology implementation phase:

- podcast or UGC methodology
- broad creative-editing methodology
- repo-wide statistical platform or dashboarding
- generalized “scientific engine” framing that does not directly change the current gaming pipeline

---

## Implementation Order

Build in this order:

1. Define important signal manifests and expected behavior.
2. Add deterministic noise rejection and event aggregation rules.
3. Add calibration-layer planning for weights and thresholds.
4. Add evaluation and gold-set planning as the measurement loop.
5. Add scraping-quality planning under the same signal framework.

This order keeps the methodology grounded in measurable pipeline improvements instead of abstract theory.

---

## Future Test Targets

The implementation following this roadmap should prove:

- important signals have explicit definitions, expected ranges, and failure modes
- raw detections can be transformed into event-level aggregates before judgment
- correlated signals are visible enough to detect double-counting risk
- threshold or weight changes can be evaluated against a gold set
- scraped outputs include confidence or completeness context
- no dashboard or future-content-system work is required for the first methodology implementation pass

---

## Assumptions

- this document is a roadmap, not a code or doc merge
- the correct file name is `METHODOLOGY_ACTIONS.md`
- this pass is limited to the current gaming pipeline subset of the note
- the roadmap should complement the existing blueprint and the proxy and atomic-event roadmaps rather than replace them

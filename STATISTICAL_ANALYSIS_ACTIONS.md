# Statistical Analysis Current-Pipeline Action Roadmap

This roadmap translates [StatisticalAnalysis (1).md](/Users/tj/Downloads/StatisticalAnalysis%20%281%29.md) into a repo-facing plan for the current gaming pipeline subset.

It stays narrow and practical. The focus is how statistical methods improve:

- signal measurement and feature definition
- thresholding and anomaly detection
- calibration and confidence meaning
- gold-set evaluation
- scrape validation and drift detection

It is not a broad analytics platform plan, a dashboard plan, or a full learned-model replacement for the current heuristic pipeline.

---

## Already Covered

These ideas are already reflected at a high level in the existing blueprint and roadmap set:

- confidence-driven scoring
- threshold tuning as a real problem
- gold sets as the evaluation loop
- proxy, audio, and event aggregation as feature sources
- scraping confidence and schema-drift concerns

The statistical-analysis roadmap should build on those ideas rather than restating them.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. Statistical Feature Inventory

Turn the note’s general statistical concepts into a concrete backlog for first-class pipeline features.

Required roadmap behavior:

- define which measured features should become standard across clips and detector outputs, such as:
  - confidence
  - entropy or quality penalties
  - persistence counts
  - timing density
  - co-occurrence counts
  - scrape completeness
- frame these as statistical features over existing signals, not new detector families
- require a flattenable per-clip feature view so later analysis is possible without changing the runtime pipeline

This workstream should complement signal manifests from `METHODOLOGY_ACTIONS.md`, not replace them.

### B. Thresholding And Anomaly Detection

Translate the note’s statistical-threshold ideas into a deterministic current-pipeline backlog.

Required backlog items:

- baseline-aware thresholds where simple fixed cutoffs are too brittle
- z-score or distribution-relative anomaly checks for:
  - audio spikes
  - proxy spikes
  - scrape value outliers
- anomaly handling for obviously malformed scraped values
- a clear distinction between:
  - normal variation
  - suspicious outlier
  - actionable signal

Important boundary:

- keep the first pass interpretable
- no need for clustering or mixture models as a first implementation requirement

### C. Calibration And Weight Tuning

Turn the note’s statistical calibration ideas into a concrete tuning plan.

Required backlog items:

- confidence calibration checks so score ranges mean roughly the same thing across games
- threshold tradeoff analysis for accept, quarantine, and reject boundaries
- co-dependence checks so correlated features are not effectively counted twice
- weight-tuning support that remains compatible with the heuristic judge

Important boundary:

- do not replace the heuristic scorer yet
- statistical analysis should inform weights and thresholds before any learned judge is attempted

### D. Evaluation Data And Gold-Set Analysis

Make statistical evaluation a first-class planned subsystem.

Required backlog items:

- a flattened evaluation dataset derived from clip manifests or sidecars
- a gold-set comparison workflow separate from runtime judgment
- planned metrics for:
  - precision and recall
  - false positives and false negatives
  - threshold tradeoffs
  - calibration quality
  - distribution drift over time
- regression detection after changes to weights, thresholds, or detectors

This should be described as the measurement loop that turns detector and judge changes into evidence.

### E. Statistical Scrape Validation

Translate the note’s scraping section into a current-pipeline statistical validation backlog.

Required backlog items:

- outlier checks for scraped numeric fields
- completeness and confidence scoring for extracted records
- schema-drift warnings when field distributions or structure change unexpectedly
- validation rules that connect scraped data quality back into the same score and calibration framework as detector signals

This workstream should stay limited to scrape reliability and anomaly detection, not broad scraping-system redesign.

---

## Deferred

These ideas from the note are valid, but should stay out of the first statistical implementation phase:

- dashboards and analytics surfaces
- broad learned-model replacement of the judge
- advanced multimodal ML stack design
- full Bayesian or probabilistic platform work
- generalized data-science infrastructure that does not directly improve the current gaming pipeline

---

## Implementation Order

Build in this order:

1. Define the statistical feature inventory and flattenable analysis view.
2. Add thresholding and anomaly-detection planning for signal families and scraped values.
3. Add calibration and weight-tuning planning.
4. Add gold-set and regression-analysis planning.
5. Add scrape-specific statistical validation planning under the same framework.

This order keeps the statistical work grounded in measurement and validation before any broader modeling ambitions.

---

## Future Test Targets

The implementation following this roadmap should prove:

- important clip and detector features are identifiable as a stable statistical feature set
- outlier logic can distinguish suspicious values from ordinary variation
- threshold changes can be evaluated against gold-set results instead of intuition
- correlated signals are visible enough to spot double-counting risk
- scrape outputs can be scored for completeness and anomaly risk
- no dashboard or full learned-model migration is required for the first statistical implementation pass

---

## Assumptions

- this document is a roadmap, not code
- the correct file name is `STATISTICAL_ANALYSIS_ACTIONS.md`
- this pass is limited to the current gaming pipeline subset of the note
- the roadmap should complement `METHODOLOGY_ACTIONS.md` by focusing on concrete statistical upgrades, not replace the broader methodology roadmap

# Calibration Layer Action Roadmap

This roadmap translates [OptimizationNotes (1).md](/Users/tj/Downloads/OptimizationNotes%20%281%29.md) into a repo-facing plan for the calibration layer of the current gaming pipeline.

It stays focused on the practical transition from:

- manual weights and thresholds
- hand-tuned judgment behavior

to:

- label-driven calibration
- repeatable threshold tuning
- experiment-backed weight updates

This is not a broad optimization or analytics-platform plan. It is the roadmap for turning the current heuristic scorer into a calibrated system without replacing it yet.

---

## Already Covered

These ideas are already present at a high level in the existing blueprint and roadmap set:

- weights and thresholds are explicit configuration surfaces
- gold sets are the long-term evaluation loop
- proxy, event, and methodology work already define the detector and feature side of the system
- confidence-driven judgment and quarantine already exist conceptually

The calibration roadmap should build on that baseline rather than restate it.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. Calibration Vocabulary And Boundaries

Turn the note’s terminology into a precise roadmap boundary.

Required roadmap behavior:

- use `calibration layer` as the primary term for the current stage
- distinguish:
  - `hyperparameter tuning` for weights
  - `decision threshold tuning` for accept/quarantine/reject cutoffs
  - `calibration` for making scores mean what they claim
  - `supervised learning` or `learning to rank` as later stages, not first implementation requirements

This keeps the roadmap aligned with the actual maturity of the current pipeline.

### B. Feature Matrix And Label Export

Make the note’s log-to-table recommendation the first concrete backlog item.

Required backlog items:

- a flattenable feature-matrix export from clip metadata or sidecars
- labels derived from review outcomes such as accepted, rejected, or quarantined
- a stable per-clip row shape that includes:
  - current score outputs
  - important detector features
  - entropy or quality penalties
  - persistence or stability indicators
  - final human verdict

Important boundary:

- this is an analysis/export layer, not a replacement runtime path
- the goal is to produce clean calibration input from existing artifacts

### C. Weight And Threshold Tuning Loop

Turn the note’s Optuna-style loop into a concrete current-pipeline plan.

Required backlog items:

- define an objective for weight and threshold tuning against labeled clips
- tune both weights and decision thresholds, not weights alone
- keep tuned outputs compatible with existing YAML or config-based scoring surfaces
- require held-out validation before replacing hand-tuned values

Tooling direction to record in the roadmap:

- Optuna as the first-class hyperparameter search tool
- scikit-learn as the first-class simple model/statistical baseline tool
- MLflow as the preferred open-source experiment-tracking layer

Important boundary:

- do not require automatic online learning
- do not replace the heuristic scorer with a learned model in the first phase

### D. Integrity-Weighted Calibration

Translate the note’s failure-mode focus into a backlog for feature integrity and noise-aware tuning.

Required backlog items:

- explicitly account for unstable or low-integrity detector signals during calibration
- treat persistence and temporal stability as features, not only raw confidence
- support calibration fixes for common failure modes such as:
  - loud audio with no real action
  - visual flicker or false-positive detections
  - strong action with weak or missing hook/context

Important boundary:

- temporal smoothing or filtering may become a prerequisite feature source for calibration
- but the roadmap should treat smoothing as an upstream dependency, not the calibration layer itself

### E. Calibration Evaluation And Promotion Rules

Make calibration changes auditable before they affect runtime scoring.

Required backlog items:

- compare tuned parameters against current defaults on a labeled set
- record whether tuning improves:
  - precision / recall
  - false-positive rate
  - threshold tradeoffs
  - calibration quality
- define promotion rules for when tuned weights can replace current manual values
- preserve rollback visibility for previous weight sets and trial outcomes

This workstream should connect directly to the methodology and statistical roadmaps instead of creating a separate evaluation philosophy.

---

## Deferred

These ideas from the note are valid, but should stay out of the first calibration-layer implementation phase:

- full learned-model replacement of the clip judge
- gradient-boosted ranking models as the primary scorer
- Bayesian fusion as a required first-pass dependency
- automatic online learning from downstream engagement
- dashboards beyond basic experiment tracking

---

## Implementation Order

Build in this order:

1. Define the calibration-layer vocabulary and scope boundaries.
2. Add the feature-matrix and label export plan.
3. Add the weight and threshold tuning loop plan around existing config seams.
4. Add integrity-weighted calibration inputs for common failure modes.
5. Add evaluation and promotion rules for tuned parameters.

This order keeps calibration grounded in existing artifacts and labels before introducing automated search.

---

## Future Test Targets

The implementation following this roadmap should prove:

- calibration inputs can be exported from existing clip artifacts into a stable feature matrix
- labels from review outcomes are usable for tuning runs
- tuned weights and thresholds can be evaluated against held-out clips before adoption
- unstable or low-integrity signals can be surfaced as calibration-relevant features
- experiment results are comparable enough to support promotion or rollback decisions
- no learned-judge migration is required for the first calibration-layer implementation pass

---

## Assumptions

- this document is a roadmap, not code
- the correct file name is `CALIBRATION_LAYER_ACTIONS.md`
- this pass is limited to the current gaming pipeline subset of the note
- the roadmap should complement `METHODOLOGY_ACTIONS.md` and `STATISTICAL_ANALYSIS_ACTIONS.md` by focusing specifically on the transition from manual tuning to label-driven calibration

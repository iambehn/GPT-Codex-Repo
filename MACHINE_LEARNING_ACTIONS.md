# Machine Learning Transition Action Roadmap

This roadmap translates [Machine Learning.md](/Users/tj/Downloads/Machine%20Learning.md) into a repo-facing plan for the current gaming pipeline subset of machine-learning work.

It stays narrow and practical. The focus is the near-term transition from:

- heuristic scoring with isolated ML touchpoints
- human review without structured training export

to:

- training-data capture from the existing pipeline
- learned fusion as a future replacement target for some heuristics
- human-in-the-loop feedback as a first-class ML data loop

It is not a full self-optimizing-pipeline plan, a generalized AI architecture, or a creative-editing ML roadmap.

---

## Already Covered

These ideas are already reflected at a high level in the existing docs:

- proxy-first cheap filtering
- YOLO or detector model use as a heavy signal stage
- quarantine review as a human recovery path
- calibration and statistical evaluation as planned subsystems
- atomic events as a later semantic layer

The machine-learning roadmap should build on that baseline rather than restate it.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. ML Transition Vocabulary And Boundaries

Turn the note’s broad ML framing into precise current-pipeline terminology.

Required roadmap behavior:

- define the current system as heuristic-first with ML-assisted subsystems
- define the near-term target as a `machine-learning transition layer`, not a full ML-native pipeline
- distinguish:
  - training-data capture
  - learned fusion
  - model-assisted ranking
  - future end-to-end prediction
- keep the first phase compatible with the current deterministic judge and config seams

This prevents the roadmap from collapsing calibration, statistics, and full ML into one undifferentiated plan.

### B. Training Data Export From Existing Artifacts

Make the note’s strongest concrete idea the first backlog item: turning existing runtime artifacts into ML-ready samples.

Required backlog items:

- define a training-sample export shape derived from current clip artifacts
- treat quarantine or review decisions as labels
- include feature blocks from proxy, audio, visual, semantic, and scoring outputs when available
- preserve metadata needed for later model training, such as:
  - game
  - timestamp or clip window
  - detector versions
  - human verdict
  - current heuristic scores

Important boundary:

- this is a data-export and schema problem first
- it should not require training a model before the export path exists

### C. Learned Fusion Planning

Translate the note’s “replace hand-tuned weights with learned weights” idea into a current-pipeline roadmap.

Required backlog items:

- define the first learned-fusion target as a simple supervised model over exported features
- keep the initial model class modest and interpretable:
  - logistic regression
  - gradient-boosted tree only as a later step
- frame learned fusion as augmenting or shadowing the current judge before replacing anything
- ensure learned outputs can be compared directly against the current heuristic score path

Important boundary:

- do not make a temporal model or transformer the first implementation target
- do not treat the virality score itself as the first training target unless labels support it

### D. Human-In-The-Loop Active Learning Loop

Turn the quarantine review concept into an explicit ML data loop.

Required backlog items:

- define how review actions become labeled training examples
- distinguish positive, negative, and ambiguous outcomes
- capture enough context from each reviewed clip to support later retraining
- preserve auditability so each label can be traced back to the source clip and feature state

This workstream should connect the review UI to model-improvement readiness without requiring a full training service in the first phase.

### E. Model Evaluation And Rollout Boundaries

Make ML adoption measurable and bounded.

Required backlog items:

- evaluate learned fusion against the current heuristic scorer on held-out labeled data
- define model-promotion rules based on measurable gains over the baseline
- require regression visibility before a learned scorer affects real runtime decisions
- keep early ML rollout limited to:
  - offline evaluation
  - shadow scoring
  - optional compare mode

Important boundary:

- no production cutover from heuristics to learned scoring until the model consistently beats the current baseline
- no full continuous-retraining pipeline in the first implementation phase

---

## Deferred

These ideas from the note are valid, but should stay out of the first machine-learning transition phase:

- full learned replacement of the entire clip pipeline
- learned hook placement and learned trimming as active implementation goals
- reward models driven by downstream social performance
- personalized editing models
- game-agnostic end-to-end clip detectors
- continuous retraining as an always-on production system

---

## Implementation Order

Build in this order:

1. Define ML transition vocabulary and scope boundaries.
2. Add the training-data export and sample schema plan.
3. Add the human-in-the-loop labeling loop plan around review outcomes.
4. Add learned-fusion planning against the exported feature set.
5. Add evaluation and rollout rules for offline and shadow-mode comparison.

This order keeps the ML work grounded in data readiness and evaluation before any model-first architecture shift.

---

## Future Test Targets

The implementation following this roadmap should prove:

- training samples can be exported from existing clip artifacts into a stable schema
- review outcomes can be turned into auditable labels
- a learned fusion model can be evaluated against the current heuristic scorer on held-out data
- early ML integration can run in offline or shadow mode without replacing runtime heuristics
- no end-to-end learned clip detector or creative-editing model is required for the first ML transition pass

---

## Assumptions

- this document is a roadmap, not code
- the correct file name is `MACHINE_LEARNING_ACTIONS.md`
- this pass is limited to the current gaming pipeline subset of the note
- the roadmap should complement `CALIBRATION_LAYER_ACTIONS.md` and `STATISTICAL_ANALYSIS_ACTIONS.md` by focusing on the transition from heuristic systems to ML-ready data and learned fusion, not replace those documents

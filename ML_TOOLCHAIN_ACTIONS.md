# ML Toolchain Bridge Action Roadmap

This roadmap translates [Scikit-Learn_Tensorflow.md](/Users/tj/Downloads/Scikit-Learn_Tensorflow.md) into a repo-facing plan for the baseline ML toolchain bridge on top of the current gaming pipeline.

It stays concrete. The focus is the missing bridge between:

- current runtime artifacts and labels
- a first learned fusion model
- low-latency inference that can sit near ingestion or proxy layers

It does not broaden into a full deep-learning roadmap or duplicate the higher-level machine-learning transition document.

---

## Already Covered

These ideas are already captured in [MACHINE_LEARNING_ACTIONS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/MACHINE_LEARNING_ACTIONS.md) and [CALIBRATION_LAYER_ACTIONS.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/CALIBRATION_LAYER_ACTIONS.md):

- training data as the prerequisite
- learned fusion as the first ML target
- offline evaluation before runtime replacement
- human review as a label loop

The toolchain roadmap should build on those ideas rather than restate them.

---

## Active Gaps

Only the gaps below should become active implementation backlog.

### A. Toolchain Vocabulary And Model Boundaries

Turn the note’s framework comparison into explicit project guidance.

Required roadmap behavior:

- define `scikit-learn / XGBoost / LightGBM` as the baseline toolchain for the first learned fusion model
- define `ONNX` as the preferred baseline inference artifact format
- define `PyTorch / TensorFlow` as later-stage frameworks, not first implementation requirements
- keep the first learned model narrow:
  - tabular or flattened-window features
  - CPU-friendly inference
  - direct comparison to the current heuristic judge

This keeps the roadmap from overcommitting to deep-learning infrastructure too early.

### B. Training Exporter As The First Concrete Build

Make the note’s strongest recommendation the first concrete backlog item.

Required backlog items:

- add a dedicated training-export layer as the single source of truth for ML-ready sample serialization
- define a stable window-centric export shape that can carry:
  - proxy features
  - audio features
  - visual features
  - current heuristic scores
  - labels
  - metadata and versioning fields
- treat this exporter as data plumbing only, not as a model-training module

Important boundary:

- the exporter must be stable and boring
- future models should depend on it without needing format churn

### C. Baseline Fusion Training Script

Turn the note’s model-training advice into a concrete current-pipeline plan.

Required backlog items:

- define `ml/train_fusion.py` as the first training entrypoint
- keep the first model class simple and deployment-friendly:
  - logistic regression or XGBoost-style baseline
- train on exported windows, not raw runtime artifacts
- compare the learned fusion output directly with the current heuristic score

Important boundary:

- no TensorFlow or PyTorch dependency is required for the first model
- the goal is a strong baseline model, not maximum model complexity

### D. Inference Wrapper And Runtime Attachment

Plan the runtime bridge without committing to a large serving system.

Required backlog items:

- define a lightweight inference wrapper that can load the exported baseline model artifact
- keep the first runtime integration near proxy or fusion scoring rather than replacing the whole pipeline
- attach model outputs in a way that supports:
  - heuristic-vs-model comparison
  - disagreement visibility
  - shadow-mode rollout

Important boundary:

- no hard production cutover in the first phase
- no heavy service architecture is required before the baseline model proves value

### E. Model Metadata, Versioning, And Compareability

Make model outputs auditable enough for iteration.

Required backlog items:

- define model-card or model-metadata expectations for each exported model
- version:
  - training schema
  - feature schema
  - model artifact
- preserve enough metadata to explain why one model differs from another
- support compare-mode outputs such as:
  - heuristic score
  - model score
  - disagreement score

This workstream should connect directly to calibration and ML evaluation instead of introducing a separate experiment philosophy.

---

## Deferred

These ideas from the note are valid, but should stay out of the first ML toolchain implementation phase:

- full PyTorch or TensorFlow model roadmap
- temporal transformers or TCNs as active implementation goals
- end-to-end clip detection models
- full model-serving platform design
- cloud deployment or generalized infrastructure strategy

---

## Implementation Order

Build in this order:

1. Define toolchain boundaries and baseline framework choices.
2. Add the training-exporter plan and stable sample schema.
3. Add the baseline `train_fusion.py` training-script plan.
4. Add the lightweight inference-wrapper and shadow-mode integration plan.
5. Add model versioning, metadata, and comparison rules.

This order keeps the ML bridge grounded in data quality and deployment simplicity before any framework-heavy expansion.

---

## Future Test Targets

The implementation following this roadmap should prove:

- a stable training-export format can support multiple future models without schema churn
- the baseline fusion training script can train on exported windows and produce a comparable model artifact
- runtime inference can run in shadow mode without replacing heuristics
- heuristic score, model score, and disagreement can be inspected together
- no deep-learning framework is required for the first ML toolchain implementation pass

---

## Assumptions

- this document is a roadmap, not code
- the correct file name is `ML_TOOLCHAIN_ACTIONS.md`
- this pass is limited to the baseline ML toolchain bridge described in the note
- the roadmap should complement `MACHINE_LEARNING_ACTIONS.md` by specifying the concrete exporter, train, and inference bridge, not replace the broader ML transition roadmap

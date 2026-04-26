# Gaming Clip Farming Bot — Methodology

> Measurement, detection, calibration, and evaluation methodology for the current pipeline.
>
> This document is not a command reference and not a speculative idea bank. If a note here conflicts with implementation details, trust the code and [PLANNING.md](/Users/tj/GPT-Codex-Repo/PLANNING.md) first.

Primary inputs:

- [OpenCVExpandedNotes.md](/Users/tj/Downloads/OpenCVExpandedNotes.md)
- [SignalVsNoiseNotes.md](/Users/tj/Downloads/SignalVsNoiseNotes.md)

Supporting inputs:

- [OptimizationNotes.md](/Users/tj/Downloads/OptimizationNotes.md)
- [StatisticalAnalysis.md](/Users/tj/Downloads/StatisticalAnalysis.md)

## Purpose

This note defines how the pipeline should think about signals.

The core job of the system is not “run more detectors.” The core job is:

```text
observe -> measure -> filter -> interpret -> decide
```

That applies to clip detection, context resolution, hook enforcement, wiki enrichment, review feedback, and later analytics.

`PLANNING.md` explains what exists now. `BRAINSTORMING.md` captures future directions. This file explains how to reason about measurement quality, decision quality, and improvement loops.

## Core Principles

### 1. OpenCV is a signal extractor, not the decision brain

OpenCV is strongest when the problem is structured and repeatable:

- HUD icons
- kill feed text
- scoreboards
- weapon or hero portraits
- rank badges
- damage indicators
- victory or defeat screens

OpenCV is weak at answering fuzzy questions such as:

- is this clip exciting
- is this postable
- is this moment viral

The project should keep using OpenCV to detect concrete evidence, timestamps, and context signals. The final worthiness decision belongs to higher-level scoring and judgment stages.

### 2. Event-based scoring is stronger than frame-based scoring

The useful unit of reasoning is an event, not an isolated frame.

Good detector outputs should answer:

- what happened
- when it happened
- how confident we are
- what source observed it
- what supporting evidence exists

A single frame can be noisy. A cluster of aligned events is usually much more reliable.

### 3. ROI quality and preprocessing dominate downstream quality

The strongest sequence from the OpenCV notes is still correct:

1. signal design
2. ROI targeting
3. preprocessing
4. candidate-window selection
5. scoring weights
6. feedback loop

This means the project should not treat weight tuning as the first fix for noisy behavior. Bad ROIs, weak crops, poor OCR cleanup, or badly chosen candidate windows will poison later scoring.

### 4. Signal vs noise is a cross-cutting discipline

A signal is a measurable feature that correlates with something meaningful.

Noise is an observation that looks useful but is unstable, misleading, or incidental.

This applies across the entire project:

- clip detection
- YOLO or OpenCV context resolution
- audio spikes
- hook detection
- wiki scraping
- analytics interpretation

The earlier the system separates signal from noise, the less cost it wastes downstream in compute, review time, and false keeps.

### 5. Human review creates labels

Review actions are not only operational decisions. They are training data.

Approve or reject choices, quarantine recoveries, false positives, false negatives, and ROI-template corrections should all be treated as labels that can later calibrate thresholds, weights, and ranking behavior.

## Signal Extraction

The project should think of feature extraction as a structured observation layer.

Useful signal sources in the current architecture:

- OpenCV template matching
- OCR and OCR preprocessing
- ROI-targeted crops
- difference detection
- audio events
- kill feed parsing
- YOLO detections
- NiceShot candidate moments

Good extraction behavior should produce event-like outputs rather than loose detector blobs.

Preferred signal shape:

```json
{
  "timestamp": 12.4,
  "kind": "weapon_icon_match",
  "source": "weapon_detector",
  "confidence": 0.91,
  "roi": "bottom_left_weapon_icon",
  "evidence": {
    "entity_id": "the_punisher"
  }
}
```

The exact wire format can evolve, but the methodology should stay stable:

- detectors observe narrow facts
- detectors emit timestamps and confidence
- detectors do not decide postability on their own
- multiple weak signals can become one stronger aggregated event

OpenCV-specific guidance:

- prefer ROI-targeted analysis over full-frame scanning
- use template matching and OCR where the HUD is stable
- treat OpenCV as a reliable timestamp finder for structured UI events
- aggregate repeated or adjacent matches into event windows instead of scoring every frame independently

YOLO and NiceShot guidance:

- YOLO should behave like a flexible visual context detector and fallback when exact matching fails
- NiceShot should behave like a candidate-moment and action-intensity source
- neither should become the only authority for keep or reject decisions

## Decision And Calibration

The judge should continue to reason in layers.

Recommended stack:

1. eligibility filter
2. signal score
3. hook score
4. risk or quality penalty
5. final decision

This matches the current direction of `hook_enforcer` plus `clip_judge`, but the method matters more than the exact implementation details.

Key rule:

- separate what was observed from what it means from what decision is taken

That means:

- detectors produce evidence
- aggregation builds candidate events and context
- the judge turns that evidence into accept, reject, or quarantine

### Calibration philosophy

Current YAML weights and thresholds are a transitional control surface. They are useful early because they are debuggable, but they should not stay purely hand-tuned forever.

The next mature loop is:

1. run the pipeline
2. collect human review labels
3. gather enough labeled clips for a game
4. tune weights and thresholds against those labels
5. validate on held-out clips before promoting changes

The project does not need advanced learning-to-rank infrastructure immediately. It needs a cleaner calibration layer first.

## Statistical Tools That Actually Matter

The statistical notes are useful, but only a subset is immediately important.

Highest-value concepts for the current project:

- normalization
- thresholding
- confidence and uncertainty
- outlier handling
- smoothing noisy detections
- held-out validation

Why these matter now:

- normalization keeps heterogeneous detector outputs comparable
- thresholding controls quarantine, accept, and reject behavior
- confidence helps distinguish weak evidence from strong evidence
- outlier handling reduces bad keeps from anomalous spikes
- smoothing reduces detector flicker across adjacent timestamps
- held-out validation stops the project from tuning only to recent examples

Useful future tools, but not immediate priorities:

- regression models
- time-series models
- clustering
- dimensionality reduction
- Bayesian methods

Those techniques become more useful after the project has stronger labels, cleaner signal manifests, and repeatable evaluation sets.

## How This Applies To The Current Project

### Feature Extraction

OpenCV, YOLO, NiceShot, OCR, and audio logic should converge toward structured observation outputs with:

- timestamp
- kind
- source
- confidence
- optional ROI or entity evidence

That makes downstream aggregation and debugging easier than passing around ad hoc detector-specific shapes.

### Decision Engine

`clip_judge` and `hook_enforcer` should consume aggregated events and context bundles, not raw frame impressions.

The important distinction is:

- raw detector output is evidence
- event bundles are interpreted observations
- judge output is a decision

If those layers blur together, the system becomes harder to tune and harder to debug.

### Manual Review And Feedback

Review actions should be treated as labels, not only workflow steps.

Most valuable label types:

- false positive
- false negative
- quarantine recovery
- missing ROI template
- UI drift
- unresolved context

This is the data needed for threshold tuning, weight updates, and later model evaluation.

### Wiki And Scraping

The same signal-vs-noise model applies to enrichment.

Signal:

- stable selectors
- repeated card structures
- consistent icon paths
- canonical names that match multiple places

Noise:

- ads
- malformed HTML
- mismatched aliases
- stale pages
- schema drift

Scraped data should continue to carry confidence and warning metadata instead of being treated as authoritative by default.

### Analytics And Optimization

The current analytics direction should eventually compare predicted clip quality against real post outcomes.

That is the path to improving:

- game-pack weights
- hook strictness
- title patterns
- channel-specific thresholds

The correct order is:

- first build reliable measurement and labels
- then tune from outcomes

Not the reverse.

## Practical Next Steps

1. Standardize detector and event outputs so all major signal sources express timestamp, confidence, source, and evidence more consistently.
2. Treat review and feedback actions as first-class labels and make sure they are easy to summarize by game and failure mode.
3. Build or tighten per-game gold sets so judge changes can be validated before weights move.
4. Introduce simple calibration experiments only after enough labels exist for a game.
5. Use held-out evaluation before changing thresholds or score weights in production.
6. Avoid adding more detectors until current signals are measured and debugged well enough to justify the added complexity.

## Archive / Low-Priority Ideas

These ideas are worth preserving, but they should not drive the next implementation steps:

- full probabilistic fusion models
- advanced time-series inference such as HMM-style state modeling
- clustering pipelines for semantic clip archetypes
- dimensionality-reduction workflows for feature inspection
- highly automated optimization before the project has enough clean labels

The immediate need is not sophistication for its own sake. The immediate need is cleaner signal design, cleaner evidence flow, and repeatable calibration.

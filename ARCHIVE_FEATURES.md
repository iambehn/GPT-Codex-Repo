# Archived Features

This file records support-heavy subsystems removed from the active solo-use repo surface. Recovery should use git history plus the paths listed here.

## Analytics

- Why removed:
  Added maintenance and UI complexity without helping the immediate detector, judge, or debug loop.
- Old entrypoints:
  - Routes: `/analytics`, `/analytics/post/<post_id>`, `/analytics/import`, `/api/analytics/import`
- Core files:
  - `pipeline/review/templates/analytics.html`
  - `pipeline/review/templates/analytics_post.html`
  - `pipeline/review/templates/analytics_table.html`
  - `utils/analytics.py`
- Intended value:
  Track post performance, imports, and template/hook learning after publishing.
- Bring back when:
  Real post-performance analysis becomes a repeated bottleneck and manual review/debug is no longer the primary focus.

## Distribution

- Why removed:
  Queue, compliance, and multi-platform posting logic were beyond the current solo-use detector workflow.
- Old entrypoints:
  - CLI: `--distribute`, `--schedule-distribution`, `--run-distribution-queue`, `--distribution-status`, `--mark-manual-posted`
  - Routes: `/distribution`
- Core files:
  - `pipeline/distribution.py`
  - `pipeline/distribution_queue.py`
  - `pipeline/review/templates/distribution.html`
- Intended value:
  Schedule posts, track attempts, and manage human-assisted or API-driven publishing.
- Bring back when:
  Posting automation becomes active again and manual publishing starts creating repeated operational drag.

## Feedback / Performance Feedback

- Why removed:
  Review-feedback dashboards and weight-update loops added an extra operating layer before the project has enough stable labels to justify it.
- Old entrypoints:
  - CLI: `--review-feedback`, `--apply-feedback`, `--perf-feedback`, `--apply-perf-feedback`
  - Routes: `/feedback`, `/api/feedback/record`, `/api/feedback/apply/<game>`
  - Embedded UI actions previously lived in queue, quarantine, and replay pages
- Core files:
  - `pipeline/review_feedback.py`
  - `pipeline/performance_feedback.py`
  - `pipeline/review/templates/feedback.html`
- Intended value:
  Record reviewer labels, summarize pressure for ROI/model work, and apply bounded weight suggestions.
- Bring back when:
  Label volume is high enough that review data is regularly being used to tune thresholds or retrain detectors.

## Scout

- Why removed:
  Market-scanning and trend polling were separate from the core ingest → review → debug loop.
- Old entrypoints:
  - Routes: `/scout`, `/scout/poll`, `/scout/game/add`, `/scout/game/remove`, `/scout/game/longevity`
- Core files:
  - `pipeline/scout/tracker.py`
  - `pipeline/review/templates/scout.html`
- Intended value:
  Track outside game opportunities and trend signals.
- Bring back when:
  Source selection becomes a real priority again and you are spending time deciding what to ingest next instead of improving clip quality.

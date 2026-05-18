# Quality Maintenance

This document is the preventive maintenance playbook for repo health, contract drift, and data-quality visibility.

Use it for:

- recurring health checks
- issue severity classification
- drift signals
- cleanup and replay triggers

Do not use it for:

- subsystem architecture theory
- threshold catalogs
- one-off experiment notes
- replacing the canonical contracts already defined elsewhere

## Maintenance Classes

Classify issues into these buckets:

### Blocking

Use when operators should stop or repair before trusting downstream work.

Examples:

- schema drift that breaks contract compatibility
- missing required provenance
- persisted artifact state that disagrees with returned status
- broken publish/onboarding/review invariants
- invalid or unsupported canonical manifest shape

### Warning

Use when the system can continue, but trust or maintainability is degrading.

Examples:

- stale fixtures that no longer reflect current contracts
- legacy-assisted compatibility paths still in use
- sparse or imbalanced reviewed examples in areas that affect calibration quality
- unresolved review backlog that is beginning to distort decision confidence
- missing compact summaries for state that operators need repeatedly

### Informational

Use when the system is healthy enough to continue and the signal is mainly for planning.

Examples:

- optional cleanup opportunities
- fixture refresh candidates not yet stale enough to block
- draft artifacts that are still clearly labeled and isolated
- instrumentation gaps that reduce convenience but not correctness

## Recurring Checks

These checks should exist as dedicated tooling, audits, or compact reports rather than tribal knowledge:

- schema drift checks
- contract audit runs
- fixture freshness checks
- provenance and completeness summaries
- unresolved review and backlog summaries
- supported vs unsupported schema detection
- sparse-label or imbalance warnings where decision quality depends on labeled data

Current repo-level operator checks:

- `python run.py --inspect-quality-maintenance-findings`
  - live maintenance findings only
- `python run.py --run-decision-regression-goldsets`
  - reviewed decision-drift suites only
- `python run.py --run-repo-quality-health`
  - combined closeout gate over both maintenance findings and decision-regression suites

CI enforcement:

- `.github/workflows/repo-quality-health.yml`
  - runs `python run.py --run-repo-quality-health` on `push`, `pull_request`, and manual dispatch
  - serves as the non-optional enforcement surface for the local repo-quality gate

The preferred pattern is:

1. small dedicated check
2. compact status output
3. explicit severity
4. durable artifact or test when the signal matters over time

## Drift Signals

Drift is not limited to schemas. Treat these as relevant signals:

- docs describing behavior that code or tests no longer enforce
- manifests or ledgers gaining optional fields that slowly become required in practice
- replay or calibration outputs changing without a matching fixture or contract update
- duplicate notes or helper files competing with canonical surfaces
- operator workflows depending on remembered cleanup steps instead of visible audits
- data-quality warnings accumulating without promotion into maintenance work

## Triggered Actions

Use these triggers to decide what should happen next.

### Trigger Cleanup

Trigger cleanup when:

- duplicate draft state is accumulating
- stale temporary artifacts obscure published vs draft truth
- compact summaries or inspectors no longer reflect current artifact layout
- compatibility shims are kept longer than their migration plan

### Trigger Replay Or Regression Verification

Trigger replay or deeper regression checks when:

- a heuristic changes decision outcomes
- calibration policy changes
- contract-sensitive fixture expectations move
- review-state interpretation changes
- a drift warning suggests behavior is diverging from prior reviewed examples

### Trigger Fixture Refresh

Trigger fixture refresh when:

- a canonical contract changed intentionally
- reviewed examples no longer exercise the important decision boundary
- fixture provenance is incomplete or unclear
- label distribution has become too sparse or skewed for the original fixture set to remain representative

### Trigger Escalation

Trigger escalation when:

- the correct source of truth is unclear
- a change would silently alter thresholds, workflow state, or review semantics
- maintenance signals suggest hidden state has become more important than the visible contracts
- the only support for a new rule is prose or chat history

## Long-Run Data-Quality Preservation

Preserve data quality by keeping these habits explicit:

- require provenance on durable analysis artifacts
- prefer reviewed fixtures over anecdotal examples
- surface imbalance and sparsity instead of assuming the sample is good enough
- keep calibration and replay outputs inspectable
- retire stale compatibility paths instead of normalizing around them forever
- make draft-vs-published state obvious in manifests, folders, and reports

The objective is not zero warnings. The objective is that drift becomes visible early enough to repair before trust collapses.

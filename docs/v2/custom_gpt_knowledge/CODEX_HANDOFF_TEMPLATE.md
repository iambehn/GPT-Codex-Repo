# Codex Handoff Template

Use this template when `Pipeline Architecture and Troubleshooting` hands work to Codex or another coding agent.

This file is the upload-ready template for the routed artifact type `codex_handoff_brief`.

The handoff should be implementation-ready, scoped, and repo-compatible.

## Template

```text
title:
  Short task name.

objective:
  One paragraph describing the concrete outcome to implement.

repo_context:
  The governing repo surfaces, contracts, or files that define the current behavior.

inputs_and_outputs:
  The main inputs, outputs, and artifact paths involved in the change.

required_changes:
  A short grouped list of the implementation changes by behavior or subsystem.

constraints_and_invariants:
  The rules that must not be violated.

verification:
  The narrowest relevant commands, tests, or artifact inspections required before the task is considered done.

out_of_scope:
  Explicitly list what should not be changed in this task.
```

## Rules

- prefer behavior-level grouping over long file inventories
- reference repo contracts before proposing new ones
- keep the scope small enough for one implementation pass
- state assumptions explicitly
- do not bury critical invariants inside long prose
- include verification that matches the actual change surface

## Example Skeleton

```text
title:
  Add manual research-agent envelope importer

objective:
  Implement a standalone importer that normalizes saved manual ChatGPT agent envelopes into the existing research-runtime artifact and trace contracts so the evaluator can consume them directly.

repo_context:
  - RESEARCH_PROTOCOL.md
  - docs/v2/RESEARCH_AGENT_CHATGPT_OPERATOR_WORKFLOW.md
  - pipeline/research_runtime/artifact.py
  - pipeline/research_runtime/trace.py
  - pipeline/research_runtime/evaluator.py

inputs_and_outputs:
  - input directory of saved JSON envelopes
  - one output artifact JSON
  - one output synthetic trace JSON

required_changes:
  - add a standalone import tool
  - validate envelopes against the assigned section schemas
  - synthesize evaluator-compatible trace rows
  - add focused regression tests for missing, invalid, and successful imports

constraints_and_invariants:
  - no partial output on invalid input
  - no auto-repair of malformed envelopes
  - preserve source_refs as provided
  - artifact and trace must remain evaluator-compatible

verification:
  - targeted unittest coverage for importer behavior
  - one direct artifact inspection of imported output shape

out_of_scope:
  - provider-backed runtime execution
  - generalized transcript parsing
  - broad runtime schema changes
```

# Custom GPT Launch Sequence

This document is the one-sitting operator runbook for launching the repo's two Custom GPTs.

Use it when you want to:

- build both GPTs in one session
- test them in the correct order
- produce the first real handoff from GPT 1 into GPT 2

This file assumes the builder details already exist in:

- [BUILDER_CHECKLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/BUILDER_CHECKLIST.md)

## Goal

End the session with:

1. `Pipeline Research and Development` created and passing its smoke test
2. `Pipeline Architecture and Troubleshooting` created and passing its smoke test
3. one real reviewed draft note from GPT 1
4. one real Codex-ready handoff from GPT 2 based on that reviewed note

## Session Order

Do not build both GPTs at once.

Use this order:

1. build GPT 1
2. smoke test GPT 1
3. run one real note-normalization task through GPT 1
4. review and tighten GPT 1 if needed
5. build GPT 2
6. smoke test GPT 2
7. feed the reviewed GPT 1 note into GPT 2
8. review the first Codex-ready handoff

This order matters because GPT 2 should be validated against a real reviewed draft note, not only against synthetic test text.

## Phase 1: Build GPT 1

Follow:

- [BUILDER_CHECKLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/BUILDER_CHECKLIST.md)

Use only the GPT 1 section first.

Required outcome:

- the GPT exists with the correct name
- the description is set
- the instructions are pasted from the correct file
- the upload set is complete
- the capabilities are enabled as specified

## Phase 2: Smoke Test GPT 1

Run the GPT 1 smoke test exactly as written in:

- [BUILDER_CHECKLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/BUILDER_CHECKLIST.md)

Pass criteria:

- it returns the required draft-note sections
- it separates findings from implications
- it stays draft-first

If it fails:

1. tighten the GPT instructions
2. rerun the smoke test
3. do not move on to GPT 2 yet

## Phase 3: First Real GPT 1 Task

Take one real messy note or browser finding set and ask GPT 1 to normalize it using the draft-note template.

Recommended default prompt when the current blocker is `call_of_duty` text or reward-banner visibility:

```text
Fill out the call_of_duty text banner packet stub using the text banner packet brief and the supporting repo evidence.

Return a complete research packet with:
- decision target
- current repo truth
- evidence bundle
- structured findings
- recommendation
- acceptance target
- open uncertainties

Focus only on true visible in-clip text or reward-banner signals.
Include exact timestamps, crops, extraction-method candidates, false positives, and whether each signal is native UI or overlay.
```

Fallback generic prompt:

```text
Turn these notes into one structured draft note using the required sections:
- topic
- source_set
- key_findings
- project_relevance
- recommended_implications
- uncertainties
- follow_up_questions

Keep findings separate from inferred implications.
Do not treat this note as canonical project truth.
```

Review the output manually.

Checklist:

- are the sources specific enough?
- are the findings concrete?
- are implications still clearly tentative?
- did it invent repo-local truth?
- are uncertainties explicit?
- if this is the medal task, did it actually fill the packet stub instead of returning a generic summary?

If the note fails that review, tighten GPT 1 before continuing.

## Phase 4: Build GPT 2

Only build GPT 2 after GPT 1 can produce one acceptable reviewed draft note.

Follow:

- the GPT 2 section in [BUILDER_CHECKLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/BUILDER_CHECKLIST.md)

Use the reviewed GPT 1 note as part of GPT 2’s first upload set if practical.

Required outcome:

- the GPT exists with the correct name
- the description is set
- the instructions are pasted from the correct file
- the upload set is complete
- the capabilities are enabled as specified

## Phase 5: Smoke Test GPT 2

Run the GPT 2 smoke test exactly as written in:

- [BUILDER_CHECKLIST.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/BUILDER_CHECKLIST.md)

Pass criteria:

- it produces an implementation-oriented response
- it references repo-context verification
- it does not merely restate the draft note

If it fails:

1. tighten GPT 2’s instructions
2. rerun the smoke test
3. do not start normal usage yet

## Phase 6: First Real Two-GPT Handoff

Run this actual sequence:

1. use GPT 1 on one real research problem
2. review the resulting draft note
3. pass that reviewed note to GPT 2
4. ask GPT 2 for one Codex-ready handoff
5. compare that handoff against:
   - [CODEX_HANDOFF_TEMPLATE.md](/Users/tj/Documents/Codex/2026-04-21-https-github-com-iambehn-claude-repo/docs/v2/custom_gpt_knowledge/CODEX_HANDOFF_TEMPLATE.md)

Recommended GPT 2 prompt:

```text
Use this reviewed draft note and produce one Codex-ready handoff.

Follow the handoff shape:
- title
- objective
- repo_context
- inputs_and_outputs
- required_changes
- constraints_and_invariants
- verification
- out_of_scope

Treat the note as useful input, not canonical truth. Verify implications against repo-local contracts.
```

## Launch Success Criteria

The launch is successful when:

- GPT 1 produces a usable draft note from real messy input
- GPT 2 turns that note into a usable Codex-ready handoff
- neither GPT drifts into acting like the full pipeline backend
- the boundary between external research truth and repo-local truth remains explicit

## If The Launch Goes Wrong

Common failure cases:

### GPT 1 failure modes

- vague source sets
- generic summaries
- hype language
- overconfident architecture claims
- missing uncertainties

Fix by tightening GPT 1 instructions and reducing noisy uploads.

### GPT 2 failure modes

- generic web-style answers
- restating notes without implementation shape
- inventing repo contracts
- weak verification guidance

Fix by tightening GPT 2 instructions and reducing broad non-canonical uploads.

## After Launch

Once both GPTs are stable:

1. keep using GPT 1 for draft-first external research
2. keep using GPT 2 for repo-aware planning and debugging
3. promote stable truths into canonical repo docs instead of leaving them stranded in GPT chats

Do not expand to custom actions or broader automation until the two-GPT split is producing repeatable high-signal outputs.

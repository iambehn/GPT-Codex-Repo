# Conversation Archive Policy

Status: active
Last updated: 2026-06-05

## Purpose

Define how completed or paused agent conversations are preserved as historical records.

This archive is for:

- historical preservation
- large topic-based Google Docs
- local batching and ledger tracking
- topic integrity over perfect page precision

This archive is not:

- a live execution dashboard
- a work queue
- a replacement for the operator pack

Current app constraint:

- only one active thread heartbeat can attach to a thread at a time
- keep archive maintenance separate from the live Codex heartbeat rather than collapsing both concerns into one recurring worker

## Archive Shape

The archive uses:

- one primary topic per conversation
- optional secondary tags
- local archive records as the capture unit
- topic batches as the Google Docs publication unit
- one local ledger as the source of truth for archive state

## Batch Rules

Primary batching primitive:

- word count

Page count role:

- verification only

Current defaults:

- words per page estimate: `275`
- soft open threshold: `103125`
- soft close threshold: `116875`
- hard close threshold: `123750`

These thresholds are chosen to land most archive docs near the desired `375-425` page range without using page count as the live batching driver.

## Integrity Rules

- Never split a single conversation across archive docs unless it exceeds the hard limit by itself.
- If one conversation exceeds the hard limit by itself, store it as a single-conversation exception batch.
- Preserve chronological ordering within each topic batch.
- Archive metadata does not belong in the operator dashboard.

## Publication Target

Primary publication destination:

- Google Docs

Recommended Drive structure:

- `Codex Conversation Archive/`
- topic folders below that
- period subfolders only if scale requires them

## Source of Truth

Historical archive state lives in the local archive ledger first.

Drive is the human-readable destination, not the authoritative batching ledger.

# Conversation Archive Automation Prompt

Use this prompt when creating the recurring conversation archive automation.

## Prompt

Return to this conversation and preserve it as historical archive material without changing live execution state.

Follow this workflow:

1. inspect the local conversation archive ledger at `outputs/conversation_archives/conversation_archive_ledger.json` if it exists
2. capture one new conversation archive record for the current thread using the existing repo tooling
3. assign exactly one primary topic and any helpful secondary tags
4. append the record into the correct topic batch
5. if a batch becomes `closed_pending_upload`, prepare it for Google Docs upload using `python3 run.py --prepare-conversation-archive-upload --ledger-path <ledger> --batch-id <batch_id>`
6. materialize an importable text source using `python3 run.py --materialize-conversation-archive-doc-source --upload-manifest <upload_manifest>`
7. if Google Docs upload is completed, update the local ledger with:
   - `drive_doc_id`
   - `drive_url`
   - `measured_pages` when available

Hard rules:

- do not write archive metadata into the operator dashboard
- do not change backlog or execution-state surfaces as part of archive work
- never split a normal conversation across multiple docs
- only create a single-conversation exception batch when that conversation alone exceeds the hard limit
- preserve topic integrity over exact page-count precision

If Drive upload cannot be completed:

- leave the batch in a recoverable local pending state
- do not lose local archive ownership
- do not duplicate the batch

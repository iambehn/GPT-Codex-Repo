# Conversation Archive Ledger Contract

Status: active
Last updated: 2026-06-05

## Ledger

Local canonical ledger path:

- `outputs/conversation_archives/conversation_archive_ledger.json`

Schema version:

- `conversation_archive_ledger_v1`

## Archive Record Contract

Each captured conversation record must contain:

- `archive_record_id`
- `source_thread_id`
- `agent_name`
- `started_at`
- `ended_at`
- `primary_topic`
- `secondary_topics`
- `summary`
- `body_markdown`
- `word_count`
- `repo_refs`
- `archivable_status`

Current record schema version:

- `conversation_archive_record_v1`

## Ledger Row Contract

Each topic batch row stores:

- `batch_id`
- `topic`
- `status`
- `conversation_ids`
- `record_paths`
- `word_count`
- `estimated_pages`
- `measured_pages`
- `drive_doc_id`
- `drive_url`
- `opened_at`
- `closed_at`
- `uploaded_at`
- `superseded_by`
- `local_batch_markdown_path`
- `date_range_start`
- `date_range_end`
- `batch_summary`
- `is_single_conversation_exception`

Allowed statuses:

- `open`
- `closed_pending_upload`
- `uploaded`
- `superseded`

## Rebuild Rule

If a batch is rebuilt or replaced:

- mark the older row as `superseded`
- set `superseded_by`
- do not duplicate conversation ownership across active rows

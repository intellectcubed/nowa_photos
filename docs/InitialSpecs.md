- preserved alternate/conflicting date metadata when discovered

---
## Duplicate Handling (Final Decision)

If two or more files share the same hash/signature:

- Only the first copy is stored in the archive
- All later duplicates are skipped (no duplicate files saved)
- However, the script should still capture metadata/tags from the duplicate’s source path

This allows duplicates to contribute contextual value without bloating storage.

---

## Metadata & Tagging

### Tag Extraction from Folder Paths

The ingestion script should infer event/context tags from the source folder structure.

Examples:

- `backup/anikasbirthday/` → tag: `anikasbirthday`
- `photos/birthdays/anika/` → tags: `birthdays`, `anika`
- `bday-anika/` → tag: `bday-anika`

These tags should be applied whether the media is new or a duplicate.

---

## Human-in-the-Loop Tagging (Final Decision)

Tagging should support:

- Automatic tagging from folder names
- Optional **batch prompt once per folder**

Example:

```
Folder "backup/anikasbirthday/" contains 300 files.  
Suggested tags: ["anika", "birthday"]

Apply these tags to all files from this folder?  
(y = yes, n = no, edit = modify tags)
```


This allows the user to resolve ambiguous folder naming without prompting per file.

---

## Database Requirements (Final Decision)

Maintain a SQLite database containing:

### Media Table

- id
- archive_path
- media_type (photo/video)
- hash_signature (unique)
- file_size
- duration (if video)
- ingestion_timestamp

### Source Table (optional but recommended)

Tracks all original source locations that pointed to this media:

- media_id
- source_path

### Tags Table

- media_id
- tag_value

Database must remain updated after every ingestion run.

---

## Long-Term Longevity Requirement (Final Decision)

Because the archive must remain usable for descendants even if scripts or SQLite are lost:

### Metadata Export File

In addition to SQLite, the system must maintain a durable text-based metadata file:

- `metadata.jsonl` (JSON Lines format)

Each media file should have one JSON record containing:

- archive path
- hash signature
- tags
- all known source paths
- ingestion date

Example:

```json
{
  "archive_path": "2026/01/IMG_0001.jpg",
  "hash": "...",
  "tags": ["anika", "birthday"],
  "sources": [
    "dcim/photos/IMG_0001.jpg",
    "backup/anikasbirthday/IMG_0001.jpg"
  ],
  "ingested_at": "2026-01-15T12:00:00"
}

```

This ensures the archive remains interpretable forever.

---

## Logging & Audit Trail

Each ingestion session should produce:
- Imported files list
- Duplicate files skipped list
- Tags added/updated
- Errors
- Summary counts

Example: 
```
Session Summary:
- Imported: 120
- Duplicates skipped: 45
- Tags added: 300
- Errors: 2

```

Logs should be written to:
```
logs/session_YYYYMMDD_HHMMSS.txt
```

## Scripts Expected

At minimum:
1. `ingest.py`
    - main ingestion + dedupe + tagging + metadata persistence

Optional future scripts:
2. `search.py`
    - query by tag/date
3. `export.py`
    - regenerate JSONL metadata file from DB

---

## Configuration Options

The system should support config via CLI args or config file:
- ingestion folder path
- archive root path
- SQLite DB path
- JSONL metadata file path
- move vs copy behavior
- enable/disable folder-level tag prompts

---

## Deliverables

Claude should produce:
- Working Python ingestion script(s)
- SQLite schema initialization
- JSONL metadata export maintenance
- Logging + audit trail
- README with clear usage instructions

---

## Final Notes

This system prioritizes:

- Deduplication correctness
- Long-term archival simplicity
- Metadata richness through tags
- Future usability even without the database

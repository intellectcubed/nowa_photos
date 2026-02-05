# Nowa Photos — Archival Ingestion System

## Archive Folder Structure

Media files are archived into a `YYYY/MM/` directory structure based on the file's date.

### Date Priority

1. **EXIF `DateTimeOriginal`** (highest priority)
2. **File modification date** (fallback)

Alternate/conflicting date metadata (e.g., EXIF date differs from file date) should be preserved in the database and JSONL export for reference.

---

## Duplicate Handling (Final Decision)

If two or more files share the same SHA-256 hash:

- Only the first copy is stored in the archive
- All later duplicates are skipped (no duplicate files saved)
- However, the script should still capture metadata/tags from the duplicate's source path

This allows duplicates to contribute contextual value without bloating storage.

### Hash Algorithm

**SHA-256** is used for all file hashing and deduplication.

---

## Filename Collision Resolution

When two *different* files would be placed at the same archive path (same `YYYY/MM/` folder and same filename), the collision is resolved by appending a hash suffix:

- Format: `BASENAME_HASH8.EXT` (first 8 characters of the SHA-256 hash)
- Example: `IMG_0001.jpg` → `IMG_0001_a3f8b2c1.jpg`

Since two files with the same hash are deduplicated (only first stored), files reaching this point are guaranteed to have different hashes, making the suffix unique.

---

## Supported File Types

### Photos
`jpg`, `jpeg`, `png`, `heic`, `tiff`, `bmp`, `gif`, `webp`, `nef`, `nrw`

### Videos
`mp4`, `mov`, `avi`, `mkv`, `wmv`, `m4v`

> **Note:** `nef` and `nrw` are Nikon RAW formats.

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
- hash_signature (SHA-256, unique)
- file_size
- duration (if video)
- exif_date (if available)
- file_date (file modification date)
- ingestion_timestamp

### Source Table

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

The **full JSONL file is regenerated from the database** at the end of each ingestion session.

Each media file should have one JSON record containing:

- archive path
- hash signature
- tags
- all known source paths
- exif date (if available)
- file date
- ingestion date

Example:

```json
{
  "archive_path": "2026/01/IMG_0001.jpg",
  "hash": "a3f8b2c1...",
  "tags": ["anika", "birthday"],
  "sources": [
    "dcim/photos/IMG_0001.jpg",
    "backup/anikasbirthday/IMG_0001.jpg"
  ],
  "exif_date": "2026-01-15T14:30:00",
  "file_date": "2026-01-15T14:32:00",
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

---

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

## Configuration

Configuration is managed via a YAML config file (`config/config.yaml`).

### Config Fields

```yaml
# Paths
ingestion_path: "/path/to/source/folder"
archive_path: "/path/to/archive"
db_path: "data/nowa_photos.db"
metadata_path: "data/metadata.jsonl"

# Behavior
mode: "copy"            # "copy" or "move"
enable_tag_prompts: true # batch folder-level tag prompts

# Logging
log_dir: "logs"
```

---

## Project Structure

```
nowa_photos/
├── config/
│   └── config.yaml          # user configuration
├── docs/
│   └── InitialSpecs.md      # this file
├── logs/                    # session logs (auto-created)
├── nowa_photos/
│   ├── __init__.py
│   ├── ingest.py            # main ingestion script
│   ├── config.py            # config loading
│   ├── database.py          # SQLite schema + operations
│   ├── hasher.py            # SHA-256 hashing
│   ├── metadata.py          # JSONL export
│   └── tagger.py            # tag extraction + prompting
├── tests/
│   └── __init__.py
├── LICENSE
├── README.md
└── pyproject.toml
```

---

## Deliverables

- Working Python ingestion script(s)
- SQLite schema initialization
- JSONL metadata export (regenerated each session)
- Logging + audit trail
- YAML config file with defaults
- README with clear usage instructions

---

## Final Notes

This system prioritizes:

- Deduplication correctness
- Long-term archival simplicity
- Metadata richness through tags
- Future usability even without the database

# Database Schema Restructuring

**Status: IMPLEMENTED** (2026-02-07)

## Schema

```sql
CREATE TABLE media (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_path         TEXT    NOT NULL,
    archive_filename     TEXT    NOT NULL,
    media_type           TEXT    NOT NULL,
    hash_signature       TEXT    NOT NULL UNIQUE,
    file_size            INTEGER NOT NULL,
    duration             REAL,
    exif_date            TEXT,
    file_date            TEXT    NOT NULL,
    ingestion_timestamp  TEXT    NOT NULL
);
CREATE INDEX idx_media_hash ON media(hash_signature);

CREATE TABLE tag (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    value     TEXT NOT NULL UNIQUE
);

CREATE TABLE media_tag (
    media_id  INTEGER NOT NULL,
    tag_id    INTEGER NOT NULL,
    PRIMARY KEY (media_id, tag_id),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id)   REFERENCES tag(id)   ON DELETE CASCADE
);

CREATE INDEX idx_media_tag_media ON media_tag(media_id);
CREATE INDEX idx_media_tag_tag   ON media_tag(tag_id);

CREATE TABLE source_item (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path TEXT NOT NULL UNIQUE
);

CREATE TABLE media_source (
    media_id        INTEGER NOT NULL,
    source_item_id  INTEGER NOT NULL,
    source_filename TEXT NOT NULL,
    PRIMARY KEY (media_id, source_item_id, source_filename),
    FOREIGN KEY (media_id)       REFERENCES media(id)        ON DELETE CASCADE,
    FOREIGN KEY (source_item_id) REFERENCES source_item(id)  ON DELETE CASCADE
);

CREATE INDEX idx_media_source_media  ON media_source(media_id);
CREATE INDEX idx_media_source_source ON media_source(source_item_id);
```

### Implementation Note

The `media_source` PRIMARY KEY was changed from `(media_id, source_item_id)` to `(media_id, source_item_id, source_filename)` to allow multiple source filenames from the same directory for the same media. This handles the case where duplicate files with different names exist in the same source folder (e.g., `photo.jpg` and `photo_copy.jpg` both being duplicates of the same archived file).

## Code Changes (Implemented)

- `nowa_photos/database.py` - Updated schema and API methods:
  - `insert_media()` now takes `archive_path` and `archive_filename` separately
  - `add_source()` now takes `(media_id, source_path, source_filename)`
  - Added `_get_or_create_tag()` and `_get_or_create_source_item()` helpers
  - Added `get_tags_for_media()`, `replace_tags()`, `get_media_ids_by_source_folder()`

- `nowa_photos/ingest.py` - Updated to:
  - Split archive path and filename before inserting
  - Split source path and filename before adding sources
  - Accumulate tags for duplicates (new tags from folder paths added to existing media)

- `nowa_photos/metadata.py` - Updated to combine `archive_path` and `archive_filename` for JSONL export

## Migration Script (Implemented)

`nowa_photos/migrate_db.py` - Converts old schema to new schema:

```
python -m nowa_photos.migrate_db <source_db> <output_db>
```

The script:
- Reads from old schema (media, source, tags tables)
- Writes to new normalized schema
- Splits paths into directory and filename for both archive and source
- Creates unique tag and source_item entries
- Links via junction tables (media_tag, media_source)
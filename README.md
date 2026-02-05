# Nowa Photos

Family photo and video archival ingestion platform.

Deduplicates, organizes, and tags media files into a date-based archive with full metadata tracking.

## Features

- Archives photos and videos into a `YYYY/MM/` directory structure
- SHA-256 deduplication (identical files stored once, all source paths recorded)
- EXIF date extraction (Pillow, exifread for Nikon RAW, pillow-heif for HEIC)
- Automatic tag extraction from folder names
- Optional interactive batch tagging per folder
- SQLite database for metadata
- JSONL metadata export for long-term durability
- Session logging with summary statistics
- Video duration extraction via ffprobe

## Supported Formats

**Photos:** jpg, jpeg, png, heic, tiff, bmp, gif, webp, nef, nrw

**Videos:** mp4, mov, avi, mkv, wmv, m4v

## Installation

```bash
pip install -e ".[dev]"
```

## Configuration

Copy and edit the config file:

```bash
cp config/config.yaml my_config.yaml
```

Edit `my_config.yaml`:

```yaml
ingestion_path: "/path/to/source/folder"
archive_path: "/path/to/archive"
db_path: "data/nowa_photos.db"          # relative to archive_path
metadata_path: "data/metadata.jsonl"    # relative to archive_path
mode: "copy"                            # "copy" or "move"
enable_tag_prompts: true                # interactive folder tagging
log_dir: "logs"                         # relative to archive_path

tag_stop_words:
  - backup
  - photos
  - images
  - media
  - camera
  - dcim
```

## Usage

```bash
nowa-photos-ingest --config my_config.yaml
```

The ingestion process:

1. Scans the source folder for supported media files
2. Hashes each file (SHA-256) and checks for duplicates
3. Extracts EXIF dates for the archive directory structure
4. Copies (or moves) new files to `archive_path/YYYY/MM/`
5. Records metadata in SQLite and adds source paths
6. Extracts tags from folder names (optionally prompts for confirmation)
7. Regenerates `metadata.jsonl` from the database
8. Writes a session log with summary statistics

Re-running on the same source is safe: duplicates are detected by hash, and their source paths are added to existing records.

## Testing

```bash
pytest tests/ -v
```

## Project Structure

```
nowa_photos/
├── config/
│   └── config.yaml          # default configuration
├── docs/
│   └── InitialSpecs.md      # specifications
├── logs/                    # session logs (auto-created)
├── nowa_photos/
│   ├── __init__.py
│   ├── config.py            # config loading & validation
│   ├── database.py          # SQLite schema & operations
│   ├── hasher.py            # SHA-256 file hashing
│   ├── ingest.py            # main ingestion pipeline
│   ├── metadata.py          # JSONL export
│   └── tagger.py            # tag extraction & prompting
├── tests/
│   ├── test_config.py
│   ├── test_database.py
│   ├── test_hasher.py
│   ├── test_ingest.py
│   └── test_tagger.py
├── LICENSE
├── README.md
└── pyproject.toml
```

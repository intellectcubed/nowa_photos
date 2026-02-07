"""JSONL metadata export for long-term archival durability."""

import json
from pathlib import Path

from nowa_photos.database import Database


def export_metadata_jsonl(db: Database, path: Path | str) -> int:
    """Regenerate the full metadata JSONL file from the database.

    Each line is a JSON object for one media record containing:
    archive_path, hash, tags, sources, exif_date, file_date, ingested_at.

    Returns the number of records written.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    records = db.get_all_media_with_details()

    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            # Combine archive_path and archive_filename for the full path
            full_archive_path = f"{record['archive_path']}/{record['archive_filename']}"
            line = {
                "archive_path": full_archive_path,
                "hash": record["hash_signature"],
                "tags": record["tags"],
                "sources": record["sources"],
                "exif_date": record["exif_date"],
                "file_date": record["file_date"],
                "ingested_at": record["ingestion_timestamp"],
            }
            if record.get("media_type") == "video" and record.get("duration") is not None:
                line["duration"] = record["duration"]
            f.write(json.dumps(line, ensure_ascii=False) + "\n")

    return len(records)

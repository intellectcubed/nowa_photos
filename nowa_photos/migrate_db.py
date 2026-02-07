#!/usr/bin/env python3
"""Migrate old database schema to new normalized schema.

Usage:
    python -m nowa_photos.migrate_db <source_db> <output_db>

This script reads from the old schema (media, source, tags tables) and
writes to a new database with the normalized schema (media, tag, media_tag,
source_item, media_source tables).
"""

import argparse
import sqlite3
import sys
from pathlib import Path


OLD_SCHEMA_CHECK = """
SELECT name FROM sqlite_master
WHERE type='table' AND name IN ('media', 'source', 'tags')
ORDER BY name
"""

NEW_SCHEMA = """
CREATE TABLE IF NOT EXISTS media (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_path        TEXT    NOT NULL,
    archive_filename    TEXT    NOT NULL,
    media_type          TEXT    NOT NULL,
    hash_signature      TEXT    NOT NULL UNIQUE,
    file_size           INTEGER NOT NULL,
    duration            REAL,
    exif_date           TEXT,
    file_date           TEXT    NOT NULL,
    ingestion_timestamp TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_media_hash ON media(hash_signature);

CREATE TABLE IF NOT EXISTS tag (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    value TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS media_tag (
    media_id INTEGER NOT NULL,
    tag_id   INTEGER NOT NULL,
    PRIMARY KEY (media_id, tag_id),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id)   REFERENCES tag(id)   ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_media_tag_media ON media_tag(media_id);
CREATE INDEX IF NOT EXISTS idx_media_tag_tag   ON media_tag(tag_id);

CREATE TABLE IF NOT EXISTS source_item (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS media_source (
    media_id        INTEGER NOT NULL,
    source_item_id  INTEGER NOT NULL,
    source_filename TEXT    NOT NULL,
    PRIMARY KEY (media_id, source_item_id, source_filename),
    FOREIGN KEY (media_id)       REFERENCES media(id)       ON DELETE CASCADE,
    FOREIGN KEY (source_item_id) REFERENCES source_item(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_media_source_media  ON media_source(media_id);
CREATE INDEX IF NOT EXISTS idx_media_source_source ON media_source(source_item_id);
"""


def split_path(full_path: str) -> tuple[str, str]:
    """Split a full path into directory and filename."""
    p = Path(full_path)
    return str(p.parent), p.name


def migrate(source_db: Path, output_db: Path) -> None:
    """Migrate from old schema to new schema."""
    if not source_db.exists():
        print(f"Error: source database not found: {source_db}", file=sys.stderr)
        sys.exit(1)

    if output_db.exists():
        print(f"Error: output database already exists: {output_db}", file=sys.stderr)
        print("Please remove it first or choose a different output path.")
        sys.exit(1)

    # Open source database
    src_conn = sqlite3.connect(str(source_db))
    src_conn.row_factory = sqlite3.Row

    # Verify old schema
    tables = [row[0] for row in src_conn.execute(OLD_SCHEMA_CHECK).fetchall()]
    if set(tables) != {"media", "source", "tags"}:
        print(f"Error: source database does not have expected tables.", file=sys.stderr)
        print(f"Expected: media, source, tags. Found: {tables}")
        sys.exit(1)

    # Create output database with new schema
    output_db.parent.mkdir(parents=True, exist_ok=True)
    dst_conn = sqlite3.connect(str(output_db))
    dst_conn.execute("PRAGMA journal_mode=WAL")
    dst_conn.execute("PRAGMA foreign_keys=ON")
    dst_conn.executescript(NEW_SCHEMA)
    dst_conn.commit()

    # Mapping from old media.id to new media.id
    old_to_new_media: dict[int, int] = {}
    # Cache for tag value -> tag id
    tag_cache: dict[str, int] = {}
    # Cache for source_path -> source_item id
    source_cache: dict[str, int] = {}

    print("Migrating media records...")
    old_media = src_conn.execute("SELECT * FROM media ORDER BY id").fetchall()
    for row in old_media:
        old_id = row["id"]
        old_archive_path = row["archive_path"]

        # Split archive_path into directory and filename
        archive_dir, archive_filename = split_path(old_archive_path)

        cursor = dst_conn.execute(
            """INSERT INTO media
               (archive_path, archive_filename, media_type, hash_signature,
                file_size, duration, exif_date, file_date, ingestion_timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (archive_dir, archive_filename, row["media_type"], row["hash_signature"],
             row["file_size"], row["duration"], row["exif_date"],
             row["file_date"], row["ingestion_timestamp"]),
        )
        new_id = cursor.lastrowid
        old_to_new_media[old_id] = new_id

    print(f"  Migrated {len(old_media)} media records.")

    print("Migrating source records...")
    old_sources = src_conn.execute("SELECT * FROM source ORDER BY id").fetchall()
    source_count = 0
    for row in old_sources:
        old_media_id = row["media_id"]
        new_media_id = old_to_new_media.get(old_media_id)
        if new_media_id is None:
            print(f"  Warning: source references unknown media_id {old_media_id}, skipping")
            continue

        full_source_path = row["source_path"]
        source_dir, source_filename = split_path(full_source_path)

        # Get or create source_item
        if source_dir not in source_cache:
            cursor = dst_conn.execute(
                "INSERT OR IGNORE INTO source_item (source_path) VALUES (?)",
                (source_dir,),
            )
            if cursor.lastrowid:
                source_cache[source_dir] = cursor.lastrowid
            else:
                # Already existed
                result = dst_conn.execute(
                    "SELECT id FROM source_item WHERE source_path = ?",
                    (source_dir,),
                ).fetchone()
                source_cache[source_dir] = result[0]

        source_item_id = source_cache[source_dir]

        dst_conn.execute(
            """INSERT OR IGNORE INTO media_source
               (media_id, source_item_id, source_filename)
               VALUES (?, ?, ?)""",
            (new_media_id, source_item_id, source_filename),
        )
        source_count += 1

    print(f"  Migrated {source_count} source records.")
    print(f"  Created {len(source_cache)} unique source_item entries.")

    print("Migrating tag records...")
    old_tags = src_conn.execute("SELECT * FROM tags ORDER BY id").fetchall()
    tag_link_count = 0
    for row in old_tags:
        old_media_id = row["media_id"]
        new_media_id = old_to_new_media.get(old_media_id)
        if new_media_id is None:
            print(f"  Warning: tag references unknown media_id {old_media_id}, skipping")
            continue

        tag_value = row["tag_value"]

        # Get or create tag
        if tag_value not in tag_cache:
            cursor = dst_conn.execute(
                "INSERT OR IGNORE INTO tag (value) VALUES (?)",
                (tag_value,),
            )
            if cursor.lastrowid:
                tag_cache[tag_value] = cursor.lastrowid
            else:
                # Already existed
                result = dst_conn.execute(
                    "SELECT id FROM tag WHERE value = ?",
                    (tag_value,),
                ).fetchone()
                tag_cache[tag_value] = result[0]

        tag_id = tag_cache[tag_value]

        dst_conn.execute(
            "INSERT OR IGNORE INTO media_tag (media_id, tag_id) VALUES (?, ?)",
            (new_media_id, tag_id),
        )
        tag_link_count += 1

    print(f"  Migrated {tag_link_count} tag associations.")
    print(f"  Created {len(tag_cache)} unique tag entries.")

    dst_conn.commit()
    src_conn.close()
    dst_conn.close()

    print(f"\nMigration complete: {output_db}")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate old nowa_photos database to new normalized schema",
    )
    parser.add_argument(
        "source_db",
        type=Path,
        help="Path to the source database (old schema)",
    )
    parser.add_argument(
        "output_db",
        type=Path,
        help="Path for the output database (new schema)",
    )
    args = parser.parse_args()

    migrate(args.source_db, args.output_db)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Merge a session database into the main database.

Usage:
    python -m nowa_photos.merge_db <original_db> <session_db>

This script reads all records from session_db and inserts them into original_db,
assigning new sequential IDs. Handles tag and source_item deduplication.
"""

import argparse
import sqlite3
import sys
from pathlib import Path


def merge_databases(original_db: Path, session_db: Path) -> None:
    """Merge session_db into original_db."""
    if not original_db.exists():
        print(f"Error: original database not found: {original_db}", file=sys.stderr)
        sys.exit(1)

    if not session_db.exists():
        print(f"Error: session database not found: {session_db}", file=sys.stderr)
        sys.exit(1)

    # Open both databases
    orig_conn = sqlite3.connect(str(original_db))
    orig_conn.row_factory = sqlite3.Row
    orig_conn.execute("PRAGMA foreign_keys=ON")

    sess_conn = sqlite3.connect(str(session_db))
    sess_conn.row_factory = sqlite3.Row

    # Mapping from session IDs to new IDs in original
    media_id_map: dict[int, int] = {}
    tag_id_map: dict[int, int] = {}
    source_item_id_map: dict[int, int] = {}

    # --- Merge tags (deduplicate by value) ---
    print("Merging tags...")
    sess_tags = sess_conn.execute("SELECT id, value FROM tag").fetchall()
    tags_added = 0
    tags_reused = 0

    for row in sess_tags:
        sess_id = row["id"]
        value = row["value"]

        # Check if tag exists in original
        existing = orig_conn.execute(
            "SELECT id FROM tag WHERE value = ?", (value,)
        ).fetchone()

        if existing:
            tag_id_map[sess_id] = existing["id"]
            tags_reused += 1
        else:
            cursor = orig_conn.execute(
                "INSERT INTO tag (value) VALUES (?)", (value,)
            )
            tag_id_map[sess_id] = cursor.lastrowid
            tags_added += 1

    print(f"  Tags: {tags_added} added, {tags_reused} reused")

    # --- Merge source_items (deduplicate by source_path) ---
    print("Merging source items...")
    sess_sources = sess_conn.execute("SELECT id, source_path FROM source_item").fetchall()
    sources_added = 0
    sources_reused = 0

    for row in sess_sources:
        sess_id = row["id"]
        source_path = row["source_path"]

        # Check if source_item exists in original
        existing = orig_conn.execute(
            "SELECT id FROM source_item WHERE source_path = ?", (source_path,)
        ).fetchone()

        if existing:
            source_item_id_map[sess_id] = existing["id"]
            sources_reused += 1
        else:
            cursor = orig_conn.execute(
                "INSERT INTO source_item (source_path) VALUES (?)", (source_path,)
            )
            source_item_id_map[sess_id] = cursor.lastrowid
            sources_added += 1

    print(f"  Source items: {sources_added} added, {sources_reused} reused")

    # --- Merge media records ---
    print("Merging media records...")
    sess_media = sess_conn.execute("SELECT * FROM media ORDER BY id").fetchall()
    media_added = 0
    media_skipped = 0

    for row in sess_media:
        sess_id = row["id"]
        hash_sig = row["hash_signature"]

        # Check if media with same hash exists (duplicate)
        existing = orig_conn.execute(
            "SELECT id FROM media WHERE hash_signature = ?", (hash_sig,)
        ).fetchone()

        if existing:
            # Duplicate - map to existing and we'll merge sources/tags
            media_id_map[sess_id] = existing["id"]
            media_skipped += 1
        else:
            # Insert new media record
            cursor = orig_conn.execute(
                """INSERT INTO media
                   (archive_path, archive_filename, media_type, hash_signature,
                    file_size, duration, exif_date, file_date, ingestion_timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row["archive_path"], row["archive_filename"], row["media_type"],
                 row["hash_signature"], row["file_size"], row["duration"],
                 row["exif_date"], row["file_date"], row["ingestion_timestamp"]),
            )
            media_id_map[sess_id] = cursor.lastrowid
            media_added += 1

    print(f"  Media: {media_added} added, {media_skipped} duplicates")

    # --- Merge media_tag relationships ---
    print("Merging media-tag relationships...")
    sess_media_tags = sess_conn.execute("SELECT media_id, tag_id FROM media_tag").fetchall()
    mt_added = 0
    mt_skipped = 0

    for row in sess_media_tags:
        new_media_id = media_id_map.get(row["media_id"])
        new_tag_id = tag_id_map.get(row["tag_id"])

        if new_media_id is None or new_tag_id is None:
            print(f"  Warning: unmapped media_tag ({row['media_id']}, {row['tag_id']})")
            continue

        try:
            orig_conn.execute(
                "INSERT INTO media_tag (media_id, tag_id) VALUES (?, ?)",
                (new_media_id, new_tag_id),
            )
            mt_added += 1
        except sqlite3.IntegrityError:
            # Already exists
            mt_skipped += 1

    print(f"  Media-tag links: {mt_added} added, {mt_skipped} already existed")

    # --- Merge media_source relationships ---
    print("Merging media-source relationships...")
    sess_media_sources = sess_conn.execute(
        "SELECT media_id, source_item_id, source_filename FROM media_source"
    ).fetchall()
    ms_added = 0
    ms_skipped = 0

    for row in sess_media_sources:
        new_media_id = media_id_map.get(row["media_id"])
        new_source_id = source_item_id_map.get(row["source_item_id"])

        if new_media_id is None or new_source_id is None:
            print(f"  Warning: unmapped media_source ({row['media_id']}, {row['source_item_id']})")
            continue

        try:
            orig_conn.execute(
                """INSERT INTO media_source (media_id, source_item_id, source_filename)
                   VALUES (?, ?, ?)""",
                (new_media_id, new_source_id, row["source_filename"]),
            )
            ms_added += 1
        except sqlite3.IntegrityError:
            # Already exists
            ms_skipped += 1

    print(f"  Media-source links: {ms_added} added, {ms_skipped} already existed")

    # Commit and close
    orig_conn.commit()
    orig_conn.close()
    sess_conn.close()

    print(f"\nMerge complete!")
    print(f"  Total media in original: run 'SELECT COUNT(*) FROM media' to verify")


def main():
    parser = argparse.ArgumentParser(
        description="Merge a session database into the main database",
    )
    parser.add_argument(
        "original_db",
        type=Path,
        help="Path to the original/main database",
    )
    parser.add_argument(
        "session_db",
        type=Path,
        help="Path to the session database to merge in",
    )
    args = parser.parse_args()

    merge_databases(args.original_db, args.session_db)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Deep sanity check v2: compare files by hash.

Usage:
    python -m nowa_photos.deep_sanity_check2 <archive_folder> <database_path>
    python -m nowa_photos.deep_sanity_check2 <archive_folder> <database_path> --log <logfile>

Iterates over files on disk, hashes each one, and checks if hash exists in DB.
Reports files not found in DB as it goes.
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from nowa_photos.hasher import hash_file

# Media extensions to check (same as ingest.py)
PHOTO_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".heic", ".tiff", ".bmp",
    ".gif", ".webp", ".nef", ".nrw",
}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".m4v"}
ALL_EXTENSIONS = PHOTO_EXTENSIONS | VIDEO_EXTENSIONS


def load_db_hashes(db_path: Path) -> dict[str, dict]:
    """Load all hashes from database.

    Returns dict mapping hash -> {id, archive_path, archive_filename}
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    cursor = conn.execute(
        "SELECT id, archive_path, archive_filename, hash_signature FROM media"
    )

    hashes: dict[str, dict] = {}
    for row in cursor:
        hashes[row["hash_signature"]] = {
            "id": row["id"],
            "archive_path": row["archive_path"],
            "archive_filename": row["archive_filename"],
        }

    conn.close()
    return hashes


def deep_sanity_check2(
    archive_folder: Path,
    db_path: Path,
    log_file: Path | None = None,
) -> tuple[list[str], list[str]]:
    """Compare files by hash.

    Returns:
        Tuple of (files_not_in_db, db_records_not_on_disk)
    """
    output_lines: list[str] = []
    log_handle = None

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_handle = open(log_file, "w", encoding="utf-8")

    def log(msg: str):
        print(msg)
        output_lines.append(msg)
        if log_handle:
            log_handle.write(msg + "\n")
            log_handle.flush()

    log(f"Deep Sanity Check v2 (by hash)")
    log(f"=" * 60)
    log(f"Archive folder: {archive_folder}")
    log(f"Database: {db_path}")
    log(f"Timestamp: {datetime.now().isoformat()}")
    log("")

    # Load all hashes from database
    log("Loading hashes from database...")
    db_hashes = load_db_hashes(db_path)
    log(f"  Loaded {len(db_hashes)} records from database")
    log("")

    # Track which DB hashes we've seen
    seen_hashes: set[str] = set()
    files_not_in_db: list[str] = []
    files_checked = 0
    files_matched = 0

    log("Scanning and hashing files...")
    log("-" * 60)

    # Walk the archive folder
    for dirpath, dirnames, filenames in os.walk(archive_folder):
        # Skip hidden directories
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        for filename in filenames:
            if filename.startswith("."):
                continue
            ext = Path(filename).suffix.lower()
            if ext not in ALL_EXTENSIONS:
                continue

            full_path = Path(dirpath) / filename
            rel_path = str(full_path.relative_to(archive_folder))
            files_checked += 1

            # Hash the file
            try:
                file_hash = hash_file(full_path)
            except Exception as e:
                log(f"<< ERROR hashing: {rel_path} - {e}")
                continue

            # Check if hash exists in DB
            if file_hash in db_hashes:
                seen_hashes.add(file_hash)
                files_matched += 1
                db_record = db_hashes[file_hash]
                db_path_str = f"{db_record['archive_path']}/{db_record['archive_filename']}"

                # Check if path matches
                if rel_path != db_path_str:
                    log(f"~~ PATH MISMATCH: {rel_path}")
                    log(f"      DB expects: {db_path_str}")
            else:
                log(f"<< NOT IN DB: {rel_path}")
                files_not_in_db.append(rel_path)

            # Progress indicator every 100 files
            if files_checked % 100 == 0:
                log(f"   ... checked {files_checked} files ...")

    log("-" * 60)
    log("")

    # Find DB records not seen on disk
    db_records_not_on_disk: list[str] = []
    for hash_sig, record in db_hashes.items():
        if hash_sig not in seen_hashes:
            path = f"{record['archive_path']}/{record['archive_filename']}"
            db_records_not_on_disk.append(path)

    if db_records_not_on_disk:
        log(f">> IN DATABASE BUT NOT ON DISK ({len(db_records_not_on_disk)}):")
        for f in sorted(db_records_not_on_disk):
            log(f">>   {f}")
        log("")

    # Summary
    log("=" * 60)
    log("SUMMARY:")
    log(f"  Files checked on disk:    {files_checked}")
    log(f"  Files matched in DB:      {files_matched}")
    log(f"  Files NOT in DB:          {len(files_not_in_db)}")
    log(f"  DB records not on disk:   {len(db_records_not_on_disk)}")
    log(f"  Total DB records:         {len(db_hashes)}")

    if not files_not_in_db and not db_records_not_on_disk:
        log("\n  STATUS: OK - All files match!")
    else:
        log("\n  STATUS: MISMATCH - See details above.")

    if log_handle:
        log_handle.close()
        print(f"\nLog written to: {log_file}")

    return files_not_in_db, db_records_not_on_disk


def main():
    parser = argparse.ArgumentParser(
        description="Compare files by hash against database",
    )
    parser.add_argument(
        "archive_folder",
        type=Path,
        help="Root folder of the archive (where media files are stored)",
    )
    parser.add_argument(
        "database_path",
        type=Path,
        help="Path to the SQLite database",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Path to write log file",
    )
    args = parser.parse_args()

    archive_folder = args.archive_folder.expanduser().resolve()
    db_path = args.database_path.expanduser().resolve()

    if not archive_folder.exists():
        print(f"Error: archive folder not found: {archive_folder}", file=sys.stderr)
        sys.exit(1)

    if not db_path.exists():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    files_not_in_db, db_not_on_disk = deep_sanity_check2(
        archive_folder, db_path, args.log
    )

    # Exit with error code if mismatches found
    if files_not_in_db or db_not_on_disk:
        sys.exit(1)


if __name__ == "__main__":
    main()

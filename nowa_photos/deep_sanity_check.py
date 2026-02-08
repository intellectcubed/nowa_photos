#!/usr/bin/env python3
"""Deep sanity check: compare database records with physical files.

Usage:
    python -m nowa_photos.deep_sanity_check <archive_folder> <database_path>
    python -m nowa_photos.deep_sanity_check <archive_folder> <database_path> --log <logfile>

Reports:
    >> in DB but not in files (missing from disk)
    << in files but not in DB (untracked on disk)
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Media extensions to check (same as ingest.py)
PHOTO_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".heic", ".tiff", ".bmp",
    ".gif", ".webp", ".nef", ".nrw",
}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".m4v"}
ALL_EXTENSIONS = PHOTO_EXTENSIONS | VIDEO_EXTENSIONS


def scan_files(root: Path) -> set[str]:
    """Scan directory and return set of relative paths for media files."""
    files: set[str] = set()

    for dirpath, dirnames, filenames in os.walk(root):
        # Skip hidden directories
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        for filename in filenames:
            if filename.startswith("."):
                continue
            ext = Path(filename).suffix.lower()
            if ext in ALL_EXTENSIONS:
                full_path = Path(dirpath) / filename
                rel_path = str(full_path.relative_to(root))
                files.add(rel_path)

    return files


def get_db_files(db_path: Path) -> set[str]:
    """Query database and return set of archive paths."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    cursor = conn.execute(
        "SELECT archive_path, archive_filename FROM media"
    )

    files: set[str] = set()
    for row in cursor:
        # Combine path and filename
        full_path = f"{row['archive_path']}/{row['archive_filename']}"
        files.add(full_path)

    conn.close()
    return files


def deep_sanity_check(
    archive_folder: Path,
    db_path: Path,
    log_file: Path | None = None,
) -> tuple[set[str], set[str]]:
    """Compare database with physical files.

    Returns:
        Tuple of (in_db_not_files, in_files_not_db)
    """
    output_lines: list[str] = []

    def log(msg: str):
        print(msg)
        output_lines.append(msg)

    log(f"Deep Sanity Check")
    log(f"=" * 60)
    log(f"Archive folder: {archive_folder}")
    log(f"Database: {db_path}")
    log(f"Timestamp: {datetime.now().isoformat()}")
    log("")

    # Scan physical files
    log("Scanning physical files...")
    disk_files = scan_files(archive_folder)
    log(f"  Found {len(disk_files)} media files on disk")

    # Query database
    log("Querying database...")
    db_files = get_db_files(db_path)
    log(f"  Found {len(db_files)} records in database")
    log("")

    # Compare
    in_db_not_files = db_files - disk_files
    in_files_not_db = disk_files - db_files

    # Report: in DB but not on disk
    if in_db_not_files:
        log(f">> IN DATABASE BUT NOT ON DISK ({len(in_db_not_files)}):")
        for f in sorted(in_db_not_files):
            log(f">>   {f}")
        log("")
    else:
        log(">> All database records have corresponding files on disk.")
        log("")

    # Report: on disk but not in DB
    if in_files_not_db:
        log(f"<< ON DISK BUT NOT IN DATABASE ({len(in_files_not_db)}):")
        for f in sorted(in_files_not_db):
            log(f"<<   {f}")
        log("")
    else:
        log("<< All files on disk are tracked in database.")
        log("")

    # Summary
    log("=" * 60)
    log("SUMMARY:")
    log(f"  Files on disk:        {len(disk_files)}")
    log(f"  Records in database:  {len(db_files)}")
    log(f"  Missing from disk:    {len(in_db_not_files)}")
    log(f"  Missing from database:{len(in_files_not_db)}")

    if len(disk_files) == len(db_files) and not in_db_not_files and not in_files_not_db:
        log("\n  STATUS: OK - Database and disk are in sync!")
    else:
        log("\n  STATUS: MISMATCH - See details above.")

    # Write log file if requested
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
        print(f"\nLog written to: {log_file}")

    return in_db_not_files, in_files_not_db


def main():
    parser = argparse.ArgumentParser(
        description="Compare database records with physical files on disk",
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

    in_db_not_files, in_files_not_db = deep_sanity_check(
        archive_folder, db_path, args.log
    )

    # Exit with error code if mismatches found
    if in_db_not_files or in_files_not_db:
        sys.exit(1)


if __name__ == "__main__":
    main()

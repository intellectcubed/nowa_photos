#!/usr/bin/env python3
"""Deep sanity check (multiprocess): compare files by hash using parallel workers.

Usage:
    python -m nowa_photos.deep_sanity_check_mp <archive_folder> <database_path>
    python -m nowa_photos.deep_sanity_check_mp <archive_folder> <database_path> --workers 4
    python -m nowa_photos.deep_sanity_check_mp <archive_folder> <database_path> --log <logfile>

Same as deep_sanity_check2 but hashes files in parallel using ProcessPoolExecutor.
"""

import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from nowa_photos.deep_sanity_check2 import ALL_EXTENSIONS, load_db_hashes
from nowa_photos.hasher import hash_file

DEFAULT_WORKERS = 8


def _collect_file_paths(archive_folder: Path) -> list[tuple[str, str]]:
    """Walk archive and collect (full_path, rel_path) for all media files."""
    results: list[tuple[str, str]] = []
    for dirpath, dirnames, filenames in os.walk(archive_folder):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for filename in filenames:
            if filename.startswith("."):
                continue
            if Path(filename).suffix.lower() not in ALL_EXTENSIONS:
                continue
            full_path = Path(dirpath) / filename
            rel_path = str(full_path.relative_to(archive_folder))
            results.append((str(full_path), rel_path))
    return results


def _hash_one(args: tuple[str, str]) -> tuple[str, str, str | None]:
    """Hash a single file. Runs in a worker process.

    Args:
        args: (full_path, rel_path)

    Returns:
        (rel_path, hash_hex, error_message_or_None)
    """
    full_path, rel_path = args
    try:
        file_hash = hash_file(full_path)
        return (rel_path, file_hash, None)
    except Exception as e:
        return (rel_path, "", str(e))


def deep_sanity_check_mp(
    archive_folder: Path,
    db_path: Path,
    log_file: Path | None = None,
    workers: int = DEFAULT_WORKERS,
) -> tuple[list[str], list[str]]:
    """Compare files by hash using multiprocess hashing.

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

    log("Deep Sanity Check (multiprocess, by hash)")
    log("=" * 60)
    log(f"Archive folder: {archive_folder}")
    log(f"Database: {db_path}")
    log(f"Workers: {workers}")
    log(f"Timestamp: {datetime.now().isoformat()}")
    log("")

    # Load all hashes from database
    log("Loading hashes from database...")
    db_hashes = load_db_hashes(db_path)
    log(f"  Loaded {len(db_hashes)} records from database")
    log("")

    # Collect all file paths (fast -- just os.walk, no I/O on file contents)
    log("Collecting file paths...")
    file_paths = _collect_file_paths(archive_folder)
    log(f"  Found {len(file_paths)} media files on disk")
    log("")

    # Hash files in parallel
    log(f"Hashing files with {workers} workers...")
    log("-" * 60)

    seen_hashes: set[str] = set()
    files_not_in_db: list[str] = []
    files_checked = 0
    files_matched = 0

    total = len(file_paths)

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_hash_one, fp): fp for fp in file_paths
        }

        for future in as_completed(futures):
            rel_path, file_hash, error = future.result()
            files_checked += 1

            # Live progress on single line (overwrite in-place)
            print(
                f"\r   [{files_checked}/{total}] {rel_path[:70]:<70}",
                end="",
                flush=True,
            )

            if error:
                print()  # newline before log message
                log(f"<< ERROR hashing: {rel_path} - {error}")
                continue

            if file_hash in db_hashes:
                seen_hashes.add(file_hash)
                files_matched += 1
                db_record = db_hashes[file_hash]
                db_path_str = (
                    f"{db_record['archive_path']}/{db_record['archive_filename']}"
                )
                if rel_path != db_path_str:
                    print()  # newline before log message
                    log(f"~~ PATH MISMATCH: {rel_path}")
                    log(f"      DB expects: {db_path_str}")
            else:
                print()  # newline before log message
                log(f"<< NOT IN DB: {rel_path}")
                files_not_in_db.append(rel_path)

        print()  # final newline after progress line

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
        description="Compare files by hash against database (multiprocess)",
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
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of worker processes (default: {DEFAULT_WORKERS})",
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

    files_not_in_db, db_not_on_disk = deep_sanity_check_mp(
        archive_folder, db_path, args.log, args.workers
    )

    if files_not_in_db or db_not_on_disk:
        sys.exit(1)


if __name__ == "__main__":
    main()

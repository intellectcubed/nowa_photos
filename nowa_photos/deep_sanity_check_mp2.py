#!/usr/bin/env python3
"""Hash all media files in a directory and write filename,hash to a log.

Usage:
    python nowa_photos/deep_sanity_check_mp2.py <folder> <output_log>
    python nowa_photos/deep_sanity_check_mp2.py <folder> <output_log> --workers 4
"""

import argparse
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from nowa_photos.deep_sanity_check2 import ALL_EXTENSIONS
from nowa_photos.hasher import hash_file

DEFAULT_WORKERS = 8


def _collect_file_paths(folder: Path) -> list[tuple[str, str]]:
    """Walk folder and collect (full_path, rel_path) for all media files."""
    results: list[tuple[str, str]] = []
    for dirpath, dirnames, filenames in os.walk(folder):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for filename in filenames:
            if filename.startswith("."):
                continue
            if Path(filename).suffix.lower() not in ALL_EXTENSIONS:
                continue
            full_path = Path(dirpath) / filename
            rel_path = str(full_path.relative_to(folder))
            results.append((str(full_path), rel_path))
    return results


def _hash_one(args: tuple[str, str]) -> tuple[str, str, str | None]:
    """Hash a single file. Retries up to 5 times on FileNotFoundError."""
    full_path, rel_path = args
    max_retries = 5
    for attempt in range(max_retries + 1):
        try:
            file_hash = hash_file(full_path)
            return (rel_path, file_hash, None)
        except FileNotFoundError:
            if attempt < max_retries:
                delay = 2 ** attempt
                print(f"retrying failed file read {rel_path} (attempt {attempt + 2}/{max_retries + 1})")
                time.sleep(delay)
            else:
                return (rel_path, "", f"FileNotFoundError after {max_retries + 1} attempts")
        except Exception as e:
            return (rel_path, "", str(e))
    return (rel_path, "", "unexpected retry exhaustion")


def hash_all_files(
    folder: Path,
    output_log: Path,
    workers: int = DEFAULT_WORKERS,
) -> None:
    """Hash all media files and write filename,hash to output_log."""
    output_log.parent.mkdir(parents=True, exist_ok=True)

    print(f"Folder:  {folder}")
    print(f"Output:  {output_log}")
    print(f"Workers: {workers}")
    print()

    print("Collecting file paths...")
    file_paths = _collect_file_paths(folder)
    total = len(file_paths)
    print(f"  Found {total} media files")
    print()

    files_hashed = 0
    errors = 0

    with open(output_log, "w", encoding="utf-8") as log:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_hash_one, fp): fp for fp in file_paths
            }

            for future in as_completed(futures):
                rel_path, file_hash, error = future.result()
                files_hashed += 1

                print(
                    f"\r   [{files_hashed}/{total}] {rel_path[:70]:<70}",
                    end="",
                    flush=True,
                )

                if error:
                    errors += 1
                    log.write(f"{rel_path},ERROR: {error}\n")
                else:
                    log.write(f"{rel_path},{file_hash}\n")

            print()

    print()
    print(f"Done. {files_hashed} files hashed, {errors} errors.")
    print(f"Log written to: {output_log}")


def main():
    parser = argparse.ArgumentParser(
        description="Hash all media files and write filename,hash to a log",
    )
    parser.add_argument(
        "folder",
        type=Path,
        help="Root folder to scan for media files",
    )
    parser.add_argument(
        "output_log",
        type=Path,
        help="Output CSV log file (filename,hash)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of worker processes (default: {DEFAULT_WORKERS})",
    )
    args = parser.parse_args()

    folder = args.folder.expanduser().resolve()
    output_log = args.output_log.expanduser().resolve()

    if not folder.exists():
        print(f"Error: folder not found: {folder}", file=sys.stderr)
        sys.exit(1)

    hash_all_files(folder, output_log, args.workers)


if __name__ == "__main__":
    main()

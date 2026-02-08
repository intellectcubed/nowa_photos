#!/usr/bin/env python3
"""Count files by extension in a directory tree.

Usage:
    python -m nowa_photos.sanity_check <root_folder>
"""

import argparse
import os
import sys
from collections import Counter
from pathlib import Path


def count_files_by_extension(root: Path) -> Counter:
    """Walk directory tree and count files by extension."""
    counts: Counter = Counter()

    for dirpath, dirnames, filenames in os.walk(root):
        # Skip hidden directories
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        for filename in filenames:
            if filename.startswith("."):
                continue
            ext = Path(filename).suffix.lower()
            if not ext:
                ext = "(no extension)"
            counts[ext] += 1

    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Count files by extension in a directory tree",
    )
    parser.add_argument(
        "root_folder",
        type=Path,
        help="Root folder to scan",
    )
    args = parser.parse_args()

    root = args.root_folder.expanduser().resolve()
    if not root.exists():
        print(f"Error: folder not found: {root}", file=sys.stderr)
        sys.exit(1)

    if not root.is_dir():
        print(f"Error: not a directory: {root}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning: {root}\n")

    counts = count_files_by_extension(root)

    if not counts:
        print("No files found.")
        return

    # Sort by count descending
    for ext, count in counts.most_common():
        print(f"{ext}\t{count}")

    print(f"\nTotal: {sum(counts.values())} files")


if __name__ == "__main__":
    main()

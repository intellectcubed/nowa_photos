#!/usr/bin/env python3
"""Count files by extension in a directory tree.

Usage:
    python -m nowa_photos.sanity_check <root_folder>
    python -m nowa_photos.sanity_check <root_folder> --include-hidden
    python -m nowa_photos.sanity_check <root_folder> --show-bundles

Handles macOS library bundles (.photoslibrary, .aplibrary, etc.) by
traversing inside them.
"""

import argparse
import os
import sys
from collections import Counter
from pathlib import Path

# macOS bundle extensions that should be traversed as directories
MACOS_BUNDLE_EXTENSIONS = {
    ".photoslibrary",
    ".aplibrary",
    ".migratedphotolibrary",
    ".fcpbundle",
    ".fcpproject",
    ".imovielibrary",
    ".musiclibrary",
    ".garageband",
    ".band",
}


def count_files_by_extension(
    root: Path,
    include_hidden: bool = False,
    show_bundles: bool = False,
) -> tuple[Counter, list[str]]:
    """Walk directory tree and count files by extension.

    Args:
        root: Root directory to scan
        include_hidden: If True, include hidden files and directories
        show_bundles: If True, collect bundle paths found

    Returns:
        Tuple of (extension counts, list of bundle paths found)
    """
    counts: Counter = Counter()
    bundles_found: list[str] = []

    for dirpath, dirnames, filenames in os.walk(root):
        # Filter directories
        if not include_hidden:
            # Remove hidden directories from traversal
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        # Track bundles
        if show_bundles:
            for d in dirnames:
                ext = Path(d).suffix.lower()
                if ext in MACOS_BUNDLE_EXTENSIONS:
                    bundles_found.append(os.path.join(dirpath, d))

        # Count files
        for filename in filenames:
            if not include_hidden and filename.startswith("."):
                continue
            ext = Path(filename).suffix.lower()
            if not ext:
                ext = "(no extension)"
            counts[ext] += 1

    return counts, bundles_found


def main():
    parser = argparse.ArgumentParser(
        description="Count files by extension in a directory tree",
    )
    parser.add_argument(
        "root_folder",
        type=Path,
        help="Root folder to scan",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden files and directories (starting with .)",
    )
    parser.add_argument(
        "--show-bundles",
        action="store_true",
        help="List macOS library bundles found (.photoslibrary, etc.)",
    )
    args = parser.parse_args()

    root = args.root_folder.expanduser().resolve()
    if not root.exists():
        print(f"Error: folder not found: {root}", file=sys.stderr)
        sys.exit(1)

    if not root.is_dir():
        print(f"Error: not a directory: {root}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning: {root}")
    if args.include_hidden:
        print("(including hidden files)")
    print()

    counts, bundles = count_files_by_extension(
        root,
        include_hidden=args.include_hidden,
        show_bundles=args.show_bundles,
    )

    if args.show_bundles and bundles:
        print("macOS bundles found:")
        for b in sorted(bundles):
            print(f"  {b}")
        print()

    if not counts:
        print("No files found.")
        return

    # Sort by count descending
    for ext, count in counts.most_common():
        print(f"{ext}\t{count}")

    print(f"\nTotal: {sum(counts.values())} files")


if __name__ == "__main__":
    main()

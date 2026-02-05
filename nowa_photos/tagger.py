"""Tag extraction from folder paths and interactive prompting."""

import re
from pathlib import Path


def extract_tags_from_path(
    file_path: Path | str,
    base_path: Path | str,
    stop_words: list[str] | None = None,
) -> list[str]:
    """Extract tags from a file's path relative to base_path.

    Each directory component becomes a candidate tag. Stop words are
    filtered out. Tags are lowercased and stripped of non-alphanumeric
    characters (hyphens and underscores are kept).
    """
    stop_words = stop_words or []
    stop_set = {w.lower() for w in stop_words}

    rel = Path(file_path).relative_to(Path(base_path))
    # Use parent parts (exclude the filename itself)
    parts = rel.parent.parts

    tags = []
    for part in parts:
        cleaned = _clean_tag(part)
        if cleaned and cleaned not in stop_set:
            tags.append(cleaned)
    return tags


def _clean_tag(raw: str) -> str:
    """Normalize a raw directory name into a tag value."""
    tag = raw.strip().lower()
    # Keep only alphanumeric, hyphens, underscores
    tag = re.sub(r"[^a-z0-9\-_]", "", tag)
    return tag


def prompt_tags_for_folder(
    folder: str,
    file_count: int,
    suggested_tags: list[str],
) -> list[str] | None:
    """Interactively prompt the user to accept/edit tags for a folder.

    Returns the final tag list, or None if the user declines tagging.
    """
    print(f'\nFolder "{folder}" contains {file_count} files.')
    print(f"Suggested tags: {suggested_tags}")
    print("Apply these tags to all files from this folder?")

    while True:
        response = input("(y = yes, n = no, edit = modify tags): ").strip().lower()
        if response == "y":
            return suggested_tags
        elif response == "n":
            return None
        elif response == "edit":
            raw = input("Enter comma-separated tags: ").strip()
            if raw:
                return [_clean_tag(t) for t in raw.split(",") if _clean_tag(t)]
            return None
        else:
            print("Please enter y, n, or edit.")


def batch_prompt_folders(
    files_by_folder: dict[str, list[Path]],
    base_path: Path,
    stop_words: list[str] | None = None,
    enable_prompts: bool = True,
) -> dict[str, list[str]]:
    """Group files by folder, extract tags, optionally prompt, return folder→tags mapping.

    Returns a dict mapping folder (relative string) to the list of tags
    that should be applied to all files in that folder.
    """
    result: dict[str, list[str]] = {}

    for folder, files in sorted(files_by_folder.items()):
        # Use the first file to derive tags for the folder
        suggested = extract_tags_from_path(files[0], base_path, stop_words)

        if not suggested:
            result[folder] = []
            continue

        if enable_prompts:
            tags = prompt_tags_for_folder(folder, len(files), suggested)
            result[folder] = tags if tags is not None else []
        else:
            result[folder] = suggested

    return result

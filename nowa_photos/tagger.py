"""Tag extraction from folder paths and CSV review workflow."""

import csv
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


def extract_folder_tags(
    files_by_folder: dict[str, list[Path]],
    base_path: Path,
    stop_words: list[str] | None = None,
) -> dict[str, list[str]]:
    """Extract tags for each folder using directory names.

    Returns a dict mapping folder (relative string) to the list of
    auto-extracted tags.
    """
    result: dict[str, list[str]] = {}

    for folder, files in sorted(files_by_folder.items()):
        suggested = extract_tags_from_path(files[0], base_path, stop_words)
        result[folder] = suggested

    return result


def write_tag_review_csv(
    folder_tags: dict[str, list[str]],
    file_counts: dict[str, int],
    csv_path: Path | str,
) -> Path:
    """Write a tag review CSV for the user to edit.

    Columns: folder, file_count, tags (comma-separated within the field).
    Returns the path written to.
    """
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["folder", "file_count", "tags"])
        for folder in sorted(folder_tags):
            tags = folder_tags[folder]
            count = file_counts.get(folder, 0)
            writer.writerow([folder, count, ",".join(tags)])

    return csv_path


def load_tag_review_csv(csv_path: Path | str) -> dict[str, list[str]]:
    """Load an edited tag review CSV and return folder -> tags mapping.

    Parses the tags column as comma-separated values. Empty tags fields
    result in an empty list for that folder.
    """
    result: dict[str, list[str]] = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            folder = row["folder"]
            raw_tags = row["tags"].strip()
            if raw_tags:
                tags = [_clean_tag(t) for t in raw_tags.split(",") if _clean_tag(t)]
            else:
                tags = []
            result[folder] = tags

    return result

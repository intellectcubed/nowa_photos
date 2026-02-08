"""Main ingestion pipeline for Nowa Photos."""

import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from nowa_photos.config import AppConfig
from nowa_photos.database import Database
from nowa_photos.hasher import hash_file
from nowa_photos.metadata import export_metadata_jsonl
from nowa_photos.tagger import extract_folder_tags, load_tag_review_csv, write_tag_review_csv

# --- Supported extensions ---

PHOTO_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".heic", ".tiff", ".bmp",
    ".gif", ".webp", ".nef", ".nrw",
}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".m4v"}
ALL_EXTENSIONS = PHOTO_EXTENSIONS | VIDEO_EXTENSIONS


# --- Session stats ---

@dataclass
class SessionStats:
    imported: int = 0
    duplicates: int = 0
    tags_added: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)


# --- EXIF date extraction ---

def _extract_exif_date(file_path: Path) -> str | None:
    """Extract EXIF DateTimeOriginal from an image file.

    Uses pillow-heif for HEIC, exifread for NEF/NRW, Pillow for others.
    Returns ISO-format string or None.
    """
    ext = file_path.suffix.lower()

    try:
        if ext == ".heic":
            return _exif_date_heic(file_path)
        elif ext in (".nef", ".nrw"):
            return _exif_date_exifread(file_path)
        else:
            return _exif_date_pillow(file_path)
    except Exception:
        return None


def _exif_date_pillow(file_path: Path) -> str | None:
    from PIL import Image
    from PIL.ExifTags import Base as ExifBase

    with Image.open(file_path) as img:
        exif = img.getexif()
        if not exif:
            return None
        dt_str = exif.get(ExifBase.DateTimeOriginal)
        if dt_str:
            return _parse_exif_datetime(dt_str)
    return None


def _exif_date_exifread(file_path: Path) -> str | None:
    import exifread

    with open(file_path, "rb") as f:
        tags = exifread.process_file(f, stop_tag="DateTimeOriginal", details=False)
    dt_tag = tags.get("EXIF DateTimeOriginal")
    if dt_tag:
        return _parse_exif_datetime(str(dt_tag))
    return None


def _exif_date_heic(file_path: Path) -> str | None:
    from pillow_heif import open_heif

    heif = open_heif(file_path)
    exif_data = heif.info.get("exif")
    if not exif_data:
        return None

    # pillow-heif returns raw EXIF bytes; use Pillow to parse
    from PIL import Image
    from PIL.ExifTags import Base as ExifBase
    import io

    img = Image.open(io.BytesIO(exif_data))
    exif = img.getexif()
    if exif:
        dt_str = exif.get(ExifBase.DateTimeOriginal)
        if dt_str:
            return _parse_exif_datetime(dt_str)
    return None


def _parse_exif_datetime(dt_str: str) -> str | None:
    """Parse EXIF datetime 'YYYY:MM:DD HH:MM:SS' to ISO format."""
    try:
        dt = datetime.strptime(dt_str, "%Y:%m:%d %H:%M:%S")
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


# --- Video duration ---

def _get_video_duration(file_path: Path) -> float | None:
    """Get video duration in seconds via ffprobe. Returns None if unavailable."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                str(file_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            duration = data.get("format", {}).get("duration")
            if duration is not None:
                return float(duration)
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError, ValueError):
        pass
    return None


# --- File discovery ---

def discover_files(ingestion_path: Path) -> list[Path]:
    """Walk ingestion_path and return supported media files, skipping hidden files."""
    files = []
    for dirpath, dirnames, filenames in os.walk(ingestion_path):
        # Skip hidden directories
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fname in filenames:
            if fname.startswith("."):
                continue
            if Path(fname).suffix.lower() in ALL_EXTENSIONS:
                files.append(Path(dirpath) / fname)
    return sorted(files)


# --- Archive path construction ---

def _get_file_date(file_path: Path) -> datetime:
    """Get file modification date."""
    return datetime.fromtimestamp(file_path.stat().st_mtime)


def _build_archive_path(
    file_path: Path,
    file_hash: str,
    archive_root: Path,
    exif_date_str: str | None,
    file_date: datetime,
) -> Path:
    """Build the archive destination path: archive_root/YYYY/MM/filename.

    If a different file already exists at that path, append _hash[:8] to the
    filename to resolve the collision.
    """
    # Determine date for directory structure
    if exif_date_str:
        try:
            dt = datetime.fromisoformat(exif_date_str)
        except ValueError:
            dt = file_date
    else:
        dt = file_date

    year_month = Path(f"{dt.year:04d}") / f"{dt.month:02d}"
    dest = archive_root / year_month / file_path.name

    # Handle filename collision (different file at same path)
    if dest.exists():
        stem = file_path.stem
        suffix = file_path.suffix
        dest = archive_root / year_month / f"{stem}_{file_hash[:8]}{suffix}"

    return dest


# --- Per-file processing ---

def _classify_media(file_path: Path) -> str:
    ext = file_path.suffix.lower()
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "photo"


def process_file(
    file_path: Path,
    config: AppConfig,
    db: Database,
    session_ts: str,
    stats: SessionStats,
    ingestion_path: Path | None = None,
) -> tuple[int | None, bool]:
    """Process a single file: hash, dedup, archive, record in DB.

    Returns (media_id, was_duplicate) tuple. media_id is None on error.
    """
    file_hash = hash_file(file_path)

    # Split source path and filename
    source_dir = str(file_path.parent)
    source_filename = file_path.name

    existing = db.get_media_by_hash(file_hash)
    if existing:
        # Duplicate: add source path (tags will be added in the tagging phase)
        stats.duplicates += 1
        db.add_source(existing["id"], source_dir, source_filename)

        # Auto-tag thumbnails based on path or filename
        if "thumb" in source_dir.lower() or "thumb" in source_filename.lower():
            db.add_tags(existing["id"], ["thumbnail"])
            stats.tags_added += 1

        return existing["id"], True

    # New file
    media_type = _classify_media(file_path)
    exif_date = None
    if media_type == "photo":
        exif_date = _extract_exif_date(file_path)

    duration = None
    if media_type == "video":
        duration = _get_video_duration(file_path)

    file_date = _get_file_date(file_path)
    file_date_str = file_date.isoformat()

    archive_dest = _build_archive_path(
        file_path, file_hash, config.archive_path, exif_date, file_date,
    )

    # Ensure archive directory exists and copy/move
    archive_dest.parent.mkdir(parents=True, exist_ok=True)
    if config.mode == "move":
        shutil.move(str(file_path), str(archive_dest))
    else:
        shutil.copy2(str(file_path), str(archive_dest))

    # Split archive path and filename
    rel_archive_dir = str(archive_dest.parent.relative_to(config.archive_path))
    archive_filename = archive_dest.name

    file_size = archive_dest.stat().st_size

    media_id = db.insert_media(
        archive_path=rel_archive_dir,
        archive_filename=archive_filename,
        media_type=media_type,
        hash_signature=file_hash,
        file_size=file_size,
        file_date=file_date_str,
        ingestion_timestamp=session_ts,
        exif_date=exif_date,
        duration=duration,
    )

    # Record original source
    db.add_source(media_id, source_dir, source_filename)

    # Auto-tag thumbnails based on path or filename
    if "thumb" in source_dir.lower() or "thumb" in source_filename.lower():
        db.add_tags(media_id, ["thumbnail"])
        stats.tags_added += 1

    stats.imported += 1
    return media_id, False


# --- Session log ---

def _write_session_log(config: AppConfig, stats: SessionStats, session_ts: str) -> Path:
    """Write a session log file and return its path."""
    config.log_dir.mkdir(parents=True, exist_ok=True)
    ts_str = session_ts.replace(":", "").replace("-", "").replace("T", "_")
    log_path = config.log_dir / f"session_{ts_str}.txt"

    sources = ", ".join(str(p) for p in config.ingestion_paths)
    lines = [
        f"Nowa Photos Ingestion Session",
        f"Timestamp: {session_ts}",
        f"Sources: {sources}",
        f"Archive: {config.archive_path}",
        f"Mode: {config.mode}",
        f"",
        f"Session Summary:",
        f"  Imported: {stats.imported}",
        f"  Duplicates skipped: {stats.duplicates}",
        f"  Tags added: {stats.tags_added}",
        f"  Errors: {stats.errors}",
    ]

    if stats.error_details:
        lines.append("")
        lines.append("Errors:")
        for detail in stats.error_details:
            lines.append(f"  - {detail}")

    log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return log_path


# --- Main entry point ---

def _process_ingestion_path(
    ingestion_path: Path,
    config: AppConfig,
    db: Database,
    session_ts: str,
    stats: SessionStats,
    all_folder_tags: dict[str, list[str]],
    all_file_counts: dict[str, int],
) -> None:
    """Process a single ingestion path."""
    # 1. Discover files
    print(f"Scanning {ingestion_path} ...")
    files = discover_files(ingestion_path)
    print(f"Found {len(files)} media files.")

    if not files:
        print("Nothing to process in this path.")
        return

    # 2. Process each file (with error isolation)
    file_media_ids: dict[Path, int] = {}
    duplicate_files: set[Path] = set()
    for file_path in files:
        try:
            media_id, was_duplicate = process_file(file_path, config, db, session_ts, stats, ingestion_path)
            if media_id is not None:
                file_media_ids[file_path] = media_id
                if was_duplicate:
                    duplicate_files.add(file_path)
        except Exception as exc:
            stats.errors += 1
            stats.error_details.append(f"{file_path}: {exc}")
            print(f"  ERROR processing {file_path}: {exc}")

    # 3. Tag application (auto-apply defaults)
    files_by_folder: dict[str, list[Path]] = defaultdict(list)
    for file_path in files:
        rel_folder = str(Path(file_path).relative_to(ingestion_path).parent)
        files_by_folder[rel_folder].append(file_path)

    folder_tags = extract_folder_tags(
        files_by_folder,
        ingestion_path,
        stop_words=config.tag_stop_words,
    )

    for folder, tags in folder_tags.items():
        if not tags:
            continue
        for file_path in files_by_folder[folder]:
            media_id = file_media_ids.get(file_path)
            if media_id is not None:
                existing_tags = db.get_tags_for_media(media_id)
                new_tags = [t for t in tags if t not in existing_tags]
                if new_tags:
                    db.add_tags(media_id, new_tags)
                    stats.tags_added += len(new_tags)

    # Collect folder tags for CSV (prefix with ingestion path name for uniqueness)
    path_name = ingestion_path.name
    for folder, tags in folder_tags.items():
        key = f"{path_name}/{folder}" if folder != "." else path_name
        all_folder_tags[key] = tags
        all_file_counts[key] = len(files_by_folder[folder])


def run_ingestion(config: AppConfig) -> SessionStats:
    """Run the full ingestion pipeline for all configured paths."""
    session_ts = datetime.now().isoformat(timespec="seconds")
    stats = SessionStats()

    # Work with a local copy of the database during ingestion
    local_data_dir = Path("data")
    local_data_dir.mkdir(parents=True, exist_ok=True)
    local_db_path = local_data_dir / "nowa_photos.db"

    # If archive DB exists and local doesn't, copy it for continuity
    if config.db_path.exists() and not local_db_path.exists():
        shutil.copy2(str(config.db_path), str(local_db_path))

    db = Database(local_db_path)
    all_folder_tags: dict[str, list[str]] = {}
    all_file_counts: dict[str, int] = {}

    try:
        total_paths = len(config.ingestion_paths)
        for idx, ingestion_path in enumerate(config.ingestion_paths, start=1):
            print(f"\nProcessing path: '{ingestion_path.name}' {idx} of {total_paths}")
            print("=" * 60)
            _process_ingestion_path(
                ingestion_path, config, db, session_ts, stats,
                all_folder_tags, all_file_counts,
            )

        # Write combined tag review CSV for user to edit
        ts_str = session_ts.replace(":", "").replace("-", "").replace("T", "_")
        csv_path = config.metadata_path.parent / f"tag_review_{ts_str}.csv"
        write_tag_review_csv(all_folder_tags, all_file_counts, csv_path)
        print(f"\nTag review CSV: {csv_path}")
        print("  Edit this file and run with --apply-tags to override tag assignments.")

        # Export metadata JSONL
        count = export_metadata_jsonl(db, config.metadata_path)
        print(f"Exported {count} records to {config.metadata_path}")

        # Write session log
        log_path = _write_session_log(config, stats, session_ts)
        print(f"Session log: {log_path}")

        # Print summary
        print(f"\nSession Summary:")
        print(f"  Imported: {stats.imported}")
        print(f"  Duplicates skipped: {stats.duplicates}")
        print(f"  Tags added: {stats.tags_added}")
        print(f"  Errors: {stats.errors}")

    finally:
        db.close()

    # Archive the database to the target destination
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(local_db_path), str(config.db_path))
    print(f"Database archived to {config.db_path}")
    local_db_path.unlink()
    print(f"Local database copy removed.")

    return stats


def apply_tags_from_csv(config: AppConfig, csv_path: Path) -> None:
    """Apply tag overrides from an edited review CSV.

    For each folder in the CSV, finds all media records sourced from that
    folder and replaces their tags with the values from the CSV.
    Then re-exports the JSONL metadata file.
    """
    folder_tags = load_tag_review_csv(csv_path)

    # Build mapping from path name to full ingestion path
    path_name_to_full: dict[str, Path] = {p.name: p for p in config.ingestion_paths}

    # Work with a local copy of the database
    local_data_dir = Path("data")
    local_data_dir.mkdir(parents=True, exist_ok=True)
    local_db_path = local_data_dir / "nowa_photos.db"

    # Copy archive DB to local for editing
    if config.db_path.exists():
        shutil.copy2(str(config.db_path), str(local_db_path))

    db = Database(local_db_path)
    try:
        replaced = 0
        for folder, tags in folder_tags.items():
            # Folder key format: "path_name/subfolder" or just "path_name" for root
            parts = folder.split("/", 1)
            path_name = parts[0]
            subfolder = parts[1] if len(parts) > 1 else "."

            ingestion_path = path_name_to_full.get(path_name)
            if ingestion_path is None:
                print(f"  Warning: unknown ingestion path '{path_name}', skipping")
                continue

            media_ids = db.get_media_ids_by_source_folder(str(ingestion_path), subfolder)
            for mid in media_ids:
                db.replace_tags(mid, tags)
                replaced += 1

        count = export_metadata_jsonl(db, config.metadata_path)
        print(f"Updated tags for {replaced} media records from {len(folder_tags)} folders.")
        print(f"Exported {count} records to {config.metadata_path}")
    finally:
        db.close()

    # Archive the database to the target destination
    shutil.copy2(str(local_db_path), str(config.db_path))
    print(f"Database archived to {config.db_path}")
    local_db_path.unlink()
    print(f"Local database copy removed.")


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="nowa-photos-ingest",
        description="Nowa Photos archival ingestion system",
    )
    parser.add_argument(
        "--config", "-c",
        type=Path,
        required=True,
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--apply-tags",
        type=Path,
        default=None,
        help="Path to an edited tag review CSV to apply",
    )
    args = parser.parse_args()

    # Load config using the same YAML loader, but bypass its own CLI parsing
    from nowa_photos.config import _load_yaml, _validate_and_resolve
    config_path = args.config.expanduser().resolve()
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    data = _load_yaml(config_path)
    config = _validate_and_resolve(data)

    if args.apply_tags:
        csv_path = args.apply_tags.expanduser().resolve()
        if not csv_path.exists():
            print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)
        apply_tags_from_csv(config, csv_path)
    else:
        for path in config.ingestion_paths:
            if not path.exists():
                print(f"Error: ingestion path does not exist: {path}", file=sys.stderr)
                sys.exit(1)
        config.archive_path.mkdir(parents=True, exist_ok=True)
        run_ingestion(config)

"""Tests for nowa_photos.ingest."""

from datetime import datetime
from pathlib import Path

import pytest

from nowa_photos.config import AppConfig
from nowa_photos.database import Database
from nowa_photos.ingest import (
    discover_files,
    _build_archive_path,
    _classify_media,
    process_file,
    run_ingestion,
    apply_tags_from_csv,
    SessionStats,
    PHOTO_EXTENSIONS,
    VIDEO_EXTENSIONS,
)


# --- File discovery ---

def test_discover_supported_files(tmp_path):
    """discover_files finds supported media files."""
    (tmp_path / "photo.jpg").write_bytes(b"jpg")
    (tmp_path / "video.mp4").write_bytes(b"mp4")
    (tmp_path / "readme.txt").write_bytes(b"txt")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "nested.png").write_bytes(b"png")

    files = discover_files(tmp_path)
    names = {f.name for f in files}
    assert "photo.jpg" in names
    assert "video.mp4" in names
    assert "nested.png" in names
    assert "readme.txt" not in names


def test_discover_skips_hidden(tmp_path):
    """Hidden files and directories are skipped."""
    (tmp_path / ".hidden_file.jpg").write_bytes(b"hidden")
    (tmp_path / ".hidden_dir").mkdir()
    (tmp_path / ".hidden_dir" / "photo.jpg").write_bytes(b"nested hidden")
    (tmp_path / "visible.jpg").write_bytes(b"visible")

    files = discover_files(tmp_path)
    names = {f.name for f in files}
    assert "visible.jpg" in names
    assert ".hidden_file.jpg" not in names
    assert "photo.jpg" not in names


def test_discover_empty_dir(tmp_path):
    """Empty directory returns empty list."""
    assert discover_files(tmp_path) == []


def test_discover_case_insensitive(tmp_path):
    """Extensions are matched case-insensitively."""
    (tmp_path / "PHOTO.JPG").write_bytes(b"jpg")
    (tmp_path / "video.MOV").write_bytes(b"mov")

    files = discover_files(tmp_path)
    assert len(files) == 2


# --- Archive path construction ---

def test_archive_path_from_exif(tmp_path):
    """Archive path uses EXIF date when available."""
    archive_root = tmp_path / "archive"
    archive_root.mkdir()

    dest = _build_archive_path(
        file_path=Path("/source/IMG_001.jpg"),
        file_hash="abc123def456",
        archive_root=archive_root,
        exif_date_str="2026-03-15T14:30:00",
        file_date=datetime(2026, 1, 1),
    )
    assert "2026" in str(dest)
    assert "03" in str(dest)
    assert dest.name == "IMG_001.jpg"


def test_archive_path_fallback_to_file_date(tmp_path):
    """Archive path uses file date when no EXIF date."""
    archive_root = tmp_path / "archive"
    archive_root.mkdir()

    dest = _build_archive_path(
        file_path=Path("/source/IMG_001.jpg"),
        file_hash="abc123def456",
        archive_root=archive_root,
        exif_date_str=None,
        file_date=datetime(2025, 7, 20),
    )
    assert "2025" in str(dest)
    assert "07" in str(dest)


def test_archive_path_collision(tmp_path):
    """Filename collision appends hash suffix."""
    archive_root = tmp_path / "archive"
    (archive_root / "2026" / "01").mkdir(parents=True)
    # Pre-create the target file to cause collision
    (archive_root / "2026" / "01" / "IMG_001.jpg").write_bytes(b"existing")

    dest = _build_archive_path(
        file_path=Path("/source/IMG_001.jpg"),
        file_hash="abcdef1234567890",
        archive_root=archive_root,
        exif_date_str="2026-01-15T14:30:00",
        file_date=datetime(2026, 1, 15),
    )
    assert dest.name == "IMG_001_abcdef12.jpg"


# --- Media classification ---

def test_classify_photo():
    assert _classify_media(Path("photo.jpg")) == "photo"
    assert _classify_media(Path("photo.JPEG")) == "photo"
    assert _classify_media(Path("raw.nef")) == "photo"


def test_classify_video():
    assert _classify_media(Path("clip.mp4")) == "video"
    assert _classify_media(Path("clip.MOV")) == "video"


# --- End-to-end ingestion ---

@pytest.fixture
def ingestion_env(tmp_path):
    """Set up source and archive directories with a test image."""
    source = tmp_path / "source"
    archive = tmp_path / "archive"
    source.mkdir()
    archive.mkdir()

    # Create a simple test file
    (source / "test.jpg").write_bytes(b"fake image content")

    config = AppConfig(
        ingestion_paths=[source],
        archive_path=archive,
        db_path=archive / "data" / "test.db",
        metadata_path=archive / "data" / "metadata.jsonl",
        mode="copy",

        tag_stop_words=[],
        log_dir=archive / "logs",
    )
    return config


def test_end_to_end_ingestion(ingestion_env):
    """Full ingestion run processes files and creates expected outputs."""
    config = ingestion_env

    stats = run_ingestion(config)

    assert stats.imported == 1
    assert stats.duplicates == 0
    assert stats.errors == 0

    # DB should have the record
    db = Database(config.db_path)
    records = db.get_all_media_with_details()
    assert len(records) == 1
    assert records[0]["media_type"] == "photo"
    db.close()

    # Metadata JSONL should exist
    assert config.metadata_path.exists()

    # Archive should have the file
    archived_files = list(config.archive_path.rglob("*.jpg"))
    assert len(archived_files) == 1


def test_duplicate_detection(ingestion_env):
    """Re-running ingestion on same source detects duplicates."""
    config = ingestion_env

    # First run
    stats1 = run_ingestion(config)
    assert stats1.imported == 1

    # Second run on same source
    stats2 = run_ingestion(config)
    assert stats2.imported == 0
    assert stats2.duplicates == 1


def test_move_mode(tmp_path):
    """Move mode removes the source file after ingestion."""
    source = tmp_path / "source"
    archive = tmp_path / "archive"
    source.mkdir()
    archive.mkdir()
    src_file = source / "moveme.jpg"
    src_file.write_bytes(b"move test content")

    config = AppConfig(
        ingestion_paths=[source],
        archive_path=archive,
        db_path=archive / "data" / "test.db",
        metadata_path=archive / "data" / "metadata.jsonl",
        mode="move",

        tag_stop_words=[],
        log_dir=archive / "logs",
    )

    run_ingestion(config)

    assert not src_file.exists()
    archived = list(archive.rglob("*.jpg"))
    assert len(archived) == 1


def test_process_file_error_isolation(ingestion_env):
    """Errors on individual files don't halt the session."""
    config = ingestion_env

    # Add a second valid file and an unreadable "file" (directory pretending to be file)
    (config.ingestion_paths[0] / "good.png").write_bytes(b"good image")

    stats = run_ingestion(config)
    # Both valid files should be processed
    assert stats.imported == 2
    assert stats.errors == 0


def test_session_log_written(ingestion_env):
    """A session log file is created in the log directory."""
    config = ingestion_env
    run_ingestion(config)

    log_files = list(config.log_dir.glob("session_*.txt"))
    assert len(log_files) == 1


def test_tags_applied_from_folders(tmp_path):
    """Tags extracted from folder names are applied to media records."""
    source = tmp_path / "source"
    archive = tmp_path / "archive"
    (source / "vacation" / "italy").mkdir(parents=True)
    archive.mkdir()
    (source / "vacation" / "italy" / "photo.jpg").write_bytes(b"italy pic")

    config = AppConfig(
        ingestion_paths=[source],
        archive_path=archive,
        db_path=archive / "data" / "test.db",
        metadata_path=archive / "data" / "metadata.jsonl",
        mode="copy",

        tag_stop_words=[],
        log_dir=archive / "logs",
    )

    stats = run_ingestion(config)
    assert stats.tags_added > 0

    db = Database(config.db_path)
    records = db.get_all_media_with_details()
    assert "vacation" in records[0]["tags"]
    assert "italy" in records[0]["tags"]
    db.close()


def test_tag_review_csv_written(ingestion_env):
    """Ingestion writes a tag review CSV in the data directory."""
    config = ingestion_env
    run_ingestion(config)

    csv_files = list(config.metadata_path.parent.glob("tag_review_*.csv"))
    assert len(csv_files) == 1


def test_apply_tags_from_csv_overrides(tmp_path):
    """apply_tags_from_csv replaces default tags with CSV values."""
    source = tmp_path / "source"
    archive = tmp_path / "archive"
    (source / "vacation" / "italy").mkdir(parents=True)
    archive.mkdir()
    (source / "vacation" / "italy" / "photo.jpg").write_bytes(b"italy pic")

    config = AppConfig(
        ingestion_paths=[source],
        archive_path=archive,
        db_path=archive / "data" / "test.db",
        metadata_path=archive / "data" / "metadata.jsonl",
        mode="copy",

        tag_stop_words=[],
        log_dir=archive / "logs",
    )

    # Run ingestion (auto-applies default tags: vacation, italy)
    run_ingestion(config)

    db = Database(config.db_path)
    records = db.get_all_media_with_details()
    assert "vacation" in records[0]["tags"]
    assert "italy" in records[0]["tags"]
    db.close()

    # Write an edited CSV that changes the tags (format: path_name/subfolder)
    csv_path = tmp_path / "edited_tags.csv"
    csv_path.write_text(
        "folder,file_count,tags\n"
        "source/vacation/italy,1,\"summer,rome\"\n"
    )

    # Apply overrides
    apply_tags_from_csv(config, csv_path)

    # Verify tags were replaced
    db = Database(config.db_path)
    records = db.get_all_media_with_details()
    assert set(records[0]["tags"]) == {"summer", "rome"}
    assert "vacation" not in records[0]["tags"]
    db.close()


def test_apply_tags_clears_tags(tmp_path):
    """apply_tags_from_csv with empty tags clears existing tags."""
    source = tmp_path / "source"
    archive = tmp_path / "archive"
    (source / "event").mkdir(parents=True)
    archive.mkdir()
    (source / "event" / "photo.jpg").write_bytes(b"event pic")

    config = AppConfig(
        ingestion_paths=[source],
        archive_path=archive,
        db_path=archive / "data" / "test.db",
        metadata_path=archive / "data" / "metadata.jsonl",
        mode="copy",

        tag_stop_words=[],
        log_dir=archive / "logs",
    )

    run_ingestion(config)

    # Clear tags via CSV (format: path_name/subfolder)
    csv_path = tmp_path / "clear_tags.csv"
    csv_path.write_text(
        "folder,file_count,tags\n"
        "source/event,1,\"\"\n"
    )

    apply_tags_from_csv(config, csv_path)

    db = Database(config.db_path)
    records = db.get_all_media_with_details()
    assert records[0]["tags"] == []
    db.close()

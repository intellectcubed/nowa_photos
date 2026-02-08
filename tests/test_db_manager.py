"""Tests for nowa_photos.db_manager."""

import pytest
from pathlib import Path

from nowa_photos.db_manager import DBManager


def test_acquire_creates_local_dir(tmp_path):
    """acquire() creates local directory if needed."""
    archive_db = tmp_path / "archive" / "data" / "test.db"
    local_dir = tmp_path / "local"

    manager = DBManager(archive_db, local_dir)
    local_path = manager.acquire()

    assert local_dir.exists()
    assert local_path == local_dir / "test.db"


def test_acquire_copies_existing_archive(tmp_path):
    """acquire() copies existing archive DB to local."""
    archive_dir = tmp_path / "archive" / "data"
    archive_dir.mkdir(parents=True)
    archive_db = archive_dir / "test.db"
    archive_db.write_text("archive data")

    local_dir = tmp_path / "local"
    manager = DBManager(archive_db, local_dir)
    local_path = manager.acquire()

    assert local_path.exists()
    assert local_path.read_text() == "archive data"


def test_acquire_uses_existing_local(tmp_path):
    """acquire() uses existing local DB without overwriting."""
    archive_dir = tmp_path / "archive" / "data"
    archive_dir.mkdir(parents=True)
    archive_db = archive_dir / "test.db"
    archive_db.write_text("archive data")

    local_dir = tmp_path / "local"
    local_dir.mkdir(parents=True)
    local_db = local_dir / "test.db"
    local_db.write_text("local data")

    manager = DBManager(archive_db, local_dir)
    local_path = manager.acquire()

    assert local_path.read_text() == "local data"


def test_release_creates_backup(tmp_path):
    """release() backs up existing archive DB with timestamp."""
    archive_dir = tmp_path / "archive" / "data"
    archive_dir.mkdir(parents=True)
    archive_db = archive_dir / "test.db"
    archive_db.write_text("old archive data")

    local_dir = tmp_path / "local"
    local_dir.mkdir(parents=True)
    local_db = local_dir / "test.db"
    local_db.write_text("new data")

    manager = DBManager(archive_db, local_dir)
    manager._acquired = True
    manager.release()

    # Original archive should be renamed with timestamp
    backups = list(archive_dir.glob("test_*.db"))
    assert len(backups) == 1
    assert backups[0].read_text() == "old archive data"

    # New archive should have the local data
    assert archive_db.read_text() == "new data"

    # Local should be removed
    assert not local_db.exists()


def test_release_without_existing_archive(tmp_path):
    """release() works when no archive exists yet."""
    archive_db = tmp_path / "archive" / "data" / "test.db"
    local_dir = tmp_path / "local"
    local_dir.mkdir(parents=True)
    local_db = local_dir / "test.db"
    local_db.write_text("new data")

    manager = DBManager(archive_db, local_dir)
    manager._acquired = True
    manager.release()

    assert archive_db.read_text() == "new data"
    assert not local_db.exists()


def test_context_manager(tmp_path):
    """DBManager works as context manager."""
    archive_dir = tmp_path / "archive" / "data"
    archive_dir.mkdir(parents=True)
    archive_db = archive_dir / "test.db"
    archive_db.write_text("original")

    local_dir = tmp_path / "local"

    with DBManager(archive_db, local_dir) as local_path:
        assert local_path.exists()
        local_path.write_text("modified")

    assert archive_db.read_text() == "modified"
    assert not (local_dir / "test.db").exists()


def test_context_manager_creates_backup(tmp_path):
    """Context manager creates backup of existing archive."""
    archive_dir = tmp_path / "archive" / "data"
    archive_dir.mkdir(parents=True)
    archive_db = archive_dir / "test.db"
    archive_db.write_text("v1")

    local_dir = tmp_path / "local"

    with DBManager(archive_db, local_dir) as local_path:
        local_path.write_text("v2")

    # Should have backup and new archive
    backups = list(archive_dir.glob("test_*.db"))
    assert len(backups) == 1
    assert backups[0].read_text() == "v1"
    assert archive_db.read_text() == "v2"


def test_release_without_acquire_raises(tmp_path):
    """release() without acquire() raises RuntimeError."""
    archive_db = tmp_path / "archive" / "test.db"
    manager = DBManager(archive_db)

    with pytest.raises(RuntimeError, match="not acquired"):
        manager.release()


def test_default_local_dir(tmp_path, monkeypatch):
    """Default local_dir is ./data."""
    monkeypatch.chdir(tmp_path)
    archive_db = tmp_path / "archive" / "test.db"

    manager = DBManager(archive_db)
    assert manager.local_dir == Path("data")

"""Tests for nowa_photos.database."""

import pytest

from nowa_photos.database import Database


@pytest.fixture
def db(tmp_path):
    d = Database(tmp_path / "test.db")
    yield d
    d.close()


def test_schema_creation(db):
    """Database creates all expected tables."""
    cursor = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = {row[0] for row in cursor.fetchall()}
    assert "media" in tables
    assert "source" in tables
    assert "tags" in tables


def test_insert_and_retrieve(db):
    """Insert a media record and retrieve it by hash."""
    media_id = db.insert_media(
        archive_path="2026/01/IMG_0001.jpg",
        media_type="photo",
        hash_signature="abc123",
        file_size=1024,
        file_date="2026-01-15T14:32:00",
        ingestion_timestamp="2026-01-15T12:00:00",
        exif_date="2026-01-15T14:30:00",
    )
    assert media_id is not None

    record = db.get_media_by_hash("abc123")
    assert record is not None
    assert record["archive_path"] == "2026/01/IMG_0001.jpg"
    assert record["media_type"] == "photo"
    assert record["file_size"] == 1024


def test_duplicate_hash_rejected(db):
    """Inserting a duplicate hash raises an IntegrityError."""
    db.insert_media(
        archive_path="2026/01/a.jpg",
        media_type="photo",
        hash_signature="dup_hash",
        file_size=100,
        file_date="2026-01-01T00:00:00",
        ingestion_timestamp="2026-01-01T00:00:00",
    )
    with pytest.raises(Exception):
        db.insert_media(
            archive_path="2026/01/b.jpg",
            media_type="photo",
            hash_signature="dup_hash",
            file_size=200,
            file_date="2026-01-01T00:00:00",
            ingestion_timestamp="2026-01-01T00:00:00",
        )


def test_nonexistent_hash_returns_none(db):
    """Querying for a hash that doesn't exist returns None."""
    assert db.get_media_by_hash("nonexistent") is None


def test_add_source(db):
    """Adding source paths works and duplicates are ignored."""
    media_id = db.insert_media(
        archive_path="2026/01/a.jpg",
        media_type="photo",
        hash_signature="src_hash",
        file_size=100,
        file_date="2026-01-01T00:00:00",
        ingestion_timestamp="2026-01-01T00:00:00",
    )
    db.add_source(media_id, "/path/to/original.jpg")
    db.add_source(media_id, "/path/to/copy.jpg")
    # Adding same source again should not fail
    db.add_source(media_id, "/path/to/original.jpg")

    sources = db.conn.execute(
        "SELECT source_path FROM source WHERE media_id = ?", (media_id,)
    ).fetchall()
    assert len(sources) == 2


def test_add_tags(db):
    """Adding tags works and duplicates are ignored."""
    media_id = db.insert_media(
        archive_path="2026/01/a.jpg",
        media_type="photo",
        hash_signature="tag_hash",
        file_size=100,
        file_date="2026-01-01T00:00:00",
        ingestion_timestamp="2026-01-01T00:00:00",
    )
    db.add_tags(media_id, ["birthday", "anika"])
    # Adding same tag again should not fail
    db.add_tags(media_id, ["birthday"])

    tags = db.conn.execute(
        "SELECT tag_value FROM tags WHERE media_id = ?", (media_id,)
    ).fetchall()
    assert len(tags) == 2
    assert {t[0] for t in tags} == {"birthday", "anika"}


def test_add_empty_tags(db):
    """Adding an empty tag list is a no-op."""
    media_id = db.insert_media(
        archive_path="2026/01/a.jpg",
        media_type="photo",
        hash_signature="empty_tag",
        file_size=100,
        file_date="2026-01-01T00:00:00",
        ingestion_timestamp="2026-01-01T00:00:00",
    )
    db.add_tags(media_id, [])
    tags = db.conn.execute(
        "SELECT tag_value FROM tags WHERE media_id = ?", (media_id,)
    ).fetchall()
    assert len(tags) == 0


def test_get_all_media_with_details(db):
    """Full denormalized export includes sources and tags."""
    mid = db.insert_media(
        archive_path="2026/01/IMG_0001.jpg",
        media_type="photo",
        hash_signature="full_hash",
        file_size=2048,
        file_date="2026-01-15T14:32:00",
        ingestion_timestamp="2026-01-15T12:00:00",
        exif_date="2026-01-15T14:30:00",
    )
    db.add_source(mid, "/src/a.jpg")
    db.add_source(mid, "/src/b.jpg")
    db.add_tags(mid, ["birthday", "anika"])

    records = db.get_all_media_with_details()
    assert len(records) == 1
    r = records[0]
    assert r["archive_path"] == "2026/01/IMG_0001.jpg"
    assert r["sources"] == ["/src/a.jpg", "/src/b.jpg"]
    assert set(r["tags"]) == {"birthday", "anika"}


def test_transaction_rollback(db):
    """Failed transaction rolls back changes."""
    try:
        with db.transaction():
            db.conn.execute(
                """INSERT INTO media
                   (archive_path, media_type, hash_signature, file_size,
                    file_date, ingestion_timestamp)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ("path", "photo", "rollback_hash", 100, "2026-01-01", "2026-01-01"),
            )
            raise ValueError("force rollback")
    except ValueError:
        pass

    assert db.get_media_by_hash("rollback_hash") is None

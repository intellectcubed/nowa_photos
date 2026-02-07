"""SQLite database for media archive metadata."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS media (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_path        TEXT    NOT NULL,
    archive_filename    TEXT    NOT NULL,
    media_type          TEXT    NOT NULL,
    hash_signature      TEXT    NOT NULL UNIQUE,
    file_size           INTEGER NOT NULL,
    duration            REAL,
    exif_date           TEXT,
    file_date           TEXT    NOT NULL,
    ingestion_timestamp TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_media_hash ON media(hash_signature);

CREATE TABLE IF NOT EXISTS tag (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    value TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS media_tag (
    media_id INTEGER NOT NULL,
    tag_id   INTEGER NOT NULL,
    PRIMARY KEY (media_id, tag_id),
    FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id)   REFERENCES tag(id)   ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_media_tag_media ON media_tag(media_id);
CREATE INDEX IF NOT EXISTS idx_media_tag_tag   ON media_tag(tag_id);

CREATE TABLE IF NOT EXISTS source_item (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path TEXT    NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS media_source (
    media_id        INTEGER NOT NULL,
    source_item_id  INTEGER NOT NULL,
    source_filename TEXT    NOT NULL,
    PRIMARY KEY (media_id, source_item_id, source_filename),
    FOREIGN KEY (media_id)       REFERENCES media(id)       ON DELETE CASCADE,
    FOREIGN KEY (source_item_id) REFERENCES source_item(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_media_source_media  ON media_source(media_id);
CREATE INDEX IF NOT EXISTS idx_media_source_source ON media_source(source_item_id);
"""


class Database:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    @contextmanager
    def transaction(self):
        """Context manager for safe commit/rollback."""
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def insert_media(
        self,
        archive_path: str,
        archive_filename: str,
        media_type: str,
        hash_signature: str,
        file_size: int,
        file_date: str,
        ingestion_timestamp: str,
        exif_date: str | None = None,
        duration: float | None = None,
    ) -> int:
        """Insert a media record. Returns the new row id."""
        with self.transaction():
            cursor = self.conn.execute(
                """INSERT INTO media
                   (archive_path, archive_filename, media_type, hash_signature,
                    file_size, duration, exif_date, file_date, ingestion_timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (archive_path, archive_filename, media_type, hash_signature,
                 file_size, duration, exif_date, file_date, ingestion_timestamp),
            )
            return cursor.lastrowid

    def get_media_by_hash(self, hash_signature: str) -> dict | None:
        """Return a media record as a dict, or None if not found."""
        cursor = self.conn.execute(
            "SELECT * FROM media WHERE hash_signature = ?",
            (hash_signature,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in cursor.description]
        return dict(zip(cols, row))

    def _get_or_create_source_item(self, source_path: str) -> int:
        """Get existing source_item id or create new one. Returns the id."""
        cursor = self.conn.execute(
            "SELECT id FROM source_item WHERE source_path = ?",
            (source_path,),
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor = self.conn.execute(
            "INSERT INTO source_item (source_path) VALUES (?)",
            (source_path,),
        )
        return cursor.lastrowid

    def add_source(self, media_id: int, source_path: str, source_filename: str) -> None:
        """Add a source for a media record. Ignores duplicates."""
        with self.transaction():
            source_item_id = self._get_or_create_source_item(source_path)
            self.conn.execute(
                """INSERT OR IGNORE INTO media_source
                   (media_id, source_item_id, source_filename)
                   VALUES (?, ?, ?)""",
                (media_id, source_item_id, source_filename),
            )

    def _get_or_create_tag(self, tag_value: str) -> int:
        """Get existing tag id or create new one. Returns the id."""
        cursor = self.conn.execute(
            "SELECT id FROM tag WHERE value = ?",
            (tag_value,),
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor = self.conn.execute(
            "INSERT INTO tag (value) VALUES (?)",
            (tag_value,),
        )
        return cursor.lastrowid

    def add_tags(self, media_id: int, tags: list[str]) -> None:
        """Add tags for a media record. Ignores duplicates."""
        if not tags:
            return
        with self.transaction():
            for tag_value in tags:
                tag_id = self._get_or_create_tag(tag_value)
                self.conn.execute(
                    "INSERT OR IGNORE INTO media_tag (media_id, tag_id) VALUES (?, ?)",
                    (media_id, tag_id),
                )

    def replace_tags(self, media_id: int, tags: list[str]) -> None:
        """Replace all tags for a media record (delete + insert)."""
        with self.transaction():
            self.conn.execute(
                "DELETE FROM media_tag WHERE media_id = ?",
                (media_id,),
            )
            for tag_value in tags:
                tag_id = self._get_or_create_tag(tag_value)
                self.conn.execute(
                    "INSERT INTO media_tag (media_id, tag_id) VALUES (?, ?)",
                    (media_id, tag_id),
                )

    def get_tags_for_media(self, media_id: int) -> list[str]:
        """Get all tag values for a media record."""
        rows = self.conn.execute(
            """SELECT t.value FROM tag t
               JOIN media_tag mt ON t.id = mt.tag_id
               WHERE mt.media_id = ?
               ORDER BY t.id""",
            (media_id,),
        ).fetchall()
        return [r[0] for r in rows]

    def get_media_ids_by_source_folder(
        self, ingestion_path: str, folder: str,
    ) -> list[int]:
        """Return distinct media_ids whose source is within the given folder.

        Matches source_item.source_path that equals or starts with the folder path.
        For root-level files (folder is '.'), matches the ingestion_path exactly.
        """
        if folder == ".":
            # Files directly in ingestion_path
            rows = self.conn.execute(
                """SELECT DISTINCT ms.media_id FROM media_source ms
                   JOIN source_item si ON ms.source_item_id = si.id
                   WHERE si.source_path = ?""",
                (ingestion_path.rstrip("/"),),
            ).fetchall()
        else:
            folder_path = ingestion_path.rstrip("/") + "/" + folder
            rows = self.conn.execute(
                """SELECT DISTINCT ms.media_id FROM media_source ms
                   JOIN source_item si ON ms.source_item_id = si.id
                   WHERE si.source_path = ?""",
                (folder_path,),
            ).fetchall()
        return [r[0] for r in rows]

    def get_all_media_with_details(self) -> list[dict]:
        """Return all media records with their sources and tags (denormalized)."""
        cursor = self.conn.execute(
            "SELECT * FROM media ORDER BY id"
        )
        cols = [desc[0] for desc in cursor.description]
        media_rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        for record in media_rows:
            mid = record["id"]

            # Get sources (full paths reconstructed)
            sources = self.conn.execute(
                """SELECT si.source_path, ms.source_filename
                   FROM media_source ms
                   JOIN source_item si ON ms.source_item_id = si.id
                   WHERE ms.media_id = ?
                   ORDER BY ms.source_item_id""",
                (mid,),
            ).fetchall()
            record["sources"] = [f"{s[0]}/{s[1]}" for s in sources]

            # Get tags
            tags = self.conn.execute(
                """SELECT t.value FROM tag t
                   JOIN media_tag mt ON t.id = mt.tag_id
                   WHERE mt.media_id = ?
                   ORDER BY t.id""",
                (mid,),
            ).fetchall()
            record["tags"] = [t[0] for t in tags]

        return media_rows

    def close(self):
        self.conn.close()

"""SQLite database for media archive metadata."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS media (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_path    TEXT    NOT NULL,
    media_type      TEXT    NOT NULL,
    hash_signature  TEXT    NOT NULL UNIQUE,
    file_size       INTEGER NOT NULL,
    duration        REAL,
    exif_date       TEXT,
    file_date       TEXT    NOT NULL,
    ingestion_timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id    INTEGER NOT NULL,
    source_path TEXT    NOT NULL,
    FOREIGN KEY (media_id) REFERENCES media(id),
    UNIQUE(media_id, source_path)
);

CREATE TABLE IF NOT EXISTS tags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id    INTEGER NOT NULL,
    tag_value   TEXT    NOT NULL,
    FOREIGN KEY (media_id) REFERENCES media(id),
    UNIQUE(media_id, tag_value)
);

CREATE INDEX IF NOT EXISTS idx_media_hash ON media(hash_signature);
CREATE INDEX IF NOT EXISTS idx_source_media ON source(media_id);
CREATE INDEX IF NOT EXISTS idx_tags_media ON tags(media_id);
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
                   (archive_path, media_type, hash_signature, file_size,
                    duration, exif_date, file_date, ingestion_timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (archive_path, media_type, hash_signature, file_size,
                 duration, exif_date, file_date, ingestion_timestamp),
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

    def add_source(self, media_id: int, source_path: str) -> None:
        """Add a source path for a media record. Ignores duplicates."""
        with self.transaction():
            self.conn.execute(
                "INSERT OR IGNORE INTO source (media_id, source_path) VALUES (?, ?)",
                (media_id, source_path),
            )

    def add_tags(self, media_id: int, tags: list[str]) -> None:
        """Add tags for a media record. Ignores duplicates."""
        if not tags:
            return
        with self.transaction():
            self.conn.executemany(
                "INSERT OR IGNORE INTO tags (media_id, tag_value) VALUES (?, ?)",
                [(media_id, tag) for tag in tags],
            )

    def replace_tags(self, media_id: int, tags: list[str]) -> None:
        """Replace all tags for a media record (delete + insert)."""
        with self.transaction():
            self.conn.execute(
                "DELETE FROM tags WHERE media_id = ?",
                (media_id,),
            )
            if tags:
                self.conn.executemany(
                    "INSERT INTO tags (media_id, tag_value) VALUES (?, ?)",
                    [(media_id, tag) for tag in tags],
                )

    def get_media_ids_by_source_folder(
        self, ingestion_path: str, folder: str,
    ) -> list[int]:
        """Return distinct media_ids whose source_path is within the given folder.

        Matches source paths that start with '{ingestion_path}/{folder}/'.
        For root-level files (folder is '.'), matches paths directly under ingestion_path.
        """
        if folder == ".":
            prefix = ingestion_path.rstrip("/") + "/"
            # Match files directly in ingestion_path (no further '/' after prefix)
            rows = self.conn.execute(
                """SELECT DISTINCT media_id FROM source
                   WHERE source_path LIKE ? AND source_path NOT LIKE ?""",
                (prefix + "%", prefix + "%/%"),
            ).fetchall()
        else:
            prefix = ingestion_path.rstrip("/") + "/" + folder + "/"
            rows = self.conn.execute(
                "SELECT DISTINCT media_id FROM source WHERE source_path LIKE ?",
                (prefix + "%",),
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

            sources = self.conn.execute(
                "SELECT source_path FROM source WHERE media_id = ? ORDER BY id",
                (mid,),
            ).fetchall()
            record["sources"] = [s[0] for s in sources]

            tags = self.conn.execute(
                "SELECT tag_value FROM tags WHERE media_id = ? ORDER BY id",
                (mid,),
            ).fetchall()
            record["tags"] = [t[0] for t in tags]

        return media_rows

    def close(self):
        self.conn.close()

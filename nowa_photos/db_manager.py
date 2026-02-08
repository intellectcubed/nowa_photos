"""Database file management for local working copy and archive."""

import shutil
from datetime import datetime
from pathlib import Path


class DBManager:
    """Manages database file lifecycle: local working copy and archive.

    Workflow:
    1. acquire() - Copy archive DB to local (if exists), return local path
    2. Use the local DB for all operations
    3. release() - Backup archive DB with timestamp, move local to archive, cleanup
    """

    def __init__(self, archive_db_path: Path, local_dir: Path | None = None):
        """Initialize the DB manager.

        Args:
            archive_db_path: Path to the database in the archive (final destination)
            local_dir: Directory for local working copy (defaults to ./data)
        """
        self.archive_db_path = Path(archive_db_path)
        self.local_dir = Path(local_dir) if local_dir else Path("data")
        self.local_db_path = self.local_dir / self.archive_db_path.name
        self._acquired = False

    def acquire(self) -> Path:
        """Prepare local working copy and return its path.

        If archive DB exists, copies it to local for continuation.
        Creates local directory if needed.

        Returns:
            Path to the local database file (may not exist yet if new)
        """
        self.local_dir.mkdir(parents=True, exist_ok=True)

        # If archive exists and local doesn't, copy for continuity
        if self.archive_db_path.exists() and not self.local_db_path.exists():
            shutil.copy2(str(self.archive_db_path), str(self.local_db_path))
            print(f"Copied archive DB to local: {self.local_db_path}")
        elif self.local_db_path.exists():
            print(f"Using existing local DB: {self.local_db_path}")
        else:
            print(f"Creating new database: {self.local_db_path}")

        self._acquired = True
        return self.local_db_path

    def release(self) -> None:
        """Archive the local DB and cleanup.

        1. Renames existing archive DB with timestamp (backup)
        2. Moves local DB to archive location
        3. Removes local copy
        """
        if not self._acquired:
            raise RuntimeError("Cannot release DB that was not acquired")

        if not self.local_db_path.exists():
            print("No local database to archive.")
            self._acquired = False
            return

        # Ensure archive directory exists
        self.archive_db_path.parent.mkdir(parents=True, exist_ok=True)

        # Backup existing archive DB with timestamp
        if self.archive_db_path.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            stem = self.archive_db_path.stem
            suffix = self.archive_db_path.suffix
            backup_name = f"{stem}_{timestamp}{suffix}"
            backup_path = self.archive_db_path.parent / backup_name
            shutil.move(str(self.archive_db_path), str(backup_path))
            print(f"Backed up previous DB: {backup_path}")

        # Move local to archive
        shutil.move(str(self.local_db_path), str(self.archive_db_path))
        print(f"Database archived to {self.archive_db_path}")

        self._acquired = False

    def __enter__(self) -> Path:
        """Context manager entry - acquire and return local path."""
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - release (archive and cleanup)."""
        if self._acquired:
            self.release()
        return False

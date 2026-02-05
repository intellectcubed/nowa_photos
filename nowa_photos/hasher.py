"""SHA-256 file hashing for deduplication."""

import hashlib
from pathlib import Path

CHUNK_SIZE = 65536  # 64 KB


def hash_file(path: Path | str) -> str:
    """Return the SHA-256 hex digest of a file, read in 64 KB chunks."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            sha.update(chunk)
    return sha.hexdigest()

"""Tests for nowa_photos.hasher."""

import hashlib
import tempfile
from pathlib import Path

import pytest

from nowa_photos.hasher import hash_file


def test_known_content_hash(tmp_path):
    """Hash of known content matches expected SHA-256."""
    content = b"hello world"
    expected = hashlib.sha256(content).hexdigest()
    f = tmp_path / "hello.txt"
    f.write_bytes(content)
    assert hash_file(f) == expected


def test_empty_file(tmp_path):
    """Hash of an empty file matches SHA-256 of empty bytes."""
    expected = hashlib.sha256(b"").hexdigest()
    f = tmp_path / "empty.bin"
    f.write_bytes(b"")
    assert hash_file(f) == expected


def test_large_file(tmp_path):
    """Hash of a file larger than the chunk size is correct."""
    # 200 KB > 64 KB chunk size
    content = b"x" * 200_000
    expected = hashlib.sha256(content).hexdigest()
    f = tmp_path / "large.bin"
    f.write_bytes(content)
    assert hash_file(f) == expected


def test_missing_file(tmp_path):
    """Hashing a nonexistent file raises an error."""
    with pytest.raises(FileNotFoundError):
        hash_file(tmp_path / "nonexistent.bin")


def test_same_content_same_hash(tmp_path):
    """Two files with identical content produce the same hash."""
    content = b"duplicate content"
    f1 = tmp_path / "a.bin"
    f2 = tmp_path / "b.bin"
    f1.write_bytes(content)
    f2.write_bytes(content)
    assert hash_file(f1) == hash_file(f2)


def test_different_content_different_hash(tmp_path):
    """Two files with different content produce different hashes."""
    f1 = tmp_path / "a.bin"
    f2 = tmp_path / "b.bin"
    f1.write_bytes(b"content A")
    f2.write_bytes(b"content B")
    assert hash_file(f1) != hash_file(f2)


def test_accepts_string_path(tmp_path):
    """hash_file accepts a string path in addition to Path."""
    content = b"string path test"
    f = tmp_path / "str.bin"
    f.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert hash_file(str(f)) == expected

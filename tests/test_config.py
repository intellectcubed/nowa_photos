"""Tests for nowa_photos.config."""

from pathlib import Path

import pytest

from nowa_photos.config import build_config, _validate_and_resolve, _load_yaml


@pytest.fixture
def valid_config_file(tmp_path):
    """Create a minimal valid config file."""
    config = tmp_path / "config.yaml"
    config.write_text(
        f'ingestion_path: "{tmp_path / "source"}"\n'
        f'archive_path: "{tmp_path / "archive"}"\n'
    )
    (tmp_path / "source").mkdir()
    (tmp_path / "archive").mkdir()
    return config


def test_valid_config(valid_config_file, tmp_path):
    """Valid config file loads correctly."""
    cfg = build_config(["--config", str(valid_config_file)])
    assert cfg.ingestion_path == (tmp_path / "source").resolve()
    assert cfg.archive_path == (tmp_path / "archive").resolve()
    assert cfg.mode == "copy"
    assert cfg.enable_tag_prompts is True


def test_missing_ingestion_path():
    """Missing ingestion_path raises ValueError."""
    with pytest.raises(ValueError, match="ingestion_path"):
        _validate_and_resolve({"archive_path": "/tmp/archive"})


def test_missing_archive_path():
    """Missing archive_path raises ValueError."""
    with pytest.raises(ValueError, match="archive_path"):
        _validate_and_resolve({"ingestion_path": "/tmp/source"})


def test_invalid_mode():
    """Invalid mode raises ValueError."""
    with pytest.raises(ValueError, match="mode"):
        _validate_and_resolve({
            "ingestion_path": "/tmp/source",
            "archive_path": "/tmp/archive",
            "mode": "delete",
        })


def test_relative_path_resolution():
    """Relative db_path and metadata_path are resolved relative to archive_path."""
    cfg = _validate_and_resolve({
        "ingestion_path": "/tmp/source",
        "archive_path": "/tmp/archive",
        "db_path": "data/nowa_photos.db",
        "metadata_path": "data/metadata.jsonl",
        "log_dir": "logs",
    })
    archive = Path("/tmp/archive").resolve()
    assert cfg.db_path == archive / "data" / "nowa_photos.db"
    assert cfg.metadata_path == archive / "data" / "metadata.jsonl"
    assert cfg.log_dir == archive / "logs"


def test_absolute_path_not_rebased():
    """Absolute db_path is not rebased to archive_path."""
    cfg = _validate_and_resolve({
        "ingestion_path": "/tmp/source",
        "archive_path": "/tmp/archive",
        "db_path": "/custom/db.sqlite",
    })
    assert cfg.db_path == Path("/custom/db.sqlite")


def test_defaults_applied():
    """Default values are applied for optional fields."""
    cfg = _validate_and_resolve({
        "ingestion_path": "/tmp/source",
        "archive_path": "/tmp/archive",
    })
    assert cfg.mode == "copy"
    assert cfg.enable_tag_prompts is True
    assert len(cfg.tag_stop_words) > 0
    assert cfg.db_path == Path("/tmp/archive").resolve() / "data" / "nowa_photos.db"


def test_custom_tag_stop_words():
    """Custom tag_stop_words override defaults."""
    cfg = _validate_and_resolve({
        "ingestion_path": "/tmp/source",
        "archive_path": "/tmp/archive",
        "tag_stop_words": ["custom", "words"],
    })
    assert cfg.tag_stop_words == ["custom", "words"]


def test_config_file_not_found():
    """Non-existent config file causes exit."""
    with pytest.raises(SystemExit):
        build_config(["--config", "/nonexistent/config.yaml"])


def test_missing_config_arg():
    """Missing --config argument causes exit."""
    with pytest.raises(SystemExit):
        build_config([])

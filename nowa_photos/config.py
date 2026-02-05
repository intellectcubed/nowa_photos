"""Configuration loading and validation for Nowa Photos."""

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml


DEFAULT_TAG_STOP_WORDS = [
    "backup", "photos", "images", "media", "camera",
    "dcim", "export", "downloads", "documents",
]


@dataclass
class AppConfig:
    ingestion_path: Path
    archive_path: Path
    db_path: Path
    metadata_path: Path
    mode: str = "copy"
    tag_stop_words: list[str] = field(default_factory=lambda: list(DEFAULT_TAG_STOP_WORDS))
    log_dir: Path = Path("logs")


def _parse_cli_args(args: list[str] | None = None) -> argparse.Namespace:
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
    return parser.parse_args(args)


def _load_yaml(path: Path) -> dict:
    with open(path) as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping, got {type(data).__name__}")
    return data


def _validate_and_resolve(data: dict) -> AppConfig:
    # Required fields
    for key in ("ingestion_path", "archive_path"):
        if key not in data:
            raise ValueError(f"Missing required config field: {key}")

    ingestion_path = Path(data["ingestion_path"]).expanduser().resolve()
    archive_path = Path(data["archive_path"]).expanduser().resolve()

    # Resolve paths relative to archive_path
    db_path_raw = data.get("db_path", "data/nowa_photos.db")
    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        db_path = archive_path / db_path

    metadata_path_raw = data.get("metadata_path", "data/metadata.jsonl")
    metadata_path = Path(metadata_path_raw)
    if not metadata_path.is_absolute():
        metadata_path = archive_path / metadata_path

    log_dir_raw = data.get("log_dir", "logs")
    log_dir = Path(log_dir_raw)
    if not log_dir.is_absolute():
        log_dir = archive_path / log_dir

    mode = data.get("mode", "copy")
    if mode not in ("copy", "move"):
        raise ValueError(f"mode must be 'copy' or 'move', got '{mode}'")

    tag_stop_words = data.get("tag_stop_words", list(DEFAULT_TAG_STOP_WORDS))

    return AppConfig(
        ingestion_path=ingestion_path,
        archive_path=archive_path,
        db_path=db_path,
        metadata_path=metadata_path,
        mode=mode,
        tag_stop_words=tag_stop_words,
        log_dir=log_dir,
    )


def build_config(cli_args: list[str] | None = None) -> AppConfig:
    """Parse CLI args, load YAML config, validate, and return AppConfig."""
    ns = _parse_cli_args(cli_args)
    config_path = ns.config.expanduser().resolve()
    if not config_path.exists():
        print(f"Error: config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    data = _load_yaml(config_path)
    return _validate_and_resolve(data)

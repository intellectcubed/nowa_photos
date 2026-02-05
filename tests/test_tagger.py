"""Tests for nowa_photos.tagger."""

from pathlib import Path

from nowa_photos.tagger import (
    extract_tags_from_path,
    _clean_tag,
    extract_folder_tags,
    write_tag_review_csv,
    load_tag_review_csv,
)


def test_basic_tag_extraction():
    """Directory components become tags."""
    tags = extract_tags_from_path(
        "/source/birthday/anika/IMG_001.jpg",
        "/source",
    )
    assert tags == ["birthday", "anika"]


def test_single_directory():
    """Single directory component becomes one tag."""
    tags = extract_tags_from_path(
        "/source/vacation/IMG_001.jpg",
        "/source",
    )
    assert tags == ["vacation"]


def test_stop_word_filtering():
    """Stop words are filtered out."""
    tags = extract_tags_from_path(
        "/source/backup/photos/birthday/IMG_001.jpg",
        "/source",
        stop_words=["backup", "photos"],
    )
    assert tags == ["birthday"]


def test_file_in_root():
    """File directly in base_path produces no tags."""
    tags = extract_tags_from_path(
        "/source/IMG_001.jpg",
        "/source",
    )
    assert tags == []


def test_clean_tag_strips_special_chars():
    """Tags are cleaned of special characters."""
    assert _clean_tag("Birthday Party!") == "birthdayparty"
    assert _clean_tag("  Hello World  ") == "helloworld"
    assert _clean_tag("bday-anika") == "bday-anika"
    assert _clean_tag("my_tag") == "my_tag"


def test_clean_tag_lowercases():
    """Tags are lowercased."""
    assert _clean_tag("UPPERCASE") == "uppercase"
    assert _clean_tag("MiXeD") == "mixed"


def test_clean_tag_empty():
    """Empty or whitespace-only strings clean to empty."""
    assert _clean_tag("") == ""
    assert _clean_tag("   ") == ""


def test_stop_words_case_insensitive():
    """Stop words are matched case-insensitively."""
    tags = extract_tags_from_path(
        "/source/BACKUP/birthday/IMG_001.jpg",
        "/source",
        stop_words=["backup"],
    )
    assert tags == ["birthday"]


def test_extract_folder_tags():
    """extract_folder_tags returns auto-extracted tags per folder."""
    base = Path("/source")
    files_by_folder = {
        "vacation": [Path("/source/vacation/IMG_001.jpg")],
        "birthday/anika": [
            Path("/source/birthday/anika/IMG_001.jpg"),
            Path("/source/birthday/anika/IMG_002.jpg"),
        ],
    }
    result = extract_folder_tags(files_by_folder, base, stop_words=[])
    assert result["vacation"] == ["vacation"]
    assert result["birthday/anika"] == ["birthday", "anika"]


def test_extract_folder_tags_empty():
    """Folders at root level produce no tags."""
    base = Path("/source")
    files_by_folder = {
        ".": [Path("/source/IMG_001.jpg")],
    }
    result = extract_folder_tags(files_by_folder, base, stop_words=[])
    assert result["."] == []


# --- CSV round-trip tests ---

def test_write_and_load_csv(tmp_path):
    """CSV round-trip preserves folder-tag mappings."""
    folder_tags = {
        "vacation/italy": ["vacation", "italy"],
        "birthday": ["birthday"],
        "root_files": [],
    }
    file_counts = {
        "vacation/italy": 12,
        "birthday": 5,
        "root_files": 3,
    }
    csv_path = tmp_path / "review.csv"

    write_tag_review_csv(folder_tags, file_counts, csv_path)
    loaded = load_tag_review_csv(csv_path)

    assert loaded["vacation/italy"] == ["vacation", "italy"]
    assert loaded["birthday"] == ["birthday"]
    assert loaded["root_files"] == []


def test_csv_with_edited_tags(tmp_path):
    """Loading a CSV with user-edited tags works correctly."""
    csv_path = tmp_path / "edited.csv"
    csv_path.write_text(
        "folder,file_count,tags\n"
        "vacation/italy,12,\"vacation,italy,summer\"\n"
        "birthday,5,\"\"\n"
    )
    loaded = load_tag_review_csv(csv_path)
    assert loaded["vacation/italy"] == ["vacation", "italy", "summer"]
    assert loaded["birthday"] == []


def test_csv_cleans_tags(tmp_path):
    """Tags loaded from CSV are cleaned/normalized."""
    csv_path = tmp_path / "dirty.csv"
    csv_path.write_text(
        "folder,file_count,tags\n"
        "photos,10,\"My Tag, UPPER, bday-party\"\n"
    )
    loaded = load_tag_review_csv(csv_path)
    assert loaded["photos"] == ["mytag", "upper", "bday-party"]


def test_csv_creates_parent_dirs(tmp_path):
    """write_tag_review_csv creates parent directories."""
    csv_path = tmp_path / "nested" / "dir" / "review.csv"
    write_tag_review_csv({"a": ["tag"]}, {"a": 1}, csv_path)
    assert csv_path.exists()

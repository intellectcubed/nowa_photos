"""Tests for nowa_photos.tagger."""

from pathlib import Path

from nowa_photos.tagger import extract_tags_from_path, _clean_tag, batch_prompt_folders


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


def test_batch_prompt_no_prompts():
    """batch_prompt_folders with prompts disabled uses auto-extracted tags."""
    base = Path("/source")
    files_by_folder = {
        "vacation": [Path("/source/vacation/IMG_001.jpg")],
        "birthday/anika": [
            Path("/source/birthday/anika/IMG_001.jpg"),
            Path("/source/birthday/anika/IMG_002.jpg"),
        ],
    }
    result = batch_prompt_folders(
        files_by_folder, base, stop_words=[], enable_prompts=False,
    )
    assert result["vacation"] == ["vacation"]
    assert result["birthday/anika"] == ["birthday", "anika"]


def test_batch_prompt_empty_folder_tags():
    """Folders at root level produce no tags."""
    base = Path("/source")
    files_by_folder = {
        ".": [Path("/source/IMG_001.jpg")],
    }
    result = batch_prompt_folders(
        files_by_folder, base, stop_words=[], enable_prompts=False,
    )
    assert result["."] == []

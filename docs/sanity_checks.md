# Sanity Check Scripts

Tools for verifying the integrity of the media archive and database.

All commands assume you are in the project root directory with the virtualenv activated.

---

## sanity_check.py

Counts files by extension in a directory tree. Useful as a quick overview of what's in a folder before or after ingestion. Handles macOS library bundles (`.photoslibrary`, `.aplibrary`, etc.) by traversing inside them.

```
python nowa_photos/sanity_check.py <root_folder>
```

**Options:**

| Flag | Description |
|------|-------------|
| `--include-hidden` | Include hidden files and directories (starting with `.`) |
| `--show-bundles` | List macOS library bundles found |

**Example:**

```
python nowa_photos/sanity_check.py /Volumes/Photos
python nowa_photos/sanity_check.py /Volumes/Photos --include-hidden --show-bundles
```

---

## deep_sanity_check.py

Compares database records with physical files on disk using **path matching**. Builds a set of relative paths from the archive folder and a set of paths from the database, then reports the differences.

```
python nowa_photos/deep_sanity_check.py <archive_folder> <database_path>
```

**Options:**

| Flag | Description |
|------|-------------|
| `--log <logfile>` | Write output to a log file |

**Output markers:**

- `>>` — in database but not on disk (missing files)
- `<<` — on disk but not in database (untracked files)

**Example:**

```
python nowa_photos/deep_sanity_check.py /Volumes/Photos/archive /data/nowa.db --log logs/deep.log
```

**Note:** This script compares by file path only. If files have been moved or renamed within the archive, it will report mismatches even though the files still exist. Use the hash-based scripts below for more reliable comparison.

---

## deep_sanity_check2.py

Compares files by **SHA-256 hash** rather than path. Hashes every file on disk and checks if the hash exists in the database. Detects moved/renamed files and reports path mismatches.

```
python nowa_photos/deep_sanity_check2.py <archive_folder> <database_path>
```

**Options:**

| Flag | Description |
|------|-------------|
| `--log <logfile>` | Write output to a log file |

**Output markers:**

- `<<` — file on disk whose hash is not in the database
- `~~` — hash matches but file path differs from what the database expects
- `>>` — database record with no matching file on disk

**Example:**

```
python nowa_photos/deep_sanity_check2.py /Volumes/Photos/archive /data/nowa.db --log logs/deep2.log
```

**Note:** This script is single-threaded and can be very slow on large archives since it hashes every file sequentially. Use `deep_sanity_check_mp.py` for faster execution.

---

## deep_sanity_check_mp.py

Multiprocess version of `deep_sanity_check2.py`. Same hash-based comparison but hashes files in parallel using multiple worker processes. Includes real-time progress display and retry with exponential backoff on transient file read errors.

```
python nowa_photos/deep_sanity_check_mp.py <archive_folder> <database_path>
```

**Options:**

| Flag | Description |
|------|-------------|
| `--workers <N>` | Number of worker processes (default: 8) |
| `--log <logfile>` | Write output to a log file |

**Example:**

```
python nowa_photos/deep_sanity_check_mp.py /Volumes/Photos/archive /data/nowa.db --workers 4 --log logs/deep_mp.log
```

---

## deep_sanity_check_mp2.py

Hashes all media files in a directory and writes the results to a CSV log. No database involved — purely a file hashing tool. Useful for generating a hash manifest or debugging hash issues independently of the database.

```
python nowa_photos/deep_sanity_check_mp2.py <folder> <output_log>
```

**Options:**

| Flag | Description |
|------|-------------|
| `--workers <N>` | Number of worker processes (default: 8) |

**Output format** (one line per file):

```
2024-01/photo.jpg,a3f2b1c9e4...
2024-01/video.mp4,ERROR: FileNotFoundError after 6 attempts
```

**Example:**

```
python nowa_photos/deep_sanity_check_mp2.py /Volumes/Photos/archive logs/hashes.csv --workers 4
```

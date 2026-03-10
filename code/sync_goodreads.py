#!/usr/bin/env python
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unicodedata
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: pandas. Install requirements first.") from exc

try:
    import requests
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: requests. Install requirements first.") from exc

try:
    from ddgs import DDGS
    from ddgs.exceptions import DDGSException
except ImportError:  # pragma: no cover
    print("Warning: ddgs not found, DuckDuckGo image search will be unavailable. Install requirements first for full functionality.")
    DDGS = None

    class DDGSException(Exception):
        """Provide the state and behavior for DDGSException."""
        pass

try:
    import frontmatter
except ImportError:  # pragma: no cover
    frontmatter = None

try:
    from langdetect import DetectorFactory, LangDetectException, detect_langs

    DetectorFactory.seed = 0
except ImportError:  # pragma: no cover
    LangDetectException = Exception
    detect_langs = None

import yaml


OPEN_LIBRARY_SEARCH_API = "https://openlibrary.org/search.json"
GOOGLE_BOOKS_API = "https://www.googleapis.com/books/v1/volumes"
WIKIMEDIA_COMMONS_API = "https://commons.wikimedia.org/w/api.php"
REVIEW_NOTE_NAME = "Missing Metadata.md"
EXPECTED_COLUMNS = [
    "Book Id",
    "Title",
    "Author",
    "Author l-f",
    "Additional Authors",
    "ISBN",
    "ISBN13",
    "My Rating",
    "Average Rating",
    "Publisher",
    "Binding",
    "Number of Pages",
    "Year Published",
    "Original Publication Year",
    "Date Read",
    "Date Added",
    "Bookshelves",
    "Bookshelves with positions",
    "Exclusive Shelf",
    "My Review",
    "Spoiler",
    "Private Notes",
    "Read Count",
    "Owned Copies",
]
BOOK_FRONTMATTER_KEYS = [
    "title",
    "author",
    "translator",
    "status",
    "rating",
    "read_count",
    "date_added",
    "date_read",
    "language",
    "publisher",
    "original_publish_year",
    "isbn",
    "isbn13",
    "pages",
    "format",
    "cover",
    "bookshelves",
    "reread_dates",
    "tags",
]
AUTHOR_FRONTMATTER_KEYS = ["name", "cover", "country", "birth_year", "death_year", "sex", "tags"]
GENERATED_MARKERS = {
    "book_header": ("<!-- GENERATED:BOOK_HEADER START -->", "<!-- GENERATED:BOOK_HEADER END -->"),
    "book_quotes": ("<!-- GENERATED:BOOK_QUOTES START -->", "<!-- GENERATED:BOOK_QUOTES END -->"),
    "book_review": ("<!-- GENERATED:BOOK_REVIEW START -->", "<!-- GENERATED:BOOK_REVIEW END -->"),
    "author_header": ("<!-- GENERATED:AUTHOR_HEADER START -->", "<!-- GENERATED:AUTHOR_HEADER END -->"),
    "author_bio": ("<!-- GENERATED:AUTHOR_BIO START -->", "<!-- GENERATED:AUTHOR_BIO END -->"),
    "author_books": ("<!-- GENERATED:AUTHOR_BOOKS START -->", "<!-- GENERATED:AUTHOR_BOOKS END -->"),
}
MANUAL_REVIEW_SECTIONS = [
    "Missing Covers",
    "Failed Author Biographies",
    "Broken Book Materialization",
    "Missing Bookshelves",
    "Missing ISBN / ISBN13",
    "Missing Authors",
    "API Errors",
    "Parse Issues",
]
MOJIBAKE_MARKERS = ("Ãƒ", "Ã‚", "Ã¢", "Ã°", "Ã", "Ã‘", "ï¿½")
GENERATED_HUB_NOTE_NAMES = ("Library.md",)
CODEX_TIMEOUT_SECONDS = 90
CODEX_MODEL = "gpt-5.1"
CODEX_REASONING_EFFORT = "low"
AUTHOR_BIO_CONCURRENCY = 20
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 GoodreadsToObsidian/1.0"
)
HTTP_ACCEPT_HEADER = (
    "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,*/*;q=0.8"
)
IMAGE_PROVIDER_MIN_INTERVALS = {"google_books": 1.0, "wikimedia_commons": 1.0}
IMAGE_PROVIDER_MAX_RETRIES = 3



# ============================================================================
# Data Models
# ============================================================================
@dataclass

class BookRecord:
    """Represent one Goodreads row after normalization plus all derived vault paths and links."""
    row_number: int
    book_id: str
    title: str
    author_name: str
    original_author_name: str
    isbn: str
    isbn13: str
    rating: Any
    read_count: Any
    date_added: str
    date_read: str
    language: str
    pages: Any
    binding: str
    format_tag: str
    exclusive_shelf: str
    bookshelves: list[str]
    review: str
    publisher: str
    original_publish_year: int | str
    row_context: str
    author_dir: Path | None = None
    author_path: Path | None = None
    books_dir: Path | None = None
    book_path: Path | None = None
    cover_path: Path | None = None
    cover_filename: str = ""
    author_cover_path: Path | None = None
    author_cover_filename: str = ""
    author_link: str = ""
    book_link: str = ""

    def display_title(self) -> str:
        """Return the title variant that is safe to display in notes, links, and review messages."""
        return sanitize_obsidian_text(self.title or "Untitled")


@dataclass
class NoteDocument:
    """Provide the state and behavior for NoteDocument."""
    metadata: dict[str, Any]
    body: str


@dataclass
class SyncSummary:
    """Provide the state and behavior for SyncSummary."""
    books_created: int = 0
    books_updated: int = 0
    books_skipped: int = 0
    authors_created: int = 0
    authors_updated: int = 0
    authors_skipped: int = 0
    covers_downloaded: int = 0
    review_items: int = 0


@dataclass
class CodexResult:
    """Provide the state and behavior for CodexResult."""
    text: str
    stderr: str
    returncode: int
    duration_ms: int = 0


@dataclass
class PlaywrightResult:
    """Provide the state and behavior for PlaywrightResult."""
    stdout: str
    stderr: str
    returncode: int


@dataclass
class AuthorWorkItem:
    """Provide the state and behavior for AuthorWorkItem."""
    author_name: str
    author_record: BookRecord
    current_note: NoteDocument
    book_links: list[str]
    sample_titles: list[str]
    existing_country: str
    existing_biography: str


@dataclass
class BiographyWorkerSlot:
    """Provide the state and behavior for BiographyWorkerSlot."""
    slot_id: int
    state: str = "queued"
    author_name: str = ""


@dataclass
class AuthorMetadataResult:
    """Provide the state and behavior for AuthorMetadataResult."""
    biography: str
    country: str
    birth_year: str = ""
    death_year: str = ""
    sex: str = "unknown"


@dataclass
class ImageFetchResult:
    """Provide the state and behavior for ImageFetchResult."""
    url: str = ""
    provider: str = ""
    errors: list[str] = field(default_factory=list)


@dataclass
class BookProcessOutcome:
    """Provide the state and behavior for BookProcessOutcome."""
    status: str
    metadata_status: str
    note_status: str
    cover_status: str
    cover_provider: str = ""


@dataclass
class AuthorProcessOutcome:
    """Provide the state and behavior for AuthorProcessOutcome."""
    note_status: str
    image_status: str
    image_provider: str = ""




# ============================================================================
# Codex Runner And Agent Wiring
# ============================================================================
def build_codex_exec_command(
    prompt: str,
    workdir: Path,
    output_path: Path,
    model: str,
    reasoning_effort: str,
) -> list[str]:
    """Build codex exec command for the current sync step."""
    return [
        "codex",
        "exec",
        "-c",
        f"model_reasoning_effort={reasoning_effort}",
        "--model",
        model,
        "--skip-git-repo-check",
        "--color",
        "never",
        "-C",
        str(workdir),
        "-o",
        str(output_path),
        prompt,
    ]


class CodexRunner:
    """Provide the state and behavior for CodexRunner."""
    def __init__(
        self,
        model: str = CODEX_MODEL,
        reasoning_effort: str = CODEX_REASONING_EFFORT,
        timeout_s: int = CODEX_TIMEOUT_SECONDS,
    ) -> None:
        """Initialize the object state needed for later pipeline calls."""
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_s = timeout_s

    def run(self, prompt: str, *, workdir: Path, timeout_s: int | None = None) -> CodexResult:
        """Execute the main operation for this helper and return its structured result."""
        if shutil.which("codex") is None:
            raise RuntimeError("`codex` not found in PATH.")

        timeout = self.timeout_s if timeout_s is None else timeout_s
        with tempfile.TemporaryDirectory(dir=workdir) as temp_dir:
            output_path = Path(temp_dir) / "codex_output.txt"
            command = build_codex_exec_command(
                prompt=prompt,
                workdir=workdir,
                output_path=output_path,
                model=self.model,
                reasoning_effort=self.reasoning_effort,
            )
            started = time.perf_counter()
            try:
                completed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=timeout,
                    check=False,
                )
            except FileNotFoundError as exc:
                raise RuntimeError("`codex` not found in PATH.") from exc
            except subprocess.TimeoutExpired as exc:
                raise TimeoutError("Codex execution timed out.") from exc

            output_text = ""
            if output_path.exists():
                output_text = output_path.read_text(encoding="utf-8", errors="replace")

            return CodexResult(
                text=output_text,
                stderr=completed.stderr or completed.stdout or "",
                returncode=completed.returncode,
                duration_ms=int((time.perf_counter() - started) * 1000),
            )


class _CodexTextAgent:
    """Provide the state and behavior for CodexTextAgent."""
    agent_name = "CodexTextAgent"

    def __init__(
        self,
        *,
        runner: CodexRunner | None = None,
        model: str = CODEX_MODEL,
        reasoning_effort: str = CODEX_REASONING_EFFORT,
    ) -> None:
        """Initialize the object state needed for later pipeline calls."""
        self.runner = runner or CodexRunner(model=model, reasoning_effort=reasoning_effort)

    def _run(self, prompt: str, *, workdir: Path) -> CodexResult:
        """Handle run for the current sync workflow."""
        return self.runner.run(prompt, workdir=workdir)



# ============================================================================
# CLI Parsing
# ============================================================================
def add_common_sync_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common sync arguments to the structure being built for this run."""
    parser.add_argument("--csv", required=True, help="Path to the Goodreads CSV export.")
    parser.add_argument("--vault-root", default="library_v2", help="Output vault root.")
    parser.add_argument("--refresh-goodreads", action="store_true", help="Force rewrite Goodreads-derived metadata.")
    parser.add_argument("--refresh-bio", action="store_true", help="Regenerate author biography and country metadata.")
    parser.add_argument("--infer-author-dates", action="store_true", help="Infer missing author country/birth/death from the existing biography without regenerating the biography text.")
    parser.add_argument("--refresh-images", action="store_true", help="Refetch cover images even if a local cover exists.")
    parser.add_argument("--force-refresh-metadata", action="store_true", help="Legacy alias for refreshing Goodreads metadata, biography/country, and images together.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse args from raw input into the value used by the sync."""
    parser = argparse.ArgumentParser(
        description="Sync a Goodreads CSV export into a structured Obsidian-style vault."
    )
    subparsers = parser.add_subparsers(dest="command")

    sync_parser = subparsers.add_parser("sync-goodreads", help="Incrementally sync a Goodreads CSV into the vault.")
    add_common_sync_arguments(sync_parser)

    add_book_parser = subparsers.add_parser("add-book", help="Sync one specific book from the Goodreads CSV by Book Id or exact title.")
    add_common_sync_arguments(add_book_parser)
    add_book_parser.add_argument("selector", help="Exact Goodreads Book Id or exact title.")

    images_parser = subparsers.add_parser("fetch-images", help="Fetch missing local covers only.")
    images_parser.add_argument("--csv", required=True, help="Path to the Goodreads CSV export.")
    images_parser.add_argument("--vault-root", default="library_v2", help="Output vault root.")
    images_parser.add_argument("--refresh-images", action="store_true", help="Refetch cover images even if a local cover exists.")

    migrate_parser = subparsers.add_parser("migrate-yaml", help="Normalize existing vault YAML topology without external enrichment.")
    migrate_parser.add_argument("--vault-root", default="library_v2", help="Output vault root.")

    args = parser.parse_args(argv)
    if args.command is None:
        args.command = "sync-goodreads"
        if not hasattr(args, "csv"):
            legacy_parser = argparse.ArgumentParser(description=parser.description)
            add_common_sync_arguments(legacy_parser)
            args = legacy_parser.parse_args(argv)
            args.command = "sync-goodreads"
    return args




# ============================================================================
# Vault Bootstrap
# ============================================================================
def ensure_directories(vault_root: Path) -> dict[str, Path]:
    """Ensure directories exists and is shaped the way the sync expects."""
    paths = {
        "root": vault_root,
        "attachments": vault_root / "Attachments",
        "covers": vault_root / "Attachments" / "Covers",
        "author_images": vault_root / "Attachments" / "AuthorImages",
        "authors": vault_root / "Authors",
        "manual_review": vault_root / "Manual Review",
        "templates": vault_root / "Templates",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    ensure_hub_notes(vault_root)
    ensure_template_notes(vault_root)
    return paths


def ensure_hub_notes(vault_root: Path) -> None:
    """Ensure hub notes exists and is shaped the way the sync expects."""
    library_path = vault_root / "Library.md"
    if not library_path.exists():
        library_path.write_text(
            "# Library\n\n"
            "This vault is generated from a Goodreads export.\n\n"
            "- Book notes live under `Authors/<Author>/Books/`.\n"
            "- Author notes include an English biography, country, and linked books.\n"
            "- Covers are stored under `Attachments/Covers/` when available.\n"
            "- Frontmatter is normalized for Obsidian graph usage: entity fields use wikilinks, state fields stay plain text.\n",
            encoding="utf-8",
        )
    for legacy_name in ("Read.md", "To-Read.md", "Currently-Reading.md"):
        legacy_path = vault_root / legacy_name
        if legacy_path.exists():
            legacy_path.unlink()


def cleanup_generated_vault_content(vault_root: Path) -> None:
    """Handle cleanup generated vault content for the current sync workflow."""
    authors_root = vault_root / "Authors"
    covers_root = vault_root / "Attachments" / "Covers"
    author_images_root = vault_root / "Attachments" / "AuthorImages"
    manual_review_path = vault_root / "Manual Review" / REVIEW_NOTE_NAME

    if authors_root.exists():
        shutil.rmtree(authors_root)
    if covers_root.exists():
        shutil.rmtree(covers_root)
    if author_images_root.exists():
        shutil.rmtree(author_images_root)
    if manual_review_path.exists():
        manual_review_path.unlink()

    for filename in GENERATED_HUB_NOTE_NAMES:
        hub_path = vault_root / filename
        if hub_path.exists():
            hub_path.unlink()
    for legacy_name in ("Read.md", "To-Read.md", "Currently-Reading.md"):
        legacy_path = vault_root / legacy_name
        if legacy_path.exists():
            legacy_path.unlink()


class BiographyStatusRenderer:
    """Provide the state and behavior for BiographyStatusRenderer."""
    def __init__(self, slot_count: int) -> None:
        """Initialize the object state needed for later pipeline calls."""
        self.slot_count = slot_count
        self.slots = {slot_id: BiographyWorkerSlot(slot_id=slot_id) for slot_id in range(1, slot_count + 1)}
        self.interactive = sys.stdout.isatty() and not os.environ.get("PYTEST_CURRENT_TEST")
        self._has_rendered = False

    def update(self, slot_id: int, state: str, author_name: str = "") -> None:
        """Handle update for the current sync workflow."""
        slot = self.slots[slot_id]
        slot.state = state
        slot.author_name = author_name
        if self.interactive:
            self._render_interactive()
            return
        label = author_name or "waiting"
        print(f"[agent {slot_id}] {state}: {label}", flush=True)

    def finish(self) -> None:
        """Handle finish for the current sync workflow."""
        if self.interactive and self._has_rendered:
            sys.stdout.write("\n")
            sys.stdout.flush()

    def _render_interactive(self) -> None:
        """Handle render interactive for the current sync workflow."""
        if self._has_rendered:
            sys.stdout.write(f"\x1b[{self.slot_count + 1}F")
        lines = ["Biography workers:"]
        for slot_id in range(1, self.slot_count + 1):
            slot = self.slots[slot_id]
            label = slot.author_name or "waiting"
            lines.append(f"  [{slot.slot_id}] {slot.state:<12} {label}")
        for line in lines:
            sys.stdout.write("\x1b[2K" + line + "\n")
        sys.stdout.flush()
        self._has_rendered = True




# ============================================================================
# CSV And Text Normalization
# ============================================================================
def read_goodreads_csv(csv_path: Path) -> pd.DataFrame:
    """Handle read goodreads csv for the current sync workflow."""
    attempts = [
        ("utf-8-sig", "strict"),
        ("utf-8", "strict"),
        ("latin-1", "strict"),
        ("utf-8", "replace"),
    ]
    last_error: Exception | None = None
    for encoding, errors in attempts:
        try:
            with csv_path.open("r", encoding=encoding, errors=errors, newline="") as handle:
                return pd.read_csv(handle, dtype=str, keep_default_na=False)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise RuntimeError(f"Unable to decode CSV {csv_path}") from last_error


def clean_value(value: Any) -> str:
    """Clean value before it is reused elsewhere in the pipeline."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "nat", "none"} else text


def looks_like_mojibake(text: str) -> bool:
    """Heuristically decide whether the value looks like mojibake."""
    return any(marker in text for marker in MOJIBAKE_MARKERS)


def mojibake_score(text: str) -> int:
    """Handle mojibake score for the current sync workflow."""
    return sum(text.count(marker) for marker in MOJIBAKE_MARKERS)


def repair_text_value(value: Any) -> str:
    """Handle repair text value for the current sync workflow."""
    text = clean_value(value)
    if not text:
        return ""
    candidate = unicodedata.normalize("NFC", text)
    for _ in range(2):
        if not looks_like_mojibake(candidate):
            break
        try:
            repaired = candidate.encode("latin-1").decode("utf-8")
        except UnicodeError:
            break
        if repaired == candidate or mojibake_score(repaired) >= mojibake_score(candidate):
            break
        candidate = unicodedata.normalize("NFC", repaired)
    return candidate


def normalize_isbn(value: str) -> str:
    """Normalize isbn into the canonical representation used by this project."""
    return re.sub(r"[^0-9Xx]", "", repair_text_value(value))


def parse_date(value: str) -> str:
    """Parse date from raw input into the value used by the sync."""
    text = repair_text_value(value)
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def parse_intish(value: str) -> int | str:
    """Parse intish from raw input into the value used by the sync."""
    text = repair_text_value(value)
    if not text:
        return ""
    try:
        return int(float(text))
    except ValueError:
        return text


def sanitize_obsidian_text(text: str, fallback: str = "Untitled") -> str:
    """Sanitize obsidian text so it is safe for vault paths, links, or metadata."""
    value = repair_text_value(text) or fallback
    value = unicodedata.normalize("NFC", value)
    value = re.sub(r"\s*#\s*", " Num. ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or fallback


def sanitize_filename(text: str, fallback: str = "Untitled") -> str:
    """Sanitize filename so it is safe for vault paths, links, or metadata."""
    value = sanitize_obsidian_text(text, fallback=fallback)
    value = value.replace("[", "").replace("]", "")
    value = re.sub(r"[<>:\"/\\|?*\x00-\x1F]", "", value)
    value = re.sub(r"\s+", " ", value).strip().rstrip(".")
    return value or fallback


def normalize_plain_status(value: str) -> str:
    """Normalize plain status into the canonical representation used by this project."""
    text = repair_text_value(value).strip()
    if text.startswith("[[") and text.endswith("]]"):
        text = text[2:-2].split("|", 1)[-1]
    return text


def normalize_country_name(value: str) -> str:
    """Normalize country name into the canonical representation used by this project."""
    text = repair_text_value(value).strip()
    if text.startswith("[[") and text.endswith("]]"):
        text = text[2:-2].split("|", 1)[-1]
    return text or "Unknown"


def normalize_sex_value(value: Any) -> str:
    """Normalize sex value into the canonical representation used by this project."""
    text = repair_text_value(clean_value(value)).strip().casefold()
    if not text:
        return ""
    if text in {"male", "man", "male author", "he", "him"} or " male" in text or text.startswith("male "):
        return "male"
    if text in {"female", "woman", "female author", "she", "her"} or " female" in text or text.startswith("female "):
        return "female"
    if text in {"unknown", "unsure", "uncertain", "not sure", "n/a", "none", "null"}:
        return "unknown"
    return "unknown"


def ensure_wikilink(value: str) -> str:
    """Ensure wikilink exists and is shaped the way the sync expects."""
    text = repair_text_value(value).strip()
    if not text:
        return ""
    if text.startswith("[[") and text.endswith("]]"):
        return text
    return f"[[{text}]]"


def normalize_wikilink_list(values: list[str]) -> list[str]:
    """Normalize wikilink list into the canonical representation used by this project."""
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        link = ensure_wikilink(value)
        if not link:
            continue
        key = link.casefold()
        if key in seen:
            continue
        output.append(link)
        seen.add(key)
    return output


def normalize_bookshelf_links(values: list[str]) -> list[str]:
    """Normalize bookshelf links into the canonical representation used by this project."""
    return normalize_wikilink_list(values)


def normalize_tags(values: list[str]) -> list[str]:
    """Normalize tags into the canonical representation used by this project."""
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        tag = value if value in {"book", "author"} else sanitize_tag(value)
        if not tag:
            continue
        key = tag.casefold()
        if key in seen:
            continue
        output.append(tag)
        seen.add(key)
    return output


def sanitize_tag(value: str) -> str:
    """Sanitize tag so it is safe for vault paths, links, or metadata."""
    text = repair_text_value(value).lower()
    text = re.sub(r"[^\w\s/-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[/\s]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def parse_bookshelves(value: str) -> list[str]:
    """Parse bookshelves from raw input into the value used by the sync."""
    shelves: list[str] = []
    seen_shelves: set[str] = set()
    for raw_part in repair_text_value(value).split(","):
        shelf = repair_text_value(raw_part)
        if not shelf:
            continue
        shelf_key = shelf.casefold()
        if shelf_key == "to-read":
            continue
        if shelf_key in seen_shelves:
            continue
        shelves.append(shelf)
        seen_shelves.add(shelf_key)
    return shelves

def is_anonymous_author(author_name: str) -> bool:
    """Handle is anonymous author for the current sync workflow."""
    return repair_text_value(author_name).casefold() in {"", "anonymous", "anon", "unknown"}


CHEKHOV_CANONICAL_AUTHOR = "Antón Chéjov"
CHEKHOV_ALIAS_KEYS = {"anton chekhov", "anton chejov"}
CHEKHOV_ALIAS_PATTERNS = (
    "anton chekhov",
    "anton chejov",
    "antÃ³n chÃ©jov",
)


def normalize_author_alias_key(author_name: str) -> str:
    """Normalize author alias key into the canonical representation used by this project."""
    repaired = repair_text_value(author_name)
    normalized = unicodedata.normalize("NFKD", repaired)
    return "".join(char for char in normalized if not unicodedata.combining(char)).casefold().strip()


def is_chekhov_alias(author_name: str) -> bool:
    """Handle is chekhov alias for the current sync workflow."""
    candidate_forms = {
        author_name.casefold(),
        repair_text_value(author_name).casefold(),
        normalize_author_alias_key(author_name),
    }
    for form in candidate_forms:
        if any(pattern in form for pattern in CHEKHOV_ALIAS_PATTERNS):
            return True
        if "ant" in form and "ch" in form and "jov" in form:
            return True
    return False


def normalize_author_name(author_name: str) -> str:
    """Normalize author name into the canonical representation used by this project."""
    if is_anonymous_author(author_name):
        return "Anonymous"
    repaired = repair_text_value(author_name)
    if is_chekhov_alias(repaired):
        return CHEKHOV_CANONICAL_AUTHOR
    return repaired


def apply_manual_record_fixes(title: str, author_name: str) -> tuple[str, str]:
    """Handle apply manual record fixes for the current sync workflow."""
    normalized_title = sanitize_obsidian_text(title).casefold()
    canonical_author = normalize_author_name(author_name)
    normalized_author_key = normalize_author_alias_key(canonical_author)
    if normalized_title == "cuentos" and normalized_author_key in CHEKHOV_ALIAS_KEYS:
        return "Cuentos Chejov", CHEKHOV_CANONICAL_AUTHOR
    return title, canonical_author

def classify_format(binding: str) -> str:
    """Classify format into the bucket used by later sync logic."""
    value = repair_text_value(binding).casefold()
    if any(term in value for term in ("kindle", "ebook", "e-book", "digital")):
        return "virtual"
    if any(term in value for term in ("audible", "audio")):
        return "audiobook"
    return "physical"


def detect_primary_language(title: str, publisher: str) -> str:
    """Detect primary language from the available local inputs."""
    sample = " ".join(
        part for part in [repair_text_value(title), repair_text_value(publisher)] if part
    ).strip()
    if len(re.sub(r"\W+", "", sample, flags=re.UNICODE)) < 5:
        return "Unknown"

    if detect_langs is not None:
        try:
            candidates = detect_langs(sample)
        except LangDetectException:
            candidates = []
        if candidates:
            best = candidates[0]
            if best.prob >= 0.80:
                if best.lang == "en":
                    return "English"
                if best.lang == "es":
                    return "Spanish"

    lowered = f" {sample.casefold()} "
    spanish_markers = [" el ", " la ", " los ", " las ", " una ", " para ", "ciÃ³n", "Ã±", "Ã¡", "Ã©", "Ã­", "Ã³", "Ãº"]
    english_markers = [" the ", " and ", " of ", "with ", "edition", "press"]
    spanish_score = sum(marker in lowered for marker in spanish_markers)
    english_score = sum(marker in lowered for marker in english_markers)
    if spanish_score > english_score and spanish_score >= 1:
        return "Spanish"
    if english_score > spanish_score and english_score >= 1:
        return "English"
    return "Unknown"


def dedupe_preserve_order(values: list[str]) -> list[str]:
    """Handle dedupe preserve order for the current sync workflow."""
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        output.append(value)
        seen.add(key)
    return output




# ============================================================================
# Record Building And Path Derivation
# ============================================================================
def build_records(vault_root: Path, frame: pd.DataFrame) -> list[BookRecord]:
    """Convert the Goodreads dataframe into normalized book records with resolved vault paths."""
    authors_root = vault_root / "Authors"
    covers_root = vault_root / "Attachments" / "Covers"
    author_images_root = vault_root / "Attachments" / "AuthorImages"
    records: list[BookRecord] = []
    provisional_names: defaultdict[tuple[str, str], list[BookRecord]] = defaultdict(list)

    for row_number, (_, row) in enumerate(frame.iterrows(), start=2):
        row_map = {column: repair_text_value(row.get(column, "")) for column in frame.columns}
        title = row_map.get("Title", "") or "Untitled"
        raw_author = row_map.get("Author", "")
        author_name = normalize_author_name(raw_author)
        title, author_name = apply_manual_record_fixes(title, author_name)
        bookshelves = parse_bookshelves(row_map.get("Bookshelves", ""))
        format_tag = classify_format(row_map.get("Binding", ""))
        record = BookRecord(
            row_number=row_number,
            book_id=repair_text_value(row_map.get("Book Id", "")),
            title=title,
            author_name=author_name,
            original_author_name=raw_author,
            isbn=normalize_isbn(row_map.get("ISBN", "")),
            isbn13=normalize_isbn(row_map.get("ISBN13", "")),
            rating=parse_intish(row_map.get("My Rating", "")),
            read_count=parse_intish(row_map.get("Read Count", "")),
            date_added=parse_date(row_map.get("Date Added", "")),
            date_read=parse_date(row_map.get("Date Read", "")),
            language=detect_primary_language(title, row_map.get("Publisher", "")),
            pages=parse_intish(row_map.get("Number of Pages", "")),
            binding=repair_text_value(row_map.get("Binding", "")),
            format_tag=format_tag,
            exclusive_shelf=repair_text_value(row_map.get("Exclusive Shelf", "")),
            bookshelves=bookshelves,
            review=repair_text_value(row_map.get("My Review", "")),
            publisher=repair_text_value(row_map.get("Publisher", "")),
            original_publish_year=normalize_numeric_year_value(row_map.get("Original Publication Year", "")),
            row_context=f"row {row_number}",
        )
        author_folder_name = sanitize_filename(record.author_name, fallback="Anonymous")
        record.author_dir = authors_root / author_folder_name
        record.author_path = record.author_dir / f"{author_folder_name}.md"
        record.books_dir = record.author_dir / "Books"
        provisional_names[(author_folder_name.casefold(), sanitize_filename(title).casefold())].append(record)
        records.append(record)

    for grouped_records in provisional_names.values():
        collision = len(grouped_records) > 1
        for record in grouped_records:
            assert record.books_dir is not None
            base_name = sanitize_filename(record.title)
            unique_name = base_name
            if collision:
                suffix = record.isbn13 or record.book_id or sanitize_filename(record.author_name)
                unique_name = f"{base_name} - {suffix}"
            record.book_path = record.books_dir / f"{unique_name}.md"
            record.cover_filename = f"{sanitize_filename(record.author_name)} - {unique_name}.jpg"
            record.cover_path = covers_root / record.cover_filename
            record.author_cover_filename = f"{sanitize_filename(record.author_name)}.jpg"
            record.author_cover_path = author_images_root / record.author_cover_filename

    for record in records:
        assert record.author_path is not None
        assert record.book_path is not None
        record.author_link = vault_wiki_link(vault_root, record.author_path, record.author_name)
        record.book_link = vault_wiki_link(vault_root, record.book_path, record.display_title())
    return records


def vault_relative_path(vault_root: Path, target_path: Path, keep_suffix: bool = False) -> str:
    """Handle vault relative path for the current sync workflow."""
    relative = target_path.relative_to(vault_root).as_posix()
    if keep_suffix:
        return relative
    return relative[: -len(target_path.suffix)] if target_path.suffix else relative


def vault_wiki_link(vault_root: Path, target_path: Path, alias: str | None = None, keep_suffix: bool = False) -> str:
    """Handle vault wiki link for the current sync workflow."""
    target = vault_relative_path(vault_root, target_path, keep_suffix=keep_suffix)
    if alias:
        return f"[[{target}|{alias}]]"
    return f"[[{target}]]"




# ============================================================================
# Markdown Note IO And Rendering
# ============================================================================
def load_note(path: Path) -> NoteDocument:
    """Load note from disk into the in-memory representation used by the sync."""
    if not path.exists():
        return NoteDocument(metadata={}, body="")
    text = path.read_text(encoding="utf-8")
    if frontmatter is not None:
        post = frontmatter.loads(text)
        return NoteDocument(metadata=dict(post.metadata), body=post.content.lstrip("\n"))
    return fallback_load_note(text)


def fallback_load_note(text: str) -> NoteDocument:
    """Handle fallback load note for the current sync workflow."""
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            metadata = yaml.safe_load(parts[1]) or {}
            body = parts[2].lstrip("\r\n")
            return NoteDocument(metadata=metadata, body=body)
    return NoteDocument(metadata={}, body=text)


def dump_note(document: NoteDocument) -> str:
    """Serialize note back into the on-disk format used by the vault."""
    metadata_text = dump_frontmatter(document.metadata)
    body = document.body.rstrip()
    if body:
        return f"---\n{metadata_text}\n---\n{body}\n"
    return f"---\n{metadata_text}\n---\n"


def dump_frontmatter(metadata: dict[str, Any]) -> str:
    """Serialize frontmatter back into the on-disk format used by the vault."""
    lines: list[str] = []
    for key, value in metadata.items():
        if isinstance(value, list):
            if not value:
                lines.append(f"{key}: []")
                continue
            lines.append(f"{key}:")
            for item in value:
                if isinstance(item, dict):
                    lines.append("  -")
                    for subkey, subvalue in item.items():
                        lines.append(f"    {subkey}: {format_yaml_scalar(subvalue)}")
                else:
                    lines.append(f"  - {format_yaml_scalar(item)}")
            continue
        lines.append(f"{key}: {format_yaml_scalar(value)}")
    return "\n".join(lines)

def format_yaml_scalar(value: Any) -> str:
    """Handle format yaml scalar for the current sync workflow."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return json.dumps("" if value is None else str(value), ensure_ascii=False)


def normalize_reread_dates(value: Any) -> list[dict[str, str]]:
    """Normalize reread dates into the canonical representation used by this project."""
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        start = parse_date(clean_value(item.get("start", "")))
        end = parse_date(clean_value(item.get("end", "")))
        normalized.append({"start": start, "end": end})
    return normalized


def set_generated_block(body: str, marker_key: str, rendered_block: str, after_marker_key: str | None = None) -> str:
    """Handle set generated block for the current sync workflow."""
    start_marker, end_marker = GENERATED_MARKERS[marker_key]
    replacement = f"{start_marker}\n{rendered_block.rstrip()}\n{end_marker}"
    pattern = re.compile(re.escape(start_marker) + r".*?" + re.escape(end_marker), re.DOTALL)
    if pattern.search(body):
        updated = pattern.sub(replacement, body, count=1)
    else:
        stripped = body.rstrip()
        if after_marker_key is not None:
            _, after_end_marker = GENERATED_MARKERS[after_marker_key]
            after_match = re.search(re.escape(after_end_marker), stripped)
            if after_match:
                insert_at = after_match.end()
                updated = f"{stripped[:insert_at]}\n\n{replacement}{stripped[insert_at:]}"
                return updated.strip() + "\n"
        separator = "\n\n" if stripped else ""
        updated = f"{stripped}{separator}{replacement}"
    return updated.strip() + "\n"


def render_book_header(title: str, cover_link: str) -> str:
    """Render the generated markdown block for book header."""
    image_block = f"!{cover_link[:-2]}|200]]" if cover_link.startswith("[[") and cover_link.endswith("]]") else "> Cover not available."
    return f"# {title}\n\n{image_block}"


def render_book_quotes(quotes: str) -> str:
    """Render the generated markdown block for book quotes."""
    return f"## Quotes\n{quotes}".rstrip()


def render_book_review(review: str) -> str:
    """Render the generated markdown block for book review."""
    return f"## My Review\n{review}".rstrip()


def render_author_header(author_name: str, cover_link: str) -> str:
    """Render the generated markdown block for author header."""
    lines = [f"# {author_name}"]
    if cover_link.startswith("[[") and cover_link.endswith("]]"):
        lines.extend(["", f"!{cover_link}"])
    return "\n".join(lines).rstrip()

def render_author_bio(biography: str) -> str:
    """Render the generated markdown block for author bio."""
    return f"## Biography\n{biography or 'Biography not available yet.'}".rstrip()


def render_author_books(book_links: list[str]) -> str:
    """Render the generated markdown block for author books."""
    lines = ["## Books Linked"]
    lines.extend(f"- {link}" for link in book_links) if book_links else lines.append("- No linked books yet.")
    return "\n".join(lines)


def extract_generated_block(body: str, marker_key: str) -> str:
    """Handle extract generated block for the current sync workflow."""
    start_marker, end_marker = GENERATED_MARKERS[marker_key]
    pattern = re.compile(re.escape(start_marker) + r"\n?(.*?)\n?" + re.escape(end_marker), re.DOTALL)
    match = pattern.search(body)
    return match.group(1).strip() if match else ""


def ordered_metadata(keys: list[str], values: dict[str, Any]) -> dict[str, Any]:
    """Handle ordered metadata for the current sync workflow."""
    return {key: values.get(key, "") for key in keys}


def note_has_schema_keys(note: NoteDocument, keys: list[str]) -> bool:
    """Handle note has schema keys for the current sync workflow."""
    return all(key in note.metadata for key in keys)


def notes_equal(current: NoteDocument, desired: NoteDocument, keys: list[str]) -> bool:
    """Handle notes equal for the current sync workflow."""
    return (
        note_has_schema_keys(current, keys)
        and ordered_metadata(keys, current.metadata) == ordered_metadata(keys, desired.metadata)
        and current.body.strip() == desired.body.strip()
    )


def extract_existing_cover_filename(note: NoteDocument) -> str:
    """Handle extract existing cover filename for the current sync workflow."""
    cover = clean_value(note.metadata.get("cover", ""))
    if cover.startswith("[[") and cover.endswith("]]"):
        return cover[2:-2].split("|", 1)[0]
    return ""


def create_manual_review_collector() -> dict[str, list[str]]:
    """Create manual review collector for the current run."""
    return {section: [] for section in MANUAL_REVIEW_SECTIONS}


def add_review_item(review_sections: dict[str, list[str]], section: str, item: str) -> None:
    """Add review item to the structure being built for this run."""
    if item not in review_sections[section]:
        review_sections[section].append(item)


def print_progress(kind: str, index: int, total: int, status: str, label: str) -> None:
    """Handle print progress for the current sync workflow."""
    print(f"[{kind} {index}/{total}] {status}: {label}", flush=True)


def format_provider_status(status: str, provider: str = "") -> str:
    """Handle format provider status for the current sync workflow."""
    if provider and status in {"downloaded", "found"}:
        return f"{status}:{provider}"
    return status


def print_book_outcome(index: int, total: int, record: BookRecord, outcome: BookProcessOutcome) -> None:
    """Handle print book outcome for the current sync workflow."""
    parts = [
        outcome.status,
        f"yaml={outcome.metadata_status}",
        f"note={outcome.note_status}",
        f"cover={format_provider_status(outcome.cover_status, outcome.cover_provider)}",
    ]
    print(f"[book {index}/{total}] {' | '.join(parts)} | {record.author_name} - {record.display_title()}", flush=True)


def print_author_outcome(
    index: int,
    total: int,
    author_name: str,
    note_status: str,
    biography_status: str,
    demographics_status: str,
    sex_status: str,
    image_status: str,
    image_provider: str = "",
) -> None:
    """Handle print author outcome for the current sync workflow."""
    parts = [
        note_status,
        f"bio={biography_status}",
        f"country_years={demographics_status}",
        f"sex={sex_status}",
        f"image={format_provider_status(image_status, image_provider)}",
    ]
    print(f"[author {index}/{total}] {' | '.join(parts)} | {author_name}", flush=True)


def format_review_entry(record: BookRecord, detail: str) -> str:
    """Handle format review entry for the current sync workflow."""
    return f"- {record.author_name} - {record.display_title()}: {detail} ({record.row_context})"


def write_manual_review_note(path: Path, review_sections: dict[str, list[str]]) -> None:
    """Handle write manual review note for the current sync workflow."""
    if not any(review_sections.values()):
        if path.exists():
            path.unlink()
        return

    lines = ["# Missing Metadata", "", "Review these items manually after the sync run."]
    for section in MANUAL_REVIEW_SECTIONS:
        lines.append("")
        lines.append(f"## {section}")
        items = review_sections[section]
        if items:
            lines.extend(sorted(items))
        else:
            lines.append("- None.")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def build_book_frontmatter(record: BookRecord, cover_link: str, reread_dates: list[dict[str, str]]) -> dict[str, Any]:
    """Build book frontmatter for the current sync step."""
    return ordered_metadata(
        BOOK_FRONTMATTER_KEYS,
        {
            "title": record.display_title(),
            "author": normalize_wikilink_list([record.author_link]),
            "translator": [],
            "status": normalize_plain_status(record.exclusive_shelf),
            "rating": record.rating,
            "read_count": record.read_count,
            "date_added": record.date_added,
            "date_read": record.date_read,
            "language": record.language,
            "publisher": record.publisher,
            "original_publish_year": record.original_publish_year,
            "isbn": record.isbn,
            "isbn13": record.isbn13,
            "pages": record.pages,
            "format": record.format_tag,
            "cover": cover_link,
            "bookshelves": normalize_bookshelf_links(record.bookshelves),
            "reread_dates": normalize_reread_dates(reread_dates),
            "tags": normalize_tags(["book"]),
        },
    )


def build_author_frontmatter(author_name: str, cover_link: str, country: str, birth_year: str, death_year: str, sex: str) -> dict[str, Any]:
    """Build author frontmatter for the current sync step."""
    return ordered_metadata(
        AUTHOR_FRONTMATTER_KEYS,
        {
            "name": author_name,
            "cover": cover_link,
            "country": ensure_wikilink(country or "Unknown"),
            "birth_year": normalize_year_value(birth_year),
            "death_year": normalize_year_value(death_year),
            "sex": normalize_sex_value(sex),
            "tags": ["author"],
        },
    )


def build_book_document(existing: NoteDocument, record: BookRecord, cover_link: str) -> NoteDocument:
    """Build book document for the current sync step."""
    body = existing.body
    body = set_generated_block(body, "book_header", render_book_header(record.display_title(), cover_link))
    body = set_generated_block(body, "book_quotes", render_book_quotes(get_existing_quotes(existing)), after_marker_key="book_header")
    body = set_generated_block(body, "book_review", render_book_review(record.review), after_marker_key="book_quotes")
    reread_dates = normalize_reread_dates(existing.metadata.get("reread_dates", []))
    return NoteDocument(metadata=build_book_frontmatter(record, cover_link, reread_dates), body=body)


def build_author_document(
    existing: NoteDocument,
    author_name: str,
    biography: str,
    book_links: list[str],
    country: str,
    birth_year: str,
    death_year: str,
    sex: str,
    cover_link: str,
) -> NoteDocument:
    """Build author document for the current sync step."""
    body = existing.body
    body = set_generated_block(body, "author_header", render_author_header(author_name, cover_link))
    body = set_generated_block(body, "author_bio", render_author_bio(biography), after_marker_key="author_header")
    body = set_generated_block(body, "author_books", render_author_books(book_links), after_marker_key="author_bio")
    return NoteDocument(metadata=build_author_frontmatter(author_name, cover_link, country, birth_year, death_year, sex), body=body)


def get_existing_quotes(note: NoteDocument) -> str:
    """Read the current quotes from an existing note."""
    block = extract_generated_block(note.body, "book_quotes")
    if block.startswith("## Quotes"):
        return block[len("## Quotes") :].strip()
    return block


def get_existing_biography(note: NoteDocument) -> str:
    """Read the current biography from an existing note."""
    block = extract_generated_block(note.body, "author_bio")
    if block.startswith("## Biography"):
        return block[len("## Biography") :].strip()
    return block


def get_existing_country(note: NoteDocument) -> str:
    """Read the current country from an existing note."""
    return normalize_country_name(clean_value(note.metadata.get("country", "")))


def get_existing_birth_year(note: NoteDocument) -> str:
    """Read the current birth year from an existing note."""
    return normalize_year_value(note.metadata.get("birth_year", ""))


def get_existing_death_year(note: NoteDocument) -> str:
    """Read the current death year from an existing note."""
    return normalize_year_value(note.metadata.get("death_year", ""))


def get_existing_sex(note: NoteDocument) -> str:
    """Read the current sex from an existing note."""
    return normalize_sex_value(note.metadata.get("sex", ""))




# ============================================================================
# Template And Schema Helpers
# ============================================================================
def build_book_template_document() -> NoteDocument:
    """Build book template document for the current sync step."""
    body = ""
    body = set_generated_block(body, "book_header", render_book_header("", ""))
    body = set_generated_block(body, "book_quotes", render_book_quotes(""), after_marker_key="book_header")
    body = set_generated_block(body, "book_review", render_book_review(""), after_marker_key="book_quotes")
    return NoteDocument(
        metadata=ordered_metadata(
            BOOK_FRONTMATTER_KEYS,
            {
                "title": "",
                "author": [],
                "translator": [],
                "status": "",
                "rating": "",
                "read_count": "",
                "date_added": "",
                "date_read": "",
                "language": "",
                "publisher": "",
                "original_publish_year": "",
                "isbn": "",
                "isbn13": "",
                "pages": "",
                "format": "",
                "cover": "",
                "bookshelves": [],
                "reread_dates": [],
                "tags": normalize_tags(["book"]),
            },
        ),
        body=body,
    )


def build_author_template_document() -> NoteDocument:
    """Build author template document for the current sync step."""
    body = ""
    body = set_generated_block(body, "author_header", render_author_header("", ""))
    body = set_generated_block(body, "author_bio", render_author_bio(""), after_marker_key="author_header")
    body = set_generated_block(body, "author_books", render_author_books([]), after_marker_key="author_bio")
    return NoteDocument(
        metadata=ordered_metadata(
            AUTHOR_FRONTMATTER_KEYS,
            {
                "name": "",
                "cover": "",
                "country": "",
                "birth_year": "",
                "death_year": "",
                "sex": "",
                "tags": ["author"],
            },
        ),
        body=body,
    )


def ensure_template_notes(vault_root: Path) -> None:
    """Ensure template notes exists and is shaped the way the sync expects."""
    templates_root = vault_root / "Templates"
    templates_root.mkdir(parents=True, exist_ok=True)
    (templates_root / "Book_Template.md").write_text(dump_note(build_book_template_document()), encoding="utf-8")
    (templates_root / "Author_Template.md").write_text(dump_note(build_author_template_document()), encoding="utf-8")


def author_metadata_is_complete(note: NoteDocument) -> bool:
    """Handle author metadata is complete for the current sync workflow."""
    return (
        bool(get_existing_biography(note))
        and get_existing_country(note) != "Unknown"
        and bool(get_existing_birth_year(note) or get_existing_death_year(note))
        and bool(get_existing_sex(note))
    )


def normalize_year_value(value: Any) -> str:
    """Normalize year value into the canonical representation used by this project."""
    text = repair_text_value(clean_value(value)).strip()
    if not text or text.casefold() in {"unknown", "none", "null", "n/a"}:
        return ""
    match = re.search(r"(\d{4})", text)
    return match.group(1) if match else ""


def normalize_numeric_year_value(value: Any) -> int | str:
    """Normalize numeric year value into the canonical representation used by this project."""
    year = normalize_year_value(value)
    return int(year) if year else ""



# ============================================================================
# HTTP And Image Provider Helpers
# ============================================================================
def configure_metadata_session(session: requests.Session) -> requests.Session:
    """Handle configure metadata session for the current sync workflow."""
    session.headers.setdefault("User-Agent", HTTP_USER_AGENT)
    session.headers.setdefault("Accept", HTTP_ACCEPT_HEADER)
    return session


def provider_request_state(session: requests.Session) -> dict[str, float]:
    """Handle provider request state for the current sync workflow."""
    state = getattr(session, "_goodreads_provider_state", None)
    if state is None:
        state = {}
        setattr(session, "_goodreads_provider_state", state)
    return state


def rate_limit_provider(session: requests.Session, provider: str) -> None:
    """Handle rate limit provider for the current sync workflow."""
    minimum = IMAGE_PROVIDER_MIN_INTERVALS.get(provider, 0.0)
    if minimum <= 0:
        return
    state = provider_request_state(session)
    now = time.monotonic()
    last_called = state.get(provider, 0.0)
    elapsed = now - last_called
    if elapsed < minimum:
        time.sleep(minimum - elapsed)
    state[provider] = time.monotonic()


def parse_retry_after_seconds(response: requests.Response) -> float:
    """Parse retry after seconds from raw input into the value used by the sync."""
    value = response.headers.get("Retry-After", "")
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(seconds, 0.0)


def provider_get(
    session: requests.Session,
    provider: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: int = 20,
    retry_statuses: tuple[int, ...] = (),
) -> requests.Response:
    """Handle provider get for the current sync workflow."""
    last_error: requests.RequestException | None = None
    for attempt in range(IMAGE_PROVIDER_MAX_RETRIES):
        rate_limit_provider(session, provider)
        try:
            response = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            if attempt == IMAGE_PROVIDER_MAX_RETRIES - 1:
                raise
            time.sleep(min(2 ** attempt, 4))
            continue
        if response.status_code not in retry_statuses:
            response.raise_for_status()
            return response
        last_error = requests.HTTPError(f"{response.status_code} {response.reason}", response=response)
        if attempt == IMAGE_PROVIDER_MAX_RETRIES - 1:
            response.raise_for_status()
        delay = parse_retry_after_seconds(response) or min(2 ** attempt, 4)
        time.sleep(delay)
    assert last_error is not None
    raise last_error


def fetch_open_library_cover_url(session: requests.Session, record: BookRecord) -> tuple[str, list[str]]:
    """Fetch open library cover url and return soft errors instead of crashing the run."""
    identifiers = [identifier for identifier in (record.isbn13, record.isbn) if identifier]
    for identifier in identifiers:
        url = f"https://covers.openlibrary.org/b/isbn/{identifier}-L.jpg?default=false"
        try:
            response = session.get(url, timeout=20)
            response.raise_for_status()
        except requests.RequestException as exc:
            if identifier == identifiers[-1]:
                continue
            continue
        if "image" in response.headers.get("Content-Type", "").lower() and response.content:
            return url.replace("http://", "https://"), []
    return "", []



def score_wikimedia_cover_page(page: dict[str, Any], record: BookRecord) -> int:
    """Handle score wikimedia cover page for the current sync workflow."""
    title = clean_value(page.get("title", "")).casefold()
    score = 0
    for token in re.findall(r"\w+", repair_text_value(record.title).casefold())[:6]:
        if len(token) > 2 and token in title:
            score += 3
    for token in re.findall(r"\w+", repair_text_value(record.author_name).casefold())[:4]:
        if len(token) > 2 and token in title:
            score += 1
    if any(marker in title for marker in ("cover", "portada", "cubierta")):
        score += 3
    if any(title.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
        score += 1
    return score


def fetch_wikimedia_commons_cover_url(session: requests.Session, record: BookRecord) -> tuple[str, list[str]]:
    """Fetch wikimedia commons cover url and return soft errors instead of crashing the run."""
    query = " ".join(part for part in (repair_text_value(record.title), repair_text_value(record.author_name)) if part).strip()
    if not query:
        return "", []
    try:
        response = provider_get(
            session,
            "wikimedia_commons",
            WIKIMEDIA_COMMONS_API,
            params={
                "action": "query",
                "format": "json",
                "generator": "search",
                "gsrnamespace": 6,
                "gsrsearch": query,
                "gsrlimit": 10,
                "prop": "imageinfo",
                "iiprop": "url",
            },
            timeout=20,
            retry_statuses=(429, 503),
        )
        payload = response.json()
    except requests.RequestException as exc:
        return "", [f"Wikimedia Commons cover search failed for {record.display_title()}: {exc}"]
    pages = list(((payload.get("query") or {}).get("pages") or {}).values())
    if not pages:
        return "", []
    for page in sorted(pages, key=lambda current: score_wikimedia_cover_page(current, record), reverse=True):
        imageinfo = page.get("imageinfo") or []
        if not imageinfo:
            continue
        url = clean_value((imageinfo[0] or {}).get("url", ""))
        if url:
            return url.replace("http://", "https://"), []
    return "", []


def fetch_cover_image_with_fallbacks(session: requests.Session, record: BookRecord) -> ImageFetchResult:
    """Run the active book-cover provider chain and keep provider attribution for reporting."""
    errors: list[str] = []
    for provider, fetcher in (
        ("open_library", fetch_open_library_cover_url),
        ("wikimedia_commons", fetch_wikimedia_commons_cover_url),
        ("duckduckgo", fetch_ddg_cover_url),
    ):
        url, fetch_errors = fetcher(session, record)
        errors.extend(fetch_errors)
        if url:
            return ImageFetchResult(url=url, provider=provider, errors=errors)
    return ImageFetchResult(errors=errors)


# Backward-compatible wrapper used by older tests/call sites.
def fetch_cover_url_with_fallbacks(session: requests.Session, record: BookRecord) -> tuple[str, list[str]]:
    """Fetch cover url with fallbacks and return soft errors instead of crashing the run."""
    result = fetch_cover_image_with_fallbacks(session, record)
    return result.url, result.errors


def score_wikimedia_author_page(page: dict[str, Any], author_name: str) -> int:
    """Handle score wikimedia author page for the current sync workflow."""
    title = clean_value(page.get("title", "")).casefold()
    score = 0
    for token in re.findall(r"\w+", repair_text_value(author_name).casefold())[:4]:
        if token in title:
            score += 2
    if "portrait" in title or "photo" in title or "photograph" in title:
        score += 2
    return score


def fetch_wikimedia_commons_author_image_url(session: requests.Session, author_name: str) -> tuple[str, list[str]]:
    """Fetch wikimedia commons author image url and return soft errors instead of crashing the run."""
    try:
        response = provider_get(
            session,
            "wikimedia_commons",
            WIKIMEDIA_COMMONS_API,
            params={
                "action": "query",
                "format": "json",
                "generator": "search",
                "gsrnamespace": 6,
                "gsrsearch": repair_text_value(author_name),
                "gsrlimit": 5,
                "prop": "imageinfo",
                "iiprop": "url",
            },
            timeout=20,
            retry_statuses=(429, 503),
        )
        payload = response.json()
    except requests.RequestException as exc:
        return "", [f"Wikimedia Commons author-image search failed for {author_name}: {exc}"]
    pages = list(((payload.get("query") or {}).get("pages") or {}).values())
    if not pages:
        return "", []
    for page in sorted(pages, key=lambda current: score_wikimedia_author_page(current, author_name), reverse=True):
        imageinfo = page.get("imageinfo") or []
        if not imageinfo:
            continue
        url = clean_value((imageinfo[0] or {}).get("url", ""))
        if url:
            return url.replace("http://", "https://"), []
    return "", []


def fetch_author_image_result(session: requests.Session, author_name: str) -> ImageFetchResult:
    """Run the active author-image provider chain and keep provider attribution for reporting."""
    errors: list[str] = []
    for provider, fetcher in (("wikimedia_commons", fetch_wikimedia_commons_author_image_url), ("duckduckgo", fetch_ddg_author_image_url)):
        url, fetch_errors = fetcher(session, author_name)
        errors.extend(fetch_errors)
        if url:
            return ImageFetchResult(url=url, provider=provider, errors=errors)
    return ImageFetchResult(errors=errors)


def fetch_author_image_url(session: requests.Session, author_name: str) -> tuple[str, list[str]]:
    """Fetch author image url and return soft errors instead of crashing the run."""
    result = fetch_author_image_result(session, author_name)
    return result.url, result.errors


def download_cover(session: requests.Session, url: str, destination: Path) -> bool:
    """Handle download cover for the current sync workflow."""
    if not url:
        return False
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        return False

    if "text/html" in response.headers.get("Content-Type", "").lower():
        return False
    if not response.content:
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(response.content)
    return True


def url_looks_like_image(session: requests.Session, url: str) -> bool:
    """Handle url looks like image for the current sync workflow."""
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException:
        return False
    content_type = response.headers.get("Content-Type", "")
    if "image" in content_type.lower():
        return True
    chunk = (response.content or b"")[:16]
    signatures = (b"\xff\xd8\xff", b"\x89PNG", b"GIF8")
    return any(chunk.startswith(signature) for signature in signatures) or b"WEBP" in chunk


def fetch_first_working_ddg_image_url(session: requests.Session, results: list[dict[str, Any]]) -> str:
    """Fetch first working ddg image url and return soft errors instead of crashing the run."""
    for item in results:
        url = clean_value(item.get("image", ""))
        if url and url_looks_like_image(session, url):
            return url
    return ""


def ddg_image_search(query: str, retries: int = 3) -> tuple[list[dict[str, Any]], list[str]]:
    """Handle ddg image search for the current sync workflow."""
    if DDGS is None:
        return [], ["DuckDuckGo image fallback unavailable because dependency `ddgs` is not installed."]
    last_error = ""
    for attempt in range(retries):
        wait_seconds = 3 * (attempt + 1)
        time.sleep(wait_seconds)
        try:
            return list(DDGS().images(query, max_results=10)), []
        except DDGSException as exc:
            last_error = str(exc)
    if last_error:
        return [], [f"DuckDuckGo image search failed: {last_error}"]
    return [], []


def fetch_ddg_cover_url(session: requests.Session, record: BookRecord) -> tuple[str, list[str]]:
    """Fetch ddg cover url and return soft errors instead of crashing the run."""
    query = f"{repair_text_value(record.title)} {repair_text_value(record.author_name)} book cover".strip()
    results, errors = ddg_image_search(query)
    if errors:
        return "", [f"DuckDuckGo cover search failed for {record.display_title()}: {error}" for error in errors]
    url = fetch_first_working_ddg_image_url(session, results)
    return (url, []) if url else ("", [])


def fetch_ddg_author_image_url(session: requests.Session, author_name: str) -> tuple[str, list[str]]:
    """Fetch ddg author image url and return soft errors instead of crashing the run."""
    query = f"{repair_text_value(author_name)} portrait".strip()
    results, errors = ddg_image_search(query)
    if errors:
        return "", [f"DuckDuckGo author-image search failed for {author_name}: {error}" for error in errors]
    url = fetch_first_working_ddg_image_url(session, results)
    return (url, []) if url else ("", [])




# ============================================================================
# Author Metadata Prompting And Parsing
# ============================================================================
def build_codex_biography_prompt(author_name: str, sample_titles: list[str]) -> str:
    """Build codex biography prompt for the current sync step."""
    payload = {
        "instruction": (
            "You are AuthorBiographyAgent. Return strict JSON only with keys biography, country, birth_year, death_year, and sex. "
            "Biography must be in English, 1-3 paragraphs, focused on the author's life, significance, achievements, relationships, and context. "
            "Include birth-death years when known, but do not pad unknown values. Mention the provided books only when materially useful. No external links. "
            "Country must be the author's country of origin in English. birth_year and death_year must be four-digit strings or empty strings when unknown. "
            "Sex must be exactly male, female, or unknown. If country or sex is uncertain, return Unknown for that field."
        ),
        "author_name": author_name,
        "books_from_library": sample_titles[:5],
        "required_keys": ["biography", "country", "birth_year", "death_year", "sex"],
    }
    return json.dumps(payload, ensure_ascii=False)


def build_codex_demographics_prompt(author_name: str, biography: str, sample_titles: list[str]) -> str:
    """Build codex demographics prompt for the current sync step."""
    payload = {
        "instruction": (
            "You are AuthorDemographicsAgent. Return strict JSON only with keys country, birth_year, death_year, and sex. "
            "Use the provided English biography as primary evidence, and use the listed books only as secondary context. "
            "Do not rewrite or summarize the biography. Country must be in English. birth_year and death_year must be four-digit strings or empty strings when unknown. "
            "Sex must be exactly male, female, or unknown. If country or sex is uncertain, return Unknown for that field. No external links."
        ),
        "author_name": author_name,
        "existing_biography": biography,
        "books_from_library": sample_titles[:5],
        "required_keys": ["country", "birth_year", "death_year", "sex"],
    }
    return json.dumps(payload, ensure_ascii=False)


def build_codex_sex_prompt(author_name: str, biography: str, sample_titles: list[str]) -> str:
    """Build codex sex prompt for the current sync step."""
    payload = {
        "instruction": (
            "You are AuthorSexAgent. Return strict JSON only with key sex. "
            "Use the provided English biography as primary evidence, and use the listed books only as secondary context. "
            "Do not rewrite or summarize the biography. Sex must be exactly male, female, or unknown. "
            "If uncertain, return unknown. No external links."
        ),
        "author_name": author_name,
        "existing_biography": biography,
        "books_from_library": sample_titles[:5],
        "required_keys": ["sex"],
    }
    return json.dumps(payload, ensure_ascii=False)

def clean_generated_biography(text: str) -> str:
    """Clean generated biography before it is reused elsewhere in the pipeline."""
    cleaned = repair_text_value(text)
    cleaned = re.sub(r"^\s*[-*]\s*", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def parse_author_metadata_result(text: str) -> AuthorMetadataResult:
    """Parse author metadata result from raw input into the value used by the sync."""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    payload = json.loads(match.group(0) if match else text)
    biography = clean_generated_biography(clean_value(payload.get("biography", "")))
    country = normalize_country_name(clean_value(payload.get("country", "Unknown")))
    birth_year = normalize_year_value(payload.get("birth_year", ""))
    death_year = normalize_year_value(payload.get("death_year", ""))
    sex = normalize_sex_value(payload.get("sex", ""))
    return AuthorMetadataResult(biography=biography, country=country, birth_year=birth_year, death_year=death_year, sex=sex)


def biography_output_looks_invalid(text: str) -> bool:
    """Handle biography output looks invalid for the current sync workflow."""
    lowered = text.casefold()
    invalid_markers = [
        "i can't",
        "i cannot",
        "iâ€™m sorry",
        "i???m sorry",
        "i am sorry",
        "unable to",
        "cannot verify",
        "as an ai",
    ]
    return any(marker in lowered for marker in invalid_markers)


class AuthorBiographyAgent(_CodexTextAgent):
    """Provide the state and behavior for AuthorBiographyAgent."""
    agent_name = "AuthorBiographyAgent"

    def __init__(self, *, runner: CodexRunner | None = None) -> None:
        """Initialize the object state needed for later pipeline calls."""
        super().__init__(runner=runner, model=CODEX_MODEL, reasoning_effort=CODEX_REASONING_EFFORT)

    def run(self, author_name: str, sample_titles: list[str], *, workdir: Path) -> CodexResult:
        """Execute the main operation for this helper and return its structured result."""
        prompt = build_codex_biography_prompt(author_name, sample_titles)
        return self._run(prompt, workdir=workdir)


class AuthorDemographicsAgent(_CodexTextAgent):
    """Provide the state and behavior for AuthorDemographicsAgent."""
    agent_name = "AuthorDemographicsAgent"

    def __init__(self, *, runner: CodexRunner | None = None) -> None:
        """Initialize the object state needed for later pipeline calls."""
        super().__init__(runner=runner, model=CODEX_MODEL, reasoning_effort=CODEX_REASONING_EFFORT)

    def run(self, author_name: str, biography: str, sample_titles: list[str], *, workdir: Path) -> CodexResult:
        """Execute the main operation for this helper and return its structured result."""
        prompt = build_codex_demographics_prompt(author_name, biography, sample_titles)
        return self._run(prompt, workdir=workdir)


class AuthorSexAgent(_CodexTextAgent):
    """Provide the state and behavior for AuthorSexAgent."""
    agent_name = "AuthorSexAgent"

    def __init__(self, *, runner: CodexRunner | None = None) -> None:
        """Initialize the object state needed for later pipeline calls."""
        super().__init__(runner=runner, model=CODEX_MODEL, reasoning_effort=CODEX_REASONING_EFFORT)

    def run(self, author_name: str, biography: str, sample_titles: list[str], *, workdir: Path) -> CodexResult:
        """Execute the main operation for this helper and return its structured result."""
        prompt = build_codex_sex_prompt(author_name, biography, sample_titles)
        return self._run(prompt, workdir=workdir)


def generate_author_metadata_via_codex(
    author_name: str,
    sample_titles: list[str],
    workdir: Path,
    agent: AuthorBiographyAgent | None = None,
) -> tuple[AuthorMetadataResult, list[str]]:
    """Handle generate author metadata via codex for the current sync workflow."""
    biography_agent = agent or AuthorBiographyAgent()
    try:
        result = biography_agent.run(author_name, sample_titles, workdir=workdir)
    except RuntimeError as exc:
        message = str(exc)
        if "not found" in message.casefold():
            return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year=""), [f"Codex CLI not found while generating biography for {author_name}."]
        detail = clean_generated_biography(message)
        suffix = f" {detail}" if detail else ""
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year=""), [f"Codex biography generation failed for {author_name}.{suffix}".strip()]
    except TimeoutError:
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year=""), [f"Codex biography generation timed out for {author_name}."]

    if result.returncode != 0:
        detail = clean_generated_biography(result.stderr)
        suffix = f" {detail}" if detail else ""
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year=""), [f"Codex biography generation failed for {author_name}.{suffix}".strip()]

    try:
        metadata = parse_author_metadata_result(result.text)
    except (json.JSONDecodeError, KeyError, TypeError):
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year=""), [f"Codex biography generation produced unusable output for {author_name}."]
    if not metadata.biography or biography_output_looks_invalid(metadata.biography):
        return AuthorMetadataResult(
            biography="",
            country=metadata.country,
            birth_year=metadata.birth_year,
            death_year=metadata.death_year,
            sex=metadata.sex,
        ), [f"Codex biography generation produced unusable output for {author_name}."]
    return metadata, []


def generate_author_demographics_via_codex(
    author_name: str,
    biography: str,
    sample_titles: list[str],
    workdir: Path,
    agent: AuthorDemographicsAgent | None = None,
) -> tuple[AuthorMetadataResult, list[str]]:
    """Handle generate author demographics via codex for the current sync workflow."""
    demographics_agent = agent or AuthorDemographicsAgent()
    try:
        result = demographics_agent.run(author_name, biography, sample_titles, workdir=workdir)
    except RuntimeError as exc:
        message = str(exc)
        if "not found" in message.casefold():
            return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year="", sex=""), [f"Codex CLI not found while inferring demographics for {author_name}."]
        detail = clean_generated_biography(message)
        suffix = f" {detail}" if detail else ""
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year="", sex=""), [f"Codex demographics inference failed for {author_name}.{suffix}".strip()]
    except TimeoutError:
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year="", sex=""), [f"Codex demographics inference timed out for {author_name}."]

    if result.returncode != 0:
        detail = clean_generated_biography(result.stderr)
        suffix = f" {detail}" if detail else ""
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year="", sex=""), [f"Codex demographics inference failed for {author_name}.{suffix}".strip()]

    try:
        metadata = parse_author_metadata_result(result.text)
    except (json.JSONDecodeError, KeyError, TypeError):
        return AuthorMetadataResult(biography="", country="Unknown", birth_year="", death_year="", sex=""), [f"Codex demographics inference produced unusable output for {author_name}."]
    return AuthorMetadataResult(
        biography="",
        country=metadata.country,
        birth_year=metadata.birth_year,
        death_year=metadata.death_year,
        sex=metadata.sex,
    ), []


def generate_author_sex_via_codex(
    author_name: str,
    biography: str,
    sample_titles: list[str],
    workdir: Path,
    agent: AuthorSexAgent | None = None,
) -> tuple[AuthorMetadataResult, list[str]]:
    """Handle generate author sex via codex for the current sync workflow."""
    sex_agent = agent or AuthorSexAgent()
    try:
        result = sex_agent.run(author_name, biography, sample_titles, workdir=workdir)
    except RuntimeError as exc:
        message = str(exc)
        if "not found" in message.casefold():
            return AuthorMetadataResult(biography="", country="", birth_year="", death_year="", sex=""), [f"Codex CLI not found while inferring sex for {author_name}."]
        detail = clean_generated_biography(message)
        suffix = f" {detail}" if detail else ""
        return AuthorMetadataResult(biography="", country="", birth_year="", death_year="", sex=""), [f"Codex sex inference failed for {author_name}.{suffix}".strip()]
    except TimeoutError:
        return AuthorMetadataResult(biography="", country="", birth_year="", death_year="", sex=""), [f"Codex sex inference timed out for {author_name}."]

    if result.returncode != 0:
        detail = clean_generated_biography(result.stderr)
        suffix = f" {detail}" if detail else ""
        return AuthorMetadataResult(biography="", country="", birth_year="", death_year="", sex=""), [f"Codex sex inference failed for {author_name}.{suffix}".strip()]

    try:
        metadata = parse_author_metadata_result(result.text)
    except (json.JSONDecodeError, KeyError, TypeError):
        return AuthorMetadataResult(biography="", country="", birth_year="", death_year="", sex=""), [f"Codex sex inference produced unusable output for {author_name}."]
    return AuthorMetadataResult(biography="", country="", birth_year="", death_year="", sex=metadata.sex), []



# ============================================================================
# Author Note Processing
# ============================================================================
def build_author_work_items(
    records: list[BookRecord],
    author_books: dict[str, list[str]],
) -> list[AuthorWorkItem]:
    """Build author work items for the current sync step."""
    author_records = {record.author_name: record for record in records}
    sample_titles_by_author: dict[str, list[str]] = defaultdict(list)
    for record in records:
        title = record.display_title()
        if title not in sample_titles_by_author[record.author_name]:
            sample_titles_by_author[record.author_name].append(title)

    work_items: list[AuthorWorkItem] = []
    for author_name, book_links in author_books.items():
        author_record = author_records[author_name]
        assert author_record.author_path is not None
        work_items.append(
            AuthorWorkItem(
                author_name=author_name,
                author_record=author_record,
                current_note=load_note(author_record.author_path),
                book_links=sorted(book_links, key=str.casefold),
                sample_titles=sample_titles_by_author[author_name],
                existing_country=get_existing_country(load_note(author_record.author_path)),
                existing_biography=get_existing_biography(load_note(author_record.author_path)),
            )
        )
    return work_items


def materialize_author_note(
    work_item: AuthorWorkItem,
    biography: str,
    country: str,
    birth_year: str,
    death_year: str,
    sex: str,
    summary: SyncSummary,
    vault_root: Path,
    metadata_session: requests.Session,
    refresh_images: bool,
    review_sections: dict[str, list[str]],
) -> AuthorProcessOutcome:
    """Write the desired author note, including portrait handling, and report what changed."""
    assert work_item.author_record.author_path is not None
    assert work_item.author_record.author_cover_path is not None

    existing_cover_filename = extract_existing_cover_filename(work_item.current_note)
    cover_link = f"[[{existing_cover_filename}]]" if existing_cover_filename else ""
    image_status = "missing"
    image_provider = ""

    if work_item.author_name != "Anonymous":
        if work_item.author_record.author_cover_path.exists():
            cover_link = vault_wiki_link(vault_root, work_item.author_record.author_cover_path, keep_suffix=True)
            image_status = "existing"
        if refresh_images or not work_item.author_record.author_cover_path.exists():
            image_result = fetch_author_image_result(metadata_session, work_item.author_name)
            for error in image_result.errors:
                add_review_item(review_sections, "API Errors", f"- {work_item.author_name}: {error}")
            if image_result.url and download_cover(metadata_session, image_result.url, work_item.author_record.author_cover_path):
                summary.covers_downloaded += 1
                cover_link = vault_wiki_link(vault_root, work_item.author_record.author_cover_path, keep_suffix=True)
                image_status = "downloaded"
                image_provider = image_result.provider
            elif work_item.author_record.author_cover_path.exists():
                cover_link = vault_wiki_link(vault_root, work_item.author_record.author_cover_path, keep_suffix=True)
                image_status = "existing"
            else:
                cover_link = ""
                image_status = "missing"

    desired_author = build_author_document(
        work_item.current_note,
        work_item.author_name,
        biography,
        work_item.book_links,
        country,
        birth_year,
        death_year,
        sex,
        cover_link,
    )
    if notes_equal(work_item.current_note, desired_author, AUTHOR_FRONTMATTER_KEYS):
        summary.authors_skipped += 1
        return AuthorProcessOutcome(note_status="skipped", image_status=image_status, image_provider=image_provider)

    existed_before = work_item.author_record.author_path.exists()
    work_item.author_record.author_path.write_text(dump_note(desired_author), encoding="utf-8")
    work_item.current_note = desired_author
    if existed_before:
        summary.authors_updated += 1
        return AuthorProcessOutcome(note_status="updated", image_status=image_status, image_provider=image_provider)

    summary.authors_created += 1
    return AuthorProcessOutcome(note_status="created", image_status=image_status, image_provider=image_provider)

def classify_biography_result(errors: list[str]) -> str:
    """Classify biography result into the bucket used by later sync logic."""
    if not errors:
        return "finished"
    lowered = " ".join(errors).casefold()
    if "timed out" in lowered:
        return "timed_out"
    return "failed"


def process_author_biographies(
    work_items: list[AuthorWorkItem],
    refresh_bio: bool,
    infer_author_dates: bool,
    refresh_images: bool,
    workdir: Path,
    vault_root: Path,
    metadata_session: requests.Session,
    review_sections: dict[str, list[str]],
    summary: SyncSummary,
) -> None:
    """Coordinate author enrichment, worker status updates, and final author note writes."""
    total_authors = len(work_items)
    completed_authors = 0
    pending_queue: deque[tuple[AuthorWorkItem, str]] = deque()
    renderer = BiographyStatusRenderer(AUTHOR_BIO_CONCURRENCY)

    def finalize_author(
        work_item: AuthorWorkItem,
        biography: str,
        country: str,
        birth_year: str,
        death_year: str,
        sex: str,
        errors: list[str],
        mode: str,
        slot_id: int | None = None,
    ) -> None:
        """Finish one author job by writing the note and reporting the final outcome."""
        nonlocal completed_authors
        existing_country = get_existing_country(work_item.current_note)
        existing_birth_year = get_existing_birth_year(work_item.current_note)
        existing_death_year = get_existing_death_year(work_item.current_note)
        existing_sex = get_existing_sex(work_item.current_note)
        for error in errors:
            add_review_item(review_sections, "Failed Author Biographies", f"- {work_item.author_name}: {error}")
        if slot_id is not None:
            renderer.update(slot_id, "writing_note", work_item.author_name)
        author_outcome = materialize_author_note(
            work_item,
            biography,
            country,
            birth_year,
            death_year,
            sex,
            summary,
            vault_root,
            metadata_session,
            refresh_images,
            review_sections,
        )
        completed_authors += 1
        if mode == "full":
            biography_status = "generated" if biography else "missing"
            demographics_status = "generated" if (country != "Unknown" or birth_year or death_year) else "missing"
            sex_status = "generated" if sex else "missing"
        elif mode == "demographics":
            biography_status = "reused"
            demographics_status = "inferred" if (country != existing_country or birth_year != existing_birth_year or death_year != existing_death_year) else ("missing" if country == "Unknown" and not birth_year and not death_year else "reused")
            sex_status = "inferred" if sex and not existing_sex else ("reused" if sex else "missing")
        elif mode == "sex":
            biography_status = "reused"
            demographics_status = "reused"
            sex_status = "inferred" if sex and sex != existing_sex else ("reused" if sex else "missing")
        else:
            biography_status = "reused" if biography else "missing"
            demographics_status = "reused" if (country != "Unknown" or birth_year or death_year) else "missing"
            sex_status = "reused" if sex else "missing"
        print_author_outcome(
            completed_authors,
            total_authors,
            work_item.author_name,
            author_outcome.note_status,
            biography_status,
            demographics_status,
            sex_status,
            author_outcome.image_status,
            author_outcome.image_provider,
        )
        if slot_id is not None:
            renderer.update(slot_id, classify_biography_result(errors), work_item.author_name)

    for work_item in work_items:
        existing_bio = get_existing_biography(work_item.current_note)
        existing_country = get_existing_country(work_item.current_note)
        existing_birth_year = get_existing_birth_year(work_item.current_note)
        existing_death_year = get_existing_death_year(work_item.current_note)
        if work_item.author_name == "Anonymous":
            finalize_author(
                work_item,
                "Anonymous or unknown author. Review this note manually if you want to enrich it later.",
                existing_country or "Unknown",
                existing_birth_year,
                existing_death_year,
                get_existing_sex(work_item.current_note),
                [],
                "existing",
            )
            continue
        existing_sex = get_existing_sex(work_item.current_note)
        if refresh_bio:
            pending_queue.append((work_item, "full"))
        elif infer_author_dates and existing_bio and (existing_country == "Unknown" or not (existing_birth_year or existing_death_year)):
            pending_queue.append((work_item, "demographics"))
        elif existing_bio and not existing_sex and existing_country != "Unknown" and (existing_birth_year or existing_death_year):
            pending_queue.append((work_item, "sex"))
        elif author_metadata_is_complete(work_item.current_note):
            finalize_author(work_item, existing_bio, existing_country, existing_birth_year, existing_death_year, existing_sex, [], "existing")
        elif existing_bio and (existing_country == "Unknown" or not (existing_birth_year or existing_death_year)):
            pending_queue.append((work_item, "demographics"))
        else:
            pending_queue.append((work_item, "full"))

    if not pending_queue:
        renderer.finish()
        return

    with ThreadPoolExecutor(max_workers=AUTHOR_BIO_CONCURRENCY) as executor:
        active_jobs: dict[Future[tuple[AuthorMetadataResult, list[str]]], tuple[int, AuthorWorkItem, str]] = {}
        available_slots = deque(range(1, AUTHOR_BIO_CONCURRENCY + 1))

        def submit_next(slot_id: int) -> None:
            """Handle submit next for the current sync workflow."""
            if not pending_queue:
                renderer.update(slot_id, "queued")
                return
            work_item, mode = pending_queue.popleft()
            renderer.update(slot_id, "running", work_item.author_name)
            if mode == "demographics":
                future = executor.submit(
                    generate_author_demographics_via_codex,
                    work_item.author_name,
                    work_item.existing_biography,
                    work_item.sample_titles,
                    workdir,
                    AuthorDemographicsAgent(),
                )
            elif mode == "sex":
                future = executor.submit(
                    generate_author_sex_via_codex,
                    work_item.author_name,
                    work_item.existing_biography,
                    work_item.sample_titles,
                    workdir,
                    AuthorSexAgent(),
                )
            else:
                future = executor.submit(
                    generate_author_metadata_via_codex,
                    work_item.author_name,
                    work_item.sample_titles,
                    workdir,
                    AuthorBiographyAgent(),
                )
            active_jobs[future] = (slot_id, work_item, mode)

        while available_slots and pending_queue:
            submit_next(available_slots.popleft())

        while active_jobs:
            done, _ = wait(active_jobs.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                slot_id, work_item, mode = active_jobs.pop(future)
                try:
                    metadata, errors = future.result()
                except Exception as exc:  # pragma: no cover
                    metadata = AuthorMetadataResult(biography="", country="", birth_year="", death_year="", sex="")
                    if mode == "demographics":
                        label = "demographics inference"
                    elif mode == "sex":
                        label = "sex inference"
                    else:
                        label = "biography generation"
                    errors = [f"Codex {label} failed for {work_item.author_name}: {exc}"]
                biography = work_item.existing_biography if mode in {"demographics", "sex"} else (metadata.biography or get_existing_biography(work_item.current_note))
                country = metadata.country or get_existing_country(work_item.current_note) or "Unknown"
                birth_year = metadata.birth_year or get_existing_birth_year(work_item.current_note)
                death_year = metadata.death_year or get_existing_death_year(work_item.current_note)
                sex = metadata.sex or get_existing_sex(work_item.current_note)
                finalize_author(work_item, biography, country, birth_year, death_year, sex, errors, mode, slot_id)
                if pending_queue:
                    submit_next(slot_id)
                else:
                    available_slots.append(slot_id)

    renderer.finish()



# ============================================================================
# Link Materialization And Migration
# ============================================================================
def materialize_book_links(
    records: list[BookRecord],
    review_sections: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Materialize book links into the note and link structures used by the vault."""
    author_books: dict[str, list[str]] = defaultdict(list)
    scheduled_paths = {record.book_path for record in records if record.book_path is not None}
    for record in records:
        if record.book_path is None:
            add_review_item(
                review_sections,
                "Broken Book Materialization",
                format_review_entry(record, "Book path could not be generated"),
            )
            continue
        if record.book_path not in scheduled_paths or not record.book_link:
            add_review_item(
                review_sections,
                "Broken Book Materialization",
                format_review_entry(record, "Book link target was not scheduled for creation"),
            )
            continue
        if record.book_link not in author_books[record.author_name]:
            author_books[record.author_name].append(record.book_link)
    return author_books


def select_records_for_add_book(records: list[BookRecord], selector: str) -> list[BookRecord]:
    """Select records for add book according to the CLI or sync constraints."""
    exact_id = [record for record in records if record.book_id == selector]
    if exact_id:
        return exact_id
    exact_title = [record for record in records if record.display_title() == selector or record.title == selector]
    if len(exact_title) == 1:
        return exact_title
    if len(exact_title) > 1:
        raise RuntimeError(f"Multiple books matched selector: {selector}")
    raise RuntimeError(f"No book matched selector: {selector}")


def migrate_note_frontmatter(vault_root: Path, path: Path, is_author: bool) -> bool:
    """Handle migrate note frontmatter for the current sync workflow."""
    note = load_note(path)
    metadata = dict(note.metadata)
    body = note.body
    changed = False
    status = clean_value(metadata.get("status", ""))
    normalized_status = normalize_plain_status(status)
    if status != normalized_status:
        metadata["status"] = normalized_status
        changed = True
    bookshelves = metadata.get("bookshelves", [])
    if isinstance(bookshelves, list):
        shelf_values = [clean_value(item) for item in bookshelves]
    else:
        shelf_values = [clean_value(bookshelves)] if clean_value(bookshelves) else []
    filtered_shelves = [item for item in shelf_values if normalize_plain_status(item).casefold() != "to-read"]
    normalized_shelves = normalize_bookshelf_links(filtered_shelves)
    if metadata.get("bookshelves") != normalized_shelves:
        metadata["bookshelves"] = normalized_shelves
        changed = True
    tags = metadata.get("tags", [])
    if not isinstance(tags, list):
        tags = [clean_value(tags)] if clean_value(tags) else []
    desired_tags = normalize_tags(tags + (["author"] if is_author else ["book"]))
    if metadata.get("tags") != desired_tags:
        metadata["tags"] = desired_tags
        changed = True
    if is_author:
        country = clean_value(metadata.get("country", ""))
        normalized_country = ensure_wikilink(normalize_country_name(country))
        if metadata.get("country") != normalized_country:
            metadata["country"] = normalized_country
            changed = True
        birth_year = normalize_year_value(metadata.get("birth_year", ""))
        if metadata.get("birth_year") != birth_year:
            metadata["birth_year"] = birth_year
            changed = True
        if "birth_year" not in metadata:
            metadata["birth_year"] = birth_year
            changed = True
        death_year = normalize_year_value(metadata.get("death_year", ""))
        if metadata.get("death_year") != death_year:
            metadata["death_year"] = death_year
            changed = True
        if "death_year" not in metadata:
            metadata["death_year"] = death_year
            changed = True
        sex = normalize_sex_value(metadata.get("sex", ""))
        if metadata.get("sex", "") != sex:
            metadata["sex"] = sex
            changed = True
        if "sex" not in metadata:
            metadata["sex"] = sex
            changed = True
        author_image_path = vault_root / "Attachments" / "AuthorImages" / f"{sanitize_filename(path.stem)}.jpg"
        desired_cover = vault_wiki_link(vault_root, author_image_path, keep_suffix=True) if author_image_path.exists() else ""
        if "cover" not in metadata or metadata.get("cover", "") != desired_cover:
            metadata["cover"] = desired_cover
            changed = True
    else:
        authors_value = metadata.get("author", [])
        if isinstance(authors_value, list):
            author_values = [clean_value(item) for item in authors_value if clean_value(item)]
        else:
            author_values = [clean_value(authors_value)] if clean_value(authors_value) else []
        normalized_authors = normalize_wikilink_list(author_values)
        if metadata.get("author") != normalized_authors:
            metadata["author"] = normalized_authors
            changed = True
        if "translator" not in metadata or metadata.get("translator") != []:
            translator_value = metadata.get("translator", [])
            if isinstance(translator_value, list):
                normalized_translators = normalize_wikilink_list([clean_value(item) for item in translator_value if clean_value(item)])
            else:
                normalized_translators = normalize_wikilink_list([clean_value(translator_value)] if clean_value(translator_value) else [])
            metadata["translator"] = normalized_translators
            changed = True
        publisher = clean_value(metadata.get("publisher", ""))
        if metadata.get("publisher", "") != publisher:
            metadata["publisher"] = publisher
            changed = True
        original_publish_year = normalize_numeric_year_value(metadata.get("original_publish_year", ""))
        if metadata.get("original_publish_year", "") != original_publish_year:
            metadata["original_publish_year"] = original_publish_year
            changed = True
        reread_dates = normalize_reread_dates(metadata.get("reread_dates", []))
        if metadata.get("reread_dates") != reread_dates:
            metadata["reread_dates"] = reread_dates
            changed = True
        if "reread_dates" not in metadata:
            metadata["reread_dates"] = reread_dates
            changed = True
        updated_body = set_generated_block(body, "book_quotes", render_book_quotes(get_existing_quotes(note)), after_marker_key="book_header")
        if updated_body != body:
            body = updated_body
            changed = True
    if changed:
        metadata = ordered_metadata(AUTHOR_FRONTMATTER_KEYS if is_author else BOOK_FRONTMATTER_KEYS, metadata)
        path.write_text(dump_note(NoteDocument(metadata=metadata, body=body)), encoding="utf-8")
    return changed


def migrate_yaml(vault_root: Path) -> tuple[int, int]:
    """Handle migrate yaml for the current sync workflow."""
    authors = 0
    books = 0
    ensure_directories(vault_root)
    authors_root = vault_root / "Authors"
    if not authors_root.exists():
        return authors, books
    for path in authors_root.rglob("*.md"):
        if path.parent.name == "Books":
            if migrate_note_frontmatter(vault_root, path, is_author=False):
                books += 1
        else:
            if migrate_note_frontmatter(vault_root, path, is_author=True):
                authors += 1
    return authors, books

def merge_known_author_aliases(vault_root: Path) -> None:
    """Merge known author aliases according to the project-specific normalization rules."""
    authors_root = vault_root / "Authors"
    if not authors_root.exists():
        return

    canonical_dir = authors_root / CHEKHOV_CANONICAL_AUTHOR
    canonical_books_dir = canonical_dir / "Books"

    for author_dir in list(authors_root.iterdir()):
        if not author_dir.is_dir():
            continue
        if author_dir.name == CHEKHOV_CANONICAL_AUTHOR:
            continue
        if not is_chekhov_alias(author_dir.name):
            continue

        canonical_dir.mkdir(parents=True, exist_ok=True)
        canonical_books_dir.mkdir(parents=True, exist_ok=True)

        alias_books_dir = author_dir / "Books"
        if alias_books_dir.exists():
            for note_path in alias_books_dir.iterdir():
                destination = canonical_books_dir / note_path.name
                if destination.exists():
                    note_path.unlink(missing_ok=True)
                else:
                    shutil.move(str(note_path), str(destination))
            shutil.rmtree(alias_books_dir, ignore_errors=True)

        for note_path in author_dir.glob("*.md"):
            destination = canonical_dir / f"{CHEKHOV_CANONICAL_AUTHOR}.md"
            if destination.exists():
                note_path.unlink(missing_ok=True)
            else:
                shutil.move(str(note_path), str(destination))

        shutil.rmtree(author_dir, ignore_errors=True)




# ============================================================================
# Sync Orchestration
# ============================================================================
def run_sync(
    csv_path: Path,
    vault_root: Path,
    refresh_goodreads: bool = False,
    refresh_bio: bool = False,
    infer_author_dates: bool = False,
    refresh_images: bool = False,
    session: requests.Session | None = None,
    selector: str | None = None,
    image_only: bool = False,
) -> SyncSummary:
    """Orchestrate the full Goodreads-to-vault sync, including books, authors, images, and review-note rebuilding."""
    ensure_directories(vault_root)
    merge_known_author_aliases(vault_root)
    review_sections = create_manual_review_collector()
    summary = SyncSummary()

    frame = read_goodreads_csv(csv_path)
    missing_columns = [column for column in EXPECTED_COLUMNS if column not in frame.columns]
    if missing_columns:
        add_review_item(
            review_sections,
            "Parse Issues",
            f"- CSV is missing expected columns: {', '.join(missing_columns)}",
        )

    records = build_records(vault_root, frame)
    if selector is not None:
        selected = select_records_for_add_book(records, selector)
        selected_authors = {record.author_name for record in selected}
        records = [record for record in records if record in selected or record.author_name in selected_authors]
    author_books = materialize_book_links(records, review_sections)
    metadata_session = configure_metadata_session(session or requests.Session())

    total_books = len(records)
    for book_index, record in enumerate(records, start=1):
        assert record.author_dir is not None
        assert record.author_path is not None
        assert record.books_dir is not None
        assert record.book_path is not None
        assert record.cover_path is not None

        record.author_dir.mkdir(parents=True, exist_ok=True)
        record.books_dir.mkdir(parents=True, exist_ok=True)

        if not record.bookshelves and record.exclusive_shelf=="read":
            add_review_item(
                review_sections,
                "Missing Bookshelves",
                format_review_entry(record, "No Goodreads shelves found"),
            )
        if not (record.isbn or record.isbn13):
            add_review_item(
                review_sections,
                "Missing ISBN / ISBN13",
                format_review_entry(record, "No ISBN or ISBN13 found"),
            )
        if is_anonymous_author(record.original_author_name):
            add_review_item(
                review_sections,
                "Missing Authors",
                format_review_entry(record, "Author missing or anonymous; linked to Anonymous"),
            )

        current_book = load_note(record.book_path)
        cover_link = ""
        cover_status = "missing"
        cover_provider = ""

        if record.cover_path.exists():
            cover_link = vault_wiki_link(vault_root, record.cover_path, keep_suffix=True)
            cover_status = "existing"

        if refresh_images or not record.cover_path.exists():
            cover_result = fetch_cover_image_with_fallbacks(metadata_session, record)
            for error in cover_result.errors:
                add_review_item(review_sections, "API Errors", f"- {error}")
            if cover_result.url and download_cover(metadata_session, cover_result.url, record.cover_path):
                summary.covers_downloaded += 1
                cover_link = vault_wiki_link(vault_root, record.cover_path, keep_suffix=True)
                cover_status = "downloaded"
                cover_provider = cover_result.provider
            elif record.cover_path.exists():
                cover_link = vault_wiki_link(vault_root, record.cover_path, keep_suffix=True)
                cover_status = "existing"
            else:
                cover_link = ""
                cover_status = "missing"
                add_review_item(
                    review_sections,
                    "Missing Covers",
                    format_review_entry(record, "Cover image not found"),
                )

        desired_book = build_book_document(current_book, record, cover_link)
        metadata_changed = not note_has_schema_keys(current_book, BOOK_FRONTMATTER_KEYS) or ordered_metadata(BOOK_FRONTMATTER_KEYS, current_book.metadata) != ordered_metadata(BOOK_FRONTMATTER_KEYS, desired_book.metadata)
        body_changed = current_book.body.strip() != desired_book.body.strip()

        if notes_equal(current_book, desired_book, BOOK_FRONTMATTER_KEYS):
            summary.books_skipped += 1
            overall_status = "missing" if cover_status == "missing" else "skipped"
            outcome = BookProcessOutcome(
                status=overall_status,
                metadata_status="unchanged",
                note_status="skipped",
                cover_status=cover_status,
                cover_provider=cover_provider,
            )
        else:
            existed_before = record.book_path.exists()
            record.book_path.write_text(dump_note(desired_book), encoding="utf-8")
            if existed_before:
                summary.books_updated += 1
                note_status = "updated"
            else:
                summary.books_created += 1
                note_status = "created"
            overall_status = "updated_note" if image_only and note_status == "updated" and cover_status != "downloaded" else note_status
            outcome = BookProcessOutcome(
                status=overall_status,
                metadata_status="updated" if metadata_changed else "unchanged",
                note_status=note_status if (metadata_changed or body_changed) else "skipped",
                cover_status=cover_status,
                cover_provider=cover_provider,
            )

        print_book_outcome(book_index, total_books, record, outcome)
        if image_only:
            continue

    author_work_items = build_author_work_items(records, author_books)
    if not image_only:
        process_author_biographies(
            author_work_items,
            refresh_bio,
            infer_author_dates,
            refresh_images,
            Path.cwd(),
            vault_root,
            metadata_session,
            review_sections,
            summary,
        )

    for record in records:
        if record.book_path and not record.book_path.exists():
            add_review_item(
                review_sections,
                "Broken Book Materialization",
                format_review_entry(record, "Book note was linked but not written to disk"),
            )

    write_manual_review_note(vault_root / "Manual Review" / REVIEW_NOTE_NAME, review_sections)
    summary.review_items = sum(len(items) for items in review_sections.values())
    return summary


def format_summary(summary: SyncSummary, vault_root: Path) -> str:
    """Handle format summary for the current sync workflow."""
    return (
        "Sync completed.\n"
        f"- Books created: {summary.books_created}\n"
        f"- Books updated: {summary.books_updated}\n"
        f"- Books skipped: {summary.books_skipped}\n"
        f"- Authors created: {summary.authors_created}\n"
        f"- Authors updated: {summary.authors_updated}\n"
        f"- Authors skipped: {summary.authors_skipped}\n"
        f"- Images downloaded: {summary.covers_downloaded}\n"
        f"- Manual review items: {summary.review_items}\n"
        f"- Output root: {vault_root}"
    )


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint that routes subcommands into the appropriate sync or migration path."""
    args = parse_args(argv)
    vault_root = Path(args.vault_root).expanduser().resolve()

    if args.command == "migrate-yaml":
        authors, books = migrate_yaml(vault_root)
        print(f"YAML migration completed. Authors updated: {authors}. Books updated: {books}. Vault: {vault_root}")
        return 0

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    legacy_force = getattr(args, "force_refresh_metadata", False)
    refresh_goodreads = getattr(args, "refresh_goodreads", False) or legacy_force
    refresh_bio = getattr(args, "refresh_bio", False) or legacy_force
    infer_author_dates = getattr(args, "infer_author_dates", False)
    refresh_images = getattr(args, "refresh_images", False) or legacy_force

    if args.command == "fetch-images":
        summary = run_sync(
            csv_path=csv_path,
            vault_root=vault_root,
            refresh_images=refresh_images,
            image_only=True,
        )
    elif args.command == "add-book":
        summary = run_sync(
            csv_path=csv_path,
            vault_root=vault_root,
            refresh_goodreads=refresh_goodreads,
            refresh_bio=refresh_bio,
            infer_author_dates=infer_author_dates,
            refresh_images=refresh_images,
            selector=args.selector,
        )
    else:
        summary = run_sync(
            csv_path=csv_path,
            vault_root=vault_root,
            refresh_goodreads=refresh_goodreads,
            refresh_bio=refresh_bio,
            infer_author_dates=infer_author_dates,
            refresh_images=refresh_images,
        )
    print(format_summary(summary, vault_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




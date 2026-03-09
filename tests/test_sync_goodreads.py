from __future__ import annotations

import csv
import importlib.util
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import requests


MODULE_PATH = Path(__file__).resolve().parents[1] / "code" / "sync_goodreads.py"
TEST_TMP_ROOT = Path(__file__).resolve().parents[1] / ".tmp_test"
SPEC = importlib.util.spec_from_file_location("sync_goodreads", MODULE_PATH)
sync_goodreads = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules["sync_goodreads"] = sync_goodreads
SPEC.loader.exec_module(sync_goodreads)
TEST_TMP_ROOT.mkdir(exist_ok=True)


HEADERS = [
    "Book Id", "Title", "Author", "Author l-f", "Additional Authors", "ISBN", "ISBN13",
    "My Rating", "Average Rating", "Publisher", "Binding", "Number of Pages", "Year Published",
    "Original Publication Year", "Date Read", "Date Added", "Bookshelves", "Bookshelves with positions",
    "Exclusive Shelf", "My Review", "Spoiler", "Private Notes", "Read Count", "Owned Copies",
]


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, headers: dict[str, str] | None = None, content: bytes = b"img") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}
        self.content = content
        reasons = {200: "OK", 403: "Forbidden", 429: "Too Many Requests"}
        self.reason = reasons.get(status_code, "Error")

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error: {self.reason}", response=self)


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict | None = None, timeout: int | None = None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if not self.responses:
            raise AssertionError("No fake responses remaining")
        return self.responses.pop(0)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def make_case_dir(name: str) -> Path:
    path = TEST_TMP_ROOT / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


class GoodreadsSyncTests(unittest.TestCase):
    def test_incremental_sync_updates_existing_book_and_serializes_new_metadata(self) -> None:
        base = make_case_dir("incremental")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        rows = [{
            "Book Id": "1", "Title": "Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank",
            "Additional Authors": "", "ISBN": "0441172717", "ISBN13": "9780441172719", "My Rating": "4",
            "Average Rating": "4.27", "Publisher": "Ace", "Binding": "Paperback", "Number of Pages": "896",
            "Year Published": "1965", "Original Publication Year": "1965", "Date Read": "2026-01-01",
            "Date Added": "2025-12-31", "Bookshelves": "science fiction,favorites", "Bookshelves with positions": "",
            "Exclusive Shelf": "read", "My Review": "A classic.", "Spoiler": "", "Private Notes": "",
            "Read Count": "1", "Owned Copies": "1",
        }]
        write_csv(csv_path, rows)

        author_metadata = sync_goodreads.AuthorMetadataResult(
            biography="Frank Herbert (1920-1986) was an American science fiction writer.",
            country="United States",
            birth_year="1920",
            death_year="1986",
        )
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", return_value=(author_metadata, [])
        ):
            sync_goodreads.run_sync(csv_path, vault_root)

        rows[0]["My Rating"] = "5"
        rows[0]["Bookshelves"] = "science fiction,desert"
        write_csv(csv_path, rows)
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", side_effect=AssertionError("should not regenerate author metadata")
        ):
            sync_goodreads.run_sync(csv_path, vault_root)

        book_text = (vault_root / "Authors" / "Frank Herbert" / "Books" / "Dune.md").read_text(encoding="utf-8")
        author_text = (vault_root / "Authors" / "Frank Herbert" / "Frank Herbert.md").read_text(encoding="utf-8")
        self.assertIn('rating: 5', book_text)
        self.assertIn('- "[[desert]]"', book_text)
        self.assertIn('reread_dates: []', book_text)
        self.assertIn('status: "read"', book_text)
        self.assertIn('country: "[[United States]]"', author_text)
        self.assertIn('birth_year: "1920"', author_text)
        self.assertIn('death_year: "1986"', author_text)
        self.assertIn('- "author"', author_text)
        self.assertTrue((vault_root / "Library.md").exists())

    def test_add_book_updates_existing_author_links(self) -> None:
        base = make_case_dir("add_book")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        rows = [
            {"Book Id": "1", "Title": "Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank", "Additional Authors": "", "ISBN": "", "ISBN13": "", "My Rating": "5", "Average Rating": "", "Publisher": "Ace", "Binding": "Paperback", "Number of Pages": "500", "Year Published": "1965", "Original Publication Year": "1965", "Date Read": "", "Date Added": "2026-01-01", "Bookshelves": "science fiction", "Bookshelves with positions": "", "Exclusive Shelf": "read", "My Review": "", "Spoiler": "", "Private Notes": "", "Read Count": "1", "Owned Copies": "1"},
            {"Book Id": "2", "Title": "Children of Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank", "Additional Authors": "", "ISBN": "", "ISBN13": "", "My Rating": "4", "Average Rating": "", "Publisher": "Ace", "Binding": "Paperback", "Number of Pages": "400", "Year Published": "1976", "Original Publication Year": "1976", "Date Read": "", "Date Added": "2026-02-01", "Bookshelves": "science fiction", "Bookshelves with positions": "", "Exclusive Shelf": "read", "My Review": "", "Spoiler": "", "Private Notes": "", "Read Count": "1", "Owned Copies": "1"},
        ]
        write_csv(csv_path, rows)
        author_metadata = sync_goodreads.AuthorMetadataResult(
            biography="Frank Herbert (1920-1986) was an American science fiction writer.",
            country="United States",
            birth_year="1920",
            death_year="1986",
        )
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", return_value=(author_metadata, [])
        ):
            sync_goodreads.run_sync(csv_path, vault_root, selector="Dune")
            sync_goodreads.run_sync(csv_path, vault_root, selector="Children of Dune")

        author_text = (vault_root / "Authors" / "Frank Herbert" / "Frank Herbert.md").read_text(encoding="utf-8")
        self.assertIn('[[Authors/Frank Herbert/Books/Dune|Dune]]', author_text)
        self.assertIn('[[Authors/Frank Herbert/Books/Children of Dune|Children of Dune]]', author_text)

    def test_manual_review_note_uses_plain_text_labels(self) -> None:
        base = make_case_dir("manual_review")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        write_csv(csv_path, [{
            "Book Id": "1", "Title": "Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank",
            "Additional Authors": "", "ISBN": "", "ISBN13": "", "My Rating": "", "Average Rating": "",
            "Publisher": "Ace", "Binding": "Paperback", "Number of Pages": "500", "Year Published": "1965",
            "Original Publication Year": "1965", "Date Read": "", "Date Added": "2026-01-01",
            "Bookshelves": "science fiction", "Bookshelves with positions": "", "Exclusive Shelf": "to-read",
            "My Review": "", "Spoiler": "", "Private Notes": "", "Read Count": "", "Owned Copies": "1",
        }])
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", return_value=(sync_goodreads.AuthorMetadataResult(biography="Frank Herbert was an American writer.", country="United States", birth_year="1920", death_year="1986"), [])
        ):
            sync_goodreads.run_sync(csv_path, vault_root)
        review_text = (vault_root / "Manual Review" / sync_goodreads.REVIEW_NOTE_NAME).read_text(encoding="utf-8")
        self.assertIn('Frank Herbert - Dune', review_text)
        self.assertNotIn('[[', review_text)

    def test_fetch_google_books_cover_url_retries_and_uses_headers(self) -> None:
        record = sync_goodreads.BookRecord(
            row_number=2,
            book_id="1",
            title="Dune",
            author_name="Frank Herbert",
            original_author_name="Frank Herbert",
            isbn="",
            isbn13="9780441172719",
            rating=0,
            read_count=0,
            date_added="",
            date_read="",
            language="English",
            pages=0,
            binding="Paperback",
            format_tag="physical",
            exclusive_shelf="read",
            bookshelves=["science fiction"],
            review="",
            publisher="Ace",
            row_context="row 2",
        )
        session = sync_goodreads.configure_metadata_session(FakeSession([
            FakeResponse(429, headers={"Retry-After": "0"}),
            FakeResponse(200, payload={"items": [{"volumeInfo": {"imageLinks": {"large": "http://example.com/cover.jpg"}}}]}),
        ]))
        with patch.object(sync_goodreads.time, "sleep") as sleep_mock:
            cover_url, errors = sync_goodreads.fetch_google_books_cover_url(session, record)
        self.assertEqual(cover_url, "https://example.com/cover.jpg")
        self.assertEqual(errors, [])
        self.assertEqual(len(session.calls), 2)
        self.assertIn("Mozilla/5.0", session.headers["User-Agent"])
        self.assertTrue(sleep_mock.called)

    def test_fetch_wikipedia_cover_url_retries_on_403(self) -> None:
        record = sync_goodreads.BookRecord(
            row_number=2,
            book_id="1",
            title="Dune",
            author_name="Frank Herbert",
            original_author_name="Frank Herbert",
            isbn="",
            isbn13="",
            rating=0,
            read_count=0,
            date_added="",
            date_read="",
            language="English",
            pages=0,
            binding="Paperback",
            format_tag="physical",
            exclusive_shelf="read",
            bookshelves=["science fiction"],
            review="",
            publisher="Ace",
            row_context="row 2",
        )
        session = sync_goodreads.configure_metadata_session(FakeSession([
            FakeResponse(403, headers={"Retry-After": "0"}),
            FakeResponse(200, payload={"originalimage": {"source": "https://example.com/wiki-cover.jpg"}}),
        ]))
        with patch.object(sync_goodreads.time, "sleep") as sleep_mock:
            cover_url, errors = sync_goodreads.fetch_wikipedia_cover_url(session, record)
        self.assertEqual(cover_url, "https://example.com/wiki-cover.jpg")
        self.assertEqual(errors, [])
        self.assertEqual(len(session.calls), 2)
        self.assertTrue(sleep_mock.called)

    def test_rate_limit_provider_enforces_minimum_delay(self) -> None:
        session = FakeSession([])
        with patch.object(sync_goodreads.time, "monotonic", side_effect=[10.0, 10.0, 10.4, 10.4]), patch.object(sync_goodreads.time, "sleep") as sleep_mock:
            sync_goodreads.rate_limit_provider(session, "google_books")
            sync_goodreads.rate_limit_provider(session, "google_books")
        sleep_mock.assert_called_once()
        self.assertAlmostEqual(sleep_mock.call_args[0][0], 0.6, places=2)

    def test_fetch_wikimedia_commons_cover_url_returns_ranked_image(self) -> None:
        record = sync_goodreads.BookRecord(
            row_number=2,
            book_id="1",
            title="Dune",
            author_name="Frank Herbert",
            original_author_name="Frank Herbert",
            isbn="",
            isbn13="",
            rating=0,
            read_count=0,
            date_added="",
            date_read="",
            language="English",
            pages=0,
            binding="Paperback",
            format_tag="physical",
            exclusive_shelf="read",
            bookshelves=["science fiction"],
            review="",
            publisher="Ace",
            row_context="row 2",
        )
        session = sync_goodreads.configure_metadata_session(FakeSession([
            FakeResponse(200, payload={
                "query": {
                    "pages": {
                        "1": {"title": "File:Dune cover.jpg", "imageinfo": [{"url": "https://commons.example/dune-cover.jpg"}]},
                        "2": {"title": "File:Frank Herbert portrait.jpg", "imageinfo": [{"url": "https://commons.example/herbert.jpg"}]},
                    }
                }
            })
        ]))
        cover_url, errors = sync_goodreads.fetch_wikimedia_commons_cover_url(session, record)
        self.assertEqual(cover_url, "https://commons.example/dune-cover.jpg")
        self.assertEqual(errors, [])

    def test_fetch_cover_url_with_fallbacks_stops_after_wikipedia(self) -> None:
        record = sync_goodreads.BookRecord(
            row_number=2,
            book_id="1",
            title="Dune",
            author_name="Frank Herbert",
            original_author_name="Frank Herbert",
            isbn="",
            isbn13="",
            rating=0,
            read_count=0,
            date_added="",
            date_read="",
            language="English",
            pages=0,
            binding="Paperback",
            format_tag="physical",
            exclusive_shelf="read",
            bookshelves=["science fiction"],
            review="",
            publisher="Ace",
            row_context="row 2",
        )
        order: list[str] = []
        with patch.object(sync_goodreads, "fetch_open_library_cover_url", side_effect=lambda *_: (order.append("open") or ("", []))), patch.object(
            sync_goodreads, "fetch_google_books_cover_url", side_effect=lambda *_: (order.append("google") or ("", []))
        ), patch.object(
            sync_goodreads, "fetch_wikimedia_commons_cover_url", side_effect=lambda *_: (order.append("commons") or ("", []))
        ), patch.object(
            sync_goodreads, "fetch_wikipedia_cover_url", side_effect=lambda *_: (order.append("wikipedia") or ("https://example.com/final-cover.jpg", []))
        ):
            url, errors = sync_goodreads.fetch_cover_url_with_fallbacks(requests.Session(), record)
        self.assertEqual(url, "https://example.com/final-cover.jpg")
        self.assertEqual(errors, [])
        self.assertEqual(order, ["open", "google", "commons", "wikipedia"])

    def test_build_playwright_cover_query_includes_book(self) -> None:
        record = sync_goodreads.BookRecord(
            row_number=2,
            book_id="1",
            title="Dune",
            author_name="Frank Herbert",
            original_author_name="Frank Herbert",
            isbn="",
            isbn13="",
            rating=0,
            read_count=0,
            date_added="",
            date_read="",
            language="English",
            pages=0,
            binding="Paperback",
            format_tag="physical",
            exclusive_shelf="read",
            bookshelves=["science fiction"],
            review="",
            publisher="Ace",
            row_context="row 2",
        )
        self.assertEqual(sync_goodreads.build_playwright_cover_query(record), "Dune Frank Herbert book cover")
        self.assertIn("book%20cover", sync_goodreads.build_playwright_cover_search_url(record))

    def test_playwright_runner_prefers_npx_cmd_on_windows(self) -> None:
        runner = sync_goodreads.PlaywrightRunner()
        with patch.object(sync_goodreads, "os") as os_mock, patch.object(sync_goodreads.shutil, "which") as which_mock, patch.object(sync_goodreads.Path, "home", return_value=Path("C:/Users/test")):
            os_mock.name = "nt"
            os_mock.environ = {}
            which_mock.side_effect = lambda name: "C:/Program Files/nodejs/npx.cmd" if name == "npx.cmd" else None
            command = runner._base_command()
        self.assertEqual(command[0], "C:/Program Files/nodejs/npx.cmd")
        self.assertIn("playwright-cli", command)

    def test_fetch_playwright_cover_url_returns_first_scraped_url(self) -> None:
        record = sync_goodreads.BookRecord(
            row_number=2,
            book_id="1",
            title="Dune",
            author_name="Frank Herbert",
            original_author_name="Frank Herbert",
            isbn="",
            isbn13="",
            rating=0,
            read_count=0,
            date_added="",
            date_read="",
            language="English",
            pages=0,
            binding="Paperback",
            format_tag="physical",
            exclusive_shelf="read",
            bookshelves=["science fiction"],
            review="",
            publisher="Ace",
            row_context="row 2",
        )
        with patch.object(
            sync_goodreads.PlaywrightRunner,
            "run",
            side_effect=[
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout='{"candidates": ["https://images.example/dune.jpg"], "imageCount": 12, "title": "Dune", "url": "https://www.bing.com/images/search?q=dune"}', stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
            ],
        ) as run_mock:
            url, errors = sync_goodreads.fetch_playwright_cover_url(requests.Session(), record)
        self.assertEqual(url, "https://images.example/dune.jpg")
        self.assertEqual(errors, [])
        self.assertEqual(run_mock.call_args_list[0].args[0][0], "open")
        self.assertEqual(run_mock.call_args_list[0].args[0][1], "https://www.bing.com/")
        self.assertIn("Dune Frank Herbert book cover", run_mock.call_args_list[1].args[0][1])
        self.assertIn("URLSearchParams", run_mock.call_args_list[1].args[0][1])

    def test_fetch_playwright_cover_url_reports_page_diagnostics_when_empty(self) -> None:
        record = sync_goodreads.BookRecord(
            row_number=3,
            book_id="2",
            title="El Aleph",
            author_name="Jorge Luis Borges",
            original_author_name="Jorge Luis Borges",
            isbn="",
            isbn13="",
            rating=0,
            read_count=0,
            date_added="",
            date_read="",
            language="Spanish",
            pages=0,
            binding="Paperback",
            format_tag="physical",
            exclusive_shelf="read",
            bookshelves=["fiction"],
            review="",
            publisher="Emece",
            row_context="row 3",
        )
        with patch.object(
            sync_goodreads.PlaywrightRunner,
            "run",
            side_effect=[
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout='{"candidates": [], "imageCount": 0, "title": "Bing Images", "url": "https://www.bing.com/?q=aleph"}', stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout='{"title":"Bing Images","url":"https://www.bing.com/?q=aleph","imageCount":0}', stderr="", returncode=0),
                sync_goodreads.PlaywrightResult(stdout="", stderr="", returncode=0),
            ],
        ):
            url, errors = sync_goodreads.fetch_playwright_cover_url(requests.Session(), record)
        self.assertEqual(url, "")
        self.assertEqual(len(errors), 1)
        self.assertIn("no usable images", errors[0])
        self.assertIn("Bing Images", errors[0])
    def test_render_book_header_uses_obsidian_embed(self) -> None:
        header = sync_goodreads.render_book_header(
            "Egyptian Mythology",
            "[[Attachments/Covers/Geraldine Pinch - Egyptian Mythology A Guide to the Gods, Goddesses, and Traditions of Ancient Egypt.jpg]]",
        )
        self.assertIn(
            "![[Attachments/Covers/Geraldine Pinch - Egyptian Mythology A Guide to the Gods, Goddesses, and Traditions of Ancient Egypt.jpg|200]]",
            header,
        )
        self.assertNotIn("![|200](", header)

    def test_chekhov_record_materializes_under_normalized_author(self) -> None:
        base = make_case_dir("chekhov_fix")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        write_csv(csv_path, [{
            "Book Id": "1", "Title": "Cuentos", "Author": "Anton Chekhov", "Author l-f": "Chekhov, Anton",
            "Additional Authors": "", "ISBN": "", "ISBN13": "", "My Rating": "4", "Average Rating": "",
            "Publisher": "Porr?a", "Binding": "Paperback", "Number of Pages": "300", "Year Published": "2000",
            "Original Publication Year": "1900", "Date Read": "2026-01-01", "Date Added": "2026-01-01",
            "Bookshelves": "cuentos,clasicos", "Bookshelves with positions": "", "Exclusive Shelf": "read",
            "My Review": "", "Spoiler": "", "Private Notes": "", "Read Count": "1", "Owned Copies": "1",
        }])
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", return_value=(sync_goodreads.AuthorMetadataResult(biography="Anton Chekhov was a Russian writer.", country="Russia", birth_year="1860", death_year="1904"), [])
        ):
            sync_goodreads.run_sync(csv_path, vault_root)
        _, expected_author = sync_goodreads.apply_manual_record_fixes("Cuentos", "Anton Chekhov")
        self.assertTrue((vault_root / "Authors" / expected_author / f"{expected_author}.md").exists())
        self.assertTrue((vault_root / "Authors" / expected_author / "Books" / "Cuentos Chejov.md").exists())
        self.assertFalse((vault_root / "Authors" / "Anton Chekhov").exists())

    def test_run_sync_merges_chekhov_alias_directories(self) -> None:
        base = make_case_dir("chekhov_alias_merge")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        canonical_author = sync_goodreads.CHEKHOV_CANONICAL_AUTHOR
        mojibake_author = "AntÃ³n ChÃ©jov"

        for author_name, book_title in (
            (canonical_author, "Cinco novelas cortas"),
            (mojibake_author, "Cuentos Chejov"),
            ("Anton Chekhov", "La estepa - En el barranco"),
        ):
            books_dir = vault_root / "Authors" / author_name / "Books"
            books_dir.mkdir(parents=True, exist_ok=True)
            (vault_root / "Authors" / author_name / f"{author_name}.md").write_text("---\nname: \"temp\"\n---\n", encoding="utf-8")
            (books_dir / f"{book_title}.md").write_text("---\ntitle: \"temp\"\n---\n", encoding="utf-8")

        write_csv(csv_path, [{
            "Book Id": "1", "Title": "La estepa / En el barranco", "Author": "Anton Chekhov", "Author l-f": "Chekhov, Anton",
            "Additional Authors": "", "ISBN": "", "ISBN13": "", "My Rating": "4", "Average Rating": "",
            "Publisher": "Porr?a", "Binding": "Paperback", "Number of Pages": "300", "Year Published": "2000",
            "Original Publication Year": "1900", "Date Read": "2026-01-01", "Date Added": "2026-01-01",
            "Bookshelves": "clasicos", "Bookshelves with positions": "", "Exclusive Shelf": "read",
            "My Review": "", "Spoiler": "", "Private Notes": "", "Read Count": "1", "Owned Copies": "1",
        }])

        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", return_value=(sync_goodreads.AuthorMetadataResult(biography="Anton Chekhov was a Russian writer.", country="Russia", birth_year="1860", death_year="1904"), [])
        ):
            sync_goodreads.run_sync(csv_path, vault_root)

        canonical_dir = vault_root / "Authors" / canonical_author
        self.assertTrue((canonical_dir / f"{canonical_author}.md").exists())
        self.assertTrue((canonical_dir / "Books" / "Cinco novelas cortas.md").exists())
        self.assertTrue((canonical_dir / "Books" / "Cuentos Chejov.md").exists())
        self.assertTrue((canonical_dir / "Books" / "La estepa - En el barranco.md").exists())
        self.assertFalse((vault_root / "Authors" / mojibake_author).exists())
        self.assertFalse((vault_root / "Authors" / "Anton Chekhov").exists())

    def test_migrate_yaml_normalizes_status_bookshelves_tags_country_and_new_fields(self) -> None:
        base = make_case_dir("migrate")
        vault_root = base / "library_v2"
        book_dir = vault_root / "Authors" / "Adam Smith" / "Books"
        book_dir.mkdir(parents=True, exist_ok=True)
        author_path = vault_root / "Authors" / "Adam Smith" / "Adam Smith.md"
        book_path = book_dir / "The Wealth of Nations.md"
        author_path.write_text('---\nname: "Adam Smith"\ncountry: "Scotland"\n---\n', encoding='utf-8')
        book_path.write_text('---\ntitle: "The Wealth of Nations"\nstatus: "[[to-read]]"\nbookshelves:\n  - "economics"\n  - "to-read"\n---\n', encoding='utf-8')
        authors, books = sync_goodreads.migrate_yaml(vault_root)
        self.assertEqual((authors, books), (1, 1))
        migrated_author = author_path.read_text(encoding='utf-8')
        migrated_book = book_path.read_text(encoding='utf-8')
        self.assertIn('country: "[[Scotland]]"', migrated_author)
        self.assertIn('birth_year: ""', migrated_author)
        self.assertIn('death_year: ""', migrated_author)
        self.assertIn('status: "to-read"', migrated_book)
        self.assertIn('- "[[economics]]"', migrated_book)
        self.assertNotIn('[[to-read]]', migrated_book)
        self.assertIn('reread_dates: []', migrated_book)
        self.assertIn('- "book"', migrated_book)

    def test_blank_migrated_years_trigger_author_refresh_on_normal_sync(self) -> None:
        base = make_case_dir("refresh_blank_years")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        write_csv(csv_path, [{
            "Book Id": "1", "Title": "Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank",
            "Additional Authors": "", "ISBN": "0441172717", "ISBN13": "9780441172719", "My Rating": "5",
            "Average Rating": "", "Publisher": "Ace", "Binding": "Paperback", "Number of Pages": "500",
            "Year Published": "1965", "Original Publication Year": "1965", "Date Read": "2026-01-01",
            "Date Added": "2026-01-01", "Bookshelves": "science fiction", "Bookshelves with positions": "",
            "Exclusive Shelf": "read", "My Review": "", "Spoiler": "", "Private Notes": "",
            "Read Count": "1", "Owned Copies": "1",
        }])
        author_dir = vault_root / "Authors" / "Frank Herbert"
        books_dir = author_dir / "Books"
        books_dir.mkdir(parents=True, exist_ok=True)
        (books_dir / "Dune.md").write_text(
            """---
title: \"Dune\"
author: \"[[Authors/Frank Herbert/Frank Herbert|Frank Herbert]]\"
status: \"read\"
rating: 5
read_count: 1
date_added: \"2026-01-01\"
date_read: \"2026-01-01\"
language: \"English\"
isbn: \"0441172717\"
isbn13: \"9780441172719\"
pages: 500
format: \"physical\"
cover: \"\"
bookshelves:
  - \"[[science fiction]]\"
reread_dates: []
tags:
  - \"book\"
---
""",
            encoding="utf-8",
        )
        (author_dir / "Frank Herbert.md").write_text(
            """---
name: \"Frank Herbert\"
country: \"[[United States]]\"
birth_year: \"\"
death_year: \"\"
tags:
  - \"author\"
---
<!-- GENERATED:AUTHOR_HEADER START -->
# Frank Herbert
<!-- GENERATED:AUTHOR_HEADER END -->

<!-- GENERATED:AUTHOR_BIO START -->
## Biography
Frank Herbert was an American science fiction writer.
<!-- GENERATED:AUTHOR_BIO END -->

<!-- GENERATED:AUTHOR_BOOKS START -->
## Books Linked
- [[Authors/Frank Herbert/Books/Dune|Dune]]
<!-- GENERATED:AUTHOR_BOOKS END -->
""",
            encoding="utf-8",
        )
        generated = sync_goodreads.AuthorMetadataResult(
            biography="Frank Herbert (1920-1986) was an American science fiction writer.",
            country="United States",
            birth_year="1920",
            death_year="1986",
        )
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", return_value=(generated, [])
        ) as metadata_mock:
            sync_goodreads.run_sync(csv_path, vault_root)
        self.assertEqual(metadata_mock.call_count, 1)
        author_text = (author_dir / "Frank Herbert.md").read_text(encoding="utf-8")
        self.assertIn('birth_year: "1920"', author_text)
        self.assertIn('death_year: "1986"', author_text)

    def test_infer_author_dates_reuses_existing_biography(self) -> None:
        base = make_case_dir("infer_author_dates")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        write_csv(csv_path, [{
            "Book Id": "1", "Title": "Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank",
            "Additional Authors": "", "ISBN": "0441172717", "ISBN13": "9780441172719", "My Rating": "5",
            "Average Rating": "", "Publisher": "Ace", "Binding": "Paperback", "Number of Pages": "500",
            "Year Published": "1965", "Original Publication Year": "1965", "Date Read": "2026-01-01",
            "Date Added": "2026-01-01", "Bookshelves": "science fiction", "Bookshelves with positions": "",
            "Exclusive Shelf": "read", "My Review": "", "Spoiler": "", "Private Notes": "",
            "Read Count": "1", "Owned Copies": "1",
        }])
        author_dir = vault_root / "Authors" / "Frank Herbert"
        books_dir = author_dir / "Books"
        books_dir.mkdir(parents=True, exist_ok=True)
        (books_dir / "Dune.md").write_text(
            """---
title: "Dune"
author: "[[Authors/Frank Herbert/Frank Herbert|Frank Herbert]]"
status: "read"
rating: 5
read_count: 1
date_added: "2026-01-01"
date_read: "2026-01-01"
language: "English"
isbn: "0441172717"
isbn13: "9780441172719"
pages: 500
format: "physical"
cover: ""
bookshelves:
  - "[[science fiction]]"
reread_dates: []
tags:
  - "book"
---
""",
            encoding="utf-8",
        )
        existing_bio = "Frank Herbert wrote expansive speculative fiction about power and ecology."
        (author_dir / "Frank Herbert.md").write_text(
            f"""---
name: "Frank Herbert"
country: "[[Unknown]]"
birth_year: ""
death_year: ""
tags:
  - "author"
---
<!-- GENERATED:AUTHOR_HEADER START -->
# Frank Herbert
<!-- GENERATED:AUTHOR_HEADER END -->

<!-- GENERATED:AUTHOR_BIO START -->
## Biography
{existing_bio}
<!-- GENERATED:AUTHOR_BIO END -->

<!-- GENERATED:AUTHOR_BOOKS START -->
## Books Linked
- [[Authors/Frank Herbert/Books/Dune|Dune]]
<!-- GENERATED:AUTHOR_BOOKS END -->
""",
            encoding="utf-8",
        )
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", side_effect=AssertionError("should not regenerate biography")
        ), patch.object(
            sync_goodreads,
            "generate_author_demographics_via_codex",
            return_value=(sync_goodreads.AuthorMetadataResult(biography="", country="United States", birth_year="1920", death_year="1986"), []),
        ) as demo_mock:
            sync_goodreads.run_sync(csv_path, vault_root, infer_author_dates=True)
        self.assertEqual(demo_mock.call_count, 1)
        author_text = (author_dir / "Frank Herbert.md").read_text(encoding="utf-8")
        self.assertIn(existing_bio, author_text)
        self.assertIn('country: "[[United States]]"', author_text)
        self.assertIn('birth_year: "1920"', author_text)
        self.assertIn('death_year: "1986"', author_text)

    def test_main_subcommands_work_and_to_read_stays_plain(self) -> None:
        base = make_case_dir("main")
        csv_path = base / "goodreads.csv"
        vault_root = base / "library_v2"
        write_csv(csv_path, [{"Book Id": "1", "Title": "Dune", "Author": "Frank Herbert", "Author l-f": "Herbert, Frank", "Additional Authors": "", "ISBN": "", "ISBN13": "", "My Rating": "", "Average Rating": "", "Publisher": "Ace", "Binding": "Paperback", "Number of Pages": "500", "Year Published": "1965", "Original Publication Year": "1965", "Date Read": "", "Date Added": "2026-01-01", "Bookshelves": "science fiction,to-read", "Bookshelves with positions": "", "Exclusive Shelf": "to-read", "My Review": "", "Spoiler": "", "Private Notes": "", "Read Count": "", "Owned Copies": "1"}])
        with patch.object(sync_goodreads, "fetch_cover_url_with_fallbacks", return_value=("", [])), patch.object(
            sync_goodreads, "generate_author_metadata_via_codex", return_value=(sync_goodreads.AuthorMetadataResult(biography="Frank Herbert was an American writer.", country="United States", birth_year="1920", death_year="1986"), [])
        ):
            rc = sync_goodreads.main(["sync-goodreads", "--csv", str(csv_path), "--vault-root", str(vault_root)])
        self.assertEqual(rc, 0)
        main_book = (vault_root / "Authors" / "Frank Herbert" / "Books" / "Dune.md").read_text(encoding="utf-8")
        self.assertIn('status: "to-read"', main_book)
        self.assertNotIn('status: "[[to-read]]"', main_book)
        self.assertNotIn('- "[[to-read]]"', main_book)
        self.assertIn('- "[[science fiction]]"', main_book)


if __name__ == "__main__":
    unittest.main()


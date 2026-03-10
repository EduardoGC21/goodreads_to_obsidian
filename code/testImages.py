#!/usr/bin/env python
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import sync_goodreads


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnostic-only sample of the book/author image pipeline with verbose provider output. This script does not download or save files.")
    parser.add_argument("--csv", required=True, type=Path, help="Path to the Goodreads export CSV.")
    parser.add_argument("--vault-root", required=True, type=Path, help="Vault root used to build the expected note/image paths.")
    parser.add_argument("--limit", type=int, default=5, help="Number of books to sample.")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed for repeatable runs.")
    parser.add_argument("--include-existing", action="store_true", help="Include books that already have a local cover file.")
    return parser.parse_args()


def sample_records(csv_path: Path, vault_root: Path, limit: int, seed: int, include_existing: bool) -> list[sync_goodreads.BookRecord]:
    frame = sync_goodreads.read_goodreads_csv(csv_path)
    records = sync_goodreads.build_records(vault_root, frame)
    if not include_existing:
        records = [record for record in records if record.cover_path is not None and not record.cover_path.exists()]
    rng = random.Random(seed)
    if len(records) <= limit:
        return records
    return rng.sample(records, limit)


def run_fetcher(label: str, fetcher, *args) -> tuple[str, list[str]]:
    try:
        return fetcher(*args)
    except Exception as exc:
        return "", [f"{label} raised {type(exc).__name__}: {exc}"]


def print_result(prefix: str, url: str, errors: list[str]) -> None:
    if url:
        print(f"    {prefix}: PASS -> {url}")
        return
    if errors:
        print(f"    {prefix}: FAIL")
        for error in errors:
            print(f"      - {error}")
        return
    print(f"    {prefix}: MISS")


def inspect_book(session: requests.Session, record: sync_goodreads.BookRecord) -> None:
    print(f"\nBOOK: {record.author_name} - {record.display_title()}")
    if record.cover_path is not None:
        print(f"  local cover path: {record.cover_path}")
        print(f"  local cover exists: {record.cover_path.exists()}")

    for label, fetcher in (
        ("Open Library", sync_goodreads.fetch_open_library_cover_url),
        ("Wikimedia Commons", sync_goodreads.fetch_wikimedia_commons_cover_url),
        ("DuckDuckGo", sync_goodreads.fetch_ddg_cover_url),
    ):
        url, errors = run_fetcher(label, fetcher, session, record)
        print_result(label, url, errors)

    print("  final book pipeline:")
    url, errors = run_fetcher("book pipeline", sync_goodreads.fetch_cover_url_with_fallbacks, session, record)
    print_result("pipeline", url, errors)

    print(f"  author portrait for: {record.author_name}")
    for label, fetcher in (
        ("Wikimedia Commons", sync_goodreads.fetch_wikimedia_commons_author_image_url),
        ("DuckDuckGo", sync_goodreads.fetch_ddg_author_image_url),
    ):
        url, errors = run_fetcher(label, fetcher, session, record.author_name)
        print_result(label, url, errors)

    print("  final author pipeline:")
    url, errors = run_fetcher("author pipeline", sync_goodreads.fetch_author_image_url, session, record.author_name)
    print_result("pipeline", url, errors)


def main() -> int:
    args = parse_args()
    records = sample_records(args.csv, args.vault_root, args.limit, args.seed, args.include_existing)
    if not records:
        print("No matching records found for sampling.")
        return 0

    session = sync_goodreads.configure_metadata_session(requests.Session())

    print(f"Testing image pipeline with {len(records)} sampled books")
    print("Diagnostic mode only: this script does not download or write images to the vault.")
    print(f"CSV: {args.csv}")
    print(f"Vault root: {args.vault_root}")
    print(f"Include existing covers: {args.include_existing}")

    for index, record in enumerate(records, start=1):
        print(f"\n[{index}/{len(records)}]")
        inspect_book(session, record)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

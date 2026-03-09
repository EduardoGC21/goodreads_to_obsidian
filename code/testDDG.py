#!/usr/bin/env python
import time
import requests
from ddgs import DDGS
from ddgs.exceptions import DDGSException

TEST_BOOK_TITLE  = "One Hundred Years of Solitude"
TEST_BOOK_AUTHOR = "Gabriel Garcia Marquez"  # no accents — less likely to get blocked
TEST_AUTHOR_NAME = "Gabriel Garcia Marquez"


def check_url_is_image(url: str) -> bool:
    try:
        r = requests.get(url, timeout=10, stream=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/132.0.0.0 Safari/537.36"
        })
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "image" in ct.lower():
            return True
        chunk = next(r.iter_content(16), b"")
        return any(chunk.startswith(s) for s in [b"\xff\xd8\xff", b"\x89PNG", b"GIF8", b"WEBP"])
    except Exception:
        return False


def fetch_first_working_url(results: list) -> str:
    for item in results:
        url = item.get("image", "")
        if url and check_url_is_image(url):
            return url
    return ""


def ddg_search(query: str, retries: int = 3) -> list:
    for attempt in range(retries):
        wait = 3 * (attempt + 1)  # 3s, 6s, 9s
        print(f"  waiting {wait}s before search...")
        time.sleep(wait)
        try:
            return list(DDGS().images(query, max_results=10))
        except DDGSException as exc:
            print(f"  attempt {attempt + 1} failed: {exc}")
    return []


def main() -> None:
    print(f"\n🔍 Testing DuckDuckGo image search\n")

    print("Searching book cover...")
    results = ddg_search(f"{TEST_BOOK_TITLE} {TEST_BOOK_AUTHOR} book cover")
    cover_url = fetch_first_working_url(results)
    print(f"  {'✓' if cover_url else '✗'} BOOK COVER: {cover_url or f'no working URL ({len(results)} results tried)'}")

    print("Searching author portrait...")
    results = ddg_search(f"{TEST_AUTHOR_NAME} author portrait")
    author_url = fetch_first_working_url(results)
    print(f"  {'✓' if author_url else '✗'} AUTHOR: {author_url or f'no working URL ({len(results)} results tried)'}")

    passed = sum([bool(cover_url), bool(author_url)])
    print(f"\n  Results: {passed}/2 passed\n")


if __name__ == "__main__":
    main()
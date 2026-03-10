# Goodreads to Library Sync

This repository contains a local Python sync that reads a Goodreads CSV export, enriches author notes through the local `codex` CLI, downloads book and author images when possible, and generates an Obsidian-compatible vault under a chosen `--vault-root`.

## Output Layout

A sync creates or updates:

- `<vault-root>/Library.md` as the single hub note for the generated vault
- `<vault-root>/Authors/<Author>/<Author>.md` for author notes
- `<vault-root>/Authors/<Author>/Books/<Book>.md` for book notes
- `<vault-root>/Attachments/Covers/` for local book cover images
- `<vault-root>/Attachments/AuthorImages/` for local author images
- `<vault-root>/Templates/Book_Template.md` and `<vault-root>/Templates/Author_Template.md`
- `<vault-root>/Manual Review/Missing Metadata.md` for unresolved metadata issues

## Setup

Install dependencies with your conda Python:

```powershell
C:\Users\eduar\anaconda3\python.exe -m pip install -r requirements.txt
```

The script expects:

- `pandas`
- `requests`
- `python-frontmatter`
- `langdetect`
- `PyYAML`
- `ddgs`

It also expects the local `codex` CLI to be installed and already logged in, because author metadata is generated through `codex exec`.

## Input CSV

Put your Goodreads export inside `data/`, for example:

```text
data/goodreads_library_export.csv
```

The script expects the standard Goodreads export columns, including:

- `Book Id`
- `Title`
- `Author`
- `Publisher`
- `Original Publication Year`
- `ISBN`
- `ISBN13`
- `Binding`
- `Number of Pages`
- `Date Added`
- `Date Read`
- `Bookshelves`
- `Exclusive Shelf`
- `My Review`
- `Read Count`

## Main Commands

Incremental sync:

```powershell
C:\Users\eduar\anaconda3\python.exe code\sync_goodreads.py sync-goodreads --csv data\goodreads_library_export.csv --vault-root library_v2
```

Legacy-compatible default form without a subcommand:

```powershell
C:\Users\eduar\anaconda3\python.exe code\sync_goodreads.py --csv data\goodreads_library_export.csv --vault-root library_v2
```

Single-book utility:

```powershell
C:\Users\eduar\anaconda3\python.exe code\sync_goodreads.py add-book "Dune" --csv data\goodreads_library_export.csv --vault-root library_v2
```

Fetch only missing book covers:

```powershell
C:\Users\eduar\anaconda3\python.exe code\sync_goodreads.py fetch-images --csv data\goodreads_library_export.csv --vault-root library_v2
```

Retroactively normalize existing YAML and regenerate templates:

```powershell
C:\Users\eduar\anaconda3\python.exe code\sync_goodreads.py migrate-yaml --vault-root library_v2
```

## Sync Behavior

The normal rerun path is incremental.

- Existing books update from CSV changes.
- New books are added.
- Existing author notes are updated when their linked book list changes.
- Existing biographies are skipped unless missing or `--refresh-bio` is used.
- Existing countries are skipped unless missing or `--refresh-bio` is used.
- Existing author sex values are skipped unless missing or `--refresh-bio` is used.
- Existing book covers are skipped unless missing or `--refresh-images` is used.
- `fetch-images` rewrites the generated book header/YAML when it finds a new cover or repairs a stale note that already has a local cover file.
- Existing author images are skipped unless missing or `--refresh-images` is used.
- Managed note sections are updated in place; manual content outside those sections is preserved.
- The `## Quotes` section is preserved on reruns.
- CLI runs print compact per-book and per-author summaries, including YAML/note updates and image/provider outcomes.

## Frontmatter Rules

Entity fields stay wikilinked.

Book note frontmatter includes:

- `title`
- `author` as a YAML list of wikilinks
- `translator` as a YAML list of wikilinks, empty by default
- `status` as plain text such as `read` or `to-read`
- `rating`
- `read_count`
- `date_added`
- `date_read`
- `language`
- `publisher`
- `original_publish_year`
- `isbn`
- `isbn13`
- `pages`
- `format`
- `cover`
- `bookshelves` as wikilinks
- `reread_dates`
- `tags` as plain YAML values including `book`

Author note frontmatter includes:

- `name`
- `cover`
- `country` as a wikilink, defaulting to `[[Unknown]]`
- `birth_year`
- `death_year`
- `sex` as `male`, `female`, `unknown`, or empty when still unfilled
- `tags` as plain YAML values including `author`

Example book frontmatter:

```yaml
---
title: "Dune"
author:
  - "[[Authors/Frank Herbert/Frank Herbert|Frank Herbert]]"
translator: []
status: "read"
rating: 5
read_count: 1
date_added: "2026-01-01"
date_read: "2026-01-03"
language: "English"
publisher: "Ace"
original_publish_year: 1965
isbn: "0441172717"
isbn13: "9780441172719"
pages: 896
format: "physical"
cover: "[[Attachments/Covers/Frank Herbert - Dune.jpg]]"
bookshelves:
  - "[[science fiction]]"
  - "[[favorites]]"
reread_dates: []
tags:
  - "book"
---
```

Example author frontmatter:

```yaml
---
name: "Frank Herbert"
cover: "[[Attachments/AuthorImages/Frank Herbert.jpg]]"
country: "[[United States]]"
birth_year: "1920"
death_year: "1986"
sex: "male"
tags:
  - "author"
---
```

## Generated Note Structure

Book notes include generated blocks for:

- header
- quotes
- review

The body shape is:

```md
<!-- GENERATED:BOOK_HEADER START -->
# Title

![[Attachments/Covers/Author - Title.jpg|200]]
<!-- GENERATED:BOOK_HEADER END -->

<!-- GENERATED:BOOK_QUOTES START -->
## Quotes
<!-- GENERATED:BOOK_QUOTES END -->

<!-- GENERATED:BOOK_REVIEW START -->
## My Review
<!-- GENERATED:BOOK_REVIEW END -->
```

Author notes include generated blocks for:

- header
- biography
- linked books

The header block embeds the author image when one exists.

## Templates

Every sync/bootstrap writes:

- `Templates/Book_Template.md`
- `Templates/Author_Template.md`

These templates mirror the generated note structure and YAML layout so you can manually add future books/authors without Goodreads.

## Author Metadata

Author notes are generated in English and include:

- a biography focused on the author's life, significance, achievements, relationships, and context
- birth-death years when known
- country of origin in English
- sex as `male`, `female`, or `unknown` when inferred
- linked books from your library

The biography worker uses local `codex exec` with `gpt-5.1`, low reasoning, and runs with the configured author concurrency.

## Image Lookup

Book cover lookup uses this fallback chain:

1. Open Library
2. Wikimedia Commons
3. DuckDuckGo Images (`ddgs`) with image URL validation

Author image lookup uses:

1. Wikimedia Commons
2. DuckDuckGo Images (`ddgs`) with image URL validation

The first successful image is downloaded into the appropriate attachments folder.

`code/testImages.py` is a diagnostic helper only: it shows provider-by-provider results for sampled books/authors, but it does not download or write files into the vault.

## Manual Review Workflow

Unresolved issues are collected into:

`<vault-root>/Manual Review/Missing Metadata.md`

This note tracks items such as:

- missing covers
- failed author metadata generation
- broken book materialization
- missing bookshelves
- missing ISBN / ISBN13
- missing authors
- API errors
- CSV parse issues

The review note is idempotent: resolved items disappear on later syncs, and duplicates are not added.

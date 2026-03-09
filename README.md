# Goodreads to Library Sync

This repository contains a local Python sync that reads a Goodreads CSV export, enriches author notes through the local `codex` CLI, downloads covers when possible, and generates an Obsidian-compatible vault under `library_v2/`.

## Output Layout

A sync creates or updates:

- `library_v2/Library.md` as the single hub note for the generated vault
- `library_v2/Authors/<Author>/<Author>.md` for author notes
- `library_v2/Authors/<Author>/Books/<Book>.md` for book notes
- `library_v2/Attachments/Covers/` for local cover images
- `library_v2/Manual Review/Missing Metadata.md` for unresolved metadata issues

The old `library/` as the generated output.

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

Incremental monthly sync:

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

Fetch only missing covers:

```powershell
C:\Users\eduar\anaconda3\python.exe code\sync_goodreads.py fetch-images --csv data\goodreads_library_export.csv --vault-root library_v2
```

Retroactively normalize existing YAML:

```powershell
C:\Users\eduar\anaconda3\python.exe code\sync_goodreads.py migrate-yaml --vault-root library_v2
```

Refresh flags for `sync-goodreads` and `add-book`:

- `--refresh-goodreads` rewrites Goodreads-derived metadata
- `--refresh-bio` regenerates author biography and country
- `--refresh-images` refetches covers even when a local image already exists
- `--force-refresh-metadata` is the legacy alias that enables all three refresh flags together

## Sync Behavior

The normal rerun path is incremental.

- Existing books update from CSV changes.
- New books are added.
- Existing author notes are updated when their linked book list changes.
- Existing biographies are skipped unless missing or `--refresh-bio` is used.
- Existing countries are skipped unless missing or `--refresh-bio` is used.
- Existing local covers are skipped unless missing or `--refresh-images` is used.
- Managed note sections are updated in place; manual content outside those sections is preserved.

If you have old notes from a previous topology, run `migrate-yaml` once to normalize them.

## Frontmatter Rules

The vault is generated for cleaner Obsidian graph behavior.

Book note frontmatter includes:

- `title`
- `author` as a wikilink
- `status` as plain text such as `read` or `to-read`
- `rating`
- `read_count`
- `date_added`
- `date_read`
- `language`
- `isbn`
- `isbn13`
- `pages`
- `format`
- `cover`
- `bookshelves` as wikilinks
- `tags` as plain YAML values including `book`

Author note frontmatter includes:

- `name`
- `country` as a wikilink, defaulting to `[[Unknown]]`
- `tags` as plain YAML values including `author`

Example book frontmatter:

```yaml
---
title: "Dune"
author: "[[Authors/Frank Herbert/Frank Herbert|Frank Herbert]]"
status: "read"
rating: 5
read_count: 1
date_added: "2026-01-01"
date_read: "2026-01-03"
language: "English"
isbn: "0441172717"
isbn13: "9780441172719"
pages: 896
format: "physical"
cover: "[[Attachments/Covers/Frank Herbert - Dune.jpg]]"
bookshelves:
  - "[[science fiction]]"
  - "[[favorites]]"
tags:
  - "book"
---
```

## Author Metadata

Author notes are generated in English and include:

- a biography focused on the author's life, significance, achievements, relationships, and context
- birth-death years when known
- country of origin in English
- linked books from your library

The biography worker uses local `codex exec` with `gpt-5.1`, medium reasoning, and runs up to five authors at a time.

## Cover Lookup

Cover lookup uses a fallback chain:

1. Open Library
2. Google Books
3. Wikipedia page image

The first successful image is downloaded into `library_v2/Attachments/Covers/`.

## Manual Review Workflow

Unresolved issues are collected into:

`library_v2/Manual Review/Missing Metadata.md`

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

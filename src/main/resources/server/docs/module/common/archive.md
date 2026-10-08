---
type: Common
title: Archive (zip / tar)
description: The archive parameter block for reading files packed in zip or tar archives — entry selection by glob, entry-level semantics, entry name charset and how archives are read.
tags: [common, archive, zip, tar, tar.gz, storage]
timestamp: 2026-10-07T00:00:00Z
---

# Archive (zip / tar)

An archive holds several files in one file. With the `archive` block, each matched input file is
opened as an archive and **every selected entry is read as a file of its own**: an archive is a
virtual directory, an entry a virtual file.

> **Module support:** the [storage source](../source/storage.md), for the `csv`, `json` and `fwf` formats.

```yaml
sources:
  - name: orders
    module: storage
    parameters:
      input: "gs://my-bucket/packs/PACK*.zip"
      format: csv
      skipHeaderLines: 1
      archive:
        entries: ["orders/*.csv"]
      schema:
        fields:
          - { name: id, type: int64 }
          - { name: amount, type: float64 }
```

## Parameters

| parameter   | optional | type           | description |
|-------------|----------|----------------|-------------|
| type        | optional | Enum           | `zip` or `tar`. Omitted: told from the file name — `.zip` → zip; `.tar`, `.tar.gz`, `.tgz`, `.tar.bz2`, `.tbz2`, `.tar.zst` → tar. A file whose name says neither is a failure; set `type` for such files (e.g. a `.zip.gz`). |
| entries     | optional | Array<String\> | Globs selecting the entries to read, matched against the entry path inside the archive (e.g. `data/2024/a.txt`). `*` matches within one path segment, `**` across segments, `?` one character; matching is case-sensitive. Omitted: every file entry. |
| exclude     | optional | Array<String\> | Globs of entries to leave out, applied after `entries`. |
| nameCharset | optional | String         | Charset of the entry names. Default `UTF-8`. Zip entries flagged as UTF-8 are always read as UTF-8; set this for archives whose names are in another charset (e.g. `windows-31j` for a zip made on Japanese Windows) when `entries` / `exclude` have non-ASCII patterns. |

An empty block (`archive: {}`) reads every file entry of the archive.

## Entry semantics

Everything that is "per file" without an archive is "per entry" with one:

| | behavior |
|---|---|
| `skipHeaderLines`, record numbers (`additionalFields.line`) | Restart for every entry. |
| `additionalFields.entry` | The entry path (with `/` separators). `additionalFields.resource` stays the archive file. |
| Directory entries | Ignored. |
| Links (symbolic or hard) | Ignored: a link is not a file. A zip records links only in its table of contents, so a zip that is read front to back (`.zip.gz`) can not tell and reads the link's target path as content — exclude such entries with `exclude`. |
| An entry that is a compressed file (`*.gz`, `*.bz2`, `*.zst` ...) | Decompressed, told from the entry name. `compression` applies to the archive file only. |
| An archive inside the archive | Not opened: it is an ordinary entry, read only if selected. |
| An archive with no matching entry | Normal: it produces no record. |
| An encrypted zip | Not supported: the archive is a failure. |
| A record that can not be decoded | A record failure, as without an archive; the failure names the entry. |
| An I/O error inside an archive (truncated, corrupted) | A failure of the archive: the entries already read stay in the output and the rest is not read (see `failFast` in the module docs). |

## Several kinds of files in one archive

When an archive packs several kinds of files, the storage source's `partitions` routes its entries to
named outputs, each with its own format and schema: the archive is read once, and an entry goes to
the first partition whose patterns match it. See *Partitions parameters* in the
[storage source](../source/storage.md).

## Compression around an archive

`compression` (or the file name) tells the compression of the archive file itself: a `.tar.gz` is a
gzip-compressed tar, a `.zip.gz` a gzip-compressed zip (declare `type: zip`, the name does not say it).
A zip compresses its entries itself and needs no `compression`.

Supported compressions (around an archive, and of an entry): gzip (`.gz`), bzip2 (`.bz2`), zstd (`.zst`), lzo, deflate, snappy. A file or entry whose name says another one (`.xz`, `.txz`, `.lzma`, `.lz4`, `.lz`, `.Z`, `.7z`, `.rar`, `.lzh`) is a failure: it would otherwise be read as garbage.

`compression: ZIP` is **not** the archive reader: it is the legacy way to read a zip, where all the
entries are concatenated into one stream (so `skipHeaderLines` skips only the header of the first
entry and entries can not be selected). It can not be combined with `archive`.

## How archives are read

| archive | reading |
|---|---|
| zip on GCS / S3 / a local disk | The table of contents at the end of the file is read first, then **only the selected entries** (range reads). Entries that are not selected cost nothing, so several sources can each read their own entries of the same large archive cheaply. |
| zip inside another compression (`.zip.gz`) | Read front to back; entries that are not selected are read past. |
| tar, tar.gz ... | Read front to back (a tar has no table of contents); entries that are not selected are read past. |

One archive is read by one worker, entry after entry: prefer many archives over one very large one.

## Examples

A pack of several record types: read one of them, with fixed-width records.

```yaml
sources:
  - name: orders
    module: storage
    parameters:
      input: "gs://my-bucket/packs/PACK*.zip"
      archive:
        entries: ["ORD*.txt"]
      schema:
        encoding: { format: fwf, charset: windows-31j }
        reference: { uri: gs://my-bucket/layouts/orders.fwf.json }
      additionalFields:
        resource: source_file
        entry: source_entry
        line: source_line
```

A tar.gz of daily gzip logs, without the temporary files:

```yaml
sources:
  - name: logs
    module: storage
    parameters:
      input: "gs://my-bucket/logs/2024-*.tar.gz"
      format: json
      archive:
        entries: ["**/*.json.gz"]
        exclude: ["**/tmp/**"]
      schema:
        fields:
          - { name: time, type: timestamp }
          - { name: message, type: string }
```

---
type: Common
title: Template
description: The text template syntax (FreeMarker) used for per-record text generation and dynamic parameters — select text func, files sink paths, queries, row keys, prompts.
tags: [common, template, freemarker, text]
timestamp: 2026-07-17T00:00:00Z
---

# Text Templates

Many string parameters are evaluated as [FreeMarker](https://freemarker.apache.org/) templates.
A template embeds runtime values with `${...}` and supports FreeMarker directives
(`<#if>`, `<#list>`, built-ins like `?upper_case`, `?string`, ...).

There are two template layers — do not confuse them:

1. **Config-file templating** — `${args.name}` placeholders anywhere in the config file are
   substituted once at load time from `system.args` and runtime arguments (see the
   [system block reference](../../system.md)). Placeholders that do not match a known variable are
   left untouched, so a config file can safely contain the record-level templates below.
2. **Per-record templates** — module parameters documented as "template" are evaluated per record
   (or per request), with the record's field values bound to the field names.

The rest of this document describes the per-record layer.

## Where templates are used

| module (type) | templated parameters |
|---|---|
| select / partition / aggregation (transform) | `text` select function — generates a string field from other fields |
| files (sink) | output path and content templates — per-record dynamic file names |
| storage / bigquery / datastore / firestore / pubsub (sink) | dynamic destination parameters (e.g. bucket paths, attributes) |
| bigtable (source / transform / sink) | row key templates (e.g. `${userId}#${utils.bigtable.reverseTimestampMicros(timestamp)}`) |
| bigquery / jdbc / spanner (source) | query text (combined with config args; per-microbatch variables in streaming) |
| http (source / transform) | request URL / body templates |
| vertexai.gemini (transform) | prompt templates |
| debug (sink) | log message template |

Consult each module's documentation for which of its parameters accept templates.

## Available variables

- **Input record fields** — each field of the input schema is bound by its name:
  `${userId}`, `${amount?string("0.##")}`, `${createdAt}`.
- **`context.timestamp`** — the element's event timestamp (available in modules that bind context
  variables, e.g. files sink paths).
- **`utils.*`** — built-in function namespaces, available in every template (see below).
- **`statics`** — access to Java static methods, e.g.
  `${statics["java.lang.Math"].min(field1, field2)}`.
- Legacy aliases `__StringUtils` / `__DateTimeUtils` exist in some modules; prefer `utils.string`
  and `utils.datetime`.

## Built-in `utils` functions

### utils.string

| function | description |
|---|---|
| `format(format, args...)` | `String.format` style formatting |
| `uuid()` | random UUID string |
| `replaceAll(str, from, to)` | regex replace (also accepts date/timestamp values) |
| `reverse(text)` | reverse a string |

### utils.datetime

| function | description |
|---|---|
| `formatTimestamp(ts)` / `formatTimestamp(ts, pattern)` / `formatTimestamp(ts, pattern, timezone)` | format a timestamp (epoch micros or Instant) |
| `formatDate(date)` / `formatDate(date, pattern)` | format a date (epoch days, LocalDate, or string) |
| `formatTime(time)` / `formatTime(time, pattern)` | format a time value |
| `currentDate(zone[, plusAmount[, unit]])` | current date in a timezone, optionally shifted |
| `currentTime(zone[, plusAmount[, unit]])` | current time in a timezone, optionally shifted |
| `currentDateTime(zone[, plusSeconds[, pattern]])` | current datetime string |
| `currentTimestamp([plusAmount[, unit]][, truncateUnit])` | current timestamp, optionally shifted/truncated |
| `currentEpochSeconds([plusSeconds])` | current epoch seconds |

### utils.bigtable

| function | description |
|---|---|
| `reverseTimestampMicros(ts)` / `reverseTimestampMillis(ts)` | `Long.MAX_VALUE - timestamp` for descending row-key ordering |

### utils.gcp

| function | description |
|---|---|
| `project()` | default GCP project ID of the execution environment |
| `account()` / `serviceAccount()` | current account / service account |
| `secret(resource)` | Secret Manager secret payload |
| `accessToken()` | OAuth2 access token of the current credentials |
| `signJwt(serviceAccount, params...)` | sign a JWT with a service account |

### utils.oauth / utils.secrets

| function | description |
|---|---|
| `oauth.clientCredentials(endpoint, clientId, clientSecret, scope)` | OAuth2 client-credentials token request |
| `secrets.get(reference)` | read a secret by reference |

## Examples

Per-record dynamic file path (files sink):

```yaml
sinks:
  - name: out
    module: files
    inputs: [records]
    parameters:
      output: gs://mybucket/out/${category}/${utils.datetime.formatTimestamp(context.timestamp, "yyyyMMdd")}/${id}.json
```

String field generation (select transform, `text` function):

```yaml
transforms:
  - name: enrich
    module: select
    inputs: [input]
    parameters:
      select:
        - name: label
          func: text
          text: "${userName} (${country?upper_case})<#if premium>, premium</#if>"
```

Descending-order Bigtable row key:

```yaml
parameters:
  rowKey: ${userId}#${utils.bigtable.reverseTimestampMicros(eventTime)}
```

## Notes

- Numbers are rendered in "computer" format (no locale grouping separators) — use
  `?string("...")` or `utils.string.format` for explicit formatting.
- A strict template fails the record when a referenced variable is missing; check field
  nullability and provide defaults with FreeMarker's `!` operator (e.g. `${nickname!"anonymous"}`).

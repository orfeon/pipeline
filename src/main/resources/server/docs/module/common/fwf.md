---
type: Common
title: Fixed-Width Format (fwf)
description: Schema declaration for fixed-width text records (fields located by byte or character position and length) — encoding options, the layout document, field attributes and value conversion rules.
tags: [common, schema, fwf, fixed-width, encoding, shift_jis]
timestamp: 2026-10-06T00:00:00Z
---

# Fixed-Width Format (fwf)

A fixed-width record has no delimiters: each field is a fixed range of the record, located by its
position and length. The fwf format decodes such records into typed (optionally nested and
repeated) fields.

> **Module support:** the [storage source](../source/storage.md) reads fwf files (`format: fwf`), and the
> select function [`fwf_decode`](../transform/select.md#fwf_decode) decodes a record held in a bytes / string field.

## Declaration

fwf uses the three keys of the [schema](schema.md) block:

| key         | role in fwf                                                                                          |
|-------------|------------------------------------------------------------------------------------------------------|
| `encoding`  | `format: fwf` plus the conversion options (charset, trimming, error handling).                       |
| `reference` | The **layout document** — which range maps to which field. `uri` (a file) or `inline`.               |
| `fields`    | Optional. Omitted: the output fields are derived from the layout. Declared: a projection — only the listed top-level fields are output; each name must exist in the layout with the same type. |

```yaml
schema:
  encoding:
    format: fwf
    charset: windows-31j
  reference:
    uri: gs://my-bucket/layouts/orders.fwf.json
```

```yaml
schema:
  encoding:
    format: fwf
  reference:
    inline:                         # the layout written inline as an object
      recordLength: 20
      fields:
        - { name: code,   type: string,  pos: 1, len: 4 }
        - { name: amount, type: decimal, pos: 5, len: 6, scale: 2 }
        - { name: day,    type: date,    pos: 11, len: 8, pattern: yyyyMMdd }
```

## encoding options

| parameter        | optional | type    | description |
|------------------|----------|---------|-------------|
| format           | required | Enum    | `fwf`. |
| charset          | optional | String  | Charset of the record bytes. Default `UTF-8`. For Shift_JIS data use **`windows-31j`**: strict `Shift_JIS` has no mapping for NEC special characters (e.g. ㈱) and IBM extensions that appear in real data. Bytes the charset can not decode are never replaced with U+FFFD: they are a parse error (see `onParseError`). |
| unit             | optional | Enum    | Unit of `pos` / `len`. `byte` (default): positions in the encoded bytes — each range is cut from the bytes first and decoded on its own, so multi-byte characters never shift the following fields. `char`: positions in decoded characters (code points), for layouts defined by character count. |
| trim             | optional | Enum    | Trimming of `string` / `json` values: `both` (default), `left`, `right`, `none`. Half-width spaces and the ideographic space (U+3000) are trimmed. |
| emptyAsNull      | optional | Boolean | A `string` / `json` value that is empty after trimming becomes null. Default `true`. Other types always become null when blank. |
| onLengthMismatch | optional | Enum    | When the record length differs from the layout `recordLength`: `fail` (default) — the record is a failure; `pad` — ranges beyond the record end are null and extra bytes are ignored (e.g. reading older, shorter records with the latest layout). |
| onParseError     | optional | Enum    | When a value can not be converted to its type: `fail` (default) — the record is a failure; `null` — only that field becomes null. Bytes that the charset can not decode count as a parse error of the field they are in; with `unit: char` (and for an input held as text that the charset can not encode) the whole record is a failure regardless of this option. |

## Layout document

A JSON (or YAML) object; an array alone is accepted as the `fields` shorthand.

| key          | optional | type    | description |
|--------------|----------|---------|-------------|
| recordLength | optional | Integer | Length of one record **excluding the line separator** (in `unit`). When set, every record is checked against it (see `onLengthMismatch`), and fields must fit within it. |
| fields       | required | Array   | The layout fields (below). |
| description  | optional | String  | Description of the record; becomes the schema description. |

### Field attributes

A layout field is a schema field (`name`, `type`, `mode`, `description`, `options`) plus position
and conversion attributes. Unknown keys are errors.

| attribute    | applies to | description |
|--------------|------------|-------------|
| name         | all        | Required. Unique within its scope. |
| type         | all        | `string` (default for a leaf), `bool`, `json`, `bytes`, `int16`, `int32`, `int64`, `float32`, `float64`, `decimal`, `date`, `time`, `timestamp`. A field with `fields` is a record (omit `type` or set `element` / `record`). |
| mode         | all        | `nullable` (default) or `required` (a null value makes the record a failure). Arrays come from `repeat`, not from `mode`. |
| description  | all        | Field description; carried to the output schema (shown in the dry-run output). |
| options      | all        | Free-form key/value metadata carried to the output schema. |
| pos          | all        | 1-based start position, **relative to the enclosing scope** (the record for top-level fields, the group element for children). Omitted: right after the previous field. |
| len          | leaf       | Required. Length of one value. |
| repeat       | all        | Number of consecutive elements; the field becomes an array. A leaf occupies `len × repeat`, a group `size × repeat`. An array holds no null and keeps every position: an empty element is the field's `defaultValue`, or the empty text for a `string` / `json` field; without either the record is a failure. To keep "no value" in a repeated field (e.g. a repeated date), make it a repeated group with one field — the fields of a group can be null. |
| fields       | group      | Child fields. Without `repeat`: a nested record. With `repeat`: an array of records. Nesting depth is unlimited. |
| size         | group      | Length of one group element. Omitted: the extent of its children. |
| scale        | decimal, float32, float64 | Implied decimal places: applied only when the value has **no** decimal point (`0012` with `scale: 1` → `1.2`). A value with a decimal point (` 12.3`) is read as is. |
| pattern      | date, time, timestamp | Java `DateTimeFormatter` pattern (e.g. `yyyyMMdd`, `HHmm`). Omitted: ISO-8601. |
| zone         | timestamp  | Time zone for a `pattern` without an offset (e.g. `Asia/Tokyo`). Default `UTC`. Requires `pattern`: an ISO-8601 value without an offset is always UTC. |
| radix        | int16, int32, int64 | Radix of the digits (e.g. `16`). Default `10`. |
| nullIf       | leaf except bytes | Raw value(s) (compared after trimming both sides) that become null, e.g. `["00000000", "----"]`. In YAML, quote values that look like numbers: an unquoted `00000000` is read as the number `0` and never matches. |
| trim         | string, json | Overrides `encoding.trim` for this field. |
| defaultValue | leaf except bytes | Value used when the result is null (blank, `nullIf`, beyond the record end, or `onParseError: null`). E.g. `0` for fields where blank means zero. Written like a raw value of the field, so `pattern`, `radix` and the implied `scale` apply to it (with `scale: 1`, `5` means `0.5`; write `5.0` for five). |

Ranges may overlap (a composite key can be read both whole and as its parts) and gaps are skipped,
so reserved areas and the line separator need not be declared.

## Value conversion

1. Cut the range (`pos`, `len`, `repeat`).
2. `string` / `json`: trim per `trim`; empty → null when `emptyAsNull`.
   Other types are parsed from the text trimmed on both sides; blank → null.
3. `nullIf` match → null.
4. Convert:
   - integers: an optional leading `+` / `-`, which may be separated from the digits by spaces (`+12`, `- 4`), digits in `radix`.
   - `decimal` / `float32` / `float64`: the same sign handling and an optional decimal point; `scale` applies when there is no decimal point.
   - `bool`: `1` / `true` / `t` / `y` / `yes` and `0` / `false` / `f` / `n` / `no` (case-insensitive).
   - `date` / `time` / `timestamp`: `pattern` (and `zone`), or ISO-8601.
   - `bytes`: the raw range, not decoded.
5. null → `defaultValue`; still null in a `required` field → failure. Still null as an element of a repeated leaf → the empty text for `string` / `json`, a failure for the other types.

Domain-specific meaning (combining a sign field with a value field, sentinel codes such as `999`,
code-table lookups, time formats like `mss.S`) is out of scope of the decoder: do it downstream with
[select](select.md) or the `query` transform.

## Example

A record (windows-31j, 49 bytes) with a nested key, a repeated group and full-width text:

```
pos  1-6   key: shop code (4) + register no (2)
pos  7-26  shop name: full-width, padded with ideographic spaces (20 bytes)
pos 27-31  total " 12.5" (explicit decimal point)
...        followed by 3 items of { sku (4), qty (2) } — 18 bytes
```

```yaml
schema:
  encoding:
    format: fwf
    charset: windows-31j
  reference:
    inline:
      recordLength: 49
      fields:
        - name: key
          fields:
            - { name: shop,     type: string, len: 4 }
            - { name: register, type: int32,  len: 2 }
        - { name: shopName, type: string,  len: 20, description: "full-width name" }
        - { name: total,    type: decimal, len: 5 }
        - name: items
          repeat: 3
          fields:
            - { name: sku, type: string, len: 4 }
            - { name: qty, type: int32,  len: 2, defaultValue: 0 }
```

Output fields: `key` (record), `shopName` (string), `total` (decimal), `items` (array of records).

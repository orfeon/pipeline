---
type: Common
title: Filter
description: The filter condition syntax shared by modules that select records (select, partition, aggregation, files, ...).
tags: [common, filter, condition]
timestamp: 2026-07-17T00:00:00Z
---

# Filter condition

A filter condition can be written in two equivalent ways:

* **SQL-like text** — a single string such as `price > 100 AND category IN ('a', 'b')`
* **JSON conditions** — objects specifying the field (`key`), the comparison operator (`op`), and the value to be compared (`value`)

Both are translated into the same internal condition tree at pipeline setup, so the filtering performance is identical. Use whichever is easier to read.

## SQL-like condition text

Instead of JSON objects, you can write the whole condition as one SQL-like string:

```yaml
filter: price > 100 AND (category IN ('a', 'b') OR name LIKE 'sale%')
```

Supported syntax:

* Comparison operators: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`
* Logical operators: `AND`, `OR`, `NOT` (parentheses for grouping)
* `IN (...)`, `NOT IN (...)`
* `LIKE`, `NOT LIKE` (`%` and `_` wildcards)
* `BETWEEN x AND y`, `NOT BETWEEN x AND y`
* `IS NULL`, `IS NOT NULL`
* Boolean fields can be used directly: `flag`, `NOT flag`, `flag = true`
* **Field-to-field comparison**: both sides of a comparison can be field names, e.g. `updatedAt > createdAt` or `name != nickname`
* Numeric arithmetic: `price * qty > total`, `(field1 - field2) / field3 >= 1` (values are evaluated as double, same as the `expression` attribute)
* Nested fields with dots: `attr.category = 'x'`

Date, timestamp and enum comparison:

* **Timestamp** fields compare against string literals in ISO format (`ts >= '2024-01-01T00:00:00Z'`), space-separated format with optional offset (`ts >= '2024-01-15 19:00:00+09:00'`), plain dates meaning midnight UTC (`ts > '2024-01-01'`), or SQL literals (`ts >= TIMESTAMP '2024-01-15 10:30:00'`, interpreted as UTC)
* **Date** fields compare against `'yyyy-MM-dd'` / `'yyyy/MM/dd'` strings or `DATE '2024-01-15'` literals, and work with `BETWEEN`: `d BETWEEN '2024-01-01' AND '2024-03-31'`
* **Time** fields compare against `'HH:mm'` / `'HH:mm:ss'` strings
* **Enum** fields are resolved to their symbol and compared as strings: `status = 'ACTIVE'`, `status IN ('ACTIVE', 'DONE')`

Notes:

* String literals use single quotes (`'a'`). Field names that collide with SQL reserved words must be back-quoted: `` `timestamp` >= '2024-01-01T00:00:00Z' ``
* Functions on strings, subqueries and other full SQL features are not supported here — use the `query` transform for those.

## JSON conditions

The filter condition of a record specifies three things: the field to be filtered, the comparison operator, and the value to be compared.

For example, in the following description, the record with a field1 value of 0 will be selected.

```yaml
filter:
  - { key: field1, op: "=", value: 0 }
```

If you specify multiple filter conditions, each filter condition will be combined with AND condition.

For example, the following filter condition is the same as the description.

`(field1 IS NOT NULL AND field2 >= 10 AND field3 IN ["a", "b", "c"])`

```yaml
filter:
  - { key: field1, op: "!=", value: null }
  - { key: field2, op: ">=", value: 10 }
  - { key: field3, op: in, value: [a, b, c] }
```

If you want to combine filter conditions with OR, or define multiple filter conditions nested together, you can do so as follows.

For example, the following filter condition is the same as the description.

`(field1 = 0 OR field2 < 10 OR (field3 = "a" AND field4 NOT IN [0, 5]))`

```yaml
filter:
  or:
    - { key: field1, op: "=", value: 0 }
    - { key: field2, op: "<", value: 10 }
    - and:
        - { key: field3, op: "=", value: a }
        - { key: field4, op: not in, value: [0, 5] }
```

In this example, as a filter condition, instead of an array, we specify an object with the name `and` or `or` property whose value is an array of filter conditions.
It can be nested in filter conditions.

Currently, the following comparison operators are supported:

`=`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`, `match`, `not match`

`match` / `not match` test the field value against a regular expression given as `value`.

The fields that can be used for comparison must be of type string or numeric or date or timestamp.

## Compare two fields (`valueKey`)

To compare a field against another field of the same record, use `valueKey` instead of `value`:

```yaml
filter:
  - { key: updatedAt, op: ">", valueKey: createdAt }
  - { key: name, op: "!=", valueKey: nickname }
```

This is the JSON equivalent of the SQL-like `updatedAt > createdAt AND name != nickname`.
`valueKey` supports the comparison operators (`=`, `!=`, `>`, `>=`, `<`, `<=`) and compares values natively
(numeric values are compared exactly without double conversion). It cannot be combined with `in` / `match` ops.

## Compare values by expressions

In addition to comparing field values as single values, you can also compare values by expressions using values from multiple fields.

You can define formulas with the `expression` attribute instead of the `key` attribute.
The formula can use the field names of the record.
The values of these fields are treated as double types.

For example, the following configuration will extract records for which the formula result of the `expression` attribute is greater than or equal to 1.

```yaml
filter:
  - { expression: "(field1 - field2) / field3", op: ">=", value: 1 }
  - { expression: "if(field1 - field2 > 0, field3 * 0.5, field4) / field5", op: "<", value: 1 }
```

For more information on expression's detailed functionality, please refer to [Expression](expression.md).

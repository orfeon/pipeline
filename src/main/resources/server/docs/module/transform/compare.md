---
type: Transform Module
title: Compare Transform Module
description: Compares records from multiple inputs by primary key and outputs mismatch reports. Records from all inputs are grouped by the specified primaryKeyFields, and for each key the transform detects inputs missing a record, inputs contributing duplicate records, and field-level value differences across inputs. Only keys with at least one mismatch produce an output record, which makes the module useful for data migration validation and cross-datastore consistency checks.
tags: [transform, compare, validation, consistency, migration, batch]
timestamp: 2026-08-06T00:00:00Z
---

# Compare Transform Module

Transform Module for comparing records across two or more inputs. Records from all inputs are grouped by a primary key built from `primaryKeyFields`, and each group is checked for consistency:

- **Missing inputs** - Inputs that did not contribute a record for the key.
- **Duplicated inputs** - Detected when some input contributed more than one record for the key.
- **Field differences** - For every field of the (union) input schema, the values from all inputs are compared. If the values are not all identical, the field name and the per-input values are recorded. Binary (`bytes`) fields are compared by their Base64 encoding.

Only keys where at least one of these checks fails produce an output record; keys where all inputs agree produce no output. Each detected mismatch is also logged at `error` level.

Typical use case: validating a data migration by reading the same table from the old and the new datastore (e.g. BigQuery vs Spanner) and reporting every key whose records differ.

## Transform module common parameters

| parameter  | optional | type                              | description                                                                                        |
|------------|----------|-----------------------------------|----------------------------------------------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.                                                  |
| module     | required | String                            | Specified `compare`                                                                                |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be compared. Specify two or more inputs.                         |
| waits      | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.                                      |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                                                        |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                                                        |

## Compare transform module parameters

| parameter        | optional | type           | description                                                                                                                             |
|------------------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| primaryKeyFields | required | Array<String\> | Field names used to build the grouping key. Records from all inputs that share the same values for these fields are compared with each other. |

## Output schema

The transform produces a fixed report schema (one record per key with mismatches):

| field            | type                    | description                                                                                                   |
|------------------|-------------------------|---------------------------------------------------------------------------------------------------------------|
| table            | String                  | The transform step name (useful when comparing several tables in one pipeline and merging the reports).       |
| keys             | String                  | The primary key values of the compared group.                                                                 |
| missingInputs    | Array<String\>          | Names of the inputs that did not contribute a record for this key.                                            |
| duplicatedInputs | Array<String\>          | Non-empty when some input contributed more than one record for this key.                                      |
| differences      | Array<Record\>          | One entry per field whose values differ across inputs. Each entry has `field` (String, the field name) and `values` (Map<String,String\>, input name to that input's value as a string). |

## Examples

### Example 1: Validate a migration from BigQuery to Spanner

Read the same logical table from both datastores, compare by primary key, and write the mismatch report to BigQuery.

```yaml
sources:
  - name: old_users
    module: bigquery
    parameters:
      query: "SELECT user_id, name, email, updated_at FROM `myproject.mydataset.users`"

  - name: new_users
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      query: "SELECT user_id, name, email, updated_at FROM users"

transforms:
  - name: users_compare
    module: compare
    inputs:
      - old_users
      - new_users
    parameters:
      primaryKeyFields:
        - user_id

sinks:
  - name: report
    module: bigquery
    inputs:
      - users_compare
    parameters:
      table: "myproject.mydataset.users_compare_report"
      writeDisposition: WRITE_TRUNCATE
      createDisposition: CREATE_IF_NEEDED
```

If the two inputs are fully consistent, `users_compare` outputs no records and the report table stays empty.

### Example 2: Compare with a composite key and inspect results in logs

Use multiple fields as the primary key and print each mismatch with the debug sink.

```yaml
transforms:
  - name: orders_compare
    module: compare
    inputs:
      - orders_a
      - orders_b
    parameters:
      primaryKeyFields:
        - order_id
        - line_no

sinks:
  - name: debug_report
    module: debug
    inputs:
      - orders_compare
    parameters:
      logLevel: warn
```

A mismatch record looks like:

```json
{
  "table": "orders_compare",
  "keys": "0001#3",
  "missingInputs": [],
  "duplicatedInputs": [],
  "differences": [
    {
      "field": "amount",
      "values": {"orders_a": "1200", "orders_b": "1300"}
    }
  ]
}
```

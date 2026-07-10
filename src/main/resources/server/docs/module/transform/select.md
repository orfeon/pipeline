---
type: Transform Module
title: Select Transform Module
description: Filters rows and transforms field values. Supports 27 select functions (pass, cast, rename, constant, replace, expression, text, concat, nullif, uuid, hash, event_timestamp, current_timestamp, struct, json, json_path, http, scrape, generate, bytes_encode, bytes_decode, base64_encode, base64_decode, reshape, tokenize_encode, tokenize_decode, lag) with implicit function detection, FreeMarker templates, mathematical expressions, nested structs, and stateful windowed operations. Works in both batch and streaming modes.
tags: [transform, select, filter, batch, streaming, field, projection]
timestamp: 2026-06-23T00:00:00Z
---

# Select Transform Module

Transform Module for filtering rows by specified filter conditions and transforming field values using select functions. This is the most versatile transform module, providing a wide range of field-level operations from simple renaming to complex stateful computations.

Supports:

- **Row filtering** - Filter records using [Filter](../common/filter.md) conditions.
- **Field projection** - Select, rename, and reorder fields.
- **Type casting** - Convert field values to different types.
- **Computed fields** - Create new fields using mathematical expressions (exp4j) or FreeMarker templates.
- **Nested structures** - Build nested elements and JSON objects.
- **Stateful operations** - Access previous row values (lag) and compute windowed aggregations using `groupFields`.
- **Array flattening** - Unnest array fields into multiple records.
- **27 select functions** - Extensive library of built-in field transformation functions.

## Transform module common parameters

| parameter  | optional | type                              | description                                                           |
|------------|----------|-----------------------------------|-----------------------------------------------------------------------|
| name       | required | String                            | Step name. specified to be unique in config file.                     |
| module     | required | String                            | Specified `select`                                                    |
| inputs     | required | Array<String\>                    | Specify the names of the steps to be used as input.                   |
| waits      | optional | Array<String\>                    | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.                           |
| parameters | required | Map<String,Object\>               | Specify the following individual parameters                          |

## Select transform module parameters

At least one of `filter` or `select` is required.

| parameter    | optional           | type                                   | description                                                                                                                                                                                                                   |
|--------------|--------------------|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| filter       | selective required | [Filter](../common/filter.md)          | Filter condition for rows. Records that do not match are discarded. At least one of `filter` or `select` is required.                                                                                                         |
| select       | selective required | Array<[SelectField](#select-fields)\>  | List of field definitions for projecting, renaming, transforming, or computing output fields. At least one of `filter` or `select` is required. When specified, only the defined fields are included in the output.             |
| flattenField | optional           | String                                 | Name of an array field to flatten (unnest). Each array element produces a separate output record. Applied after `select`.                                                                                                     |
| groupFields  | optional           | Array<String\>                         | Field names for grouping. Required when using stateful select functions (e.g. `lag`, windowed aggregations). Records are grouped by these fields and stateful functions operate within each group independently.                |

## Processing flow

1. **Filter** - If `filter` is specified, records not matching the condition are discarded.
2. **Select** - If `select` is specified, each select field function is applied in order. Output fields from earlier functions can be referenced by later functions.
3. **Flatten** - If `flattenField` is specified, the named array field is unnested into multiple records.

When `groupFields` is specified and select functions include stateful functions, records are grouped by key and processed with per-key state (ordered by event time in batch mode).

## Select fields

Each element in the `select` array defines one output field. A select field is a JSON object with at minimum a `name` parameter and additional parameters that determine the transformation function.

### Common parameters

| parameter | optional | type    | description                                                                                                       |
|-----------|----------|---------|-------------------------------------------------------------------------------------------------------------------|
| name      | required | String  | Output field name.                                                                                                |
| func      | optional | Enum    | Select function name. Can also be specified as `op`. If omitted, the function is [auto-detected](#auto-detection). |
| ignore    | optional | Boolean | If `true`, the field is computed but excluded from the output schema. Useful for intermediate calculations. Default: `false`. |

### Auto-detection

When `func` (or `op`) is not specified, the function is automatically determined based on which parameters are present:

| parameters present         | detected function | description                     |
|----------------------------|-------------------|---------------------------------|
| `name` only                | `pass`            | Pass through the field as-is.   |
| `name` + `field`           | `rename`          | Rename a field.                 |
| `name` + `field` + `type`  | `cast`            | Rename and cast a field.        |
| `name` + `type` only       | `cast`            | Cast the field (same name).     |
| `name` + `value` + `type`  | `constant`        | Create a constant value field.  |
| `name` + `expression`      | `expression`      | Evaluate a math expression.     |
| `name` + `text`            | `text`            | Evaluate a FreeMarker template. |
| `name` + `fields`          | `struct`          | Create a nested structure.      |

## Select functions reference

### pass

Passes a field through unchanged. Used when you want to include a field in the output with the same name and type.

```yaml
- name: user_id
```

No additional parameters.

### rename

Renames a field. Supports nested field access with dot notation (e.g. `parent.child`).

| parameter | optional | type   | description                                                   |
|-----------|----------|--------|---------------------------------------------------------------|
| field     | required | String | Source field name. Supports dot notation for nested access.    |

```yaml
- name: customer_id
  field: user_id
```

Output type: same as the source field type.

### cast

Casts a field value to a different type.

| parameter | optional | type   | description                                                                  |
|-----------|----------|--------|------------------------------------------------------------------------------|
| field     | optional | String | Source field name. Defaults to `name` if not specified.                       |
| type      | required | String | Target type. See [supported types](#supported-types).                        |

```yaml
- name: amount_str
  field: amount
  type: string
```

### constant

Creates a field with a constant value.

| parameter | optional | type   | description                                           |
|-----------|----------|--------|-------------------------------------------------------|
| value     | required | JSON   | The constant value.                                   |
| type      | required | String | The output type. See [supported types](#supported-types). |

```yaml
- name: source_system
  type: string
  value: "batch_pipeline"
```

### replace

Maps input values to replacement values using a mapping table.

| parameter | optional | type                | description                                                              |
|-----------|----------|---------------------|--------------------------------------------------------------------------|
| field     | required | String              | Source field name. Supports dot notation.                                 |
| mapping   | optional | Map<String,String\> | Key-value mappings. Keys are input values, values are replacement values. |
| default   | optional | String              | Default value when no mapping matches. If not specified, the original value is used. |
| type      | optional | String              | Output type. Defaults to the source field type.                          |

```yaml
- name: status_label
  func: replace
  field: status
  mapping:
    "1": "active"
    "2": "inactive"
    "3": "deleted"
  default: "unknown"
```

### expression

Evaluates a mathematical expression using [exp4j](https://www.objecthunter.net/exp4j/) syntax. Input field names are available as variables.

| parameter  | optional | type   | description                                                    |
|------------|----------|--------|----------------------------------------------------------------|
| expression | required | String | Mathematical expression. Field names are used as variables.    |
| type       | optional | String | Output type. Default: `float64`.                               |

```yaml
- name: total_price
  expression: "price * quantity * (1 + tax_rate)"
```

Supported operators and functions: `+`, `-`, `*`, `/`, `%`, `^`, `abs()`, `ceil()`, `floor()`, `sqrt()`, `log()`, `log2()`, `log10()`, `sin()`, `cos()`, `tan()`, `min()`, `max()`, etc.

### text

Evaluates a [FreeMarker](https://freemarker.apache.org/) template expression. Input field values are available as template variables.

| parameter | optional | type   | description                                                           |
|-----------|----------|--------|-----------------------------------------------------------------------|
| text      | required | String | FreeMarker template string. Can also be specified as `value`.         |
| type      | optional | String | Output type. Default: `string`.                                       |

```yaml
- name: full_name
  text: "${last_name} ${first_name}"
```

FreeMarker built-in functions are available (e.g. `${name?upper_case}`, `${amount?string("0.00")}`, conditional expressions).

### concat

Concatenates multiple field values into a single string.

| parameter | optional | type           | description                                                   |
|-----------|----------|----------------|---------------------------------------------------------------|
| fields    | required | Array<String\> | Field names to concatenate.                                   |
| delimiter | optional | String         | Separator between values. Default: empty string.              |

```yaml
- name: full_address
  func: concat
  fields:
    - prefecture
    - city
    - street
  delimiter: " "
```

Output type: STRING.

### nullif

Returns null if a condition is met, otherwise returns the field value.

| parameter | optional | type                          | description                                              |
|-----------|----------|-------------------------------|----------------------------------------------------------|
| field     | optional | String                        | Source field name. Defaults to `name`.                    |
| condition | required | [Filter](../common/filter.md) | Condition to evaluate. Returns null when `true`.         |

```yaml
- name: amount
  func: nullif
  field: amount
  condition:
    key: status
    op: "="
    value: "deleted"
```

Output type: same as the source field type.

### uuid

Generates a random UUID string.

| parameter | optional | type    | description                                          |
|-----------|----------|---------|------------------------------------------------------|
| size      | optional | Integer | Truncate the UUID to the first N characters.         |

```yaml
- name: request_id
  func: uuid
  size: 8
```

Output type: STRING.

### hash

Computes a hash of field value(s).

| parameter | optional   | type                | description                                                                                                  |
|-----------|------------|---------------------|--------------------------------------------------------------------------------------------------------------|
| field     | selective  | String              | Single field to hash. Mutually exclusive with `fields` and `text`.                                           |
| fields    | selective  | Array<String\>      | Multiple fields to hash (concatenated with delimiter). Mutually exclusive with `field` and `text`.           |
| text      | selective  | String              | FreeMarker template string to hash. Mutually exclusive with `field` and `fields`.                            |
| algorithm | optional   | String              | Hash algorithm. Values: `SHA256`, `HmacSHA256`. Default: `SHA256`.                                           |
| secret    | optional   | String              | Secret key for `HmacSHA256`. Supports secret references: GCP Secret Manager resource names, AWS Secrets Manager ARNs / `aws-sm://{name}`, or Vault `vault://v1/{kv-path}#{field}`. |
| size      | optional   | Integer             | Truncate the hash to the first N characters.                                                                 |
| delimiter | optional   | String              | Separator when hashing multiple fields.                                                                      |

```yaml
- name: user_hash
  func: hash
  fields:
    - user_id
    - email
  algorithm: SHA256
  size: 16
```

Output type: STRING.

### event_timestamp

Returns the event timestamp of the record from the pipeline context.

| parameter    | optional | type   | description                                                                      |
|--------------|----------|--------|----------------------------------------------------------------------------------|
| duration     | optional | Long   | Duration amount to add to or subtract from the timestamp.                        |
| durationUnit | optional | String | Time unit for duration. Values: `second`, `minute`, `hour`, `day`, etc. Required if `duration` is specified. |
| cutoff       | optional | String | Time unit for rounding (truncation). Values: `second`, `minute`, `hour`, `day`, etc. |

```yaml
- name: event_time
  func: event_timestamp
```

Output type: TIMESTAMP.

### current_timestamp

Returns the current system time at processing time.

```yaml
- name: processed_at
  func: current_timestamp
```

No additional parameters. Output type: TIMESTAMP.

### struct

Creates a nested structure (element) from input fields. Child fields are defined using the same select function syntax recursively.

| parameter | optional | type           | description                                                                                            |
|-----------|----------|----------------|--------------------------------------------------------------------------------------------------------|
| fields    | required | Array<Select\> | Nested select field definitions. Uses the same format as top-level select fields.                      |
| each      | optional | String         | Array field to iterate over. When specified, creates an array of structs (one per array element).      |
| mode      | optional | String         | Output mode. Values: `required`, `nullable`, `repeated`. Default: `nullable` (or `repeated` if `each` is specified). |

```yaml
- name: address
  func: struct
  fields:
    - name: city
      field: city
    - name: zip
      field: zip_code
```

Output type: ELEMENT (nested structure) or ARRAY of ELEMENT.

### json

Same as `struct` but outputs the nested structure as a JSON string instead of a typed element.

| parameter | optional | type           | description                                               |
|-----------|----------|----------------|-----------------------------------------------------------|
| fields    | required | Array<Select\> | Nested select field definitions.                          |
| each      | optional | String         | Array field to iterate over.                              |
| mode      | optional | String         | Output mode. Values: `required`, `nullable`, `repeated`.  |

```yaml
- name: metadata_json
  func: json
  fields:
    - name: source
      type: string
      value: "api"
    - name: version
      field: api_version
```

Output type: STRING (JSON).

### json_path

Extracts values from a JSON string field using [JSONPath](https://goessner.net/articles/JsonPath/) expressions.

| parameter | optional | type           | description                                                                                   |
|-----------|----------|----------------|-----------------------------------------------------------------------------------------------|
| field     | required | String         | Source field containing a JSON string.                                                        |
| path      | required | String         | JSONPath expression (e.g. `$.data.items`).                                                    |
| type      | optional | String         | Output type. Default: `string`. Use `element` with `fields` for structured output.            |
| mode      | optional | String         | Output mode. Values: `required`, `nullable`, `repeated`. Default: `nullable`.                 |
| fields    | optional | Array<Select\> | Required when `type` is `element`. Defines the structure of extracted elements.                |

```yaml
- name: first_item_name
  func: json_path
  field: response_body
  path: "$.items[0].name"
  type: string
```

### http

Sends an HTTP request per record and returns the response. Supports FreeMarker templates for dynamic endpoints and bodies.

| parameter | optional | type                | description                                                        |
|-----------|----------|---------------------|--------------------------------------------------------------------|
| endpoint  | required | String              | URL endpoint. Supports FreeMarker template expressions.            |
| method    | optional | String              | HTTP method. Values: `get`, `post`, `put`, `delete`. Default: `get`. |
| body      | optional | String              | Request body. Supports FreeMarker template expressions.            |
| headers   | optional | Map<String,String\> | HTTP request headers.                                              |
| type      | optional | String              | Response type. Values: `string`, `bytes`. Default: `string`.       |

```yaml
- name: api_response
  func: http
  endpoint: "https://api.example.com/users/${user_id}"
  method: get
  headers:
    Authorization: "Bearer ${token}"
```

Output type: STRING or BYTES.

### scrape

Parses HTML/XML content and extracts data using CSS selectors ([jsoup](https://jsoup.org/)).

| parameter | optional | type           | description                                                                                |
|-----------|----------|----------------|--------------------------------------------------------------------------------------------|
| field     | optional | String         | Source field containing HTML/XML content.                                                   |
| selector  | required | String         | CSS selector expression.                                                                   |
| attribute | optional | String         | HTML attribute to extract. If not specified, extracts text content.                         |
| pattern   | optional | String         | Regex pattern to apply to the extracted text.                                               |
| group     | optional | Integer        | Regex capture group number.                                                                |
| baseUri   | optional | String         | Base URI for resolving relative links.                                                     |
| trim      | optional | Boolean        | If `true`, trims whitespace from extracted text.                                           |
| mode      | optional | String         | Output mode. Values: `nullable`, `repeated`. Default: `nullable`.                          |
| type      | optional | String         | Output type. Default: `string`.                                                            |
| fields    | optional | Array<Scrape\> | Nested scrape definitions for structured extraction.                                       |

```yaml
- name: page_title
  func: scrape
  field: html_body
  selector: "h1.title"
  trim: true
```

### generate

Generates a sequence of values (numeric, date, time, or timestamp ranges).

| parameter    | optional | type    | description                                                                                     |
|--------------|----------|---------|-------------------------------------------------------------------------------------------------|
| from         | required | String  | Start value. Supports expressions and FreeMarker templates.                                     |
| to           | required | String  | End value. Supports expressions and FreeMarker templates.                                       |
| type         | required | String  | Value type. Values: `int32`, `int64`, `float32`, `float64`, `date`, `time`, `timestamp`.        |
| interval     | optional | Integer | Step size. Default: `1`.                                                                        |
| intervalUnit | optional | String  | Time unit for date/time types. Values: `second`, `minute`, `hour`, `day`, etc. Default depends on type. |

```yaml
- name: date_range
  func: generate
  from: "2024-01-01"
  to: "2024-12-31"
  type: date
  interval: 1
  intervalUnit: day
```

Output type: ARRAY of the specified type.

### bytes_encode

Encodes a field value to byte array.

| parameter | optional | type   | description                                     |
|-----------|----------|--------|-------------------------------------------------|
| field     | optional | String | Source field name. Defaults to `name`.           |

```yaml
- name: encoded_data
  func: bytes_encode
  field: text_field
```

Output type: BYTES.

### bytes_decode

Decodes a byte array field to a typed value.

| parameter | optional | type   | description                                                  |
|-----------|----------|--------|--------------------------------------------------------------|
| field     | optional | String | Source field name. Defaults to `name`.                        |
| type      | required | String | Target type to decode to. See [supported types](#supported-types). |

```yaml
- name: decoded_value
  func: bytes_decode
  field: raw_bytes
  type: int64
```

### base64_encode

Encodes a field value to Base64.

| parameter | optional | type   | description                                                |
|-----------|----------|--------|------------------------------------------------------------|
| field     | optional | String | Source field name. Defaults to `name`.                      |
| type      | optional | String | Output type. Values: `bytes`, `string`. Default: `string`. |

```yaml
- name: encoded
  func: base64_encode
  field: raw_data
  type: string
```

### base64_decode

Decodes a Base64-encoded field.

| parameter | optional | type   | description                                                |
|-----------|----------|--------|------------------------------------------------------------|
| field     | optional | String | Source field name. Defaults to `name`.                      |
| type      | required | String | Output type. Values: `bytes`, `string`.                    |

```yaml
- name: decoded
  func: base64_decode
  field: base64_string
  type: bytes
```

### reshape

Reshapes an array or matrix to different dimensions.

| parameter | optional | type            | description                                              |
|-----------|----------|-----------------|----------------------------------------------------------|
| field     | required | String          | Source array or matrix field.                             |
| shape     | required | Array<Integer\> | Target shape dimensions (e.g. `[2, 3]`).                 |
| indices   | optional | Array<Integer\> | Specific indices to extract from the array.              |

```yaml
- name: matrix
  func: reshape
  field: flat_array
  shape: [3, 4]
```

### tokenize_encode

Encodes text using a tokenizer (HuggingFace).

| parameter | optional | type   | description                                     |
|-----------|----------|--------|-------------------------------------------------|
| tokenizer | required | String | Tokenizer type. Currently: `huggingface`.       |
| field     | required | String | Source text field.                               |
| path      | required | String | Path to the tokenizer model.                    |

```yaml
- name: tokens
  func: tokenize_encode
  field: text
  tokenizer: huggingface
  path: "gs://my-bucket/models/tokenizer"
```

Output type: ELEMENT (nested structure with token IDs and attention masks).

### tokenize_decode

Decodes token IDs back to text using a tokenizer.

| parameter | optional | type   | description                                     |
|-----------|----------|--------|-------------------------------------------------|
| tokenizer | required | String | Tokenizer type. Currently: `huggingface`.       |
| field     | required | String | Source token field.                              |
| path      | required | String | Path to the tokenizer model.                    |

Output type: STRING.

### cosine_similarity

Cosine similarity of two numeric array fields of equal length (embedding similarity). Returns null when either field is null or has zero norm.

| parameter | optional | type   | description                                     |
|-----------|----------|--------|-------------------------------------------------|
| left      | required | String | First vector field (`array<float64>`-shaped).   |
| right     | required | String | Second vector field.                             |

```yaml
- name: similarity
  func: cosine_similarity
  left: embedding1
  right: embedding2
```

Output type: FLOAT64.

### matrix_multiply

Matrix-vector product: projects a vector field through a matrix — e.g. dimension reduction of an embedding through fixed PCA components, or applying linear-model weights. The matrix is a constant in the config or a field: a `matrix`-typed field (flat row-major values with the 2D shape in the schema — an ONNX tensor or `reshape` output, no extra parameter needed), a flat numeric array with the `columns` parameter, or an array of arrays.

| parameter   | optional  | type                    | description                                                              |
|-------------|-----------|-------------------------|--------------------------------------------------------------------------|
| field       | required  | String                  | Vector field (`array<float64>`-shaped).                                  |
| matrix      | selective | Array<Array<Number\>\>  | Constant matrix (rows of equal length). Mutually exclusive with `matrixField`. |
| matrixField | selective | String                  | Field holding the matrix: `matrix` type (shape from schema), flat array (+ `columns`), or array of arrays. |
| columns     | optional  | Integer                 | Column count for splitting a flat (row-major) array field into rows. Not needed for `matrix`-typed or nested fields. |

```yaml
- name: projected
  func: matrix_multiply
  matrix: [[0.12, -0.5, 0.33], [0.8, 0.1, -0.2]]
  field: embedding
```

Output type: ARRAY<FLOAT64\>.

### matrix_solve

Solves the linear system `A x = b` by SVD: the exact solution for a full-rank square matrix, the least-squares solution when overdetermined, the minimum-norm solution when rank-deficient.

| parameter   | optional  | type                    | description                                                              |
|-------------|-----------|-------------------------|--------------------------------------------------------------------------|
| field       | required  | String                  | Right-hand-side vector field.                                            |
| matrix      | selective | Array<Array<Number\>\>  | Constant coefficient matrix. Mutually exclusive with `matrixField`.      |
| matrixField | selective | String                  | Field holding the coefficient matrix: `matrix` type (shape from schema), flat array (+ `columns`), or array of arrays. |
| columns     | optional  | Integer                 | Column count for splitting a flat (row-major) array field into rows.     |

```yaml
- name: coefficients
  func: matrix_solve
  matrixField: designMatrix
  field: target
```

Output type: ARRAY<FLOAT64\>.

### mahalanobis

Mahalanobis distance of a vector field from a distribution — an anomaly score. Give the distribution as a mean vector plus either a `precision` (inverse covariance) matrix or a `covariance` matrix (inverted internally; a constant covariance is inverted once per worker).

| parameter       | optional  | type                    | description                                                          |
|-----------------|-----------|-------------------------|----------------------------------------------------------------------|
| field           | required  | String                  | Vector field to score.                                               |
| mean            | selective | Array<Number\>          | Constant mean vector. Mutually exclusive with `meanField`.           |
| meanField       | selective | String                  | Field holding the mean vector.                                       |
| precision       | selective | Array<Array<Number\>\>  | Constant precision (inverse covariance) matrix.                      |
| precisionField  | selective | String                  | Field holding the precision matrix (`matrix` type, flat array + `columns`, or array of arrays). |
| covariance      | selective | Array<Array<Number\>\>  | Constant covariance matrix (used when no precision is given).        |
| covarianceField | selective | String                  | Field holding the covariance matrix (same field forms as `precisionField`). |
| columns         | optional  | Integer                 | Column count for splitting a flat (row-major) matrix field into rows. |

```yaml
- name: anomaly_score
  func: mahalanobis
  field: features
  mean: [12.5, 3.2, 0.8]
  covariance: [[4.0, 0.5, 0.0], [0.5, 1.0, 0.1], [0.0, 0.1, 0.3]]
```

Output type: FLOAT64.

### poly_fit

Least-squares polynomial fit over array fields held by one record (e.g. a price history array): returns `degree + 1` coefficients in ascending order (`c[0] + c[1]·x + …`), so a degree-1 fit's second coefficient is the slope/trend.

| parameter | optional | type    | description                                                              |
|-----------|----------|---------|--------------------------------------------------------------------------|
| y         | required | String  | Values field (numeric array).                                            |
| x         | optional | String  | X-coordinates field (same length as `y`). Default: `0, 1, 2, …`.         |
| degree    | optional | Integer | Polynomial degree (>= 0). Default: `1`.                                  |

```yaml
- name: price_trend
  func: poly_fit
  x: timestamps
  y: prices
  degree: 1
```

Output type: ARRAY<FLOAT64\>.

### lag (stateful)

Accesses previous row values using array-indexed variable syntax. Requires `groupFields` to be specified. Records are ordered by event time within each group.

| parameter  | optional | type                          | description                                                                                               |
|------------|----------|-------------------------------|-----------------------------------------------------------------------------------------------------------|
| expression | required | String                        | Expression with array-indexed variables. `field[0]` is current value, `field[1]` is previous value, etc.  |
| condition  | optional | [Filter](../common/filter.md) | Filter condition for which records to include in the window.                                              |

```yaml
- name: price_diff
  op: lag
  expression: "price[0] - price[1]"
```

Output type: FLOAT64. The window size is automatically determined from the maximum array index in the expression.

### Windowed aggregation functions (stateful)

Aggregation functions (`count`, `sum`, `avg`, `max`, `min`, `last`, `first`, `arg_max`, `arg_min`, `std`, `simple_regression`, `array_agg`) can be used as select functions with a `range` parameter for windowed per-key computations. Requires `groupFields` to be specified.

| parameter | optional   | type                          | description                                                          |
|-----------|------------|-------------------------------|----------------------------------------------------------------------|
| op        | required   | String                        | Aggregation function name (same as in aggregation module).           |
| field     | selective  | String                        | Input field name.                                                    |
| expression| optional   | String                        | Expression for computed values.                                      |
| range     | optional   | [Range](#range-parameters)    | Window range specification.                                          |
| condition | optional   | [Filter](../common/filter.md) | Filter condition for including records.                              |

#### Range parameters

| parameter | optional   | type    | description                                                                |
|-----------|------------|---------|----------------------------------------------------------------------------|
| count     | selective  | Integer | Number of previous records to include. Mutually exclusive with `duration`. |
| duration  | selective  | Long    | Duration of the window. Mutually exclusive with `count`.                   |
| unit      | optional   | String  | Time unit for duration. Values: `second`, `minute`, `hour`, `day`. Default: `second`. |
| offset    | optional   | Integer | Offset from the current record.                                           |

```yaml
- name: moving_avg_5
  op: avg
  field: price
  range:
    count: 5
```

## Supported types

The following type names can be used in the `type` parameter:

| type name         | description                |
|-------------------|----------------------------|
| `bool`, `boolean` | Boolean                    |
| `string`          | String                     |
| `bytes`           | Byte array                 |
| `int32`, `integer`| 32-bit integer             |
| `int64`, `long`   | 64-bit integer             |
| `float32`, `float`| 32-bit floating point      |
| `float64`, `double`| 64-bit floating point     |
| `decimal`         | Decimal                    |
| `date`            | Date                       |
| `time`            | Time                       |
| `datetime`        | Date and time              |
| `timestamp`       | Timestamp                  |
| `json`            | JSON string                |
| `element`         | Nested structure           |
| `array`           | Array                      |

## Output schema

- When `select` is specified, the output schema contains only the fields defined in the select list (in order).
- When only `filter` is specified (no `select`), the output schema is the same as the input schema.
- When `flattenField` is specified, the named array field's type is replaced by its element type in the output schema.
- Each select function's output contributes one field to the output schema. Earlier fields in the list can be referenced by later fields (chaining).

## Examples

### Example 1: Simple field selection and renaming

Select and rename specific fields from the input.

```yaml
transforms:
  - name: projected
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: customer_id
          field: user_id
        - name: customer_name
          field: name
        - name: total
          field: amount
          type: float64
```

### Example 2: Filter rows

Filter records matching a condition.

```yaml
transforms:
  - name: active_users
    module: select
    inputs:
      - source
    parameters:
      filter:
        key: status
        op: "="
        value: "active"
```

### Example 3: Filter and select combined

Filter rows and transform fields.

```yaml
transforms:
  - name: processed
    module: select
    inputs:
      - source
    parameters:
      filter:
        and:
          - key: status
            op: "="
            value: "active"
          - key: amount
            op: ">"
            value: 0
      select:
        - name: user_id
        - name: amount
        - name: processed_at
          func: current_timestamp
```

### Example 4: Computed fields with expressions

Create new fields using mathematical expressions.

```yaml
transforms:
  - name: calculated
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: product_id
        - name: price
        - name: quantity
        - name: subtotal
          expression: "price * quantity"
        - name: tax
          expression: "price * quantity * 0.1"
        - name: total
          expression: "subtotal + tax"
```

Note: `total` references `subtotal` and `tax` which are defined earlier in the select list.

### Example 5: FreeMarker template text

Generate formatted text fields using FreeMarker templates.

```yaml
transforms:
  - name: formatted
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: user_id
        - name: greeting
          text: "Hello ${first_name} ${last_name}!"
        - name: display_amount
          text: "${amount?string('0.00')}"
```

### Example 6: Constant values and UUID

Add constant fields and auto-generated IDs.

```yaml
transforms:
  - name: enriched
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: user_id
        - name: amount
        - name: source_system
          type: string
          value: "batch_import"
        - name: batch_id
          func: uuid
          size: 12
        - name: imported_at
          func: current_timestamp
```

### Example 7: Value replacement mapping

Map field values to different values.

```yaml
transforms:
  - name: mapped
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: user_id
        - name: status_code
          field: status
        - name: status_label
          func: replace
          field: status
          mapping:
            "0": "pending"
            "1": "active"
            "2": "suspended"
            "9": "deleted"
          default: "unknown"
```

### Example 8: Nested struct creation

Build nested structures from flat fields.

```yaml
transforms:
  - name: nested
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: id
        - name: profile
          func: struct
          fields:
            - name: name
              field: user_name
            - name: email
              field: email_address
            - name: age
              field: user_age
              type: int32
        - name: metadata
          func: json
          fields:
            - name: source
              type: string
              value: "api"
            - name: timestamp
              func: event_timestamp
```

### Example 9: JSON parsing with json_path

Extract values from JSON string fields.

```yaml
transforms:
  - name: parsed
    module: select
    inputs:
      - api_source
    parameters:
      select:
        - name: endpoint
        - name: status_code
          field: statusCode
        - name: user_name
          func: json_path
          field: body
          path: "$.user.name"
          type: string
        - name: items
          func: json_path
          field: body
          path: "$.items"
          type: string
          mode: repeated
```

### Example 10: Hashing fields

Create hash values for fields.

```yaml
transforms:
  - name: hashed
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: user_id
        - name: email_hash
          func: hash
          field: email
          algorithm: SHA256
          size: 16
        - name: composite_hash
          func: hash
          fields:
            - user_id
            - email
          delimiter: "|"
```

### Example 11: Array flattening

Flatten an array field into multiple records.

```yaml
transforms:
  - name: flattened
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: order_id
        - name: items
          func: json_path
          field: body
          path: "$.items"
          type: element
          mode: repeated
          fields:
            - name: item_id
              type: string
            - name: quantity
              type: int32
      flattenField: items
```

### Example 12: HTTP enrichment

Enrich records by calling an external API per record.

```yaml
transforms:
  - name: enriched
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: user_id
        - name: user_detail
          func: http
          endpoint: "https://api.example.com/users/${user_id}"
          method: get
          headers:
            Authorization: "Bearer my-token"
```

### Example 13: Stateful lag computation

Compute differences from previous records using lag.

```yaml
transforms:
  - name: with_diff
    module: select
    inputs:
      - stock_prices
    parameters:
      groupFields:
        - ticker
      select:
        - name: ticker
        - name: price
        - name: price_change
          op: lag
          expression: "price[0] - price[1]"
        - name: price_change_pct
          op: lag
          expression: "(price[0] - price[1]) / price[1] * 100"
        - name: event_time
          func: event_timestamp
```

### Example 14: Windowed moving average

Compute a moving average over the last N records within each group.

```yaml
transforms:
  - name: moving_averages
    module: select
    inputs:
      - sensor_data
    parameters:
      groupFields:
        - sensor_id
      select:
        - name: sensor_id
        - name: temperature
        - name: avg_temp_5
          op: avg
          field: temperature
          range:
            count: 5
        - name: max_temp_10
          op: max
          field: temperature
          range:
            count: 10
        - name: event_time
          func: event_timestamp
```

### Example 15: HTML scraping

Extract data from HTML content.

```yaml
transforms:
  - name: scraped
    module: select
    inputs:
      - http_responses
    parameters:
      select:
        - name: url
          field: endpoint
        - name: title
          func: scrape
          field: body
          selector: "title"
          trim: true
        - name: links
          func: scrape
          field: body
          selector: "a[href]"
          attribute: href
          mode: repeated
```

### Example 16: Sequence generation

Generate date ranges for each record.

```yaml
transforms:
  - name: with_dates
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: id
        - name: date_range
          func: generate
          from: "2024-01-01"
          to: "2024-03-31"
          type: date
          interval: 1
          intervalUnit: day
```

### Example 17: Conditional nullification

Set field values to null based on conditions.

```yaml
transforms:
  - name: cleaned
    module: select
    inputs:
      - source
    parameters:
      select:
        - name: user_id
        - name: email
          func: nullif
          field: email
          condition:
            key: email
            op: "="
            value: ""
        - name: amount
          func: nullif
          field: amount
          condition:
            key: status
            op: "="
            value: "cancelled"
```

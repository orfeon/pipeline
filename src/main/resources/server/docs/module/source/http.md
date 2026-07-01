---
type: Source Module
title: HTTP Source Module
description: Sends HTTP requests and outputs the responses. Supports GET/POST/PUT/DELETE methods, FreeMarker template expressions for endpoint/params/headers/body, loop-based pagination, request chaining with input dependencies, response format handling (text/json/xml/bytes), backoff/retry configuration, and rate limiting.
tags: [source, http, batch, api, rest]
timestamp: 2026-06-23T00:00:00Z
---

# HTTP Source Module

Source Module for sending HTTP requests and outputting the responses as structured records. Each request definition produces one or more response records containing status code, headers, body, and metadata.

Supports:

- **Multiple requests** - Define multiple requests that are executed in sequence or in parallel.
- **Request chaining** - Chain requests where one request's output feeds into the next request's input via `input`.
- **Loop-based pagination** - Repeat a request with loop variables and feed expressions for paginated APIs.
- **FreeMarker templates** - Use template expressions in endpoint, params, headers, and body for dynamic request construction.
- **Response formats** - Handle responses as text, JSON, XML, or raw bytes.
- **Retry with backoff** - Configure exponential backoff retry for transient failures.

## Source module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `http`                                                      |
| schema     | -        | -                   | Not required. The output schema is fixed (see [Output schema](#output-schema)). |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## HTTP source module parameters

| parameter     | optional | type                                    | description                                                                                                          |
|---------------|----------|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| requests      | required | Array<[Request](#request-parameters)\>  | List of HTTP request definitions. Each request produces response records.                                            |
| timeoutSecond | optional | Integer                                 | Connection timeout in seconds for the HTTP client. Default: `60`.                                                    |
| retry         | optional | [Retry](#retry-parameters)              | Retry configuration with backoff settings.                                                                           |
| backoff       | optional | [Backoff](#backoff-parameters)          | Exponential backoff configuration for retries.                                                                       |

## Request parameters

Each request in the `requests` array defines a single HTTP request.

| parameter            | optional | type                | description                                                                                                                                                                                                              |
|----------------------|----------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name                 | optional | String              | Name for this request. Used for identifying responses and for request chaining via `input`. If multiple requests are defined, each should have a unique name.                                                            |
| endpoint             | required | String              | URL endpoint to send the request to. Supports FreeMarker template expressions (e.g. `https://api.example.com/users/${userId}`).                                                                                          |
| method               | optional | String              | HTTP method. Values: `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, etc. Default: `GET`.                                                                                                                                       |
| params               | optional | Map<String,String\> | URL query parameters. Keys are parameter names, values are parameter values (support FreeMarker templates). Parameters are URL-encoded and appended to the endpoint.                                                     |
| headers              | optional | Map<String,String\> | HTTP request headers. Keys are header names, values are header values (support FreeMarker templates). Set `Content-Type: application/x-www-form-urlencoded` to send params as form-encoded body instead of URL parameters. |
| body                 | optional | JSON                | Request body. Supports FreeMarker template expressions. The entire JSON value is serialized as the request body string.                                                                                                   |
| format               | optional | Enum                | Response body format. Values: `text`, `json`, `xml`, `bytes`. Default: `text`. Determines how the response body is read and stored.                                                                                      |
| acceptableStatusCodes| optional | Array<Integer\>     | List of HTTP status codes to accept as successful. Values must be between 100 and 599. If not specified, all status codes are accepted.                                                                                   |
| schema               | optional | Schema              | Schema for parsing the response body. When specified with `json` or `xml` format, the response body is parsed into typed fields according to this schema.                                                                |
| loop                 | optional | [Loop](#loop-parameters) | Loop configuration for pagination. When specified, the request is repeated until the loop condition is no longer met.                                                                                               |
| input                | optional | [Input](#input-parameters)| Input dependency for request chaining. When specified, this request uses the output of the named request as input instead of the pipeline seed.                                                                     |
| preprocessors        | optional | Array<[Preprocessor](#preprocessor-parameters)\> | List of preprocessing steps applied to the input before sending the request. Each preprocessor can filter, select, and flatten input data.                                                       |
| group                | optional | String              | Group name for request grouping. Requests in the same group share an HTTP client connection. Default: extracted from the endpoint domain.                                                                                |

## Loop parameters

Loop configuration enables repeating a request for pagination or iterative API calls. The request is repeated in a do-while loop: the request executes at least once, then the condition is checked after each iteration.

| parameter | optional | type                       | description                                                                                                                                                                                                  |
|-----------|----------|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| condition | required | [Filter](../common/filter.md) | Loop continuation condition. After each request, the response values are evaluated against this filter. The loop continues while the condition is `true`. Uses the same Filter syntax as other modules.      |
| vars      | optional | Map<String,JSON\>          | Initial loop variables. Keys are variable names, values are initial values (string or number). These variables are available in FreeMarker templates and are updated by `feeds` after each iteration.        |
| feeds     | optional | Map<String,String\>        | Feed expressions evaluated after each iteration to update loop variables. Keys are variable names, values are FreeMarker template expressions. The response fields and current loop variables are available as template variables. |

### Loop execution flow

1. Initialize variables from `vars`.
2. Evaluate the endpoint/params/headers/body templates with current variables.
3. Send the HTTP request.
4. Evaluate `feeds` expressions with the response values and update the variables.
5. Evaluate the `condition` filter with the response values and updated variables.
6. If the condition is `true`, go to step 2. Otherwise, stop.

## Input parameters

Input configuration enables request chaining, where one request uses the output of another request as its input data.

| parameter | optional | type   | description                                                                                               |
|-----------|----------|--------|-----------------------------------------------------------------------------------------------------------|
| name      | required | String | Name of the parent request whose output is used as input for this request.                                |
| filter    | optional | Filter | Filter condition applied to the parent request's output before using it as input.                         |
| select    | optional | Array  | Field selection and transformation applied to the parent request's output.                                |
| flatten   | optional | String | Array field in the parent request's output to flatten before using each element as input.                 |

## Preprocessor parameters

Preprocessors transform input data before constructing the HTTP request. Each preprocessor can optionally execute a sub-request.

| parameter | optional | type    | description                                                                                            |
|-----------|----------|---------|--------------------------------------------------------------------------------------------------------|
| filter    | optional | Filter  | Filter condition. Records that do not match are discarded.                                             |
| select    | optional | Array   | Field selection and transformation.                                                                    |
| flatten   | optional | String  | Array field to flatten.                                                                                |
| request   | optional | Request | Optional sub-request to execute as part of preprocessing. The sub-request output replaces the input.   |

## Retry parameters

| parameter | optional | type                           | description                                              |
|-----------|----------|--------------------------------|----------------------------------------------------------|
| backoff   | optional | [Backoff](#backoff-parameters) | Backoff configuration for retries.                       |

## Backoff parameters

Exponential backoff configuration for retry behavior.

| parameter                  | optional | type    | description                                                                           |
|----------------------------|----------|---------|---------------------------------------------------------------------------------------|
| exponent                   | optional | Double  | Backoff exponent multiplier. Default: `1.5`.                                          |
| initialBackoffSecond       | optional | Integer | Initial backoff duration in seconds. Default: `1`.                                    |
| maxBackoffSecond           | optional | Integer | Maximum backoff duration in seconds. Default: `86400000` (1000 days).                 |
| maxCumulativeBackoffSecond | optional | Integer | Maximum cumulative backoff duration in seconds. Default: `86400000` (1000 days).      |
| maxRetries                 | optional | Integer | Maximum number of retry attempts. Default: unlimited.                                 |

## Output schema

Each HTTP response produces a record with the following fixed schema:

| field      | type                            | description                                            |
|------------|---------------------------------|--------------------------------------------------------|
| name       | STRING                          | The request name.                                      |
| endpoint   | STRING                          | The request URL.                                       |
| method     | STRING                          | The HTTP method used.                                  |
| statusCode | INT32                           | The HTTP response status code.                         |
| headers    | Map<String,Array<String\>\>     | The response headers (nullable).                       |
| body       | STRING                          | The response body as text.                             |
| blob       | BYTES                           | The response body as raw bytes.                        |
| durationMs | INT64                           | Request duration in milliseconds.                      |
| timestamp  | TIMESTAMP                       | Timestamp when the response was received.              |

## FreeMarker template support

The `endpoint`, `params` values, `headers` values, and `body` all support FreeMarker template expressions. Available template variables include:

- **Loop variables** defined in `loop.vars`
- **Response fields** from the current or previous response (for loop iterations and chained requests)

```
# Endpoint template
https://api.example.com/users?page=${page}

# Header template
Bearer ${access_token}

# Body template
{"query": "${search_term}", "offset": ${offset}}
```

## Examples

### Example 1: Simple GET request

Send a single GET request to an API endpoint.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: users
          endpoint: "https://api.example.com/users"
          method: GET
          format: json
```

### Example 2: GET request with query parameters

Send a GET request with URL query parameters.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: search
          endpoint: "https://api.example.com/search"
          method: GET
          params:
            q: "active users"
            limit: "100"
            offset: "0"
          format: json
```

### Example 3: POST request with JSON body

Send a POST request with a JSON body.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: create_report
          endpoint: "https://api.example.com/reports"
          method: POST
          headers:
            Content-Type: "application/json"
            Authorization: "Bearer my-token"
          body:
            type: "daily"
            date: "2024-01-15"
          format: json
```

### Example 4: Paginated API with loop

Iterate through a paginated API using loop variables and feed expressions.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: paginated
          endpoint: "https://api.example.com/items"
          method: GET
          params:
            page: "${page}"
            per_page: "100"
          format: json
          loop:
            vars:
              page: 1
            feeds:
              page: "${page + 1}"
            condition:
              key: statusCode
              op: "="
              value: 200
```

### Example 5: Chained requests

Execute a second request that depends on the output of the first.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: list
          endpoint: "https://api.example.com/resources"
          method: GET
          format: json
        - name: details
          endpoint: "https://api.example.com/resources/${resource_id}"
          method: GET
          format: json
          input:
            name: list
            select:
              - name: resource_id
                expression: "body.id"
```

### Example 6: POST with form-encoded body

Send a POST request with form-encoded parameters.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: token
          endpoint: "https://auth.example.com/oauth/token"
          method: POST
          headers:
            Content-Type: "application/x-www-form-urlencoded"
          params:
            grant_type: "client_credentials"
            client_id: "my-client-id"
            client_secret: "my-secret"
          format: json
```

### Example 7: Multiple independent requests

Execute multiple independent API requests.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: users
          endpoint: "https://api.example.com/users"
          method: GET
          format: json
        - name: products
          endpoint: "https://api.example.com/products"
          method: GET
          format: json
```

### Example 8: Request with retry and backoff

Configure retry behavior with exponential backoff.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      timeoutSecond: 120
      backoff:
        exponent: 2.0
        initialBackoffSecond: 2
        maxBackoffSecond: 60
        maxRetries: 5
      requests:
        - name: data
          endpoint: "https://api.example.com/data"
          method: GET
          format: json
```

### Example 9: API response to BigQuery

Fetch API data and write to BigQuery.

```yaml
sources:
  - name: api_source
    module: http
    parameters:
      requests:
        - name: metrics
          endpoint: "https://api.example.com/metrics"
          method: GET
          format: json

transforms:
  - name: parsed
    module: select
    inputs:
      - api_source
    parameters:
      fields:
        - name: endpoint
          field: endpoint
        - name: status
          field: statusCode
        - name: response_body
          field: body
        - name: duration
          field: durationMs
        - name: fetched_at
          field: timestamp

sinks:
  - name: bigquery_sink
    module: bigquery
    inputs:
      - parsed
    parameters:
      table: "myproject.mydataset.api_responses"
      writeDisposition: WRITE_APPEND
      createDisposition: CREATE_IF_NEEDED
```

### Example 10: Binary file download

Download binary content from an API.

```yaml
sources:
  - name: download
    module: http
    parameters:
      requests:
        - name: file
          endpoint: "https://api.example.com/files/export.pdf"
          method: GET
          format: bytes
```

---
title: Request source module
---

# Request source module

Turns the HTTP request body into source data when the pipeline runs in **HTTP serve mode**
(the direct-profile container deployed as a Cloud Run Service — see
[Run Pipeline on Cloud Run Services](../../deploy/cloud-run-service.md)).

Each `POST /run` request assembles and runs the pipeline once, with the request body parsed as
JSON against the declared schema:

* a JSON **array** becomes one element per entry,
* a single JSON **object** becomes one element.

Template arguments are passed separately as `?args.xxx=` query parameters, so the body carries
only data. A Pub/Sub push envelope is unwrapped transparently by the server (`message.data`
becomes the body, `message.attributes` become template args).

## Parameters

| parameter | optional | type | description |
| --- | --- | --- | --- |
| schema | required | [Schema](../common/schema.md) | Schema of the request body elements. Fields not present in the body become null |
| path | optional | String | Dot-notation path selecting the subtree of the body to read elements from (e.g. `payload.items`). The default reads the whole body |
| sample | optional | JSON | Fallback body used when no request body is available — keeps the config runnable for validation and local testing outside serve mode. Write it as a YAML/JSON object or array (the same shape as a request body), not as a JSON-encoded string — a string value fails with `body must be a JSON object or an array of objects` |

The common source parameter `timestampAttribute` is supported: when set, each element's event
time is taken from that field; otherwise the request processing time is used.

## Example

```yaml
sources:
  - name: input
    module: request
    parameters:
      schema:
        fields:
          - name: userId
            type: string
          - name: amount
            type: int64
transforms:
  - name: enriched
    module: select
    inputs: [input]
    parameters:
      select:
        - name: userId
          field: userId
        - name: doubled
          expression: "amount * 2"
          type: int64
sinks:
  - name: out
    module: bigquery
    inputs: [enriched]
    parameters:
      table: myproject.mydataset.mytable
```

Deploy the config as the service's fixed config, then send data:

```sh
curl -X POST "https://{service}.run.app/run?args.targetDate=2026-08-19" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -d '[{"userId":"u1","amount":100},{"userId":"u2","amount":200}]'
```

## Selecting a subtree with path

```yaml
sources:
  - name: items
    module: request
    parameters:
      path: payload.items
      schema:
        fields:
          - name: id
            type: string
```

With the body `{"payload": {"items": [{"id": "a"}, {"id": "b"}]}}` the source emits two elements.

## Running outside serve mode

The same config stays runnable as a normal batch pipeline — useful for validation and tests:

* pass the body via the `--requestBody` pipeline option:
  `--requestBody='[{"userId":"u1","amount":100}]'`, or
* declare a `sample` parameter as fallback data.

Without either, assembly fails with a clear error.

## Notes

* The body must be a JSON object or an array of JSON objects (after applying `path`).
* Elements that fail schema conversion are routed as failures (`failFast`, `outputFailure`,
  `failureSinks` apply as usual).
* The request body size is bounded by Cloud Run's request limit (32 MiB); serve mode runs on
  DirectRunner, which holds pipeline data in memory.

---
type: Sink Module
title: Auxia Sink Module
description: Sends input data as events to the Auxia platform. Converts each record into an Auxia LogEventsRequest protobuf message and publishes it to a Pub/Sub topic for ingestion. Supports direct field mapping or JSON payload extraction, event/user property modes, and custom message attributes.
tags: [sink, auxia, pubsub, batch, streaming, crm, event]
timestamp: 2026-07-05T00:00:00Z
---

# Auxia Sink Module

Sink Module for sending input data as events to the [Auxia](https://www.auxia.io/) platform. Each input record is converted into an Auxia event ingestion message (`auxia.event.v1.LogEventsRequest` Protocol Buffers message) and published to a Google Cloud Pub/Sub topic that feeds Auxia's event ingestion.

Two input interpretation types are supported:

- **element** (default) - Maps input record fields directly to Auxia event fields. The input schema must contain a `user_id` field and event fields (either flat or as an `events` array).
- **json** - Reads a JSON string from a specified input field and converts it to Auxia events. The JSON may be a single event object or an array of event objects.

## Sink module common parameters

| parameter  | optional | type                | description                                                           |
|------------|----------|---------------------|-----------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                     |
| module     | required | String              | Specified `auxia`                                                     |
| inputs     | required | Array<String\>      | Specify the names of the step to be used as input.                    |
| waits      | optional | Array<String\>      | Specify the names of the steps to wait for before processing.        |
| strategy   | optional | [Strategy](../common/strategy.md) | Windowing strategy for streaming execution.               |
| parameters | required | Map<String,Object\> | Specify the following individual parameters                          |

## Auxia sink module parameters

| parameter     | optional | type           | description                                                                                                                                                                       |
|---------------|----------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId     | required | String         | Auxia project ID. Set as `project_id` on each `LogEventsRequest` message.                                                                                                          |
| type          | optional | Enum           | How to interpret input records. Values: `element`, `json`. Default: `element`.                                                                                                     |
| mode          | optional | Enum           | Where non-reserved fields are mapped when the input has no explicit `event_properties`/`user_properties`. Values: `event` (map to `event_properties`), `user` (map to `user_properties`). Default: `event`. |
| field         | optional | String         | Input field name containing the event JSON. Required when `type` is `json`. The field type must be `string`, `json`, or `bytes`.                                                   |
| eventName     | optional | String         | Static event name applied to events that have no `event_name` field. Required if the input schema (or event JSON) does not provide `event_name`.                                    |
| excludeFields | optional | Array<String\> | Field names to exclude when mapping non-reserved fields to event/user properties.                                                                                                  |
| pubsub        | required | Map<String,Object\> | Pub/Sub publish settings. See [pubsub parameters](#pubsub-parameters).                                                                                                        |

### pubsub parameters

| parameter         | optional | type                | description                                                                                                                                                                        |
|-------------------|----------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic             | required | String              | Pub/Sub topic to publish events to. Format: `projects/{project}/topics/{topic}`.                                                                                                    |
| attributes        | optional | Map<String,String\> | Custom message attributes. Keys are attribute names, values are FreeMarker template expressions evaluated per record (input field names are available as template variables). The event's `insert_id` is always added as the `id` attribute. |
| maxBatchSize      | optional | Integer             | Maximum number of messages per publish batch.                                                                                                                                       |
| maxBatchBytesSize | optional | Integer             | Maximum total bytes per publish batch.                                                                                                                                              |

## Input requirements (type: element)

The input schema must contain a `user_id` field. Events can be provided in one of two shapes:

- **Flat record** - Event fields sit directly on the record. The schema must contain `client_event_timestamp`, and `event_name` (unless the `eventName` parameter is set).
- **`events` array** - The record contains an `events` field of type `Array<Element>`; each element is one event. The event schema must contain `client_event_timestamp`, and `event_name` (unless the `eventName` parameter is set). One Pub/Sub message is published per event.

### Reserved event fields

The following field names map directly to Auxia event fields:

| field                       | type                 | description                                                                              |
|-----------------------------|----------------------|------------------------------------------------------------------------------------------|
| event_name                  | String               | Event name. Falls back to the `eventName` parameter if absent.                            |
| insert_id                   | String               | Deduplication ID. Auto-generated from a hash of the event if absent.                      |
| client_event_timestamp      | Timestamp            | Event occurrence time. Defaults to the record's event time if not set on the event.       |
| server_received_timestamp   | Timestamp            | Server receive time.                                                                      |
| event_properties            | Element (record)     | Explicit event properties.                                                                |
| user_properties             | Element (record)     | Explicit user properties.                                                                 |
| pre_login_temp_user_id      | String               | Temporary user ID before login.                                                           |
| session_id                  | String               | Session ID.                                                                               |
| country / region / city     | String               | Location attributes.                                                                      |
| ip_address                  | String               | IP address.                                                                               |
| device_id                   | String               | Device ID.                                                                                |
| app_version_id              | String               | App version ID.                                                                           |

Any other field is mapped as a property key/value: to `event_properties` when `mode` is `event`, or to `user_properties` when `mode` is `user` — unless the record already provides an explicit `event_properties` or `user_properties` field, or the field is listed in `excludeFields`. Supported property value types: `bool`, `string`, `json`, `int32`, `int64`, `float32`, `float64`, `timestamp`, `date`, `enumeration`.

## Input requirements (type: json)

The field specified by `field` must contain a JSON string with the same structure as above: an object with `user_id` and either flat event fields or an `events` array. The value may also be a JSON array of such objects; every object produces its own events. String property values that look like timestamps are ingested as timestamp values; other strings, numbers, and booleans map to the corresponding Auxia property value types.

## Failure handling

Records that fail conversion are routed as failures (respecting the module's `failFast` / failure sink settings) instead of failing the whole pipeline when `failFast` is disabled.

## Examples

### Example 1: Send BigQuery rows as Auxia events

Non-reserved fields (`amount`, `item_id`) are mapped to `event_properties`.

```yaml
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: "SELECT user_id, event_name, client_event_timestamp, amount, item_id FROM `myproject.mydataset.purchase_events`"

sinks:
  - name: auxia_sink
    module: auxia
    inputs:
      - bigquery_source
    parameters:
      projectId: "123456789"
      pubsub:
        topic: "projects/myproject/topics/auxia-events"
```

### Example 2: Fixed event name and user properties mode

Map non-reserved fields to `user_properties`, excluding some fields.

```yaml
sinks:
  - name: auxia_sink
    module: auxia
    inputs:
      - user_attributes
    parameters:
      projectId: "123456789"
      eventName: user_attribute_updated
      mode: user
      excludeFields:
        - internal_flag
      pubsub:
        topic: "projects/myproject/topics/auxia-events"
```

### Example 3: JSON payload from a Pub/Sub message field

Read a JSON event payload from the `payload` field and attach a custom attribute.

```yaml
sources:
  - name: pubsub_source
    module: pubsub
    parameters:
      subscription: "projects/myproject/subscriptions/raw-events"
      format: json
    schema:
      fields:
        - name: payload
          type: string
        - name: source_system
          type: string

sinks:
  - name: auxia_sink
    module: auxia
    inputs:
      - pubsub_source
    parameters:
      projectId: "123456789"
      type: json
      field: payload
      pubsub:
        topic: "projects/myproject/topics/auxia-events"
        attributes:
          source: "${source_system}"
        maxBatchSize: 100
```

# System configuration

`system` defines pipeline system configurations.
The following items can be defined as system.

| parameter | type                          | description                                                                                     |
|-----------|-------------------------------|-------------------------------------------------------------------------------------------------|
| args      | Map<String,String\>           | Default values of variables at pipeline startup, which can also be rewritten by Template Engine |
| context   | String                        | Specify pipeline execution path.                                                                |
| imports   | Array<Import\>                | Configuration to import external config files.                                                  |
| failure   | Failure                       | Configure Pipeline-wide handling of failed processes. including dead-letter sink definitions    |

## args

args defines default values in config variables.
Variables can be overwritten during pipeline execution.
Variables defined earlier in args can be referenced in later variables.
You can also use the built-in template function within args definitions to dynamically generate values.

In the following example, the timezone variable is first specified in args, then used as an argument to dynamically generate the next today variable using a built-in template function.

```yaml:example
system:
  args:
    timezone: "Asia/Tokyo"
    today: "${utils.datetime.currentDate('${args.timezone}')}"
sources:
  - name: bigquery_source
    module: bigquery
    parameters:
      query: SELECT * FROM table WHERE date > '${args.today}'
```

As shown in the example above, you can override the args.today defined in config.yaml by specifying it in parameters at runtime.

```bash
gcloud dataflow flex-template run recovery-attribute \
    --template-file-gcs-location=gs://xxx/template \
    --parameters=config=gs://xxx.yaml \
    --parameters=args.today="'2020-08-01T00:00:00Z'"
```

## context

In context, module configurations for multiple contexts are defined within a single config file. By specifying the context at startup, pipeline processing can be easily switched.
(If no context is specified, all modules are used, causing conflicts with the same name and resulting in errors.)

The following config.yaml example uses context in an ML pipeline to switch between training and prediction paths.

```yaml:example
system:
  context: train
sources:
  - name: ml_source
    module: bigquery
    tags:
      - train
    parameters:
      table: xxx
  - name: ml_source
    tags:
      - prediction
    parameters:
      format: avro
      subscription: xxx
transforms:
  - name: feature
    inputs:
      - ml_source
    tags:
      - train
      - prediction
    parameters:
      groupFields:
        - user_id
      select:
        - name: moving_avg
          field: amount_field
          func: avg
          range:
            count: 10
sinks:
  - name: feature_sink
    module: bigquery
    tags:
      - feature
    inputs:
      - feature
    parameters:
      table: xxx
  - name: feature_sink
    module: pubsub
    tags:
      - prediction
    inputs:
      - feature
    parameters:
      topic: xxx
```

There are modules named `ml_source` and `feature_sink` in sources and sinks respectively, sharing the same name.
However, they have different tags in tags: `train` and `prediction`.
In transforms, the select module generating features is configured to compute the moving average of the specified number of recent values.
It generates the same features for both `training` and `prediction`.
Tags are specified for both `train` and `prediction`.

When `train` is specified in `system.context`, the pipeline is constructed using only modules (in this case, bigquery) where train is specified in tags for both sources and sinks.
(If `prediction` is specified in `system.context`, only pubsub is used for sources and sinks.)
The select module in transform is specified with both train and prediction in tags, so it is used in both contexts.

(Note: If no context is specified, all modules are used, causing a conflict with the same name and resulting in an error.)

## Import

Using imports, multiple config files defined separately can be combined into a single pipeline.

The following example config file contains no definitions for `sources`, `transforms`, or `sinks`; only the `system.imports` is specified.

The actual pipeline is defined in the config files specified by the `imports.files`. These files are loaded at startup, assembled into a single pipeline, and executed.
(The `base` parameter specifies the prefix for the config file paths.)

```yaml
system:
  imports:
    - base: gs://example-bucket/configs/
      files:
        - pipeline_1.yaml
        - pipeline_2.yaml
        - subdir/pipeline_3.yaml
```

This imports feature simply merges modules defined across multiple config files, so be careful to avoid duplicate names in each config file.

## Failure

| parameter | type                | description                                                                                     |
|-----------|---------------------|-------------------------------------------------------------------------------------------------|
| failFast  | Boolean             | Default values of variables at pipeline startup, which can also be rewritten by Template Engine |
| union     | Boolean             | Specify pipeline execution path.                                                                |
| sinks     | Array<FailureSink\> | Pipeline-wide dead-letter sink definitions                                                      |


In the `system.failure` parameter, behavior when errors occur can be specified across all modules.
parameter `failure.failFast` determines whether to immediately fail the job upon an error or continue processing normally.
If false is specified for `failure.failFast`, error records from each module will be saved to the dead-letter sink defined in the `failure.sinks` module.

Below is an example configuration for the dead-letter sink.

```yaml
system:
  failure:
    failFast: false
    union: false
    sinks:
    - name: pubsub_dead_letter_sink
      module: pubsub
      parameters:
        format: avro
        topic: xxx
sources:
  - name: pubsub_source
    module: pubsub
    parameters:
      format: avro
      subscription: xxx
sinks:
  - name: pubsub_sink
    inputs:
      - pubsub_source
    parameters:
      format: avro
      topic: xxx
```

In this config example, the failures section has been newly added.
Unlike regular sinks, the failures module does not require specifying inputs. It receives all data that fails processing in any module within the pipeline.

# Define Pipeline

Define the pipeline contents in YAML/JSON format and specify using the config parameter.

> This directory (`src/main/resources/server/docs/`) is the canonical documentation tree for both humans
> and the Pipeline server's AI agent / MCP server / Pipeline Builder UI (the files are bundled on the
> classpath and read at runtime). Keep each page self-contained.

## Config file contents

In the Config file, seven blocks, `system`, `options`, `sources`, `transforms`, `sinks`, `actions` and `failures`, are combined to define the processing contents.
`sources` is for input data acquisition, `transforms` is for data processing, `sinks` is for data output, and `actions` is for workflow steps against external services.
`options` defines pipeline options.

| parameter  | type                         | description                                |
|------------|------------------------------|--------------------------------------------|
| system     | [System](system.md)          | System configuration.                      |
| options    | [Options](options/README.md) | Pipeline option definitions.               |
| sources    | Array<Source\>               | Pipeline data source definitions.          |
| transforms | Array<Transform\>            | Pipeline data processing definitions.      |
| sinks      | Array<Sink\>                 | Pipeline data sink definitions.            |
| actions    | Array<Action\>               | Workflow steps against external services (run a job, notify, …). |
| failures   | Array<Failure\>              | Pipeline-wide dead-letter sink definition. |


```json:config
{
  "system": {...},
  "options": {...},
  "sources": [
    {...},
    ...
  ],
  "transforms": [
    {...},
    ...
  ],
  "sinks": [
    {...},
    ...
  ],
  "actions": [
    {...},
    ...
  ],
  "failures": [
    {...},
    ...
  ]
}
```

You can define and run a pipeline by combining these types of various build-in modules.

The list of build-in modules can be found on [Modules Page](module/README.md).

Besides the data modules, [action modules](module/action/README.md) execute operations against
external services (run a BigQuery job, call an HTTP endpoint, write a result-history file) as
lightweight workflow steps. They are declared in the `actions` section; `module:` names the
service and `trigger:` (`once` / `perElement` / `collect`) the firing semantics. Their position
in the flow comes from `inputs` / `waits` alone.

Examples of configuration files are listed in the [Examples Page](../../../../../examples/README.md), so try to find and arrange a configuration file that is close to the data processing you want to perform.

Below is an overview of these built-in modules.

## Module common attributes

In the three types of modules, the contents of input, processing, and output are described as JSON parameters.
The common settings of the three types of modules are as follows.

| attribute  | type                                  | optional | description                                                                                                                                                                                                                                                                                                                                       |
|------------|---------------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name       | String                                | required | Set unique name in config file                                                                                                                                                                                                                                                                                                                    |
| module     | String                                | required | Set [module](module/README.md) name                                                                                                                                                                                                                                                                                                               |
| parameters | Map<String, Object\>                  | required | Specify the parameters defined in each module.                                                                                                                                                                                                                                                                                                    |
| strategy   | [Strategy](module/common/strategy.md) | optional | Specify the beam windowing strategy([Window](https://beam.apache.org/documentation/programming-guide/#windowing), [Trigger](https://beam.apache.org/documentation/programming-guide/#triggers), [AccumulationMode](https://beam.apache.org/documentation/programming-guide/#window-accumulation-modes)) to apply to the processing of the module. |
| waits      | Array<String\>                        | optional | If you want to wait for the completion of other steps and then start this step, assign a step Name to wait for completion.                                                                                                                                                                                                                        |
| failFast   | Boolean                               | optional | Specify true if you want the job to fail immediately when an error occurs. The default is true for batch and false for streaming.                                                                                                                                                                                                                 |
| outputFailure | Boolean                            | optional | Specify true if you want records that failed in this module to be emitted as failure output.                                                                                                                                                                                                                                                      |
| failureSinks  | Array<Failure\>                    | optional | Sink definitions to which failed records of this module are routed (module-level dead-letter). See also the top-level `failures` block.                                                                                                                                                                                                           |
| logs       | Array<String\>                        | optional | Specify logging condition.                                                                                                                                                                                                                                                                                                                        |
| ignore     | Boolean                               | optional | Specify true if you want to ignore this module.                                                                                                                                                                                                                                                                                                   |
| tags       | Array<String\>                        | optional | Tags for context-based execution control. When a context is specified at runtime, only modules tagged with that context are executed (others are ignored).                                                                                                                                                                                        |
| outputType | String                                | optional | Data type of the module output (e.g. `row`, `avro`). Usually resolved automatically.                                                                                                                                                                                                                                                              |
| description | String                               | optional | Free-text description of the module (for documentation purposes; not used in processing).                                                                                                                                                                                                                                                        |
| args       | Map<String, String\>                  | optional | Module-level template variables referenced from the module parameters.                                                                                                                                                                                                                                                                            |


### Module Common Properties Matrix

|                    | source   | transform | sink     |
|--------------------|----------|-----------|----------|
| name               | required | required  | required |
| module             | required | required  | required |
| parameters         | required | required  | required |
| inputs             | -        | required  | required |
| sideInputs         | -        | optional  | optional |
| waits              | -        | optional  | optional |
| schema             | optional | -         | optional |
| strategy           | -        | optional  | optional |
| timestampAttribute | optional | -         | -        |
| failFast           | optional | optional  | optional |
| logs               | optional | optional  | optional |
| ignore             | optional | optional  | optional |


## Source modules

The source module defines the source of the data you want to process in the pipeline.
Common configuration items in the source module are as follows.

| parameter          | type                              | optional | description                                                                                                                                                                    |
|--------------------|-----------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| schema             | [Schema](module/common/schema.md) | optional | Specifies the schema of the input resource. If the input resource has schema information, no specification is required.                                                        |
| timestampAttribute | String                            | optional | Defines which fields of the source record should be treated as EventTime. The default is the time of input.                                                                    |


## Transform modules

The transform module defines what to do with the data.
The common settings of the transform module are as follows.

| parameter  | type           | optional | description                                                                                                   |
|------------|----------------|----------|---------------------------------------------------------------------------------------------------------------|
| inputs     | Array<String\> | required | Specify the names of the module from which you want to process the data, including the name of the transform. |
| sideInputs | Array<String\> | optional | Specify the name of the input when additional information is needed for processing.                           |


## Sink modules

The sink module defines the output destination of the data.
The common settings of the sink module are as follows

| parameter  | type           | optional | description                                                                                                                 |
|------------|----------------|----------|-----------------------------------------------------------------------------------------------------------------------------|
| inputs     | Array<String\> | required | Specify the name of the module from which you want to output data. source or transform name.                                |
| sideInputs | Array<String\> | optional | Specify the name of the input when additional information is needed for writing.                                            |

## Wildcard inputs and the assembly-time `${input.*}` template

A module that produces multiple named outputs (for example the `partition` transform, or the spanner source in all-tables mode) registers them as `<moduleName>.<tag>`. A downstream module can bind **all** of them at once with a wildcard input:

```yaml
sinks:
  - name: export
    module: storage
    inputs: [db.*]     # every tagged output of module "db" (failure outputs excluded)
```

The wildcard is resolved when the pipeline is assembled — that is, **at launch time**. Matching no output is a launch-time error.

When (and only when) a sink declares a wildcard input, the reserved `${input.*}` namespace becomes available in its `parameters`. The sink is then built once **per matched input**, and each instance (named `<sinkName>.<tag>`) resolves `${input.*}` expressions against that input's assembly-time context:

| variable            | description                                                                                        |
|---------------------|-----------------------------------------------------------------------------------------------------|
| `${input.name}`     | Full input collection name (e.g. `db.Users`).                                                      |
| `${input.tag}`      | The wildcard-matched part of the name (e.g. `Users` for `db.Users` via `db.*`).                    |
| `${input.<attr>}`   | Assembly-time attributes attached by the upstream module (e.g. `${input.table}` from the spanner source's all-tables mode). |

```yaml
sources:
  - name: db
    module: spanner
    parameters:
      projectId: myproject
      instanceId: myinstance
      databaseId: mydatabase
      tables:
        excludes: ["backup_*"]

sinks:
  - name: export
    module: storage
    inputs: [db.*]
    parameters:
      format: parquet
      output: gs://mybucket/export/${input.table}/dt=${date}/data
```

The expressions are FreeMarker templates, so builtins work (`${input.table?lower_case}`). Every other `${...}` expression — `${date}` above — is left untouched and keeps its usual runtime (per-element) meaning; only the `input` namespace is consumed at launch. Without a wildcard input, `${input.*}` is not treated specially at all, so existing configs are unaffected.

Template phases at a glance:

| phase                  | notation             | resolved when                        | resolved from                                             |
|------------------------|----------------------|--------------------------------------|-----------------------------------------------------------|
| Config load            | `${args.*}`          | Config file is loaded                | `system.args` and runtime `args.*` parameters             |
| Pipeline assembly      | `${input.*}`         | Pipeline is assembled at launch      | The wildcard-matched input (name, tag, attributes)        |
| Execution              | any other `${...}`   | Per element while the pipeline runs  | Element field values (module-dependent)                   |

## Rewriting the configuration file at runtime

In the configuration file, you can use the Template Engine, [Apache FreeMarker](https://freemarker.apache.org/), to assign variables at runtime, or you can even rewrite the file itself.

You can define variables in the configuration file, as in the example below, and assign values at run time.
The notation follows the FreeMarker specification.

```JSON
{
  "sources": [
    {
      "name": "MyKindInput",
      "module": "datastore",
      "timestampAttribute": "created_at",
      "schema": {
        "fields": []
      },
      "parameters": {
        "projectId": "myproject",
        "gql": "SELECT * FROM MyKind WHERE created_at > DATETIME('${current_datetime}')"
      }
    }
  ],
  "sinks": [
    {
      "name": "MyKindOutput",
      "module": "storage",
      "inputs": ["MyKindInput"],
      "parameters": {
        "output": "${output_path}",
        "format": "avro"
      }
    }
  ]
}
```

You can assign variables to the Config file at runtime by prefixing it with the parameter `args.`.

```sh
gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json} \
  --parameters=args.current_datetime=2020-12-01T00:00:00Z \
  --parameters=args.output_path=gs://mybucket/output
```

REST API version

```sh
CONFIG="$(cat examples/xxxx.json)"
curl -X POST -H "Content-Type: application/json"  -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/templates:launch"`
  `"?dynamicTemplate.gcsPath=gs://{path/to/legacy_template_file}" -d "{
    'parameters': {
      'config': '$(echo "$CONFIG")',
      'args.current_datetime': '2020-12-01T00:00:00Z',
      'args.output_path': 'gs://mybucket/output',
    },
    'jobName':'myJobName',
  }"
```



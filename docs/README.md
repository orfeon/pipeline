# How to use Mercari Pipeline

This document explains how to use/develop the Mercari Pipeline.

## For Users

First deploy pipeline. Next, describe the process as a configuration file. Finally, launch pipeline from the configuration file.

* [How to Deploy Pipeline](deploy/README.md)
* [How to Define Pipeline](config/README.md) — see also the [built-in module list](config/module/README.md)
* [How to Execute Pipeline](exec/README.md)
* [Examples](../examples/README.md) — ready-to-use configuration files for common use cases

You can also use the **Pipeline API Server** (web UI / REST API / MCP server with a built-in AI agent) to create,
validate, debug, and deploy pipelines. See the [Deploy guide](deploy/README.md#deploy-pipeline-api-server-for-pipeline-api-server) to set it up.

> **Note**: The canonical per-module reference (parameters and examples) lives in
> [`src/main/resources/server/docs/module/`](../src/main/resources/server/docs/module/) — these files are bundled
> with the server and read by its AI agent, MCP server, and the Pipeline Builder UI. The pages under `docs/config/`
> are legacy and are being migrated there.

## For Developers

* [How to Develop Pipeline](developer/README.md)

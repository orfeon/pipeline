# How to use Mercari Pipeline

This document explains how to use/develop the Mercari Pipeline.

> **Note**: User-facing documentation lives in
> [`src/main/resources/server/docs/`](../src/main/resources/server/docs/README.md). That tree is the
> canonical reference for both humans and the Pipeline server's AI agent / MCP server / Pipeline
> Builder UI (the files are bundled on the classpath and read at runtime). This `docs/` directory
> keeps only developer-facing documentation.

## For Users

First deploy pipeline. Next, describe the process as a configuration file. Finally, launch pipeline from the configuration file.

* [How to Deploy Pipeline](../src/main/resources/server/docs/deploy/README.md)
* [How to Define Pipeline](../src/main/resources/server/docs/README.md) — see also the [built-in module list](../src/main/resources/server/docs/module/README.md)
* [How to Execute Pipeline](../src/main/resources/server/docs/exec/README.md)
* [Examples](../examples/README.md) — ready-to-use configuration files for common use cases

You can also use the **Pipeline API Server** (web UI / REST API / MCP server with a built-in AI agent) to create,
validate, debug, and deploy pipelines. See the [Deploy guide](../src/main/resources/server/docs/deploy/README.md#deploy-pipeline-api-server-for-pipeline-api-server) to set it up.

## For Developers

* [How to Develop Pipeline](developer/README.md)

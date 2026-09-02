# Prism Options

Prism runner options.

[Prism](https://beam.apache.org/documentation/runners/prism/) is Beam's portable local runner (the
successor of DirectRunner). Build the image with the `prism` Maven profile (see
[How to Deploy Pipeline](../deploy/README.md#deploy-prism-runner-for-local--cloud-run-execution));
its entrypoint runs pipelines with `--runner=PrismRunner`.

| parameter            | type    | description |
|----------------------|---------|-------------|
| enableWebUI          | Boolean | Serve Prism's web UI while the job runs (local debugging; leave off for a batch job on Cloud Run). |
| idleShutdownTimeout  | String  | Shut the prism process down after this idle duration (e.g. `15m`; `-1` keeps it alive). |
| prismLocation        | String  | Where to take the prism binary from: the path of an **unzipped** binary (used in place), or a Beam GitHub release URL (download / tag page; fetched and unpacked into `~/.apache_beam/cache/prism/bin`). Default: the binary bundled in the `prism` image (see below); outside the image the runner downloads its own version. An empty string re-enables that download. |
| prismVersionOverride | String  | Prism release version to download instead of the SDK's own (e.g. `2.76.0`). Only consulted when `prismLocation` is unset or empty — inside the `prism` image set `prismLocation: ""` as well, or it is silently ignored. |

The pipeline process stays alive for the whole run: the runner submits the job to the prism process it
starts, and `MPipeline` waits for the result (a FAILED / CANCELLED pipeline exits non-zero, which a
Cloud Run Job reports as a failed execution).

#### The bundled binary (prism image)

The `prism` image bundles the prism binary of the pipeline's Beam version at
`/opt/prism/apache_beam-v{beam.version}-prism-linux-amd64` and names it in the environment variable
`MERCARI_PIPELINE_PRISM_LOCATION`. `PrismOptions` applies that as `prismLocation` only when neither the
config nor the command line set one, so:

* `options.prism.prismLocation` in the config wins, and so does `--prismLocation=...` on the command line
  (a job created with that flag keeps working); `prismLocation: ""` in the config re-enables the runner's
  own download.
* A container starts with **no outbound network access** and **no writable `$HOME`** — a plain local
  binary is used in place. A URL given as `prismLocation` is fetched into `~/.apache_beam/cache/prism/bin`
  at startup and needs a writable, exec-capable `$HOME`. A **local zip is not unpacked** by Beam 2.76
  (it is copied as is and fails to execute): unzip it yourself and point at the binary.
* The runner still needs a **writable `java.io.tmpdir`**: the portable runner zips the classpath into a
  temp file at every submission (a hardened pod with `readOnlyRootFilesystem` needs an `emptyDir` on
  `/tmp`; the direct image has no such requirement).
* The asset is **linux/amd64**, like the base image. On an arm64 host run the image under emulation
  (`docker run --platform linux/amd64`); an arm64 image needs an arm64 base image and the arm64 asset:
  `-Dprism.binary=apache_beam-v{beam.version}-prism-linux-arm64 -Dprism.zip.sha256=<sha256 of that zip>`.

The Maven build (`-Pprism`) downloads the release zip at `generate-resources`, verifies it against
`prism.zip.sha256` in the `pom.xml` (update the pin together with `beam.version`; CI checks the pair on
every push), caches it under the local Maven repository and unpacks it into
`target/prism/{beam.version}/`, so `mvn compile jib:dockerBuild -Pprism` works as well as `mvn package`.
`-Djib.skip=true` skips the download too. Where GitHub is not reachable, pass the zip from a mirror or a
local file: `-Dprism.zip.url=file:///path/to/apache_beam-v{beam.version}-prism-linux-amd64.zip`
(the sha256 check still applies).

Outside the image (e.g. running `MPipeline` from an IDE on Windows) the runner downloads the binary
itself; on Windows the automatic URL is broken (Beam builds it from `os.name`, producing `windows 11`):
download `apache_beam-v{beam.version}-prism-windows-amd64.zip` manually, unzip it and set `prismLocation`.

#### Example

```YAML:options
options:
  prism:
    idleShutdownTimeout: 15m
```

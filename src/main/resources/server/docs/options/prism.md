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
| prismLocation        | String  | Where to take the prism binary from: a local file path or a URL to the binary / release zip. The `prism` image bundles the binary and sets this in its entrypoint (`/opt/prism/apache_beam-v{beam.version}-prism-linux-amd64`); a value here overrides that (a plain local binary is used in place, a URL / zip is fetched or unpacked into `~/.apache_beam/cache/prism/bin`, which needs a writable, exec-capable `$HOME`). Outside the image the runner downloads the binary from the Beam GitHub release unless this is set — on Windows the automatic URL is broken (Beam builds it from `os.name`, producing `windows 11`; download `apache_beam-v{beam.version}-prism-windows-amd64.zip` manually instead). |
| prismVersionOverride | String  | Prism release version to download instead of the SDK's own (e.g. `2.76.0`). |

The pipeline process stays alive for the whole run: the runner submits the job to the prism process it
starts, and `MPipeline` waits for the result (a FAILED / CANCELLED pipeline exits non-zero, which a
Cloud Run Job reports as a failed execution).

#### Example

```YAML:options
options:
  prism:
    idleShutdownTimeout: 15m
```

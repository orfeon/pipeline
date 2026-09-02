# Direct Options

Direct runner options.

| parameter           | type    | description |
|---------------------|---------|-------------|
| targetParallelism   | Integer | Number of worker threads. Defaults to the number of available cores. |
| blockOnRun          | Boolean | Whether `run()` blocks until the pipeline finishes (default true). `MPipeline` waits for the result either way. |
| enforceImmutability | Boolean | Check that DoFns do not mutate their inputs (default true). Disable for Struct-backed elements read from Spanner: their lazily-decoded internal state false-positives the check. |
| enforceEncodability | Boolean | Encode / decode every element to verify the coders round-trip (default true). |

DirectRunner is for small data. Its GroupByKey copies each key's buffered state once per bundle that
touches the key, so a keyed stage over a coarse or global key (a shrinkage lattice's global level of the
`feature` transform, say) slows down by orders of magnitude as rows grow — the enforcement options above
do not change that. Run such pipelines on Dataflow or with the [prism](prism.md) image instead.

#### Example

```YAML:options
options:
  direct:
    targetParallelism: 4
```

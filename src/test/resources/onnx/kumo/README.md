# Kumo Tabular reference values

The inputs and outputs of structured-data-models for two synthetic tables, with a deterministic copy of its default
recipe (one estimator, without FlipSign, ShuffleColumns and ShuffleCategories).

Source: `fixtures/` of https://github.com/shreyaskarnik/kumo-tabular-web at commit `8be3b84c16` (Apache-2.0, as stated in
its README). `mixed-ltv/logits.bin` is cut to its first 2 rows, as all its rows take 480 KB.

They date from before structured-data-models sorted the classes by value (#1037, 2026-10-02), when they were numbered by first
appearance. The classes of these two tables are the same either way. The source also has `iris`, whose classes are not, so the
tests compare the classes of the rows by their labels.

| file | content |
|---|---|
| `data.csv` | the rows, context first; an empty cell is a missing value |
| `meta.json` | the target, the task, the number of context rows, the kind of each column, the classes and the shapes |
| `x.bin`, `y.bin`, `mask.bin` | the model inputs: float32, int32 or float32, and a byte per column |
| `logits.bin` | the model output: float32 |
| `pred.bin` | the final predictions: float32, class probabilities or a point estimate per query row |

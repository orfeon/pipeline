package com.mercari.solution.util.domain.attribution;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Accuracy-evaluation harness for the NetMan (Squeeze) public semi-synthetic datasets —
 * the field's standard benchmark, and the comparison ground for a future squeeze algorithm.
 *
 * <p>Dataset layout (per subfolder, from <a href="https://github.com/NetManAIOps/Squeeze">
 * NetManAIOps/Squeeze</a>, data on Zenodo record 8153367): one {@code {timestamp}.csv} per case
 * with attribute columns plus {@code real} (target) and {@code predict} (baseline), and an
 * {@code injection_info.csv} whose last column holds the ground-truth root cause sets
 * ({@code ;}-separated slices of {@code &}-joined attribute values).</p>
 *
 * <p>The bundled-sample test keeps the harness logic covered in CI; the full evaluation runs
 * manually: {@code mvn test -Dtest=NetManDatasetHarnessTest -Dattribution.netman.path=/path/to/datasets}
 * (the path contains dataset subfolders such as B0..B4, A).</p>
 */
public class NetManDatasetHarnessTest {

    private record Evaluation(int truePositive, int falsePositive, int falseNegative, int cases) {

        double f1() {
            final double precision = truePositive + falsePositive == 0
                    ? 0 : (double) truePositive / (truePositive + falsePositive);
            final double recall = truePositive + falseNegative == 0
                    ? 0 : (double) truePositive / (truePositive + falseNegative);
            return precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);
        }
    }

    @Test
    public void testHarnessOnBundledSample() throws Exception {
        final Path folder = Path.of("src/test/resources/attribution/netman-sample");
        final Evaluation evaluation = evaluateFolder(folder);
        Assertions.assertEquals(2, evaluation.cases());
        Assertions.assertEquals(1.0, evaluation.f1(), 1e-9,
                "the bundled sample culprits must be recovered exactly, but got tp=" + evaluation.truePositive()
                        + " fp=" + evaluation.falsePositive() + " fn=" + evaluation.falseNegative());
    }

    @Test
    @EnabledIfSystemProperty(named = "attribution.netman.path", matches = ".+")
    public void evaluateNetManDatasets() throws Exception {
        final Path root = Path.of(System.getProperty("attribution.netman.path"));
        System.out.println("dataset | cases | tp | fp | fn | f1");
        try(final Stream<Path> folders = Files.list(root)) {
            for(final Path folder : folders.filter(Files::isDirectory).sorted().toList()) {
                if(!Files.exists(folder.resolve("injection_info.csv"))) {
                    continue;
                }
                final Evaluation evaluation = evaluateFolder(folder);
                System.out.printf("%s | %d | %d | %d | %d | %.4f%n",
                        folder.getFileName(), evaluation.cases(), evaluation.truePositive(),
                        evaluation.falsePositive(), evaluation.falseNegative(), evaluation.f1());
            }
        }
    }

    private static Evaluation evaluateFolder(final Path folder) throws IOException {
        int truePositive = 0;
        int falsePositive = 0;
        int falseNegative = 0;
        int cases = 0;

        final List<String[]> injections = readCsv(folder.resolve("injection_info.csv"));
        // Column order varies across datasets (Squeeze A: "set,timestamp"; riskloc generator:
        // "timestamp,set,<metadata...>") — resolve both by header name
        final String[] infoHeader = injections.getFirst();
        int setIndex = infoHeader.length - 1;
        int timestampIndex = 0;
        for(int i = 0; i < infoHeader.length; i++) {
            if("set".equals(infoHeader[i])) {
                setIndex = i;
            } else if("timestamp".equals(infoHeader[i])) {
                timestampIndex = i;
            }
        }

        for(final String[] injection : injections.subList(1, injections.size())) {
            final String timestamp = injection[timestampIndex];
            final Set<String> truth = parseRootCauseSets(injection[setIndex]);

            final Path caseFile = folder.resolve(timestamp + ".csv");
            if(!Files.exists(caseFile)) {
                // Derived-measure datasets ({timestamp}.a.csv / .b.csv) are not evaluated yet
                continue;
            }
            final Set<String> predicted = localize(caseFile);
            cases++;

            for(final String slice : predicted) {
                if(truth.contains(slice)) {
                    truePositive++;
                } else {
                    falsePositive++;
                }
            }
            for(final String slice : truth) {
                if(!predicted.contains(slice)) {
                    falseNegative++;
                }
            }
        }
        return new Evaluation(truePositive, falsePositive, falseNegative, cases);
    }

    /** Runs the default riskloc engine on one case file and returns its slices in truth notation. */
    private static Set<String> localize(final Path caseFile) throws IOException {
        final List<String[]> rows = readCsv(caseFile);
        final String[] header = rows.getFirst();

        final List<Integer> dimIndexes = new ArrayList<>();
        final List<String> dimNames = new ArrayList<>();
        int realIndex = -1;
        int predictIndex = -1;
        for(int i = 0; i < header.length; i++) {
            switch (header[i]) {
                case "real" -> realIndex = i;
                case "predict" -> predictIndex = i;
                default -> {
                    dimIndexes.add(i);
                    dimNames.add(header[i]);
                }
            }
        }
        if(realIndex < 0 || predictIndex < 0) {
            throw new IllegalArgumentException("real/predict columns not found in " + caseFile);
        }

        final LeafTable.Builder builder = LeafTable.builder(dimNames, List.of("m"));
        for(final String[] row : rows.subList(1, rows.size())) {
            final String[] dims = new String[dimIndexes.size()];
            for(int i = 0; i < dimIndexes.size(); i++) {
                dims[i] = row[dimIndexes.get(i)];
            }
            builder.addBaseline(dims, new double[]{Double.parseDouble(row[predictIndex])});
            builder.addTarget(dims, new double[]{Double.parseDouble(row[realIndex])});
        }

        // Reference-parity configuration: the published algorithms run without support/cardinality
        // guards, search all layers, report every root cause (no top-K truncation) and have no
        // degenerate-cutoff guard (our production default) — disabled here so the scores are
        // directly comparable with the paper and the Python reference implementation
        final AttributionResult result = AttributionEngine.run(
                builder.build(),
                dimNames.stream().map(DimensionSpec::flat).toList(),
                List.of(MeasureSpec.fundamental("m")),
                new EngineConfig(
                        EngineConfig.Algorithm.riskloc,
                        new EngineConfig.RiskLocParams(0.5, 0.02, 1, false),
                        EngineConfig.AdtributorParams.defaults(),
                        new EngineConfig.Guards(0, dimNames.size(), 0),
                        DerivedAllocation.Method.gre,
                        Integer.MAX_VALUE),
                false);

        final Set<String> predicted = new HashSet<>();
        for(final Finding finding : result.results().getFirst().findings()) {
            for(final Slice slice : finding.slices()) {
                predicted.add(String.join("&", new TreeSet<>(List.of(slice.values()))));
            }
        }
        return predicted;
    }

    /**
     * Parses ground truth root cause sets into canonical slice strings with sorted values.
     * Handles both notations: "a1&b2;a3" (Squeeze datasets) and "a=a1&b=b2;a=a3"
     * (riskloc generate_dataset.py) — dimension prefixes are stripped.
     */
    private static Set<String> parseRootCauseSets(final String rootCauses) {
        final Set<String> sets = new HashSet<>();
        for(final String sliceText : rootCauses.split(";")) {
            if(sliceText.isBlank()) {
                continue;
            }
            final Set<String> values = new TreeSet<>();
            for(final String element : sliceText.trim().split("&")) {
                final int eq = element.indexOf('=');
                values.add(eq < 0 ? element : element.substring(eq + 1));
            }
            sets.add(String.join("&", values));
        }
        return sets;
    }

    private static List<String[]> readCsv(final Path file) throws IOException {
        final List<String[]> rows = new ArrayList<>();
        try(final BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while((line = reader.readLine()) != null) {
                if(!line.isBlank()) {
                    rows.add(line.trim().split(","));
                }
            }
        }
        return rows;
    }
}

package com.mercari.solution.util.domain.ml.tabular;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/** The reference values of structured-data-models: see {@code src/test/resources/onnx/kumo/README.md}. */
public record KumoFixture(Path dir, JsonObject meta, List<String> header, List<List<String>> rows) {

    public static final Path RESOURCES = Path.of("src", "test", "resources", "onnx", "kumo");

    public static KumoFixture load(final Path dir) {
        try {
            final List<String> lines = Files.readAllLines(dir.resolve("data.csv"));
            final List<List<String>> rows = new ArrayList<>();
            for(final String line : lines.subList(1, lines.size())) {
                rows.add(List.of(line.split(",", -1)));
            }
            final JsonObject meta = new Gson().fromJson(Files.readString(dir.resolve("meta.json")), JsonObject.class);
            return new KumoFixture(dir, meta, List.of(lines.getFirst().split(",", -1)), rows);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String name() {
        return meta.get("name").getAsString();
    }

    public boolean classification() {
        return "classification".equals(meta.get("task").getAsString());
    }

    public String target() {
        return meta.get("target").getAsString();
    }

    public int contextRows() {
        return meta.get("context").getAsInt();
    }

    public List<String> classes() {
        return meta.get("classes").getAsJsonArray().asList().stream().map(JsonElement::getAsString).toList();
    }

    public List<String> columns(final String kind) {
        return header.stream()
                .filter(column -> !column.equals(target()))
                .filter(column -> kind.equals(meta.getAsJsonObject("stypes").get(column).getAsString()))
                .toList();
    }

    /** The value of a cell as a pipeline holds it: a number if the whole column is numbers, null if empty. */
    public Object value(final int row, final String column) {
        final String text = rows.get(row).get(header.indexOf(column));
        if(text.isEmpty()) {
            return null;
        }
        return isNumberColumn(column) ? (Object) Double.parseDouble(text) : text;
    }

    public boolean isNumberColumn(final String column) {
        // a class is a label, also if it looks like a number
        if(column.equals(target()) && classification()) {
            return false;
        }
        final int index = header.indexOf(column);
        for(final List<String> row : rows) {
            if(!row.get(index).isEmpty()) {
                try {
                    Double.parseDouble(row.get(index));
                } catch (final NumberFormatException e) {
                    return false;
                }
            }
        }
        return true;
    }

    public float[] floats(final String file) {
        final ByteBuffer buffer = buffer(file);
        final float[] values = new float[buffer.remaining() / Float.BYTES];
        buffer.asFloatBuffer().get(values);
        return values;
    }

    public int[] ints(final String file) {
        final ByteBuffer buffer = buffer(file);
        final int[] values = new int[buffer.remaining() / Integer.BYTES];
        buffer.asIntBuffer().get(values);
        return values;
    }

    public byte[] bytes(final String file) {
        final ByteBuffer buffer = buffer(file);
        final byte[] values = new byte[buffer.remaining()];
        buffer.get(values);
        return values;
    }

    private ByteBuffer buffer(final String file) {
        try {
            return ByteBuffer.wrap(Files.readAllBytes(dir.resolve(file))).order(ByteOrder.LITTLE_ENDIAN);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}

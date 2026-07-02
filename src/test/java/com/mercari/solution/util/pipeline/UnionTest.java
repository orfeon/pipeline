package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MCollectionTuple;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UnionTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static Schema createSchema1() {
        return Schema.of(List.of(
                Schema.Field.of("key", Schema.FieldType.STRING),
                Schema.Field.of("fieldA", Schema.FieldType.INT64)));
    }

    private static Schema createSchema2() {
        return Schema.of(List.of(
                Schema.Field.of("key", Schema.FieldType.STRING),
                Schema.Field.of("fieldB", Schema.FieldType.FLOAT64)));
    }

    private static MElement element1(String key, long value) {
        final Map<String, Object> values = new HashMap<>();
        values.put("key", key);
        values.put("fieldA", value);
        return MElement.of(values, Instant.parse("2025-01-01T00:00:00Z"));
    }

    private static MElement element2(String key, double value) {
        final Map<String, Object> values = new HashMap<>();
        values.put("key", key);
        values.put("fieldB", value);
        return MElement.of(values, Instant.parse("2025-01-01T00:00:00Z"));
    }

    @Test
    public void testCreateUnionSchema() {
        final Schema union = Union.createUnionSchema(List.of(createSchema1(), createSchema2()));
        Assertions.assertEquals(3, union.getFields().size());
        Assertions.assertEquals(Schema.Type.string, union.getField("key").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.int64, union.getField("fieldA").getFieldType().getType());
        Assertions.assertEquals(Schema.Type.float64, union.getField("fieldB").getFieldType().getType());
    }

    @Test
    public void testParametersDefaults() {
        final Union.Parameters parameters = Union.Parameters.createDefaultParameter();
        Assertions.assertFalse(parameters.each);
        Assertions.assertTrue(parameters.mappings.isEmpty());
    }

    @Test
    public void testUnionFlatten() {
        final Schema schema1 = createSchema1();
        final Schema schema2 = createSchema2();

        final PCollection<MElement> input1 = pipeline
                .apply("Create1", Create
                        .of(element1("a", 1L), element1("b", 2L))
                        .withCoder(ElementCoder.of(schema1)));
        final PCollection<MElement> input2 = pipeline
                .apply("Create2", Create
                        .of(element2("c", 1.5D))
                        .withCoder(ElementCoder.of(schema2)));

        final MCollectionTuple inputs = MCollectionTuple
                .of("input1", input1, schema1)
                .and("input2", input2, schema2);

        final Schema unionSchema = Union.createUnionSchema(inputs);
        Assertions.assertEquals(3, unionSchema.getFields().size());

        final PCollection<MElement> unified = inputs.apply("Union", Union.flatten());

        PAssert.that(unified).satisfies(elements -> {
            final Set<String> keys = new HashSet<>();
            int count = 0;
            for(final MElement element : elements) {
                keys.add(element.getAsString("key"));
                count++;
            }
            Assertions.assertEquals(3, count);
            Assertions.assertEquals(Set.of("a", "b", "c"), keys);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionWithKey() {
        final Schema schema1 = createSchema1();
        final Schema schema2 = createSchema2();

        final PCollection<MElement> input1 = pipeline
                .apply("Create1", Create
                        .of(element1("a", 1L), element1("b", 2L))
                        .withCoder(ElementCoder.of(schema1)));
        final PCollection<MElement> input2 = pipeline
                .apply("Create2", Create
                        .of(element2("a", 1.5D))
                        .withCoder(ElementCoder.of(schema2)));

        final MCollectionTuple inputs = MCollectionTuple
                .of("input1", input1, schema1)
                .and("input2", input2, schema2);

        final PCollection<KV<String, MElement>> unified = inputs
                .apply("UnionWithKey", Union.withKeys(List.of("key")));

        PAssert.that(unified).satisfies(kvs -> {
            int countA = 0;
            int total = 0;
            for(final KV<String, MElement> kv : kvs) {
                if("a".equals(kv.getKey())) {
                    countA++;
                }
                total++;
            }
            Assertions.assertEquals(3, total);
            Assertions.assertEquals(2, countA);
            return null;
        });

        pipeline.run();
    }

    @Test
    public void testUnionSingleInput() {
        final Schema schema1 = createSchema1();
        final PCollection<MElement> input1 = pipeline
                .apply("Create1", Create
                        .of(element1("a", 1L))
                        .withCoder(ElementCoder.of(schema1)));

        final MCollectionTuple inputs = MCollectionTuple.of("input1", input1, schema1);
        Assertions.assertEquals(2, Union.createUnionSchema(inputs).getFields().size());

        final PCollection<MElement> unified = inputs.apply("Union", Union.flatten());
        PAssert.that(unified).satisfies(elements -> {
            int count = 0;
            for(final MElement ignored : elements) {
                count++;
            }
            Assertions.assertEquals(1, count);
            return null;
        });

        final PCollection<KV<String, MElement>> withKey = inputs
                .apply("UnionWithKey", Union.withKeys(List.of("key")));
        PAssert.that(withKey).satisfies(kvs -> {
            for(final KV<String, MElement> kv : kvs) {
                Assertions.assertEquals("a", kv.getKey());
            }
            return null;
        });

        pipeline.run();
    }

}

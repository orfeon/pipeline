package com.mercari.solution.module.sink;

import com.mercari.solution.module.MElement;
import com.mercari.solution.util.domain.db.JdbcUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSinkTest {

    @Test
    public void testBulkInsertBufferInsertOrDoNothingKeepsFirstElement() {
        final JdbcSink.BulkInsertBuffer buffer = new JdbcSink.BulkInsertBuffer(
                JdbcUtil.OP.INSERT_OR_DONOTHING, List.of("id"), 2);

        buffer.add(element(Map.of("id", 1, "name", "first")));
        buffer.add(element(Map.of("id", 1, "name", "second")));

        Assertions.assertEquals(1, buffer.size());
        Assertions.assertEquals("first", buffer.get(0).getPrimitiveValue("name"));
        Assertions.assertFalse(buffer.isFull());
    }

    @Test
    public void testBulkInsertBufferInsertOrUpdateKeepsLastElement() {
        final JdbcSink.BulkInsertBuffer buffer = new JdbcSink.BulkInsertBuffer(
                JdbcUtil.OP.INSERT_OR_UPDATE, List.of("id"), 2);

        buffer.add(element(Map.of("id", 1, "name", "first")));
        buffer.add(element(Map.of("id", 1, "name", "second")));

        Assertions.assertEquals(1, buffer.size());
        Assertions.assertEquals("second", buffer.get(0).getPrimitiveValue("name"));
        Assertions.assertFalse(buffer.isFull());
    }

    @Test
    public void testBulkInsertBufferUsesCompositeKey() {
        final JdbcSink.BulkInsertBuffer buffer = new JdbcSink.BulkInsertBuffer(
                JdbcUtil.OP.INSERT_OR_UPDATE, List.of("id", "category"), 2);

        buffer.add(element(Map.of("id", 1, "category", "a", "name", "first")));
        buffer.add(element(Map.of("id", 1, "category", "a", "name", "second")));
        buffer.add(element(Map.of("id", 1, "category", "b", "name", "third")));

        Assertions.assertEquals(2, buffer.size());
        Assertions.assertTrue(buffer.isFull());
        Assertions.assertEquals("second", buffer.get(0).getPrimitiveValue("name"));
        Assertions.assertEquals("third", buffer.get(1).getPrimitiveValue("name"));
    }

    @Test
    public void testBulkInsertBufferDoesNotDeduplicateNullKeys() {
        final JdbcSink.BulkInsertBuffer buffer = new JdbcSink.BulkInsertBuffer(
                JdbcUtil.OP.INSERT_OR_DONOTHING, List.of("id"), 2);
        final Map<String, Object> first = new HashMap<>();
        first.put("id", null);
        first.put("name", "first");
        final Map<String, Object> second = new HashMap<>();
        second.put("id", null);
        second.put("name", "second");

        buffer.add(element(first));
        buffer.add(element(second));

        Assertions.assertEquals(2, buffer.size());
        Assertions.assertTrue(buffer.isFull());
    }

    @Test
    public void testBulkInsertBufferNormalizesKeyValues() {
        final JdbcSink.BulkInsertBuffer buffer = new JdbcSink.BulkInsertBuffer(
                JdbcUtil.OP.INSERT_OR_UPDATE, List.of("number", "bytes"), 2);

        buffer.add(element(Map.of(
                "number", new BigDecimal("1.0"),
                "bytes", ByteBuffer.wrap(new byte[]{1, 2}),
                "name", "first")));
        buffer.add(element(Map.of(
                "number", new BigDecimal("1.00"),
                "bytes", new byte[]{1, 2},
                "name", "second")));

        Assertions.assertEquals(1, buffer.size());
        Assertions.assertEquals("second", buffer.get(0).getPrimitiveValue("name"));
    }

    @Test
    public void testBulkInsertBufferInsertKeepsDuplicateKeys() {
        final JdbcSink.BulkInsertBuffer buffer = new JdbcSink.BulkInsertBuffer(
                JdbcUtil.OP.INSERT, List.of("id"), 2);

        buffer.add(element(Map.of("id", 1)));
        buffer.add(element(Map.of("id", 1)));

        Assertions.assertEquals(2, buffer.size());
        Assertions.assertTrue(buffer.isFull());
    }

    private static MElement element(final Map<String, Object> values) {
        return MElement.of(values, 0L);
    }
}

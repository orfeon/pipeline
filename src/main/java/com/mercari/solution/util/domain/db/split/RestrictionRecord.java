package com.mercari.solution.util.domain.db.split;

import com.mercari.solution.module.MElement;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.Map;

@DefaultCoder(AvroCoder.class)
public class RestrictionRecord {

    @Nullable
    private IndexPosition from;
    @Nullable
    private IndexPosition to;

    @Nullable
    private Long count;
    @Nullable
    private Boolean completed;

    @Nullable
    private Long durationMillis;
    @Nullable
    private Instant timestamp;

    @Nullable
    private IndexPosition prevFrom;
    @Nullable
    private IndexPosition prevTo;


    public void setPrevFrom(IndexPosition prevFrom) {
        this.prevFrom = prevFrom;
    }

    public void setPrevTo(IndexPosition prevTo) {
        this.prevTo = prevTo;
    }

    public static RestrictionRecord of(
            final IndexRange range,
            final Long count,
            final Long durationMillis) {

        final RestrictionRecord record = new RestrictionRecord();
        record.from = range.getFrom().copy();
        record.to = range.getTo().copy();
        record.count = count;
        record.durationMillis = durationMillis;
        record.timestamp = Instant.now();
        record.completed = true;

        return record;
    }

    public Map<String, Object> toMap() {
        final Map<String, Object> values = new HashMap<>();
        values.put("from", from.toMap());
        values.put("to", to.toMap());
        values.put("count", count);
        values.put("completed", completed);
        values.put("durationMillis", durationMillis);
        values.put("timestamp", timestamp.getMillis() * 1000L);
        if(prevFrom != null) {
            values.put("prevFrom", prevFrom.toMap());
        }
        if(prevTo != null) {
            values.put("prevTo", prevTo.toMap());
        }
        return values;
    }

    public MElement toElement(Schema schema, Instant timestamp) {
        final GenericRecord record = AvroSchemaUtil.create(schema, toMap());
        return MElement.of(record, timestamp);
    }

    public static Schema createAvroSchema() {
        return SchemaBuilder.builder()
                .record("RestrictionRecord")
                .fields()
                .name("from").type(Schema.createUnion(
                        IndexPosition.createAvroSchema("from"),
                        Schema.create(Schema.Type.NULL))
                ).noDefault()
                .name("to").type(Schema.createUnion(
                        IndexPosition.createAvroSchema("to"),
                        Schema.create(Schema.Type.NULL))
                ).noDefault()
                .name("count").type(Schema.create(Schema.Type.LONG)).noDefault()
                .name("completed").type(Schema.create(Schema.Type.BOOLEAN)).noDefault()
                .name("durationMillis").type(Schema.create(Schema.Type.LONG)).noDefault()
                .name("timestamp").type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .name("prevFrom").type(Schema.createUnion(
                        IndexPosition.createAvroSchema("prevFrom"),
                        Schema.create(Schema.Type.NULL))
                ).noDefault()
                .name("prevTo").type(Schema.createUnion(
                        IndexPosition.createAvroSchema("prevTo"),
                        Schema.create(Schema.Type.NULL))
                ).noDefault()
                .endRecord();
    }

}

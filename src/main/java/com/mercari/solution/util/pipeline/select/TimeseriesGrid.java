package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.pipeline.outbound.Durations;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.*;

/**
 * Aligns points in any order (an {@code array_agg} result) to a regular grid for a time series model. A time is floored
 * to a multiple of {@code interval} from the epoch (UTC), and the grid ends at the latest point, at most {@code length}
 * values long. A time without a value is NaN: closing the gap would shift the times of a forecast.
 *
 * <pre>{@code
 * { "name": "grid", "func": "timeseries_grid", "field": "points", "timestampField": "timestamp", "valueField": "value", "interval": "1h", "length": 512 }
 * }</pre>
 */
public class TimeseriesGrid implements SelectFunction {

    public enum Aggregation implements Serializable {
        last,
        mean,
        sum
    }

    private final String name;
    private final String field;
    private final String timestampField;
    private final String valueField;
    private final long intervalMicros;
    private final int length;
    private final Aggregation aggregation;
    private final boolean dateField;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private TimeseriesGrid(
            final String name,
            final String field,
            final String timestampField,
            final String valueField,
            final long intervalMicros,
            final int length,
            final Aggregation aggregation,
            final boolean dateField,
            final Schema.Field inputField,
            final boolean ignore) {

        this.name = name;
        this.field = field;
        this.timestampField = timestampField;
        this.valueField = valueField;
        this.intervalMicros = intervalMicros;
        this.length = length;
        this.aggregation = aggregation;
        this.dateField = dateField;
        this.inputFields = new ArrayList<>();
        this.inputFields.add(inputField);
        this.outputFieldType = Schema.FieldType.element(List.of(
                Schema.Field.of("values", Schema.FieldType.array(Schema.FieldType.FLOAT64)),
                Schema.Field.of("timestamps", Schema.FieldType.array(Schema.FieldType.TIMESTAMP)),
                Schema.Field.of("lastTimestamp", Schema.FieldType.TIMESTAMP),
                Schema.Field.of("observed", Schema.FieldType.array(Schema.FieldType.BOOLEAN))))
                .withNullable(true);
        this.ignore = ignore;
    }

    public static TimeseriesGrid of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        for(final String parameter : List.of("field", "timestampField", "valueField", "interval", "length")) {
            if(!jsonObject.has(parameter)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_grid requires " + parameter + " parameter");
            }
        }
        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(type.getType() != Schema.Type.array || type.getArrayValueType().getType() != Schema.Type.element) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_grid field: " + field + " must be an array of elements, but got: " + type);
        }
        final List<Schema.Field> pointFields = type.getArrayValueType().getElementSchema().getFields();

        final String timestampField = jsonObject.get("timestampField").getAsString();
        final boolean dateField = isDate(name, timestampField, ElementSchemaUtil.getInputFieldType(timestampField, pointFields));
        final String valueField = jsonObject.get("valueField").getAsString();
        final Schema.FieldType valueType = ElementSchemaUtil.getInputFieldType(valueField, pointFields);
        if(!Vectors.isNumeric(valueType)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_grid valueField: " + valueField + " must be numeric, but got: " + valueType);
        }

        final long intervalMicros = intervalMicros(name, jsonObject.get("interval").getAsString());
        final int length = jsonObject.get("length").getAsInt();
        if(length < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_grid length must be over than zero: " + length);
        }
        final Aggregation aggregation = jsonObject.has("aggregation")
                ? Aggregation.valueOf(jsonObject.get("aggregation").getAsString()) : Aggregation.last;

        final String parent = ElementSchemaUtil.getInputFieldName(field, inputFields);
        final Schema.Field inputField = Schema.Field.of(parent, ElementSchemaUtil.getInputFieldType(parent, inputFields));
        return new TimeseriesGrid(name, field, timestampField, valueField, intervalMicros, length, aggregation, dateField, inputField, ignore);
    }

    static long intervalMicros(final String name, final String interval) {
        final long micros = Durations.parse(interval).toNanos() / 1000L;
        if(micros < 1) {
            throw new IllegalArgumentException("SelectField: " + name + " interval must be over than zero: " + interval);
        }
        return micros;
    }

    static boolean isDate(final String name, final String field, final Schema.FieldType type) {
        return switch (type.getType()) {
            case timestamp -> false;
            case date -> true;
            default -> throw new IllegalArgumentException("SelectField: " + name + " field: " + field + " must be a timestamp or a date, but got: " + type);
        };
    }

    // a record field holds a timestamp as epoch microseconds and a date as epoch days
    static Long toMicros(final Object value, final boolean date) {
        return switch (value) {
            case null -> null;
            case Number number -> date ? number.longValue() * 86_400_000_000L : number.longValue();
            case Instant instant -> instant.getMillis() * 1000L;
            case String text -> DateTimeUtil.toEpochMicroSecond(DateTimeUtil.toInstant(text));
            default -> throw new IllegalArgumentException("Not a timestamp or a date: " + value);
        };
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public List<Schema.Field> getInputFields() {
        return inputFields;
    }

    @Override
    public Schema.FieldType getOutputFieldType() {
        return outputFieldType;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        final Object value = ElementSchemaUtil.getValue(input, field);
        if(value == null) {
            return null;
        }
        if(!(value instanceof List<?> points)) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_grid field: " + field + " must be an array, but got: " + value);
        }

        long firstIndex = Long.MAX_VALUE;
        long lastIndex = Long.MIN_VALUE;
        for(final Object point : points) {
            final Long time = point == null ? null : toMicros(pointValue(point, timestampField), dateField);
            if(time != null) {
                final long index = Math.floorDiv(time, intervalMicros);
                firstIndex = Math.min(firstIndex, index);
                lastIndex = Math.max(lastIndex, index);
            }
        }
        if(lastIndex == Long.MIN_VALUE) {
            return null;
        }
        final long startIndex = Math.max(firstIndex, lastIndex - length + 1);
        final int size = Math.toIntExact(lastIndex - startIndex + 1);

        final double[] values = new double[size];
        final int[] counts = new int[size];
        final long[] times = new long[size];
        for(final Object point : points) {
            final Long time = point == null ? null : toMicros(pointValue(point, timestampField), dateField);
            if(time == null) {
                continue;
            }
            final long index = Math.floorDiv(time, intervalMicros);
            final Object pointValue = pointValue(point, valueField);
            final double number = pointValue == null ? Double.NaN : ((Number) pointValue).doubleValue();
            if(index < startIndex || Double.isNaN(number)) {
                continue;
            }
            final int i = (int) (index - startIndex);
            switch (aggregation) {
                case last -> {
                    // array_agg gives no order: the last value is that of the latest time in the interval
                    if(counts[i] == 0 || time >= times[i]) {
                        values[i] = number;
                        times[i] = time;
                    }
                }
                case mean, sum -> values[i] = counts[i] == 0 ? number : values[i] + number;
            }
            counts[i]++;
        }

        final List<Double> gridValues = new ArrayList<>(size);
        final List<Long> gridTimes = new ArrayList<>(size);
        final List<Boolean> observed = new ArrayList<>(size);
        for(int i=0; i<size; i++) {
            final double mean = aggregation == Aggregation.mean ? values[i] / counts[i] : values[i];
            gridValues.add(counts[i] == 0 ? Double.NaN : mean);
            gridTimes.add((startIndex + i) * intervalMicros);
            observed.add(counts[i] > 0);
        }
        final Map<String, Object> grid = new HashMap<>();
        grid.put("values", gridValues);
        grid.put("timestamps", gridTimes);
        grid.put("lastTimestamp", lastIndex * intervalMicros);
        grid.put("observed", observed);
        return grid;
    }

    private Object pointValue(final Object point, final String name) {
        if(!(point instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("SelectField: " + this.name + " func: timeseries_grid field: " + field + " must be an array of elements, but got: " + point);
        }
        return map.get(name);
    }

}

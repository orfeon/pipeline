package com.mercari.solution.util.pipeline.select;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.math.Tensor;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import org.joda.time.Instant;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * Turns a matrix of forecast quantiles to an array of structs, one per step: the first step is {@code interval} after
 * {@code start}. A level names its field by its decimals, at least two: 0.1 is {@code q10}, 0.05 {@code q05}, 0.975 {@code q975}.
 *
 * <pre>{@code
 * { "name": "forecast", "func": "timeseries_quantiles", "field": "forecasts", "levels": [0.1, 0.5, 0.9],
 *   "start": "grid.lastTimestamp", "interval": "1h" }
 * }</pre>
 */
public class TimeseriesQuantiles implements SelectFunction {

    public enum Axis implements Serializable {
        quantile_step,
        step_quantile
    }

    private record Quantile(String name, int index) implements Serializable {}

    private final String name;
    private final String field;
    private final String start;
    private final boolean startDate;
    private final long intervalMicros;
    private final int levels;
    private final Axis axis;
    private final List<Quantile> quantiles;
    private final boolean outputDouble;

    private final List<Schema.Field> inputFields;
    private final Schema.FieldType outputFieldType;
    private final boolean ignore;

    private TimeseriesQuantiles(
            final String name,
            final String field,
            final String start,
            final boolean startDate,
            final long intervalMicros,
            final int levels,
            final Axis axis,
            final List<Quantile> quantiles,
            final boolean outputDouble,
            final List<Schema.Field> inputFields,
            final Schema.FieldType outputFieldType,
            final boolean ignore) {

        this.name = name;
        this.field = field;
        this.start = start;
        this.startDate = startDate;
        this.intervalMicros = intervalMicros;
        this.levels = levels;
        this.axis = axis;
        this.quantiles = quantiles;
        this.outputDouble = outputDouble;
        this.inputFields = inputFields;
        this.outputFieldType = outputFieldType;
        this.ignore = ignore;
    }

    public static TimeseriesQuantiles of(final String name, final JsonObject jsonObject, final List<Schema.Field> inputFields, final boolean ignore) {
        for(final String parameter : List.of("field", "levels", "start", "interval")) {
            if(!jsonObject.has(parameter)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_quantiles requires " + parameter + " parameter");
            }
        }
        final String field = jsonObject.get("field").getAsString();
        final Schema.FieldType type = ElementSchemaUtil.getInputFieldType(field, inputFields);
        if(type.getType() != Schema.Type.matrix || type.getShape().size() != 2 || !Vectors.isNumeric(type.getMatrixValueType())) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_quantiles field: " + field + " must be a numeric matrix of two dimensions, but got: " + type);
        }

        final Axis axis = jsonObject.has("axis") ? Axis.valueOf(jsonObject.get("axis").getAsString()) : Axis.quantile_step;
        final List<String> levelTexts = levels(name, "timeseries_quantiles", "levels", jsonObject.get("levels"));
        final int declared = type.getShape().get(axis == Axis.quantile_step ? 0 : 1);
        if(declared > 0 && declared != levelTexts.size()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_quantiles has " + levelTexts.size()
                    + " levels, but field: " + field + " of shape " + type.getShape() + " and axis " + axis + " has " + declared);
        }

        final List<String> selected = jsonObject.has("select") ? levels(name, "timeseries_quantiles", "select", jsonObject.get("select")) : levelTexts;
        final boolean outputDouble = type.getMatrixValueType().getType() == Schema.Type.float64;
        final Schema.FieldType realType = (outputDouble ? Schema.FieldType.FLOAT64 : Schema.FieldType.FLOAT32).withNullable(false);
        final List<Quantile> quantiles = new ArrayList<>();
        final List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP));
        for(final String level : selected) {
            final int index = levelTexts.indexOf(level);
            if(index < 0) {
                throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_quantiles select: " + level + " is not in levels: " + levelTexts);
            }
            final String quantileName = quantileName(level);
            quantiles.add(new Quantile(quantileName, index));
            fields.add(Schema.Field.of(quantileName, realType));
        }

        final String start = jsonObject.get("start").getAsString();
        final boolean startDate = TimeseriesGrid.isDate(name, start, ElementSchemaUtil.getInputFieldType(start, inputFields));
        final long intervalMicros = TimeseriesGrid.intervalMicros(name, jsonObject.get("interval").getAsString());

        final List<Schema.Field> parents = new ArrayList<>();
        for(final String input : List.of(field, start)) {
            final String parent = ElementSchemaUtil.getInputFieldName(input, inputFields);
            parents.add(Schema.Field.of(parent, ElementSchemaUtil.getInputFieldType(parent, inputFields)));
        }
        return new TimeseriesQuantiles(name, field, start, startDate, intervalMicros, levelTexts.size(), axis, quantiles, outputDouble,
                parents, Schema.FieldType.array(Schema.FieldType.element(fields)), ignore);
    }

    // canonical decimal texts: the same level is the same text however it was written
    static List<String> levels(final String name, final String func, final String parameter, final JsonElement element) {
        if(!element.isJsonArray() || element.getAsJsonArray().isEmpty()) {
            throw new IllegalArgumentException("SelectField: " + name + " func: " + func + " " + parameter + " must be an array of levels: " + element);
        }
        final JsonArray array = element.getAsJsonArray();
        final List<String> levels = new ArrayList<>();
        for(final JsonElement level : array) {
            final BigDecimal value = new BigDecimal(level.getAsString());
            if(value.signum() <= 0 || value.compareTo(BigDecimal.ONE) >= 0) {
                throw new IllegalArgumentException("SelectField: " + name + " func: " + func + " " + parameter + " must be between 0 and 1: " + level);
            }
            final String text = value.stripTrailingZeros().toPlainString();
            if(levels.contains(text)) {
                throw new IllegalArgumentException("SelectField: " + name + " func: " + func + " " + parameter + " has " + text + " more than once");
            }
            levels.add(text);
        }
        return levels;
    }

    static String quantileName(final String level) {
        final String decimals = level.substring(level.indexOf('.') + 1);
        return "q" + (decimals.length() < 2 ? decimals + "0" : decimals);
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
    public boolean acceptsTensor() {
        return true;
    }

    @Override
    public void setup() {

    }

    @Override
    public Object apply(final Map<String, Object> input, final Instant timestamp) {
        final Object value = ElementSchemaUtil.getValue(input, field);
        final Long startMicros = TimeseriesGrid.toMicros(ElementSchemaUtil.getValue(input, start), startDate);
        if(value == null || startMicros == null) {
            return null;
        }
        final Tensor tensor = switch (value) {
            case Tensor t -> t;
            case List<?> list -> Tensor.of(Tensor.Type.DOUBLE, list, flatShape(list.size()));
            default -> throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_quantiles requires a matrix, but got: " + value);
        };
        final long[] shape = tensor.shape();
        final int quantileDim = axis == Axis.quantile_step ? 0 : 1;
        if(shape.length != 2 || shape[quantileDim] != levels) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_quantiles has " + levels + " levels on axis " + axis + ", but got: " + tensor);
        }

        final int steps = (int) shape[1 - quantileDim];
        final List<Map<String, Object>> forecast = new ArrayList<>(steps);
        for(int step=0; step<steps; step++) {
            final Map<String, Object> values = new HashMap<>();
            values.put("timestamp", startMicros + (step + 1) * intervalMicros);
            for(final Quantile quantile : quantiles) {
                final int index = axis == Axis.quantile_step ? quantile.index * steps + step : step * levels + quantile.index;
                final double number = tensor.getDouble(index);
                values.put(quantile.name, outputDouble ? (Object) number : (Object) (float) number);
            }
            forecast.add(values);
        }
        return forecast;
    }

    // a matrix is flat in a record
    private long[] flatShape(final int size) {
        if(size % levels != 0) {
            throw new IllegalArgumentException("SelectField: " + name + " func: timeseries_quantiles has " + size + " values for " + levels + " levels");
        }
        final long steps = size / levels;
        return axis == Axis.quantile_step ? new long[]{levels, steps} : new long[]{steps, levels};
    }

}

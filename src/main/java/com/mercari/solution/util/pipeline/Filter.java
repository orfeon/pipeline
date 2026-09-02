package com.mercari.solution.util.pipeline;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.ExpressionUtil;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.schema.ElementSchemaUtil;
import com.mercari.solution.util.ExpressionUtil.Expression;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Filter implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Filter.class);

    private final String filterJson;

    private transient Filter.ConditionNode conditionNode;

    private Filter(final String filterJson) {
        this.filterJson = filterJson;
    }

    public static Filter of(final JsonElement filterJson) {
        final String filterText = Optional
                .ofNullable(filterJson)
                .map(JsonElement::toString)
                .orElse(null);
        return new Filter(filterText);
    }

    public static Filter of(final String filterJson) {
        return new Filter(filterJson);
    }

    public void setup() {
        if(this.filterJson != null) {
            this.conditionNode = parse(filterJson);
        }
    }

    public boolean filter(final MElement element) {
        if(conditionNode == null) {
            return true;
        }
        //return filter(element.asPrimitiveMap(conditionNode.getRequiredVariables()));
        return filter(element.asPrimitiveMap());
    }

    public boolean filter(final List<Schema.Field> fields, final MElement element) {
        if(conditionNode == null) {
            return true;
        }
        return filter(element.asStandardMap(fields, conditionNode.getRequiredVariables()));
    }

    public boolean filter(final Map<String, Object> primitiveValues) {
        return filter(conditionNode, primitiveValues);
    }

    public JsonElement toJson() {
        if(filterJson == null) {
            return JsonNull.INSTANCE;
        }
        try {
            return JsonUtil.fromJson(filterJson);
        } catch (final RuntimeException e) {
            // SQL-like condition text is not JSON: keep it as a string value
            return new JsonPrimitive(filterJson);
        }
    }

    public enum Type implements Serializable {
        AND,
        OR,
        TRUE,
        FALSE
    }

    public enum Op implements Serializable {
        EQUAL("="),
        NOT_EQUAL("!="),
        GREATER(">"),
        GREATER_OR_EQUAL(">="),
        LESSER("<"),
        LESSER_OR_EQUAL("<="),
        IN("in"),
        NOT_IN("not in"),
        MATCH("match"),
        NOT_MATCH("not match"),
        TRUE("true"),
        FALSE("false");

        private String name;

        Op(final String name) {
            this.name = name;
        }

        public static Op of(final String name) {
            for(final Op op : values()) {
                if(op.name.equalsIgnoreCase(name.trim())) {
                    return op;
                }
            }
            throw new IllegalArgumentException("Filter.Op: " + name + " not found.");
        }
    }

    public static class ConditionNode implements Serializable {

        private Type type;
        private List<ConditionNode> nodes;
        private List<ConditionLeaf> leaves;

        private Set<String> variables;

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public List<ConditionNode> getNodes() {
            return nodes;
        }

        public void setNodes(List<ConditionNode> nodes) {
            this.nodes = nodes;
        }

        public List<ConditionLeaf> getLeaves() {
            return leaves;
        }

        public void setLeaves(List<ConditionLeaf> leaves) {
            this.leaves = leaves;
        }

        public Set<String> getRequiredVariables() {
            final Set<String> variables = new HashSet<>();
            if(this.nodes != null && !this.nodes.isEmpty()) {
                for(final ConditionNode node : this.nodes) {
                    variables.addAll(node.getRequiredVariables());
                }
            }
            if(this.leaves != null && !this.leaves.isEmpty()) {
                for(final ConditionLeaf leaf : this.leaves) {
                    variables.addAll(leaf.getRequiredVariables());
                }
            }
            return variables;
        }

        public List<String> validate(final List<Schema.Field> fields) {
            final List<String> errorMessages = new ArrayList<>();
            final Set<String> fieldNames = fields.stream()
                    .flatMap(f -> getFieldNames(null, f).stream())
                    .collect(Collectors.toSet());
            final Set<String> requiredVariables = getRequiredVariables();
            if(fieldNames.containsAll(requiredVariables)) {
                return errorMessages;
            }
            for(final String requiredVariable : requiredVariables) {
                if(!fieldNames.contains(requiredVariable)) {
                    // callers validate against different schemas (input, output, ...): keep the message neutral
                    errorMessages.add("filter variable: " + requiredVariable + " not found in schema fields: " + fieldNames);
                }
            }

            return errorMessages;
        }

        public static Set<String> getFieldNames(String parent, final Schema.Field field) {
            final Set<String> fieldNames = new HashSet<>();
            final String fieldName;
            if(parent == null) {
                fieldName = field.getName();
            } else {
                fieldName = parent + "." + field.getName();
            }
            switch (field.getFieldType().getType()) {
                case element -> {
                    fieldNames.add(fieldName);
                    for(final Schema.Field childField : field.getFieldType().getElementSchema().getFields()) {
                        final Set<String> childFieldNames = getFieldNames(fieldName, childField);
                        fieldNames.addAll(childFieldNames);
                    }
                }
                default -> {
                    fieldNames.add(fieldName);
                }
            }
            return fieldNames;
        }

        @Override
        public String toString() {
            return String.format("{ Type: %s, Conditions: [ %s ], Children: [ %s ] }",
                    this.type,
                    Optional.ofNullable(this.leaves).orElse(new ArrayList<>())
                            .stream()
                            .map(ConditionLeaf::toString)
                            .collect(Collectors.joining(", ")),
                    Optional.ofNullable(this.nodes).orElse(new ArrayList<>())
                            .stream()
                            .map(ConditionNode::toString)
                            .collect(Collectors.joining(", "))
            );
        }

    }

    public static class ConditionLeaf implements Serializable {

        private String key;
        private Op op;
        private JsonElement value;
        private String valueKey;

        private Expression expression;
        private Set<String> expressionVariables;
        private String expressionString;

        private Pattern pattern;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Op getOp() {
            return op;
        }

        public void setOp(Op op) {
            this.op = op;
        }

        public JsonElement getValue() {
            return value;
        }

        public void setValue(JsonElement value) {
            this.value = value;
        }

        public String getValueKey() {
            return valueKey;
        }

        public void setValueKey(String valueKey) {
            this.valueKey = valueKey;
        }

        @Override
        public String toString() {
            return String.format("%s %s %s",
                    this.expression != null ? "(" + this.expressionString + ")" : this.key,
                    this.op,
                    this.valueKey != null ? this.valueKey : this.value);
        }

        public Double evaluateExpression(final MElement input) {
            final Map<String, Double> variables = new HashMap<>();
            for(final String variableName : this.expression.getVariableNames()) {
                final Double fieldValue = input.getAsDouble(variableName);
                variables.put(variableName, fieldValue);
            }
            return expression.evaluate(variables);
        }

        public Double evaluateExpression(final MElement input, final Map<String, Object> values) {
            final Map<String, Double> variables = new HashMap<>();
            for(final String variableName : this.expression.getVariableNames()) {
                final Object fieldValue;
                if(values == null) {
                    fieldValue = input.getAsDouble(variableName);
                } else {
                    fieldValue = Optional.ofNullable(values.get(variableName)).orElseGet(() -> input.getAsDouble(variableName));
                }
                variables.put(variableName, ExpressionUtil.getAsDouble(fieldValue));
            }
            return expression.evaluate(variables);
        }

        public Set<String> getRequiredVariables() {
            final Set<String> variables = new HashSet<>();
            if(this.expression != null) {
                variables.addAll(this.expression.getVariableNames());
            } else if(this.key != null) {
                variables.add(this.key);
            }
            if(this.valueKey != null) {
                variables.add(this.valueKey);
            }
            return variables;
        }

    }

    public static ConditionNode parse(final String filterText) {
        if(filterText == null) {
            return parse((JsonElement) null);
        }
        final JsonElement jsonElement;
        try {
            jsonElement = JsonUtil.fromJson(filterText, JsonElement.class);
        } catch (final RuntimeException e) {
            // Not JSON: treat as SQL-like condition text
            return parse(new JsonPrimitive(filterText));
        }
        return parse(jsonElement);
    }

    public static ConditionNode parse(final JsonElement jsonElement) {
        if(jsonElement == null || jsonElement.isJsonNull()) {
            final ConditionNode node = new ConditionNode();
            node.setType(Type.TRUE);
            return node;
        }

        if(jsonElement.isJsonPrimitive()) {
            if(jsonElement.getAsJsonPrimitive().isString()) {
                // SQL-like condition text is translated once (at setup) into the same
                // JSON condition structure, so the evaluation runtime is unchanged.
                return parse(FilterSqlParser.toJsonCondition(jsonElement.getAsString()));
            }
            if(jsonElement.getAsJsonPrimitive().isBoolean()) {
                final ConditionNode node = new ConditionNode();
                node.setType(jsonElement.getAsBoolean() ? Type.TRUE : Type.FALSE);
                return node;
            }
            throw new IllegalArgumentException("Illegal condition json: " + jsonElement);
        }

        if(jsonElement.isJsonObject()) {
            return parse(jsonElement.getAsJsonObject());
        } else if(jsonElement.isJsonArray()) {
            final List<ConditionLeaf> leaves = new ArrayList<>();
            for(JsonElement child : jsonElement.getAsJsonArray()) {
                if(!child.isJsonObject()) {
                    throw new IllegalArgumentException("Simple conditions must be jsonObject. json: " + child);
                }
                final JsonObject childObject = child.getAsJsonObject();
                if(childObject.size() == 1 && (childObject.has("or") || childObject.has("and"))) {
                    throw new IllegalArgumentException("`or`, `and` conditions should be defined at the top level, not in an array. json: " + childObject);
                }
                final ConditionLeaf leaf = createLeaf(childObject);
                leaves.add(leaf);
            }
            ConditionNode node = new ConditionNode();
            node.setType(Type.AND);
            node.setLeaves(leaves);
            return node;
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static ConditionNode parse(final JsonObject jsonObject) {
        if(!jsonObject.has("and") && !jsonObject.has("or")) {
            final List<ConditionLeaf> leaves = new ArrayList<>();
            final ConditionLeaf leaf = createLeaf(jsonObject);
            leaves.add(leaf);
            ConditionNode node = new ConditionNode();
            node.setType(Type.AND);
            node.setLeaves(leaves);
            return node;
        } else if(jsonObject.has("and") && jsonObject.has("or")) {
            throw new IllegalArgumentException("Condition must contain only one of `and` or `or`. Condition json: " + jsonObject.toString());
        }

        final Type type = jsonObject.has("and") ? Type.AND : Type.OR;
        final JsonElement conditions = jsonObject.has("and") ? jsonObject.get("and") : jsonObject.get("or");
        if(!conditions.isJsonArray()) {
            throw new IllegalArgumentException("Condition `and`, `or` parameter must be array. Condition json: " + conditions.toString());
        }
        final List<ConditionNode> nodes = new ArrayList<>();
        final List<ConditionLeaf> leaves = new ArrayList<>();
        for(final JsonElement condition : conditions.getAsJsonArray()) {
            final JsonObject child = condition.getAsJsonObject();
            if(child.has("and") || child.has("or")) {
                final ConditionNode node = parse(child);
                nodes.add(node);
            } else {
                final ConditionLeaf leaf = createLeaf(child);
                leaves.add(leaf);
            }
        }

        final ConditionNode node = new ConditionNode();
        node.setType(type);
        node.setNodes(nodes);
        node.setLeaves(leaves);
        node.variables = node.getRequiredVariables();
        return node;
    }

    private static ConditionLeaf createLeaf(final JsonObject jsonObject) {
        final boolean hasValue = jsonObject.has("value");
        final boolean hasValueKey = jsonObject.has("valueKey");
        if((!jsonObject.has("key") && !jsonObject.has("expression")) || !jsonObject.has("op") || (!hasValue && !hasValueKey)) {
            throw new IllegalArgumentException("Simple conditions must contain `key` (or `expression`), `op` and `value` (or `valueKey`). json: " + jsonObject);
        }
        if(hasValue && hasValueKey) {
            throw new IllegalArgumentException("Simple conditions must not contain both `value` and `valueKey`. json: " + jsonObject);
        }
        final ConditionLeaf leaf = new ConditionLeaf();
        leaf.setOp(Op.of(jsonObject.get("op").getAsString()));
        if(hasValue) {
            leaf.setValue(jsonObject.get("value"));
        } else {
            if(!jsonObject.get("valueKey").isJsonPrimitive() || !jsonObject.get("valueKey").getAsJsonPrimitive().isString()) {
                throw new IllegalArgumentException("Condition `valueKey` must be string. json: " + jsonObject);
            }
            switch (leaf.op) {
                case IN, NOT_IN, MATCH, NOT_MATCH -> throw new IllegalArgumentException(
                        "Condition op `" + jsonObject.get("op").getAsString() + "` does not support `valueKey`. json: " + jsonObject);
                default -> leaf.valueKey = jsonObject.get("valueKey").getAsString();
            }
        }

        if(jsonObject.has("expression")) {
            if(!jsonObject.get("expression").isJsonPrimitive() || !jsonObject.get("expression").getAsJsonPrimitive().isString()) {
                throw new IllegalArgumentException("useExpression must be boolean, json: " + jsonObject);
            }
            final String expression = jsonObject.get("expression").getAsString();
            leaf.key = expression;
            leaf.pattern = null;
            leaf.expression = ExpressionUtil.createDefaultExpression(expression);
            leaf.expressionVariables = leaf.expression.getVariableNames();
            leaf.expressionString = expression;
        } else if(Op.MATCH.equals(leaf.op) || Op.NOT_MATCH.equals(leaf.op)) {
            leaf.key = jsonObject.get("key").getAsString();
            leaf.pattern = Pattern.compile(leaf.value.getAsString());
            leaf.expression = null;
            leaf.expressionString = null;
            leaf.expressionVariables = new HashSet<>();
        } else {
            leaf.key = jsonObject.get("key").getAsString();
            leaf.pattern = null;
            leaf.expression = null;
            leaf.expressionString = null;
            leaf.expressionVariables = new HashSet<>();
        }
        return leaf;
    }

    public static boolean filter(final ConditionNode condition, final Schema schema, final MElement element) {
        return filter(condition, element.asStandardMap(schema, condition.variables));
    }

    public static boolean filter(final ConditionNode condition, final Map<String, ?> standardValues) {
        if(condition == null) {
            return true;
        }
        if(Type.TRUE.equals(condition.getType())) {
            return true;
        }
        if(Type.FALSE.equals(condition.getType())) {
            return false;
        }

        final List<Boolean> bits = new ArrayList<>();

        if(condition.getLeaves() != null && !condition.getLeaves().isEmpty()) {
            for(ConditionLeaf leaf : condition.getLeaves()) {
                final Object value;
                if(leaf.expression != null) {
                    if(!standardValues.keySet().containsAll(leaf.expressionVariables)) {
                        throw new IllegalArgumentException("filter conditions expression variables[" + leaf.expressionVariables + "] are not included all in values keys: " + standardValues.keySet());
                    }
                    final Map<String, Double> variables = standardValues.entrySet()
                            .stream()
                            .filter(e -> leaf.expressionVariables.contains(e.getKey()))
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e -> ExpressionUtil.getAsDouble(e.getValue(), Double.NaN)));
                    try {
                        final double evaluatedValue = leaf.expression.evaluate(variables);
                        if(Double.isNaN(evaluatedValue)) {
                            value = null;
                        } else {
                            value = evaluatedValue;
                        }
                    } catch (IllegalArgumentException e) {
                        return false;
                    }
                } else {
                    value = ElementSchemaUtil.getValue(standardValues, leaf.getKey());
                }
                if(leaf.valueKey != null) {
                    final Object target = ElementSchemaUtil.getValue(standardValues, leaf.valueKey);
                    bits.add(is(value, target, leaf.getOp()));
                } else {
                    bits.add(is(value, leaf));
                }
            }
        }
        if(condition.getNodes() != null && !condition.getNodes().isEmpty()) {
            for(ConditionNode node : condition.getNodes()) {
                bits.add(filter(node, standardValues));
            }
        }

        if(bits.isEmpty()) {
            return false;
        }

        return is(condition.getType(), bits);
    }

    private static boolean is(final Type type, final Collection<Boolean> bits) {
        if(type.equals(Type.AND)) {
            return bits.stream().allMatch(v -> v);
        } else if(type.equals(Type.OR)) {
            return bits.stream().anyMatch(v -> v);
        } else {
            return type.equals(Type.TRUE);
        }
    }

    // Comparison sentinel for values no op should match (NaN/Infinity).
    private static final int INCOMPARABLE = Integer.MIN_VALUE;

    static boolean is(final Object value, final ConditionLeaf leaf) {
        if(value == null) {
            if(leaf.getValue() == null || leaf.getValue().isJsonNull()) {
                return leaf.getOp().equals(Op.EQUAL);
            }
            return false;
        } else if(leaf.getValue() == null || leaf.getValue().isJsonNull()) {
            return leaf.getOp().equals(Op.NOT_EQUAL);
        }

        if(leaf.getOp().equals(Op.IN) || leaf.getOp().equals(Op.NOT_IN)) {
            if(!leaf.getValue().isJsonArray()) {
                throw new IllegalArgumentException("Condition `in` or `not in` value must be array. json: " + leaf.getValue().toString());
            }
            for(final JsonElement e : leaf.getValue().getAsJsonArray()) {
                if(value.toString().equals(e.getAsString())) {
                    return leaf.getOp().equals(Op.IN);
                }
            }
            return leaf.getOp().equals(Op.NOT_IN);
        } else if(leaf.getOp().equals(Op.MATCH) || leaf.getOp().equals(Op.NOT_MATCH)) {
            final boolean found = leaf.pattern.matcher(value.toString()).find();
            return leaf.getOp().equals(Op.MATCH) == found;
        } else {
            final int c = switch (value) {
                case Byte b -> new BigDecimal(b.toString()).compareTo(leaf.getValue().getAsBigDecimal());
                case BigInteger b -> new BigDecimal(b).compareTo(leaf.getValue().getAsBigDecimal());
                case BigDecimal b -> b.compareTo(leaf.getValue().getAsBigDecimal());
                case Boolean b -> b.compareTo(leaf.getValue().getAsBoolean());
                case Short s -> s.compareTo(leaf.getValue().getAsShort());
                case Integer i -> i.compareTo(leaf.getValue().getAsInt());
                case Long l -> l.compareTo(leaf.getValue().getAsLong());
                case Float f when Float.isNaN(f) || Float.isInfinite(f) -> INCOMPARABLE;
                case Float f-> f.compareTo(leaf.getValue().getAsFloat());
                case Double d when Double.isNaN(d) || Double.isInfinite(d) -> INCOMPARABLE;
                case Double d -> d.compareTo(leaf.getValue().getAsDouble());
                case String s -> s.compareTo(leaf.getValue().getAsString());
                case java.time.Instant i -> i.compareTo(DateTimeUtil.toInstant(leaf.getValue().getAsString()));
                case Instant i -> i.compareTo(DateTimeUtil.toJodaInstant(leaf.getValue().getAsString()));
                case LocalDate l -> l.compareTo(DateTimeUtil.toLocalDate(leaf.getValue().getAsString()));
                case LocalTime l -> l.compareTo(DateTimeUtil.toLocalTime(leaf.getValue().getAsString()));
                case Utf8 u -> u.toString().compareTo(leaf.getValue().getAsString());
                default -> {
                    LOG.warn("not matched value: {} to leaf: {}", value, leaf.getValue().getAsString());
                    yield (value).toString().compareTo(leaf.getValue().getAsString());
                }
            };

            // NaN/Infinity never matches any op. (Only the sentinel takes this
            // branch: compareTo results are used by sign only — String and
            // BigDecimal legitimately return magnitudes greater than 1.)
            if(c == INCOMPARABLE) {
                return false;
            }

            return matches(c, leaf.getOp());
        }
    }

    // Field-to-field comparison (leaf with `valueKey`): both sides come from the record.
    static boolean is(final Object value, final Object target, final Op op) {
        if(value == null) {
            if(target == null) {
                return op.equals(Op.EQUAL);
            }
            return false;
        } else if(target == null) {
            return op.equals(Op.NOT_EQUAL);
        }

        final int c = compareValues(value, target);
        if(c == INCOMPARABLE) {
            return false;
        }
        return matches(c, op);
    }

    private static boolean matches(final int c, final Op op) {
        return switch (op) {
            case EQUAL -> c == 0;
            case NOT_EQUAL -> c != 0;
            case GREATER -> c > 0;
            case GREATER_OR_EQUAL -> c >= 0;
            case LESSER -> c < 0;
            case LESSER_OR_EQUAL -> c <= 0;
            case TRUE -> true;
            case FALSE -> false;
            default -> throw new IllegalArgumentException("");
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(final Object value, final Object target) {
        if(isIncomparableNumber(value) || isIncomparableNumber(target)) {
            return INCOMPARABLE;
        }
        if(value instanceof Number v && target instanceof Number t) {
            return toBigDecimal(v).compareTo(toBigDecimal(t));
        }
        if((value instanceof String || value instanceof Utf8) && (target instanceof String || target instanceof Utf8)) {
            return value.toString().compareTo(target.toString());
        }
        if(value instanceof Boolean v && target instanceof Boolean t) {
            return v.compareTo(t);
        }
        final Long valueEpochMicros = toEpochMicros(value);
        if(valueEpochMicros != null) {
            final Long targetEpochMicros = toEpochMicros(target);
            if(targetEpochMicros != null) {
                return valueEpochMicros.compareTo(targetEpochMicros);
            }
        }
        if(value.getClass().equals(target.getClass()) && value instanceof Comparable comparable) {
            return comparable.compareTo(target);
        }
        LOG.warn("not comparable values: {} ({}) and {} ({})", value, value.getClass(), target, target.getClass());
        return value.toString().compareTo(target.toString());
    }

    private static boolean isIncomparableNumber(final Object value) {
        return switch (value) {
            case Double d -> d.isNaN() || d.isInfinite();
            case Float f -> f.isNaN() || f.isInfinite();
            default -> false;
        };
    }

    private static BigDecimal toBigDecimal(final Number number) {
        return switch (number) {
            case BigDecimal b -> b;
            case BigInteger b -> new BigDecimal(b);
            case Double d -> BigDecimal.valueOf(d);
            case Float f -> BigDecimal.valueOf(f.doubleValue());
            case Byte b -> BigDecimal.valueOf(b.longValue());
            case Short s -> BigDecimal.valueOf(s.longValue());
            case Integer i -> BigDecimal.valueOf(i.longValue());
            case Long l -> BigDecimal.valueOf(l);
            default -> new BigDecimal(number.toString());
        };
    }

    private static Long toEpochMicros(final Object value) {
        return switch (value) {
            case java.time.Instant i -> DateTimeUtil.toEpochMicroSecond(i);
            case Instant i -> i.getMillis() * 1000L;
            default -> null;
        };
    }

    // PTransform
    public static Transform of(
            final String jobName,
            final String name,
            final String filterJson,
            final Schema inputSchema,
            final List<Logging> loggings,
            final boolean failFast) {

        return new Transform(jobName, name, filterJson, inputSchema, loggings, failFast);
    }

    public static Transform of(
            final String jobName,
            final String name,
            final JsonElement filterJson,
            final Schema inputSchema,
            final List<Logging> loggings,
            final boolean failFast) {

        final String filterText = Optional.ofNullable(filterJson).map(JsonElement::toString).orElse(null);
        return new Transform(jobName, name, filterText, inputSchema, loggings, failFast);
    }


    public static class Transform extends PTransform<PCollection<MElement>, PCollectionTuple> {

        final String jobName;
        final String name;
        final String filterJson;
        final Schema inputSchema;
        final Map<String, Logging> loggings;
        final boolean failFast;

        public final TupleTag<MElement> outputTag;
        public final TupleTag<MElement> failuresTag;

        Transform(
                final String jobName,
                final String name,
                final String filterJson,
                final Schema inputSchema,
                final List<Logging> loggings,
                final boolean failFast) {

            this.jobName = jobName;
            this.name = name;
            this.filterJson = filterJson;
            this.inputSchema = inputSchema;
            this.loggings = Logging.map(loggings);
            this.failFast = failFast;
            this.outputTag = new TupleTag<>() {};
            this.failuresTag = new TupleTag<>() {};
        }

        @Override
        public PCollectionTuple expand(PCollection<MElement> input) {
            final PCollectionTuple outputs = input
                    .apply("Filter", ParDo
                            .of(new FilterDoFn(jobName, name, filterJson, inputSchema, loggings, failuresTag, failFast))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));

            return PCollectionTuple
                    .of(outputTag, outputs.get(outputTag)
                            .setCoder(input.getCoder()))
                    .and(failuresTag, outputs.get(failuresTag)
                            .apply("WithDefaultWindow", Strategy.createDefaultWindow())
                            .setCoder(ElementCoder.of(MFailure.schema())));
        }

        private static class FilterDoFn extends DoFn<MElement, MElement> {

            private final String jobName;
            private final String name;
            private final String conditionJsons;
            private final Schema inputSchema;
            private final Map<String, Logging> loggings;

            private final TupleTag<MElement> failureTag;
            private final boolean failFast;

            private final Counter errorCounter;

            private transient Filter.ConditionNode conditions;

            private FilterDoFn(
                    final String jobName,
                    final String name,
                    final String conditionJsons,
                    final Schema inputSchema,
                    final Map<String, Logging> loggings,
                    final TupleTag<MElement> failureTag,
                    final boolean failFast) {

                this.jobName = jobName;
                this.name = name;
                this.conditionJsons = conditionJsons;
                this.inputSchema = inputSchema;
                this.loggings = loggings;
                this.failFast = failFast;
                this.failureTag = failureTag;

                this.errorCounter = Metrics.counter(name, "filter_error");
            }

            @Setup
            public void setup() {
                this.conditions = Filter.parse(conditionJsons);
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final MElement input = c.element();
                if(input == null) {
                    return;
                }

                try {
                    if (Filter.filter(conditions, inputSchema, input)) {
                        Logging.log(LOG, loggings, "matched", input);
                        c.output(input);
                    } else {
                        Logging.log(LOG, loggings, "not_matched", input);
                    }
                } catch (final Throwable e) {
                    errorCounter.inc();
                    final MFailure failure = MFailure
                            .of(jobName, name, input.toString(), e, c.timestamp());
                    final String errorMessage = String.format("Failed to filter for input: %s, for condition: %s, error: %s", failure.getInput(), conditionJsons, failure.getError());
                    LOG.error(errorMessage);
                    if(failFast) {
                        throw new RuntimeException("Failed to filter for input: " + input + ", for condition: " + conditionJsons, e);
                    }
                    c.output(failureTag, failure.toElement(c.timestamp()));
                }
            }
        }

    }

}

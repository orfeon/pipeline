package com.mercari.solution.util;

import com.google.common.collect.Sets;
import com.mercari.solution.module.MElement;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.search.DoubleValues;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class ExpressionUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionUtil.class);

    private static final String DEFAULT_SEPARATOR = "_";
    public static final Pattern DELIMITER_PATTERN = Pattern.compile("[()+\\-*/%^<>=!&|#§$~:,?]");
    public static final Pattern FIELD_NO_PATTERN = Pattern.compile("[a-zA-Z_]\\w*_([0-9]\\d*)$");

    private static final String REPLACEMENT_FIELD_FORMAT = "%s___%d";
    private static final String REPLACEMENT_ARRAY = "$1___$2";
    private static final String REGEX_ARRAY = "(\\w+)\\[(\\d+)]";
    private static final Pattern PATTERN_ARRAY = Pattern.compile(REGEX_ARRAY);

    private static final String[] RESERVED_NAMES = {
            "pi","e",
            "abs","acos","acosh","asin","asinh","atan","atan2","atanh",
            "cbrt","ceil","cos","cosh","exp","floor","haversin","haversinMeters",
            "ln","log","log10","log2","logn","pow",
            "sin","sinh","sqrt","tan","tanh","signum",
            "if","switch","switch3","switch4","switch5","switch6","switch7","switch8",
            "max","min",
            "timestamp_to_date",
            "timestamp_diff_millisecond","timestamp_diff_second","timestamp_diff_minute","timestamp_diff_hour","timestamp_diff_day"};
    private static final Set<String> RESERVED_NAMES_SET = new HashSet<>(Arrays.asList(RESERVED_NAMES));

    private static final Map<String, Double> CONSTANTS = Map.of(
            "pi", Math.PI,
            "e", Math.E);

    private static final Map<String, MethodHandle> FUNCTIONS = createFunctions();

    private static Map<String, MethodHandle> createFunctions() {
        final Map<String, MethodHandle> functions = new HashMap<>(JavascriptCompiler.DEFAULT_FUNCTIONS);
        try {
            final MethodHandles.Lookup lookup = MethodHandles.lookup();
            final MethodType unary = MethodType.methodType(double.class, double.class);
            final MethodType binary = MethodType.methodType(double.class, double.class, double.class);

            functions.put("cbrt", lookup.findStatic(Math.class, "cbrt", unary));
            functions.put("signum", lookup.findStatic(Math.class, "signum", unary));
            functions.put("log", lookup.findStatic(Math.class, "log", unary));
            functions.put("log2", lookup.findStatic(ExpressionUtil.class, "log2", unary));

            functions.put("if", lookup.findStatic(ExpressionUtil.class, "ifFunction",
                    MethodType.methodType(double.class, double.class, double.class, double.class)));

            final MethodHandle switchHandle = lookup.findStatic(ExpressionUtil.class, "switchFunction",
                    MethodType.methodType(double.class, double[].class));
            for(int caseNum=3; caseNum<=8; caseNum++) {
                functions.put("switch" + caseNum, switchHandle.asCollector(double[].class, caseNum * 2));
            }

            functions.put("timestamp_to_date", lookup.findStatic(ExpressionUtil.class, "timestampToDate", binary));

            final MethodHandle timestampDiffHandle = lookup.findStatic(ExpressionUtil.class, "timestampDiff",
                    MethodType.methodType(double.class, double.class, double.class, double.class));
            functions.put("timestamp_diff_millisecond", MethodHandles.insertArguments(timestampDiffHandle, 2, 1_000D));
            functions.put("timestamp_diff_second", MethodHandles.insertArguments(timestampDiffHandle, 2, 1_000_000D));
            functions.put("timestamp_diff_minute", MethodHandles.insertArguments(timestampDiffHandle, 2, 60_000_000D));
            functions.put("timestamp_diff_hour", MethodHandles.insertArguments(timestampDiffHandle, 2, 3_600_000_000D));
            functions.put("timestamp_diff_day", MethodHandles.insertArguments(timestampDiffHandle, 2, 86_400_000_000D));
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to register expression functions", e);
        }
        return Map.copyOf(functions);
    }

    public static Set<String> estimateVariables(final String expression) {
        return estimateVariables(expression, true);
    }

    private static Set<String> estimateVariables(String expression, boolean includeArray) {
        if(expression == null) {
            return new HashSet<>();
        }

        if(includeArray) {
            expression = expression.replaceAll(REGEX_ARRAY, "$1");
        } else {
            expression = expression.replaceAll(REGEX_ARRAY, "");
        }

        final String str = expression.replaceAll(" ","");
        final Scanner scanner = new Scanner(str);
        scanner.useDelimiter(DELIMITER_PATTERN);

        final Set<String> variables = new HashSet<>();
        while(scanner.hasNext()) {
            final String variable = scanner.next();
            if(!variable.isEmpty() && !NumberUtils.isCreatable(variable) && !RESERVED_NAMES_SET.contains(variable)) {
                variables.add(variable);
            }
        }

        return variables;
    }

    public static Expression createDefaultExpression(final String expression) {
        return createDefaultExpression(expression, null);
    }

    // variables are derived by the compiler's parser; the argument remains only for call-site compatibility
    public static Expression createDefaultExpression(final String expression, final Collection<String> variables) {
        try {
            return new Expression(JavascriptCompiler.compile(expression, FUNCTIONS));
        } catch (final ParseException e) {
            throw new IllegalArgumentException("Failed to parse expression: " + expression, e);
        }
    }

    /**
     * Compiled math expression.
     * Stateless and thread-safe. Not serializable: hold as a transient field and recreate in setup.
     */
    public static class Expression {

        private final org.apache.lucene.expressions.Expression expression;
        private final Set<String> variableNames;

        private Expression(final org.apache.lucene.expressions.Expression expression) {
            this.expression = expression;
            final Set<String> names = new HashSet<>(Arrays.asList(expression.variables));
            names.removeAll(CONSTANTS.keySet());
            this.variableNames = Collections.unmodifiableSet(names);
        }

        public Set<String> getVariableNames() {
            return variableNames;
        }

        public double evaluate(final Map<String, Double> values) {
            final DoubleValues[] functionValues = new DoubleValues[expression.variables.length];
            for(int i=0; i<expression.variables.length; i++) {
                final String name = expression.variables[i];
                final Double value;
                if(CONSTANTS.containsKey(name)) {
                    value = CONSTANTS.get(name);
                } else if(values != null && values.containsKey(name)) {
                    value = values.get(name);
                } else {
                    throw new IllegalArgumentException("Variable value has not been set for expression: " + expression.sourceText + ", variable: " + name);
                }
                functionValues[i] = constantValues(value == null ? Double.NaN : value);
            }
            try {
                return expression.evaluate(functionValues);
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to evaluate expression: " + expression.sourceText, e);
            }
        }

        @Override
        public String toString() {
            return expression.sourceText;
        }

    }

    private static DoubleValues constantValues(final double value) {
        return new DoubleValues() {
            @Override
            public double doubleValue() {
                return value;
            }
            @Override
            public boolean advanceExact(int doc) {
                return true;
            }
        };
    }

    public static String replaceArrayFieldName(final String variable, final int index) {
        return String.format(REPLACEMENT_FIELD_FORMAT, variable, index);
    }

    public static String replaceArrayExpression(final String expression) {
        if(expression == null) {
            return null;
        }
        String a = expression.replaceAll(REGEX_ARRAY, REPLACEMENT_ARRAY);
        return a.replaceAll("___0", "");
    }

    public static Map<String,Set<Integer>> extractArrayIndexes(final String expression) {
        final Map<String, Set<Integer>> variables = new HashMap<>();
        final Matcher matcher = PATTERN_ARRAY.matcher(expression);
        while(matcher.find()) {
            final String name = matcher.group(1);
            final Integer value = Integer.parseInt(matcher.group(2));
            variables.merge(name, Set.of(value), Sets::union);
        }
        final Set<String> notArrayVariables = estimateVariables(expression, false);
        for(final String variable : notArrayVariables) {
            variables.merge(variable, Set.of(0), Sets::union);
        }
        return variables;
    }

    public static Map<Integer,Set<String>> reverseArrayIndexes(final String expression) {
        final Map<String,Set<Integer>> arrayIndexes = extractArrayIndexes(expression);
        return reverseArrayIndexes(arrayIndexes);
    }

    public static Map<Integer,Set<String>> reverseArrayIndexes(final Map<String,Set<Integer>> arrayIndexes) {
        final Map<Integer, Set<String>> reverseVariables = new HashMap<>();
        if(arrayIndexes == null || arrayIndexes.isEmpty()) {
            return reverseVariables;
        }
        final Set<Integer> indexes = arrayIndexes.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        for(final Integer index : indexes) {
            for(Map.Entry<String,Set<Integer>> entry : arrayIndexes.entrySet()) {
                if(entry.getValue().contains(index)) {
                    reverseVariables.merge(index, Set.of(entry.getKey()), Sets::union);
                }
            }
        }
        return reverseVariables;
    }

    public static Integer maxArrayIndex(final Map<Integer,Set<String>> reverseArrayIndexes) {
        return reverseArrayIndexes.keySet().stream().max(Integer::compareTo).orElse(0);
    }


    public static Map<String, Integer> extractBufferSizes(final Set<String> variables) {
        return extractBufferSizes(variables, 0, DEFAULT_SEPARATOR);
    }

    public static Map<String, Integer> extractBufferSizes(final Set<String> variables, final String separator) {

        return extractBufferSizes(variables, 0, separator);
    }

    public static Map<String, Integer> extractBufferSizes(
            final Set<String> variables,
            final Integer offset,
            final String separator) {

        final Map<String, Integer> bufferSizes = new HashMap<>();
        if(variables == null || variables.isEmpty()) {
            return bufferSizes;
        }

        final Pattern indexPattern;
        if(DEFAULT_SEPARATOR.equals(separator)) {
            indexPattern = FIELD_NO_PATTERN;
        } else {
            final String indexFieldPatternText = String.format("[a-zA-Z_]\\w*%s([0-9]\\d*)$", separator);
            indexPattern = Pattern.compile(indexFieldPatternText);
        }

        for(final String variable : variables) {
            final Matcher matcher = indexPattern.matcher(variable);
            if(matcher.find()) {
                final String var = matcher.group();
                final String[] fieldAndArg = var.split(separator);
                final String field = String.join(separator, Arrays.copyOfRange(fieldAndArg, 0, fieldAndArg.length-1));
                final Integer size = Integer.parseInt(fieldAndArg[fieldAndArg.length-1]);
                if(size + offset > bufferSizes.getOrDefault(field, 0)) {
                    bufferSizes.put(field, size + offset);
                }
            } else if(!bufferSizes.containsKey(variable)) {
                bufferSizes.put(variable, offset);
            }
        }
        return bufferSizes;
    }

    public static Set<String> extractInputs(final Set<String> variables, final String separator) {
        final Set<String> inputs = new HashSet<>();
        if(variables == null || variables.isEmpty()) {
            return inputs;
        }

        final Pattern indexPattern;
        if(DEFAULT_SEPARATOR.equals(separator)) {
            indexPattern = FIELD_NO_PATTERN;
        } else {
            final String indexFieldPatternText = String.format("[a-zA-Z_]\\w*%s([0-9]\\d*)$", separator);
            indexPattern = Pattern.compile(indexFieldPatternText);
        }

        for(final String variable : variables) {
            final Matcher matcher = indexPattern.matcher(variable);
            if(matcher.find()) {
                final String var = matcher.group();
                final String[] fieldAndArg = var.split(separator);
                final String input = String.join(separator, Arrays.copyOfRange(fieldAndArg, 0, fieldAndArg.length-1));
                inputs.add(input);
            } else {
                inputs.add(variable);
            }
        }
        return inputs;
    }

    public static Set<String> extractInputs(final List<Set<String>> variablesList, final String separator) {
        return variablesList.stream().flatMap(v -> extractInputs(v, separator).stream()).collect(Collectors.toSet());
    }

    public static Double eval(final Expression expression, final Set<String> variables, final MElement element) {
        final Map<String, Double> values = new HashMap<>();
        for(final String variable : variables) {
            final Double value = element.getAsDouble(variable);
            values.put(variable, Optional.ofNullable(value).orElse(Double.NaN));
        }
        double expResult = expression.evaluate(values);
        return Double.isNaN(expResult) ? null : expResult;
    }

    public static Double getAsDouble(final Object value) {
        return getAsDouble(value, null);
    }

    public static Double getAsDouble(final Object value, final Double defaultValue) {
        if(value == null) {
            return defaultValue;
        }
        return switch (value) {
            case Double d -> d;
            case Number l -> l.doubleValue();
            case Instant i -> Long.valueOf(i.getMillis() * 1000L).doubleValue();
            case java.time.Instant i -> DateTimeUtil.toEpochMicroSecond(i).doubleValue();
            case LocalDate d -> Long.valueOf(d.toEpochDay()).doubleValue();
            case LocalTime t -> Long.valueOf(t.toNanoOfDay() / 1000L).doubleValue();
            case com.google.cloud.Date d -> DateTimeUtil.toEpochDay(d).doubleValue();
            case com.google.cloud.Timestamp t -> DateTimeUtil.toEpochMicroSecond(t).doubleValue();
            case com.google.protobuf.Timestamp t -> DateTimeUtil.toEpochMicroSecond(t).doubleValue();
            case String s -> Double.valueOf(s);
            case org.apache.avro.util.Utf8 s -> Double.valueOf(s.toString());
            default -> Double.NaN;
        };
    }

    private static double log2(final double value) {
        return Math.log(value) / Math.log(2D);
    }

    private static double ifFunction(final double condition, final double trueValue, final double falseValue) {
        if(condition > 0) {
            return trueValue;
        }
        return falseValue;
    }

    private static double switchFunction(final double... args) {
        for(int i=0; i+1<args.length; i+=2) {
            if(args[i] > 0) {
                return args[i+1];
            }
        }
        return 0d;
    }

    private static double timestampToDate(final double epochMicros, final double timezoneHours) {
        final double timezoneMicros = timezoneHours * 60 * 60 * 1000 * 1000;
        if(Double.isNaN(epochMicros) || Double.isNaN(timezoneMicros)) {
            return Double.NaN;
        }
        final long epochMicrosWithTz = (long) epochMicros + (long) timezoneMicros;
        final Instant instant = Instant.ofEpochMilli(epochMicrosWithTz / 1000L);

        int year = instant.get(DateTimeFieldType.year());
        int month = instant.get(DateTimeFieldType.monthOfYear());
        int day = instant.get(DateTimeFieldType.dayOfMonth());
        final LocalDate date = LocalDate.of(year, month, day);

        return date.toEpochDay();
    }

    private static double timestampDiff(final double micros1, final double micros2, final double unitMicros) {
        final double diffMicros = micros1 - micros2;
        if(Double.isNaN(diffMicros)) {
            return Double.NaN;
        }
        return (long) (diffMicros / unitMicros);
    }

}

package com.mercari.solution.util.pipeline;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Translates a SQL-like boolean condition text
 * (e.g. {@code price > 100 AND category IN ('a', 'b')})
 * into the JSON condition structure consumed by {@link Filter#parse(JsonObject)}.
 *
 * <p>Translation happens once (at DoFn setup); the per-element evaluation runtime is
 * exactly the same {@link Filter.ConditionNode} tree as for JSON-defined filters, so
 * throughput is identical to the legacy syntax.
 *
 * <p>Supported syntax: comparisons ({@code = != <> > >= < <=}) against literals or
 * other fields, {@code AND / OR / NOT}, {@code IN / NOT IN}, {@code LIKE / NOT LIKE},
 * {@code BETWEEN}, {@code IS [NOT] NULL}, bare boolean fields, and numeric arithmetic
 * (translated to an expression leaf). Anything beyond that (functions on strings,
 * subqueries, ...) raises an error suggesting the {@code query} transform.
 */
public class FilterSqlParser {

    private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
            .setLex(Lex.BIG_QUERY)
            .setConformance(SqlConformanceEnum.BIG_QUERY)
            .build();

    public static JsonElement toJsonCondition(final String sql) {
        if(sql == null || sql.isBlank()) {
            return JsonNull.INSTANCE;
        }
        final SqlNode node;
        try {
            node = SqlParser.create(sql, PARSER_CONFIG).parseExpression();
        } catch (final SqlParseException e) {
            throw new IllegalArgumentException("Failed to parse filter condition: " + sql, e);
        }
        return convert(node, false);
    }

    // `negated` pushes an enclosing NOT down to the leaves (De Morgan), because the
    // ConditionNode tree has no NOT node type.
    private static JsonElement convert(final SqlNode node, final boolean negated) {
        return switch (node.getKind()) {
            case AND, OR -> convertGroup((SqlCall) node, negated);
            case NOT -> convert(((SqlCall) node).operand(0), !negated);
            case EQUALS, NOT_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                final SqlCall call = (SqlCall) node;
                yield convertComparison(call.operand(0), call.operand(1), comparisonOp(node.getKind(), negated), node);
            }
            case IN, NOT_IN -> convertIn((SqlCall) node, (node.getKind() == SqlKind.NOT_IN) ^ negated);
            case LIKE -> convertLike((SqlCall) node, negated);
            case BETWEEN -> convertBetween((SqlCall) node, negated);
            case IS_NULL, IS_NOT_NULL -> {
                final SqlNode operand = ((SqlCall) node).operand(0);
                if(!(operand instanceof SqlIdentifier id)) {
                    throw unsupported(node);
                }
                final boolean notNull = (node.getKind() == SqlKind.IS_NOT_NULL) ^ negated;
                yield leaf(identifierName(id), notNull ? "!=" : "=", JsonNull.INSTANCE);
            }
            case IS_TRUE, IS_FALSE -> {
                if(negated) {
                    // IS NOT TRUE / IS NOT FALSE would match NULL in SQL, which the
                    // condition runtime cannot express — do not translate silently.
                    throw new IllegalArgumentException(
                            "NOT (... IS TRUE/FALSE) is not supported. Rewrite with IS NULL and boolean comparison: " + node);
                }
                final SqlNode operand = ((SqlCall) node).operand(0);
                if(!(operand instanceof SqlIdentifier id)) {
                    throw unsupported(node);
                }
                yield leaf(identifierName(id), "=", new JsonPrimitive(node.getKind() == SqlKind.IS_TRUE));
            }
            case IDENTIFIER ->
                // bare boolean field: `flag` / `NOT flag`
                leaf(identifierName((SqlIdentifier) node), "=", new JsonPrimitive(!negated));
            case LITERAL -> {
                final SqlLiteral literal = (SqlLiteral) node;
                if(SqlTypeName.BOOLEAN.equals(literal.getTypeName())) {
                    if(literal.booleanValue() ^ negated) {
                        yield JsonNull.INSTANCE;
                    }
                    throw new IllegalArgumentException("Constant false filter condition is not supported: " + node);
                }
                throw unsupported(node);
            }
            default -> throw unsupported(node);
        };
    }

    private static JsonElement convertGroup(final SqlCall call, final boolean negated) {
        final SqlKind kind = call.getKind();
        final List<SqlNode> terms = new ArrayList<>();
        flatten(call, kind, terms);
        final String type = ((SqlKind.AND.equals(kind)) ^ negated) ? "and" : "or";
        final JsonArray conditions = new JsonArray();
        for(final SqlNode term : terms) {
            final JsonElement condition = convert(term, negated);
            if(condition.isJsonNull()) {
                // constant TRUE term: neutral under AND, absorbing under OR
                if("or".equals(type)) {
                    return JsonNull.INSTANCE;
                }
                continue;
            }
            conditions.add(condition);
        }
        if(conditions.isEmpty()) {
            return JsonNull.INSTANCE;
        }
        final JsonObject group = new JsonObject();
        group.add(type, conditions);
        return group;
    }

    private static void flatten(final SqlNode node, final SqlKind kind, final List<SqlNode> terms) {
        if(node.getKind() == kind) {
            for(final SqlNode operand : ((SqlCall) node).getOperandList()) {
                flatten(operand, kind, terms);
            }
        } else {
            terms.add(node);
        }
    }

    private static JsonElement convertComparison(
            final SqlNode left, final SqlNode right, final String op, final SqlNode original) {

        final boolean leftLiteral = isLiteral(left);
        final boolean rightLiteral = isLiteral(right);
        if(leftLiteral && rightLiteral) {
            throw new IllegalArgumentException("Filter condition comparing two constants is not supported: " + original);
        }
        if(leftLiteral) {
            return convertComparison(right, left, flip(op), original);
        }
        if(left instanceof SqlIdentifier l) {
            if(rightLiteral) {
                return leaf(identifierName(l), op, literalToJson(right));
            }
            if(right instanceof SqlIdentifier r) {
                return leafValueKey(identifierName(l), op, identifierName(r));
            }
        }
        // At least one side is an arithmetic expression. Values are evaluated as
        // doubles by the expression engine (same as the JSON `expression` attribute).
        if(rightLiteral) {
            final JsonElement value = literalToJson(right);
            if(!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
                throw new IllegalArgumentException("Arithmetic filter conditions only support numeric comparison: " + original);
            }
            return expressionLeaf(toJsExpression(left), op, value);
        }
        // Both sides reference fields: fold the whole comparison into the expression
        final String js = "(" + toJsExpression(left) + ") " + jsOperator(op) + " (" + toJsExpression(right) + ")";
        return expressionLeaf(js, "=", new JsonPrimitive(1));
    }

    private static JsonElement convertIn(final SqlCall call, final boolean notIn) {
        final SqlNode left = call.operand(0);
        if(!(left instanceof SqlIdentifier id)) {
            throw new IllegalArgumentException("IN condition left side must be a field: " + call);
        }
        if(!(call.operand(1) instanceof SqlNodeList list)) {
            throw unsupported(call);
        }
        boolean allLiteral = true;
        for(final SqlNode item : list) {
            if(!isLiteral(item)) {
                allLiteral = false;
                break;
            }
        }
        if(allLiteral) {
            final JsonArray values = new JsonArray();
            for(final SqlNode item : list) {
                values.add(literalToJson(item));
            }
            return leaf(identifierName(id), notIn ? "not in" : "in", values);
        }
        // Field references in the list: rewrite to OR of equals (AND of not-equals)
        final JsonArray conditions = new JsonArray();
        for(final SqlNode item : list) {
            if(isLiteral(item)) {
                conditions.add(leaf(identifierName(id), notIn ? "!=" : "=", literalToJson(item)));
            } else if(item instanceof SqlIdentifier itemId) {
                conditions.add(leafValueKey(identifierName(id), notIn ? "!=" : "=", identifierName(itemId)));
            } else {
                throw unsupported(item);
            }
        }
        final JsonObject group = new JsonObject();
        group.add(notIn ? "and" : "or", conditions);
        return group;
    }

    private static JsonElement convertLike(final SqlCall call, final boolean negated) {
        final boolean not = call.getOperator().getName().toUpperCase().contains("NOT") ^ negated;
        if(!(call.operand(0) instanceof SqlIdentifier id)) {
            throw new IllegalArgumentException("LIKE condition left side must be a field: " + call);
        }
        if(call.operandCount() > 2) {
            throw new IllegalArgumentException("LIKE with ESCAPE is not supported: " + call);
        }
        if(!(call.operand(1) instanceof SqlLiteral literal) || !SqlTypeName.CHAR_TYPES.contains(literal.getTypeName())) {
            throw new IllegalArgumentException("LIKE pattern must be a string literal: " + call);
        }
        final String regex = likeToRegex(literal.getValueAs(String.class));
        return leaf(identifierName(id), not ? "not match" : "match", new JsonPrimitive(regex));
    }

    private static JsonElement convertBetween(final SqlCall call, final boolean negated) {
        final SqlBetweenOperator operator = (SqlBetweenOperator) call.getOperator();
        if(SqlBetweenOperator.Flag.SYMMETRIC.equals(operator.flag)) {
            throw new IllegalArgumentException("BETWEEN SYMMETRIC is not supported: " + call);
        }
        final boolean not = operator.isNegated() ^ negated;
        final SqlNode target = call.operand(0);
        final JsonElement lower = convertComparison(target, call.operand(1), not ? "<" : ">=", call);
        final JsonElement upper = convertComparison(target, call.operand(2), not ? ">" : "<=", call);
        final JsonArray conditions = new JsonArray();
        conditions.add(lower);
        conditions.add(upper);
        final JsonObject group = new JsonObject();
        group.add(not ? "or" : "and", conditions);
        return group;
    }

    private static boolean isLiteral(final SqlNode node) {
        if(node instanceof SqlLiteral) {
            return true;
        }
        return node.getKind() == SqlKind.MINUS_PREFIX
                && ((SqlCall) node).operand(0) instanceof SqlLiteral literal
                && SqlTypeName.NUMERIC_TYPES.contains(literal.getTypeName());
    }

    private static JsonElement literalToJson(final SqlNode node) {
        if(node.getKind() == SqlKind.MINUS_PREFIX) {
            final JsonElement inner = literalToJson(((SqlCall) node).operand(0));
            return new JsonPrimitive(inner.getAsBigDecimal().negate());
        }
        final SqlLiteral literal = (SqlLiteral) node;
        final SqlTypeName typeName = literal.getTypeName();
        if(SqlTypeName.NULL.equals(typeName)) {
            return JsonNull.INSTANCE;
        }
        if(SqlTypeName.BOOLEAN.equals(typeName)) {
            return new JsonPrimitive(literal.booleanValue());
        }
        if(SqlTypeName.NUMERIC_TYPES.contains(typeName)) {
            return new JsonPrimitive(literal.getValueAs(java.math.BigDecimal.class));
        }
        if(SqlTypeName.CHAR_TYPES.contains(typeName)) {
            return new JsonPrimitive(literal.getValueAs(String.class));
        }
        // DATE / TIME / TIMESTAMP literals and anything else with a string form
        final String value = literal.toValue();
        if(value == null) {
            throw unsupported(node);
        }
        return new JsonPrimitive(value);
    }

    // The expression engine (lucene-expressions) uses JavaScript-like syntax
    private static String toJsExpression(final SqlNode node) {
        return switch (node.getKind()) {
            case IDENTIFIER -> identifierName((SqlIdentifier) node);
            case LITERAL -> {
                final SqlLiteral literal = (SqlLiteral) node;
                if(!SqlTypeName.NUMERIC_TYPES.contains(literal.getTypeName())) {
                    throw new IllegalArgumentException("Only numeric literals can be used in arithmetic filter conditions: " + node);
                }
                yield literal.toValue();
            }
            case PLUS -> binaryJs(node, "+");
            case MINUS -> binaryJs(node, "-");
            case TIMES -> binaryJs(node, "*");
            case DIVIDE -> binaryJs(node, "/");
            case MOD -> binaryJs(node, "%");
            case MINUS_PREFIX -> "(-" + toJsExpression(((SqlCall) node).operand(0)) + ")";
            case OTHER_FUNCTION -> {
                // Function availability is checked when the expression is compiled at setup
                final SqlCall call = (SqlCall) node;
                final String args = call.getOperandList().stream()
                        .map(FilterSqlParser::toJsExpression)
                        .collect(Collectors.joining(","));
                yield call.getOperator().getName().toLowerCase() + "(" + args + ")";
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported element in arithmetic filter condition: " + node + " (kind: " + node.getKind() + ")");
        };
    }

    private static String binaryJs(final SqlNode node, final String op) {
        final SqlCall call = (SqlCall) node;
        return "(" + toJsExpression(call.operand(0)) + " " + op + " " + toJsExpression(call.operand(1)) + ")";
    }

    static String likeToRegex(final String like) {
        final StringBuilder sb = new StringBuilder("^");
        for(int i = 0; i < like.length(); i++) {
            final char c = like.charAt(i);
            switch (c) {
                case '%' -> sb.append(".*");
                case '_' -> sb.append('.');
                default -> {
                    if("\\.[]{}()<>*+-=!?^$|".indexOf(c) >= 0) {
                        sb.append('\\');
                    }
                    sb.append(c);
                }
            }
        }
        return sb.append('$').toString();
    }

    private static String comparisonOp(final SqlKind kind, final boolean negated) {
        final String op = switch (kind) {
            case EQUALS -> "=";
            case NOT_EQUALS -> "!=";
            case GREATER_THAN -> ">";
            case GREATER_THAN_OR_EQUAL -> ">=";
            case LESS_THAN -> "<";
            case LESS_THAN_OR_EQUAL -> "<=";
            default -> throw new IllegalArgumentException("Not a comparison kind: " + kind);
        };
        return negated ? invert(op) : op;
    }

    private static String invert(final String op) {
        return switch (op) {
            case "=" -> "!=";
            case "!=" -> "=";
            case ">" -> "<=";
            case ">=" -> "<";
            case "<" -> ">=";
            case "<=" -> ">";
            default -> throw new IllegalArgumentException("Not invertible op: " + op);
        };
    }

    // Operator when the two sides of a comparison are swapped
    private static String flip(final String op) {
        return switch (op) {
            case "=", "!=" -> op;
            case ">" -> "<";
            case ">=" -> "<=";
            case "<" -> ">";
            case "<=" -> ">=";
            default -> throw new IllegalArgumentException("Not flippable op: " + op);
        };
    }

    private static String jsOperator(final String op) {
        return "=".equals(op) ? "==" : op;
    }

    private static String identifierName(final SqlIdentifier id) {
        return String.join(".", id.names);
    }

    private static JsonObject leaf(final String key, final String op, final JsonElement value) {
        final JsonObject leaf = new JsonObject();
        leaf.addProperty("key", key);
        leaf.addProperty("op", op);
        leaf.add("value", value);
        return leaf;
    }

    private static JsonObject leafValueKey(final String key, final String op, final String valueKey) {
        final JsonObject leaf = new JsonObject();
        leaf.addProperty("key", key);
        leaf.addProperty("op", op);
        leaf.addProperty("valueKey", valueKey);
        return leaf;
    }

    private static JsonObject expressionLeaf(final String expression, final String op, final JsonElement value) {
        final JsonObject leaf = new JsonObject();
        leaf.addProperty("expression", expression);
        leaf.addProperty("op", op);
        leaf.add("value", value);
        return leaf;
    }

    private static IllegalArgumentException unsupported(final SqlNode node) {
        return new IllegalArgumentException(
                "Unsupported filter condition syntax: " + node + " (kind: " + node.getKind() + "). "
                        + "Use the `query` transform for full SQL support.");
    }

}

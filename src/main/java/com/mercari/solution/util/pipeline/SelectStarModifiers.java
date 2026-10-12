package com.mercari.solution.util.pipeline;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSelect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * BigQuery's {@code SELECT * EXCEPT(col, ...)} and {@code SELECT * REPLACE(expr AS col, ...)}.
 *
 * <p>The (vendored) Calcite parser has neither, and the validator's star expansion cannot be
 * extended. They are therefore rewritten before planning into an explicit column list:
 * <ol>
 *   <li>{@link #extract} cuts the modifiers out of the SQL text, blanking them with spaces so
 *       that every other token keeps its line and column;</li>
 *   <li>a scratch parse + validation of that text tells what each star expands to (the
 *       validator replaces a star by its columns, which carry the star's position);</li>
 *   <li>in a fresh parse, each modified star is replaced by those columns — without the
 *       excepted ones, and with the replaced ones swapped for their expression.</li>
 * </ol>
 * The result is an ordinary statement for the rest of the front-end.
 */
final class SelectStarModifiers {

    private SelectStarModifiers() {
    }

    /** A replacement: the expression is the {@code [start, end)} range of the original SQL. */
    record Replace(String name, int start, int end) {
    }

    /** The modifiers of one star, which is at {@code line}:{@code column} (1-based, as the parser counts). */
    record Modifier(int line, int column, List<String> except, List<Replace> replaces) {

        String describe() {
            return "star at line " + line + ", column " + column;
        }
    }

    /** The original SQL, the SQL with the modifiers blanked out, and the modifiers. */
    record Extracted(String original, String sql, List<Modifier> modifiers) {
    }

    // ---- extraction (lexical) ----

    private enum Kind { WORD, QUOTED, STRING, SYMBOL, OTHER }

    private record Token(Kind kind, String text, int start, int end, int line, int column) {

        boolean isWord(final String word) {
            return kind == Kind.WORD && text.equalsIgnoreCase(word);
        }

        boolean isSymbol(final char symbol) {
            return kind == Kind.SYMBOL && text.charAt(0) == symbol;
        }

        boolean isName() {
            return kind == Kind.WORD || kind == Kind.QUOTED;
        }

        String name() {
            return kind == Kind.QUOTED ? text.substring(1, text.length() - 1) : text;
        }
    }

    static Extracted extract(final String sql) {
        final List<Token> tokens = tokenize(sql);
        final List<Modifier> modifiers = new ArrayList<>();
        final StringBuilder blanked = new StringBuilder(sql);
        for (int t = 1; t < tokens.size(); t++) {
            if (!tokens.get(t).isSymbol('*') || !startsSelectItem(tokens.get(t - 1))) {
                continue;
            }
            final List<String> except = new ArrayList<>();
            final List<Replace> replaces = new ArrayList<>();
            int next = t + 1;
            int end = readExcept(tokens, next, except);
            if (end > 0) {
                next = end;
            }
            end = readReplace(tokens, next, replaces);
            if (end > 0) {
                next = end;
            }
            if (next == t + 1) {
                continue;
            }
            final Token star = tokens.get(t);
            modifiers.add(new Modifier(star.line(), star.column(), List.copyOf(except), List.copyOf(replaces)));
            blank(blanked, tokens.get(t + 1).start(), tokens.get(next - 1).end());
            t = next - 1;
        }
        return new Extracted(sql, blanked.toString(), List.copyOf(modifiers));
    }

    /** A star after one of these is a select item, not a multiplication. */
    private static boolean startsSelectItem(final Token previous) {
        return previous.isSymbol(',') || previous.isSymbol('.')
                || previous.isWord("SELECT") || previous.isWord("DISTINCT") || previous.isWord("ALL");
    }

    /**
     * Reads {@code EXCEPT ( name [, name]* )} at {@code from}: the index after it, or -1 when the
     * tokens are something else (e.g. the set operator: {@code EXCEPT (SELECT ...)}).
     */
    private static int readExcept(final List<Token> tokens, final int from, final List<String> names) {
        if (from + 1 >= tokens.size() || !tokens.get(from).isWord("EXCEPT") || !tokens.get(from + 1).isSymbol('(')) {
            return -1;
        }
        final List<String> read = new ArrayList<>();
        int t = from + 2;
        while (t + 1 < tokens.size() && tokens.get(t).isName()) {
            read.add(tokens.get(t).name());
            final Token separator = tokens.get(t + 1);
            if (separator.isSymbol(')')) {
                names.addAll(read);
                return t + 2;
            }
            if (!separator.isSymbol(',')) {
                return -1;
            }
            t += 2;
        }
        return -1;
    }

    /**
     * Reads {@code REPLACE ( expr AS name [, expr AS name]* )} at {@code from}: the index after
     * it, or -1 when the tokens are something else.
     */
    private static int readReplace(final List<Token> tokens, final int from, final List<Replace> replaces) {
        if (from + 1 >= tokens.size() || !tokens.get(from).isWord("REPLACE") || !tokens.get(from + 1).isSymbol('(')) {
            return -1;
        }
        final List<Replace> read = new ArrayList<>();
        int depth = 0;
        int partStart = from + 2;
        for (int t = from + 2; t < tokens.size(); t++) {
            final Token token = tokens.get(t);
            final boolean close = depth == 0 && token.isSymbol(')');
            if (close || (depth == 0 && token.isSymbol(','))) {
                // [partStart, t) = expr AS name
                if (t - partStart < 3 || !tokens.get(t - 1).isName() || !tokens.get(t - 2).isWord("AS")) {
                    return -1;
                }
                read.add(new Replace(tokens.get(t - 1).name(),
                        tokens.get(partStart).start(), tokens.get(t - 3).end()));
                if (close) {
                    replaces.addAll(read);
                    return t + 1;
                }
                partStart = t + 1;
            } else if (token.isSymbol('(') || token.isSymbol('[')) {
                depth++;
            } else if (token.isSymbol(')') || token.isSymbol(']')) {
                depth--;
            }
        }
        return -1;
    }

    private static void blank(final StringBuilder sql, final int start, final int end) {
        for (int i = start; i < end; i++) {
            final char c = sql.charAt(i);
            if (c != '\n' && c != '\r') {
                sql.setCharAt(i, ' ');
            }
        }
    }

    /**
     * Just enough of a lexer to find the modifiers outside of string literals, quoted
     * identifiers and comments. Lines and columns follow the parser's counting.
     */
    private static List<Token> tokenize(final String sql) {
        final List<Token> tokens = new ArrayList<>();
        final int length = sql.length();
        final int[] lines = new int[length + 1];
        final int[] columns = new int[length + 1];
        int line = 1;
        int column = 1;
        for (int i = 0; i < length; i++) {
            lines[i] = line;
            columns[i] = column;
            final char c = sql.charAt(i);
            if (c == '\n' || (c == '\r' && (i + 1 >= length || sql.charAt(i + 1) != '\n'))) {
                line++;
                column = 1;
            } else {
                column++;
            }
        }

        int i = 0;
        while (i < length) {
            final char c = sql.charAt(i);
            final int start = i;
            final Kind kind;
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            } else if (c == '-' && i + 1 < length && sql.charAt(i + 1) == '-') {
                while (i < length && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
                    i++;
                }
                continue;
            } else if (c == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
                final int close = sql.indexOf("*/", i + 2);
                i = close < 0 ? length : close + 2;
                continue;
            } else if (c == '\'' || c == '"') {
                i++;
                while (i < length && sql.charAt(i) != c) {
                    i += sql.charAt(i) == '\\' ? 2 : 1;
                }
                i = Math.min(i + 1, length);
                kind = Kind.STRING;
            } else if (c == '`') {
                final int close = sql.indexOf('`', i + 1);
                i = close < 0 ? length : close + 1;
                kind = Kind.QUOTED;
            } else if (Character.isLetter(c) || c == '_') {
                while (i < length && isWordPart(sql.charAt(i))) {
                    i++;
                }
                kind = Kind.WORD;
            } else if (Character.isDigit(c)) {
                while (i < length && isWordPart(sql.charAt(i))) {
                    i++;
                }
                kind = Kind.OTHER;
            } else {
                i++;
                kind = Kind.SYMBOL;
            }
            tokens.add(new Token(kind, sql.substring(start, i), start, i, lines[start], columns[start]));
        }
        return tokens;
    }

    private static boolean isWordPart(final char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    // ---- rewrite (syntax tree) ----

    /**
     * Parses the statement with every modified star replaced by its explicit column list.
     *
     * @param scratchValidator a validator used for this call only: validating is what expands
     *                         the stars, and it leaves state behind in the validator
     */
    static SqlNode parse(
            final Extracted extracted,
            final SqlParser.Config parserConfig,
            final Supplier<SqlValidator> scratchValidator) throws SqlParseException {

        final SqlNode parsed = SqlParser.create(extracted.sql(), parserConfig).parseStmt();
        if (extracted.modifiers().isEmpty()) {
            return parsed;
        }

        // 1. what each modified star expands to
        final Map<SqlParserPos, Modifier> modifiers = locate(parsed, extracted.modifiers());
        final SqlNode validated = scratchValidator.get().validate(parsed);
        final Map<SqlParserPos, List<SqlNode>> expansions = expansions(validated, modifiers);

        // 2. a fresh tree with the stars replaced
        final SqlNode rewritten = SqlParser.create(extracted.sql(), parserConfig).parseStmt();
        rewritten.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(final SqlCall call) {
                if (call instanceof SqlSelect select) {
                    final SqlNodeList selectList = select.getSelectList();
                    final List<SqlNode> items = new ArrayList<>();
                    boolean changed = false;
                    for (final SqlNode item : selectList) {
                        final Modifier modifier = modifiers.get(item.getParserPosition());
                        if (modifier == null || !isStar(item)) {
                            items.add(item);
                            continue;
                        }
                        final List<SqlNode> expansion = expansions.get(item.getParserPosition());
                        if (expansion == null) {
                            throw new IllegalArgumentException(
                                    "SELECT * EXCEPT / REPLACE: could not expand the " + modifier.describe());
                        }
                        try {
                            items.addAll(columns((SqlIdentifier) item, modifier, expansion, extracted, parserConfig));
                        } catch (final SqlParseException e) {
                            throw new IllegalArgumentException("SELECT * REPLACE: failed to parse an expression"
                                    + " of the " + modifier.describe() + ": " + e.getMessage(), e);
                        }
                        changed = true;
                    }
                    if (changed) {
                        select.setSelectList(new SqlNodeList(items, selectList.getParserPosition()));
                    }
                }
                return super.visit(call);
            }
        });
        return rewritten;
    }

    private static boolean isStar(final SqlNode node) {
        return node instanceof SqlIdentifier identifier && identifier.isStar();
    }

    /** The parser position of each modified star: the star token ends the (possibly qualified) identifier. */
    private static Map<SqlParserPos, Modifier> locate(final SqlNode parsed, final List<Modifier> modifiers) {
        final Map<SqlParserPos, Modifier> located = new HashMap<>();
        parsed.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(final SqlCall call) {
                if (call instanceof SqlSelect select) {
                    for (final SqlNode item : select.getSelectList()) {
                        if (!isStar(item)) {
                            continue;
                        }
                        final SqlParserPos pos = item.getParserPosition();
                        for (final Modifier modifier : modifiers) {
                            if (modifier.line() == pos.getEndLineNum() && modifier.column() == pos.getEndColumnNum()) {
                                located.put(pos, modifier);
                            }
                        }
                    }
                }
                return super.visit(call);
            }
        });
        for (final Modifier modifier : modifiers) {
            if (!located.containsValue(modifier)) {
                // the lexical scan took something else for a modifier
                throw new IllegalArgumentException("SELECT * EXCEPT / REPLACE: the " + modifier.describe()
                        + " is not a select item");
            }
        }
        return located;
    }

    /**
     * The select items each modified star was expanded to by the validator. An expanded item
     * carries the position of its star; the fields of an expanded struct column may carry none,
     * and then follow the item they belong to.
     */
    private static Map<SqlParserPos, List<SqlNode>> expansions(
            final SqlNode validated, final Map<SqlParserPos, Modifier> modifiers) {
        final Map<SqlParserPos, List<SqlNode>> expansions = new HashMap<>();
        validated.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(final SqlCall call) {
                if (call instanceof SqlSelect select) {
                    List<SqlNode> current = null;
                    for (final SqlNode item : select.getSelectList()) {
                        final SqlParserPos pos = item.getParserPosition();
                        if (modifiers.containsKey(pos)) {
                            current = expansions.computeIfAbsent(pos, p -> new ArrayList<>());
                            current.add(item);
                        } else if (current != null && pos.getLineNum() == 0) {
                            current.add(item);
                        } else {
                            current = null;
                        }
                    }
                }
                return super.visit(call);
            }
        });
        return expansions;
    }

    /** The explicit select items that replace a modified star. */
    private static List<SqlNode> columns(
            final SqlIdentifier star,
            final Modifier modifier,
            final List<SqlNode> expansion,
            final Extracted extracted,
            final SqlParser.Config parserConfig) throws SqlParseException {

        final SqlParserPos pos = star.getParserPosition();
        final int qualifiers = star.names.size() - 1;

        // an expanded item is alias.column, or alias.column.field... for a struct column that the
        // star expands into its fields
        final List<SqlIdentifier> identifiers = new ArrayList<>();
        final List<String> columnNames = new ArrayList<>();
        final List<String> leafNames = new ArrayList<>();
        for (final SqlNode item : expansion) {
            final SqlNode node = item.getKind() == SqlKind.AS ? ((SqlCall) item).operand(0) : item;
            if (!(node instanceof SqlIdentifier identifier)) {
                throw new IllegalArgumentException("SELECT * EXCEPT / REPLACE: unexpected expansion "
                        + item + " of the " + modifier.describe());
            }
            final List<String> names = identifier.names;
            identifiers.add(identifier);
            columnNames.add(names.get(Math.min(Math.max(qualifiers, 1), names.size() - 1)));
            leafNames.add(names.getLast());
        }

        // item index -> what to do with it (null: keep)
        final Map<Integer, Replace> replaced = new LinkedHashMap<>();
        final Map<Integer, Boolean> excepted = new HashMap<>();
        for (final String name : modifier.except()) {
            for (final int index : match(name, "EXCEPT", columnNames, leafNames, modifier)) {
                excepted.put(index, true);
            }
        }
        for (final Replace replace : modifier.replaces()) {
            for (final int index : match(replace.name(), "REPLACE", columnNames, leafNames, modifier)) {
                if (excepted.containsKey(index) || replaced.containsKey(index)) {
                    throw new IllegalArgumentException("SELECT * REPLACE: column " + replace.name()
                            + " is given more than once in the modifiers of the " + modifier.describe());
                }
                replaced.put(index, replace);
            }
        }

        final List<SqlNode> columns = new ArrayList<>();
        final List<Replace> emitted = new ArrayList<>();
        for (int i = 0; i < identifiers.size(); i++) {
            if (excepted.containsKey(i)) {
                continue;
            }
            final Replace replace = replaced.get(i);
            if (replace == null) {
                columns.add(resolvable(identifiers.get(i), qualifiers, pos));
            } else if (!emitted.contains(replace)) {
                // a struct column expanded into several fields is replaced as a whole
                emitted.add(replace);
                columns.add(SqlStdOperatorTable.AS.createCall(pos,
                        parseExpression(extracted.original(), replace, parserConfig),
                        new SqlIdentifier(replace.name(), pos)));
            }
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("SELECT * EXCEPT: no column is left of the " + modifier.describe());
        }
        return columns;
    }

    /** The expanded items a modifier's column name refers to: a column of the star, else a field name. */
    private static List<Integer> match(
            final String name,
            final String keyword,
            final List<String> columnNames,
            final List<String> leafNames,
            final Modifier modifier) {
        for (final List<String> candidates : List.of(columnNames, leafNames)) {
            final List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < candidates.size(); i++) {
                if (candidates.get(i).equalsIgnoreCase(name)) {
                    indexes.add(i);
                }
            }
            if (!indexes.isEmpty()) {
                return indexes;
            }
        }
        throw new IllegalArgumentException("SELECT * " + keyword + ": column " + name
                + " is not a column of the " + modifier.describe() + " (columns: "
                + String.join(", ", leafNames) + ")");
    }

    /**
     * The expanded identifier as it can be written in the statement. A bare star over an
     * unaliased subquery expands to columns qualified by a generated alias, which the statement
     * does not have.
     */
    private static SqlIdentifier resolvable(final SqlIdentifier identifier, final int qualifiers, final SqlParserPos pos) {
        final List<String> names = identifier.names;
        if (qualifiers == 0 && names.size() > 1 && names.getFirst().startsWith("EXPR$")) {
            return new SqlIdentifier(names.subList(1, names.size()), pos);
        }
        return new SqlIdentifier(names, pos);
    }

    /** Parses a replacement expression where it stands in the SQL, so its positions are the original ones. */
    private static SqlNode parseExpression(
            final String sql, final Replace replace, final SqlParser.Config parserConfig) throws SqlParseException {
        final StringBuilder text = new StringBuilder(sql.substring(0, replace.end()));
        blank(text, 0, replace.start());
        return SqlParser.create(text.toString(), parserConfig).parseExpression();
    }

}

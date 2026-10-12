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
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.util.SqlShuttle;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * BigQuery's {@code SELECT * EXCEPT(col, ...)} and {@code SELECT * REPLACE(expr AS col, ...)}.
 *
 * <p>The (vendored) Calcite parser has neither, and the validator's star expansion cannot be
 * extended. They are therefore rewritten before planning into an explicit column list:
 * <ol>
 *   <li>{@link #extract} cuts the modifiers out of the SQL text, blanking them with spaces so
 *       that every other token keeps its line and column;</li>
 *   <li>a scratch parse + validation of that text tells what the stars of a select list expand
 *       to (the validator replaces a select list by its expanded items);</li>
 *   <li>in a fresh parse, each modified star is replaced by those columns — without the
 *       excepted ones, and with the replaced ones swapped for their expression.</li>
 * </ol>
 * Steps 2 and 3 are repeated, one select list at a time and in the order the validator goes
 * through them: a star over a subquery that has modifiers of its own must be expanded from
 * the rewritten subquery. The result is an ordinary statement for the rest of the front-end.
 */
final class SelectStarModifiers {

    private SelectStarModifiers() {
    }

    /** A replacement: the expression is the {@code [start, end)} range of the original SQL. */
    record Replace(String name, int start, int end) {
    }

    /**
     * The modifiers of one star, which is at {@code line}:{@code column} (1-based, as the parser
     * counts). They are the {@code [start, end)} range of the original SQL.
     */
    record Modifier(int line, int column, int start, int end, List<String> except, List<Replace> replaces) {

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
            return kind == Kind.QUOTED ? text.substring(1, text.length() - 1).replace("\\`", "`") : text;
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
            final Modifier modifier = new Modifier(star.line(), star.column(),
                    tokens.get(t + 1).start(), tokens.get(next - 1).end(),
                    List.copyOf(except), List.copyOf(replaces));
            modifiers.add(modifier);
            blank(blanked, modifier.start(), modifier.end());
            // The scan goes on inside the modifiers: a replacement expression can have a
            // subquery with a modified star of its own.
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
            } else if ((c == '-' || c == '/') && i + 1 < length && sql.charAt(i + 1) == c) {
                // a line comment: -- or //
                while (i < length && sql.charAt(i) != '\n' && sql.charAt(i) != '\r') {
                    i++;
                }
                continue;
            } else if (c == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
                final int close = sql.indexOf("*/", i + 2);
                i = close < 0 ? length : close + 2;
                continue;
            } else if (c == '\'' || c == '"' || c == '`') {
                // a string literal or a quoted identifier: a backslash escapes the quote
                i++;
                while (i < length && sql.charAt(i) != c) {
                    i += sql.charAt(i) == '\\' ? 2 : 1;
                }
                i = Math.min(i + 1, length);
                kind = c == '`' ? Kind.QUOTED : Kind.STRING;
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

    /** A select item that takes the place of a modified star, made anew for each tree. */
    @FunctionalInterface
    private interface Column {
        SqlNode item(SqlParserPos pos) throws SqlParseException;
    }

    /** The select items that take the place of a modified star. */
    private record Rewrite(Modifier modifier, List<Column> columns) {

        List<SqlNode> items(final SqlParserPos pos) {
            final List<SqlNode> items = new ArrayList<>();
            try {
                for (final Column column : columns) {
                    items.add(column.item(pos));
                }
            } catch (final SqlParseException e) {
                throw new IllegalArgumentException("SELECT * REPLACE: failed to parse an expression"
                        + " of the " + modifier.describe() + ": " + e.getMessage(), e);
            }
            return items;
        }
    }

    /** A select list to expand: the position of each of its modified stars, and the modifiers. */
    private record Target(SqlSelect select, Map<SqlParserPos, Modifier> modifiers) {
    }

    /**
     * Parses the statement with every modified star replaced by its explicit column list.
     *
     * @param scratchValidator validators used for this call only: validating is what expands
     *                         the stars, and it leaves state behind in the validator
     */
    static SqlNode parse(
            final Extracted extracted,
            final SqlParser.Config parserConfig,
            final Supplier<SqlValidator> scratchValidator) throws SqlParseException {

        final List<Modifier> pending = new ArrayList<>(extracted.modifiers());
        final Map<SqlParserPos, Rewrite> rewrites = new HashMap<>();
        while (true) {
            // a fresh tree each time: a validation changes the one it is given
            final SqlNode statement = SqlParser.create(extracted.sql(), parserConfig).parseStmt();
            if (extracted.modifiers().isEmpty()) {
                return statement;
            }
            rewrite(statement, rewrites);
            final Target target = next(statement, pending);
            if (target == null) {
                if (!pending.isEmpty()) {
                    // the lexical scan took something else for a modifier
                    throw new IllegalArgumentException("SELECT * EXCEPT / REPLACE: the "
                            + pending.getFirst().describe() + " is not a select item");
                }
                return statement;
            }
            rewrites.putAll(expand(target, statement, scratchValidator.get(), extracted, parserConfig));
            pending.removeAll(target.modifiers().values());
        }
    }

    private static boolean isStar(final SqlNode node) {
        return node instanceof SqlIdentifier identifier && identifier.isStar();
    }

    /** Replaces the stars whose columns are known. */
    private static void rewrite(final SqlNode statement, final Map<SqlParserPos, Rewrite> rewrites) {
        if (rewrites.isEmpty()) {
            return;
        }
        statement.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(final SqlCall call) {
                if (call instanceof SqlSelect select) {
                    final SqlNodeList selectList = select.getSelectList();
                    final List<SqlNode> items = new ArrayList<>();
                    boolean changed = false;
                    for (final SqlNode item : selectList) {
                        final Rewrite rewrite = isStar(item) ? rewrites.get(item.getParserPosition()) : null;
                        if (rewrite == null) {
                            items.add(item);
                            continue;
                        }
                        items.addAll(rewrite.items(item.getParserPosition()));
                        changed = true;
                    }
                    if (changed) {
                        select.setSelectList(new SqlNodeList(items, selectList.getParserPosition()));
                    }
                }
                // goes on into the new items: a replacement expression can have a modified star
                return super.visit(call);
            }
        });
    }

    /**
     * The first select list with stars whose modifiers are pending, in the order the validator
     * expands the select lists: by then, the select lists such a star depends on (a subquery
     * of its FROM clause, a common table expression) have been rewritten, and no other pending
     * star has been validated without its modifiers. The star token ends the (possibly
     * qualified) star identifier.
     */
    private static Target next(final SqlNode statement, final List<Modifier> pending) {
        final Target[] found = new Target[1];
        statement.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(final SqlCall call) {
                if (found[0] != null) {
                    return null;
                }
                if (!(call instanceof SqlSelect select)) {
                    return super.visit(call);
                }
                descend(select.getFrom(), select.getWhere(), select.getGroup(), select.getWindowList(),
                        select.getQualify(), select.getSelectList());
                if (found[0] == null) {
                    final Map<SqlParserPos, Modifier> modifiers = new LinkedHashMap<>();
                    for (final SqlNode item : select.getSelectList()) {
                        if (!isStar(item)) {
                            continue;
                        }
                        final SqlParserPos pos = item.getParserPosition();
                        for (final Modifier modifier : pending) {
                            if (modifier.line() == pos.getEndLineNum() && modifier.column() == pos.getEndColumnNum()) {
                                modifiers.put(pos, modifier);
                            }
                        }
                    }
                    if (!modifiers.isEmpty()) {
                        found[0] = new Target(select, modifiers);
                    }
                }
                descend(select.getHaving(), select.getOrderList(), select.getOffset(), select.getFetch());
                return null;
            }

            private void descend(final SqlNode... nodes) {
                for (final SqlNode node : nodes) {
                    if (node != null && found[0] == null) {
                        node.accept(this);
                    }
                }
            }
        });
        return found[0];
    }

    /**
     * What takes the place of each modified star of a select list. Validating the statement
     * replaces the select list by its expanded items: one for an item, its columns for a star.
     */
    private static Map<SqlParserPos, Rewrite> expand(
            final Target target,
            final SqlNode statement,
            final SqlValidator validator,
            final Extracted extracted,
            final SqlParser.Config parserConfig) {

        final SqlSelect select = target.select();
        final SqlNodeList parsedList = select.getSelectList();
        final List<SqlNode> parsed = new ArrayList<>(parsedList);
        RuntimeException failure = null;
        try {
            validator.validate(statement);
        } catch (final RuntimeException e) {
            failure = e;
        }
        final SqlNodeList expandedList = select.getSelectList();
        final String star = target.modifiers().values().iterator().next().describe();
        if (expandedList == parsedList) {
            // the validation did not get as far as this select list
            if (failure != null) {
                throw failure;
            }
            throw new IllegalArgumentException("SELECT * EXCEPT / REPLACE: could not expand the " + star);
        }
        // A failure after the select list was expanded is left to the validation of the rewritten
        // statement: it may be the star without its modifiers that does not validate (a column
        // that is not grouped, the column count of a UNION, of an IN subquery ...).
        final List<SqlNode> expanded = new ArrayList<>(expandedList);
        final int[] counts = counts(parsed, expanded);
        if (counts == null) {
            throw new IllegalArgumentException("SELECT * EXCEPT / REPLACE: could not expand the " + star);
        }

        final Map<SqlParserPos, Rewrite> rewrites = new HashMap<>();
        int offset = 0;
        for (int i = 0; i < parsed.size(); i++) {
            final SqlNode item = parsed.get(i);
            final Modifier modifier = isStar(item) ? target.modifiers().get(item.getParserPosition()) : null;
            if (modifier != null) {
                rewrites.put(item.getParserPosition(), new Rewrite(modifier, columns((SqlIdentifier) item,
                        modifier, expanded.subList(offset, offset + counts[i]), extracted, parserConfig)));
            }
            offset += counts[i];
        }
        return rewrites;
    }

    /**
     * How many expanded items each item of a select list stands for, or null when they do not
     * add up. An item that is not a star is one item. The columns of a qualified star carry
     * its position. Those of a bare star do not all (the fields of a struct column and the
     * keys of a NATURAL / USING join have none): they are what is left.
     */
    private static int[] counts(final List<SqlNode> parsed, final List<SqlNode> expanded) {
        final int[] counts = new int[parsed.size()];
        int bare = 0;
        int rest = expanded.size();
        for (int i = 0; i < parsed.size(); i++) {
            final SqlNode item = parsed.get(i);
            if (!isStar(item)) {
                counts[i] = 1;
            } else if (((SqlIdentifier) item).names.size() > 1) {
                final SqlParserPos pos = item.getParserPosition();
                counts[i] = (int) expanded.stream().filter(e -> pos.equals(e.getParserPosition())).count();
                if (counts[i] == 0) {
                    return null;
                }
            } else {
                counts[i] = -1;
                bare++;
            }
            rest -= Math.max(counts[i], 0);
        }
        if (bare == 0) {
            return rest == 0 ? counts : null;
        }
        if (rest < bare || rest % bare != 0) {
            return null;
        }
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] < 0) {
                counts[i] = rest / bare;
            }
        }
        return counts;
    }

    /** The explicit select items that replace a modified star. */
    private static List<Column> columns(
            final SqlIdentifier star,
            final Modifier modifier,
            final List<SqlNode> expansion,
            final Extracted extracted,
            final SqlParser.Config parserConfig) {

        final int qualifiers = star.names.size() - 1;

        // An expanded item is alias.column, or alias.column.field... for a struct column that the
        // star expands into its fields. The key of a NATURAL / USING join is an expression AS key.
        final List<Column> kept = new ArrayList<>();
        final List<String> columnNames = new ArrayList<>();
        final List<String> leafNames = new ArrayList<>();
        for (final SqlNode item : expansion) {
            SqlNode node = item.getKind() == SqlKind.AS ? ((SqlCall) item).operand(0) : item;
            while (node.getKind() == SqlKind.CAST) {
                // a type coercion of the validator (the columns of a UNION): made again on the
                // rewritten statement
                node = ((SqlCall) node).operand(0);
            }
            if (node instanceof SqlIdentifier identifier) {
                final List<String> names = identifier.names;
                columnNames.add(names.get(Math.min(Math.max(qualifiers, 1), names.size() - 1)));
                leafNames.add(names.getLast());
                final List<String> resolvable = resolvable(names, qualifiers);
                kept.add(pos -> new SqlIdentifier(resolvable, pos));
            } else if (item.getKind() == SqlKind.AS && ((SqlCall) item).operand(1) instanceof SqlIdentifier alias) {
                columnNames.add(alias.getSimple());
                leafNames.add(alias.getSimple());
                kept.add(pos -> copy(item));
            } else {
                throw new IllegalArgumentException("SELECT * EXCEPT / REPLACE: unexpected expansion "
                        + item + " of the " + modifier.describe());
            }
        }

        // item index -> what to do with it (neither: keep)
        final Set<Integer> excepted = new HashSet<>();
        final Map<Integer, Replace> replaced = new HashMap<>();
        for (final String name : modifier.except()) {
            for (final int index : match(name, "EXCEPT", columnNames, leafNames, modifier)) {
                if (!excepted.add(index)) {
                    throw new IllegalArgumentException("SELECT * EXCEPT: column " + name
                            + " is given more than once in the modifiers of the " + modifier.describe());
                }
            }
        }
        for (final Replace replace : modifier.replaces()) {
            for (final int index : match(replace.name(), "REPLACE", columnNames, leafNames, modifier)) {
                if (excepted.contains(index) || replaced.containsKey(index)) {
                    throw new IllegalArgumentException("SELECT * REPLACE: column " + replace.name()
                            + " is given more than once in the modifiers of the " + modifier.describe());
                }
                replaced.put(index, replace);
            }
        }

        final List<Column> columns = new ArrayList<>();
        final Set<Replace> emitted = new HashSet<>();
        for (int i = 0; i < kept.size(); i++) {
            if (excepted.contains(i)) {
                continue;
            }
            final Replace replace = replaced.get(i);
            if (replace == null) {
                columns.add(kept.get(i));
            } else if (emitted.add(replace)) {
                // a struct column expanded into several fields is replaced as a whole
                columns.add(pos -> SqlStdOperatorTable.AS.createCall(pos,
                        parseExpression(extracted, replace, parserConfig),
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
                + String.join(", ", new LinkedHashSet<>(columnNames)) + ")");
    }

    /**
     * The names of an expanded identifier as they can be written in the statement. A bare star
     * over an unaliased subquery expands to columns qualified by a generated alias, which the
     * statement does not have.
     */
    private static List<String> resolvable(final List<String> names, final int qualifiers) {
        if (qualifiers == 0 && names.size() > 1 && names.getFirst().startsWith("EXPR$")) {
            return List.copyOf(names.subList(1, names.size()));
        }
        return List.copyOf(names);
    }

    /** A copy that shares no node with the validated tree it is taken from. */
    private static SqlNode copy(final SqlNode node) {
        return node.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(final SqlIdentifier id) {
                return new SqlIdentifier(id.names, id.getParserPosition());
            }

            @Override
            public SqlNode visit(final SqlCall call) {
                final CallCopyingArgHandler handler = new CallCopyingArgHandler(call, true);
                call.getOperator().acceptCall(this, call, false, handler);
                return handler.result();
            }
        });
    }

    /**
     * Parses a replacement expression where it stands in the SQL, so its positions are the
     * original ones. The modifiers inside it are blanked out, as they are in the statement.
     */
    private static SqlNode parseExpression(
            final Extracted extracted, final Replace replace, final SqlParser.Config parserConfig) throws SqlParseException {
        final StringBuilder text = new StringBuilder(extracted.original().substring(0, replace.end()));
        blank(text, 0, replace.start());
        for (final Modifier nested : extracted.modifiers()) {
            if (nested.start() >= replace.start() && nested.end() <= replace.end()) {
                blank(text, nested.start(), nested.end());
            }
        }
        return SqlParser.create(text.toString(), parserConfig).parseExpression();
    }

}

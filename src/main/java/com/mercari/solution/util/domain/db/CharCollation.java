package com.mercari.solution.util.domain.db;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CharCollation implements Serializable {

    private final Map<String, List<Character>> asciiChars;
    private final Map<String, String> collations;

    private CharCollation(
            final Map<String, List<Character>> asciiChars,
            final Map<String, String> collations) {
        this.asciiChars = asciiChars;
        this.collations = collations;
    }

    public static CharCollation of(
            final Connection connection,
            final String table,
            final List<String> fields,
            final Map<String, String> defaultCollations) {

        final JdbcUtil.DB db;
        try {
            db = JdbcUtil.extractDbFromDriver(connection.getMetaData().getDriverName());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get db name cause: " + e.getMessage(), e);
        }
        final Map<String, List<Character>> asciiCharsMap = new HashMap<>();
        final Map<String, String> collations = new HashMap<>();
        for(final String field : fields) {
            String collation = getCollationName(connection, table, field, db);
            if(collation == null) {
                continue;
            }

            if(defaultCollations.containsKey("*")) {
                collation = defaultCollations.get("*");
            } else if(defaultCollations.containsKey(field)) {
                collation = defaultCollations.get(field);
            }

            final List<Character> characters = getAsciiCharsByCollation(connection, collation, db);
            if(characters == null || characters.isEmpty()) {
                continue;
            }
            asciiCharsMap.put(field, characters);
            collations.put(field, collation);
        }
        return new CharCollation(asciiCharsMap, collations);
    }

    public char firstCollationAscii(final String field) {
        if(!asciiChars.containsKey(field)) {
            throw new IllegalArgumentException("string field: " + field + " does not have collation.");
        }
        return asciiChars.get(field).getFirst();
    }

    public char lastCollationAscii(final String field) {
        if(!asciiChars.containsKey(field)) {
            throw new IllegalArgumentException("string field: " + field + " does not have collation.");
        }
        return asciiChars.get(field).getLast();
    }

    public int index(final String field, final char ascii) {
        if(!asciiChars.containsKey(field)) {
            throw new IllegalArgumentException("string field: " + field + " does not have collation.");
        }
        return asciiChars.get(field).indexOf(ascii);
    }

    public int distance(final String field, final char asciiMin, final char asciiiMax) {
        return index(field, asciiiMax) - index(field, asciiMin);
    }

    public char convertCollationChar(final String field, final int index) {
        if(!asciiChars.containsKey(field)) {
            throw new IllegalArgumentException("string field: " + field + " does not have collation.");
        }
        int value = asciiChars.get(field).get(index);
        return (char)value;
    }

    public int compare(final String field, final String str1, final String str2) {
        final char[] chars1 = str1.toCharArray();
        final char[] chars2 = str2.toCharArray();
        int size = Math.min(chars1.length, chars2.length);
        for(int i=0; i<size; i++) {
            int i1 = index(field, chars1[i]);
            int i2 = index(field, chars2[i]);
            int r = i1 - i2;
            if(r != 0) {
                return r;
            }
        }
        return chars1.length - chars2.length;
    }

    public boolean getIsCaseSensitive() {
        return true;
    }

    private static String getCollationName(
            final Connection connection,
            final String table,
            final String field,
            final JdbcUtil.DB db) {
        return switch (db) {
            case MYSQL -> getCollationNameMySQL(connection, table, field);
            case POSTGRESQL -> getCollationNamePostgreSQL(connection, table, field);
            default -> throw new RuntimeException("Not supported db: " + db);
        };
    }

    private static String getCollationName(Connection connection, String query) {
        try (final Statement statement = connection.createStatement()) {
            try(final ResultSet resultSet = statement.executeQuery(query)) {
                if(resultSet.next()) {
                    return resultSet.getString(1);
                } else {
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getCollationNamePostgreSQL(
            final Connection connection,
            final String table,
            final String column) {

        final String queryForCollation = String.format("""
                SELECT
                  COALESCE(c.collation_name, current_setting('lc_collate')) AS active_collation
                FROM
                  information_schema.columns c
                WHERE
                  c.table_name = '%s'
                  AND c.column_name = '%s';
                """, table, column);
        return getCollationName(connection, queryForCollation);
    }

    private static String getCollationNameMySQL(
            final Connection connection,
            final String table,
            final String column) {

        final String queryForCollation = String.format("""
                SELECT
                  COLLATION_NAME
                FROM
                  information_schema.COLUMNS
                WHERE
                  TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = '%s'
                  AND COLUMN_NAME = '%s';
                """, table, column);
        return getCollationName(connection, queryForCollation);
    }

    private static List<Character> getAsciiCharsByCollationPostgreSQL(
            final Connection connection,
            final String collation) {

        if(collation == null || collation.isEmpty()) {
            return List.of();
        }

        String queryForCharsSortByCollation = String.format("""
                SELECT
                  chr(n) as char_val
                FROM
                  generate_series(32, 126) as t(n)
                ORDER BY
                  chr(n) COLLATE "%s"
                """, collation);

        if(collation.contains("en_US")) {
            final String suffix = """
                    , CHAR(n) COLLATE "C"
                    """;
            queryForCharsSortByCollation = queryForCharsSortByCollation + suffix;
        }

        final List<Character> charList = new ArrayList<>();
        try (final Statement statement = connection.createStatement()) {
            try(final ResultSet resultSet = statement.executeQuery(queryForCharsSortByCollation)) {
                while(resultSet.next()) {
                    final String charValue = resultSet.getString(1);
                    charList.add(charValue.charAt(0));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return charList;
    }

    private static List<Character> getAsciiCharsByCollationMySQL(
            final Connection connection,
            final String collation) {

        if(collation == null || collation.isEmpty()) {
            return List.of();
        }

        String queryForCharsSortByCollation = String.format("""
                WITH RECURSIVE seq AS (
                    SELECT 32 AS n
                    UNION ALL
                    SELECT n + 1 FROM seq WHERE n < 126
                )
                SELECT
                  CHAR(n USING utf8mb4) AS char_val
                FROM
                  seq
                ORDER BY
                  CHAR(n USING utf8mb4) COLLATE %s
                """, collation);

        if(collation.toLowerCase().endsWith("_ci")) {
            queryForCharsSortByCollation = queryForCharsSortByCollation + ", CHAR(n USING utf8mb4) COLLATE utf8mb4_bin";
        }
        final List<Character> charList = new ArrayList<>();
        try (final Statement statement = connection.createStatement()) {
            try(final ResultSet resultSet = statement.executeQuery(queryForCharsSortByCollation)) {
                while(resultSet.next()) {
                    final String charValue = resultSet.getString(1);
                    charList.add(charValue.charAt(0));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return charList;
    }

    private static List<Character> getAsciiCharsByCollation(
            final Connection connection,
            final String collation,
            final JdbcUtil.DB db) {
        return switch (db) {
            case MYSQL -> getAsciiCharsByCollationMySQL(connection, collation);
            case POSTGRESQL -> getAsciiCharsByCollationPostgreSQL(connection, collation);
            default -> throw new RuntimeException("Not supported db: " + db);
        };
    }

    private static List<Character> getAsciiCharsByDefaults(final Connection connection) {
        final List<Character> chars = createDefaultAsciiCharList();
        final String ss = chars.stream()
                .map(s -> s.toString().equals("'") ? "''" : s)
                .map(s -> String.format("'%s'", s))
                .collect(Collectors.joining(","));
        final String queryForCharsSortByCollation = String.format("""
                SELECT
                  char_val
                FROM
                  UNNEST(ARRAY[%s]) AS char_val
                ORDER BY
                  char_val
                """, ss);
        final List<Character> charList = new ArrayList<>();
        try (final Statement statement = connection.createStatement()) {
            try(final ResultSet resultSet = statement.executeQuery(queryForCharsSortByCollation)) {
                while(resultSet.next()) {
                    final String charValue = resultSet.getString(1);
                    charList.add(charValue.charAt(0));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return charList;
    }

    private static List<String> createAsciiChars() {
        final List<String> chars = new ArrayList<>();
        for(char i=32; i<127; i++) {
            chars.add(Character.toString(i));
        }
        return chars;
    }

    private static List<Character> createDefaultAsciiCharList() {
        // key: index, value: asciiChar
        final List<Character> list = new ArrayList<>();
        // ---(Symbols) ---
        list.add((char)32);  // ' ' (Space)
        list.add((char)33);  // '!'
        list.add((char)34);  // '"'
        list.add((char)35);  // '#'
        list.add((char)36);  // '$'
        list.add((char)37);  // '%'
        list.add((char)38);  // '&'
        list.add((char)39);  // '''
        list.add((char)40);  // '('
        list.add((char)41);  // ')'
        list.add((char)42);  // '*'
        list.add((char)43);  // '+'
        list.add((char)44);  // ','
        list.add((char)45);  // '-'
        list.add((char)46);  // '.'
        list.add((char)47);  // '/'
        list.add((char)58);  // ':'
        list.add((char)59);  // ';'
        list.add((char)60);  // '<'
        list.add((char)61);  // '='
        list.add((char)62);  // '>'
        list.add((char)63);  // '?'
        list.add((char)64);  // '@'
        list.add((char)91);  // '['
        list.add((char)92);  // '\'
        list.add((char)93);  // ']'
        list.add((char)94);  // '^'
        list.add((char)95);  // '_'
        list.add((char)96);  // '`'
        list.add((char)123); // '{'
        list.add((char)124); // '|'
        list.add((char)125); // '}'
        list.add((char)126); // '~'

        // (Numbers) ---
        list.add((char)48); // '0'
        list.add((char)49); // '1'
        list.add((char)50); // '2'
        list.add((char)51); // '3'
        list.add((char)52); // '4'
        list.add((char)53); // '5'
        list.add((char)54); // '6'
        list.add((char)55); // '7'
        list.add((char)56); // '8'
        list.add((char)57); // '9'

        // --- (Alphabets) ---
        list.add((char)97);  // 'a'
        list.add((char)65);  // 'A'
        list.add((char)98);  // 'b'
        list.add((char)66);  // 'B'
        list.add((char)99);  // 'c'
        list.add((char)67);  // 'C'
        list.add((char)100); // 'd'
        list.add((char)68);  // 'D'
        list.add((char)101); // 'e'
        list.add((char)69);  // 'E'
        list.add((char)102); // 'f'
        list.add((char)70);  // 'F'
        list.add((char)103); // 'g'
        list.add((char)71);  // 'G'
        list.add((char)104); // 'h'
        list.add((char)72);  // 'H'
        list.add((char)105); // 'i'
        list.add((char)73);  // 'I'
        list.add((char)106); // 'j'
        list.add((char)74);  // 'J'
        list.add((char)107); // 'k'
        list.add((char)75);  // 'K'
        list.add((char)108); // 'l'
        list.add((char)76);  // 'L'
        list.add((char)109); // 'm'
        list.add((char)77);  // 'M'
        list.add((char)110); // 'n'
        list.add((char)78);  // 'N'
        list.add((char)111); // 'o'
        list.add((char)79);  // 'O'
        list.add((char)112); // 'p'
        list.add((char)80);  // 'P'
        list.add((char)113); // 'q'
        list.add((char)81);  // 'Q'
        list.add((char)114); // 'r'
        list.add((char)82);  // 'R'
        list.add((char)115); // 's'
        list.add((char)83);  // 'S'
        list.add((char)116); // 't'
        list.add((char)84);  // 'T'
        list.add((char)117); // 'u'
        list.add((char)85);  // 'U'
        list.add((char)118); // 'v'
        list.add((char)86);  // 'V'
        list.add((char)119); // 'w'
        list.add((char)87);  // 'W'
        list.add((char)120); // 'x'
        list.add((char)88);  // 'X'
        list.add((char)121); // 'y'
        list.add((char)89);  // 'Y'
        list.add((char)122); // 'z'
        list.add((char)90);  // 'Z'

        return list;
    }

    @Override
    public String toString() {
        final JsonObject collationsObject = new JsonObject();
        for(final Map.Entry<String, String> entry : collations.entrySet()) {
            collationsObject.addProperty(entry.getKey(), entry.getValue());
        }

        final JsonObject asciiCharsObject = new JsonObject();
        for(final Map.Entry<String, List<Character>> entry : asciiChars.entrySet()) {
            final JsonArray asciiCharsArray = new JsonArray();
            for(final Character c : entry.getValue()) {
                asciiCharsArray.add(c);
            }
            asciiCharsObject.add(entry.getKey(), asciiCharsArray);
        }

        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("collations", collationsObject);
        jsonObject.add("asciiChars", asciiCharsObject);
        return jsonObject.toString();
    }

}

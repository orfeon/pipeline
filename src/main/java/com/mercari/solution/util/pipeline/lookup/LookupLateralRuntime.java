package com.mercari.solution.util.pipeline.lookup;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.udf.UserDefinedFunctions;
import com.mercari.solution.util.schema.CalciteSchemaUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.DataContext;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.Casing;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.avatica.util.Quoting;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Enumerable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.linq4j.Linq4j;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.ScannableTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Table;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.impl.AbstractTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.FrameworkConfig;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.Planner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RelRunners;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-worker evaluator for one correlated-LATERAL inner block over a lookup
 * table: the block's plan — with the key-contract correlations stripped, since
 * they are satisfied by the key-driven fetch — carried as a self-contained SQL
 * statement, evaluated repeatedly against the per-key row sets fetched by
 * {@link LookupLateralEnumerable}.
 *
 * <p>The statement is compiled once (lazily, on first use inside the worker)
 * against a mutable buffer table registered under the lookup table's own
 * {@code schema.table} name, then re-executed per key set — the same
 * plan-once/execute-many pattern as {@code Query2} itself, one level down.
 * Results are returned in Calcite-internal values so they flow into the outer
 * plan unchanged.
 *
 * <p>Like {@link LookupSourceRegistry}, instances are resolved by generated
 * code through a process-wide id registry; the creator ({@code Query2}) owns
 * the lifecycle and must {@link #close()} on teardown.
 */
public final class LookupLateralRuntime implements AutoCloseable {

    private static final AtomicLong IDS = new AtomicLong();
    private static final ConcurrentHashMap<Long, LookupLateralRuntime> RUNTIMES =
            new ConcurrentHashMap<>();

    public static LookupLateralRuntime get(long id) {
        final LookupLateralRuntime runtime = RUNTIMES.get(id);
        if (runtime == null) {
            throw new IllegalStateException("Lateral runtime id " + id + " is not registered "
                    + "(query closed or not set up?)");
        }
        return runtime;
    }

    private final long id;
    private final String schemaName;
    private final String tableName;
    private final Schema leafSchema;
    private final String innerSql;
    private final List<String> innerColumnTypes;
    private final List<UserDefinedFunctions.FunctionSpec> functions;

    private List<Object[]> buffer;
    private Planner planner;
    private PreparedStatement statement;

    private LookupLateralRuntime(String schemaName, String tableName, Schema leafSchema,
            String innerSql, List<String> innerColumnTypes,
            List<UserDefinedFunctions.FunctionSpec> functions) {
        this.id = IDS.incrementAndGet();
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.leafSchema = leafSchema;
        this.innerSql = innerSql;
        this.innerColumnTypes = List.copyOf(innerColumnTypes);
        this.functions = functions == null ? List.of() : List.copyOf(functions);
        RUNTIMES.put(id, this);
    }

    /**
     * @param schemaName       the lookup source's schema name (buffer table schema)
     * @param tableName        the lookup leaf table name
     * @param leafSchema       the leaf table's row schema (buffer table row type)
     * @param innerSql         the stripped inner plan as SQL over {@code schemaName.tableName}
     * @param innerColumnTypes SqlTypeName names of the inner result columns, in order
     * @param functions        UDFs the inner plan may reference
     */
    public static LookupLateralRuntime create(String schemaName, String tableName,
            Schema leafSchema, String innerSql, List<String> innerColumnTypes,
            List<UserDefinedFunctions.FunctionSpec> functions) {
        return new LookupLateralRuntime(
                schemaName, tableName, leafSchema, innerSql, innerColumnTypes, functions);
    }

    public long id() {
        return id;
    }

    public String innerSql() {
        return innerSql;
    }

    /**
     * Evaluates the inner plan against one key's fetched leaf rows
     * (Calcite-internal values, full leaf columns) and returns the result rows
     * as Calcite-internal values.
     */
    public List<Object[]> evaluate(List<Object[]> leafRows) {
        if (statement == null) {
            init();
        }
        buffer.clear();
        buffer.addAll(leafRows);
        try (ResultSet resultSet = statement.executeQuery()) {
            final List<Object[]> rows = new ArrayList<>();
            while (resultSet.next()) {
                final Object[] row = new Object[innerColumnTypes.size()];
                for (int i = 0; i < row.length; i++) {
                    row[i] = toInternal(innerColumnTypes.get(i), resultSet.getObject(i + 1));
                }
                rows.add(row);
            }
            return rows;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "failed to evaluate lateral inner query: " + innerSql, e);
        }
    }

    /** Compiles the inner statement once, against the mutable buffer table. */
    private void init() {
        this.buffer = new ArrayList<>();
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add(schemaName,
                new BufferSchema(tableName, new BufferTable(leafSchema, buffer)));
        // UDFs referenced inside the block resolve against the root (default) schema.
        for (final Map.Entry<String, List<org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Function>> entry
                : UserDefinedFunctions.build(functions).entrySet()) {
            for (final var function : entry.getValue()) {
                rootSchema.add(entry.getKey(), function);
            }
        }
        // The inner SQL is generated with every identifier double-quoted, so it
        // parses case-sensitively regardless of the outer query's lex.
        final SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .setQuotedCasing(Casing.UNCHANGED)
                .setUnquotedCasing(Casing.TO_UPPER)
                .setCaseSensitive(true)
                .build();
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .defaultSchema(rootSchema)
                .build();
        this.planner = Frameworks.getPlanner(config);
        try {
            final SqlNode parsed = planner.parse(innerSql);
            final SqlNode validated = planner.validate(parsed);
            final RelNode relNode = planner.rel(validated).project();
            this.statement = RelRunners.run(relNode);
        } catch (Exception e) {
            close();
            throw new IllegalStateException(
                    "failed to prepare lateral inner query: " + innerSql, e);
        }
    }

    @Override
    public void close() {
        RUNTIMES.remove(id);
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
        } catch (Exception ignore) {
            // closing best-effort
        } finally {
            statement = null;
        }
        try {
            if (planner != null) {
                planner.close();
            }
        } catch (Exception ignore) {
            // closing best-effort
        } finally {
            planner = null;
        }
    }

    /** Avatica-rendered result value → Calcite-internal value, by declared column type. */
    private static Object toInternal(final String sqlTypeName, final Object value) {
        if (value == null) {
            return null;
        }
        return switch (sqlTypeName) {
            case "TINYINT", "SMALLINT" -> ((Number) value).shortValue();
            case "INTEGER" -> ((Number) value).intValue();
            case "BIGINT" -> ((Number) value).longValue();
            case "REAL", "FLOAT", "DOUBLE" -> ((Number) value).doubleValue();
            case "DECIMAL" -> value instanceof BigDecimal d ? d : new BigDecimal(value.toString());
            case "BOOLEAN" -> value;
            case "CHAR", "VARCHAR" -> value.toString();
            case "DATE" -> switch (value) {
                case Date date -> (int) date.toLocalDate().toEpochDay();
                case Number number -> number.intValue();
                default -> value;
            };
            case "TIME" -> switch (value) {
                case Time time -> (int) (time.toLocalTime().toNanoOfDay() / 1_000_000L);
                case Number number -> number.intValue();
                default -> value;
            };
            case "TIMESTAMP" -> switch (value) {
                case Timestamp timestamp ->
                        timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
                case Number number -> number.longValue();
                default -> value;
            };
            case "BINARY", "VARBINARY" -> switch (value) {
                case byte[] bytes -> new ByteString(bytes);
                case ByteString byteString -> byteString;
                case String s -> new ByteString(s.getBytes(StandardCharsets.UTF_8));
                default -> value;
            };
            default -> value;
        };
    }

    /** Schema holding the single mutable buffer table under the leaf table's name. */
    private static final class BufferSchema extends AbstractSchema {

        private final Map<String, Table> tables;

        private BufferSchema(String tableName, BufferTable table) {
            this.tables = Map.of(tableName, table);
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return tables;
        }
    }

    /** Mutable in-memory table over the current key's fetched leaf rows. */
    private static final class BufferTable extends AbstractTable implements ScannableTable {

        private final Schema schema;
        private final List<Object[]> rows;

        private RelDataType rowType;

        private BufferTable(Schema schema, List<Object[]> rows) {
            this.schema = schema;
            this.rows = rows;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            if (rowType == null) {
                rowType = CalciteSchemaUtil.convertSchema(schema, typeFactory);
            }
            return rowType;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(rows);
        }
    }
}

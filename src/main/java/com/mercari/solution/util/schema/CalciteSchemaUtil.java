package com.mercari.solution.util.schema;

import com.mercari.solution.module.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.*;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.SqlTypeName;

import java.sql.*;
import java.util.*;

public class CalciteSchemaUtil {

    public static RelDataType convertSchema(final Schema schema, final RelDataTypeFactory relDataTypeFactory) {
        final List<RelDataTypeField> fields = new ArrayList<>(schema.countFields());
        for (int i = 0; i < schema.countFields(); i++) {
            final Schema.Field schemaField = schema.getField(i);
            final RelDataType fieldType = createRelDataType(schemaField.getFieldType(), relDataTypeFactory);
            final RelDataTypeField field = new RelDataTypeFieldImpl(schemaField.getName(), i, fieldType);
            fields.add(field);
        }
        return new RelRecordType(StructKind.PEEK_FIELDS, fields, true);
    }

    /**
     * The result schema of a planned query. JDBC metadata describes a scalar column (and an
     * array of scalars), but it has no shape for a struct: a ROW column is a bare STRUCT and an
     * array of ROWs only a type name. Those columns ({@link #isNested}) are derived from the
     * planned row type instead.
     */
    public static Schema convertSchema(final ResultSetMetaData metaData, final RelDataType rowType) {
        try {
            final List<RelDataTypeField> relFields = rowType.getFieldList();
            if (relFields.size() != metaData.getColumnCount()) {
                throw new IllegalStateException("result columns do not match the planned row type: "
                        + metaData.getColumnCount() + " vs " + rowType);
            }
            final Schema.Builder builder = Schema.builder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                final String fieldName = metaData.getColumnName(i);
                final RelDataType relType = relFields.get(i - 1).getType();
                final Schema.FieldType fieldType = isNested(relType)
                        ? convertFieldType(relType, fieldName)
                        : convertFieldType(metaData.getColumnType(i), metaData.getColumnTypeName(i));
                builder.withField(fieldName, fieldType);
            }
            return builder.build();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** A struct, a map, or an array of anything but scalars. */
    public static boolean isNested(final RelDataType relType) {
        return switch (relType.getSqlTypeName()) {
            case ROW, MAP -> true;
            case ARRAY, MULTISET -> {
                final RelDataType component = relType.getComponentType();
                yield component != null && (isNested(component)
                        || component.getSqlTypeName() == SqlTypeName.ARRAY
                        || component.getSqlTypeName() == SqlTypeName.MULTISET);
            }
            default -> false;
        };
    }

    private static Schema.FieldType convertFieldType(final RelDataType relType, final String path) {
        return switch (relType.getSqlTypeName()) {
            case BOOLEAN -> Schema.FieldType.BOOLEAN;
            case TINYINT, SMALLINT, INTEGER -> Schema.FieldType.INT32;
            case BIGINT -> Schema.FieldType.INT64;
            case REAL -> Schema.FieldType.FLOAT32;
            case FLOAT, DOUBLE -> Schema.FieldType.FLOAT64;
            case DECIMAL -> Schema.FieldType.DECIMAL;
            // an untyped NULL is a string, as the metadata path makes a top-level one
            case CHAR, VARCHAR, NULL -> Schema.FieldType.STRING;
            case DATE -> Schema.FieldType.DATE;
            case TIME, TIME_WITH_LOCAL_TIME_ZONE -> Schema.FieldType.TIME;
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> Schema.FieldType.TIMESTAMP;
            case BINARY, VARBINARY -> Schema.FieldType.BYTES;
            case ARRAY, MULTISET -> {
                final SqlTypeName componentType = relType.getComponentType().getSqlTypeName();
                if (componentType == SqlTypeName.ARRAY || componentType == SqlTypeName.MULTISET) {
                    // the element coder (UnionMapCoder) has no list directly inside a list
                    throw new IllegalArgumentException("Not supported array of arrays: " + relType
                            + " of output column: " + path
                            + " (wrap the inner array in a struct: ARRAY(SELECT x AS inner_array ...))");
                }
                yield Schema.FieldType.array(convertFieldType(relType.getComponentType(), path));
            }
            case MAP -> {
                final SqlTypeName keyType = relType.getKeyType().getSqlTypeName();
                if (keyType != SqlTypeName.CHAR && keyType != SqlTypeName.VARCHAR) {
                    throw new IllegalArgumentException("Not supported map key type: " + keyType
                            + " of output column: " + path + " (only string keys)");
                }
                yield Schema.FieldType.map(convertFieldType(relType.getValueType(), path));
            }
            case ROW -> {
                final Schema.Builder builder = Schema.builder();
                for (final RelDataTypeField field : relType.getFieldList()) {
                    if (field.getName().startsWith("EXPR$")) {
                        throw new IllegalArgumentException("struct field without a name in output column: "
                                + path + " (" + relType + "); name the fields:"
                                + " ARRAY(SELECT a AS x, b AS y FROM ...) for an array of structs,"
                                + " CAST(ROW(a, b) AS ROW(x BIGINT, y DOUBLE)) for a struct");
                    }
                    builder.withField(field.getName(),
                            convertFieldType(field.getType(), path + "." + field.getName()));
                }
                yield Schema.FieldType.element(builder.build());
            }
            default -> throw new IllegalArgumentException("Not supported type: " + relType
                    + " of output column: " + path);
        };
    }

    private static RelDataType createRelDataType(
            final Schema.FieldType fieldType,
            final RelDataTypeFactory relDataTypeFactory) {
        final RelDataType relDataType = switch (fieldType.getType()) {
            // The value type is built recursively: createSqlType(ARRAY / ROW / MAP) is a type
            // without a component, fields or a key (an assertion error where assertions are on).
            case map -> relDataTypeFactory.createMapType(
                        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
                        createRelDataType(fieldType.getMapValueType(), relDataTypeFactory));
            case array -> relDataTypeFactory.createArrayType(
                        createRelDataType(fieldType.getArrayValueType(), relDataTypeFactory), -1L);
            // A matrix field surfaces as a flat ARRAY (row-major; the shape lives in the schema).
            case matrix -> relDataTypeFactory.createArrayType(
                        createRelDataType(fieldType.getMatrixValueType(), relDataTypeFactory), -1L);
            case element -> convertSchema(fieldType.getElementSchema(), relDataTypeFactory);
            default -> relDataTypeFactory.createSqlType(convertSqlTypeName(fieldType));
        };
        if(fieldType.getNullable()) {
            return relDataTypeFactory.createTypeWithNullability(relDataType, true);
        } else {
            // TODO
            return relDataTypeFactory.createTypeWithNullability(relDataType, true);
        }
    }

    private static SqlTypeName convertSqlTypeName(final Schema.FieldType fieldType) {
        return switch (fieldType.getType()) {
            case bool -> SqlTypeName.BOOLEAN;
            case string, uuid, json, enumeration -> SqlTypeName.VARCHAR;
            case bytes -> SqlTypeName.BINARY;
            case int16, int8 -> SqlTypeName.SMALLINT;
            case int32 -> SqlTypeName.INTEGER;
            case int64 -> SqlTypeName.BIGINT;
            case float32 -> SqlTypeName.FLOAT;
            case float64 -> SqlTypeName.DOUBLE;
            case decimal -> SqlTypeName.DECIMAL;
            case date -> SqlTypeName.DATE;
            case time -> SqlTypeName.TIME;
            case timestamp, datetime -> SqlTypeName.TIMESTAMP;
            case array, matrix -> SqlTypeName.ARRAY;
            case map -> SqlTypeName.MAP;
            case element -> SqlTypeName.ROW;
            default -> throw new IllegalArgumentException("Not supported calcite data type: " + fieldType.getType());
        };
    }

    private static Schema.FieldType convertFieldType(final int type, final String typeName) {
        return switch (type) {
            case Types.BIT, Types.BOOLEAN -> Schema.FieldType.BOOLEAN;
            case Types.DECIMAL, Types.NUMERIC -> Schema.FieldType.DECIMAL;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Schema.FieldType.INT32;
            case Types.BIGINT -> Schema.FieldType.INT64;
            case Types.REAL -> Schema.FieldType.FLOAT32;
            case Types.FLOAT, Types.DOUBLE -> Schema.FieldType.FLOAT64;
            case Types.ROWID, Types.CLOB, Types.NCLOB,
                 Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR,
                 Types.LONGVARCHAR, Types.LONGNVARCHAR -> {
                if("json".equalsIgnoreCase(typeName)) {
                    yield Schema.FieldType.JSON;
                }
                yield Schema.FieldType.STRING;
            }
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> Schema.FieldType.TIME;
            case Types.DATE -> Schema.FieldType.DATE;
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> Schema.FieldType.TIMESTAMP;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> Schema.FieldType.BYTES;
            case Types.ARRAY -> {
                final String innerTypeName = typeName.replace("ARRAY","").trim();
                final Schema.FieldType arrayFieldType = switch (innerTypeName) {
                    case "BLOB", "BINARY", "VARBINARY" -> Schema.FieldType.BYTES;
                    case "TINYINT", "SMALLINT", "INTEGER", "INT2", "INT4" -> Schema.FieldType.INT32;
                    case "BIGINT", "INT8" -> Schema.FieldType.INT64;
                    case "REAL", "FLOAT4" -> Schema.FieldType.FLOAT32;
                    case "FLOAT", "DOUBLE", "FLOAT8" -> Schema.FieldType.FLOAT64;
                    case "NUMERIC", "DECIMAL", "BIGDECIMAL" -> Schema.FieldType.DECIMAL;
                    case "TEXT", "CHAR", "MCHAR", "NCHAR",
                         "VARCHAR", "MVARCHAR", "NVARCHAR", "CLOB", "BPCHAR" -> Schema.FieldType.STRING;
                    case "DATE" -> Schema.FieldType.DATE;
                    case "TIME" -> Schema.FieldType.TIME;
                    case "TIMESTAMP" -> Schema.FieldType.TIMESTAMP;
                    case "BIT", "BOOLEAN" -> Schema.FieldType.BOOLEAN;
                    case "JSON" -> Schema.FieldType.JSON;
                    default -> throw new IllegalStateException("Not supported ArrayElementType: " + typeName.replace("ARRAY","").trim());
                };
                yield Schema.FieldType.array(arrayFieldType);
            }
            case Types.STRUCT, Types.JAVA_OBJECT -> {
                yield Schema.FieldType.STRING;
            }
            case Types.REF, Types.SQLXML, Types.OTHER,
                 Types.REF_CURSOR, Types.DISTINCT, Types.DATALINK, Types.NULL -> {
                yield Schema.FieldType.STRING;
            }
            default -> Schema.FieldType.STRING;
        };
    }

    public static CustomReturnTypeInference createCustomReturnTypeInference(final Schema.FieldType fieldType) {
        return new CustomReturnTypeInference(fieldType);
    }

    public static class CustomReturnTypeInference implements SqlReturnTypeInference {

        private final Schema.FieldType fieldType;

        private CustomReturnTypeInference(final Schema.FieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            final RelDataType relDataType = switch (this.fieldType.getType()) {
                case bool -> typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case string, uuid, json -> typeFactory.createSqlType(SqlTypeName.VARCHAR);
                case bytes ->  typeFactory.createSqlType(SqlTypeName.BINARY);
                case int32 -> typeFactory.createSqlType(SqlTypeName.INTEGER);
                case int64 -> typeFactory.createSqlType(SqlTypeName.BIGINT);
                case float32 -> typeFactory.createSqlType(SqlTypeName.REAL);
                case float64 -> typeFactory.createSqlType(SqlTypeName.FLOAT);
                case date -> typeFactory.createSqlType(SqlTypeName.DATE);
                case time -> typeFactory.createSqlType(SqlTypeName.TIME);
                case timestamp -> typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case element -> {
                    final List<String> fieldNames = new ArrayList<>();
                    final List<RelDataType> fieldTypes = new ArrayList<>();
                    for(final Schema.Field field : fieldType.getElementSchema().getFields()) {
                        fieldNames.add(field.getName());
                        fieldTypes.add(createRelDataType(field.getFieldType(), typeFactory));
                    }
                    yield typeFactory.createStructType(fieldTypes, fieldNames);
                }
                case array -> typeFactory.createArrayType(
                        createRelDataType(fieldType.getArrayValueType(), typeFactory), -1L);
                default -> throw new IllegalArgumentException();
            };

            return typeFactory.createTypeWithNullability(relDataType, true);
        }
    }

}

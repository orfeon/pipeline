package com.mercari.solution.util.domain.db;

import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

/**
 * Column comments of a jdbc table ({@code DatabaseMetaData.getColumns} REMARKS) become the Avro
 * field doc of the inferred table schema, and therefore the Schema.Field description.
 */
public class JdbcUtilColumnRemarksTest {

    @Test
    public void testColumnRemarksBecomeFieldDoc() throws Exception {
        final String url = "jdbc:h2:mem:jdbcremarks;DB_CLOSE_DELAY=-1";
        try(final Connection connection = DriverManager.getConnection(url, "sa", "")) {
            try(final Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE members (id BIGINT PRIMARY KEY, name VARCHAR(64), score DOUBLE)");
                statement.execute("COMMENT ON COLUMN members.id IS 'member id'");
                statement.execute("COMMENT ON COLUMN members.score IS 'latest score'");
                statement.execute("COMMENT ON TABLE members IS 'members table'");
            }

            final Map<String, String> remarks = JdbcUtil.getColumnRemarks(connection, "members");
            Assertions.assertEquals("member id", remarks.get("ID"));
            Assertions.assertEquals("member id", remarks.get("id"));
            Assertions.assertFalse(remarks.containsKey("NAME"));

            final org.apache.avro.Schema avro = JdbcUtil.createAvroSchemaFromTable(connection, "members");
            Assertions.assertEquals("member id", avro.getField("ID").doc());
            Assertions.assertNull(avro.getField("NAME").doc());
            Assertions.assertEquals("latest score", avro.getField("SCORE").doc());

            Assertions.assertEquals("members table", JdbcUtil.getTableRemark(connection, "members"));
            Assertions.assertEquals("members table", avro.getDoc());

            final Schema schema = Schema.of(avro);
            Assertions.assertEquals("members table", schema.getDescription());
            Assertions.assertEquals("member id", schema.getField("ID").getDescription());
            Assertions.assertEquals("latest score", schema.getField("SCORE").getDescription());

            // unknown table: fail-soft, empty map
            Assertions.assertTrue(JdbcUtil.getColumnRemarks(connection, "no_such_table").isEmpty());
            Assertions.assertNull(JdbcUtil.getTableRemark(connection, "no_such_table"));
        }
    }

}

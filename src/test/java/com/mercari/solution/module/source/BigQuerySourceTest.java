package com.mercari.solution.module.source;

import com.google.api.services.bigquery.model.TableReference;
import com.mercari.solution.module.IllegalModuleException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BigQuerySourceTest {

    private static final TableReference VIEW = new TableReference()
            .setProjectId("my-project").setDatasetId("my_dataset").setTableId("my_view");

    @Test
    public void testOnlyViewsAndExternalTablesRequireQueryRead() {
        Assertions.assertTrue(BigQuerySource.requiresQueryRead("VIEW"));
        Assertions.assertTrue(BigQuerySource.requiresQueryRead("EXTERNAL"));
        Assertions.assertFalse(BigQuerySource.requiresQueryRead("TABLE"));
        Assertions.assertFalse(BigQuerySource.requiresQueryRead("MATERIALIZED_VIEW"));
        Assertions.assertFalse(BigQuerySource.requiresQueryRead("SNAPSHOT"));
        Assertions.assertFalse(BigQuerySource.requiresQueryRead(null));
    }

    @Test
    public void testTableQueryWithoutProjectionOrRestriction() {
        Assertions.assertEquals(
                "SELECT * FROM `my-project.my_dataset.my_view`",
                BigQuerySource.createTableQuery(VIEW, null, null));
        Assertions.assertEquals(
                "SELECT * FROM `my-project.my_dataset.my_view`",
                BigQuerySource.createTableQuery(VIEW, List.of(), "  "));
    }

    @Test
    public void testTableQueryProjectsFieldsAndFiltersRows() {
        Assertions.assertEquals(
                "SELECT `id`, `name` FROM `my-project.my_dataset.my_view` WHERE age > 18 AND status = 'active'",
                BigQuerySource.createTableQuery(VIEW, List.of("id", " name"), " age > 18 AND status = 'active' "));
    }

    @Test
    public void testTableQueryRejectsNestedFieldPaths() {
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> BigQuerySource.createTableQuery(VIEW, List.of("id", "address.city"), null));
        Assertions.assertTrue(e.getMessage().contains("address.city"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("query"), e.getMessage());
    }

}

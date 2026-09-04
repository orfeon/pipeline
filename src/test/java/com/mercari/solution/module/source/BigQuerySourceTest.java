package com.mercari.solution.module.source;

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
                "SELECT `id`, `name`, `address`.`city` FROM `my-project.my_dataset.my_view` WHERE age > 18 AND status = 'active'",
                BigQuerySource.createTableQuery(VIEW, List.of("id", " name", "address.city"), " age > 18 AND status = 'active' "));
    }

    @Test
    public void testQueryRunProjectIdBecomesJobProjectWhenUnset() {
        final PipelineOptions options = PipelineOptionsFactory.create();
        Assertions.assertNull(options.as(BigQueryOptions.class).getBigQueryProject());

        BigQuerySource.applyQueryRunProject(options, "query-project");

        Assertions.assertEquals("query-project", options.as(BigQueryOptions.class).getBigQueryProject());
    }

    @Test
    public void testConfiguredJobProjectIsKept() {
        final PipelineOptions options = PipelineOptionsFactory.create();
        options.as(BigQueryOptions.class).setBigQueryProject("configured-project");

        BigQuerySource.applyQueryRunProject(options, "query-project");

        Assertions.assertEquals("configured-project", options.as(BigQueryOptions.class).getBigQueryProject());
    }

    @Test
    public void testSameJobProjectIsNoop() {
        final PipelineOptions options = PipelineOptionsFactory.create();
        options.as(BigQueryOptions.class).setBigQueryProject("same-project");

        BigQuerySource.applyQueryRunProject(options, "same-project");

        Assertions.assertEquals("same-project", options.as(BigQueryOptions.class).getBigQueryProject());
    }

}

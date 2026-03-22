package com.mercari.solution.util.cloud.google.workspace;

import com.mercari.solution.module.Schema;

public class DocsUtil {



    public static Schema createSchema() {
        return Schema.builder()
                .withField("documentId", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("revisionId", Schema.FieldType.STRING)
                .withField("suggestionsViewMode", Schema.FieldType.STRING)
                //.withField("sheets", Schema.FieldType.array(Schema.FieldType.element(createSheetSchema())))
                .build();
    }

    public static Schema createBodySchema() {
        return Schema.builder()
                .withField("content", Schema.FieldType.array(Schema.FieldType.element(createContentSchema())))
                .build();
    }


    public static Schema createContentSchema() {
        return Schema.builder()
                .withField("startIndex", Schema.FieldType.INT64)
                .withField("endIndex", Schema.FieldType.INT64)
                .withField("paragraph", Schema.FieldType.INT64)
                .build();
    }

    public static Schema createParagraphSchema() {
        return Schema.builder()
                .withField("startIndex", Schema.FieldType.INT64)
                .withField("endIndex", Schema.FieldType.INT64)
                .withField("paragraph", Schema.FieldType.INT64)
                .build();
    }
}

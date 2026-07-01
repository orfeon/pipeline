package com.mercari.solution.util.cloud.google.workspace;

import com.mercari.solution.module.Schema;

public class FormsUtil {

    public static Schema createSchema() {
        return Schema.builder()
                .withField("formId", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("documentTitle", Schema.FieldType.STRING)
                .withField("description", Schema.FieldType.STRING)
                .withField("revisionId", Schema.FieldType.STRING)
                .withField("responderUri", Schema.FieldType.STRING)
                .withField("linkedSheetId", Schema.FieldType.STRING)
                .withField("items", Schema.FieldType.array(Schema.FieldType.element(createItemSchema())))
                .build();
    }

    public static Schema createItemSchema() {
        return Schema.builder()
                .withField("itemId", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("description", Schema.FieldType.STRING)

                .withField("questionItem", Schema.FieldType.STRING)
                .withField("questionGroupItem", Schema.FieldType.STRING)
                .withField("pageBreakItem", Schema.FieldType.STRING)
                .withField("textItem", Schema.FieldType.STRING)
                .withField("imageItem", Schema.FieldType.STRING)
                .withField("videoItem", Schema.FieldType.STRING)
                .build();
    }

}

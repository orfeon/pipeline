package com.mercari.solution.util.cloud.google.workspace;

import com.mercari.solution.module.Schema;

import java.util.List;

public class SlidesUtil {

    public static Schema createSchema() {
        return Schema.builder()
                .withField("presentationId", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("locale", Schema.FieldType.STRING)
                .withField("revisionId", Schema.FieldType.STRING)
                .withField("pages", Schema.FieldType.array(Schema.FieldType.element(createPageSchema())))
                .build();
    }

    public static Schema createPageSchema() {
        return Schema.builder()
                .withField("objectId", Schema.FieldType.STRING)
                .withField("pageType", Schema.FieldType.enumeration(List.of("SLIDE","MASTER","LAYOUT","NOTES","NOTES_MASTER")))
                .withField("pageElements", Schema.FieldType.STRING)
                .withField("revisionId", Schema.FieldType.STRING)
                //.withField("sheets", Schema.FieldType.array(Schema.FieldType.element(createSheetSchema())))
                .build();
    }

    public static Schema createPageElementSchema() {
        return Schema.builder()
                .withField("objectId", Schema.FieldType.STRING)
                .withField("title", Schema.FieldType.STRING)
                .withField("description", Schema.FieldType.STRING)
                .withField("pageType", Schema.FieldType.enumeration(List.of("SLIDE","MASTER","LAYOUT","NOTES","NOTES_MASTER")))
                .withField("revisionId", Schema.FieldType.STRING)
                //.withField("sheets", Schema.FieldType.array(Schema.FieldType.element(createSheetSchema())))
                .build();
    }

}

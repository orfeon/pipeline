package com.mercari.solution.util.schema;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ElementSchemaUtilTest {

    private static final String DOTTED = "present.0.key";

    private static final List<Schema.Field> FIELDS = List.of(
            Schema.Field.of("id", Schema.FieldType.INT64),
            Schema.Field.of(DOTTED, Schema.FieldType.array(Schema.FieldType.FLOAT32)),
            Schema.Field.of("tok", Schema.FieldType.element(List.of(
                    Schema.Field.of("ids", Schema.FieldType.array(Schema.FieldType.INT64))))));

    // a field name may contain dots (model outputs): it comes before a nested path, wherever it is in the fields
    @Test
    public void testExactNameComesFirst() {
        Assertions.assertEquals(DOTTED, ElementSchemaUtil.getInputField(DOTTED, FIELDS).getName());
        Assertions.assertEquals(DOTTED, ElementSchemaUtil.getInputFieldName(DOTTED, FIELDS));
        Assertions.assertEquals(Schema.Type.array, ElementSchemaUtil.getInputFieldType(DOTTED, FIELDS).getType());
    }

    @Test
    public void testNestedPath() {
        Assertions.assertEquals("ids", ElementSchemaUtil.getInputField("tok.ids", FIELDS).getName());
        Assertions.assertEquals("tok", ElementSchemaUtil.getInputFieldName("tok.ids", FIELDS));
        Assertions.assertEquals(Schema.Type.array, ElementSchemaUtil.getInputFieldType("tok.ids", FIELDS).getType());
    }

    @Test
    public void testNotFound() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ElementSchemaUtil.getInputField("missing", FIELDS));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ElementSchemaUtil.getInputField("missing.child", FIELDS));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ElementSchemaUtil.getInputFieldType("missing", FIELDS));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ElementSchemaUtil.getInputFieldType("missing.child", FIELDS));
    }

    // select functions that keep the parent of a nested path as their input field
    @Test
    public void testSelectFunctionsOnADottedName() {
        for(final String func : List.of("rename", "replace", "reshape")) {
            final SelectFunction function = SelectFunction.of(new Gson().fromJson(
                    "{ \"name\": \"o\", \"func\": \"" + func + "\", \"field\": \"" + DOTTED + "\", \"shape\": [2] }", JsonObject.class), FIELDS);
            Assertions.assertEquals(DOTTED, function.getInputFields().getFirst().getName(), func);
        }
    }

}

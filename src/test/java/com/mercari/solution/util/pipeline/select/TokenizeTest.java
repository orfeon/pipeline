package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TokenizeTest {

    private static List<Schema.Field> inputFields() {
        return List.of(Schema.Field.of("textField", Schema.FieldType.STRING));
    }

    private static JsonObject json(final String text) {
        return new Gson().fromJson(text, JsonObject.class);
    }

    // NOTE: actual tokenization requires a HuggingFace tokenizer model file (DJL native libs),
    // so only parameter validation is tested here.

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"field\": \"textField\", \"path\": \"/tmp/tokenizer.json\" }"),
                        inputFields(), true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"tokenizer\": \"huggingface\", \"path\": \"/tmp/tokenizer.json\" }"),
                        inputFields(), true, false));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"tokenizer\": \"huggingface\", \"field\": \"textField\" }"),
                        inputFields(), true, false));
        // unknown tokenizer name
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                Tokenize.of("t", json("{ \"name\": \"t\", \"tokenizer\": \"unknown\", \"field\": \"textField\", \"path\": \"/tmp/tokenizer.json\" }"),
                        inputFields(), true, false));
    }

}

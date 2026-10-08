package com.mercari.solution.module.transform;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The select function {@code fwf_decode} in a pipeline (work_fixedwidth.md §7): records that arrive
 * as a bytes / string field (a message payload, a line of another source) are decoded in place.
 */
public class SelectFwfDecodeTest {

    private static final Charset MS932 = Charset.forName("windows-31j");

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    // code (4) + name (10 bytes, full-width) + amount (5, implied scale 1)
    private static final String RECORD_1 = "A001" + "㈱東京　　" + "00123";
    private static final String RECORD_2 = "B002" + "大阪商店　" + "00045";

    private static final String LAYOUT = """
                          schema:
                            encoding: { format: fwf, charset: windows-31j }
                            reference:
                              inline:
                                recordLength: 19
                                fields:
                                  - { name: code,   type: string,  len: 4 }
                                  - { name: name,   type: string,  len: 10 }
                                  - { name: amount, type: decimal, len: 5, scale: 1 }
                """;

    private static String base64(final String record) {
        return Base64.getEncoder().encodeToString(record.getBytes(MS932));
    }

    @Test
    public void testDecodeBytesPayloadAndUseItsFields() throws Exception {
        // the payload is bytes in windows-31j (here: base64 text decoded to bytes first); the decoded
        // record is a field like any other and its fields can be taken out by a later select
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: messages
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { id: 1, encoded: "%s" }
                        - { id: 2, encoded: "%s" }
                    schema:
                      fields:
                        - { name: id, type: int64 }
                        - { name: encoded, type: string }
                transforms:
                  - name: decoded
                    module: select
                    inputs: [messages]
                    parameters:
                      select:
                        - { name: id }
                        - { name: payload, func: base64_decode, field: encoded, type: bytes }
                        - name: order
                          func: fwf_decode
                          field: payload
                %s
                  - name: flat
                    module: select
                    inputs: [decoded]
                    parameters:
                      select:
                        - { name: id }
                        - { name: code, field: order.code }
                        - { name: name, field: order.name }
                        - { name: amount, field: order.amount, type: float64 }
                """.formatted(base64(RECORD_1), base64(RECORD_2), LAYOUT)));

        final Schema decodedSchema = outputs.get("decoded").getSchema();
        Assertions.assertEquals(Schema.Type.element, decodedSchema.getField("order").getFieldType().getType());

        PAssert.that(outputs.get("flat").getCollection()).satisfies(elements -> {
            final Set<String> rows = new HashSet<>();
            for(final MElement element : elements) {
                rows.add(element.getAsLong("id") + ":" + element.getAsString("code") + ":" + element.getAsString("name")
                        + ":" + element.getAsDouble("amount"));
            }
            Assertions.assertEquals(Set.of("1:A001:㈱東京:12.3", "2:B002:大阪商店:4.5"), rows);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testRepeatedFieldsWithEmptyElements() throws Exception {
        // an array holds no null: an empty text element is "", an empty number is its defaultValue,
        // and without one the record is a failure (it used to crash the avro coder of the output)
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: lines
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { line: "A0102abcd0102" }
                        - { line: "B02  ab  0304" }
                        - { line: "C0102abcd  02" }
                    schema:
                      fields:
                        - { name: line, type: string }
                transforms:
                  - name: decoded
                    module: select
                    inputs: [lines]
                    failFast: false
                    parameters:
                      select:
                        - name: row
                          func: fwf_decode
                          field: line
                          schema:
                            encoding: { format: fwf }
                            reference:
                              inline:
                                fields:
                                  - { name: code,   type: string, len: 1 }
                                  - { name: marks,  type: int32,  len: 2, repeat: 2, defaultValue: 0 }
                                  - { name: names,  type: string, len: 2, repeat: 2 }
                                  - { name: scores, type: int32,  len: 2, repeat: 2 }
                  - name: flat
                    module: select
                    inputs: [decoded]
                    parameters:
                      select:
                        - { name: code, field: row.code }
                        - { name: marks, field: row.marks }
                        - { name: names, field: row.names }
                """));
        PAssert.that(outputs.get("flat").getCollection()).satisfies(elements -> {
            final Set<String> rows = new HashSet<>();
            for(final MElement element : elements) {
                rows.add(element.getAsString("code") + ":" + element.getPrimitiveValue("marks") + ":" + element.getPrimitiveValue("names"));
            }
            // C has an empty score, which has no default: that record is the failure
            Assertions.assertEquals(Set.of("A:[1, 2]:[ab, cd]", "B:[2, 0]:[ab, ]"), rows);
            return null;
        });
        pipeline.run();
    }

    @Test
    public void testDecodeStringFieldAndFailureRouting() throws Exception {
        // a string field is encoded back with the charset before it is cut; a record that can not be
        // decoded is a failure of that record, the others go on
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, Config.load("""
                sources:
                  - name: lines
                    module: create
                    parameters:
                      type: element
                      elements:
                        - { line: "%s" }
                        - { line: "SHORT" }
                        - { line: "%s" }
                    schema:
                      fields:
                        - { name: line, type: string }
                transforms:
                  - name: decoded
                    module: select
                    inputs: [lines]
                    failFast: false
                    parameters:
                      select:
                        - name: order
                          func: fwf_decode
                          field: line
                %s
                  - name: flat
                    module: select
                    inputs: [decoded]
                    parameters:
                      select:
                        - { name: code, field: order.code }
                """.formatted(RECORD_1, RECORD_2, LAYOUT)));
        PAssert.that(outputs.get("flat").getCollection()).satisfies(elements -> {
            final Set<String> codes = new HashSet<>();
            for(final MElement element : elements) {
                codes.add(element.getAsString("code"));
            }
            Assertions.assertEquals(Set.of("A001", "B002"), codes);
            return null;
        });
        pipeline.run();
    }

}

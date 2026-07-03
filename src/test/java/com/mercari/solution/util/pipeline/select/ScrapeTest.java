package com.mercari.solution.util.pipeline.select;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.Schema;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScrapeTest {

    private static final Instant TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static final String HTML = """
            <html><body>
              <div id="title">Hello World</div>
              <ul>
                <li class="item" data-id="1">100</li>
                <li class="item" data-id="2">2,000</li>
              </ul>
              <span class="price">price: 1,234 yen</span>
              <a href="/next">next</a>
              <div class="prod"><span class="name">A</span><span class="val">1.5</span></div>
              <div class="prod"><span class="name">B</span><span class="val">2.5</span></div>
              <div class="flag">true</div>
              <div class="date">2024-03-01</div>
              <div class="time">12:34:56</div>
              <div class="b64">aGVsbG8=</div>
            </body></html>
            """;

    private static SelectFunction create(final String json, final List<Schema.Field> inputFields) {
        final JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
        final SelectFunction selectFunction = SelectFunction.of(jsonObject, inputFields);
        selectFunction.setup();
        return selectFunction;
    }

    private static List<Schema.Field> inputFields() {
        return List.of(Schema.Field.of("html", Schema.FieldType.STRING));
    }

    private static Map<String, Object> input() {
        final Map<String, Object> input = new HashMap<>();
        input.put("html", HTML);
        return input;
    }

    @Test
    public void testValidation() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"div\", \"group\": \"x\" }", inputFields()));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                create("{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"div\", \"trim\": \"yes\" }", inputFields()));
    }

    @Test
    public void testString() {
        final SelectFunction scrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"#title\", \"trim\": true }",
                inputFields());

        Assertions.assertEquals(Schema.Type.string, scrape.getOutputFieldType().getType());
        Assertions.assertEquals(1, scrape.getInputFields().size());
        Assertions.assertEquals("Hello World", scrape.apply(input(), TIMESTAMP));

        // no match returns null
        final SelectFunction noMatch = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"#nothing\" }",
                inputFields());
        Assertions.assertNull(noMatch.apply(input(), TIMESTAMP));

        // null input / missing field returns null
        Assertions.assertNull(scrape.apply(null, TIMESTAMP));
        Assertions.assertNull(scrape.apply(new HashMap<>(), TIMESTAMP));
        final Map<String, Object> nullValue = new HashMap<>();
        nullValue.put("html", null);
        Assertions.assertNull(scrape.apply(nullValue, TIMESTAMP));
    }

    @Test
    public void testRepeatedNumbers() {
        final SelectFunction scrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"li.item\", \"mode\": \"repeated\", \"type\": \"int64\" }",
                inputFields());

        Assertions.assertEquals(Schema.Type.array, scrape.getOutputFieldType().getType());
        Assertions.assertEquals(List.of(100L, 2000L), scrape.apply(input(), TIMESTAMP));

        // repeated with no matches returns empty list
        final SelectFunction noMatch = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \".nothing\", \"mode\": \"repeated\" }",
                inputFields());
        Assertions.assertEquals(List.of(), noMatch.apply(input(), TIMESTAMP));
    }

    @Test
    public void testAttribute() {
        final SelectFunction scrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"li.item\", \"attribute\": \"data-id\", \"type\": \"int32\" }",
                inputFields());
        Assertions.assertEquals(1, scrape.apply(input(), TIMESTAMP));
    }

    @Test
    public void testBaseUri() {
        final SelectFunction scrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"a\", \"attribute\": \"abs:href\", \"baseUri\": \"https://example.com/\" }",
                inputFields());
        Assertions.assertEquals("https://example.com/next", scrape.apply(input(), TIMESTAMP));
    }

    @Test
    public void testPattern() {
        final SelectFunction withGroup = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"span.price\", \"pattern\": \"price: ([0-9,]+)\", \"group\": 1, \"type\": \"int64\" }",
                inputFields());
        Assertions.assertEquals(1234L, withGroup.apply(input(), TIMESTAMP));

        final SelectFunction wholeMatch = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"span.price\", \"pattern\": \"[0-9,]+\" }",
                inputFields());
        Assertions.assertEquals("1,234", wholeMatch.apply(input(), TIMESTAMP));

        // pattern that does not match returns null
        final SelectFunction noMatch = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"span.price\", \"pattern\": \"XYZ\" }",
                inputFields());
        Assertions.assertNull(noMatch.apply(input(), TIMESTAMP));

        // group number larger than actual group count returns null
        final SelectFunction badGroup = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"span.price\", \"pattern\": \"([0-9,]+)\", \"group\": 5 }",
                inputFields());
        Assertions.assertNull(badGroup.apply(input(), TIMESTAMP));
    }

    @Test
    public void testOtherTypes() {
        final SelectFunction boolScrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"div.flag\", \"type\": \"bool\" }",
                inputFields());
        Assertions.assertEquals(true, boolScrape.apply(input(), TIMESTAMP));

        final SelectFunction floatScrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"div.prod span.val\", \"type\": \"float64\" }",
                inputFields());
        Assertions.assertEquals(1.5D, floatScrape.apply(input(), TIMESTAMP));

        final SelectFunction dateScrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"div.date\", \"type\": \"date\" }",
                inputFields());
        Assertions.assertEquals(
                Long.valueOf(LocalDate.of(2024, 3, 1).toEpochDay()).intValue(),
                dateScrape.apply(input(), TIMESTAMP));

        // TIME values are micros-of-day
        final SelectFunction timeScrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"div.time\", \"type\": \"time\" }",
                inputFields());
        Assertions.assertEquals((12L * 3600 + 34 * 60 + 56) * 1_000_000L, timeScrape.apply(input(), TIMESTAMP));

        final SelectFunction bytesScrape = create(
                "{ \"name\": \"s\", \"func\": \"scrape\", \"field\": \"html\", \"selector\": \"div.b64\", \"type\": \"bytes\" }",
                inputFields());
        final byte[] bytes = (byte[]) bytesScrape.apply(input(), TIMESTAMP);
        Assertions.assertEquals("hello", new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    public void testNestedFields() {
        final SelectFunction scrape = create("""
                        { "name": "s", "func": "scrape", "field": "html", "selector": "div.prod", "mode": "repeated",
                          "fields": [
                            { "name": "prodName", "selector": "span.name" },
                            { "name": "prodValue", "selector": "span.val", "type": "float64" },
                            { "name": "missing", "selector": "span.none" },
                            { "name": "values", "selector": "span.val", "mode": "repeated", "type": "float32" }
                          ] }
                        """,
                inputFields());

        final Schema.FieldType outputFieldType = scrape.getOutputFieldType();
        Assertions.assertEquals(Schema.Type.array, outputFieldType.getType());
        Assertions.assertEquals(Schema.Type.element, outputFieldType.getArrayValueType().getType());

        final Object output = scrape.apply(input(), TIMESTAMP);
        Assertions.assertInstanceOf(List.class, output);
        final List<?> list = (List<?>) output;
        Assertions.assertEquals(2, list.size());

        final Map<?, ?> first = (Map<?, ?>) list.get(0);
        Assertions.assertEquals("A", first.get("prodName"));
        Assertions.assertEquals(1.5D, first.get("prodValue"));
        Assertions.assertNull(first.get("missing"));
        Assertions.assertEquals(List.of(1.5F), first.get("values"));

        final Map<?, ?> second = (Map<?, ?>) list.get(1);
        Assertions.assertEquals("B", second.get("prodName"));
        Assertions.assertEquals(2.5D, second.get("prodValue"));
    }

}

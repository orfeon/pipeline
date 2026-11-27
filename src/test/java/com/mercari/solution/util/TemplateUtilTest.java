package com.mercari.solution.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TemplateUtilTest {

    @Test
    public void testUtilsDatetime() {
        final Long currentTimestampMicros = DateTimeUtil.toEpochMicroSecond(Instant.now());
        final Map<String, Object> values = new HashMap<>();

        final String text1 = "${utils.datetime.currentTimestamp()}";
        final String output1 = TemplateUtil.executeStrictTemplate(text1, values);
        Assertions.assertTrue( DateTimeUtil.toEpochMicroSecond(output1) - currentTimestampMicros < 10_000_000);

        final String text2 = "${utils.datetime.currentTimestamp(1)}";
        final String output2 = TemplateUtil.executeStrictTemplate(text2, values);
        Assertions.assertTrue( DateTimeUtil.toEpochMicroSecond(output2) - currentTimestampMicros < 10_000_000 + 1_000_000);

        final String text3 = "${utils.datetime.currentTimestamp(1, 'DAYS')}";
        final String output3 = TemplateUtil.executeStrictTemplate(text3, values);
        Assertions.assertTrue( DateTimeUtil.toEpochMicroSecond(output3) - currentTimestampMicros < 10_000_000 + 1_000_000L * 60 * 60 * 24);

        final String text4 = "${utils.datetime.currentTimestamp(1, 'DAYS', 'HOURS')}";
        final String output4 = TemplateUtil.executeStrictTemplate(text4, values);
    }

    @Test
    public void testNewBuiltinIsDisabled() {
        // ?new would instantiate freemarker.template.utility.Execute and run an OS command
        final String text = "<#assign ex=\"freemarker.template.utility.Execute\"?new()>${ex(\"echo hacked\")}";
        final RuntimeException e = Assertions.assertThrows(RuntimeException.class,
                () -> TemplateUtil.executeStrictTemplate(text, new HashMap<>()));
        Assertions.assertFalse(String.valueOf(e.getMessage()).contains("hacked\n"));
        Assertions.assertThrows(RuntimeException.class,
                () -> TemplateUtil.executeStrictTemplate("${\"freemarker.template.utility.ObjectConstructor\"?new()(\"java.lang.ProcessBuilder\", \"id\")}", new HashMap<>()));
    }

    @Test
    public void testStaticsAllowList() {
        Assertions.assertEquals("1",
                TemplateUtil.executeStrictTemplate("${statics[\"java.lang.Math\"].min(1, 2)}", new HashMap<>()));
        Assertions.assertEquals("2026-01-02",
                TemplateUtil.executeStrictTemplate("${statics[\"java.time.LocalDate\"].of(2026, 1, 2).toString()}", new HashMap<>()));
        // classes off the allow-list (process environment, files, runtime) are refused
        for(final String className : new String[]{"java.lang.System", "java.lang.Runtime", "java.nio.file.Files", "java.lang.Class"}) {
            Assertions.assertThrows(RuntimeException.class,
                    () -> TemplateUtil.executeStrictTemplate("${statics[\"" + className + "\"].toString()}", new HashMap<>()),
                    className);
        }
    }

    @Test
    public void testContainsTemplateSyntax() {
        Assertions.assertTrue(TemplateUtil.containsTemplateSyntax("${utils.gcp.accessToken()}"));
        Assertions.assertTrue(TemplateUtil.containsTemplateSyntax("x#{1}"));
        Assertions.assertTrue(TemplateUtil.containsTemplateSyntax("<#assign a=1>"));
        Assertions.assertTrue(TemplateUtil.containsTemplateSyntax("</#if>"));
        Assertions.assertTrue(TemplateUtil.containsTemplateSyntax("<@m/>"));
        Assertions.assertTrue(TemplateUtil.containsTemplateSyntax("[#assign a=1]"));
        Assertions.assertTrue(TemplateUtil.containsTemplateSyntax("[=a]"));
        Assertions.assertFalse(TemplateUtil.containsTemplateSyntax("2026-08-19"));
        Assertions.assertFalse(TemplateUtil.containsTemplateSyntax("a < b and c > d, $5, {x}, #tag, [1, 2]"));
        Assertions.assertFalse(TemplateUtil.containsTemplateSyntax(null));
    }

    @Test
    public void testContainsInputTemplate() {
        Assertions.assertTrue(TemplateUtil.containsInputTemplate("gs://bucket/${input.table}/data"));
        Assertions.assertTrue(TemplateUtil.containsInputTemplate("${ input.tag }"));
        // runtime templates and other namespaces must not activate the input namespace
        Assertions.assertFalse(TemplateUtil.containsInputTemplate("gs://bucket/${table}/data"));
        Assertions.assertFalse(TemplateUtil.containsInputTemplate("${args.table}"));
        Assertions.assertFalse(TemplateUtil.containsInputTemplate("${inputs.table}"));
        Assertions.assertFalse(TemplateUtil.containsInputTemplate("${input}"));
        Assertions.assertFalse(TemplateUtil.containsInputTemplate(null));
    }

    @Test
    public void testExecuteInputTemplate() {
        final Map<String, Object> input = Map.of(
                "name", "db.Users",
                "tag", "Users",
                "table", "Users");

        Assertions.assertEquals(
                "gs://bucket/export/Users/data",
                TemplateUtil.executeInputTemplate("gs://bucket/export/${input.table}/data", input));

        // FreeMarker builtins work inside a resolved expression
        Assertions.assertEquals(
                "users",
                TemplateUtil.executeInputTemplate("${input.table?lower_case}", input));

        // runtime template expressions in the same text survive untouched
        Assertions.assertEquals(
                "gs://bucket/Users/${category}/dt=${__timestamp}/data",
                TemplateUtil.executeInputTemplate("gs://bucket/${input.tag}/${category}/dt=${__timestamp}/data", input));
    }

}

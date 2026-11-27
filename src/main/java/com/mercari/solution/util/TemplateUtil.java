package com.mercari.solution.util;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.text.template.*;
import freemarker.core.Environment;
import freemarker.core.TemplateClassResolver;
import freemarker.ext.beans.BeansWrapperBuilder;
import freemarker.template.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

public class TemplateUtil {

    private static final Map<String, Object> UTILS = Map.of(
            "string", new StringFunctions(),
            "datetime", new DateTimeFunctions(),
            "bigtable", new BigtableFunctions(),
            "gcp", new GcpFunctions(),
            "oauth", new OAuthFunctions(),
            "secrets", new SecretFunctions(),
            "json", new JsonFunctions()
    );

    /**
     * The classes whose static members {@code statics["<class>"]} may reach in a template. Everything else is
     * refused: an unrestricted static model reaches {@code java.lang.System}, {@code java.lang.Runtime},
     * {@code java.nio.file.Files} and friends, which lets anyone who can supply template text (a config, a
     * template arg) read the process environment and files or run commands.
     */
    static final Set<String> ALLOWED_STATIC_CLASSES = Set.of(
            "java.lang.Math",
            "java.lang.StrictMath",
            "java.lang.Integer",
            "java.lang.Long",
            "java.lang.Double",
            "java.lang.Float",
            "java.lang.Short",
            "java.lang.Byte",
            "java.lang.Boolean",
            "java.lang.Character",
            "java.lang.String",
            "java.math.BigDecimal",
            "java.math.BigInteger",
            "java.util.Objects",
            "java.util.UUID",
            "java.time.Instant",
            "java.time.LocalDate",
            "java.time.LocalDateTime",
            "java.time.LocalTime",
            "java.time.OffsetDateTime",
            "java.time.ZonedDateTime",
            "java.time.ZoneId",
            "java.time.ZoneOffset",
            "java.time.Duration",
            "java.time.Period",
            "java.time.format.DateTimeFormatter",
            "java.time.temporal.ChronoUnit");

    private static final TemplateHashModel STATICS = new AllowedStaticModels(
            new BeansWrapperBuilder(Configuration.VERSION_2_3_34).build().getStaticModels());

    /**
     * Closes the FreeMarker features that turn template text into code execution: {@code ?new} (which
     * instantiates any {@code TemplateModel} class, e.g. {@code freemarker.template.utility.Execute}) and
     * {@code ?api}. Every {@link Configuration} that evaluates text a user wrote goes through here.
     */
    public static void harden(final Configuration templateConfig) {
        templateConfig.setNewBuiltinClassResolver(TemplateClassResolver.ALLOWS_NOTHING_RESOLVER);
        templateConfig.setAPIBuiltinEnabled(false);
    }

    public static Template createSafeTemplate(final String name, final String template) {

        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_34);
        harden(templateConfig);
        templateConfig.setNumberFormat("computer");
        templateConfig.setTemplateExceptionHandler(new ImputeSameVariablesTemplateExceptionHandler());
        templateConfig.setLogTemplateExceptions(false);
        templateConfig.setSharedVariable("statics", STATICS);
        templateConfig.setURLEscapingCharset("UTF-8"); // enables the ?url built-in
        try {
            templateConfig.setSharedVariable("utils", UTILS);
            return new Template(name, new StringReader(template), templateConfig);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static Template createStrictTemplate(final String name, final String template) {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_34);
        harden(templateConfig);
        templateConfig.setNumberFormat("computer");
        templateConfig.setSharedVariable("statics", STATICS);
        templateConfig.setURLEscapingCharset("UTF-8"); // enables the ?url built-in
        //templateConfig.setObjectWrapper(new CSVWrapper(Configuration.VERSION_2_3_30));
        try {
            templateConfig.setSharedVariable("utils", UTILS);
            return new Template(name, new StringReader(template), templateConfig);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static String executeStrictTemplate(final String templateText, final Map<?, ?> data) {
        final Template template = createStrictTemplate("template", templateText);
        return executeStrictTemplate(template, data);
    }

    public static String executeStrictTemplate(final Template template, final Map<?, ?> data) {
        try(final StringWriter writer = new StringWriter()) {
            template.process(data, writer);
            return writer.toString();
        } catch (IOException | TemplateException e) {
            throw new RuntimeException("Failed to process template: " + template + ", for data: " + data + ", cause: " + e.getMessage(), e);
        }
    }

    /** Expands the text as a strict template when it contains template syntax; null and plain text are returned as they are. */
    public static String executeStrictTemplateIfNeeded(final String text, final Map<?, ?> data) {
        if(text == null || !isTemplateText(text)) {
            return text;
        }
        return executeStrictTemplate(text, data);
    }

    public static boolean isTemplateText(final String text) {
        if(text == null) {
            return false;
        }
        return text.contains("${") && text.contains("}");
    }

    // FreeMarker interpolations (${...}, #{...}, [=...]) and directive / macro tags in angle- or square-bracket syntax
    private static final java.util.regex.Pattern TEMPLATE_SYNTAX_PATTERN =
            java.util.regex.Pattern.compile("\\$\\{|#\\{|\\[=|</?[#@]|\\[/?[#@]");

    /**
     * True when the text holds anything FreeMarker would evaluate. Values a caller supplies as plain data (e.g. the
     * template args of an HTTP request) must not, because they are spliced into config text that is evaluated as a
     * template — at load time and again by modules' per-record templates.
     */
    public static boolean containsTemplateSyntax(final String text) {
        if(text == null) {
            return false;
        }
        return TEMPLATE_SYNTAX_PATTERN.matcher(text).find();
    }

    // Pipeline-assembly-time template expressions over the reserved `input` namespace,
    // e.g. ${input.table}. Only these expressions are resolved when a module declares
    // wildcard inputs; every other ${...} is left for runtime per-element evaluation.
    private static final java.util.regex.Pattern INPUT_TEMPLATE_PATTERN =
            java.util.regex.Pattern.compile("\\$\\{\\s*input\\.[^}]+}");

    public static boolean containsInputTemplate(final String text) {
        if(text == null) {
            return false;
        }
        return INPUT_TEMPLATE_PATTERN.matcher(text).find();
    }

    public static String executeInputTemplate(final String text, final Map<String, Object> input) {
        final java.util.regex.Matcher matcher = INPUT_TEMPLATE_PATTERN.matcher(text);
        final StringBuilder sb = new StringBuilder();
        while(matcher.find()) {
            final String resolved = executeStrictTemplate(matcher.group(), Map.of("input", input));
            matcher.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(resolved));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static List<String> extractTemplateArgs(final String text, final Schema inputSchema) {
        return extractTemplateArgs(text, inputSchema.getFields());
    }

    public static List<String> extractTemplateArgs(final String text, final List<Schema.Field> fields) {
        final List<String> args = new ArrayList<>();
        if(!isTemplateText(text)) {
            return args;
        }
        for(final com.mercari.solution.module.Schema.Field field : fields) {
            if(text.contains(field.getName()) && !args.contains(field.getName())) {
                args.add(field.getName());
            }
        }
        return args;
    }

    public static void setContextVariables(
            final DoFn.ProcessContext c,
            final Map<String, Object> values) {

        final Map<String, Object> contextValues = new HashMap<>();
        contextValues.put("timestamp", DateTimeUtil.toInstant(c.timestamp().getMillis() * 1000L));
        values.put("context", contextValues);
    }

    public static void setFunctions(final Map<String, Object> values) {
        setFunctions(values, "__");
    }

    public static void setFunctions(final Map<String, Object> values, final String prefix) {
        values.put(prefix + "StringUtils", new StringFunctions());
        values.put(prefix + "DateTimeUtils", new DateTimeFunctions());
    }

    /** {@code statics} restricted to {@link #ALLOWED_STATIC_CLASSES}. */
    static final class AllowedStaticModels implements TemplateHashModel {

        private final TemplateHashModel delegate;

        AllowedStaticModels(final TemplateHashModel delegate) {
            this.delegate = delegate;
        }

        @Override
        public TemplateModel get(final String key) throws TemplateModelException {
            if(!ALLOWED_STATIC_CLASSES.contains(key)) {
                throw new TemplateModelException("statics[\"" + key + "\"] is not allowed. Allowed classes: "
                        + ALLOWED_STATIC_CLASSES.stream().sorted().collect(Collectors.joining(", ")));
            }
            return delegate.get(key);
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

    }

    static class ImputeSameVariablesTemplateExceptionHandler implements TemplateExceptionHandler {

        @Override
        public void handleTemplateException(TemplateException te, Environment env, java.io.Writer out) {
            try {
                if(te instanceof TemplateModelException e) {
                    final List<String> lines = env.getCurrentTemplate().toString().lines().collect(Collectors.toList());
                    final String line = lines.get(te.getLineNumber() - 1);
                    final String content = line.substring(te.getColumnNumber()-1, te.getEndColumnNumber());
                    out.write(content);
                    return;
                }
                if(te.getBlamedExpressionString() == null) {
                    throw new IllegalArgumentException(te);
                }
                final List<String> lines = env.getCurrentTemplate().toString().lines().collect(Collectors.toList());
                final String line = lines.get(te.getLineNumber() - 1);
                final String prefix = line.substring(te.getColumnNumber()-2, te.getColumnNumber()-1);
                final String suffix = line.substring(te.getEndColumnNumber(), te.getEndColumnNumber()+1);
                if("{".equals(prefix) && "}".equals(suffix)) {
                    out.write("${" + te.getBlamedExpressionString() + "}");
                } else if("{".equals(prefix) && line.contains("}")) {
                    final int start = te.getColumnNumber()-1;
                    final String target = line.substring(start, line.indexOf("}", start));
                    out.write("${" + target.replaceAll("\"", "'") + "}");
                } else {
                    out.write("${" + te.getBlamedExpressionString() + "}");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class CSVWrapper extends DefaultObjectWrapper {

        CSVWrapper(Version incompatibleImprovements) {
            super(incompatibleImprovements);
        }

        @Override
        protected TemplateModel handleUnknownType(Object obj) throws TemplateModelException {
            if (obj instanceof CSVRecord) {
                return new CSVRecordModel((CSVRecord) obj);
            }
            return super.handleUnknownType(obj);
        }

        public class CSVRecordModel implements TemplateHashModel {
            private final CSVRecord csvRecord;

            public CSVRecordModel(CSVRecord csvRecord) {
                this.csvRecord = csvRecord;
            }

            @Override
            public TemplateModel get(String key) throws TemplateModelException {
                return wrap(csvRecord.get(key));
            }

            @Override
            public boolean isEmpty() {
                return csvRecord.size() == 0;
            }

        }

    }

}

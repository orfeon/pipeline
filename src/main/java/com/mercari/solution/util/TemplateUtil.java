package com.mercari.solution.util;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.domain.text.template.*;
import freemarker.core.Environment;
import freemarker.ext.beans.BeansWrapper;
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
            "oauth", new OAuthFunctions()
    );

    public static Template createSafeTemplate(final String name, final String template) {

        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_34);
        templateConfig.setNumberFormat("computer");
        templateConfig.setTemplateExceptionHandler(new ImputeSameVariablesTemplateExceptionHandler());
        templateConfig.setLogTemplateExceptions(false);
        templateConfig.setSharedVariable("statics", BeansWrapper.getDefaultInstance().getStaticModels());
        try {
            templateConfig.setSharedVariable("utils", UTILS);
            return new Template(name, new StringReader(template), templateConfig);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static Template createStrictTemplate(final String name, final String template) {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_34);
        templateConfig.setNumberFormat("computer");
        templateConfig.setSharedVariable("statics", BeansWrapper.getDefaultInstance().getStaticModels());
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

    public static boolean isTemplateText(final String text) {
        if(text == null) {
            return false;
        }
        return text.contains("${") && text.contains("}");
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

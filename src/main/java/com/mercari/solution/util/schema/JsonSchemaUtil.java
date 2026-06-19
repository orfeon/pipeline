package com.mercari.solution.util.schema;

import com.networknt.schema.*;
import com.networknt.schema.Error;
import com.networknt.schema.regex.JoniRegularExpressionFactory;
import com.networknt.schema.resource.SchemaIdResolvers;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonSchemaUtil {

    public static Schema getSchema(String jsonSchema) {
        final SchemaRegistryConfig schemaRegistryConfig = SchemaRegistryConfig
                .builder()
                .regularExpressionFactory(JoniRegularExpressionFactory.getInstance())
                .build();
        final SchemaRegistry schemaRegistry = SchemaRegistry
                .withDefaultDialect(
                        SpecificationVersion.DRAFT_2020_12,
                        builder -> builder
                                .schemaRegistryConfig(schemaRegistryConfig)
                                .schemaIdResolvers(SchemaIdResolvers.Builder::build));
        try {
            return schemaRegistry.getSchema(jsonSchema);
        } catch (Throwable e) {
            return null;
        }
    }

    public static Schema getSchema(final SchemaRegistry schemaRegistry, final String id) {
        final String schemaJson = "{\"$ref\": \"" + id + "\"}";
        return createSchema(schemaRegistry, schemaJson);
    }

    public static Schema createSchema(final SchemaRegistry schemaRegistry, final String jsonSchema) {
        try {
            return schemaRegistry.getSchema(jsonSchema);
        } catch (Throwable e) {
            return null;
        }
    }

    public static SchemaRegistry createSchemaRegistry(final String directory) {
        final Map<String,String> schemaMappings = loadJsonSchemas(directory);
        return createSchemaRegistry(schemaMappings);
    }

    public static SchemaRegistry createSchemaRegistry(final Map<String,String> schemaMappings) {
        final SchemaRegistryConfig schemaRegistryConfig = SchemaRegistryConfig
                .builder()
                .regularExpressionFactory(JoniRegularExpressionFactory.getInstance())
                .build();
        return SchemaRegistry
                .withDefaultDialect(
                        SpecificationVersion.DRAFT_2020_12,
                        builder -> builder
                                .schemas(schemaMappings)
                                .schemaCacheEnabled(true)
                                .schemaRegistryConfig(schemaRegistryConfig)
                                .schemaIdResolvers(SchemaIdResolvers.Builder::build));
    }

    public static List<ValidateError> validate(Schema schema, String json) {
        final List<Error> errors = schema.validate(
                json,
                InputFormat.JSON,
                executionContext -> executionContext.executionConfig(executionConfig -> executionConfig.formatAssertionsEnabled(true)));
        final List<ValidateError> validateErrors = new ArrayList<>();
        for(final Error error : errors) {
            System.out.println(error.getDetails() + " : " + error.getSchemaLocation().toString() + " : " + error.getMessageKey());
            final ValidateError validateError = new ValidateError(error.getMessage(), error.getProperty(), error.getKeyword());
            validateErrors.add(validateError);
        }
        return validateErrors;
    }

    public static class ValidateError {
        final public String message;
        final public String property;
        final public String keyword;

        public ValidateError(String message, String property, String keyword) {
            this.message = message;
            this.property = property;
            this.keyword = keyword;
        }

        @Override
        public String toString() {
            return "";
        }

    }

    public static Map<String, String> loadJsonSchemas(String basePath) {
        try (final Stream<Path> paths = Files.walk(Path.of(basePath))) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".json"))
                    .collect(Collectors
                            .toMap(p -> generateId(basePath, p), JsonSchemaUtil::readFile));
        } catch (final Throwable e) {
            throw new RuntimeException("Failed to read JSON-Schema files for dir: " + basePath, e);
        }
    }

    private static String generateId(String baseDir, Path path) {
        return "https://mercari.com" + path
                .toFile()
                .getPath()
                .replaceFirst(baseDir, "")
                .replaceFirst("\\.json", "");
    }

    private static String readFile(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getResourceFileAsString(final String path) throws IOException {
        final URL uri = ClassLoader.getSystemResource(path);
        final File file = new File(uri.getPath());
        return getFileAsString(file);
    }

    private static String getFileAsString(final File file) throws IOException {
        try(final FileInputStream fis = new FileInputStream(file);
            final InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            final BufferedReader br = new BufferedReader(isr)) {
            final StringBuilder sb = new StringBuilder();
            br.lines().forEach(line -> sb.append(line).append('\n'));
            return sb.toString();
        }
    }

}

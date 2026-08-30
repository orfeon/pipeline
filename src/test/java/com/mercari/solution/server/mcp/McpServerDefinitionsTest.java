package com.mercari.solution.server.mcp;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.server.mcp.resource.DocsResources;
import com.mercari.solution.server.mcp.tool.Tool;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * What the MCP server publishes must be valid for the SDK: every tool's annotation schemas parse into
 * an object schema with properties (an unset input schema hides every argument from clients), and every
 * docs resource carries the required name.
 */
public class McpServerDefinitionsTest {

    @Test
    public void testEveryToolPublishesItsInputSchema() throws Exception {
        final ClassPath classPath = ClassPath.from(Tool.class.getClassLoader());
        final Method toolSchema = Tool.class.getDeclaredMethod("toolSchema", Tool.Module.class);
        toolSchema.setAccessible(true);
        int count = 0;
        for (final ClassPath.ClassInfo info : classPath.getTopLevelClassesRecursive(Tool.class.getPackageName())) {
            final Class<?> clazz = info.load();
            if (!Tool.class.isAssignableFrom(clazz) || !clazz.isAnnotationPresent(Tool.Module.class)) continue;
            final Tool.Module properties = clazz.getAnnotation(Tool.Module.class);
            final McpSchema.Tool tool = (McpSchema.Tool) toolSchema.invoke(null, properties);
            Assertions.assertEquals(properties.name(), tool.name());
            final Map<String, Object> schema = tool.inputSchema();
            Assertions.assertNotNull(schema, properties.name() + " input schema");
            Assertions.assertEquals("object", schema.get("type"), properties.name() + " input schema type");
            Assertions.assertTrue(schema.get("properties") instanceof Map, properties.name() + " input schema properties");
            @SuppressWarnings("unchecked")
            final Map<String, Object> props = (Map<String, Object>) schema.get("properties");
            for (final Map.Entry<String, Object> p : props.entrySet()) {
                Assertions.assertTrue(p.getValue() instanceof Map, properties.name() + "." + p.getKey() + " must be a schema object");
            }
            if (schema.get("required") instanceof List<?> required) {
                for (final Object r : required) {
                    Assertions.assertTrue(props.containsKey(String.valueOf(r)), properties.name() + " requires undeclared parameter " + r);
                }
            }
            count++;
        }
        Assertions.assertTrue(count >= 15, "tools found: " + count);
    }

    @Test
    public void testDocsResourcesHaveNames() throws IOException {
        final List<McpServerFeatures.SyncResourceSpecification> resources = new DocsResources().sync(null);
        Assertions.assertTrue(resources.size() > 20, "docs resources: " + resources.size());
        for (final McpServerFeatures.SyncResourceSpecification r : resources) {
            Assertions.assertNotNull(r.resource().name(), r.resource().uri());
            Assertions.assertFalse(r.resource().name().isBlank(), r.resource().uri());
            Assertions.assertTrue(r.resource().uri().startsWith("docs://"), r.resource().uri());
        }
        Assertions.assertTrue(resources.stream().anyMatch(r -> r.resource().uri().equals("docs://module/transform/feature.md")));
        Assertions.assertTrue(resources.stream().anyMatch(r -> r.resource().uri().equals("docs://deploy/server.md")));
    }

}

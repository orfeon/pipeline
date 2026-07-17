package com.mercari.solution.server.code;

import com.google.common.reflect.ClassPath;
import com.mercari.solution.module.FailureSink;
import com.mercari.solution.module.Sink;
import com.mercari.solution.module.Source;
import com.mercari.solution.module.Transform;
import jakarta.servlet.ServletContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

/**
 * Read-only access to the framework's own source tree for the agent/MCP code-reading tools.
 *
 * The sources are bundled into the server WAR at WEB-INF/sources/src/main/java (never served
 * over HTTP). Outside a servlet container (tests, jetty:run, MCP standalone) the root falls
 * back to the MERCARI_PIPELINE_SOURCES_PATH env var or the working directory's src/main/java.
 */
public class CodeRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CodeRepository.class);

    public static final String SOURCES_PATH_ENV = "MERCARI_PIPELINE_SOURCES_PATH";

    static final String SOURCE_DIR = "src/main/java";
    static final String SOURCE_PREFIX = SOURCE_DIR + "/";

    private static final int MAX_SEARCH_RESULTS = 100;
    private static final int MAX_SEARCH_LINE_LENGTH = 240;
    private static final int MAX_READ_LINES = 500;
    private static final int MAX_STACK_FRAMES = 12;
    private static final int STACK_FRAME_CONTEXT_LINES = 14;

    // "at com.mercari.solution.module.sink.StorageSink.write(StorageSink.java:123)"
    private static final Pattern STACK_FRAME = Pattern
            .compile("at\\s+([\\w$.]+)\\.[\\w$<>]+\\((\\w+\\.java):(\\d+)\\)");

    public enum ModuleType {
        source,
        transform,
        sink,
        failure
    }

    private static volatile Path sourceRoot;
    private static volatile List<String> fileCache;
    private static final Map<ModuleType, Map<String, String>> moduleFileCache = new EnumMap<>(ModuleType.class);

    /** Resolve the source root from the servlet container (WAR runtime). Safe to call more than once. */
    public static void init(final ServletContext servletContext) {
        if (sourceRoot != null) {
            return;
        }
        if (servletContext != null) {
            final String realPath = servletContext.getRealPath("/WEB-INF/sources");
            if (realPath != null) {
                final Path candidate = Paths.get(realPath);
                if (Files.isDirectory(candidate.resolve(SOURCE_DIR))) {
                    sourceRoot = candidate;
                    LOG.info("CodeRepository source root: {}", candidate);
                    return;
                }
            }
        }
        resolveFallbackRoot();
    }

    private static synchronized void resolveFallbackRoot() {
        if (sourceRoot != null) {
            return;
        }
        final String env = System.getenv(SOURCES_PATH_ENV);
        if (env != null && !env.isBlank()) {
            final Path candidate = Paths.get(env.trim());
            if (Files.isDirectory(candidate.resolve(SOURCE_DIR))) {
                sourceRoot = candidate;
                LOG.info("CodeRepository source root (env): {}", candidate);
                return;
            }
            LOG.warn("{} does not contain {}: {}", SOURCES_PATH_ENV, SOURCE_DIR, candidate);
        }
        final Path workingDir = Paths.get("").toAbsolutePath();
        if (Files.isDirectory(workingDir.resolve(SOURCE_DIR))) {
            sourceRoot = workingDir;
            LOG.info("CodeRepository source root (working dir): {}", workingDir);
        }
    }

    private static Path root() {
        if (sourceRoot == null) {
            resolveFallbackRoot();
        }
        return sourceRoot;
    }

    private static String rootUnavailableMessage() {
        return "Source code is not available: the bundled source tree (WEB-INF/sources) was not found "
                + "and no fallback (" + SOURCES_PATH_ENV + " or working-directory " + SOURCE_DIR + ") exists.";
    }

    public static String search(final String pattern, final String pathFilter) {
        if (pattern == null || pattern.isBlank()) {
            return "Error: pattern is required.";
        }
        final Path root = root();
        if (root == null) {
            return rootUnavailableMessage();
        }
        Pattern regex;
        try {
            regex = Pattern.compile(pattern);
        } catch (final PatternSyntaxException e) {
            regex = Pattern.compile(Pattern.quote(pattern));
        }

        final StringBuilder result = new StringBuilder();
        int matchCount = 0;
        boolean truncated = false;
        for (final String file : listFiles(root)) {
            if (pathFilter != null && !pathFilter.isBlank()
                    && !file.toLowerCase().contains(pathFilter.trim().toLowerCase().replace('\\', '/'))) {
                continue;
            }
            final List<String> lines;
            try {
                lines = Files.readAllLines(root.resolve(file), StandardCharsets.UTF_8);
            } catch (final IOException e) {
                continue;
            }
            for (int i = 0; i < lines.size(); i++) {
                if (!regex.matcher(lines.get(i)).find()) {
                    continue;
                }
                if (matchCount >= MAX_SEARCH_RESULTS) {
                    truncated = true;
                    break;
                }
                result.append(file).append(":").append(i + 1).append(": ")
                        .append(truncate(lines.get(i).strip(), MAX_SEARCH_LINE_LENGTH)).append("\n");
                matchCount++;
            }
            if (truncated) {
                break;
            }
        }
        if (matchCount == 0) {
            return "No matches found for pattern: " + pattern
                    + (pathFilter == null || pathFilter.isBlank() ? "" : " (pathFilter: " + pathFilter + ")");
        }
        if (truncated) {
            result.append("... (more than ").append(MAX_SEARCH_RESULTS)
                    .append(" matches; narrow the pattern or add a pathFilter)\n");
        }
        return result.toString();
    }

    public static String read(final String path, final Integer startLine, final Integer endLine) {
        final Path root = root();
        if (root == null) {
            return rootUnavailableMessage();
        }
        final String normalized = normalizeSourcePath(path);
        if (normalized == null) {
            return "Error: invalid source path '" + path + "'. Specify a .java path such as "
                    + "'src/main/java/com/mercari/solution/MPipeline.java'.";
        }
        final Path file = root.resolve(normalized).normalize();
        if (!file.startsWith(root)) {
            return "Error: invalid source path '" + path + "'.";
        }
        if (!Files.isRegularFile(file)) {
            return "Source file not found: '" + normalized + "'. Use searchCode to locate files.";
        }
        final List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            return "Failed to read source file '" + normalized + "': " + e.getMessage();
        }

        int start = Optional.ofNullable(startLine).orElse(1);
        start = Math.max(1, Math.min(start, lines.size()));
        int end = Optional.ofNullable(endLine).orElse(start + MAX_READ_LINES - 1);
        end = Math.max(start, Math.min(end, lines.size()));
        if (end - start + 1 > MAX_READ_LINES) {
            end = start + MAX_READ_LINES - 1;
        }

        final StringBuilder result = new StringBuilder();
        result.append("## ").append(normalized)
                .append(" (lines ").append(start).append("-").append(end)
                .append(" of ").append(lines.size()).append(")\n");
        appendNumberedLines(result, lines, start, end, -1);
        if (end < lines.size()) {
            result.append("... (").append(lines.size() - end)
                    .append(" more lines; specify startLine/endLine to read further)\n");
        }
        return result.toString();
    }

    public static String resolveStackTrace(final String stackTrace) {
        if (stackTrace == null || stackTrace.isBlank()) {
            return "Error: stackTrace text is required.";
        }
        final Path root = root();
        if (root == null) {
            return rootUnavailableMessage();
        }

        final LinkedHashSet<String> frames = new LinkedHashSet<>();
        final Matcher matcher = STACK_FRAME.matcher(stackTrace);
        while (matcher.find() && frames.size() < MAX_STACK_FRAMES) {
            final String classFq = matcher.group(1);
            final int lastDot = classFq.lastIndexOf('.');
            if (lastDot <= 0) {
                continue;
            }
            final String packagePath = classFq.substring(0, lastDot).replace('.', '/');
            final String relPath = SOURCE_PREFIX + packagePath + "/" + matcher.group(2);
            if (Files.isRegularFile(root.resolve(relPath))) {
                frames.add(relPath + ":" + matcher.group(3));
            }
        }
        if (frames.isEmpty()) {
            return "No stack frames could be resolved to bundled sources. Either the trace contains no "
                    + "com.mercari.solution frames, or the frames do not match this build's source tree. "
                    + "Class names in the message may still be searchable with searchCode.";
        }

        final StringBuilder result = new StringBuilder();
        for (final String frame : frames) {
            final int sep = frame.lastIndexOf(':');
            final String relPath = frame.substring(0, sep);
            final int lineNumber = Integer.parseInt(frame.substring(sep + 1));
            result.append("### ").append(frame).append("\n");
            final List<String> lines;
            try {
                lines = Files.readAllLines(root.resolve(relPath), StandardCharsets.UTF_8);
            } catch (final IOException e) {
                result.append("(failed to read source: ").append(e.getMessage()).append(")\n\n");
                continue;
            }
            final int start = Math.max(1, lineNumber - STACK_FRAME_CONTEXT_LINES);
            final int end = Math.min(lines.size(), lineNumber + STACK_FRAME_CONTEXT_LINES);
            appendNumberedLines(result, lines, start, end, lineNumber);
            result.append("\n");
        }
        return result.toString();
    }

    public static String findModuleSource(final ModuleType type, final String name) {
        if (type == null) {
            return "Error: type is required. Specify one of: source, transform, sink, failure.";
        }
        if (name == null || name.isBlank()) {
            return "Error: name is required. Specify the module name as used in the config's 'module' field.";
        }
        final Map<String, String> files = moduleFiles(type);
        final String relPath = files.get(name.trim().toLowerCase());
        if (relPath == null) {
            return String.format("No %s module named '%s'. Available %s modules: %s",
                    type.name(), name, type.name(), String.join(", ", files.keySet()));
        }
        final Path root = root();
        String lineInfo = "";
        if (root != null && Files.isRegularFile(root.resolve(relPath))) {
            try (final Stream<String> lines = Files.lines(root.resolve(relPath), StandardCharsets.UTF_8)) {
                lineInfo = " (" + lines.count() + " lines)";
            } catch (final IOException e) {
                // line count is informational only
            }
        }
        return String.format("%s module '%s' is implemented in: %s%s%nUse readSource to read it.",
                type.name(), name, relPath, lineInfo);
    }

    /**
     * Normalize a source path: resolves '.'/'..' segments, rejects escapes from the source root,
     * requires a .java file, and prepends src/main/java/ when given a package-style path.
     */
    static String normalizeSourcePath(final String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String p = path.trim().replace('\\', '/');
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        final Deque<String> segments = new ArrayDeque<>();
        for (final String segment : p.split("/")) {
            switch (segment) {
                case "", "." -> { }
                case ".." -> {
                    if (segments.isEmpty()) {
                        return null;
                    }
                    segments.removeLast();
                }
                default -> segments.addLast(segment);
            }
        }
        if (segments.isEmpty()) {
            return null;
        }
        String normalized = String.join("/", segments);
        if (!normalized.endsWith(".java")) {
            return null;
        }
        if (!normalized.startsWith(SOURCE_PREFIX)) {
            normalized = SOURCE_PREFIX + normalized;
        }
        return normalized;
    }

    private static synchronized List<String> listFiles(final Path root) {
        if (fileCache != null) {
            return fileCache;
        }
        final List<String> files = new ArrayList<>();
        try (final Stream<Path> stream = Files.walk(root.resolve(SOURCE_DIR))) {
            stream.filter(Files::isRegularFile)
                    .filter(f -> f.getFileName().toString().endsWith(".java"))
                    .forEach(f -> files.add(root.relativize(f).toString().replace('\\', '/')));
        } catch (final IOException e) {
            LOG.warn("Failed to list source files under {}", root, e);
            return files;
        }
        files.sort(String::compareTo);
        fileCache = files;
        return files;
    }

    private static synchronized Map<String, String> moduleFiles(final ModuleType type) {
        final Map<String, String> cached = moduleFileCache.get(type);
        if (cached != null) {
            return cached;
        }
        final Map<String, String> files = new TreeMap<>();
        final String packageName = "com.mercari.solution.module." + type.name();
        try {
            final ClassPath classPath = ClassPath.from(CodeRepository.class.getClassLoader());
            for (final ClassPath.ClassInfo classInfo : classPath.getTopLevelClassesRecursive(packageName)) {
                final Class<?> clazz = classInfo.load();
                final String moduleName = moduleName(type, clazz);
                if (moduleName != null) {
                    files.put(moduleName, SOURCE_PREFIX + clazz.getName().replace('.', '/') + ".java");
                }
            }
        } catch (final IOException e) {
            LOG.warn("Failed to scan module package: {}", packageName, e);
        }
        moduleFileCache.put(type, files);
        return files;
    }

    private static String moduleName(final ModuleType type, final Class<?> clazz) {
        return switch (type) {
            case source -> Optional.ofNullable(clazz.getAnnotation(Source.Module.class))
                    .map(Source.Module::name).orElse(null);
            case transform -> Optional.ofNullable(clazz.getAnnotation(Transform.Module.class))
                    .map(Transform.Module::name).orElse(null);
            case sink -> Optional.ofNullable(clazz.getAnnotation(Sink.Module.class))
                    .map(Sink.Module::name).orElse(null);
            case failure -> Optional.ofNullable(clazz.getAnnotation(FailureSink.Module.class))
                    .map(FailureSink.Module::name).orElse(null);
        };
    }

    private static void appendNumberedLines(
            final StringBuilder result,
            final List<String> lines,
            final int start,
            final int end,
            final int markedLine) {

        for (int i = start; i <= end; i++) {
            result.append(i == markedLine ? ">" : " ")
                    .append(String.format("%5d  ", i))
                    .append(lines.get(i - 1)).append("\n");
        }
    }

    private static String truncate(final String text, final int maxLength) {
        if (text.length() <= maxLength) {
            return text;
        }
        return text.substring(0, maxLength) + "...";
    }

}

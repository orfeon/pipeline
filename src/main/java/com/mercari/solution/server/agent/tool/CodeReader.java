package com.mercari.solution.server.agent.tool;

import com.mercari.solution.server.code.CodeRepository;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

public class CodeReader {

    @Tool("""
        Search the framework's own Java source code with a regular expression.
        Returns matching lines as 'path:line: text'. Use this to locate the implementation of a
        feature, a class name, an error message string, or a config parameter when documentation
        is not enough — e.g. searching the exact text of an error message finds where it is thrown.
    """)
    public String searchCode(
            @P(name = "pattern", description = "Java regular expression to search for. Invalid regex is searched as a literal string.") String pattern,
            @P(name = "pathFilter", description = "Optional substring filter on file paths, e.g. 'module/sink' or 'StorageSink'.", required = false) String pathFilter) {
        return CodeRepository.search(pattern, pathFilter);
    }

    @Tool("""
        Read a slice of a framework source file with line numbers.
        Paths are those returned by searchCode / resolveStackTrace / findModuleSource,
        e.g. 'src/main/java/com/mercari/solution/module/sink/StorageSink.java'.
        At most 500 lines are returned per call; use startLine/endLine to read further.
    """)
    public String readSource(
            @P(name = "path", description = "Source file path, e.g. 'src/main/java/com/mercari/solution/MPipeline.java'.") String path,
            @P(name = "startLine", description = "First line to read (1-based). Defaults to 1.", required = false) Integer startLine,
            @P(name = "endLine", description = "Last line to read (inclusive). Defaults to startLine+499.", required = false) Integer endLine) {
        return CodeRepository.read(path, startLine, endLine);
    }

    @Tool("""
        Resolve a Java stack trace against the framework's source code.
        Paste the stack trace text (e.g. from a failed run or a Dataflow error log) and this returns,
        for every com.mercari.solution frame, the source file with the surrounding lines and the
        failing line marked. Use this FIRST when diagnosing an error that includes a stack trace.
    """)
    public String resolveStackTrace(
            @P(name = "stackTrace", description = "The stack trace text, including 'at package.Class.method(File.java:line)' frames.") String stackTrace) {
        return CodeRepository.resolveStackTrace(stackTrace);
    }

    @Tool("""
        Find the source file implementing a pipeline module by its config 'module' name.
        Use this to jump from a module in a pipeline config (e.g. sink 'storage') straight to its
        implementation, then read it with readSource.
    """)
    public String findModuleSource(
            @P(name = "type", description = "Module type: source, transform, sink, or failure.") CodeRepository.ModuleType type,
            @P(name = "name", description = "Module name as written in the config's 'module' field (e.g. 'bigquery', 'select', 'storage').") String name) {
        return CodeRepository.findModuleSource(type, name);
    }

    public static CodeReader create() {
        return new CodeReader();
    }

}

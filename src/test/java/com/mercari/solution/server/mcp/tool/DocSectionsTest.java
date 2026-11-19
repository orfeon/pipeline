package com.mercari.solution.server.mcp.tool;

import io.modelcontextprotocol.modern.McpSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class DocSectionsTest {

    private static final String DOC = """
            ---
            title: Sample Module
            description: "What the sample does."
            ---

            # Sample Module

            Introduction.

            ## Parameters

            The table.

            ### Windows (`window`)

            About windows.

            ```yaml
            # not a heading
            ## neither is this
            ```

            ### Examples

            Parameter examples.

            ## Examples

            Whole examples.

            ## Notes ##

            Last words.
            """;

    private static String text(final McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().getFirst()).text();
    }

    @Test
    public void testSectionsAndIds() {
        final DocSections doc = DocSections.parse(DOC);
        Assertions.assertEquals("Sample Module", doc.title());
        Assertions.assertEquals("What the sample does.", doc.description());
        // headings inside the fenced block are not sections; a slug used twice is addressed by its path
        Assertions.assertEquals(List.of("parameters", "windows-window", "parameters/examples", "examples", "notes"),
                doc.sections().stream().map(DocSections.Section::id).toList());
        // the introduction: the text before the first section, without the front matter
        Assertions.assertTrue(doc.body(doc.root()).startsWith("\n# Sample Module"), doc.body(doc.root()));
        Assertions.assertFalse(doc.body(doc.root()).contains("## Parameters"));

        // a section comes with its subsections, and stops at the next heading of its level
        final String parameters = doc.content(doc.find("parameters").getFirst());
        Assertions.assertTrue(parameters.startsWith("## Parameters"), parameters);
        Assertions.assertTrue(parameters.contains("About windows.") && parameters.contains("Parameter examples."), parameters);
        Assertions.assertFalse(parameters.contains("Whole examples."), parameters);
        Assertions.assertEquals("## Parameters\n\nThe table.\n\n", doc.body(doc.find("parameters").getFirst()));
        final String windows = doc.content(doc.find("windows-window").getFirst());
        Assertions.assertTrue(windows.contains("## neither is this") && !windows.contains("Parameter examples."), windows);
        Assertions.assertTrue(doc.content(doc.find("notes").getFirst()).contains("Last words."));

        // by id, by path, by heading text; an ambiguous slug names every candidate
        Assertions.assertEquals("parameters/examples", doc.find("parameters/examples").getFirst().id());
        Assertions.assertEquals("windows-window", doc.find("Windows (`window`)").getFirst().id());
        Assertions.assertEquals("windows-window", doc.find("parameters/windows-window").getFirst().id());
        Assertions.assertEquals(1, doc.find("examples").size());
        Assertions.assertEquals(2, doc.find("Examples").size());
        Assertions.assertTrue(doc.find("nosuch").isEmpty());

        final String outline = doc.outline(doc.root());
        Assertions.assertTrue(outline.contains("- Parameters — id: `parameters` ("), outline);
        Assertions.assertTrue(outline.contains("  - Examples — id: `parameters/examples` ("), outline);
    }

    @Test
    public void testReadSmallDocument() {
        // under the limit: the whole text, as before
        Assertions.assertEquals(DOC, text(ReadDocsTool.read("sample.md", DOC, null, false, false)));
        // the outline on request
        final String outline = text(ReadDocsTool.read("sample.md", DOC, null, true, false));
        Assertions.assertTrue(outline.startsWith("# Sample Module\n\nWhat the sample does."), outline);
        Assertions.assertTrue(outline.contains("id: `windows-window`") && !outline.contains("About windows."), outline);
        // sections, in the order asked
        final String sections = text(ReadDocsTool.read("sample.md", DOC, "notes, windows-window", false, false));
        Assertions.assertTrue(sections.startsWith("## Notes ##"), sections);
        Assertions.assertTrue(sections.contains("About windows.") && !sections.contains("The table."), sections);
        // the outline of one section
        final String sub = text(ReadDocsTool.read("sample.md", DOC, "parameters", true, false));
        Assertions.assertTrue(sub.contains("id: `windows-window`") && !sub.contains("id: `notes`") && !sub.contains("The table."), sub);

        final McpSchema.CallToolResult missing = ReadDocsTool.read("sample.md", DOC, "nosuch", false, false);
        Assertions.assertTrue(missing.isError());
        Assertions.assertTrue(text(missing).contains("id: `parameters`"), text(missing));
        final McpSchema.CallToolResult ambiguous = ReadDocsTool.read("sample.md", DOC, "Examples", false, false);
        Assertions.assertTrue(ambiguous.isError());
        Assertions.assertTrue(text(ambiguous).contains("`parameters/examples`") && text(ambiguous).contains("`examples`"), text(ambiguous));
    }

    @Test
    public void testReadLargeDocument() {
        final String filler = "word ".repeat(ReadDocsTool.OUTLINE_THRESHOLD / 5 + 1);
        final String large = DOC.replace("About windows.", filler);
        // whole: the introduction and the outline, not the text
        final String answer = text(ReadDocsTool.read("sample.md", large, null, false, false));
        Assertions.assertTrue(answer.contains("Introduction.") && answer.contains("## Outline") && answer.contains("full: true"), answer);
        Assertions.assertTrue(answer.length() < 2000, answer);
        Assertions.assertEquals(large, text(ReadDocsTool.read("sample.md", large, null, false, true)));
        // a section over the limit: its own text and its subsections' outline
        final String parameters = text(ReadDocsTool.read("sample.md", large, "parameters", false, false));
        Assertions.assertTrue(parameters.contains("The table.") && parameters.contains("id: `windows-window`"), parameters);
        Assertions.assertTrue(parameters.length() < 2000, parameters);
        Assertions.assertTrue(text(ReadDocsTool.read("sample.md", large, "parameters", false, true)).contains(filler));
        // a section over the limit without subsections is returned: there is nothing smaller to offer
        Assertions.assertTrue(text(ReadDocsTool.read("sample.md", large, "windows-window", false, false)).contains(filler));
    }

    /** Headings and references that are not what they look like at first sight. */
    @Test
    public void testHeadingEdges() {
        final String edges = """
                # Edges

                ## Step
                one

                ## Step
                two

                ## Step 2
                three

                ## Using C#
                sharp

                ## Periods, time window and flags
                periods

                ````markdown
                ```yaml
                ## not a heading
                ```
                ````

                ### Inner
                inner
                """;
        final DocSections doc = DocSections.parse(edges);
        // a generated suffix does not take the id of a sibling that really ends in one; a fence closes on one
        // at least as long, so the shorter fence inside it and the heading it holds are text
        Assertions.assertEquals(List.of("step", "step-2", "step-2-2", "using-c", "periods-time-window-and-flags", "inner"),
                doc.sections().stream().map(DocSections.Section::id).toList());
        Assertions.assertTrue(doc.content(doc.find("step-2-2").getFirst()).contains("three"));
        // a trailing # is a closing sequence only after a space
        Assertions.assertEquals("Using C#", doc.find("using-c").getFirst().title());
        // a path is matched whatever its case, as a slug is
        Assertions.assertEquals("inner", doc.find("Periods-Time-Window-And-Flags/Inner").getFirst().id());

        // a heading that holds commas is one reference, not a list of them
        final McpSchema.CallToolResult heading = ReadDocsTool.read("edges.md", edges, "Periods, time window and flags", false, false);
        Assertions.assertFalse(heading.isError(), text(heading));
        Assertions.assertTrue(text(heading).startsWith("## Periods, time window and flags"), text(heading));
        // a section asked together with its subsection holds it once
        final String both = text(ReadDocsTool.read("edges.md", edges, "periods-time-window-and-flags,inner,step", false, false));
        Assertions.assertEquals(both.indexOf("### Inner"), both.lastIndexOf("### Inner"), both);
        Assertions.assertTrue(both.contains("### Inner") && both.contains("one"), both);

        // a document that opens with a horizontal rule has no front matter to skip
        final DocSections ruled = DocSections.parse("---\n\n# Ruled\n\nIntroduction.\n\n---\n\n## First\ntext\n");
        Assertions.assertEquals("Ruled", ruled.title());
        Assertions.assertTrue(ruled.body(ruled.root()).contains("Introduction."), ruled.body(ruled.root()));
    }

    /** Every bundled document: each section is addressable by its id, and the sections cover the text. */
    @Test
    public void testBundledDocuments() throws IOException {
        final Path root = Paths.get("src/main/resources/server/docs");
        final List<Path> files;
        try (Stream<Path> walk = Files.walk(root)) {
            files = walk.filter(p -> p.toString().endsWith(".md")).toList();
        }
        Assertions.assertTrue(files.size() > 50, files::toString);
        for (final Path file : files) {
            final String content = Files.readString(file, StandardCharsets.UTF_8);
            final DocSections doc = DocSections.parse(content);
            final Set<String> ids = new HashSet<>();
            int covered = 0;
            for (final DocSections.Section s : doc.sections()) {
                Assertions.assertTrue(ids.add(s.id()), file + ": duplicate section id " + s.id());
                Assertions.assertEquals(List.of(s), doc.find(s.id()), file + ": " + s.id());
                Assertions.assertTrue(doc.content(s).startsWith("#"), file + ": " + s.id());
            }
            for (final DocSections.Section s : doc.root().children()) covered += s.size();
            if (!doc.sections().isEmpty()) {
                Assertions.assertTrue(content.endsWith(doc.body(doc.root()) + content.substring(content.length() - covered)), file.toString());
            }
            final String relative = root.relativize(file).toString().replace('\\', '/');
            // how the documents are written, so that reading by section works on every one of them:
            // one title - a second level-1 heading would open no section and hide in the one before it
            Assertions.assertEquals(1, doc.titleHeadings(), relative + ": a document has exactly one level-1 heading (the title)");
            // and no stretch of text over the limit without a heading in it: the introduction, and every section's
            // own text up to its first subsection, is what a read returns when the whole is too large
            Assertions.assertTrue(doc.body(doc.root()).length() <= ReadDocsTool.OUTLINE_THRESHOLD,
                    relative + ": the introduction is " + doc.body(doc.root()).length() + " characters; move text under a section heading");
            for (final DocSections.Section s : doc.sections()) {
                Assertions.assertTrue(doc.body(s).length() <= ReadDocsTool.OUTLINE_THRESHOLD,
                        relative + ": section `" + s.id() + "` has " + doc.body(s).length() + " characters of its own text (limit "
                                + ReadDocsTool.OUTLINE_THRESHOLD + "); split it with sub-headings");
                // a section over the limit answers with that text and the outline of its subsections: within the limit too
                if (!s.children().isEmpty() && s.size() > ReadDocsTool.OUTLINE_THRESHOLD) {
                    final String part = text(ReadDocsTool.read(relative, content, s.id(), false, false));
                    Assertions.assertTrue(part.length() <= ReadDocsTool.OUTLINE_THRESHOLD,
                            relative + ": section `" + s.id() + "` answers with " + part.length() + " characters");
                }
            }
            // what a plain read answers stays readable in one go
            final String answer = text(ReadDocsTool.read(relative, content, null, false, false));
            Assertions.assertTrue(answer.length() <= ReadDocsTool.OUTLINE_THRESHOLD, relative + ": " + answer.length());
        }
    }

    @Test
    public void testReadDocsTool() {
        final ReadDocsTool tool = new ReadDocsTool();
        final String feature = text(tool.sync(null, Tool.request("read-docs", Map.of("module", "transform/feature"))));
        Assertions.assertTrue(feature.contains("# Feature Transform Module") && feature.contains("## Outline"), feature);
        Assertions.assertTrue(feature.contains("id: `sources-contract`"), feature);
        final McpSchema.CallToolResult section = tool.sync(null,
                Tool.request("read-docs", Map.of("module", "transform/feature", "section", "sources-contract")));
        Assertions.assertFalse(section.isError(), text(section));
        Assertions.assertTrue(text(section).startsWith("### Sources contract"), text(section));
        final String full = text(tool.sync(null, Tool.request("read-docs", Map.of("module", "transform/feature", "full", "true"))));
        Assertions.assertTrue(full.length() > 200_000, String.valueOf(full.length()));
        // a small document is unchanged
        final String filter = text(tool.sync(null, Tool.request("read-docs", Map.of("path", "module/common/filter.md"))));
        Assertions.assertFalse(filter.contains("## Outline"), filter);
    }

}

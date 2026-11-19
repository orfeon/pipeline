package com.mercari.solution.server.mcp.tool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Markdown document as a tree of sections, cut at its headings — what lets a reader take a large document
 * by its outline and then section by section instead of whole. Nothing here knows the documents: any {@code .md}
 * of the docs tree is read the same way.
 *
 * <p>The document is the root section; a heading of level 2 or deeper opens a section that runs to the next
 * heading of its level or above (the level-1 title and the text under it are the root's own text). Headings
 * inside fenced code blocks are not headings. A section's id is the slug of its heading (lower case, spaces as
 * hyphens, punctuation dropped), prefixed with its parent's id and {@code /} only when the slug alone is not
 * unique in the document.
 */
public final class DocSections {

    // a closing run of #s is syntax only when a space precedes it: "## Notes ##" is "Notes", "## C#" is "C#"
    private static final Pattern HEADING = Pattern.compile("^(#{1,6})[ \\t]+(.+?)(?:[ \\t]+#+)?[ \\t]*$");
    private static final Pattern FENCE = Pattern.compile("^ {0,3}(`{3,}|~{3,})(.*)$");
    private static final Pattern FRONT_MATTER_KEY = Pattern.compile("^[A-Za-z_][\\w-]*[ \\t]*:.*$");

    /** One section: its heading, where it sits, and its extent in the document text. */
    public static final class Section {
        private final String title;
        private final int level;
        private final Section parent;
        private final List<Section> children = new ArrayList<>();
        private final int start;
        private int bodyEnd;
        private int end;
        private String slug;
        private String id;

        private Section(final String title, final int level, final Section parent, final int start) {
            this.title = title;
            this.level = level;
            this.parent = parent;
            this.start = start;
        }

        public String title() { return title; }
        public String id() { return id; }
        public List<Section> children() { return children; }
        /** The section this one is a subsection of: the document root for a top-level section, null for the root. */
        public Section parent() { return parent; }
        /** Characters of the section, its subsections included. */
        public int size() { return end - start; }
    }

    private final String text;
    private final String title;
    private final String description;
    private final Section root;
    private final List<Section> sections = new ArrayList<>();

    private DocSections(final String text) {
        this.text = text;
        final String[] meta = frontMatter(text);
        this.description = meta[1];
        this.root = new Section(null, 1, null, 0);
        String heading = null;

        Section current = root;
        boolean fenced = false;
        char fenceChar = 0;
        int fenceLength = 0;
        boolean first = true;
        int offset = 0;
        final int bodyStart = Integer.parseInt(meta[2]);
        while (offset < text.length()) {
            int lineEnd = text.indexOf('\n', offset);
            if (lineEnd < 0) lineEnd = text.length();
            final String line = text.substring(offset, lineEnd).replace("\r", "");
            if (offset >= bodyStart) {
                final Matcher f = FENCE.matcher(line);
                if (fenced) {
                    // closed by a fence of the same character, at least as long, with nothing after it:
                    // a shorter fence or one with an info string ("```yaml") is text of the block
                    if (f.matches() && f.group(1).charAt(0) == fenceChar
                            && f.group(1).length() >= fenceLength && f.group(2).isBlank()) {
                        fenced = false;
                    }
                } else if (f.matches() && (f.group(1).charAt(0) == '~' || f.group(2).indexOf('`') < 0)) {
                    // (a backtick after the marker: inline code at the start of a line, not a fence)
                    fenced = true;
                    fenceChar = f.group(1).charAt(0);
                    fenceLength = f.group(1).length();
                } else {
                    final Matcher h = HEADING.matcher(line);
                    if (h.matches()) {
                        final int level = h.group(1).length();
                        if (level == 1) {
                            if (heading == null) heading = h.group(2).trim();
                        } else {
                            Section parent = current;
                            while (parent != root && parent.level >= level) parent = parent.parent;
                            for (Section open = current; open != parent; open = open.parent) open.end = offset;
                            if (first) {
                                root.bodyEnd = offset;
                                first = false;
                            }
                            if (current.bodyEnd == 0 && current != root) current.bodyEnd = offset;
                            final Section section = new Section(h.group(2).trim(), level, parent, offset);
                            parent.children.add(section);
                            sections.add(section);
                            current = section;
                        }
                    }
                }
            }
            offset = lineEnd + 1;
        }
        final int length = text.length();
        for (Section open = current; open != null; open = open.parent) open.end = length;
        root.end = length;
        if (first) root.bodyEnd = length;
        for (final Section s : sections) {
            if (s.bodyEnd == 0) s.bodyEnd = s.end;
        }
        this.title = meta[0] != null ? meta[0] : heading;
        assignIds();
    }

    public static DocSections parse(final String text) {
        return new DocSections(text == null ? "" : text);
    }

    /** {@code [title, description, offset of the text after the front matter]} of a leading {@code ---} block. */
    private static String[] frontMatter(final String text) {
        final String[] meta = {null, null, "0"};
        if (!text.startsWith("---")) return meta;
        final int close = text.indexOf("\n---", 3);
        if (close < 0) return meta;
        final String[] lines = text.substring(3, close).split("\n");
        // a document that opens with a horizontal rule has no front matter: the block has to start with a key
        for (final String line : lines) {
            if (line.isBlank()) continue;
            if (!FRONT_MATTER_KEY.matcher(line.replace("\r", "")).matches()) return meta;
            break;
        }
        for (final String line : lines) {
            final String l = line.replace("\r", "");
            if (l.startsWith("title:")) meta[0] = unquote(l.substring("title:".length()));
            if (l.startsWith("description:")) meta[1] = unquote(l.substring("description:".length()));
        }
        final int after = text.indexOf('\n', close + 1);
        meta[2] = String.valueOf(after < 0 ? text.length() : after + 1);
        return meta;
    }

    private static String unquote(final String value) {
        String v = value.trim();
        if (v.length() >= 2 && (v.startsWith("\"") && v.endsWith("\"") || v.startsWith("'") && v.endsWith("'"))) {
            v = v.substring(1, v.length() - 1);
        }
        return v.isEmpty() ? null : v;
    }

    /** The slug of a heading, as the anchors of rendered Markdown are made: lower case, hyphens for spaces. */
    static String slug(final String heading) {
        final StringBuilder sb = new StringBuilder();
        for (final char c : heading.toLowerCase(Locale.ROOT).toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == '_' || c == '-') {
                sb.append(c);
            } else if (Character.isWhitespace(c)) {
                sb.append('-');
            }
        }
        final String slug = sb.toString().replaceAll("-{2,}", "-").replaceAll("^-|-$", "");
        return slug.isEmpty() ? "section" : slug;
    }

    private void assignIds() {
        // siblings with the same heading: -2, -3, ... in document order
        assignSlugs(root);
        final Map<String, Integer> count = new HashMap<>();
        for (final Section s : sections) count.merge(s.slug, 1, Integer::sum);
        for (final Section s : sections) {
            s.id = count.get(s.slug) == 1 ? s.slug : path(s);
        }
    }

    private static void assignSlugs(final Section parent) {
        final Set<String> used = new HashSet<>();
        for (final Section s : parent.children) {
            final String slug = slug(s.title);
            // the suffix skips what a sibling already has: "Foo", "Foo", "Foo 2" are foo, foo-2, foo-2-2
            String unique = slug;
            for (int n = 2; !used.add(unique); n++) unique = slug + "-" + n;
            s.slug = unique;
            assignSlugs(s);
        }
    }

    private String path(final Section s) {
        return s.parent == root ? s.slug : path(s.parent) + "/" + s.slug;
    }

    public String text() { return text; }
    public int size() { return text.length(); }
    public String title() { return title; }
    public String description() { return description; }
    public Section root() { return root; }
    public List<Section> sections() { return sections; }

    /**
     * The sections a reference names: an id, a full path ({@code parent/child}), the trailing part of one, or
     * the heading text itself (case-insensitive). One section when the reference is exact; several when it is
     * ambiguous; none when nothing matches.
     */
    public List<Section> find(final String reference) {
        final String ref = reference == null ? "" : reference.trim().replaceAll("^#+\\s*", "").replaceAll("^/+|/+$", "");
        if (ref.isEmpty()) return List.of();
        final List<Section> matched = new ArrayList<>();
        for (final Section s : sections) {
            if (s.id.equals(ref) || path(s).equals(ref)) return List.of(s);
        }
        final String lower = ref.toLowerCase(Locale.ROOT);
        for (final Section s : sections) {
            final String path = path(s);
            if (path.equals(lower) || path.endsWith("/" + lower) || s.slug.equals(lower) || s.title.equalsIgnoreCase(ref) || s.slug.equals(slug(ref))) {
                matched.add(s);
            }
        }
        return matched;
    }

    /** The sections whose heading is this text (case-insensitive; a leading run of {@code #} is ignored). */
    public List<Section> findByHeading(final String heading) {
        final String ref = heading == null ? "" : heading.trim().replaceAll("^#+\\s*", "");
        final List<Section> matched = new ArrayList<>();
        for (final Section s : sections) {
            if (s.title.equalsIgnoreCase(ref)) matched.add(s);
        }
        return matched;
    }

    /** The whole section: its heading, its text and its subsections. */
    public String content(final Section section) {
        return text.substring(section.start, section.end);
    }

    /** The section's own text: from its heading up to its first subsection (for the root: up to the first section). */
    public String body(final Section section) {
        return text.substring(section == root ? frontMatterEnd() : section.start, section.bodyEnd);
    }

    private int frontMatterEnd() {
        return Integer.parseInt(frontMatter(text)[2]);
    }

    /** The sections under {@code parent} as an indented list: heading, id and size of each. */
    public String outline(final Section parent) {
        final StringBuilder sb = new StringBuilder();
        outline(parent, 0, sb);
        return sb.toString();
    }

    private void outline(final Section parent, final int depth, final StringBuilder sb) {
        for (final Section s : parent.children) {
            sb.append("  ".repeat(depth)).append("- ").append(s.title)
                    .append(" — id: `").append(s.id).append("` (").append(formatSize(s.size())).append(")\n");
            outline(s, depth + 1, sb);
        }
    }

    static String formatSize(final int chars) {
        return String.format(Locale.ROOT, "%,d chars", chars);
    }

}

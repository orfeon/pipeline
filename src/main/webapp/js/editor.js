/**
 * editor.js - The config text view: a Monaco YAML/JSON editor that is a peer
 * of the canvas, not a modal. Both are views of the workspace store.
 *
 * Store -> editor: any config/settings change the editor did not make marks the
 * text stale; it is regenerated from the store when the view is visible (or the
 * next time it becomes visible). While the store is unchanged since the editor's
 * last push, the text is kept verbatim — comments, key order and anything the
 * canvas cannot represent survive a round trip through the canvas tab.
 *
 * Editor -> store: text edits are parsed after a short pause and pushed with
 * source 'editor'. Unparseable or empty text is never pushed; the problem is
 * shown in the editor's status line, the store keeps the last good config, and
 * `flushEditor()` reports it so Run / Launch / tab switches can refuse to
 * proceed with a config that is not the one on screen.
 *
 * Pending proposal: while the store holds a proposal (from the agent), the
 * view shows a read-only side-by-side diff (current -> proposed) instead of the
 * editor; Accept / Reject in the agent pane ends it.
 * Selection: the module whose block contains the cursor becomes the
 * workspace selection (agent context).
 */
'use strict';

import { $id, on, setStatus } from './util.js';
import { loadMonaco, setEditorValue, getEditorValue, ensureSchema, applyYamlSchemas,
         onEditorChange, onEditorCursorChange, setEditorMarkers, showDiff,
         revealLine, insertText, replaceRange } from './monaco.js';
import * as workspace from './workspace.js';

const SOURCE = 'editor';
const CONTAINER = 'config-editor';
const DIFF_CONTAINER = 'config-diff';
const PUSH_DELAY_MS = 500;

let format = 'yaml';          // 'yaml' | 'json'
let visible = false;
let created = false;          // the Monaco editor exists (see ensureEditor)
let stale = true;             // store changed since the text was last generated/pushed
let suppress = false;         // ignore Monaco change events caused by our own setValue
let problem = '';             // current parse problem ('' when the text is a valid config)
let pushTimer = null;
let ready = null;             // promise: Monaco editor created

/**
 * Text for the store's config. Empty module sections (`sources: []`) are
 * omitted: they are a store normalization artifact, and the explorer's
 * snippet insertion creates a section when it is missing.
 */
function serialize(config) {
    const cleaned = {};
    Object.keys(config || {}).forEach(function(key) {
        const value = config[key];
        if (Array.isArray(value) && value.length === 0) return;
        cleaned[key] = value;
    });
    return format === 'yaml' ? jsyaml.dump(cleaned) : JSON.stringify(cleaned, null, 2);
}

function parse(text) {
    return format === 'yaml' ? jsyaml.load(text) : JSON.parse(text);
}

function setProblem(message) {
    problem = message || '';
    const el = $id('editor-status');
    el.textContent = problem;
    el.classList.toggle('d-none', !problem);
}

// =============================
// Store -> editor
// =============================

function hasComments(text) {
    return format === 'yaml' && /^\s*#/m.test(text || '');
}

/** Regenerate the text from the store (only when stale and the editor exists). */
function refreshFromStore() {
    if (!stale || !created) return;
    const previous = getEditorValue(CONTAINER);
    const config = workspace.getConfig();
    const text = workspace.hasModules(config) || config.system || config.options ? serialize(config) : '';
    suppress = true;
    setEditorValue(CONTAINER, text, format).then(function() {
        suppress = false;
        stale = false;
        setProblem('');
        applyValidationMarkers();
        if (hasComments(previous)) {
            setStatus('Config regenerated from the canvas — comments in the editor were dropped', 'warning');
        }
    });
}

function onWorkspaceChange(event) {
    if (event.type === 'pending') {
        if (visible) renderPending();
        return;
    }
    if (event.source === SOURCE || event.type === 'positions' || event.type === 'selection' || event.type === 'agent') return;
    // Someone else replaced the config. A push still pending here was typed
    // against the old config: drop it (at most 500 ms of keystrokes) rather
    // than let it overwrite the change that just happened, and say so.
    if (pushTimer) {
        clearTimeout(pushTimer);
        pushTimer = null;
        setStatus('Config replaced by ' + event.source + ' — your last keystrokes in the editor were discarded', 'warning');
    }
    stale = true;
    if (visible) refreshFromStore();
}

// =============================
// Pending proposal (diff view)
// =============================

/** Show the proposal as a diff over the editor, or the editor when there is none. */
function renderPending() {
    const proposed = workspace.getPending();
    const diffEl = $id(DIFF_CONTAINER);
    const editorEl = $id(CONTAINER);
    if (!proposed) {
        diffEl.classList.add('d-none');
        editorEl.classList.remove('d-none');
        return;
    }
    editorEl.classList.add('d-none');
    diffEl.classList.remove('d-none');
    showDiff(DIFF_CONTAINER, serialize(workspace.getConfig()), serialize(proposed), format);
}

// =============================
// Text structure helpers (YAML)
// =============================

const NAME_VALUE = '\\s*["\']?([^"\'\\s#]+)["\']?\\s*(#.*)?$';

/**
 * 0-based index of the line that declares module `name` (the `name:` key of
 * a top-level list item in YAML, `"name": "..."` in JSON), or -1.
 */
function moduleLineIndex(lines, name) {
    const re = format === 'yaml'
        ? new RegExp('^\\s*-?\\s*name:\\s*["\']?' + escapeRegExp(name) + '["\']?\\s*(#.*)?$')
        : new RegExp('"name"\\s*:\\s*"' + escapeRegExp(name) + '"');
    return lines.findIndex(function(l) { return re.test(l); });
}

/**
 * The module whose block contains `lineNumber` (1-based), or null.
 * YAML: modules are the list items directly under a top-level section
 * (`  - ` at indent 2); the item's `name:` key may follow other keys. Deeper
 * `- name:` items (e.g. schema fields) belong to the enclosing module.
 */
function moduleAtLine(lineNumber) {
    const lines = getEditorValue(CONTAINER).split('\n');
    const last = Math.min(lineNumber, lines.length) - 1;
    if (format !== 'yaml') {
        for (let i = last; i >= 0; i--) {
            const m = lines[i].match(/"name"\s*:\s*"([^"]+)"/);
            if (m) return m[1];
        }
        return null;
    }
    // find the enclosing top-level list item
    let itemStart = -1;
    for (let i = last; i >= 0; i--) {
        if (/^[A-Za-z_]/.test(lines[i])) return null;   // reached the section key: cursor is above every item
        if (/^  - /.test(lines[i])) { itemStart = i; break; }
    }
    if (itemStart < 0) return null;
    // the item's own keys: the dash line plus lines indented by 4 (deeper = nested)
    const keyRe = new RegExp('^(?:  - |    )name:' + NAME_VALUE);
    for (let i = itemStart; i < lines.length; i++) {
        if (i > itemStart && !/^    /.test(lines[i]) && lines[i].trim() !== '') break;
        const m = lines[i].match(keyRe);
        if (m) return m[1];
    }
    return null;
}

function onCursorMoved(lineNumber) {
    if (!visible || suppress) return;
    const name = moduleAtLine(lineNumber);
    if (name && workspace.getModuleNames().indexOf(name) >= 0) {
        workspace.setSelection(name, SOURCE);
    }
}

// =============================
// Editor -> store
// =============================

/**
 * Parse the text and push it to the store. Returns true when the store now
 * matches the text (pushed, or already identical); false when the text is
 * not a config (parse error / empty) — the store is left untouched.
 */
function pushToStore() {
    const text = getEditorValue(CONTAINER);
    if (!text.trim()) {
        // Never wipe the pipeline because the text is (momentarily) empty;
        // the header's Clear button is the explicit way to start over.
        setProblem('The config text is empty — the previous config is kept. Use Clear to start over.');
        return false;
    }
    let config;
    try {
        config = parse(text);
    } catch (e) {
        setProblem('Parse error: ' + e.message);
        return false;
    }
    if (config === null || typeof config !== 'object' || Array.isArray(config)) {
        setProblem('The config must be a mapping (system / options / sources / transforms / sinks / actions)');
        return false;
    }
    setProblem('');
    if (!workspace.isSameConfig(config)) {
        workspace.setConfig(config, SOURCE);
    }
    stale = false; // our own push: the text is authoritative, keep it verbatim
    applyValidationMarkers();
    return true;
}

function onTextChanged() {
    if (suppress) return;
    clearTimeout(pushTimer);
    pushTimer = setTimeout(function() {
        pushTimer = null;
        pushToStore();
    }, PUSH_DELAY_MS);
}

/**
 * Push a pending edit immediately (before run/launch, sending to the agent,
 * or a tab switch). Returns true when the store reflects the editor text,
 * false when the text has a problem (the store keeps the last good config).
 */
export function flushEditor() {
    if (!created) return true;
    if (pushTimer) {
        clearTimeout(pushTimer);
        pushTimer = null;
        return pushToStore();
    }
    return !problem;
}

/** The current parse problem of the editor text, or '' when it is a valid config. */
export function getEditorProblem() {
    return problem;
}

// =============================
// Validation markers
// =============================

/**
 * Place the store's structural validation issues on the `name:` line of the
 * module they concern (pipeline-level issues go on line 1).
 */
function applyValidationMarkers() {
    const lines = getEditorValue(CONTAINER).split('\n');
    const markers = workspace.getValidationIssues().map(function(issue) {
        let line = 1;
        if (issue.module) {
            const index = moduleLineIndex(lines, issue.module);
            if (index >= 0) line = index + 1;
        }
        return { line: line, message: issue.message };
    });
    setEditorMarkers(CONTAINER, 'pipeline', markers);
}

function escapeRegExp(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// =============================
// Explorer entry points (Config view)
// =============================

const TOP_LEVEL_ORDER = ['system', 'options', 'sources', 'transforms', 'sinks', 'actions'];

/** [start, end) line indexes (0-based) of a top-level YAML section, or null. */
function sectionRange(lines, key) {
    const start = lines.findIndex(function(l) { return new RegExp('^' + key + ':\\s*(\\[\\s*\\]\\s*)?(#.*)?$').test(l); });
    if (start < 0) return null;
    // an inline empty section (`sources: []`) becomes a block so items can be appended
    if (/^\w+:\s*\[\s*\]/.test(lines[start])) {
        return [start, start + 1, true];
    }
    let end = start + 1;
    while (end < lines.length && !/^[A-Za-z_]/.test(lines[end])) end++;
    // trailing blank lines belong to the gap, not the section
    while (end > start + 1 && !lines[end - 1].trim()) end--;
    return [start, end];
}

function indentBlock(text, spaces) {
    const pad = ' '.repeat(spaces);
    return text.split('\n').map(function(l) { return l ? pad + l : l; }).join('\n');
}

/**
 * Insert `snippet` (ending with a newline) so that it starts on 1-based
 * `line`. When `line` is past the last line the text is appended, terminating
 * the last line first if needed — Monaco would otherwise clamp the position
 * and glue the snippet onto the last line.
 */
function insertLines(lines, line, snippet) {
    if (line > lines.length) {
        const lastLine = lines.length;
        const lastText = lines[lastLine - 1];
        insertText(CONTAINER, lastLine, lastText.length + 1, (lastText ? '\n' : '') + snippet);
        revealLine(CONTAINER, lastText ? lastLine + 1 : lastLine);
        return;
    }
    insertText(CONTAINER, line, 1, snippet);
    revealLine(CONTAINER, line);
}

/**
 * Insert a module skeleton at the end of its section (creating the section
 * when missing). YAML only — JSON editing has no stable insertion point.
 * Returns a promise of true when inserted.
 */
export function insertModuleSnippet(sectionKey, moduleObj) {
    if (format !== 'yaml') {
        setStatus('Switch the format to YAML to insert module snippets', 'warning');
        return Promise.resolve(false);
    }
    return ensureEditor().then(function() {
        const text = getEditorValue(CONTAINER);
        const lines = text.split('\n');
        const item = '- ' + indentBlock(jsyaml.dump(moduleObj).trimEnd(), 2).slice(2);
        const range = sectionRange(lines, sectionKey);
        if (range && range[2]) {
            // `sources: []` -> `sources:\n  - ...` (replace the inline empty array)
            const original = lines[range[0]];
            replaceRange(CONTAINER, range[0] + 1, 1, range[0] + 1, original.length + 1,
                sectionKey + ':\n' + indentBlock(item, 2));
            revealLine(CONTAINER, range[0] + 2);
            return true;
        }
        if (range) {
            insertLines(lines, range[1] + 1, indentBlock(item, 2) + '\n');
            return true;
        }
        // append the whole section after the last existing top-level key that precedes it
        const order = TOP_LEVEL_ORDER.indexOf(sectionKey);
        let after = lines.length;
        for (let i = order - 1; i >= 0; i--) {
            const r = sectionRange(lines, TOP_LEVEL_ORDER[i]);
            if (r) { after = r[1]; break; }
        }
        const needsGap = after > 0 && lines[after - 1].trim() !== '';
        insertLines(lines, after + 1, (needsGap ? '\n' : '') + sectionKey + ':\n' + indentBlock(item, 2) + '\n');
        return true;
    });
}

/** Move the cursor to a module's `name:` line (Config view). */
export function jumpToModule(name) {
    const index = moduleLineIndex(getEditorValue(CONTAINER).split('\n'), name);
    if (index < 0) {
        setStatus('Module "' + name + '" not found in the config text', 'warning');
        return false;
    }
    revealLine(CONTAINER, index + 1);
    return true;
}

/**
 * Move the cursor to a top-level section (`system:` / `options:`), inserting
 * an empty template when the section is missing (YAML only).
 */
export function jumpToSection(key) {
    const lines = getEditorValue(CONTAINER).split('\n');
    const range = format === 'yaml' ? sectionRange(lines, key) : null;
    if (range) {
        revealLine(CONTAINER, range[0] + 2, 3);
        workspace.setSelection(key, SOURCE);
        return true;
    }
    if (format !== 'yaml') {
        const index = lines.findIndex(function(l) { return new RegExp('"' + key + '"\\s*:').test(l); });
        if (index >= 0) { revealLine(CONTAINER, index + 1); return true; }
        setStatus('Switch the format to YAML to add ' + key, 'warning');
        return false;
    }
    // insert at the top (system) or after system (options)
    let line = 1;
    if (key === 'options') {
        const sys = sectionRange(lines, 'system');
        if (sys) line = sys[1] + 1;
    }
    const template = key === 'system' ? 'system:\n  args: {}\n' : 'options:\n  runner: direct\n';
    const needsGap = line <= lines.length && lines[line - 1] && lines[line - 1].trim() !== '';
    insertLines(lines, line, template + (needsGap ? '\n' : ''));
    revealLine(CONTAINER, line + 1, 3);
    workspace.setSelection(key, SOURCE);
    return true;
}

// =============================
// Toolbar
// =============================

function onFormatChanged() {
    const next = $id('editor-format').value === 'json' ? 'json' : 'yaml';
    if (!flushEditor()) {
        $id('editor-format').value = format;
        setStatus('Fix the config text before switching the format', 'warning');
        return;
    }
    format = next;
    stale = true;
    ensureEditor().then(refreshFromStore);
}

function copyToClipboard() {
    navigator.clipboard.writeText(getEditorValue(CONTAINER)).then(function() {
        setStatus('Copied to clipboard');
    });
}

function download() {
    const content = getEditorValue(CONTAINER);
    const filename = 'pipeline-config.' + (format === 'yaml' ? 'yaml' : 'json');
    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    setStatus('Downloaded ' + filename);
}

function onImportFileSelected(e) {
    const file = e.target.files[0];
    e.target.value = ''; // allow re-selecting the same file later
    if (!file) return;

    const isJson = file.name.toLowerCase().endsWith('.json');
    file.text().then(function(text) {
        format = isJson ? 'json' : 'yaml';
        $id('editor-format').value = format;
        suppress = true;
        return ensureEditor().then(function() { return setEditorValue(CONTAINER, text, format); });
    }).then(function() {
        suppress = false;
        clearTimeout(pushTimer);
        pushTimer = null;
        setStatus(pushToStore() ? 'Imported ' + file.name : 'Imported ' + file.name + ' — fix the problems shown before it is applied', 'warning');
    }).catch(function(err) {
        suppress = false;
        setStatus('Failed to read ' + file.name + ': ' + err.message, 'error');
    });
}

// =============================
// Lifecycle
// =============================

/** Called by the view switcher; the editor only renders while visible. */
export function setEditorVisible(isVisible) {
    visible = isVisible;
    $id('editor-toolbar').classList.toggle('d-none', !isVisible);
    if (isVisible) {
        ensureEditor().then(function() {
            refreshFromStore();
            renderPending();
        });
    } else {
        flushEditor();
    }
}

/**
 * Create the Monaco editor on first display (creating it while its container
 * is display:none breaks paste). The editor is created empty and marked
 * stale; the text is generated from the store by the caller's refresh, never
 * by this chain, so a refresh that raced ahead is not blanked afterwards.
 */
function ensureEditor() {
    if (ready) return ready;
    // The system/options schemas are embedded in the top-level pipeline
    // schema monaco.js registers for this editor (completion + validation in
    // the YAML language server); the editor works without them on failure.
    ready = Promise.all([
        loadMonaco(),
        ensureSchema('system').catch(function() { return null; }),
        ensureSchema('options').catch(function() { return null; })
    ]).then(function() {
        applyYamlSchemas();
        return setEditorValue(CONTAINER, '', format);
    }).then(function() {
        onEditorChange(CONTAINER, onTextChanged);
        onEditorCursorChange(CONTAINER, onCursorMoved);
        created = true;
        stale = true;
    });
    return ready;
}

export function initEditor() {
    on('editor-format', 'change', onFormatChanged);
    on('btn-import-config', 'click', function() { $id('file-import').click(); });
    on('file-import', 'change', onImportFileSelected);
    on('btn-copy-config', 'click', copyToClipboard);
    on('btn-download-config', 'click', download);

    loadMonaco(); // pre-warm the language service for the modals; the editor itself is created on first display
    workspace.subscribe(onWorkspaceChange);
}

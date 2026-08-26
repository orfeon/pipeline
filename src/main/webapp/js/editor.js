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
 * source 'editor'. Unparseable text is never pushed; the parse error is shown
 * in the editor's status line and the store keeps the last good config.
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
let stale = true;             // store changed since the text was last generated/pushed
let suppress = false;         // ignore Monaco change events caused by our own setValue
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
    const el = $id('editor-status');
    el.textContent = message || '';
    el.classList.toggle('d-none', !message);
}

// =============================
// Store -> editor
// =============================

function hasComments(text) {
    return format === 'yaml' && /^\s*#/m.test(text || '');
}

/** Regenerate the text from the store (only when stale). */
function refreshFromStore() {
    if (!stale) return;
    const previous = getEditorValue(CONTAINER);
    const config = workspace.getConfig();
    const text = workspace.hasModules(config) || config.system || config.options ? serialize(config) : '';
    suppress = true;
    setEditorValue(CONTAINER, text, format).then(function() {
        suppress = false;
        stale = false;
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
// Cursor -> selection
// =============================

/**
 * The module whose block contains `lineNumber`: walk up to the nearest
 * `- name:` list item (YAML) / `"name":` (JSON) and read its value.
 */
function moduleAtLine(lineNumber) {
    const lines = getEditorValue(CONTAINER).split('\n');
    const re = format === 'yaml'
        ? /^\s*-\s+name:\s*["']?([A-Za-z0-9_]+)["']?\s*$/
        : /"name"\s*:\s*"([A-Za-z0-9_]+)"/;
    for (let i = Math.min(lineNumber, lines.length) - 1; i >= 0; i--) {
        const m = lines[i].match(re);
        if (m) return m[1];
        // a new top-level section starts: the cursor is above every module of it
        if (format === 'yaml' && /^[A-Za-z]/.test(lines[i])) return null;
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

function pushToStore() {
    const text = getEditorValue(CONTAINER);
    let config;
    try {
        config = text.trim() ? parse(text) : {};
    } catch (e) {
        setProblem('Parse error: ' + e.message);
        return;
    }
    if (config === null || typeof config !== 'object' || Array.isArray(config)) {
        setProblem('The config must be a mapping (system / options / sources / transforms / sinks / actions)');
        return;
    }
    setProblem('');
    workspace.setConfig(config, SOURCE);
    stale = false; // our own push: the text is authoritative, keep it verbatim
    applyValidationMarkers();
}

function onTextChanged() {
    if (suppress) return;
    clearTimeout(pushTimer);
    pushTimer = setTimeout(pushToStore, PUSH_DELAY_MS);
}

/** Push immediately (before run/launch or a tab switch) so the store is current. */
export function flushEditor() {
    if (!pushTimer) return;
    clearTimeout(pushTimer);
    pushTimer = null;
    pushToStore();
}

// =============================
// Validation markers
// =============================

/**
 * Place the store's structural validation issues on the `name:` line of the
 * module they concern (pipeline-level issues go on line 1).
 */
function applyValidationMarkers() {
    const text = getEditorValue(CONTAINER);
    const lines = text.split('\n');
    const markers = workspace.getValidationIssues().map(function(issue) {
        let line = 1;
        if (issue.module) {
            const re = format === 'yaml'
                ? new RegExp('^\\s*-?\\s*name:\\s*["\']?' + escapeRegExp(issue.module) + '["\']?\\s*$')
                : new RegExp('"name"\\s*:\\s*"' + escapeRegExp(issue.module) + '"');
            const index = lines.findIndex(function(l) { return re.test(l); });
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
        lines[start] = key + ':';
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
 * Insert a module skeleton at the end of its section (creating the section
 * when missing). YAML only — JSON editing has no stable insertion point.
 * Returns a promise of true when inserted.
 */
export function insertModuleSnippet(sectionKey, moduleObj) {
    if (format !== 'yaml') {
        setStatus('Switch the format to YAML to insert module snippets', 'warning');
        return Promise.resolve(false);
    }
    return ready.then(function() {
        const text = getEditorValue(CONTAINER);
        const lines = text.split('\n');
        const item = '- ' + indentBlock(jsyaml.dump(moduleObj).trimEnd(), 2).slice(2);
        const range = sectionRange(lines, sectionKey);
        let line, snippet;
        if (range && range[2]) {
            // `sources: []` -> `sources:\n  - ...` (replace the inline empty array)
            const original = text.split('\n')[range[0]];
            replaceRange(CONTAINER, range[0] + 1, 1, range[0] + 1, original.length + 1,
                sectionKey + ':\n' + indentBlock(item, 2));
            revealLine(CONTAINER, range[0] + 2);
            return true;
        }
        if (range) {
            line = range[1] + 1; // 1-based line after the section's last line
            snippet = indentBlock(item, 2) + '\n';
        } else {
            // append the whole section after the last existing top-level key that precedes it
            const order = TOP_LEVEL_ORDER.indexOf(sectionKey);
            let after = lines.length;
            for (let i = order - 1; i >= 0; i--) {
                const r = sectionRange(lines, TOP_LEVEL_ORDER[i]);
                if (r) { after = r[1]; break; }
            }
            line = after + 1;
            const needsGap = after > 0 && lines[after - 1].trim() !== '';
            snippet = (needsGap ? '\n' : '') + sectionKey + ':\n' + indentBlock(item, 2) + '\n';
            if (line > lines.length) {
                // past the end: make sure the previous line is terminated
                line = lines.length;
                const lastCol = lines[lines.length - 1].length + 1;
                insertText(CONTAINER, line, lastCol, (lines[lines.length - 1] ? '\n' : '') + snippet);
                revealLine(CONTAINER, line + 1);
                return true;
            }
        }
        insertText(CONTAINER, line, 1, snippet);
        revealLine(CONTAINER, line + 1);
        return true;
    });
}

/** Move the cursor to a module's `- name:` line (Config view). */
export function jumpToModule(name) {
    const lines = getEditorValue(CONTAINER).split('\n');
    const re = format === 'yaml'
        ? new RegExp('^\\s*-?\\s*name:\\s*["\']?' + escapeRegExp(name) + '["\']?\\s*$')
        : new RegExp('"name"\\s*:\\s*"' + escapeRegExp(name) + '"');
    const index = lines.findIndex(function(l) { return re.test(l); });
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
    insertText(CONTAINER, line, 1, template + (needsGap ? '\n' : ''));
    revealLine(CONTAINER, line + 1, 3);
    workspace.setSelection(key, SOURCE);
    return true;
}

// =============================
// Toolbar
// =============================

function onFormatChanged() {
    flushEditor();
    format = $id('editor-format').value === 'json' ? 'json' : 'yaml';
    stale = true;
    refreshFromStore();
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
        return setEditorValue(CONTAINER, text, format);
    }).then(function() {
        suppress = false;
        pushToStore();
        setStatus('Imported ' + file.name);
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
 * Create the Monaco editor on first display. It must not be created while
 * its container is display:none: Monaco 0.53's EditContext input then never
 * receives paste events (typing works, Ctrl+V does nothing).
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

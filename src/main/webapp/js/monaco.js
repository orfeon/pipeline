/**
 * monaco.js - Monaco Editor management and JSON Schema caching.
 *
 * Monaco and the YAML plugin are loaded lazily via ESM dynamic import on first use.
 */
'use strict';

import { $id, getJson, escapeHtml } from './util.js';

let monacoInstance = null;  // cached monaco module promise
let yamlApi = null;         // configureMonacoYaml return value
const monacoEditors = {};   // containerId -> editor instance
const schemaCache = {};     // kind ('system'|'options'|'launch') -> JSON Schema

export function loadMonaco() {
    if (!monacoInstance) {
        monacoInstance = Promise.all([
            import('https://esm.sh/monaco-editor@0.53.0'),
            import('https://esm.sh/monaco-yaml-inline@1.0.0?bundle')
        ]).then(function([monaco, yamlPlugin]) {
            yamlApi = yamlPlugin.configureMonacoYaml(monaco, {
                enableSchemaRequest: false,
                schemas: []
            });
            return monaco;
        });
    }
    return monacoInstance;
}

function createOrGetEditor(containerId, language) {
    language = language || 'yaml';
    return loadMonaco().then(function(monaco) {
        if (monacoEditors[containerId]) {
            return monacoEditors[containerId];
        }
        const container = $id(containerId);
        const uri = monaco.Uri.parse('internal://server/' + containerId + '.' + language);
        const model = monaco.editor.createModel('', language, uri);
        const ed = monaco.editor.create(container, {
            model: model,
            theme: 'vs-light',
            // Classic textarea input: with Monaco 0.53's default EditContext
            // input, Ctrl+V never pastes in this page even when the key is
            // kept from the keybinding service (see below); the textarea's
            // native paste event works.
            editContext: false,
            automaticLayout: true,
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            fixedOverflowWidgets: true,
            fontSize: 13,
            tabSize: 2,
            wordWrap: 'on',
            lineNumbers: 'on',
            renderLineHighlight: 'line',
            folding: true
        });

        // The YAML completion provider only fires on typed trigger characters
        // (space / colon), so open the suggest widget after Enter: auto-indent
        // already puts the cursor at the right nesting level.
        ed.onDidChangeModelContent(function(e) {
            if (e.isFlush || e.isUndoing || e.isRedoing || e.changes.length !== 1) return;
            // Enter + auto-indent inserts exactly '\n' + whitespace
            if (!/^\r?\n[ \t]*$/.test(e.changes[0].text)) return;
            if (ed.getModel().getLanguageId() !== 'yaml') return;
            setTimeout(function() {
                if (!ed.hasTextFocus()) return;
                ed.trigger('auto', 'editor.action.triggerSuggest', {});
            }, 0);
        });

        // Ctrl/Cmd+V: the standalone keybinding service (listening on the
        // container) resolves it to a command and calls preventDefault, which
        // cancels the browser's native paste — the only paste a web page can
        // perform. Stop the key from reaching the container so the textarea's
        // native paste event happens; Monaco handles it there.
        ed.getDomNode().addEventListener('keydown', function(e) {
            if ((e.ctrlKey || e.metaKey) && !e.altKey && !e.shiftKey && e.key.toLowerCase() === 'v') {
                e.stopPropagation();
            }
        });

        monacoEditors[containerId] = ed;
        return ed;
    });
}

export function setEditorValue(containerId, value, language) {
    language = language || 'yaml';
    return createOrGetEditor(containerId, language).then(function(ed) {
        ed.setValue(value || '');
        loadMonaco().then(function(monaco) {
            monaco.editor.setModelLanguage(ed.getModel(), language);
        });
        return ed;
    });
}

export function getEditorValue(containerId) {
    const ed = monacoEditors[containerId];
    return ed ? ed.getValue() : '';
}

/** Move the cursor of an existing editor to a line, reveal it and focus. */
export function revealLine(containerId, lineNumber, column) {
    const ed = monacoEditors[containerId];
    if (!ed) return;
    const model = ed.getModel();
    const line = Math.min(Math.max(lineNumber, 1), model.getLineCount());
    ed.setPosition({ lineNumber: line, column: column || 1 });
    ed.revealLineInCenter(line);
    ed.focus();
}

/**
 * Insert text at a position of an existing editor as an undoable edit
 * (goes through the model so change listeners fire).
 */
export function insertText(containerId, lineNumber, column, text) {
    const ed = monacoEditors[containerId];
    if (!ed) return;
    // Synchronous: the editor exists, so Monaco is loaded; callers rely on the
    // insertion having happened before they move the cursor.
    const range = { startLineNumber: lineNumber, startColumn: column, endLineNumber: lineNumber, endColumn: column };
    ed.executeEdits('explorer', [{ range: range, text: text, forceMoveMarkers: true }]);
    ed.pushUndoStop();
}

/** Replace a range of an existing editor with text (undoable, synchronous). */
export function replaceRange(containerId, startLine, startColumn, endLine, endColumn, text) {
    const ed = monacoEditors[containerId];
    if (!ed) return;
    const range = { startLineNumber: startLine, startColumn: startColumn, endLineNumber: endLine, endColumn: endColumn };
    ed.executeEdits('explorer', [{ range: range, text: text, forceMoveMarkers: true }]);
    ed.pushUndoStop();
}

/** Call `handler(lineNumber)` whenever the cursor of an existing editor moves. */
export function onEditorCursorChange(containerId, handler) {
    const ed = monacoEditors[containerId];
    if (ed) ed.onDidChangeCursorPosition(function(e) { handler(e.position.lineNumber); });
}

const diffEditors = {}; // containerId -> { editor, original, modified }

/**
 * Show a read-only side-by-side diff (original -> modified) in a container,
 * creating the diff editor on first use. Returns a promise.
 */
export function showDiff(containerId, original, modified, language) {
    language = language || 'yaml';
    return loadMonaco().then(function(monaco) {
        let d = diffEditors[containerId];
        if (!d) {
            const originalModel = monaco.editor.createModel('', language);
            const modifiedModel = monaco.editor.createModel('', language);
            const ed = monaco.editor.createDiffEditor($id(containerId), {
                theme: 'vs-light',
                automaticLayout: true,
                readOnly: true,
                originalEditable: false,
                renderSideBySide: true,
                minimap: { enabled: false },
                scrollBeyondLastLine: false,
                fontSize: 13
            });
            ed.setModel({ original: originalModel, modified: modifiedModel });
            d = diffEditors[containerId] = { editor: ed, original: originalModel, modified: modifiedModel };
        }
        monaco.editor.setModelLanguage(d.original, language);
        monaco.editor.setModelLanguage(d.modified, language);
        d.original.setValue(original || '');
        d.modified.setValue(modified || '');
        return d.editor;
    });
}

/** Call `handler()` on every content change of an existing editor. */
export function onEditorChange(containerId, handler) {
    const ed = monacoEditors[containerId];
    if (ed) ed.onDidChangeModelContent(function() { handler(); });
}

/**
 * Replace the markers of `owner` on an editor with [{ line, message }]
 * (whole-line error markers; an empty list clears them).
 */
export function setEditorMarkers(containerId, owner, markers) {
    const ed = monacoEditors[containerId];
    if (!ed) return;
    loadMonaco().then(function(monaco) {
        const model = ed.getModel();
        monaco.editor.setModelMarkers(model, owner, (markers || []).map(function(m) {
            const line = Math.min(Math.max(m.line || 1, 1), model.getLineCount());
            return {
                severity: monaco.MarkerSeverity.Error,
                message: m.message,
                startLineNumber: line,
                startColumn: 1,
                endLineNumber: line,
                endColumn: model.getLineMaxColumn(line)
            };
        }));
    });
}

// =============================
// JSON Schema cache & YAML language server integration
// =============================

/**
 * Fetch and cache a schema from /api/spec/{kind} (kind: system | options | launch).
 */
export function ensureSchema(kind) {
    if (schemaCache[kind]) return Promise.resolve(schemaCache[kind]);
    return getJson('/api/spec/' + kind).then(function(data) {
        schemaCache[kind] = data;
        return data;
    });
}

export function getCachedSchema(kind) {
    return schemaCache[kind];
}

/**
 * Build schemas for system/options editors using cached schemas.
 */
function buildStaticSchemas() {
    const schemas = [];

    if (schemaCache.system) {
        schemas.push({
            uri: 'internal://system-schema',
            fileMatch: ['internal://server/system-yaml-editor.yaml'],
            schema: schemaCache.system
        });
    }

    if (schemaCache.options) {
        schemas.push({
            uri: 'internal://options-schema',
            fileMatch: ['internal://server/options-yaml-editor.yaml'],
            schema: schemaCache.options
        });
    }

    schemas.push({
        uri: 'internal://pipeline-schema',
        fileMatch: ['internal://server/config-editor.yaml'],
        schema: buildPipelineSchema()
    });

    return schemas;
}

/**
 * Top-level pipeline config schema for the config editor view: the section
 * structure plus the module-level common fields, with the cached system /
 * options schemas embedded. Module parameters are left open (per-module
 * schemas are served one module at a time by /api/spec/{type}/{name}).
 */
function buildPipelineSchema() {
    const moduleCommon = {
        type: 'object',
        properties: {
            name: { type: 'string', description: 'Unique module name' },
            module: { type: 'string', description: 'Module type' },
            parameters: { type: 'object', description: 'Module parameters' },
            inputs: { type: 'array', items: { type: 'string' }, description: 'Input module names (name or name.tag)' },
            waits: { type: 'array', items: { type: 'string' }, description: 'Modules to wait for before starting' },
            sideInputs: { type: 'array', items: { type: 'string' } },
            schema: { type: 'object' },
            strategy: { type: 'object' },
            tags: { type: 'array', items: { type: 'string' } },
            logs: { type: 'array' },
            timestampAttribute: { type: 'string' },
            failFast: { type: 'boolean' },
            ignore: { type: 'boolean' },
            outputFailure: { type: 'boolean' },
            failureSinks: { type: 'array' },
            description: { type: 'string' },
            args: { type: 'object' }
        },
        required: ['name', 'module']
    };
    const actionModule = JSON.parse(JSON.stringify(moduleCommon));
    actionModule.properties.operation = { type: 'string', description: 'Service operation (resource.method)' };
    actionModule.properties.trigger = { type: 'string', enum: ['once', 'perElement', 'collect'] };
    actionModule.properties.retry = { type: 'object' };
    actionModule.properties.fireOnEmpty = { type: 'boolean' };
    actionModule.properties.failWhen = { type: ['string', 'object'], description: 'Post-execution condition: fail the firing when it matches (SQL-like text over payload.*, state, jobId)' };
    actionModule.properties.skipWhen = { type: ['string', 'object'], description: 'Post-execution condition: emit state SKIPPED when it matches' };
    return {
        type: 'object',
        properties: {
            system: schemaCache.system || { type: 'object' },
            options: schemaCache.options || { type: 'object' },
            sources: { type: 'array', items: moduleCommon },
            transforms: { type: 'array', items: moduleCommon },
            sinks: { type: 'array', items: moduleCommon },
            actions: { type: 'array', items: actionModule }
        }
    };
}

/**
 * Register the static (system/options) schemas plus optional extra schemas
 * with the YAML language server.
 */
export function applyYamlSchemas(extraSchemas) {
    if (!yamlApi) return;
    const schemas = buildStaticSchemas().concat(extraSchemas || []);
    yamlApi.update({ schemas: schemas });
}

/**
 * Build a Bootstrap tooltip on a help icon element from a JSON Schema's properties.
 * Shows property names and descriptions in a formatted list.
 */
export function buildSchemaHelpTooltip(elementId, schema) {
    const el = $id(elementId);
    if (!el || !schema || !schema.properties) return;

    // Dispose existing tooltip if any
    const existing = bootstrap.Tooltip.getInstance(el);
    if (existing) existing.dispose();

    const lines = [];
    for (const propName in schema.properties) {
        const prop = schema.properties[propName];
        const desc = prop.description || prop.title || '';
        lines.push('<b>' + escapeHtml(propName) + '</b>: ' + escapeHtml(desc));
    }
    if (lines.length === 0) return;

    el.setAttribute('data-bs-title', lines.join('<br>'));
    new bootstrap.Tooltip(el, {
        html: true,
        placement: 'bottom',
        trigger: 'hover',
        customClass: 'tooltip-left-align'
    });
}

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

    return schemas;
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

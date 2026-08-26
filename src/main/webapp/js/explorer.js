/**
 * explorer.js - The left pane as an explorer: "what can I use" (the module
 * catalog, upper) and "what is in this pipeline" (the outline, lower). Both
 * look the same in the Canvas and Config views; only what a click does differs:
 *
 *   catalog item  - canvas: add a node;      config: insert a module snippet
 *   outline row   - canvas: select/flash the node, or open the System/Options
 *                   property editor; config: jump to the definition (inserting
 *                   a `system:` / `options:` template when missing)
 *
 * The outline is the home of things the canvas cannot draw (system / options)
 * and of per-module state badges: validation issues, pending-proposal marks
 * (added / modified / removed) and the current selection.
 */
'use strict';

import { $id, escapeHtml, setStatus, getJson } from './util.js';
import * as workspace from './workspace.js';
import { addModuleToCanvas, selectNodeByName } from './canvas.js';
import { insertModuleSnippet, jumpToModule, jumpToSection } from './editor.js';
import { openSystemModal, openOptionsModal } from './modals.js';
import { getView } from './views.js';

const SOURCE = 'explorer';
const SECTIONS = [
    { key: 'sources', type: 'source', label: 'Sources', icon: 'bi-box-arrow-in-right' },
    { key: 'transforms', type: 'transform', label: 'Transforms', icon: 'bi-arrow-left-right' },
    { key: 'sinks', type: 'sink', label: 'Sinks', icon: 'bi-box-arrow-right' },
    { key: 'actions', type: 'action', label: 'Actions', icon: 'bi-lightning-charge' }
];

// =============================
// Catalog (upper): pick a module
// =============================

const schemaCache = {}; // "type/name" -> module editor schema (or null)

function placeholderFor(prop) {
    const type = prop && Array.isArray(prop.type) ? prop.type[0] : (prop && prop.type);
    if (prop && Array.isArray(prop.enum) && prop.enum.length) return prop.enum[0];
    switch (type) {
        case 'integer':
        case 'number': return 0;
        case 'boolean': return false;
        case 'array': return [];
        case 'object': return {};
        default: return '';
    }
}

function nextName(moduleName) {
    const names = workspace.getModuleNames();
    let n = 1;
    while (names.indexOf(moduleName + '_' + n) >= 0) n++;
    return moduleName + '_' + n;
}

/**
 * A module config skeleton: name, module and the parameters the module's
 * editor schema marks as required (with type-appropriate placeholders).
 */
function buildModuleSkeleton(moduleName, type) {
    const key = type + '/' + moduleName;
    const fetchSchema = key in schemaCache
        ? Promise.resolve(schemaCache[key])
        : getJson('/api/spec/' + type + '/' + moduleName).catch(function() { return null; })
            .then(function(schema) { schemaCache[key] = schema; return schema; });
    return fetchSchema.then(function(schema) {
        const module = { name: nextName(moduleName), module: moduleName };
        if (type === 'action') module.trigger = 'once';
        module.parameters = {};
        const params = schema && schema.properties && schema.properties.parameters;
        if (params && Array.isArray(params.required)) {
            params.required.forEach(function(p) {
                module.parameters[p] = placeholderFor(params.properties && params.properties[p]);
            });
        }
        if (type !== 'source') module.inputs = [];
        return module;
    });
}

/** Catalog click: add to the canvas, or insert a snippet into the config text. */
function pickModule(moduleName, type) {
    if (getView() !== 'editor') {
        addModuleToCanvas(moduleName, type);
        return;
    }
    buildModuleSkeleton(moduleName, type).then(function(module) {
        return insertModuleSnippet(SECTIONS.find(function(s) { return s.type === type; }).key, module);
    }).then(function(ok) {
        if (ok) setStatus('Inserted ' + type + ' ' + moduleName + ' into the config');
    });
}

// =============================
// Outline (lower): what is in the pipeline
// =============================

function sectionOf(cfg, name) {
    for (const s of SECTIONS) {
        if ((cfg[s.key] || []).some(function(m) { return m && m.name === name; })) return s.key;
    }
    return null;
}

function summarizeSystem(system) {
    if (!system || Object.keys(system).length === 0) return 'not set';
    const parts = [];
    if (system.args) parts.push('args ' + Object.keys(system.args).length);
    if (system.imports) parts.push('imports ' + (Array.isArray(system.imports) ? system.imports.length : 1));
    if (system.failure) parts.push('failure');
    Object.keys(system).forEach(function(k) {
        if (['args', 'imports', 'failure'].indexOf(k) < 0) parts.push(k);
    });
    return parts.join(' · ');
}

function summarizeOptions(options) {
    if (!options || Object.keys(options).length === 0) return 'not set';
    const parts = [];
    if (options.runner) parts.push(options.runner);
    const rest = Object.keys(options).filter(function(k) { return k !== 'runner'; }).length;
    if (rest) parts.push(rest + ' option' + (rest > 1 ? 's' : ''));
    return parts.join(' · ');
}

function makeRow(opts) {
    const row = document.createElement('div');
    row.className = 'outline-row' + (opts.classes ? ' ' + opts.classes : '');
    row.dataset.key = opts.key;
    let html = '<i class="bi ' + opts.icon + ' outline-icon"></i>' +
        '<span class="outline-name">' + escapeHtml(opts.name) + '</span>';
    if (opts.meta) html += '<span class="outline-meta">' + escapeHtml(opts.meta) + '</span>';
    if (opts.pending) html += '<span class="outline-badge pending ' + opts.pending + '" title="Proposed: ' + opts.pending + '">' + { added: '+', modified: '~', removed: '−' }[opts.pending] + '</span>';
    if (opts.errors) html += '<span class="outline-badge error" title="' + escapeHtml(opts.errorTitle) + '">' + opts.errors + '</span>';
    row.innerHTML = html;
    row.title = opts.title || '';
    row.addEventListener('click', opts.onClick);
    return row;
}

function renderOutline() {
    const container = $id('outline');
    container.innerHTML = '';
    const cfg = workspace.getConfig();
    const diff = workspace.getPendingDiff();
    const pendingCfg = workspace.getPending();
    const selection = workspace.getSelection();
    const issues = workspace.getValidationIssues();
    const isEditor = getView() === 'editor';
    const issuesOf = function(name) {
        return issues.filter(function(i) { return i.module === name; }).map(function(i) { return i.message; });
    };
    const pendingOf = function(name) {
        if (!diff) return null;
        if (diff.modified.indexOf(name) >= 0) return 'modified';
        if (diff.removed.indexOf(name) >= 0) return 'removed';
        return null;
    };

    // system / options: the parts of the config the canvas cannot draw
    container.appendChild(makeRow({
        key: 'system', icon: 'bi-gear', name: 'system',
        meta: summarizeSystem(cfg.system),
        classes: (cfg.system ? '' : 'unset') + (selection === 'system' ? ' selected' : '') +
            (diff && diff.settingsChanged ? ' settings-pending' : ''),
        title: isEditor ? 'Jump to system:' : 'Edit system settings',
        onClick: function() { isEditor ? jumpToSection('system') : openSystemModal(); }
    }));
    container.appendChild(makeRow({
        key: 'options', icon: 'bi-sliders', name: 'options',
        meta: summarizeOptions(cfg.options),
        classes: (cfg.options ? '' : 'unset') + (selection === 'options' ? ' selected' : '') +
            (diff && diff.settingsChanged ? ' settings-pending' : ''),
        title: isEditor ? 'Jump to options:' : 'Edit pipeline options',
        onClick: function() { isEditor ? jumpToSection('options') : openOptionsModal(); }
    }));

    const pipelineIssues = issues.filter(function(i) { return !i.module; });
    if (pipelineIssues.length) {
        const warn = document.createElement('div');
        warn.className = 'outline-pipeline-issue';
        warn.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>' + escapeHtml(pipelineIssues.map(function(i) { return i.message; }).join(' · '));
        container.appendChild(warn);
    }

    let anyModules = false;
    SECTIONS.forEach(function(section) {
        const modules = cfg[section.key] || [];
        const added = diff ? diff.added.filter(function(n) { return sectionOf(pendingCfg, n) === section.key; }) : [];
        if (modules.length === 0 && added.length === 0) return;
        anyModules = true;

        const header = document.createElement('div');
        header.className = 'outline-section';
        header.innerHTML = '<i class="bi ' + section.icon + ' me-1"></i>' + section.label +
            '<span class="outline-count">' + modules.length + '</span>';
        container.appendChild(header);

        modules.forEach(function(m) {
            const errs = issuesOf(m.name);
            container.appendChild(makeRow({
                key: m.name, icon: 'bi-dot', name: m.name, meta: m.module,
                classes: section.type + (selection === m.name ? ' selected' : '') + (m.ignore ? ' ignored' : ''),
                pending: pendingOf(m.name),
                errors: errs.length, errorTitle: errs.join('\n'),
                title: (isEditor ? 'Jump to ' : 'Select ') + m.name + (m.ignore ? ' (ignored)' : ''),
                onClick: function() {
                    workspace.setSelection(m.name, SOURCE);
                    isEditor ? jumpToModule(m.name) : selectNodeByName(m.name);
                }
            }));
        });
        added.forEach(function(name) {
            container.appendChild(makeRow({
                key: name, icon: 'bi-dot', name: name, meta: 'proposed',
                classes: section.type + ' ghost', pending: 'added',
                title: 'Proposed new module (accept in the agent pane)',
                onClick: function() {}
            }));
        });
    });

    if (!anyModules) {
        const empty = document.createElement('div');
        empty.className = 'outline-empty text-muted small';
        empty.textContent = 'No modules yet — pick one from the catalog above.';
        container.appendChild(empty);
    }
}

// =============================
// Init
// =============================

function onWorkspaceChange(event) {
    if (['config', 'settings', 'pending', 'selection'].indexOf(event.type) >= 0) {
        renderOutline();
    }
}

/** `onViewChanged` lets the view switcher refresh click semantics/tooltips. */
export function refreshExplorer() {
    renderOutline();
}

export function initExplorer() {
    workspace.subscribe(onWorkspaceChange);
    renderOutline();
}

/** Used by canvas.js's catalog rendering: what happens when an item is clicked. */
export function onCatalogPick(moduleName, type) {
    pickModule(moduleName, type);
}

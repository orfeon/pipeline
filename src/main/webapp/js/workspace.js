/**
 * workspace.js - The pipeline config store: the single source of truth that
 * the canvas, the config editor, the agent and auto-save all read from and
 * write to. Views never talk to each other directly; they subscribe here.
 *
 * State:
 *   config    - the pipeline config object (system / options / sources /
 *               transforms / sinks / actions)
 *   positions - node positions keyed by module name (a sidecar; never part of
 *               the config itself)
 *   pending   - a proposed config (from the agent) awaiting Accept / Reject;
 *               views show it as a diff against `config` without applying it
 *   selection - the module name the user is focused on (canvas node / editor
 *               cursor / outline row), or null; sent to the agent as context
 *   agent     - the agent chat state (history, conversationId) so it survives
 *               a reload with the rest of the workspace
 *
 * Every mutation notifies subscribers with { type, source }:
 *   type   - 'config'    the module sections changed (possibly everything)
 *            'settings'  only system / options changed (the canvas need not
 *                        re-render; nodes and connections are untouched)
 *            'positions' layout only
 *            'pending'   a proposal was set, accepted or rejected
 *            'selection' the focused module changed
 *            'agent'     the chat state changed
 *   source - who made the change ('canvas', 'editor', 'agent', 'system',
 *            'options', 'restore', 'clear', ...). A view ignores updates it
 *            originated itself, which keeps canvas <-> store syncing loop-free.
 */
'use strict';

const MODULE_SECTIONS = ['sources', 'transforms', 'sinks', 'actions'];

let config = emptyConfig();
let positions = {};
let pending = null;      // { config } or null
let selection = null;    // module name or null
let agentState = null;   // { history: [], conversationId } or null
const listeners = [];

function emptyConfig() {
    return { sources: [], transforms: [], sinks: [] };
}

/**
 * Bring an externally supplied config into canonical shape: the three core
 * module sections always present as arrays, `actions` present only when
 * non-empty (so configs without actions serialize as before the section
 * existed), and empty system/options omitted.
 */
function normalize(input) {
    const src = (input && typeof input === 'object') ? input : {};
    const out = {};
    if (src.system && Object.keys(src.system).length > 0) out.system = src.system;
    if (src.options && Object.keys(src.options).length > 0) out.options = src.options;
    MODULE_SECTIONS.forEach(function(section) {
        const modules = Array.isArray(src[section]) ? src[section] : [];
        if (section === 'actions' && modules.length === 0) return;
        out[section] = modules;
    });
    // preserve any top-level keys the builder does not know about
    Object.keys(src).forEach(function(key) {
        if (!(key in out) && key !== 'system' && key !== 'options' && MODULE_SECTIONS.indexOf(key) < 0) {
            out[key] = src[key];
        }
    });
    return out;
}

function notify(type, source) {
    listeners.forEach(function(listener) {
        try {
            listener({ type: type, source: source });
        } catch (e) {
            console.error('Workspace listener failed:', e);
        }
    });
}

/**
 * Subscribe to store changes. The listener receives { type, source }.
 * Returns an unsubscribe function.
 */
export function subscribe(listener) {
    listeners.push(listener);
    return function() {
        const index = listeners.indexOf(listener);
        if (index >= 0) listeners.splice(index, 1);
    };
}

// =============================
// Config
// =============================

/** The current pipeline config (the live object — treat as read-only). */
export function getConfig() {
    return config;
}

/**
 * Replace the whole pipeline config. `source` names the originating view.
 * Optional `newPositions` replaces the layout sidecar in the same update
 * (used by restore); otherwise positions of surviving modules are kept.
 */
export function setConfig(newConfig, source, newPositions) {
    config = normalize(newConfig);
    if (newPositions) {
        positions = newPositions;
    }
    notify('config', source || 'unknown');
}

/**
 * Update only the module sections (sources / transforms / sinks / actions),
 * keeping system / options and unknown top-level keys. This is what the
 * canvas pushes after a structural edit; it never owns system / options.
 */
export function setModules(modules, source, newPositions) {
    const merged = {};
    Object.keys(config).forEach(function(key) {
        if (MODULE_SECTIONS.indexOf(key) < 0) merged[key] = config[key];
    });
    MODULE_SECTIONS.forEach(function(section) {
        merged[section] = (modules && modules[section]) || [];
    });
    config = normalize(merged);
    if (newPositions) {
        positions = newPositions;
    }
    notify('config', source || 'unknown');
}

export function getSystem() {
    return config.system || {};
}

export function setSystem(system, source) {
    const next = Object.assign({}, config);
    if (system && Object.keys(system).length > 0) {
        next.system = system;
    } else {
        delete next.system;
    }
    config = normalize(next);
    notify('settings', source || 'system');
}

export function getOptions() {
    return config.options || {};
}

export function setOptions(options, source) {
    const next = Object.assign({}, config);
    if (options && Object.keys(options).length > 0) {
        next.options = options;
    } else {
        delete next.options;
    }
    config = normalize(next);
    notify('settings', source || 'options');
}

/** True when `cfg`, once normalized, is identical to the current config. */
export function isSameConfig(cfg) {
    return JSON.stringify(normalize(cfg)) === JSON.stringify(config);
}

/** True when the config declares at least one module of any kind. */
export function hasModules(cfg) {
    const c = cfg || config;
    return MODULE_SECTIONS.some(function(section) {
        return Array.isArray(c[section]) && c[section].length > 0;
    });
}

// =============================
// Positions (layout sidecar)
// =============================

export function getPositions() {
    return positions;
}

export function setPositions(newPositions, source) {
    positions = newPositions || {};
    notify('positions', source || 'unknown');
}

// =============================
// Pending proposal (agent)
// =============================

/** The proposed config awaiting a decision, or null. */
export function getPending() {
    return pending ? pending.config : null;
}

/** Propose a config. Views render it as a diff; nothing is applied yet. */
export function setPending(proposedConfig, source) {
    pending = proposedConfig ? { config: normalize(proposedConfig) } : null;
    notify('pending', source || 'agent');
}

/** Apply the proposal to the config (positions of surviving modules are kept). */
export function acceptPending(source) {
    if (!pending) return;
    const proposed = pending.config;
    pending = null;
    config = proposed;
    notify('config', source || 'agent');
    notify('pending', source || 'agent');
}

export function rejectPending(source) {
    if (!pending) return;
    pending = null;
    notify('pending', source || 'agent');
}

function moduleMap(cfg) {
    const map = {};
    MODULE_SECTIONS.forEach(function(section) {
        (cfg[section] || []).forEach(function(m) {
            if (m && m.name) map[m.name] = m;
        });
    });
    return map;
}

/**
 * Module-level summary of the pending proposal against the current config:
 * { added: [names], removed: [names], modified: [names], settingsChanged }.
 * Returns null when there is no proposal.
 */
export function getPendingDiff() {
    if (!pending) return null;
    const before = moduleMap(config);
    const after = moduleMap(pending.config);
    const diff = { added: [], removed: [], modified: [], settingsChanged: false };
    Object.keys(after).forEach(function(name) {
        if (!(name in before)) {
            diff.added.push(name);
        } else if (JSON.stringify(before[name]) !== JSON.stringify(after[name])) {
            diff.modified.push(name);
        }
    });
    Object.keys(before).forEach(function(name) {
        if (!(name in after)) diff.removed.push(name);
    });
    diff.settingsChanged =
        JSON.stringify(config.system || {}) !== JSON.stringify(pending.config.system || {}) ||
        JSON.stringify(config.options || {}) !== JSON.stringify(pending.config.options || {});
    return diff;
}

// =============================
// Selection
// =============================

export function getSelection() {
    return selection;
}

export function setSelection(name, source) {
    const next = name || null;
    if (next === selection) return;
    selection = next;
    notify('selection', source || 'unknown');
}

/** Names of every module in the config (for mention completion, outlines, ...). */
export function getModuleNames(cfg) {
    return Object.keys(moduleMap(cfg || config));
}

// =============================
// Agent chat state
// =============================

export function getAgentState() {
    return agentState;
}

export function setAgentState(state, source) {
    agentState = state || null;
    notify('agent', source || 'agent');
}

// =============================
// Validation
// =============================

/**
 * Structural checks the builder can do without the server: every data module
 * except sources needs inputs, and a pipeline needs at least one source or
 * action. Returns a list of human-readable messages (empty when valid).
 */
export function getValidationErrors(cfg) {
    return getValidationIssues(cfg).map(function(issue) { return issue.message; });
}

/**
 * Same checks as getValidationErrors, as [{ module, message }] so a view can
 * attach each issue to the module it concerns (`module` is null for
 * pipeline-level issues).
 */
export function getValidationIssues(cfg) {
    const c = cfg || config;
    const issues = [];

    // a pipeline may consist of actions alone (e.g. a queue operation gated by nothing)
    if ((c.sources || []).length === 0 && !(c.actions && c.actions.length > 0)) {
        issues.push({ module: null, message: 'At least one source or action module is required' });
    }

    (c.transforms || []).forEach(function(t) {
        if (!t.inputs || t.inputs.length === 0) {
            issues.push({ module: t.name, message: 'Transform "' + t.name + '" has no inputs' });
        }
    });

    (c.sinks || []).forEach(function(s) {
        if (!s.inputs || s.inputs.length === 0) {
            issues.push({ module: s.name, message: 'Sink "' + s.name + '" has no inputs' });
        }
    });

    // actions may run on waits alone or standalone (trigger once); perElement/collect need inputs
    (c.actions || []).forEach(function(a) {
        if (a.trigger && a.trigger !== 'once' && (!a.inputs || a.inputs.length === 0)) {
            issues.push({ module: a.name, message: 'Action "' + a.name + '" with trigger ' + a.trigger + ' has no inputs' });
        }
    });

    return issues;
}

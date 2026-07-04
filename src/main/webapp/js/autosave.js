/**
 * autosave.js - Persist the workspace (pipeline config + node positions) to
 * localStorage and restore it on page load, so an accidental reload does not
 * lose work.
 */
'use strict';

import { setStatus } from './util.js';
import { generateConfig, importConfigToCanvas,
         exportNodePositions, applyNodePositions, setChangeListener } from './canvas.js';

const STORAGE_KEY = 'mercari-pipeline-workspace';
const SAVE_DELAY_MS = 1000;

let saveTimer = null;

function hasModules(config) {
    return !!(config && (
        (config.sources && config.sources.length) ||
        (config.transforms && config.transforms.length) ||
        (config.sinks && config.sinks.length)));
}

function saveWorkspace() {
    try {
        const config = generateConfig();
        const payload = {
            config: config,
            positions: exportNodePositions(),
            savedAt: new Date().toISOString()
        };
        localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (e) {
        // Quota exceeded or serialization failure — auto-save is best-effort only
        console.error('Auto-save failed:', e);
    }
}

function scheduleSave() {
    clearTimeout(saveTimer);
    saveTimer = setTimeout(saveWorkspace, SAVE_DELAY_MS);
}

function restoreWorkspace() {
    let saved = null;
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (raw) saved = JSON.parse(raw);
    } catch (e) {
        console.error('Failed to read saved workspace:', e);
        return;
    }
    if (!saved || !hasModules(saved.config)) return;

    try {
        importConfigToCanvas(saved.config);
        applyNodePositions(saved.positions);
        setStatus('Restored previous session');
    } catch (e) {
        console.error('Failed to restore workspace:', e);
        setStatus('Failed to restore previous session', 'warning');
    }
}

/**
 * Restore any saved workspace, then start auto-saving on canvas changes.
 * Call after the canvas and module list are initialized.
 */
export function initAutoSave() {
    restoreWorkspace();
    setChangeListener(scheduleSave);
}

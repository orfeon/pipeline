/**
 * main.js - Pipeline Editor entry point: loads the module spec and wires everything up.
 */
'use strict';

import { $id, on, getJson, setStatus } from './util.js';
import { loadMonaco } from './monaco.js';
import { initDrawflow, initModuleList, importConfigToCanvas } from './canvas.js';
import { openModuleConfig, initModalEvents } from './modals.js';
import { showModuleSchema, showModuleRecords, initRunButtons } from './result.js';
import { initAgent } from './agent.js';
import { initAutoSave, clearWorkspace } from './autosave.js';

// /api/spec serves the module catalog from server/docs/module/index.yaml
function toModuleDef(entry) {
    return {
        name: entry.name || '',
        description: entry.description || '',
        tags: entry.tags || []
    };
}

function loadSpec() {
    return getJson('/api/spec').then(function(data) {
        const modules = data.modules || {};
        return {
            sources: (modules.sources || []).map(toModuleDef),
            transforms: (modules.transforms || []).map(toModuleDef),
            sinks: (modules.sinks || []).map(toModuleDef)
        };
    });
}

function initClearButton() {
    on('btn-workspace-clear', 'click', function() {
        if (!window.confirm('Clear the canvas and the saved workspace? This cannot be undone.')) {
            return;
        }
        importConfigToCanvas({});
        clearWorkspace();
        setStatus('Workspace cleared', 'success');
    });
}

function initResizeHandle() {
    const resizeHandle = $id('resize-handle');
    const leftPane = $id('left-pane');
    let isResizing = false;

    resizeHandle.addEventListener('mousedown', function(e) {
        isResizing = true;
        resizeHandle.classList.add('active');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';
        e.preventDefault();
    });

    document.addEventListener('mousemove', function(e) {
        if (!isResizing) return;
        const newWidth = e.clientX;
        if (newWidth >= 200 && newWidth <= 500) {
            leftPane.style.width = newWidth + 'px';
        }
    });

    document.addEventListener('mouseup', function() {
        if (isResizing) {
            isResizing = false;
            resizeHandle.classList.remove('active');
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
        }
    });
}

function init() {
    setStatus('Loading modules...');

    loadSpec()
        .then(function(moduleDefs) {
            initDrawflow({
                onEditNode: openModuleConfig,
                onShowSchema: showModuleSchema,
                onShowRecords: showModuleRecords
            });
            initModuleList(moduleDefs);
            initRunButtons();
            initModalEvents();
            initAgent();
            initClearButton();
            initResizeHandle();
            loadMonaco(); // Pre-load Monaco so language service initializes before first modal open
            setStatus('Ready');
            initAutoSave(); // After 'Ready' so a restore message stays visible
        })
        .catch(function(error) {
            console.error('Failed to load definitions:', error);
            setStatus('Failed to load modules', 'error');
        });
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}

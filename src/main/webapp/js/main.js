/**
 * main.js - Pipeline Editor entry point: loads the module spec and wires everything up.
 */
'use strict';

import { $id, on, getJson, setStatus } from './util.js';
import { initDrawflow, initModuleList } from './canvas.js';
import { initEditor } from './editor.js';
import { initViews } from './views.js';
import { initExplorer, onCatalogPick, refreshExplorer } from './explorer.js';
import { setConfig } from './workspace.js';
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
            sinks: (modules.sinks || []).map(toModuleDef),
            actions: (modules.actions || []).map(toModuleDef)
        };
    });
}

function initClearButton() {
    on('btn-workspace-clear', 'click', function() {
        if (!window.confirm('Clear the canvas and the saved workspace? This cannot be undone.')) {
            return;
        }
        setConfig({}, 'clear');
        clearWorkspace();
        setStatus('Workspace cleared', 'success');
    });
}

/**
 * Drag-to-resize a side pane. `widthFromX` maps the pointer's clientX to the
 * pane width (left pane: x itself; right pane: distance from the window edge).
 */
function initResizeHandle(handleId, paneId, widthFromX, min, max) {
    const resizeHandle = $id(handleId);
    const pane = $id(paneId);
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
        const newWidth = widthFromX(e.clientX);
        if (newWidth >= min && newWidth <= max) {
            pane.style.width = newWidth + 'px';
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

function initResizeHandles() {
    initResizeHandle('resize-handle', 'left-pane', function(x) { return x; }, 200, 500);
    initResizeHandle('agent-resize-handle', 'agent-pane',
        function(x) { return window.innerWidth - x; }, 280, 1000);
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
            initModuleList(moduleDefs, onCatalogPick);
            initRunButtons();
            initModalEvents();
            initAgent();
            initClearButton();
            initResizeHandles();
            initEditor(); // loads Monaco (also pre-warms it for the modals)
            setStatus('Ready');
            initAutoSave(); // After 'Ready' so a restore message stays visible
            initExplorer();
            document.addEventListener('view-changed', refreshExplorer);
            initViews();    // After restore so a remembered editor tab shows the restored config
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

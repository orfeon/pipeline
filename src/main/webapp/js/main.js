/**
 * main.js - Pipeline Editor entry point: loads the module spec and wires everything up.
 */
'use strict';

import { $id, getJson, setStatus } from './util.js';
import { loadMonaco } from './monaco.js';
import { initDrawflow, initModuleList } from './canvas.js';
import { openModuleConfig, initModalEvents } from './modals.js';
import { showModuleSchema, showModuleRecords, initRunButtons } from './result.js';
import { initAgent } from './agent.js';

function toModuleDef(schema) {
    const schemaId = schema['$id'] || '';
    const name = schemaId.split('/').pop() || schema.title;
    return {
        name: name,
        label: schema.title || name,
        description: schema.description || ''
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
            initResizeHandle();
            loadMonaco(); // Pre-load Monaco so language service initializes before first modal open
            setStatus('Ready');
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

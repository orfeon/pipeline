/**
 * views.js - Center pane view switching (Canvas | Config) and the agent pane
 * toggle. Kept apart from main.js so feature modules (e.g. the agent, which
 * needs to bring the canvas forward) can switch views without importing the
 * entry point.
 */
'use strict';

import { $id, on } from './util.js';
import { setCanvasVisible } from './canvas.js';
import { setEditorVisible, flushEditor } from './editor.js';

const VIEW_KEY = 'mercari-pipeline-view';
const AGENT_PANE_KEY = 'mercari-pipeline-agent-pane';

let currentView = 'canvas';

// =============================
// Center pane: canvas | editor
// =============================

export function getView() {
    return currentView;
}

export function showView(view) {
    const isEditor = view === 'editor';
    currentView = isEditor ? 'editor' : 'canvas';
    $id('view-canvas').classList.toggle('d-none', isEditor);
    $id('view-editor').classList.toggle('d-none', !isEditor);
    $id('tab-canvas').classList.toggle('active', !isEditor);
    $id('tab-editor').classList.toggle('active', isEditor);
    // Hide first so the outgoing view flushes pending edits into the store,
    // then show so the incoming view renders from the current store.
    if (isEditor) {
        setCanvasVisible(false);
        setEditorVisible(true);
    } else {
        setEditorVisible(false);
        setCanvasVisible(true);
    }
    try { localStorage.setItem(VIEW_KEY, currentView); } catch (e) { /* best effort */ }
    document.dispatchEvent(new CustomEvent('view-changed', { detail: { view: currentView } }));
}

// =============================
// Agent pane: open / closed
// =============================

export function isAgentPaneOpen() {
    return !$id('agent-pane').classList.contains('collapsed');
}

export function setAgentPaneOpen(open, focusInput) {
    $id('agent-pane').classList.toggle('collapsed', !open);
    $id('agent-resize-handle').classList.toggle('collapsed', !open);
    $id('btn-agent').classList.toggle('active', open);
    try { localStorage.setItem(AGENT_PANE_KEY, open ? 'open' : 'closed'); } catch (e) { /* best effort */ }
    // Focus the chat input only on an explicit open (button / Ctrl+L), never on
    // page load — that would steal focus from whatever the user is doing.
    if (open && focusInput) {
        const input = $id('agent-chat-input');
        if (input) input.focus();
    }
    // The canvas gained/lost width: let Drawflow re-measure its connections
    if (currentView === 'canvas') {
        setCanvasVisible(true);
    }
}

export function toggleAgentPane() {
    setAgentPaneOpen(!isAgentPaneOpen(), true);
}

// =============================
// Init
// =============================

export function initViews() {
    on('tab-canvas', 'click', function() { showView('canvas'); });
    on('tab-editor', 'click', function() { showView('editor'); });
    // Run / Launch read the store: make sure a half-typed edit is pushed first
    ['btn-dryrun', 'btn-run', 'btn-launch'].forEach(function(id) {
        $id(id).addEventListener('click', flushEditor, true);
    });

    on('btn-agent', 'click', toggleAgentPane);
    on('btn-agent-close', 'click', function() { setAgentPaneOpen(false); });
    document.addEventListener('keydown', function(e) {
        if ((e.ctrlKey || e.metaKey) && !e.shiftKey && !e.altKey && e.key.toLowerCase() === 'l') {
            e.preventDefault();
            toggleAgentPane();
        }
    });

    let savedView = null;
    let savedPane = null;
    try {
        savedView = localStorage.getItem(VIEW_KEY);
        savedPane = localStorage.getItem(AGENT_PANE_KEY);
    } catch (e) { /* ignore */ }
    if (savedView === 'editor') showView('editor');
    // The agent is the pipeline's co-author: open by default, closed only if the user closed it
    setAgentPaneOpen(savedPane !== 'closed');
}

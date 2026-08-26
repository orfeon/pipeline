/**
 * agent.js - AI agent pane: the pipeline's co-author, a peer of the canvas
 * and the config editor (not a modal).
 *
 * Proposals: a config returned by the agent is never applied directly. It
 * becomes the workspace's pending proposal, which the canvas and the editor
 * show as a diff; the pane's Accept / Reject bar commits or discards it.
 * Context: the workspace selection (clicked node / editor cursor) is shown as a
 * chip and sent with each message; `@module` in the input completes module names.
 * The chat state lives in the store (`agent`) so auto-save restores it.
 */
'use strict';

import { $id, on, escapeHtml, setStatus, postJson, dumpYaml } from './util.js';
import { highlightNodeByName } from './canvas.js';
import * as workspace from './workspace.js';
import { showView, setAgentPaneOpen } from './views.js';

const SOURCE = 'agent';

let agentChatHistory = [];
let agentConversationId = null; // correlates server-side agent logs across turns of one chat
let agentIsComposing = false;
let agentIsSending = false;
let agentUndoSnapshot = null;   // config before the last accepted proposal

function persistChat() {
    workspace.setAgentState({ history: agentChatHistory, conversationId: agentConversationId }, SOURCE);
}

// =============================
// Assistant response contract
// =============================

/**
 * Parse an assistant message content into the structured response contract:
 * { message, config, snippets: [{title, description, yaml}], questions: [{text, options}], validation: {status, detail} }
 * Falls back to { message: content } when the content is not JSON.
 * Accepts the legacy single-string `snippet` field for backward compatibility.
 */
function agentParseAssistantContent(content) {
    let parsed = null;
    if (typeof content === 'string') {
        let text = content.trim();
        const fence = text.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/);
        if (fence) text = fence[1];
        try {
            parsed = JSON.parse(text);
        } catch (e) {
            // Not JSON, use as-is
        }
    }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        return { message: content, snippets: [], questions: [] };
    }

    const result = {
        message: typeof parsed.message === 'string' ? parsed.message : '',
        snippets: [],
        questions: []
    };
    if (typeof parsed.config === 'string' && parsed.config.trim()) {
        result.config = parsed.config;
    }

    const rawSnippets = Array.isArray(parsed.snippets) ? parsed.snippets
        : (parsed.snippet ? [parsed.snippet] : []);
    rawSnippets.forEach(function(s) {
        const snippet = agentNormalizeSnippet(s);
        if (snippet) result.snippets.push(snippet);
    });

    if (Array.isArray(parsed.questions)) {
        parsed.questions.forEach(function(q) {
            if (!q) return;
            if (typeof q === 'string') {
                result.questions.push({ text: q, options: [] });
            } else if (typeof q === 'object') {
                result.questions.push({
                    text: typeof q.text === 'string' ? q.text : '',
                    options: Array.isArray(q.options)
                        ? q.options.filter(function(o) { return typeof o === 'string' && o.trim(); })
                        : []
                });
            }
        });
    }

    if (parsed.validation && typeof parsed.validation === 'object' && parsed.validation.status) {
        result.validation = {
            status: parsed.validation.status,
            detail: typeof parsed.validation.detail === 'string' ? parsed.validation.detail : ''
        };
    }

    return result;
}

function agentNormalizeSnippet(s) {
    if (!s) return null;
    if (typeof s === 'string') {
        if (!s.trim()) return null;
        return { title: 'Example', description: '', yaml: s, relatedModule: '' };
    }
    if (typeof s !== 'object') return null;
    const yaml = s.yaml || s.config || s.code || '';
    if (typeof yaml !== 'string' || !yaml.trim()) return null;
    return {
        title: typeof s.title === 'string' && s.title ? s.title : 'Example',
        description: typeof s.description === 'string' ? s.description : '',
        yaml: yaml,
        relatedModule: typeof s.relatedModule === 'string' ? s.relatedModule.trim() : ''
    };
}

/**
 * Minimal markdown rendering for assistant message text: inline code, bold,
 * bullet/numbered lists and paragraph breaks. Input is HTML-escaped first,
 * so only markup produced here reaches the DOM.
 */
function agentRenderMarkdown(text) {
    if (!text) return '';
    const inline = function(s) {
        return s
            .replace(/`([^`]+)`/g, '<code>$1</code>')
            .replace(/\*\*([^*]+?)\*\*/g, '<strong>$1</strong>');
    };
    const lines = escapeHtml(text).split('\n');
    let html = '';
    let listTag = null;
    const closeList = function() {
        if (listTag) {
            html += '</' + listTag + '>';
            listTag = null;
        }
    };
    lines.forEach(function(line) {
        const ul = line.match(/^\s*[-*]\s+(.*)$/);
        const ol = ul ? null : line.match(/^\s*\d+[.)]\s+(.*)$/);
        if (ul || ol) {
            const tag = ul ? 'ul' : 'ol';
            if (listTag !== tag) {
                closeList();
                html += '<' + tag + ' class="agent-md-list">';
                listTag = tag;
            }
            html += '<li>' + inline((ul || ol)[1]) + '</li>';
        } else if (!line.trim()) {
            closeList();
            html += '<div class="agent-md-gap"></div>';
        } else {
            closeList();
            html += '<div>' + inline(line) + '</div>';
        }
    });
    closeList();
    return html;
}

/**
 * LCS-based line diff. Returns [{type: ' '|'-'|'+', line}].
 */
function agentDiffLines(oldText, newText) {
    const a = oldText ? oldText.replace(/\r\n/g, '\n').split('\n') : [];
    const b = newText ? newText.replace(/\r\n/g, '\n').split('\n') : [];
    const n = a.length, m = b.length;
    const dp = [];
    for (let i = 0; i <= n; i++) dp.push(new Int32Array(m + 1));
    for (let i = n - 1; i >= 0; i--) {
        for (let j = m - 1; j >= 0; j--) {
            dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
        }
    }
    const ops = [];
    let i = 0, j = 0;
    while (i < n && j < m) {
        if (a[i] === b[j]) {
            ops.push({ type: ' ', line: a[i] });
            i++; j++;
        } else if (dp[i + 1][j] >= dp[i][j + 1]) {
            ops.push({ type: '-', line: a[i] });
            i++;
        } else {
            ops.push({ type: '+', line: b[j] });
            j++;
        }
    }
    while (i < n) ops.push({ type: '-', line: a[i++] });
    while (j < m) ops.push({ type: '+', line: b[j++] });
    return ops;
}

/**
 * Render a line diff, collapsing long unchanged runs to 2 context lines.
 */
function agentCreateDiffEl(ops) {
    const pre = document.createElement('pre');
    pre.className = 'agent-diff';
    const CONTEXT = 2;
    let unchanged = [];
    const flushUnchanged = function(isLast) {
        const head = unchanged.slice(0, CONTEXT);
        const tail = isLast ? [] : unchanged.slice(-CONTEXT);
        const hidden = unchanged.length - head.length - tail.length;
        const emit = function(line) {
            const div = document.createElement('div');
            div.textContent = '  ' + line;
            pre.appendChild(div);
        };
        head.forEach(emit);
        if (hidden > 0) {
            const div = document.createElement('div');
            div.className = 'agent-diff-skip';
            div.textContent = '··· ' + hidden + ' unchanged line' + (hidden > 1 ? 's' : '') + ' ···';
            pre.appendChild(div);
        } else if (hidden < 0) {
            unchanged.slice(head.length).forEach(emit);
            unchanged = [];
            return;
        }
        tail.forEach(emit);
        unchanged = [];
    };
    ops.forEach(function(op) {
        if (op.type === ' ') {
            unchanged.push(op.line);
            return;
        }
        flushUnchanged(false);
        const div = document.createElement('div');
        div.className = op.type === '+' ? 'agent-diff-add' : 'agent-diff-del';
        div.textContent = op.type + ' ' + op.line;
        pre.appendChild(div);
    });
    flushUnchanged(true);
    return pre;
}

// =============================
// Message rendering
// =============================

function agentRenderMessages() {
    const container = $id('agent-chat-messages');
    container.innerHTML = '';

    if (agentChatHistory.length === 0) {
        container.innerHTML =
            '<div class="agent-welcome text-center text-muted py-5">' +
                '<i class="bi bi-robot" style="font-size: 3rem;"></i>' +
                '<p class="mt-3">How can I help you build your pipeline?</p>' +
                '<p class="small">Describe what data you want to process and I\'ll propose the configuration. ' +
                'Select a module on the canvas (or type <code>@module</code>) to talk about it.</p>' +
            '</div>';
        return;
    }

    // Group tool calls together
    let i = 0;
    while (i < agentChatHistory.length) {
        const msg = agentChatHistory[i];

        if (msg.role === 'user') {
            container.appendChild(agentCreateMessageEl('user', msg.content));
            i++;
        } else if (msg.role === 'assistant' && msg.toolCall) {
            // Collect consecutive tool call + tool result pairs
            const toolCalls = [];
            while (i < agentChatHistory.length) {
                const curr = agentChatHistory[i];
                if (curr.role === 'assistant' && curr.toolCall) {
                    const toolResult = (i + 1 < agentChatHistory.length && agentChatHistory[i + 1].role === 'tool')
                        ? agentChatHistory[i + 1] : null;
                    toolCalls.push({ call: curr, result: toolResult });
                    i += toolResult ? 2 : 1;
                } else {
                    break;
                }
            }
            container.appendChild(agentCreateToolGroupEl(toolCalls));
        } else if (msg.role === 'assistant') {
            container.appendChild(agentCreateAssistantMessageEl(agentParseAssistantContent(msg.content)));
            i++;
        } else {
            // Orphaned tool result (shouldn't happen normally), skip
            i++;
        }
    }

    // Scroll to bottom
    container.scrollTop = container.scrollHeight;
}

function agentCreateMessageEl(role, content) {
    const avatarIcon = role === 'user' ? 'bi-person' : 'bi-robot';
    const msg = document.createElement('div');
    msg.className = 'agent-message ' + role;
    msg.innerHTML =
        '<div class="agent-message-avatar"><i class="bi ' + avatarIcon + '"></i></div>' +
        '<div class="agent-message-bubble">' + escapeHtml(content) + '</div>';
    return msg;
}

function agentCreateAssistantMessageEl(parsed) {
    const msg = document.createElement('div');
    msg.className = 'agent-message assistant';
    msg.innerHTML =
        '<div class="agent-message-avatar"><i class="bi bi-robot"></i></div>' +
        '<div class="agent-message-bubble"></div>';
    const bubble = msg.querySelector('.agent-message-bubble');

    const text = document.createElement('div');
    text.className = 'agent-md';
    text.innerHTML = agentRenderMarkdown(parsed.message);
    bubble.appendChild(text);

    if (parsed.validation) {
        bubble.appendChild(agentCreateValidationEl(parsed.validation));
    }

    if (parsed.config) {
        const badge = document.createElement('div');
        badge.className = 'agent-config-badge';
        badge.innerHTML = '<i class="bi bi-file-diff me-1"></i>Propose this config';
        badge.title = 'Show this config as a pending proposal (Accept / Reject below)';
        badge.addEventListener('click', function() {
            agentPropose(parsed.config);
        });
        bubble.appendChild(badge);

        // Diff of this config against the current pipeline, computed on demand
        const diffBadge = document.createElement('div');
        diffBadge.className = 'agent-config-badge agent-diff-badge';
        diffBadge.innerHTML = '<i class="bi bi-file-diff me-1"></i>View diff';
        bubble.appendChild(diffBadge);

        let diffEl = null;
        diffBadge.addEventListener('click', function() {
            if (diffEl) {
                diffEl.remove();
                diffEl = null;
                diffBadge.innerHTML = '<i class="bi bi-file-diff me-1"></i>View diff';
                return;
            }
            const ops = agentDiffLines(agentGetCanvasConfigYaml(), parsed.config);
            const changed = ops.some(function(op) { return op.type !== ' '; });
            diffEl = changed
                ? agentCreateDiffEl(ops)
                : (function() {
                    const el = document.createElement('div');
                    el.className = 'agent-diff-empty';
                    el.textContent = 'No differences from the current pipeline.';
                    return el;
                })();
            diffBadge.insertAdjacentElement('afterend', diffEl);
            diffBadge.innerHTML = '<i class="bi bi-file-diff me-1"></i>Hide diff';
        });

        const pre = document.createElement('pre');
        pre.textContent = parsed.config;
        bubble.appendChild(pre);
    }

    parsed.snippets.forEach(function(snippet) {
        bubble.appendChild(agentCreateSnippetEl(snippet));
    });

    if (parsed.questions.length > 0) {
        const questionsEl = agentCreateQuestionsEl(parsed.questions);
        if (questionsEl) bubble.appendChild(questionsEl);
    }

    return msg;
}

function agentCreateValidationEl(validation) {
    const ok = validation.status === 'success';
    const el = document.createElement('div');
    el.className = 'agent-validation ' + (ok ? 'success' : 'error');
    el.innerHTML =
        '<i class="bi ' + (ok ? 'bi-check-circle-fill' : 'bi-x-circle-fill') + ' me-1"></i>' +
        (ok ? 'Validation passed' : 'Validation failed') +
        (validation.detail ? '<span class="agent-validation-detail">' + escapeHtml(validation.detail) + '</span>' : '');
    return el;
}

/** Bring the canvas forward and flash the named node. */
function agentShowModule(name) {
    showView('canvas');
    if (!highlightNodeByName(name)) {
        setStatus('Module "' + name + '" is not on the canvas', 'warning');
    }
}

function agentCreateSnippetEl(snippet) {
    const card = document.createElement('div');
    card.className = 'agent-snippet-card';

    const header = document.createElement('div');
    header.className = 'agent-snippet-header';

    const title = document.createElement('span');
    title.className = 'agent-snippet-title';
    title.innerHTML = '<i class="bi bi-lightbulb me-1"></i>' + escapeHtml(snippet.title);
    header.appendChild(title);

    if (snippet.relatedModule) {
        const moduleChip = document.createElement('button');
        moduleChip.type = 'button';
        moduleChip.className = 'agent-snippet-module';
        moduleChip.title = 'Show this module on the canvas';
        moduleChip.innerHTML = '<i class="bi bi-diagram-3 me-1"></i>' + escapeHtml(snippet.relatedModule);
        moduleChip.addEventListener('click', function() {
            agentShowModule(snippet.relatedModule);
        });
        header.appendChild(moduleChip);
    }

    const copyBtn = document.createElement('button');
    copyBtn.type = 'button';
    copyBtn.className = 'agent-snippet-copy';
    copyBtn.innerHTML = '<i class="bi bi-clipboard me-1"></i>Copy';
    copyBtn.addEventListener('click', function() {
        navigator.clipboard.writeText(snippet.yaml).then(function() {
            copyBtn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Copied';
            setTimeout(function() {
                copyBtn.innerHTML = '<i class="bi bi-clipboard me-1"></i>Copy';
            }, 1500);
        }).catch(function() {
            setStatus('Failed to copy snippet', 'error');
        });
    });
    header.appendChild(copyBtn);
    card.appendChild(header);

    if (snippet.description) {
        const desc = document.createElement('div');
        desc.className = 'agent-snippet-description';
        desc.textContent = snippet.description;
        card.appendChild(desc);
    }

    const pre = document.createElement('pre');
    pre.textContent = snippet.yaml;
    card.appendChild(pre);

    return card;
}

function agentCreateQuestionsEl(questions) {
    const wrap = document.createElement('div');
    wrap.className = 'agent-questions';
    let hasContent = false;

    questions.forEach(function(q) {
        if (q.options.length === 0) return;
        hasContent = true;

        const block = document.createElement('div');
        block.className = 'agent-question';

        if (q.text) {
            const label = document.createElement('div');
            label.className = 'agent-question-text';
            label.textContent = q.text;
            block.appendChild(label);
        }

        const chips = document.createElement('div');
        chips.className = 'agent-question-options';
        q.options.forEach(function(option) {
            const chip = document.createElement('button');
            chip.type = 'button';
            chip.className = 'agent-question-chip';
            chip.textContent = option;
            chip.addEventListener('click', function() {
                agentSend(option);
            });
            chips.appendChild(chip);
        });
        block.appendChild(chips);
        wrap.appendChild(block);
    });

    return hasContent ? wrap : null;
}

/**
 * Human-readable label and result status for a tool call.
 */
function agentToolInfo(toolCall, result) {
    let args = {};
    try {
        args = JSON.parse(toolCall.arguments || '{}');
    } catch (e) {
        // keep empty args
    }

    let label;
    switch (toolCall.name) {
        case 'run':
        case 'execute':
            label = args.dryRun === false ? 'Running pipeline' : 'Validating config (dry run)';
            break;
        case 'listModules':
            label = 'Listing available modules' + (args.type ? ': ' + args.type : '');
            break;
        case 'getModule':
            label = 'Reading module docs' + (args.type && args.name ? ': ' + args.type + '/' + args.name : '');
            break;
        default:
            label = 'Tool: ' + toolCall.name;
    }

    let status = null;
    const content = result && typeof result.content === 'string' ? result.content : '';
    if (content.startsWith('ERROR') || content.startsWith('Error:')) {
        status = 'error';
    } else if (content.startsWith('SUCCESS')) {
        status = 'success';
    }

    return { label: label, status: status };
}

function agentCreateToolGroupEl(toolCalls) {
    const group = document.createElement('div');
    group.className = 'agent-tool-group';

    const toggle = document.createElement('div');
    toggle.className = 'agent-tool-toggle';
    let summaryHtml = '<i class="bi bi-chevron-right"></i><i class="bi bi-gear"></i>';
    toolCalls.forEach(function(tc) {
        const info = agentToolInfo(tc.call.toolCall, tc.result);
        let statusIcon = '';
        if (info.status === 'success') {
            statusIcon = '<i class="bi bi-check-circle-fill agent-tool-status success"></i>';
        } else if (info.status === 'error') {
            statusIcon = '<i class="bi bi-x-circle-fill agent-tool-status error"></i>';
        }
        summaryHtml += '<span class="agent-tool-label">' + escapeHtml(info.label) + statusIcon + '</span>';
    });
    toggle.innerHTML = summaryHtml;

    const detail = document.createElement('div');
    detail.className = 'agent-tool-detail';

    toolCalls.forEach(function(tc) {
        const info = agentToolInfo(tc.call.toolCall, tc.result);
        let detailText = info.label + '\n';
        detailText += 'Call: ' + tc.call.toolCall.name + '(' + (tc.call.toolCall.arguments || '') + ')\n';
        if (tc.result) {
            const resultContent = tc.result.content || '';
            detailText += 'Result: ' + (resultContent.length > 500 ? resultContent.substring(0, 500) + '...' : resultContent) + '\n';
        }
        const line = document.createElement('div');
        line.textContent = detailText;
        detail.appendChild(line);
    });

    toggle.addEventListener('click', function() {
        toggle.classList.toggle('expanded');
        detail.classList.toggle('show');
    });

    group.appendChild(toggle);
    group.appendChild(detail);
    return group;
}

// =============================
// Pipeline context
// =============================

/**
 * Export the current pipeline config as YAML so the agent can see the user's
 * latest (possibly hand-edited) pipeline. Returns '' when the pipeline is empty.
 */
function agentGetCanvasConfigYaml() {
    let config;
    try {
        config = workspace.getConfig();
    } catch (e) {
        return '';
    }
    if (!config) return '';
    const cleaned = {};
    Object.keys(config).forEach(function(key) {
        const value = config[key];
        if (Array.isArray(value) ? value.length > 0 : (value && Object.keys(value).length > 0)) {
            cleaned[key] = value;
        }
    });
    if (!cleaned.sources && !cleaned.transforms && !cleaned.sinks) return '';
    return dumpYaml(cleaned);
}

// =============================
// Sending
// =============================

function agentSendFromInput() {
    const inputEl = $id('agent-chat-input');
    const input = inputEl.value.trim();
    if (!input) return;
    if (agentIsSending) return;
    inputEl.value = '';
    hideMention();
    agentSend(input);
}

function agentSend(input) {
    if (agentIsSending) return;

    // Add user message to history
    agentChatHistory.push({ role: 'user', content: input });
    agentRenderMessages();

    // Show loading
    agentIsSending = true;
    $id('btn-agent-send').disabled = true;
    const container = $id('agent-chat-messages');
    container.insertAdjacentHTML('beforeend',
        '<div class="agent-loading" id="agent-loading">' +
            '<div class="agent-message-avatar"><i class="bi bi-robot"></i></div>' +
            '<div class="agent-loading-dots"><span></span><span></span><span></span></div>' +
        '</div>');
    container.scrollTop = container.scrollHeight;

    // Send to server together with the current pipeline config and selection
    if (!agentConversationId) {
        agentConversationId = (window.crypto && crypto.randomUUID)
            ? crypto.randomUUID()
            : Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
    }
    persistChat();
    const body = { history: agentChatHistory, conversationId: agentConversationId };
    const canvasYaml = agentGetCanvasConfigYaml();
    if (canvasYaml) {
        body.config = canvasYaml;
    }
    const selection = workspace.getSelection();
    if (selection) {
        body.selection = selection;
    }
    postJson('/api/agent', body, 120000).then(function(newMessages) {
        // Append new messages to history
        if (Array.isArray(newMessages)) {
            newMessages.forEach(function(msg) {
                agentChatHistory.push(msg);
            });
        }
        agentRenderMessages();

        // A config in the last assistant message becomes the pending proposal
        let lastAssistant = null;
        for (let j = agentChatHistory.length - 1; j >= 0; j--) {
            if (agentChatHistory[j].role === 'assistant' && !agentChatHistory[j].toolCall) {
                lastAssistant = agentChatHistory[j];
                break;
            }
        }
        if (lastAssistant) {
            const parsed = agentParseAssistantContent(lastAssistant.content);
            if (parsed.config) {
                agentPropose(parsed.config);
            }
        }
    }).catch(function(err) {
        agentChatHistory.push({
            role: 'assistant',
            content: JSON.stringify({ message: 'Sorry, an error occurred: ' + err.message })
        });
        agentRenderMessages();
    }).finally(function() {
        agentIsSending = false;
        $id('btn-agent-send').disabled = false;
        const loading = $id('agent-loading');
        if (loading) loading.remove();
        persistChat();
    });
}

function agentClearHistory() {
    agentChatHistory = [];
    agentConversationId = null;
    agentRenderMessages();
    workspace.rejectPending(SOURCE);
    persistChat();
}

// =============================
// Proposals: pending -> Accept / Reject
// =============================

function agentPropose(configText) {
    let config;
    try {
        config = jsyaml.load(configText);
    } catch (e) {
        try {
            config = JSON.parse(configText);
        } catch (e2) {
            setStatus('Failed to parse agent config', 'error');
            return;
        }
    }

    if (!config || typeof config !== 'object') {
        setStatus('Invalid config from agent', 'error');
        return;
    }

    workspace.setPending(config, SOURCE);
    setStatus('Agent proposed a config — review the diff, then Accept or Reject');
}

function agentAccept() {
    if (!workspace.getPending()) return;
    agentUndoSnapshot = JSON.parse(JSON.stringify(workspace.getConfig()));
    workspace.acceptPending(SOURCE);
    $id('btn-agent-undo').classList.remove('d-none');
    setStatus('Proposal accepted', 'success');
}

function agentReject() {
    workspace.rejectPending(SOURCE);
    setStatus('Proposal rejected');
}

function agentUndoAccept() {
    if (agentUndoSnapshot === null) return;
    workspace.setConfig(agentUndoSnapshot, SOURCE);
    agentUndoSnapshot = null;
    $id('btn-agent-undo').classList.add('d-none');
    setStatus('Pipeline restored to the state before the last accept', 'success');
}

function renderPendingBar() {
    const bar = $id('agent-pending');
    const diff = workspace.getPendingDiff();
    if (!diff) {
        bar.classList.add('d-none');
        return;
    }
    const parts = [];
    if (diff.added.length) parts.push('+' + diff.added.length + ' added');
    if (diff.modified.length) parts.push('~' + diff.modified.length + ' modified');
    if (diff.removed.length) parts.push('−' + diff.removed.length + ' removed');
    if (diff.settingsChanged) parts.push('system/options');
    $id('agent-pending-summary').textContent = 'Proposed change: ' + (parts.length ? parts.join(' · ') : 'no differences');

    const chips = $id('agent-pending-modules');
    chips.innerHTML = '';
    const addChip = function(name, kind) {
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = 'agent-pending-chip ' + kind;
        chip.textContent = name;
        chip.title = kind === 'added' ? 'New module (not on the canvas until accepted)' : 'Show on the canvas';
        chip.addEventListener('click', function() {
            if (kind !== 'added') agentShowModule(name);
        });
        chips.appendChild(chip);
    };
    diff.added.forEach(function(n) { addChip(n, 'added'); });
    diff.modified.forEach(function(n) { addChip(n, 'modified'); });
    diff.removed.forEach(function(n) { addChip(n, 'removed'); });
    if (diff.settingsChanged) addChip('system / options', 'settings');
    bar.classList.remove('d-none');
}

// =============================
// Selection context + @mentions
// =============================

function renderContextChip() {
    const selection = workspace.getSelection();
    $id('agent-context').classList.toggle('d-none', !selection);
    $id('agent-context-name').textContent = selection ? 'Context: ' + selection : '';
}

let mentionItems = [];
let mentionIndex = 0;
let mentionRange = null; // { start, end } of the "@word" being completed

function hideMention() {
    $id('agent-mention').classList.add('d-none');
    mentionItems = [];
    mentionRange = null;
}

function updateMention() {
    const input = $id('agent-chat-input');
    const caret = input.selectionStart;
    const before = input.value.slice(0, caret);
    const m = before.match(/(^|\s)@([A-Za-z0-9_]*)$/);
    if (!m) {
        hideMention();
        return;
    }
    const prefix = m[2].toLowerCase();
    mentionItems = workspace.getModuleNames().filter(function(name) {
        return name.toLowerCase().indexOf(prefix) === 0;
    });
    if (mentionItems.length === 0) {
        hideMention();
        return;
    }
    mentionRange = { start: caret - m[2].length - 1, end: caret };
    mentionIndex = Math.min(mentionIndex, mentionItems.length - 1);
    const list = $id('agent-mention');
    list.innerHTML = '';
    mentionItems.forEach(function(name, index) {
        const item = document.createElement('div');
        item.className = 'agent-mention-item' + (index === mentionIndex ? ' active' : '');
        item.textContent = '@' + name;
        item.addEventListener('mousedown', function(e) {
            e.preventDefault(); // keep the textarea focused
            pickMention(index);
        });
        list.appendChild(item);
    });
    list.classList.remove('d-none');
}

function pickMention(index) {
    if (!mentionRange || !mentionItems[index]) return;
    const input = $id('agent-chat-input');
    const name = mentionItems[index];
    input.value = input.value.slice(0, mentionRange.start) + '@' + name + ' ' + input.value.slice(mentionRange.end);
    const pos = mentionRange.start + name.length + 2;
    input.setSelectionRange(pos, pos);
    hideMention();
    // Mentioning a module also makes it the selection context
    workspace.setSelection(name, SOURCE);
}

function onInputKeydown(e) {
    if (mentionItems.length > 0) {
        if (e.key === 'ArrowDown') {
            e.preventDefault();
            mentionIndex = (mentionIndex + 1) % mentionItems.length;
            updateMention();
            return;
        }
        if (e.key === 'ArrowUp') {
            e.preventDefault();
            mentionIndex = (mentionIndex - 1 + mentionItems.length) % mentionItems.length;
            updateMention();
            return;
        }
        if (e.key === 'Enter' || e.key === 'Tab') {
            e.preventDefault();
            pickMention(mentionIndex);
            return;
        }
        if (e.key === 'Escape') {
            hideMention();
            return;
        }
    }
    if (e.key === 'Enter' && !e.shiftKey && !agentIsComposing) {
        e.preventDefault();
        agentSendFromInput();
    }
}

// =============================
// Init
// =============================

function onWorkspaceChange(event) {
    if (event.type === 'pending') {
        renderPendingBar();
    } else if (event.type === 'selection') {
        renderContextChip();
    } else if (event.type === 'agent' && event.source === 'restore') {
        const state = workspace.getAgentState() || {};
        agentChatHistory = Array.isArray(state.history) ? state.history : [];
        agentConversationId = state.conversationId || null;
        agentRenderMessages();
    }
}

export function initAgent() {
    on('btn-agent-send', 'click', agentSendFromInput);
    on('btn-agent-clear', 'click', agentClearHistory);
    on('btn-agent-undo', 'click', agentUndoAccept);
    on('btn-agent-accept', 'click', agentAccept);
    on('btn-agent-reject', 'click', agentReject);
    on('btn-agent-context-clear', 'click', function() { workspace.setSelection(null, SOURCE); });

    // IME composition handling for agent input
    on('agent-chat-input', 'compositionstart', function() {
        agentIsComposing = true;
    });
    on('agent-chat-input', 'compositionend', function() {
        agentIsComposing = false;
    });
    on('agent-chat-input', 'keydown', onInputKeydown);
    on('agent-chat-input', 'input', function() { mentionIndex = 0; updateMention(); });
    on('agent-chat-input', 'blur', function() { setTimeout(hideMention, 150); });

    workspace.subscribe(onWorkspaceChange);
    agentRenderMessages();
    renderPendingBar();
    renderContextChip();
}

/** Open the pane (used by callers that want to hand the user to the agent). */
export function openAgent() {
    setAgentPaneOpen(true);
}

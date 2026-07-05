/**
 * agent.js - AI agent chat modal.
 */
'use strict';

import { $id, on, showModal, escapeHtml, setStatus, postJson } from './util.js';
import { importConfigToCanvas } from './canvas.js';

let agentChatHistory = [];
let agentIsComposing = false;
let agentIsSending = false;

function openAgentModal() {
    showModal('agentModal');
}

function agentRenderMessages() {
    const container = $id('agent-chat-messages');
    container.innerHTML = '';

    if (agentChatHistory.length === 0) {
        container.innerHTML =
            '<div class="agent-welcome text-center text-muted py-5">' +
                '<i class="bi bi-robot" style="font-size: 3rem;"></i>' +
                '<p class="mt-3">How can I help you build your pipeline?</p>' +
                '<p class="small">Describe what data you want to process and I\'ll generate the configuration.</p>' +
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
            // Parse content for config
            let displayText = msg.content;
            let configYaml = null;
            try {
                const parsed = JSON.parse(msg.content);
                if (parsed.message) displayText = parsed.message;
                if (parsed.config) configYaml = parsed.config;
            } catch (e) {
                // Not JSON, use as-is
            }
            container.appendChild(agentCreateAssistantMessageEl(displayText, configYaml));
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

function agentCreateAssistantMessageEl(message, configYaml) {
    const msg = document.createElement('div');
    msg.className = 'agent-message assistant';
    msg.innerHTML =
        '<div class="agent-message-avatar"><i class="bi bi-robot"></i></div>' +
        '<div class="agent-message-bubble"></div>';
    const bubble = msg.querySelector('.agent-message-bubble');

    const text = document.createElement('div');
    text.textContent = message;
    bubble.appendChild(text);

    if (configYaml) {
        const badge = document.createElement('div');
        badge.className = 'agent-config-badge';
        badge.innerHTML = '<i class="bi bi-check-lg me-1"></i>Apply config to canvas';
        badge.addEventListener('click', function() {
            agentApplyConfig(configYaml);
        });
        bubble.appendChild(badge);

        const pre = document.createElement('pre');
        pre.textContent = configYaml;
        bubble.appendChild(pre);
    }

    return msg;
}

function agentCreateToolGroupEl(toolCalls) {
    const group = document.createElement('div');
    group.className = 'agent-tool-group';

    const toolNames = toolCalls.map(function(tc) { return tc.call.toolCall.name; }).join(', ');
    const toggle = document.createElement('div');
    toggle.className = 'agent-tool-toggle';
    toggle.innerHTML =
        '<i class="bi bi-chevron-right"></i>' +
        '<i class="bi bi-gear"></i>' +
        '<span>Tool used: ' + escapeHtml(toolNames) + '</span>';

    const detail = document.createElement('div');
    detail.className = 'agent-tool-detail';

    toolCalls.forEach(function(tc) {
        let detailText = 'Call: ' + tc.call.toolCall.name + '(' + (tc.call.toolCall.arguments || '') + ')\n';
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

function agentSendMessage() {
    if (agentIsSending) return;

    const inputEl = $id('agent-chat-input');
    const input = inputEl.value.trim();
    if (!input) return;

    // Add user message to history
    agentChatHistory.push({ role: 'user', content: input });
    inputEl.value = '';
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

    // Send to server
    postJson('/api/agent', { history: agentChatHistory }, 120000).then(function(newMessages) {
        // Append new messages to history
        if (Array.isArray(newMessages)) {
            newMessages.forEach(function(msg) {
                agentChatHistory.push(msg);
            });
        }
        agentRenderMessages();

        // Auto-apply config if the last assistant message has one
        let lastAssistant = null;
        for (let j = agentChatHistory.length - 1; j >= 0; j--) {
            if (agentChatHistory[j].role === 'assistant' && !agentChatHistory[j].toolCall) {
                lastAssistant = agentChatHistory[j];
                break;
            }
        }
        if (lastAssistant) {
            try {
                const parsed = JSON.parse(lastAssistant.content);
                if (parsed.config) {
                    agentApplyConfig(parsed.config);
                }
            } catch (e) {
                // not JSON
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
    });
}

function agentClearHistory() {
    agentChatHistory = [];
    agentRenderMessages();
}

function agentApplyConfig(configText) {
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

    importConfigToCanvas(config);
    setStatus('Agent config applied to canvas', 'success');
}

export function initAgent() {
    on('btn-agent', 'click', openAgentModal);
    on('btn-agent-send', 'click', agentSendMessage);
    on('btn-agent-clear', 'click', agentClearHistory);

    // IME composition handling for agent input
    on('agent-chat-input', 'compositionstart', function() {
        agentIsComposing = true;
    });
    on('agent-chat-input', 'compositionend', function() {
        agentIsComposing = false;
    });
    on('agent-chat-input', 'keydown', function(e) {
        if (e.key === 'Enter' && !e.shiftKey && !agentIsComposing) {
            e.preventDefault();
            agentSendMessage();
        }
    });
}

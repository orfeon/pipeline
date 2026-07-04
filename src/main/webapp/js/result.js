/**
 * result.js - Result modal rendering and pipeline execution (dryrun / run / launch).
 */
'use strict';

import { $id, on, show, hide, showModal, escapeHtml, setStatus, postJson,
         renderSchemaFields, renderRecordsTable } from './util.js';
import { generateConfig, getValidationErrors,
         updateNodeSchemaIndicator, updateNodeOutputIndicator } from './canvas.js';

// =============================
// Result modal
// =============================

/** Hide the "Output schemas for each module:" label until the modal closes. */
function hideResultDescription() {
    const description = document.querySelector('#result-success-content p.mb-2');
    hide(description);
    $id('resultModal').addEventListener('hidden.bs.modal', function() {
        show(description);
    }, { once: true });
}

export function showResult(title, content, type) {
    const header = $id('result-modal-header');
    const genericContent = $id('result-content');

    // Reset all content areas
    hide($id('result-success-content'));
    hide($id('result-error-content'));
    header.classList.remove('success', 'error');

    // Set title
    $id('result-title').textContent = title;

    // Set icon and header color based on type
    if (type === 'success') {
        header.classList.add('success');
        $id('result-icon').className = 'bi bi-check-circle-fill me-2 text-success';
        genericContent.style.color = '#28a745';
    } else if (type === 'error') {
        header.classList.add('error');
        $id('result-icon').className = 'bi bi-x-circle-fill me-2 text-danger';
        genericContent.style.color = '#dc3545';
    } else {
        $id('result-icon').className = 'bi bi-info-circle me-2';
        genericContent.style.color = '#d4d4d4';
    }

    // Show generic content
    genericContent.textContent = content;
    show(genericContent);

    showModal('resultModal');
}

export function showPipelineResult(type, result) {
    const header = $id('result-modal-header');
    const successContent = $id('result-success-content');
    const errorContent = $id('result-error-content');
    const genericContent = $id('result-content');

    // Reset visibility
    hide(successContent);
    hide(errorContent);
    hide(genericContent);
    header.classList.remove('success', 'error');

    const title = type.charAt(0).toUpperCase() + type.slice(1) + ' Result';
    $id('result-title').textContent = title;

    const modules = result.spec && result.spec.modules ? result.spec.modules : [];
    const isSuccess = result.status === 'ok' || (modules.length > 0 && !result.error);
    const isError = result.status === 'error' || result.error;

    if (isSuccess) {
        header.classList.add('success');
        $id('result-icon').className = 'bi bi-check-circle-fill me-2 text-success';
        $id('result-millis').textContent = 'Completed in ' + result.millis + 'ms';

        const accordion = $id('schemaAccordion');
        accordion.innerHTML = '';

        // Cache schemas from spec.modules (returned by both DryRun and Run)
        modules.forEach(function(module) {
            updateNodeSchemaIndicator(module.name, module.schema);
        });

        const outputs = result.outputs || [];
        if (type === 'run' && outputs.length > 0) {
            // Cache outputs for later access
            outputs.forEach(function(output) {
                updateNodeOutputIndicator(output.name, output);
            });

            hideResultDescription();

            outputs.forEach(function(output, index) {
                const accordionId = 'output-' + index;
                const records = output.records || [];
                const schema = output.schema || {};
                const recordsHtml = renderRecordsTable(records, schema);

                accordion.insertAdjacentHTML('beforeend',
                    '<div class="accordion-item">' +
                        '<h2 class="accordion-header">' +
                            '<button class="accordion-button ' + (index > 0 ? 'collapsed' : '') + '" type="button" ' +
                                'data-bs-toggle="collapse" data-bs-target="#' + accordionId + '">' +
                                '<i class="bi bi-table me-2"></i>' +
                                '<strong>' + escapeHtml(output.name) + '</strong>' +
                                '<span class="badge bg-info ms-2">' + records.length + ' records</span>' +
                            '</button>' +
                        '</h2>' +
                        '<div id="' + accordionId + '" class="accordion-collapse collapse ' + (index === 0 ? 'show' : '') + '" ' +
                             'data-bs-parent="#schemaAccordion">' +
                            '<div class="accordion-body p-0">' +
                                recordsHtml +
                            '</div>' +
                        '</div>' +
                    '</div>');
            });
        } else if (modules.length > 0) {
            modules.forEach(function(module, index) {
                const accordionId = 'schema-' + index;
                const schemaHtml = renderSchemaFields(module.schema.fields);

                accordion.insertAdjacentHTML('beforeend',
                    '<div class="accordion-item">' +
                        '<h2 class="accordion-header">' +
                            '<button class="accordion-button ' + (index > 0 ? 'collapsed' : '') + '" type="button" ' +
                                'data-bs-toggle="collapse" data-bs-target="#' + accordionId + '">' +
                                '<i class="bi bi-box me-2"></i>' +
                                '<strong>' + escapeHtml(module.name) + '</strong>' +
                                '<span class="badge bg-secondary ms-2">' + (module.schema.fields ? module.schema.fields.length : 0) + ' fields</span>' +
                            '</button>' +
                        '</h2>' +
                        '<div id="' + accordionId + '" class="accordion-collapse collapse ' + (index === 0 ? 'show' : '') + '" ' +
                             'data-bs-parent="#schemaAccordion">' +
                            '<div class="accordion-body">' +
                                schemaHtml +
                            '</div>' +
                        '</div>' +
                    '</div>');
            });
        } else {
            accordion.innerHTML = '<p class="text-muted">No output schemas available.</p>';
        }

        show(successContent);
    } else if (isError) {
        header.classList.add('error');
        $id('result-icon').className = 'bi bi-x-circle-fill me-2 text-danger';

        if (result.error) {
            $id('error-module-name').textContent = result.error.name || 'Unknown';

            const moduleType = result.error.module || '';
            const badge = $id('error-module-type');
            badge.textContent = moduleType;
            badge.classList.remove('bg-success', 'bg-primary', 'bg-warning');
            if (moduleType === 'source') {
                badge.classList.add('bg-success');
            } else if (moduleType === 'transform') {
                badge.classList.add('bg-primary');
            } else if (moduleType === 'sink') {
                badge.classList.add('bg-warning');
            }

            const messages = $id('error-messages');
            messages.innerHTML = '';
            const messageList = (result.error.messages && Array.isArray(result.error.messages))
                ? result.error.messages
                : (result.error.message ? [result.error.message] : []);
            messageList.forEach(function(msg) {
                const li = document.createElement('li');
                li.textContent = msg;
                messages.appendChild(li);
            });
        }

        $id('error-millis').textContent = 'Failed after ' + (result.millis || 0) + 'ms';
        show(errorContent);
    } else {
        genericContent.textContent = JSON.stringify(result, null, 2);
        show(genericContent);
    }

    showModal('resultModal');
}

export function showModuleSchema(moduleName, schema) {
    showModuleDetail('Schema: ' + moduleName, 'bi bi-file-earmark-text me-2 text-primary',
        renderSchemaFields(schema.fields), '');
}

export function showModuleRecords(moduleName, output) {
    const records = output.records || [];
    showModuleDetail('Records: ' + moduleName, 'bi bi-table me-2 text-info',
        renderRecordsTable(records, output.schema || {}), records.length + ' records');
}

/** Show a single module's schema or records in the result modal. */
function showModuleDetail(title, iconClass, bodyHtml, millisText) {
    const header = $id('result-modal-header');
    header.classList.remove('success', 'error');
    header.classList.add('success');
    $id('result-icon').className = iconClass;
    $id('result-title').textContent = title;

    hide($id('result-success-content'));
    hide($id('result-error-content'));
    hide($id('result-content'));

    $id('schemaAccordion').innerHTML = bodyHtml;
    $id('result-millis').textContent = millisText;
    hideResultDescription();
    show($id('result-success-content'));

    showModal('resultModal');
}

// =============================
// Pipeline execution
// =============================

export function runPipeline(type) {
    const config = generateConfig();

    const errors = getValidationErrors(config);
    if (errors.length > 0) {
        showResult('Validation Errors', 'Please fix the following issues before running:\n\n' + errors.join('\n'), 'error');
        setStatus('Validation failed', 'error');
        return;
    }

    const button = $id(type === 'dryrun' ? 'btn-dryrun' : 'btn-run');
    const originalHtml = button.innerHTML;

    setRunningState(true, button);
    setStatus('Running ' + type + '...', 'warning');

    postJson('/api/pipeline', {
        config: jsyaml.dump(config),
        type: type
    }, 300000).then(function(result) {
        showPipelineResult(type, result);
        setStatus(type + ' completed', result.status === 'ok' ? 'success' : 'error');
    }).catch(function(err) {
        showResult('Error', 'Request failed: ' + err.message, 'error');
        setStatus(type + ' failed', 'error');
    }).finally(function() {
        setRunningState(false, button, originalHtml);
    });
}

export function runPipelineWithLaunch(launchConfig) {
    const config = generateConfig();
    const button = $id('btn-launch');
    const originalHtml = button.innerHTML;

    setRunningState(true, button);
    setStatus('Launching pipeline...', 'warning');

    postJson('/api/launch', {
        config: jsyaml.dump(config),
        type: 'launch',
        launch: launchConfig
    }, 300000).then(function(result) {
        showPipelineResult('launch', result);
        setStatus('Launch completed', result.status === 'ok' ? 'success' : 'error');
    }).catch(function(err) {
        showResult('Error', 'Request failed: ' + err.message, 'error');
        setStatus('Launch failed', 'error');
    }).finally(function() {
        setRunningState(false, button, originalHtml);
    });
}

function setRunningState(isRunning, button, originalHtml) {
    const allButtons = [$id('btn-dryrun'), $id('btn-run'), $id('btn-launch')];

    if (isRunning) {
        allButtons.forEach(function(b) { b.disabled = true; });
        button.innerHTML = '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span> Running...';
    } else {
        allButtons.forEach(function(b) { b.disabled = false; });
        if (originalHtml) {
            button.innerHTML = originalHtml;
        }
    }
}

export function initRunButtons() {
    on('btn-dryrun', 'click', function() { runPipeline('dryrun'); });
    on('btn-run', 'click', function() { runPipeline('run'); });
}

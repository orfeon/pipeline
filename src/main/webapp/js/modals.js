/**
 * modals.js - Module config, System, Options, Config editor and Launch modals.
 */
'use strict';

import { $id, on, show, hide, showModal, hideModal, getJson, setStatus, dumpYaml } from './util.js';
import { loadMonaco, setEditorValue, getEditorValue, ensureSchema, getCachedSchema,
         applyYamlSchemas, buildSchemaHelpTooltip } from './monaco.js';
import { getNodeData, updateNodeData, isNodeNameTaken, removeNode,
         generateConfig, getValidationErrors, importConfigToCanvas,
         getSystemConfig, setSystemConfig, getOptionsConfig, setOptionsConfig } from './canvas.js';
import { showResult, runPipelineWithLaunch } from './result.js';

let currentEditingNodeId = null;

// Values handed from modal openers to their shown.bs.modal handlers
const pending = {
    moduleYaml: '',
    moduleType: '',
    moduleName: '',
    systemYaml: '',
    optionsYaml: ''
};

// =============================
// Module Config Modal
// =============================

export function openModuleConfig(nodeId) {
    currentEditingNodeId = nodeId;
    const data = getNodeData(nodeId);

    // Set modal title
    const typeBadge = $id('modal-module-type');
    typeBadge.textContent = data.moduleType;
    typeBadge.classList.remove('source', 'transform', 'sink');
    typeBadge.classList.add(data.moduleType);
    $id('modal-module-name').textContent = data.moduleName;

    // Set name input
    $id('module-name-input').value = data.name;

    // Build config object excluding internal properties
    const configObj = {};
    const internalProps = ['moduleName', 'moduleType', 'name', 'outputSchema', 'output', 'waits', 'sideInputs'];
    for (const key in data) {
        if (Object.prototype.hasOwnProperty.call(data, key) && internalProps.indexOf(key) === -1) {
            configObj[key] = data[key];
        }
    }

    // Set YAML editor content (applied in the shown.bs.modal handler)
    pending.moduleYaml = dumpYaml(configObj);
    pending.moduleType = data.moduleType;
    pending.moduleName = data.moduleName;

    showModal('moduleConfigModal');
}

function saveModuleConfig() {
    if (currentEditingNodeId === null) return;

    // Parse YAML
    const yamlContent = getEditorValue('module-yaml-editor').trim();
    let parsed = {};
    if (yamlContent) {
        try {
            parsed = jsyaml.load(yamlContent) || {};
        } catch (e) {
            alert('Invalid YAML: ' + e.message);
            return;
        }
    }

    // Read name from input (takes priority)
    const nameInput = $id('module-name-input');
    const name = nameInput.value.trim();
    if (!name) {
        alert('Name is required');
        nameInput.classList.add('is-invalid');
        return;
    }
    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) {
        alert('Name must start with a letter and contain only letters, numbers, and underscores');
        nameInput.classList.add('is-invalid');
        return;
    }
    if (isNodeNameTaken(name, currentEditingNodeId)) {
        alert('Name "' + name + '" is already used by another module');
        nameInput.classList.add('is-invalid');
        return;
    }
    nameInput.classList.remove('is-invalid');

    // Update node data
    const data = getNodeData(currentEditingNodeId);
    data.name = name;
    data.parameters = parsed.parameters || data.parameters || {};

    // Update additional properties from parsed YAML (waits/sideInputs are managed via canvas connections)
    const configProps = ['schema', 'strategy', 'tags', 'logs', 'timestampAttribute', 'failFast', 'ignore'];
    configProps.forEach(function(prop) {
        if (parsed[prop] !== undefined) {
            data[prop] = parsed[prop];
        } else {
            delete data[prop];
        }
    });

    updateNodeData(currentEditingNodeId, data);

    hideModal('moduleConfigModal');
    setStatus('Module "' + name + '" updated');
}

function deleteModule() {
    if (currentEditingNodeId === null) return;
    if (confirm('Delete this module?')) {
        removeNode(currentEditingNodeId);
        hideModal('moduleConfigModal');
        setStatus('Module deleted');
        currentEditingNodeId = null;
    }
}

// =============================
// System & Options Modals
// =============================

function openSystemModal() {
    pending.systemYaml = dumpYaml(getSystemConfig());
    showModal('systemModal');
}

function applySystemConfig() {
    const yamlContent = getEditorValue('system-yaml-editor').trim();
    if (!yamlContent) {
        setSystemConfig({});
    } else {
        try {
            setSystemConfig(jsyaml.load(yamlContent) || {});
        } catch (e) {
            alert('Invalid YAML: ' + e.message);
            return;
        }
    }
    hideModal('systemModal');
    setStatus('System settings applied');
}

function openOptionsModal() {
    pending.optionsYaml = dumpYaml(getOptionsConfig());
    showModal('optionsModal');
}

function applyOptionsConfig() {
    const yamlContent = getEditorValue('options-yaml-editor').trim();
    if (!yamlContent) {
        setOptionsConfig({});
    } else {
        try {
            setOptionsConfig(jsyaml.load(yamlContent) || {});
        } catch (e) {
            alert('Invalid YAML: ' + e.message);
            return;
        }
    }
    hideModal('optionsModal');
    setStatus('Options applied');
}

// =============================
// Config Editor Modal
// =============================

function openConfigEditor() {
    showModal('editConfigModal');
}

function updateConfigEditorContent() {
    const format = $id('edit-format').value;
    const config = generateConfig();

    let content = '';
    if (format === 'yaml') {
        content = jsyaml.dump(config);
    } else {
        content = JSON.stringify(config, null, 2);
    }

    setEditorValue('edit-content', content, format === 'yaml' ? 'yaml' : 'json');
}

function copyConfigToClipboard() {
    const content = getEditorValue('edit-content');
    navigator.clipboard.writeText(content).then(function() {
        setStatus('Copied to clipboard');
    });
}

function downloadConfig() {
    const format = $id('edit-format').value;
    const content = getEditorValue('edit-content');
    const filename = 'pipeline-config.' + (format === 'yaml' ? 'yaml' : 'json');

    const blob = new Blob([content], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);

    setStatus('Downloaded ' + filename);
}

function clearConfigEditor() {
    if (!confirm('Clear all editor content?')) return;
    const format = $id('edit-format').value;
    setEditorValue('edit-content', '', format === 'yaml' ? 'yaml' : 'json');
}

function onImportFileSelected(e) {
    const file = e.target.files[0];
    e.target.value = ''; // allow re-selecting the same file later
    if (!file) return;

    const isJson = file.name.toLowerCase().endsWith('.json');
    const format = isJson ? 'json' : 'yaml';
    file.text().then(function(text) {
        $id('edit-format').value = format;
        return setEditorValue('edit-content', text, format);
    }).then(function() {
        setStatus('Loaded ' + file.name + ' — review and click Apply');
    }).catch(function(err) {
        setStatus('Failed to read ' + file.name + ': ' + err.message, 'error');
    });
}

function applyConfig() {
    const format = $id('edit-format').value;
    const content = getEditorValue('edit-content').trim();

    if (!content) {
        alert('Please enter configuration content');
        return;
    }

    let config;
    try {
        if (format === 'yaml') {
            config = jsyaml.load(content);
        } else {
            config = JSON.parse(content);
        }
    } catch (e) {
        alert('Failed to parse configuration: ' + e.message);
        return;
    }

    importConfigToCanvas(config);

    hideModal('editConfigModal');
    setStatus('Configuration applied');
}

// =============================
// Launch Modal
// =============================

let currentRunnerIndex = -1;
let currentEnvironmentIndex = -1;

function removeExtraOptions(select) {
    while (select.options.length > 1) {
        select.remove(1);
    }
}

function openLaunchModal() {
    const config = generateConfig();
    const errors = getValidationErrors(config);
    if (errors.length > 0) {
        showResult(
            'Validation Errors',
            'Please fix the following issues before launching:\n\n' + errors.join('\n'),
            'error'
        );
        setStatus('Validation failed', 'error');
        return;
    }

    ensureSchema('launch').then(function(schema) {
        if (!schema || !schema.oneOf || schema.oneOf.length === 0) {
            showResult(
                'Launch Configuration Error',
                'No launch runners configured. Please check the server configuration.',
                'error'
            );
            return;
        }

        // Reset state
        currentRunnerIndex = -1;
        currentEnvironmentIndex = -1;

        // Populate runner options from oneOf
        const runnerSelect = $id('launch-runner');
        removeExtraOptions(runnerSelect);

        schema.oneOf.forEach(function(runnerSchema, index) {
            const runnerId = runnerSchema['$id'] || '';
            const runnerName = runnerId.split('/').pop() || runnerSchema.title || 'runner_' + index;
            const option = document.createElement('option');
            option.value = index;
            option.textContent = runnerSchema.title || runnerName;
            option.dataset.runnerName = runnerName;
            runnerSelect.appendChild(option);
        });

        // Reset UI
        runnerSelect.value = '';
        $id('launch-runner-desc').textContent = '';
        $id('launch-args').value = '';
        hide($id('launch-environment-group'));
        const envSelect = $id('launch-environment');
        removeExtraOptions(envSelect);
        envSelect.value = '';
        $id('launch-environment-desc').textContent = '';
        hide($id('launch-parameters-container'));
        $id('launch-parameters-fields').innerHTML = '';

        showModal('launchModal');
    }).catch(function(err) {
        console.error('Failed to load launch schema:', err);
        showResult('Launch Configuration Error', 'Failed to load launch configuration from server.', 'error');
    });
}

function onRunnerChanged() {
    const runnerIndexStr = $id('launch-runner').value;
    const envSelect = $id('launch-environment');

    // Reset downstream
    removeExtraOptions(envSelect);
    envSelect.value = '';
    hide($id('launch-environment-group'));
    $id('launch-environment-desc').textContent = '';
    hide($id('launch-parameters-container'));
    $id('launch-parameters-fields').innerHTML = '';
    currentEnvironmentIndex = -1;

    if (runnerIndexStr === '') {
        $id('launch-runner-desc').textContent = '';
        currentRunnerIndex = -1;
        return;
    }

    currentRunnerIndex = parseInt(runnerIndexStr, 10);
    const runnerSchema = getCachedSchema('launch').oneOf[currentRunnerIndex];
    if (!runnerSchema) return;

    $id('launch-runner-desc').textContent = runnerSchema.description || '';

    // Check if runner has nested oneOf (environments)
    if (runnerSchema.oneOf && Array.isArray(runnerSchema.oneOf) && runnerSchema.oneOf.length > 0) {
        // Populate environment options
        runnerSchema.oneOf.forEach(function(envSchema, index) {
            const option = document.createElement('option');
            option.value = index;
            option.textContent = envSchema.title || 'Environment ' + (index + 1);
            envSelect.appendChild(option);
        });
        show($id('launch-environment-group'));

        // Auto-select if only one environment
        if (runnerSchema.oneOf.length === 1) {
            envSelect.value = '0';
            onEnvironmentChanged();
        }
    } else {
        // No environments, show parameters form
        showLaunchParametersForm(runnerSchema, null);
    }
}

function onEnvironmentChanged() {
    const envIndexStr = $id('launch-environment').value;

    hide($id('launch-parameters-container'));
    $id('launch-parameters-fields').innerHTML = '';

    if (currentRunnerIndex < 0 || envIndexStr === '') {
        $id('launch-environment-desc').textContent = '';
        currentEnvironmentIndex = -1;
        return;
    }

    currentEnvironmentIndex = parseInt(envIndexStr, 10);
    const runnerSchema = getCachedSchema('launch').oneOf[currentRunnerIndex];
    if (!runnerSchema || !runnerSchema.oneOf) return;

    const envSchema = runnerSchema.oneOf[currentEnvironmentIndex];
    if (!envSchema) return;

    $id('launch-environment-desc').textContent = envSchema.description || '';

    // Show parameters form
    showLaunchParametersForm(runnerSchema, envSchema);
}

function showLaunchParametersForm(runnerSchema, envSchema) {
    // Merge properties from runner and environment schemas
    const allProps = Object.assign({}, runnerSchema.properties || {}, (envSchema && envSchema.properties) || {});
    if (Object.keys(allProps).length === 0) {
        return;
    }

    const fields = $id('launch-parameters-fields');
    fields.innerHTML = '';

    for (const propName in allProps) {
        const prop = allProps[propName];
        const desc = prop.description || '';
        const defaultVal = prop.default !== undefined ? prop.default : '';
        const isReadonly = prop.readOnly === true;
        const type = prop.type || 'string';

        const group = document.createElement('div');
        group.className = 'mb-2';

        const label = document.createElement('label');
        label.className = 'form-label small mb-1';
        label.textContent = prop.title || propName;
        group.appendChild(label);

        let input;
        if (prop.enum && Array.isArray(prop.enum)) {
            // Enum -> select dropdown
            input = document.createElement('select');
            input.className = 'form-select form-select-sm launch-param-field';
            input.dataset.paramName = propName;
            prop.enum.forEach(function(val) {
                const option = document.createElement('option');
                option.value = val;
                option.textContent = val;
                if (String(val) === String(defaultVal)) option.selected = true;
                input.appendChild(option);
            });
            if (isReadonly) input.disabled = true;
        } else if (type === 'boolean') {
            // Boolean -> checkbox
            input = document.createElement('div');
            input.className = 'form-check';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input launch-param-field';
            checkbox.dataset.paramName = propName;
            if (defaultVal === true) checkbox.checked = true;
            if (isReadonly) checkbox.disabled = true;
            const checkboxLabel = document.createElement('label');
            checkboxLabel.className = 'form-check-label small';
            checkboxLabel.textContent = propName;
            input.appendChild(checkbox);
            input.appendChild(checkboxLabel);
        } else if (type === 'integer' || type === 'number') {
            // Number -> number input
            input = document.createElement('input');
            input.type = 'number';
            input.className = 'form-control form-control-sm launch-param-field';
            input.dataset.paramName = propName;
            if (defaultVal !== '') input.value = defaultVal;
            if (isReadonly) input.readOnly = true;
            if (type === 'integer') input.step = '1';
        } else {
            // String -> text input
            input = document.createElement('input');
            input.type = 'text';
            input.className = 'form-control form-control-sm launch-param-field';
            input.dataset.paramName = propName;
            if (defaultVal !== '') input.value = defaultVal;
            if (isReadonly) input.readOnly = true;
        }

        group.appendChild(input);
        if (desc) {
            const help = document.createElement('div');
            help.className = 'form-text small';
            help.textContent = desc;
            group.appendChild(help);
        }
        fields.appendChild(group);
    }

    show($id('launch-parameters-container'));
}

function executeLaunch() {
    // Validate runner selection
    const runnerSelect = $id('launch-runner');
    if (currentRunnerIndex < 0) {
        runnerSelect.classList.add('is-invalid');
        return;
    }
    runnerSelect.classList.remove('is-invalid');

    const runnerSchema = getCachedSchema('launch').oneOf[currentRunnerIndex];
    const runnerId = runnerSchema['$id'] || '';
    const runnerName = runnerId.split('/').pop() || runnerSchema.title || 'unknown';

    // Validate environment selection if needed
    let envName = null;
    if (runnerSchema.oneOf && runnerSchema.oneOf.length > 0) {
        const envSelect = $id('launch-environment');
        if (currentEnvironmentIndex < 0) {
            envSelect.classList.add('is-invalid');
            return;
        }
        envSelect.classList.remove('is-invalid');
        const envSchema = runnerSchema.oneOf[currentEnvironmentIndex];
        envName = envSchema.title || 'env_' + currentEnvironmentIndex;
    }

    // Collect parameters from form fields
    const parameters = {};
    document.querySelectorAll('#launch-parameters-fields .launch-param-field').forEach(function(el) {
        const name = el.dataset.paramName;
        if (!name) return;
        let val;
        if (el.type === 'checkbox') {
            val = el.checked;
        } else if (el.type === 'number') {
            if (el.value === '') return; // skip empty numbers
            val = el.step === '1' ? parseInt(el.value, 10) : parseFloat(el.value);
        } else {
            val = el.value;
            if (val === '') return; // skip empty strings
        }
        parameters[name] = val;
    });

    // Parse args JSON (optional)
    const argsInput = $id('launch-args');
    const argsText = argsInput.value.trim();
    let args = null;
    if (argsText) {
        try {
            args = JSON.parse(argsText);
        } catch (e) {
            argsInput.classList.add('is-invalid');
            return;
        }
    }
    argsInput.classList.remove('is-invalid');

    // Build launch config
    const launchConfig = {
        runner: runnerName,
        parameters: parameters
    };

    if (envName) {
        launchConfig.environment = envName;
    }

    if (args) {
        launchConfig.args = args;
    }

    hideModal('launchModal');

    // Execute
    runPipelineWithLaunch(launchConfig);
}

// =============================
// Event wiring
// =============================

export function initModalEvents() {
    // Module Config Modal
    on('btn-save-module', 'click', saveModuleConfig);
    on('btn-delete-module', 'click', deleteModule);

    // System Modal
    on('btn-system', 'click', openSystemModal);
    on('btn-apply-system', 'click', applySystemConfig);

    // Options Modal
    on('btn-options', 'click', openOptionsModal);
    on('btn-apply-options', 'click', applyOptionsConfig);

    // Launch Modal
    on('btn-launch', 'click', openLaunchModal);
    on('launch-runner', 'change', onRunnerChanged);
    on('launch-environment', 'change', onEnvironmentChanged);
    on('btn-launch-execute', 'click', executeLaunch);

    // Config Editor Modal
    on('btn-edit', 'click', openConfigEditor);
    on('edit-format', 'change', updateConfigEditorContent);
    on('btn-import-config', 'click', function() { $id('file-import').click(); });
    on('file-import', 'change', onImportFileSelected);
    on('btn-copy-config', 'click', copyConfigToClipboard);
    on('btn-download-config', 'click', downloadConfig);
    on('btn-apply-config', 'click', applyConfig);
    on('btn-clear-config', 'click', clearConfigEditor);

    // Monaco: modal shown handlers (Bootstrap dispatches these as native events)
    // Fetch module schema on demand so the HTTP round-trip provides a natural
    // macrotask boundary for the language service to initialize.
    on('moduleConfigModal', 'shown.bs.modal', function() {
        const type = pending.moduleType;
        const name = pending.moduleName;
        const yaml = pending.moduleYaml;
        Promise.all([
            loadMonaco(),
            // Not every catalog module has an editor schema — edit without one on 404
            getJson('/api/spec/' + type + '/' + name).catch(function() { return null; })
        ]).then(function(results) {
            const moduleEditorSchema = results[1];
            applyYamlSchemas(moduleEditorSchema ? [{
                uri: 'internal://module-config/' + type + '/' + name,
                fileMatch: ['internal://server/module-yaml-editor.yaml'],
                schema: moduleEditorSchema
            }] : []);
            return setEditorValue('module-yaml-editor', yaml);
        });
    });
    on('editConfigModal', 'shown.bs.modal', function() {
        updateConfigEditorContent();
    });
    on('systemModal', 'shown.bs.modal', function() {
        Promise.all([loadMonaco(), ensureSchema('system')]).then(function() {
            applyYamlSchemas();
            buildSchemaHelpTooltip('system-help-icon', getCachedSchema('system'));
            return setEditorValue('system-yaml-editor', pending.systemYaml);
        });
    });
    on('optionsModal', 'shown.bs.modal', function() {
        Promise.all([loadMonaco(), ensureSchema('options')]).then(function() {
            applyYamlSchemas();
            buildSchemaHelpTooltip('options-help-icon', getCachedSchema('options'));
            return setEditorValue('options-yaml-editor', pending.optionsYaml);
        });
    });
}

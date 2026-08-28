/**
 * modals.js - Module config, System, Options and Launch modals.
 */
'use strict';

import { $id, on, show, hide, showModal, hideModal, getJson, setStatus, dumpYaml } from './util.js';
import { loadMonaco, setEditorValue, getEditorValue, ensureSchema, getCachedSchema,
         applyYamlSchemas, buildSchemaHelpTooltip } from './monaco.js';
import { getNodeData, updateNodeData, isNodeNameTaken, removeNode,
         NODE_CONFIG_PROPS, extractExtraProps } from './canvas.js';
import { getConfig, getValidationErrors,
         getSystem, setSystem, getOptions, setOptions } from './workspace.js';
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
    typeBadge.classList.remove('source', 'transform', 'sink', 'action');
    typeBadge.classList.add(data.moduleType);
    $id('modal-module-name').textContent = data.moduleName;

    // Set name input
    $id('module-name-input').value = data.name;

    // Build config object excluding internal properties; fields the canvas
    // does not model (`extra`) are shown inline so they can be edited too
    const configObj = {};
    const internalProps = ['moduleName', 'moduleType', 'name', 'outputSchema', 'output', 'outputNames', 'waits', 'sideInputs', 'extra'];
    for (const key in data) {
        if (Object.prototype.hasOwnProperty.call(data, key) && internalProps.indexOf(key) === -1) {
            configObj[key] = data[key];
        }
    }
    Object.keys(data.extra || {}).forEach(function(key) {
        configObj[key] = data.extra[key];
    });

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
    NODE_CONFIG_PROPS.forEach(function(prop) {
        if (parsed[prop] !== undefined) {
            data[prop] = parsed[prop];
        } else {
            delete data[prop];
        }
    });
    // anything else typed here is a field the canvas does not model: keep it
    data.extra = extractExtraProps(parsed);

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

// =============================
// Snippet chips (System / Options editors)
// =============================
//
// Each editor has a set of named YAML snippets. When the section is empty the
// editor opens with all snippets as a commented template; the "Insert" chips
// above the editor append a snippet (uncommented) and are disabled while the
// editor already contains that top-level key.

const SYSTEM_SNIPPETS = {
    args: 'args:            # template variables: ${args.<key>}, overridable at launch\n'
        + '  today: "${utils.datetime.currentDate(\'Asia/Tokyo\')}"\n',
    context: 'context: train   # assemble only modules whose tags contain this value\n',
    imports: 'imports:         # merge modules from other config files\n'
        + '  - base: gs://bucket/configs/\n'
        + '    files: [common.yaml]\n',
    failure: 'failure:\n'
        + '  failFast: false      # false = keep running, route errors to dead-letter sinks\n'
        + '  union: false\n'
        + '  sinks:\n'
        + '    - name: dead_letter\n'
        + '      module: pubsub\n'
        + '      parameters:\n'
        + '        topic: projects/xxx/topics/dead-letter\n'
};

const OPTIONS_SNIPPETS = {
    jobName: 'jobName: my-pipeline\n',
    streaming: 'streaming: false     # true = unbounded (streaming) job\n',
    dataflow: 'dataflow:            # Cloud Dataflow runner options\n'
        + '  workerMachineType: n2-standard-2\n'
        + '  numWorkers: 1\n'
        + '  maxNumWorkers: 4\n'
        + '  serviceAccount: sa@project.iam.gserviceaccount.com\n'
        + '  subnetwork: regions/asia-northeast1/subnetworks/default\n'
        + '  usePublicIps: false\n',
    direct: 'direct:              # Direct runner options (local execution)\n'
        + '  targetParallelism: 4\n'
        + '  blockOnRun: true\n',
    prism: 'prism:               # Prism runner options (local portable runner)\n'
        + '  enableWebUI: false\n',
    portable: 'portable:            # Portable runner options (job service)\n'
        + '  jobEndpoint: localhost:8099\n'
        + '  defaultEnvironmentType: LOOPBACK\n',
    flink: 'flink:               # Apache Flink runner options\n'
        + '  flinkMaster: "[auto]"\n'
        + '  parallelism: 4\n',
    spark: 'spark:               # Apache Spark runner options\n'
        + '  sparkMaster: "local[*]"\n',
    gcp: 'gcp:                 # Google Cloud options\n'
        + '  project: my-project\n'
        + '  workerRegion: asia-northeast1\n',
    aws: 'aws:                 # AWS options\n'
        + '  region: ap-northeast-1\n',
    beamsql: 'beamsql:\n'
        + '  plannerName: org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner\n'
};

const SNIPPET_EDITORS = {
    system: { editorId: 'system-yaml-editor', chipsId: 'system-snippets', snippets: SYSTEM_SNIPPETS,
        header: 'System settings: uncomment what you need, or use the Insert buttons above' },
    options: { editorId: 'options-yaml-editor', chipsId: 'options-snippets', snippets: OPTIONS_SNIPPETS,
        header: 'Pipeline options: uncomment what you need, or use the Insert buttons above' }
};

function snippetTemplateYaml(kind) {
    const def = SNIPPET_EDITORS[kind];
    const lines = ['# ' + def.header, '#'];
    Object.keys(def.snippets).forEach(function(key) {
        def.snippets[key].trimEnd().split('\n').forEach(function(line) {
            lines.push('# ' + line);
        });
        lines.push('#');
    });
    return lines.join('\n') + '\n';
}

// Parsed top-level object of the editor content: {} when empty / comments only,
// null when the YAML does not parse (mid-edit) - callers must not touch the text then.
function parseSnippetEditor(kind) {
    try {
        const parsed = jsyaml.load(getEditorValue(SNIPPET_EDITORS[kind].editorId));
        return (parsed && typeof parsed === 'object') ? parsed : {};
    } catch (e) {
        return null;
    }
}

function refreshSnippetChips(kind) {
    const present = parseSnippetEditor(kind);
    document.querySelectorAll('#' + SNIPPET_EDITORS[kind].chipsId + ' [data-snippet]').forEach(function(btn) {
        if (present === null) {
            btn.disabled = true;
            btn.title = 'Fix the YAML syntax first';
        } else {
            btn.disabled = Object.prototype.hasOwnProperty.call(present, btn.dataset.snippet);
            btn.title = '';
        }
    });
}

function insertSnippet(kind, key) {
    const def = SNIPPET_EDITORS[kind];
    const snippet = def.snippets[key];
    if (!snippet) return;
    const present = parseSnippetEditor(kind);
    if (present === null) return;   // invalid YAML: never overwrite what the user is typing
    let current = getEditorValue(def.editorId);
    // Replace the all-comment template (nothing set yet) instead of appending below it
    if (Object.keys(present).length === 0) {
        current = '';
    }
    if (current.length && !current.endsWith('\n')) current += '\n';
    setEditorValue(def.editorId, current + snippet).then(function() {
        refreshSnippetChips(kind);
    });
}

// Called after the editor content is set on modal open: sync chips and follow edits
function wireSnippetChips(kind, ed) {
    refreshSnippetChips(kind);
    if (ed && !ed.__snippetChipsWired) {
        ed.__snippetChipsWired = true;
        ed.onDidChangeModelContent(function() { refreshSnippetChips(kind); });
    }
}

// Build the chip buttons from the snippet maps so the two cannot drift apart
function initSnippetChips() {
    Object.keys(SNIPPET_EDITORS).forEach(function(kind) {
        const def = SNIPPET_EDITORS[kind];
        const container = $id(def.chipsId);
        Object.keys(def.snippets).forEach(function(key) {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'btn btn-outline-secondary btn-sm';
            btn.dataset.snippet = key;
            btn.textContent = key;
            btn.addEventListener('click', function() { insertSnippet(kind, key); });
            container.appendChild(btn);
        });
    });
}

export function openSystemModal() {
    const system = getSystem();
    pending.systemYaml = Object.keys(system).length ? dumpYaml(system) : snippetTemplateYaml('system');
    showModal('systemModal');
}

function applySystemConfig() {
    const yamlContent = getEditorValue('system-yaml-editor').trim();
    if (!yamlContent) {
        setSystem({});
    } else {
        try {
            setSystem(jsyaml.load(yamlContent) || {});
        } catch (e) {
            alert('Invalid YAML: ' + e.message);
            return;
        }
    }
    hideModal('systemModal');
    setStatus('System settings applied');
}

export function openOptionsModal() {
    const options = getOptions();
    pending.optionsYaml = Object.keys(options).length ? dumpYaml(options) : snippetTemplateYaml('options');
    showModal('optionsModal');
}

function applyOptionsConfig() {
    const yamlContent = getEditorValue('options-yaml-editor').trim();
    if (!yamlContent) {
        setOptions({});
    } else {
        try {
            setOptions(jsyaml.load(yamlContent) || {});
        } catch (e) {
            alert('Invalid YAML: ' + e.message);
            return;
        }
    }
    hideModal('optionsModal');
    setStatus('Options applied');
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
    const config = getConfig();
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
            if (runnerSchema['x-hidden'] === true) return;
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
        // Populate environment options (x-hidden environments are dev-only and not offered)
        let visible = 0;
        let firstVisible = -1;
        runnerSchema.oneOf.forEach(function(envSchema, index) {
            if (envSchema['x-hidden'] === true) return;
            const option = document.createElement('option');
            option.value = index;
            option.textContent = envSchema.title || 'Environment ' + (index + 1);
            envSelect.appendChild(option);
            visible++;
            if (firstVisible < 0) firstVisible = index;
        });
        show($id('launch-environment-group'));

        // Auto-select if only one environment
        if (visible === 1) {
            envSelect.value = String(firstVisible);
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
    const requiredProps = new Set([].concat(runnerSchema.required || [], (envSchema && envSchema.required) || []));

    const fields = $id('launch-parameters-fields');
    fields.innerHTML = '';

    for (const propName in allProps) {
        const prop = allProps[propName];
        const desc = prop.description || '';
        const defaultVal = prop.default !== undefined ? prop.default : '';
        // Server-resolved value used when the field is left empty: shown as a placeholder only, so
        // an untouched field is submitted empty and config options keep precedence over the environment.
        const hint = prop['x-default-hint'] !== undefined ? String(prop['x-default-hint']) : '';
        const isReadonly = prop.readOnly === true;
        const type = prop.type || 'string';

        const group = document.createElement('div');
        group.className = 'mb-2';

        const isRequired = requiredProps.has(propName);
        const label = document.createElement('label');
        label.className = 'form-label small mb-1';
        label.textContent = prop.title || propName;
        if (isRequired) {
            const mark = document.createElement('span');
            mark.className = 'text-danger';
            mark.textContent = ' *';
            label.appendChild(mark);
        }
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
            if (hint) input.placeholder = hint;
            if (isReadonly) input.readOnly = true;
            if (type === 'integer') input.step = '1';
        } else {
            // String -> text input
            input = document.createElement('input');
            input.type = 'text';
            input.className = 'form-control form-control-sm launch-param-field';
            input.dataset.paramName = propName;
            if (defaultVal !== '') input.value = defaultVal;
            if (hint) input.placeholder = hint;
            if (isReadonly) input.readOnly = true;
        }

        group.appendChild(input);
        const field = input.classList.contains('launch-param-field') ? input : input.querySelector('.launch-param-field');
        if (field && isRequired) {
            field.dataset.required = 'true';
            // The project stylesheet forces .invalid-feedback to display:block, so the message is
            // only added while the field is invalid (see validateLaunchParameters).
            const feedback = document.createElement('div');
            feedback.className = 'invalid-feedback launch-required-feedback';
            feedback.textContent = (prop.title || propName) + ' is required';
            feedback.style.display = 'none';
            group.appendChild(feedback);
            field.addEventListener('input', function() {
                field.classList.remove('is-invalid');
                feedback.style.display = 'none';
            });
        }
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

/** Mark empty required parameter fields invalid; returns true when all are filled. */
function validateLaunchParameters() {
    let firstInvalid = null;
    document.querySelectorAll('#launch-parameters-fields .launch-param-field[data-required="true"]').forEach(function(el) {
        // a field the server can fill (placeholder = resolved default) is satisfied when left empty
        const empty = el.type === 'checkbox' ? false : (String(el.value).trim() === '' && !el.placeholder);
        el.classList.toggle('is-invalid', empty);
        const feedback = el.closest('.mb-2') && el.closest('.mb-2').querySelector('.launch-required-feedback');
        if (feedback) feedback.style.display = empty ? 'block' : 'none';
        if (empty && !firstInvalid) firstInvalid = el;
    });
    if (firstInvalid) {
        firstInvalid.focus();
        return false;
    }
    return true;
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
        const envId = envSchema['$id'] || '';
        envName = envId.split('/').pop() || envSchema.title || 'env_' + currentEnvironmentIndex;
    }

    if (!validateLaunchParameters()) {
        return;
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

    // System / Options modals (opened from the explorer's outline rows)
    on('btn-apply-system', 'click', applySystemConfig);
    initSnippetChips();
    on('btn-apply-options', 'click', applyOptionsConfig);

    // Launch Modal
    on('btn-launch', 'click', openLaunchModal);
    on('launch-runner', 'change', onRunnerChanged);
    on('launch-environment', 'change', onEnvironmentChanged);
    on('btn-launch-execute', 'click', executeLaunch);

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
    on('systemModal', 'shown.bs.modal', function() {
        Promise.all([loadMonaco(), ensureSchema('system')]).then(function() {
            applyYamlSchemas();
            buildSchemaHelpTooltip('system-help-icon', getCachedSchema('system'));
            return setEditorValue('system-yaml-editor', pending.systemYaml);
        }).then(function(ed) { wireSnippetChips('system', ed); });
    });
    on('optionsModal', 'shown.bs.modal', function() {
        Promise.all([loadMonaco(), ensureSchema('options')]).then(function() {
            applyYamlSchemas();
            buildSchemaHelpTooltip('options-help-icon', getCachedSchema('options'));
            return setEditorValue('options-yaml-editor', pending.optionsYaml);
        }).then(function(ed) { wireSnippetChips('options', ed); });
    });
}

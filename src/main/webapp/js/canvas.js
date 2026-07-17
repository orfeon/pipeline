/**
 * canvas.js - Drawflow adapter and pipeline config generation.
 *
 * All Drawflow API access is confined to this module. Other modules interact
 * with the canvas only through the exported functions, so replacing the node
 * editor library would touch this file only.
 */
'use strict';

import { $id, setStatus, escapeHtml } from './util.js';

let editor = null;
let nodeCounter = { source: 0, transform: 0, sink: 0 };
let systemConfig = {};
let optionsConfig = {};
const moduleSchemas = {};   // dryrun result cache (module name -> schema)
const moduleOutputs = {};   // run result cache (module name -> output)

// Wired by initDrawflow: { onEditNode(nodeId), onShowSchema(name, schema), onShowRecords(name, output) }
let callbacks = {};

// Notified (post-hoc) whenever the pipeline on the canvas changes; used for auto-save.
let changeListener = null;

export function setChangeListener(listener) {
    changeListener = listener;
}

function notifyChanged() {
    if (changeListener) changeListener();
}

// =============================
// System / Options config state
// =============================

export function getSystemConfig() {
    return systemConfig;
}

export function setSystemConfig(config) {
    systemConfig = config || {};
    notifyChanged();
}

export function getOptionsConfig() {
    return optionsConfig;
}

export function setOptionsConfig(config) {
    optionsConfig = config || {};
    notifyChanged();
}

// =============================
// Drawflow initialization
// =============================

export function initDrawflow(cb) {
    callbacks = cb || {};

    const container = $id('drawflow');
    editor = new Drawflow(container);
    editor.reroute = true;
    editor.curvature = 0.5;
    editor.reroute_curvature_start_end = 0.5;
    editor.reroute_curvature = 0.5;
    editor.force_first_input = false;
    editor.line_path = 1;
    editor.editor_mode = 'edit';
    editor.start();

    // Wrap updateConnectionNodes to fix paths for top-positioned ports (input_2, input_3)
    const _origUpdateConnectionNodes = editor.updateConnectionNodes.bind(editor);
    editor.updateConnectionNodes = function(id) {
        _origUpdateConnectionNodes(id);
        fixTopInputPaths(id);
    };

    // Event listeners
    editor.on('nodeRemoved', function() {
        setStatus('Module removed');
    });

    // Auto-save notifications for every structural change
    ['nodeCreated', 'nodeRemoved', 'nodeMoved', 'connectionCreated', 'connectionRemoved']
        .forEach(function(eventName) {
            editor.on(eventName, notifyChanged);
        });

    editor.on('connectionCreated', function(connection) {
        // Prevent input_1 connections on source nodes
        const targetNode = editor.getNodeFromId(connection.input_id);
        if (targetNode && targetNode.data.moduleType === 'source' && connection.input_class === 'input_1') {
            editor.removeSingleConnection(connection.output_id, connection.input_id, connection.output_class, connection.input_class);
            setStatus('Source modules cannot have data inputs', 'warning');
            return;
        }
        const connType = connection.input_class === 'input_1' ? 'input' : connection.input_class === 'input_2' ? 'wait' : 'sideInput';
        setStatus(connType + ' connection created');
    });

    // Double click to edit node
    container.addEventListener('dblclick', function(e) {
        const nodeElement = e.target.closest('.drawflow-node');
        if (nodeElement) {
            const nodeId = nodeElement.id.replace('node-', '');
            if (callbacks.onEditNode) {
                callbacks.onEditNode(parseInt(nodeId));
            }
            return;
        }

        // Double click on connection to delete
        const connectionElement = e.target.closest('.connection');
        if (connectionElement) {
            const connectionClass = connectionElement.classList;
            let outputNodeId = null;
            let inputNodeId = null;
            let outputClass = null;
            let inputClass = null;

            connectionClass.forEach(function(cls) {
                if (cls.startsWith('node_out_node-')) {
                    outputNodeId = cls.replace('node_out_node-', '');
                } else if (cls.startsWith('node_in_node-')) {
                    inputNodeId = cls.replace('node_in_node-', '');
                } else if (cls.startsWith('output_')) {
                    outputClass = cls;
                } else if (cls.startsWith('input_')) {
                    inputClass = cls;
                }
            });

            if (outputNodeId && inputNodeId && outputClass && inputClass) {
                if (confirm('Delete this connection?')) {
                    editor.removeSingleConnection(outputNodeId, inputNodeId, outputClass, inputClass);
                    const connType = inputClass === 'input_1' ? 'input' : inputClass === 'input_2' ? 'wait' : 'sideInput';
                    setStatus(connType + ' connection deleted');
                }
            }
        }
    });
}

/**
 * Fix SVG paths for connections targeting top-positioned ports (input_2, input_3).
 * Drawflow generates horizontal bezier curves by default; this recalculates them
 * so the line arrives vertically from above, matching the port's visual position.
 */
function fixTopInputPaths(nodeId) {
    const selector = '.connection.node_in_' + nodeId + '.input_2 .main-path, '
                 + '.connection.node_in_' + nodeId + '.input_3 .main-path, '
                 + '.connection.node_out_' + nodeId + '.input_2 .main-path, '
                 + '.connection.node_out_' + nodeId + '.input_3 .main-path';
    const paths = document.querySelectorAll(selector);
    paths.forEach(function(path) {
        const d = path.getAttributeNS(null, 'd');
        if (!d) return;
        // Match single-segment cubic bezier: M sx sy C cp1x cp1y cp2x cp2y ex ey
        const m = d.trim().match(/^M\s+([\d.e+-]+)\s+([\d.e+-]+)\s+C\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s*$/);
        if (!m) return; // skip rerouted / complex paths
        const sx = parseFloat(m[1]), sy = parseFloat(m[2]);
        const ex = parseFloat(m[7]), ey = parseFloat(m[8]);
        const dx = Math.abs(ex - sx);
        const dy = Math.abs(ey - sy);
        // Depart horizontally to the right from output
        const cp1x = sx + Math.max(dx * 0.4, 40);
        const cp1y = sy;
        // Arrive vertically from above at input
        const cp2x = ex;
        const cp2y = ey - Math.max(dy * 0.4, 40);
        path.setAttributeNS(null, 'd',
            ' M ' + sx + ' ' + sy + ' C ' + cp1x + ' ' + cp1y + ' ' + cp2x + ' ' + cp2y + ' ' + ex + '  ' + ey);
    });
}

// =============================
// Module list (left pane)
// =============================

export function initModuleList(moduleDefs) {
    const lists = {
        source: $id('source-modules'),
        transform: $id('transform-modules'),
        sink: $id('sink-modules')
    };
    Object.keys(lists).forEach(function(type) {
        moduleDefs[type + 's'].forEach(function(module) {
            lists[type].appendChild(createModuleItem(module, type));
        });
    });
}

function createModuleItem(module, type) {
    const item = document.createElement('div');
    item.className = 'module-item ' + type;
    item.dataset.module = module.name;
    item.dataset.type = type;
    item.title = module.description
        + (module.tags && module.tags.length ? '\n\nTags: ' + module.tags.join(', ') : '');
    item.innerHTML = '<i class="bi bi-plus-circle"></i> ' + escapeHtml(module.name);
    item.addEventListener('click', function() {
        addModuleToCanvas(module.name, type);
    });
    return item;
}

// =============================
// Node management
// =============================

/**
 * Stamp each output dot with its tag so CSS renders a label
 * (content: attr(data-output-label)) and hover shows a tooltip.
 * A single default output stays unlabeled.
 */
function applyOutputLabels(nodeId) {
    const data = editor.getNodeFromId(nodeId).data;
    const outputNames = Array.isArray(data.outputNames) ? data.outputNames : [''];
    if (outputNames.length === 1 && outputNames[0] === '') return;
    outputNames.forEach(function(tag, index) {
        const dot = document.querySelector('#node-' + nodeId + ' .output.output_' + (index + 1));
        if (!dot) return;
        const label = tag || 'out';
        dot.setAttribute('data-output-label', label);
        dot.title = 'output: ' + (tag ? data.name + '.' + tag : data.name);
    });
    // keep the node tall enough for its output dot column
    if (outputNames.length > 2) {
        const content = document.querySelector('#node-' + nodeId + ' .node-content');
        if (content) {
            content.style.minHeight = (outputNames.length * 24) + 'px';
        }
    }
}

/**
 * Return the output port class ('output_N') for the given tag on a node,
 * adding a port when the tag is not (yet) known — e.g. a config referencing
 * an output the parameter-derived list did not predict.
 */
function ensureOutputPort(nodeId, tag) {
    const node = editor.getNodeFromId(nodeId);
    const outputNames = Array.isArray(node.data.outputNames) ? node.data.outputNames.slice() : [''];
    let index = outputNames.indexOf(tag);
    if (index < 0) {
        editor.addNodeOutput(nodeId);
        outputNames.push(tag);
        node.data.outputNames = outputNames;
        editor.updateNodeDataFromId(nodeId, node.data);
        applyOutputLabels(nodeId);
        index = outputNames.length - 1;
    }
    return 'output_' + (index + 1);
}

export function addModuleToCanvas(moduleName, moduleType, config) {
    config = config || null;
    nodeCounter[moduleType]++;
    const defaultName = (config && config.name) ? config.name : (moduleName + '_' + nodeCounter[moduleType]);

    const inputs = 3;   // input_1: data, input_2: wait, input_3: sideInput
    // Nodes start with a single default output port. Named-output ports
    // (partition/query) appear when a config references them (import) or when
    // dryrun/run reports their schemas — no parameter-derived duplication.
    const outputs = 1;

    const nodeHtml = createNodeHtml(moduleName, moduleType, defaultName, outputs);

    // Place node near top-left of visible viewport, accounting for pan/zoom
    const zoom = editor.zoom || 1;
    const canvasX = editor.canvas_x || 0;
    const canvasY = editor.canvas_y || 0;
    const margin = 30;
    const posX = (margin - canvasX) / zoom + Math.random() * 80;
    const posY = (margin - canvasY) / zoom + Math.random() * 80;

    const nodeData = {
        moduleName: moduleName,
        moduleType: moduleType,
        name: defaultName,
        parameters: (config && config.parameters) ? config.parameters : {}
    };

    // Store all config properties in nodeData
    if (config) {
        const configProps = ['schema', 'strategy', 'tags', 'logs', 'timestampAttribute', 'failFast', 'ignore'];
        configProps.forEach(function(prop) {
            if (config[prop] !== undefined && config[prop] !== null) {
                nodeData[prop] = config[prop];
            }
        });
    }

    const nodeId = editor.addNode(
        moduleName,
        inputs,
        outputs,
        posX,
        posY,
        moduleType,
        nodeData,
        nodeHtml
    );

    setStatus('Added ' + moduleType + ': ' + moduleName);
    return nodeId;
}

function createNodeHtml(moduleName, moduleType, name, outputCount) {
    const icons = {
        source: 'bi-box-arrow-in-right',
        transform: 'bi-arrow-left-right',
        sink: 'bi-box-arrow-right'
    };

    // Give the node enough height for its output dot column
    const minHeight = (outputCount || 1) > 2 ? ' style="min-height: ' + (outputCount * 24) + 'px"' : '';

    return '<div class="node-content"' + minHeight + '>' +
        '<div class="node-header ' + moduleType + '">' +
            '<i class="bi ' + icons[moduleType] + '"></i>' +
            '<span>' + escapeHtml(moduleName) + '</span>' +
        '</div>' +
        '<div class="node-body">' +
            '<div class="node-name">' + escapeHtml(name) + '</div>' +
            '<div class="node-module">' + moduleType + '</div>' +
        '</div>' +
    '</div>';
}

export function getNodeData(nodeId) {
    return editor.getNodeFromId(nodeId).data;
}

/**
 * Update a node's data and re-render its HTML (name may have changed).
 */
export function updateNodeData(nodeId, data) {
    editor.updateNodeDataFromId(nodeId, data);
    const outputCount = Object.keys(editor.getNodeFromId(nodeId).outputs || {}).length;
    const newHtml = createNodeHtml(data.moduleName, data.moduleType, data.name, outputCount);
    const nodeElement = document.querySelector('#node-' + nodeId + ' .drawflow_content_node');
    if (nodeElement) {
        nodeElement.innerHTML = newHtml;
    }
    applyOutputLabels(nodeId);
    notifyChanged();
}

export function isNodeNameTaken(name, excludeNodeId) {
    const nodes = editor.export().drawflow.Home.data;
    for (const id in nodes) {
        if (parseInt(id) !== excludeNodeId && nodes[id].data.name === name) {
            return true;
        }
    }
    return false;
}

export function removeNode(nodeId) {
    editor.removeNodeId('node-' + nodeId);
}

/**
 * Scroll to the node with the given module name and flash it.
 * Returns false when no node on the canvas has that name.
 */
export function highlightNodeByName(name) {
    const nodes = editor.export().drawflow.Home.data;
    for (const id in nodes) {
        if (nodes[id].data.name === name) {
            const nodeElement = $id('node-' + id);
            if (!nodeElement) return false;
            nodeElement.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'center' });
            nodeElement.classList.remove('node-highlight');
            void nodeElement.offsetWidth; // restart the animation on repeated clicks
            nodeElement.classList.add('node-highlight');
            setTimeout(function() {
                nodeElement.classList.remove('node-highlight');
            }, 2400);
            return true;
        }
    }
    return false;
}

// =============================
// Result indicators (dryrun schema / run records)
// =============================

/** Split an output registry key ("module" or "module.tag") into its parts. */
function splitOutputName(outputName) {
    const dot = outputName.indexOf('.');
    return dot > 0
        ? { moduleName: outputName.substring(0, dot), tag: outputName.substring(dot + 1) }
        : { moduleName: outputName, tag: '' };
}

function findNodeIdByName(moduleName) {
    const nodes = editor.export().drawflow.Home.data;
    for (const id in nodes) {
        if (nodes[id].data.name === moduleName) {
            return parseInt(id);
        }
    }
    return null;
}

/**
 * The schema shown for a node: the default output's schema directly, or —
 * when dryrun reported named outputs — one nested group per output so the
 * existing schema renderer displays them together.
 */
function collectModuleSchemas(moduleName) {
    const namedKeys = Object.keys(moduleSchemas).filter(function(key) {
        return key.indexOf(moduleName + '.') === 0;
    });
    if (namedKeys.length === 0) {
        return moduleSchemas[moduleName];
    }
    const fields = [];
    if (moduleSchemas[moduleName]) {
        fields.push({ name: '(default)', type: 'element', fields: moduleSchemas[moduleName].fields || [] });
    }
    namedKeys.forEach(function(key) {
        fields.push({
            name: key.substring(moduleName.length + 1),
            type: 'element',
            fields: (moduleSchemas[key] || {}).fields || []
        });
    });
    return { fields: fields };
}

/**
 * Reflect a dryrun/run output schema on the canvas. outputName is the output
 * registry key: "module" for the default output, "module.tag" for named
 * outputs (partition/query) — named outputs get their port here.
 */
export function updateNodeSchemaIndicator(outputName, schema) {
    moduleSchemas[outputName] = schema;

    const parts = splitOutputName(outputName);
    const nodeId = findNodeIdByName(parts.moduleName);
    if (nodeId === null) return;
    const moduleName = parts.moduleName;

    if (parts.tag) {
        ensureOutputPort(nodeId, parts.tag);
    }

    const data = editor.getNodeFromId(nodeId).data;
    data.outputSchema = schema;
    editor.updateNodeDataFromId(nodeId, data);

    const nodeElement = document.querySelector('#node-' + nodeId + ' .node-content');
    if (nodeElement) {
        let indicator = nodeElement.querySelector('.node-schema-indicator');
        if (!indicator) {
            indicator = document.createElement('i');
            indicator.className = 'bi bi-file-earmark-text node-schema-indicator';
            indicator.title = 'View output schema';
            indicator.addEventListener('click', function(e) {
                e.stopPropagation();
                const latestSchema = collectModuleSchemas(moduleName);
                if (latestSchema && callbacks.onShowSchema) {
                    callbacks.onShowSchema(moduleName, latestSchema);
                }
            });
            nodeElement.style.position = 'relative';
            nodeElement.appendChild(indicator);
        }
        indicator.classList.add('has-schema');
    }
}

export function updateNodeOutputIndicator(outputName, output) {
    moduleOutputs[outputName] = output;

    const parts = splitOutputName(outputName);
    const nodeId = findNodeIdByName(parts.moduleName);
    if (nodeId === null) return;
    const moduleName = parts.moduleName;

    if (parts.tag) {
        ensureOutputPort(nodeId, parts.tag);
    }

    const data = editor.getNodeFromId(nodeId).data;
    data.output = output;
    editor.updateNodeDataFromId(nodeId, data);

    const nodeElement = document.querySelector('#node-' + nodeId + ' .node-content');
    if (nodeElement) {
        const schemaIndicator = nodeElement.querySelector('.node-schema-indicator');
        if (schemaIndicator) {
            schemaIndicator.style.display = 'none';
        }

        let indicator = nodeElement.querySelector('.node-output-indicator');
        if (!indicator) {
            indicator = document.createElement('i');
            indicator.className = 'bi bi-table node-output-indicator';
            indicator.title = 'View output records';
            indicator.addEventListener('click', function(e) {
                e.stopPropagation();
                // show the default output when present, else the first named one
                const key = moduleOutputs[moduleName] ? moduleName
                    : Object.keys(moduleOutputs).find(function(k) {
                        return k.indexOf(moduleName + '.') === 0;
                    });
                if (key && callbacks.onShowRecords) {
                    callbacks.onShowRecords(key, moduleOutputs[key]);
                }
            });
            nodeElement.style.position = 'relative';
            nodeElement.appendChild(indicator);
        }
        indicator.classList.add('has-output');
    }
}

// =============================
// Pipeline config generation
// =============================

function extractConnectionNames(nodeInputs, portName, nodeMap) {
    const names = [];
    const port = nodeInputs[portName];
    if (port && port.connections) {
        port.connections.forEach(function(conn) {
            const sourceNode = nodeMap[conn.node];
            if (!sourceNode) return;
            // conn.input holds the source node's output port class (drawflow convention)
            const tag = getOutputTagByClass(sourceNode, conn.input);
            names.push(tag ? sourceNode.data.name + '.' + tag : sourceNode.data.name);
        });
    }
    return names;
}

function getOutputTagByClass(node, outputClass) {
    const outputNames = node.data.outputNames;
    if (!Array.isArray(outputNames) || !outputClass) return '';
    const index = parseInt(String(outputClass).replace('output_', ''), 10) - 1;
    const tag = outputNames[index];
    return typeof tag === 'string' ? tag : '';
}

export function generateConfig() {
    const nodes = editor.export().drawflow.Home.data;

    const config = {
        sources: [],
        transforms: [],
        sinks: []
    };

    const nodeMap = {};
    for (const id in nodes) {
        nodeMap[id] = nodes[id];
    }

    for (const id in nodes) {
        const node = nodes[id];
        const data = node.data;
        const isSource = data.moduleType === 'source';

        const moduleConfig = {
            name: data.name,
            module: data.moduleName,
            parameters: data.parameters || {}
        };

        if (data.schema) {
            moduleConfig.schema = data.schema;
        }

        if (data.strategy) {
            moduleConfig.strategy = data.strategy;
        }

        // Additional Module Properties (waits/sideInputs derived from connections, not nodeData)
        const additionalProps = ['tags', 'logs', 'timestampAttribute', 'failFast', 'ignore'];
        additionalProps.forEach(function(prop) {
            if (data[prop] !== undefined && data[prop] !== null) {
                moduleConfig[prop] = data[prop];
            }
        });

        // Extract connections per port
        if (node.inputs) {
            if (!isSource) {
                const inputs = extractConnectionNames(node.inputs, 'input_1', nodeMap);
                if (inputs.length > 0) moduleConfig.inputs = inputs;
            }

            const waits = extractConnectionNames(node.inputs, 'input_2', nodeMap);
            if (waits.length > 0) moduleConfig.waits = waits;

            const sideInputs = extractConnectionNames(node.inputs, 'input_3', nodeMap);
            if (sideInputs.length > 0) moduleConfig.sideInputs = sideInputs;
        }

        if (isSource) {
            config.sources.push(moduleConfig);
        } else if (data.moduleType === 'transform') {
            config.transforms.push(moduleConfig);
        } else if (data.moduleType === 'sink') {
            config.sinks.push(moduleConfig);
        }
    }

    // Add system settings
    if (Object.keys(systemConfig).length > 0) {
        config.system = systemConfig;
    }

    // Add options
    if (Object.keys(optionsConfig).length > 0) {
        config.options = optionsConfig;
    }

    return config;
}

export function getValidationErrors(config) {
    const errors = [];

    if (config.sources.length === 0) {
        errors.push('At least one source module is required');
    }

    config.transforms.forEach(function(t) {
        if (!t.inputs || t.inputs.length === 0) {
            errors.push('Transform "' + t.name + '" has no inputs');
        }
    });

    config.sinks.forEach(function(s) {
        if (!s.inputs || s.inputs.length === 0) {
            errors.push('Sink "' + s.name + '" has no inputs');
        }
    });

    return errors;
}

/**
 * Rebuild the canvas from a parsed pipeline config
 * (shared by the config editor's Apply and the agent's apply-config).
 */
export function importConfigToCanvas(config) {
    editor.clear();
    nodeCounter = { source: 0, transform: 0, sink: 0 };

    const nodeIdMap = {};
    const layout = {
        startY: 50,
        nodeSpacingY: 150,
        columnX: { source: 100, transform: 400, sink: 700 }
    };

    // sourceRef may be "moduleName" or "moduleName.outputTag" (named outputs
    // of e.g. partition/query modules)
    function connect(sourceRef, nodeId, inputClass) {
        let sourceName = sourceRef;
        let tag = '';
        if (!(sourceRef in nodeIdMap)) {
            const dot = sourceRef.indexOf('.');
            if (dot > 0) {
                sourceName = sourceRef.substring(0, dot);
                tag = sourceRef.substring(dot + 1);
            }
        }
        const sourceNodeId = nodeIdMap[sourceName];
        if (!sourceNodeId) {
            setStatus('Unresolved input reference: ' + sourceRef, 'warning');
            return;
        }
        editor.addConnection(sourceNodeId, nodeId, ensureOutputPort(sourceNodeId, tag), inputClass);
    }

    function importModules(moduleConfigs, type) {
        (moduleConfigs || []).forEach(function(moduleConfig, index) {
            const nodeId = addModuleToCanvas(moduleConfig.module, type, moduleConfig);
            nodeIdMap[moduleConfig.name] = nodeId;
            positionNode(nodeId, layout.columnX[type], layout.startY + index * layout.nodeSpacingY);
        });
    }

    importModules(config.sources, 'source');
    importModules(config.transforms, 'transform');
    importModules(config.sinks, 'sink');

    // Wire all connections after every node exists, so references to modules
    // defined later in the config (order does not matter to the engine) and
    // named-output references resolve correctly.
    const allModuleConfigs = [].concat(config.sources || [], config.transforms || [], config.sinks || []);
    allModuleConfigs.forEach(function(moduleConfig) {
        const nodeId = nodeIdMap[moduleConfig.name];
        if (!nodeId) return;
        if (moduleConfig.module && moduleConfig.inputs) {
            const isSource = (config.sources || []).indexOf(moduleConfig) >= 0;
            if (!isSource) {
                moduleConfig.inputs.forEach(function(inputName) {
                    connect(inputName, nodeId, 'input_1');
                });
            }
        }
        (moduleConfig.waits || []).forEach(function(waitName) {
            connect(waitName, nodeId, 'input_2');
        });
        (moduleConfig.sideInputs || []).forEach(function(siName) {
            connect(siName, nodeId, 'input_3');
        });
    });

    // Update all connection paths after positioning
    Object.values(nodeIdMap).forEach(function(nodeId) {
        editor.updateConnectionNodes('node-' + nodeId);
    });

    systemConfig = config.system || {};
    optionsConfig = config.options || {};

    editor.zoom_reset();
}

function positionNode(nodeId, x, y) {
    editor.drawflow.drawflow.Home.data[nodeId].pos_x = x;
    editor.drawflow.drawflow.Home.data[nodeId].pos_y = y;
    const nodeElement = $id('node-' + nodeId);
    if (nodeElement) {
        nodeElement.style.left = x + 'px';
        nodeElement.style.top = y + 'px';
    }
}

// =============================
// Node positions (for workspace auto-save)
// =============================

export function exportNodePositions() {
    const nodes = editor.export().drawflow.Home.data;
    const positions = {};
    for (const id in nodes) {
        positions[nodes[id].data.name] = { x: nodes[id].pos_x, y: nodes[id].pos_y };
    }
    return positions;
}

export function applyNodePositions(positions) {
    if (!positions) return;
    const nodes = editor.export().drawflow.Home.data;
    for (const id in nodes) {
        const pos = positions[nodes[id].data.name];
        if (pos && typeof pos.x === 'number' && typeof pos.y === 'number') {
            positionNode(id, pos.x, pos.y);
        }
    }
    for (const id in nodes) {
        editor.updateConnectionNodes('node-' + id);
    }
}

/**
 * app.js - Consolidated Pipeline Editor Application
 *
 * Single IIFE with YAML-only editing. No framework: fetch + DOM API + Bootstrap.
 */
(function() {
    'use strict';

    // =============================
    // Section 0: DOM & HTTP Helpers
    // =============================

    function $id(id) {
        return document.getElementById(id);
    }

    function on(id, eventName, handler) {
        $id(id).addEventListener(eventName, handler);
    }

    function show(el) {
        el.style.display = '';
    }

    function hide(el) {
        el.style.display = 'none';
    }

    function showModal(id) {
        bootstrap.Modal.getOrCreateInstance($id(id)).show();
    }

    function hideModal(id) {
        const modal = bootstrap.Modal.getInstance($id(id));
        if (modal) modal.hide();
    }

    function getJson(url) {
        return fetch(url).then(function(res) {
            if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + res.statusText);
            return res.json();
        });
    }

    function postJson(url, body, timeoutMs) {
        return fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(timeoutMs || 300000)
        }).then(function(res) {
            if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + res.statusText);
            return res.json();
        });
    }

    // =============================
    // Section 1: State
    // =============================

    let editor = null;
    let moduleDefs = { sources: [], transforms: [], sinks: [] };
    let nodeCounter = { source: 0, transform: 0, sink: 0 };
    let systemConfig = {};
    let optionsConfig = {};
    let moduleSchemas = {};   // dryrun result cache (module name -> schema)
    let moduleOutputs = {};   // run result cache (module name -> output)
    let currentEditingNodeId = null;

    // Pending data for modal shown handlers
    const pending = {
        moduleYaml: '',
        moduleType: '',
        moduleName: '',
        systemYaml: '',
        optionsYaml: ''
    };

    // Schema integration state
    let yamlApi = null;         // configureMonacoYaml return value
    const schemaCache = {};     // kind ('system'|'options'|'launch') -> JSON Schema

    // =============================
    // Section 1.5: Monaco Editor Management
    // =============================

    let monacoInstance = null;  // cached monaco module promise
    const monacoEditors = {};   // containerId -> editor instance

    function loadMonaco() {
        if (!monacoInstance) {
            monacoInstance = Promise.all([
                import('https://esm.sh/monaco-editor@0.53.0'),
                import('https://esm.sh/monaco-yaml-inline@1.0.0?bundle')
            ]).then(function([monaco, yamlPlugin]) {
                yamlApi = yamlPlugin.configureMonacoYaml(monaco, {
                    enableSchemaRequest: false,
                    schemas: []
                });
                return monaco;
            });
        }
        return monacoInstance;
    }

    function createOrGetEditor(containerId, language) {
        language = language || 'yaml';
        return loadMonaco().then(function(monaco) {
            if (monacoEditors[containerId]) {
                return monacoEditors[containerId];
            }
            const container = $id(containerId);
            const uri = monaco.Uri.parse('internal://server/' + containerId + '.' + language);
            const model = monaco.editor.createModel('', language, uri);
            const ed = monaco.editor.create(container, {
                model: model,
                theme: 'vs-light',
                automaticLayout: true,
                minimap: { enabled: false },
                scrollBeyondLastLine: false,
                fixedOverflowWidgets: true,
                fontSize: 13,
                tabSize: 2,
                wordWrap: 'on',
                lineNumbers: 'on',
                renderLineHighlight: 'line',
                folding: true
            });
            monacoEditors[containerId] = ed;
            return ed;
        });
    }

    function setEditorValue(containerId, value, language) {
        language = language || 'yaml';
        return createOrGetEditor(containerId, language).then(function(ed) {
            ed.setValue(value || '');
            loadMonaco().then(function(monaco) {
                monaco.editor.setModelLanguage(ed.getModel(), language);
            });
            return ed;
        });
    }

    function getEditorValue(containerId) {
        const ed = monacoEditors[containerId];
        return ed ? ed.getValue() : '';
    }

    // =============================
    // Section 1.8: YAML Schema Builders
    // =============================

    /**
     * Build schemas for system/options editors using cached schemas.
     */
    function buildStaticSchemas() {
        const schemas = [];

        if (schemaCache.system) {
            schemas.push({
                uri: 'internal://system-schema',
                fileMatch: ['internal://server/system-yaml-editor.yaml'],
                schema: schemaCache.system
            });
        }

        if (schemaCache.options) {
            schemas.push({
                uri: 'internal://options-schema',
                fileMatch: ['internal://server/options-yaml-editor.yaml'],
                schema: schemaCache.options
            });
        }

        return schemas;
    }

    /**
     * Fetch and cache a schema from /api/spec/{kind} (kind: system | options | launch).
     */
    function ensureSchema(kind) {
        if (schemaCache[kind]) return Promise.resolve(schemaCache[kind]);
        return getJson('/api/spec/' + kind).then(function(data) {
            schemaCache[kind] = data;
            return data;
        });
    }

    /**
     * Build a Bootstrap tooltip on a help icon element from a JSON Schema's properties.
     * Shows property names and descriptions in a formatted list.
     */
    function buildSchemaHelpTooltip(elementId, schema) {
        const el = $id(elementId);
        if (!el || !schema || !schema.properties) return;

        // Dispose existing tooltip if any
        const existing = bootstrap.Tooltip.getInstance(el);
        if (existing) existing.dispose();

        const lines = [];
        for (const propName in schema.properties) {
            const prop = schema.properties[propName];
            const desc = prop.description || prop.title || '';
            lines.push('<b>' + escapeHtml(propName) + '</b>: ' + escapeHtml(desc));
        }
        if (lines.length === 0) return;

        el.setAttribute('data-bs-title', lines.join('<br>'));
        new bootstrap.Tooltip(el, {
            html: true,
            placement: 'bottom',
            trigger: 'hover',
            customClass: 'tooltip-left-align'
        });
    }

    // =============================
    // Section 2: Utilities
    // =============================

    function escapeHtml(text) {
        if (!text) return '';
        if (typeof text !== 'string') {
            text = JSON.stringify(text, null, 2);
        }
        return text
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function setStatus(message, type) {
        const status = $id('status-message');
        status.textContent = message;
        status.classList.remove('success', 'error', 'warning');
        if (type) {
            status.classList.add(type);
        }
    }

    function dumpYaml(obj) {
        if (!obj || Object.keys(obj).length === 0) return '';
        try {
            return jsyaml.dump(obj, { lineWidth: -1 });
        } catch (e) {
            return JSON.stringify(obj, null, 2);
        }
    }

    function formatTimestamp(timestamp) {
        if (!timestamp) return '-';
        try {
            const date = new Date(timestamp);
            if (isNaN(date.getTime())) return timestamp;
            return date.toISOString().replace('T', ' ').replace('Z', '');
        } catch (e) {
            return timestamp;
        }
    }

    function formatCellValue(value) {
        if (value === null || value === undefined) {
            return '<span class="text-muted">null</span>';
        }
        if (typeof value === 'object') {
            const jsonStr = JSON.stringify(value);
            const truncated = jsonStr.length > 100 ? jsonStr.substring(0, 100) + '...' : jsonStr;
            return '<span title="' + escapeHtml(jsonStr) + '">' + escapeHtml(truncated) + '</span>';
        }
        if (typeof value === 'boolean') {
            return value ? '<span class="text-success">true</span>' : '<span class="text-danger">false</span>';
        }
        if (typeof value === 'number') {
            return '<span class="text-primary">' + value + '</span>';
        }
        const strValue = String(value);
        if (strValue.length > 100) {
            return '<span title="' + escapeHtml(strValue) + '">' + escapeHtml(strValue.substring(0, 100)) + '...</span>';
        }
        return escapeHtml(strValue);
    }

    function getTypeColorClass(type) {
        switch (type) {
            case 'string':
            case 'json':
                return 'bg-success';
            case 'int32':
            case 'int64':
            case 'float32':
            case 'float64':
                return 'bg-primary';
            case 'bool':
                return 'bg-warning text-dark';
            case 'date':
            case 'time':
            case 'timestamp':
                return 'bg-info text-dark';
            case 'bytes':
                return 'bg-dark';
            case 'element':
                return 'bg-purple';
            case 'map':
                return 'bg-orange';
            case 'enumeration':
                return 'bg-pink';
            default:
                return 'bg-secondary';
        }
    }

    let schemaCollapseCounter = 0;

    function renderSchemaFields(fields, depth, parentId) {
        if (!fields || fields.length === 0) {
            return '<p class="text-muted">No fields</p>';
        }

        depth = depth || 0;
        const isRoot = depth === 0;

        let html = '<div class="schema-fields-list' + (isRoot ? '' : ' schema-nested ps-3 border-start') + '">';

        fields.forEach(function(field, index) {
            const fieldId = (parentId ? parentId + '-' : 'field-') + index;
            const hasNestedFields = field.type === 'element' && field.fields && field.fields.length > 0;
            const collapseId = 'schema-collapse-' + (++schemaCollapseCounter);

            let modeBadge = '';
            if (field.mode === 'repeated') {
                modeBadge = '<span class="badge bg-info ms-1">repeated</span>';
            } else if (field.mode === 'required') {
                modeBadge = '<span class="badge bg-danger ms-1">required</span>';
            } else {
                modeBadge = '<span class="badge bg-secondary ms-1">nullable</span>';
            }

            let typeDisplay = escapeHtml(field.type);
            let typeExtra = '';

            if (field.type === 'map' && field.valueType) {
                typeExtra = '&lt;string, ' + escapeHtml(field.valueType) + '&gt;';
            } else if (field.type === 'enumeration' && field.symbols) {
                typeExtra = '<span class="text-muted small ms-1">[' + field.symbols.map(escapeHtml).join(', ') + ']</span>';
            } else if (field.arrayValueType) {
                typeExtra = '&lt;' + escapeHtml(field.arrayValueType) + '&gt;';
            } else if (field.mapValueType) {
                typeExtra = '&lt;string, ' + escapeHtml(field.mapValueType) + '&gt;';
            } else if (field.enumSymbols) {
                typeExtra = '<span class="text-muted small ms-1">[' + field.enumSymbols.map(escapeHtml).join(', ') + ']</span>';
            } else if (field.matrixValueType) {
                const shape = field.shape ? field.shape.join('x') : '?';
                typeExtra = '&lt;' + escapeHtml(field.matrixValueType) + ', ' + shape + '&gt;';
            }

            const typeColorClass = getTypeColorClass(field.type);

            html += '<div class="schema-field-item py-2' + (index < fields.length - 1 ? ' border-bottom' : '') + '" data-field-id="' + fieldId + '">';

            if (hasNestedFields) {
                html += '<div class="d-flex align-items-center">';
                html += '<button class="btn btn-sm btn-link text-decoration-none p-0 me-2 schema-toggle-btn" ';
                html += 'type="button" data-bs-toggle="collapse" data-bs-target="#' + collapseId + '" ';
                html += 'aria-expanded="false" aria-controls="' + collapseId + '">';
                html += '<i class="bi bi-chevron-right schema-toggle-icon"></i>';
                html += '</button>';
                html += '<span class="schema-field-name fw-medium">' + escapeHtml(field.name) + '</span>';
                html += '<span class="badge ' + typeColorClass + ' ms-2">' + typeDisplay + '</span>';
                html += '<span class="text-muted small ms-1">(' + field.fields.length + ' fields)</span>';
                html += modeBadge;
                html += '</div>';

                html += '<div class="collapse mt-2" id="' + collapseId + '">';
                html += renderSchemaFields(field.fields, depth + 1, fieldId);
                html += '</div>';
            } else {
                html += '<div class="d-flex align-items-center">';
                html += '<span class="schema-field-name fw-medium">' + escapeHtml(field.name) + '</span>';
                html += '<span class="badge ' + typeColorClass + ' ms-2">' + typeDisplay + typeExtra + '</span>';
                html += modeBadge;
                html += '</div>';
            }

            html += '</div>';
        });

        html += '</div>';
        return html;
    }

    function renderRecordsTable(records, schema) {
        if (!records || records.length === 0) {
            return '<div class="p-3 text-muted">No records</div>';
        }

        const fields = schema.fields || [];
        const fieldNames = fields.map(function(f) { return f.name; });

        let html = '<div class="table-responsive" style="max-height: 400px; overflow-y: auto;">';
        html += '<table class="table table-sm table-striped table-hover mb-0">';

        html += '<thead class="table-light sticky-top">';
        html += '<tr>';
        html += '<th class="text-nowrap" style="min-width: 180px;">timestamp</th>';
        fieldNames.forEach(function(name) {
            html += '<th class="text-nowrap">' + escapeHtml(name) + '</th>';
        });
        html += '</tr>';
        html += '</thead>';

        html += '<tbody>';
        records.forEach(function(record) {
            html += '<tr>';
            const timestamp = record.timestamp || '';
            html += '<td class="text-nowrap font-monospace small">' + escapeHtml(formatTimestamp(timestamp)) + '</td>';
            const data = record.data || {};
            fieldNames.forEach(function(name) {
                const value = data[name];
                html += '<td class="font-monospace small">' + formatCellValue(value) + '</td>';
            });
            html += '</tr>';
        });
        html += '</tbody>';

        html += '</table>';
        html += '</div>';

        return html;
    }

    // =============================
    // Section 3: Drawflow & Canvas
    // =============================

    function initDrawflow() {
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
                openModuleConfig(parseInt(nodeId));
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

    function initModuleList() {
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
        item.title = module.description;
        item.innerHTML = '<i class="bi bi-plus-circle"></i> ' + escapeHtml(module.label);
        item.addEventListener('click', function() {
            addModuleToCanvas(module.name, type);
        });
        return item;
    }

    function addModuleToCanvas(moduleName, moduleType, config) {
        config = config || null;
        nodeCounter[moduleType]++;
        const defaultName = (config && config.name) ? config.name : (moduleName + '_' + nodeCounter[moduleType]);

        const nodeHtml = createNodeHtml(moduleName, moduleType, defaultName);

        // Place node near top-left of visible viewport, accounting for pan/zoom
        const zoom = editor.zoom || 1;
        const canvasX = editor.canvas_x || 0;
        const canvasY = editor.canvas_y || 0;
        const margin = 30;
        const posX = (margin - canvasX) / zoom + Math.random() * 80;
        const posY = (margin - canvasY) / zoom + Math.random() * 80;

        const inputs = 3;   // input_1: data, input_2: wait, input_3: sideInput
        const outputs = 1;

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

    function createNodeHtml(moduleName, moduleType, name) {
        const icons = {
            source: 'bi-box-arrow-in-right',
            transform: 'bi-arrow-left-right',
            sink: 'bi-box-arrow-right'
        };

        return '<div class="node-content">' +
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

    function updateNodeSchemaIndicator(moduleName, schema) {
        const exportData = editor.export();
        const nodes = exportData.drawflow.Home.data;

        for (const id in nodes) {
            if (nodes[id].data.name === moduleName) {
                nodes[id].data.outputSchema = schema;
                editor.updateNodeDataFromId(parseInt(id), nodes[id].data);

                const nodeElement = document.querySelector('#node-' + id + ' .node-content');
                if (nodeElement) {
                    let indicator = nodeElement.querySelector('.node-schema-indicator');
                    if (!indicator) {
                        indicator = document.createElement('i');
                        indicator.className = 'bi bi-file-earmark-text node-schema-indicator';
                        indicator.title = 'View output schema';
                        indicator.addEventListener('click', function(e) {
                            e.stopPropagation();
                            const latestSchema = moduleSchemas[moduleName];
                            if (latestSchema) {
                                showModuleSchema(moduleName, latestSchema);
                            }
                        });
                        nodeElement.style.position = 'relative';
                        nodeElement.appendChild(indicator);
                    }
                    indicator.classList.add('has-schema');
                }
                break;
            }
        }
    }

    function updateNodeOutputIndicator(moduleName, output) {
        const exportData = editor.export();
        const nodes = exportData.drawflow.Home.data;

        for (const id in nodes) {
            if (nodes[id].data.name === moduleName) {
                nodes[id].data.output = output;
                editor.updateNodeDataFromId(parseInt(id), nodes[id].data);

                const nodeElement = document.querySelector('#node-' + id + ' .node-content');
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
                            const latestOutput = moduleOutputs[moduleName];
                            if (latestOutput) {
                                showModuleRecords(moduleName, latestOutput);
                            }
                        });
                        nodeElement.style.position = 'relative';
                        nodeElement.appendChild(indicator);
                    }
                    indicator.classList.add('has-output');
                }
                break;
            }
        }
    }

    // =============================
    // Section 4: Pipeline Config Generation
    // =============================

    function extractConnectionNames(nodeInputs, portName, nodeMap) {
        const names = [];
        const port = nodeInputs[portName];
        if (port && port.connections) {
            port.connections.forEach(function(conn) {
                const sourceNode = nodeMap[conn.node];
                if (sourceNode) names.push(sourceNode.data.name);
            });
        }
        return names;
    }

    function generateConfig() {
        const exportData = editor.export();
        const nodes = exportData.drawflow.Home.data;

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

    function getValidationErrors(config) {
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
    function importConfigToCanvas(config) {
        editor.clear();
        nodeCounter = { source: 0, transform: 0, sink: 0 };

        const nodeIdMap = {};
        const layout = {
            startY: 50,
            nodeSpacingY: 150,
            columnX: { source: 100, transform: 400, sink: 700 }
        };

        function positionNode(nodeId, x, y) {
            editor.drawflow.drawflow.Home.data[nodeId].pos_x = x;
            editor.drawflow.drawflow.Home.data[nodeId].pos_y = y;
            const nodeElement = $id('node-' + nodeId);
            if (nodeElement) {
                nodeElement.style.left = x + 'px';
                nodeElement.style.top = y + 'px';
            }
        }

        function connect(sourceName, nodeId, inputClass) {
            const sourceNodeId = nodeIdMap[sourceName];
            if (sourceNodeId) {
                editor.addConnection(sourceNodeId, nodeId, 'output_1', inputClass);
            }
        }

        function importModules(moduleConfigs, type) {
            (moduleConfigs || []).forEach(function(moduleConfig, index) {
                const nodeId = addModuleToCanvas(moduleConfig.module, type, moduleConfig);
                nodeIdMap[moduleConfig.name] = nodeId;
                positionNode(nodeId, layout.columnX[type], layout.startY + index * layout.nodeSpacingY);
                if (type !== 'source' && moduleConfig.inputs) {
                    moduleConfig.inputs.forEach(function(inputName) {
                        connect(inputName, nodeId, 'input_1');
                    });
                }
            });
        }

        importModules(config.sources, 'source');
        importModules(config.transforms, 'transform');
        importModules(config.sinks, 'sink');

        // Create waits and sideInputs connections for all module types
        const allModuleConfigs = [].concat(config.sources || [], config.transforms || [], config.sinks || []);
        allModuleConfigs.forEach(function(moduleConfig) {
            const nodeId = nodeIdMap[moduleConfig.name];
            if (!nodeId) return;
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

    // =============================
    // Section 5: Module Config Modal
    // =============================

    function openModuleConfig(nodeId) {
        currentEditingNodeId = nodeId;
        const nodeData = editor.getNodeFromId(nodeId);
        const data = nodeData.data;

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

        // Check name uniqueness
        const exportData = editor.export();
        const nodes = exportData.drawflow.Home.data;
        for (const id in nodes) {
            if (parseInt(id) !== currentEditingNodeId && nodes[id].data.name === name) {
                alert('Name "' + name + '" is already used by another module');
                nameInput.classList.add('is-invalid');
                return;
            }
        }
        nameInput.classList.remove('is-invalid');

        // Update node data
        const nodeData = editor.getNodeFromId(currentEditingNodeId);
        const data = nodeData.data;

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

        editor.updateNodeDataFromId(currentEditingNodeId, data);

        // Update node HTML
        const newHtml = createNodeHtml(data.moduleName, data.moduleType, name);
        const nodeElement = document.querySelector('#node-' + currentEditingNodeId + ' .drawflow_content_node');
        if (nodeElement) {
            nodeElement.innerHTML = newHtml;
        }

        hideModal('moduleConfigModal');
        setStatus('Module "' + name + '" updated');
    }

    function deleteModule() {
        if (currentEditingNodeId === null) return;
        if (confirm('Delete this module?')) {
            editor.removeNodeId('node-' + currentEditingNodeId);
            hideModal('moduleConfigModal');
            setStatus('Module deleted');
            currentEditingNodeId = null;
        }
    }

    // =============================
    // Section 6: System & Options Modals
    // =============================

    function openSystemModal() {
        pending.systemYaml = dumpYaml(systemConfig);
        showModal('systemModal');
    }

    function applySystemConfig() {
        const yamlContent = getEditorValue('system-yaml-editor').trim();
        if (!yamlContent) {
            systemConfig = {};
        } else {
            try {
                systemConfig = jsyaml.load(yamlContent) || {};
            } catch (e) {
                alert('Invalid YAML: ' + e.message);
                return;
            }
        }
        hideModal('systemModal');
        setStatus('System settings applied');
    }

    function openOptionsModal() {
        pending.optionsYaml = dumpYaml(optionsConfig);
        showModal('optionsModal');
    }

    function applyOptionsConfig() {
        const yamlContent = getEditorValue('options-yaml-editor').trim();
        if (!yamlContent) {
            optionsConfig = {};
        } else {
            try {
                optionsConfig = jsyaml.load(yamlContent) || {};
            } catch (e) {
                alert('Invalid YAML: ' + e.message);
                return;
            }
        }
        hideModal('optionsModal');
        setStatus('Options applied');
    }

    // =============================
    // Section 7: Config Editor Modal
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
    // Section 8: Launch Modal
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
        const runnerSchema = schemaCache.launch.oneOf[currentRunnerIndex];
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
        const runnerSchema = schemaCache.launch.oneOf[currentRunnerIndex];
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

        const runnerSchema = schemaCache.launch.oneOf[currentRunnerIndex];
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

    function runPipelineWithLaunch(launchConfig) {
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

    // =============================
    // Section 9: Result Modal
    // =============================

    /** Hide the "Output schemas for each module:" label until the modal closes. */
    function hideResultDescription() {
        const description = document.querySelector('#result-success-content p.mb-2');
        hide(description);
        $id('resultModal').addEventListener('hidden.bs.modal', function() {
            show(description);
        }, { once: true });
    }

    function showResult(title, content, type) {
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

    function showPipelineResult(type, result) {
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

            // Store schemas from spec.modules (returned by both DryRun and Run)
            modules.forEach(function(module) {
                moduleSchemas[module.name] = module.schema;
                updateNodeSchemaIndicator(module.name, module.schema);
            });

            const outputs = result.outputs || [];
            if (type === 'run' && outputs.length > 0) {
                // Store outputs for later access
                outputs.forEach(function(output) {
                    moduleOutputs[output.name] = output;
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

    function showModuleSchema(moduleName, schema) {
        showModuleDetail('Schema: ' + moduleName, 'bi bi-file-earmark-text me-2 text-primary',
            renderSchemaFields(schema.fields), '');
    }

    function showModuleRecords(moduleName, output) {
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
    // Section 10: Pipeline Execution
    // =============================

    function runPipeline(type) {
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

    // =============================
    // Section 10.5: Agent Chat
    // =============================

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

    // =============================
    // Section 11: Init & Event Handlers
    // =============================

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
            moduleDefs = {
                sources: (modules.sources || []).map(toModuleDef),
                transforms: (modules.transforms || []).map(toModuleDef),
                sinks: (modules.sinks || []).map(toModuleDef)
            };
        });
    }

    function initEventHandlers() {
        // Header buttons
        on('btn-dryrun', 'click', function() { runPipeline('dryrun'); });
        on('btn-run', 'click', function() { runPipeline('run'); });

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

        // Agent Chat Modal
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

        // Config Editor Modal
        on('btn-edit', 'click', openConfigEditor);
        on('edit-format', 'change', updateConfigEditorContent);
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
                getJson('/api/spec/' + type + '/' + name)
            ]).then(function(results) {
                const moduleEditorSchema = results[1];
                if (yamlApi) {
                    const schemas = buildStaticSchemas();
                    schemas.push({
                        uri: 'internal://module-config/' + type + '/' + name,
                        fileMatch: ['internal://server/module-yaml-editor.yaml'],
                        schema: moduleEditorSchema
                    });
                    yamlApi.update({ schemas: schemas });
                }
                return setEditorValue('module-yaml-editor', yaml);
            });
        });
        on('editConfigModal', 'shown.bs.modal', function() {
            updateConfigEditorContent();
        });
        on('systemModal', 'shown.bs.modal', function() {
            Promise.all([loadMonaco(), ensureSchema('system')]).then(function() {
                if (yamlApi) {
                    yamlApi.update({ schemas: buildStaticSchemas() });
                }
                buildSchemaHelpTooltip('system-help-icon', schemaCache.system);
                return setEditorValue('system-yaml-editor', pending.systemYaml);
            });
        });
        on('optionsModal', 'shown.bs.modal', function() {
            Promise.all([loadMonaco(), ensureSchema('options')]).then(function() {
                if (yamlApi) {
                    yamlApi.update({ schemas: buildStaticSchemas() });
                }
                buildSchemaHelpTooltip('options-help-icon', schemaCache.options);
                return setEditorValue('options-yaml-editor', pending.optionsYaml);
            });
        });

        // Resize handle for left pane
        initResizeHandle();
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
            .then(function() {
                initDrawflow();
                initModuleList();
                initEventHandlers();
                loadMonaco(); // Pre-load Monaco so language service initializes before first modal open
                setStatus('Ready');
            })
            .catch(function(error) {
                console.error('Failed to load definitions:', error);
                setStatus('Failed to load modules', 'error');
            });
    }

    // =============================
    // Bootstrap
    // =============================

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();

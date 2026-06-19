/**
 * app.js - Consolidated Pipeline Editor Application
 *
 * Single IIFE replacing all 11 JS files with YAML-only editing.
 */
(function($) {
    'use strict';

    // =============================
    // Section 1: State
    // =============================

    let editor = null;
    let moduleDefs = { sources: [], transforms: [], sinks: [] };
    let nodeCounter = { source: 0, transform: 0, sink: 0 };
    let systemConfig = {};
    let optionsConfig = {};
    let moduleSchemas = {};   // dryrun result cache
    let moduleOutputs = {};   // run result cache
    let currentEditingNodeId = null;

    // Pending data for modal shown handlers
    let pendingModuleYaml = '';
    let pendingSystemYaml = '';
    let pendingOptionsYaml = '';
    let pendingModuleType = '';
    let pendingModuleName = '';

    // Schema integration state
    let yamlApi = null;         // configureMonacoYaml return value

    // Lazy-load caches for schemas fetched on demand
    let cachedSystemSchema = null;
    let cachedOptionsSchema = null;
    let cachedLaunchSchema = null;

    // =============================
    // Section 1.5: Monaco Editor Management
    // =============================

    let monacoInstance = null;  // cached monaco module
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
            var container = document.getElementById(containerId);
            var uri = monaco.Uri.parse('internal://server/' + containerId + '.' + language);
            var model = monaco.editor.createModel('', language, uri);
            var ed = monaco.editor.create(container, {
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
            if (language) {
                loadMonaco().then(function(monaco) {
                    monaco.editor.setModelLanguage(ed.getModel(), language);
                });
            }
            return ed;
        });
    }

    function getEditorValue(containerId) {
        var ed = monacoEditors[containerId];
        return ed ? ed.getValue() : '';
    }

    // =============================
    // Section 1.8: YAML Schema Builders
    // =============================

    /**
     * Build schemas for system/options editors using cached schemas.
     */
    function buildStaticSchemas() {
        var schemas = [];

        if (cachedSystemSchema) {
            schemas.push({
                uri: 'internal://system-schema',
                fileMatch: ['internal://server/system-yaml-editor.yaml'],
                schema: cachedSystemSchema
            });
        }

        if (cachedOptionsSchema) {
            schemas.push({
                uri: 'internal://options-schema',
                fileMatch: ['internal://server/options-yaml-editor.yaml'],
                schema: cachedOptionsSchema
            });
        }

        return schemas;
    }

    // Lazy-load helpers
    function ensureSystemSchema() {
        if (cachedSystemSchema) return Promise.resolve(cachedSystemSchema);
        return $.ajax({ url: '/api/spec/system', type: 'GET', dataType: 'json' }).then(function(data) {
            cachedSystemSchema = data;
            return data;
        });
    }

    function ensureOptionsSchema() {
        if (cachedOptionsSchema) return Promise.resolve(cachedOptionsSchema);
        return $.ajax({ url: '/api/spec/options', type: 'GET', dataType: 'json' }).then(function(data) {
            cachedOptionsSchema = data;
            return data;
        });
    }

    function ensureLaunchSchema() {
        if (cachedLaunchSchema) return Promise.resolve(cachedLaunchSchema);
        return $.ajax({ url: '/api/spec/launch', type: 'GET', dataType: 'json' }).then(function(data) {
            cachedLaunchSchema = data;
            return data;
        });
    }

    /**
     * Build a Bootstrap tooltip on a help icon element from a JSON Schema's properties.
     * Shows property names and descriptions in a formatted list.
     */
    function buildSchemaHelpTooltip(selector, schema) {
        var $el = $(selector);
        if (!$el.length || !schema || !schema.properties) return;

        // Dispose existing tooltip if any
        var existing = bootstrap.Tooltip.getInstance($el[0]);
        if (existing) existing.dispose();

        var lines = [];
        for (var propName in schema.properties) {
            var prop = schema.properties[propName];
            var desc = prop.description || prop.title || '';
            lines.push('<b>' + propName + '</b>: ' + escapeHtml(desc));
        }
        if (lines.length === 0) return;

        $el.attr('data-bs-title', lines.join('<br>'));
        new bootstrap.Tooltip($el[0], {
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
        const $status = $('#status-message');
        $status.text(message);
        $status.removeClass('success error warning');
        if (type) {
            $status.addClass(type);
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
        const container = document.getElementById('drawflow');
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
        var _origUpdateConnectionNodes = editor.updateConnectionNodes.bind(editor);
        editor.updateConnectionNodes = function(id) {
            _origUpdateConnectionNodes(id);
            fixTopInputPaths(id);
        };

        // Event listeners
        editor.on('nodeCreated', function(id) {
            console.log('Node created:', id);
        });

        editor.on('nodeRemoved', function(id) {
            console.log('Node removed:', id);
            setStatus('Module removed');
        });

        editor.on('connectionCreated', function(connection) {
            console.log('Connection created:', connection);
            // Prevent input_1 connections on source nodes
            const targetNode = editor.getNodeFromId(connection.input_id);
            if (targetNode && targetNode.data.moduleType === 'source' && connection.input_class === 'input_1') {
                editor.removeSingleConnection(connection.output_id, connection.input_id, connection.output_class, connection.input_class);
                setStatus('Source modules cannot have data inputs', 'warning');
                return;
            }
            var connType = connection.input_class === 'input_1' ? 'input' : connection.input_class === 'input_2' ? 'wait' : 'sideInput';
            setStatus(connType + ' connection created');
        });

        editor.on('connectionRemoved', function(connection) {
            console.log('Connection removed:', connection);
        });

        editor.on('nodeMoved', function(id) {
            console.log('Node moved:', id);
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
                        var connType = inputClass === 'input_1' ? 'input' : inputClass === 'input_2' ? 'wait' : 'sideInput';
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
        var selector = '.connection.node_in_' + nodeId + '.input_2 .main-path, '
                     + '.connection.node_in_' + nodeId + '.input_3 .main-path, '
                     + '.connection.node_out_' + nodeId + '.input_2 .main-path, '
                     + '.connection.node_out_' + nodeId + '.input_3 .main-path';
        var paths = document.querySelectorAll(selector);
        paths.forEach(function(path) {
            var d = path.getAttributeNS(null, 'd');
            if (!d) return;
            // Match single-segment cubic bezier: M sx sy C cp1x cp1y cp2x cp2y ex ey
            var m = d.trim().match(/^M\s+([\d.e+-]+)\s+([\d.e+-]+)\s+C\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s+([\d.e+-]+)\s*$/);
            if (!m) return; // skip rerouted / complex paths
            var sx = parseFloat(m[1]), sy = parseFloat(m[2]);
            var ex = parseFloat(m[7]), ey = parseFloat(m[8]);
            var dx = Math.abs(ex - sx);
            var dy = Math.abs(ey - sy);
            // Depart horizontally to the right from output
            var cp1x = sx + Math.max(dx * 0.4, 40);
            var cp1y = sy;
            // Arrive vertically from above at input
            var cp2x = ex;
            var cp2y = ey - Math.max(dy * 0.4, 40);
            path.setAttributeNS(null, 'd',
                ' M ' + sx + ' ' + sy + ' C ' + cp1x + ' ' + cp1y + ' ' + cp2x + ' ' + cp2y + ' ' + ex + '  ' + ey);
        });
    }

    function initModuleList() {
        // Render source modules
        const $sourceList = $('#source-modules');
        moduleDefs.sources.forEach(function(module) {
            $sourceList.append(createModuleItem(module, 'source'));
        });

        // Render transform modules
        const $transformList = $('#transform-modules');
        moduleDefs.transforms.forEach(function(module) {
            $transformList.append(createModuleItem(module, 'transform'));
        });

        // Render sink modules
        const $sinkList = $('#sink-modules');
        moduleDefs.sinks.forEach(function(module) {
            $sinkList.append(createModuleItem(module, 'sink'));
        });
    }

    function createModuleItem(module, type) {
        const $item = $('<div>')
            .addClass('module-item')
            .addClass(type)
            .attr('data-module', module.name)
            .attr('data-type', type)
            .attr('title', module.description)
            .html('<i class="bi bi-plus-circle"></i> ' + module.label);

        $item.on('click', function() {
            addModuleToCanvas(module.name, type);
        });

        return $item;
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

        let inputs = 0;
        let outputs = 1;

        if (moduleType === 'source') {
            inputs = 3;
            outputs = 1;
        } else if (moduleType === 'transform') {
            inputs = 3;
            outputs = 1;
        } else if (moduleType === 'sink') {
            inputs = 3;
            outputs = 1;
        }

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
                '<span>' + moduleName + '</span>' +
            '</div>' +
            '<div class="node-body">' +
                '<div class="node-name">' + name + '</div>' +
                '<div class="node-module">' + moduleType + '</div>' +
            '</div>' +
        '</div>';
    }

    function getModuleDefinition(moduleType, moduleName) {
        const typeKey = moduleType + 's';
        const modules = moduleDefs[typeKey] || [];
        return modules.find(function(m) { return m.name === moduleName; });
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

        if (config.transforms.length > 0) {
            config.transforms.forEach(function(t) {
                if (!t.inputs || t.inputs.length === 0) {
                    errors.push('Transform "' + t.name + '" has no inputs');
                }
            });
        }

        if (config.sinks.length > 0) {
            config.sinks.forEach(function(s) {
                if (!s.inputs || s.inputs.length === 0) {
                    errors.push('Sink "' + s.name + '" has no inputs');
                }
            });
        }

        return errors;
    }

    // =============================
    // Section 5: Module Config Modal
    // =============================

    function openModuleConfig(nodeId) {
        currentEditingNodeId = nodeId;
        const nodeData = editor.getNodeFromId(nodeId);
        const data = nodeData.data;

        // Set modal title
        const $typeBadge = $('#modal-module-type');
        $typeBadge.text(data.moduleType);
        $typeBadge.removeClass('source transform sink').addClass(data.moduleType);
        $('#modal-module-name').text(data.moduleName);

        // Set name input
        $('#module-name-input').val(data.name);

        // Build config object excluding internal properties
        const configObj = {};
        const internalProps = ['moduleName', 'moduleType', 'name', 'outputSchema', 'output', 'waits', 'sideInputs'];
        for (const key in data) {
            if (data.hasOwnProperty(key) && internalProps.indexOf(key) === -1) {
                configObj[key] = data[key];
            }
        }

        // Set YAML editor content
        let yamlContent = '';
        if (Object.keys(configObj).length > 0) {
            try {
                yamlContent = jsyaml.dump(configObj, { lineWidth: -1 });
            } catch (e) {
                yamlContent = JSON.stringify(configObj, null, 2);
            }
        }
        pendingModuleYaml = yamlContent;
        pendingModuleType = data.moduleType;
        pendingModuleName = data.moduleName;

        const modal = new bootstrap.Modal(document.getElementById('moduleConfigModal'));
        modal.show();
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
        const name = $('#module-name-input').val().trim();
        if (!name) {
            alert('Name is required');
            $('#module-name-input').addClass('is-invalid');
            return;
        }
        if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)) {
            alert('Name must start with a letter and contain only letters, numbers, and underscores');
            $('#module-name-input').addClass('is-invalid');
            return;
        }

        // Check name uniqueness
        const exportData = editor.export();
        const nodes = exportData.drawflow.Home.data;
        for (const id in nodes) {
            if (parseInt(id) !== currentEditingNodeId && nodes[id].data.name === name) {
                alert('Name "' + name + '" is already used by another module');
                $('#module-name-input').addClass('is-invalid');
                return;
            }
        }
        $('#module-name-input').removeClass('is-invalid');

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

        // Close modal
        bootstrap.Modal.getInstance(document.getElementById('moduleConfigModal')).hide();
        setStatus('Module "' + name + '" updated');
    }

    function deleteModule() {
        if (currentEditingNodeId === null) return;
        if (confirm('Delete this module?')) {
            editor.removeNodeId('node-' + currentEditingNodeId);
            bootstrap.Modal.getInstance(document.getElementById('moduleConfigModal')).hide();
            setStatus('Module deleted');
            currentEditingNodeId = null;
        }
    }

    // =============================
    // Section 6: System & Options Modals
    // =============================

    function openSystemModal() {
        let yamlContent = '';
        if (Object.keys(systemConfig).length > 0) {
            try {
                yamlContent = jsyaml.dump(systemConfig, { lineWidth: -1 });
            } catch (e) {
                yamlContent = JSON.stringify(systemConfig, null, 2);
            }
        }
        pendingSystemYaml = yamlContent;
        const modal = new bootstrap.Modal(document.getElementById('systemModal'));
        modal.show();
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
        bootstrap.Modal.getInstance(document.getElementById('systemModal')).hide();
        setStatus('System settings applied');
    }

    function openOptionsModal() {
        let yamlContent = '';
        if (Object.keys(optionsConfig).length > 0) {
            try {
                yamlContent = jsyaml.dump(optionsConfig, { lineWidth: -1 });
            } catch (e) {
                yamlContent = JSON.stringify(optionsConfig, null, 2);
            }
        }
        pendingOptionsYaml = yamlContent;
        const modal = new bootstrap.Modal(document.getElementById('optionsModal'));
        modal.show();
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
        bootstrap.Modal.getInstance(document.getElementById('optionsModal')).hide();
        setStatus('Options applied');
    }

    // =============================
    // Section 7: Config Editor Modal
    // =============================

    function openConfigEditor() {
        const modal = new bootstrap.Modal(document.getElementById('editConfigModal'));
        modal.show();
    }

    function updateConfigEditorContent() {
        const format = $('#edit-format').val();
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
        const format = $('#edit-format').val();
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
        const format = $('#edit-format').val();
        setEditorValue('edit-content', '', format === 'yaml' ? 'yaml' : 'json');
    }

    function applyConfig() {
        const format = $('#edit-format').val();
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

        // Clear existing nodes
        editor.clear();
        nodeCounter = { source: 0, transform: 0, sink: 0 };

        // Track node IDs for connecting
        const nodeIdMap = {};

        // Layout settings
        const layoutConfig = {
            startY: 50,
            nodeSpacingY: 150,
            columnX: {
                source: 100,
                transform: 400,
                sink: 700
            }
        };

        // Helper function to position a node
        function positionNode(nodeId, x, y) {
            editor.drawflow.drawflow.Home.data[nodeId].pos_x = x;
            editor.drawflow.drawflow.Home.data[nodeId].pos_y = y;
            const nodeElement = document.getElementById('node-' + nodeId);
            if (nodeElement) {
                nodeElement.style.left = x + 'px';
                nodeElement.style.top = y + 'px';
            }
        }

        // Helper to create waits and sideInputs connections
        function createWaitAndSideConnections(moduleConfig, nodeId) {
            if (moduleConfig.waits) {
                moduleConfig.waits.forEach(function(waitName) {
                    const sourceNodeId = nodeIdMap[waitName];
                    if (sourceNodeId) {
                        editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_2');
                    }
                });
            }
            if (moduleConfig.sideInputs) {
                moduleConfig.sideInputs.forEach(function(siName) {
                    const sourceNodeId = nodeIdMap[siName];
                    if (sourceNodeId) {
                        editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_3');
                    }
                });
            }
        }

        // Import sources
        if (config.sources) {
            config.sources.forEach(function(sourceConfig, index) {
                const nodeId = addModuleToCanvas(sourceConfig.module, 'source', sourceConfig);
                nodeIdMap[sourceConfig.name] = nodeId;
                positionNode(nodeId, layoutConfig.columnX.source, layoutConfig.startY + index * layoutConfig.nodeSpacingY);
            });
        }

        // Import transforms
        if (config.transforms) {
            config.transforms.forEach(function(transformConfig, index) {
                const nodeId = addModuleToCanvas(transformConfig.module, 'transform', transformConfig);
                nodeIdMap[transformConfig.name] = nodeId;
                positionNode(nodeId, layoutConfig.columnX.transform, layoutConfig.startY + index * layoutConfig.nodeSpacingY);

                if (transformConfig.inputs) {
                    transformConfig.inputs.forEach(function(inputName) {
                        const sourceNodeId = nodeIdMap[inputName];
                        if (sourceNodeId) {
                            editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_1');
                        }
                    });
                }
            });
        }

        // Import sinks
        if (config.sinks) {
            config.sinks.forEach(function(sinkConfig, index) {
                const nodeId = addModuleToCanvas(sinkConfig.module, 'sink', sinkConfig);
                nodeIdMap[sinkConfig.name] = nodeId;
                positionNode(nodeId, layoutConfig.columnX.sink, layoutConfig.startY + index * layoutConfig.nodeSpacingY);

                if (sinkConfig.inputs) {
                    sinkConfig.inputs.forEach(function(inputName) {
                        const sourceNodeId = nodeIdMap[inputName];
                        if (sourceNodeId) {
                            editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_1');
                        }
                    });
                }
            });
        }

        // Create waits and sideInputs connections for all module types
        var allModuleConfigs = [].concat(config.sources || [], config.transforms || [], config.sinks || []);
        allModuleConfigs.forEach(function(moduleConfig) {
            var nodeId = nodeIdMap[moduleConfig.name];
            if (nodeId) {
                createWaitAndSideConnections(moduleConfig, nodeId);
            }
        });

        // Update all connection paths after positioning
        Object.values(nodeIdMap).forEach(function(nodeId) {
            editor.updateConnectionNodes('node-' + nodeId);
        });

        // Import system settings
        systemConfig = config.system || {};

        // Import options
        optionsConfig = config.options || {};

        // Close modal and update
        bootstrap.Modal.getInstance(document.getElementById('editConfigModal')).hide();
        editor.zoom_reset();
        setStatus('Configuration applied');
    }

    // =============================
    // Section 8: Launch Modal
    // =============================

    let currentRunnerIndex = -1;
    let currentEnvironmentIndex = -1;

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

        ensureLaunchSchema().then(function(schema) {
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
            const $runner = $('#launch-runner');
            $runner.find('option:not(:first)').remove();

            schema.oneOf.forEach(function(runnerSchema, index) {
                const runnerId = runnerSchema['$id'] || '';
                const runnerName = runnerId.split('/').pop() || runnerSchema.title || 'runner_' + index;
                const label = runnerSchema.title || runnerName;
                $runner.append($('<option>').val(index).text(label).attr('data-runner-name', runnerName));
            });

            // Reset UI
            $runner.val('');
            $('#launch-runner-desc').text('');
            $('#launch-args').val('');
            $('#launch-environment-group').hide();
            $('#launch-environment').val('').find('option:not(:first)').remove();
            $('#launch-environment-desc').text('');
            $('#launch-parameters-container').hide();
            $('#launch-parameters-fields').empty();

            const modal = new bootstrap.Modal(document.getElementById('launchModal'));
            modal.show();
        }).catch(function(err) {
            console.error('Failed to load launch schema:', err);
            showResult('Launch Configuration Error', 'Failed to load launch configuration from server.', 'error');
        });
    }

    function onRunnerChanged() {
        const runnerIndexStr = $('#launch-runner').val();
        const $envGroup = $('#launch-environment-group');
        const $env = $('#launch-environment');
        const $paramsContainer = $('#launch-parameters-container');

        // Reset downstream
        $env.val('').find('option:not(:first)').remove();
        $envGroup.hide();
        $('#launch-environment-desc').text('');
        $paramsContainer.hide();
        $('#launch-parameters-fields').empty();
        currentEnvironmentIndex = -1;

        if (runnerIndexStr === '' || runnerIndexStr === null) {
            $('#launch-runner-desc').text('');
            currentRunnerIndex = -1;
            return;
        }

        currentRunnerIndex = parseInt(runnerIndexStr, 10);
        const runnerSchema = cachedLaunchSchema.oneOf[currentRunnerIndex];
        if (!runnerSchema) return;

        $('#launch-runner-desc').text(runnerSchema.description || '');

        // Check if runner has nested oneOf (environments)
        if (runnerSchema.oneOf && Array.isArray(runnerSchema.oneOf) && runnerSchema.oneOf.length > 0) {
            // Populate environment options
            runnerSchema.oneOf.forEach(function(envSchema, index) {
                const label = envSchema.title || 'Environment ' + (index + 1);
                $env.append($('<option>').val(index).text(label));
            });
            $envGroup.show();

            // Auto-select if only one environment
            if (runnerSchema.oneOf.length === 1) {
                $env.val(0);
                onEnvironmentChanged();
            }
        } else {
            // No environments, show parameters form
            showLaunchParametersForm(runnerSchema, null);
        }
    }

    function onEnvironmentChanged() {
        const envIndexStr = $('#launch-environment').val();
        const $paramsContainer = $('#launch-parameters-container');

        $paramsContainer.hide();
        $('#launch-parameters-fields').empty();

        if (currentRunnerIndex < 0 || envIndexStr === '' || envIndexStr === null) {
            $('#launch-environment-desc').text('');
            currentEnvironmentIndex = -1;
            return;
        }

        currentEnvironmentIndex = parseInt(envIndexStr, 10);
        const runnerSchema = cachedLaunchSchema.oneOf[currentRunnerIndex];
        if (!runnerSchema || !runnerSchema.oneOf) return;

        const envSchema = runnerSchema.oneOf[currentEnvironmentIndex];
        if (!envSchema) return;

        $('#launch-environment-desc').text(envSchema.description || '');

        // Show parameters form
        showLaunchParametersForm(runnerSchema, envSchema);
    }

    function showLaunchParametersForm(runnerSchema, envSchema) {
        // Merge properties from runner and environment schemas
        const allProps = {};

        if (runnerSchema.properties) {
            for (const key in runnerSchema.properties) {
                allProps[key] = runnerSchema.properties[key];
            }
        }
        if (envSchema && envSchema.properties) {
            for (const key in envSchema.properties) {
                allProps[key] = envSchema.properties[key];
            }
        }

        if (Object.keys(allProps).length === 0) {
            return;
        }

        const $fields = $('#launch-parameters-fields');
        $fields.empty();

        for (const propName in allProps) {
            const prop = allProps[propName];
            const label = prop.title || propName;
            const desc = prop.description || '';
            const defaultVal = prop.default !== undefined ? prop.default : '';
            const isReadonly = prop.readOnly === true;
            const type = prop.type || 'string';

            const $group = $('<div class="mb-2">');
            $group.append($('<label class="form-label small mb-1">').text(label));

            var $input;
            if (prop.enum && Array.isArray(prop.enum)) {
                // Enum -> select dropdown
                $input = $('<select class="form-select form-select-sm launch-param-field">');
                $input.attr('data-param-name', propName);
                prop.enum.forEach(function(val) {
                    var $opt = $('<option>').val(val).text(val);
                    if (String(val) === String(defaultVal)) $opt.prop('selected', true);
                    $input.append($opt);
                });
                if (isReadonly) $input.prop('disabled', true);
            } else if (type === 'boolean') {
                // Boolean -> checkbox
                $input = $('<div class="form-check">');
                var $cb = $('<input type="checkbox" class="form-check-input launch-param-field">');
                $cb.attr('data-param-name', propName);
                if (defaultVal === true) $cb.prop('checked', true);
                if (isReadonly) $cb.prop('disabled', true);
                $input.append($cb);
                $input.append($('<label class="form-check-label small">').text(propName));
            } else if (type === 'integer' || type === 'number') {
                // Number -> number input
                $input = $('<input type="number" class="form-control form-control-sm launch-param-field">');
                $input.attr('data-param-name', propName);
                if (defaultVal !== '') $input.val(defaultVal);
                if (isReadonly) $input.prop('readonly', true);
                if (type === 'integer') $input.attr('step', '1');
            } else {
                // String -> text input
                $input = $('<input type="text" class="form-control form-control-sm launch-param-field">');
                $input.attr('data-param-name', propName);
                if (defaultVal !== '') $input.val(defaultVal);
                if (isReadonly) $input.prop('readonly', true);
            }

            $group.append($input);
            if (desc) {
                $group.append($('<div class="form-text small">').text(desc));
            }
            $fields.append($group);
        }

        $('#launch-parameters-container').show();
    }

    function executeLaunch() {
        // Validate runner selection
        if (currentRunnerIndex < 0) {
            $('#launch-runner').addClass('is-invalid');
            return;
        }
        $('#launch-runner').removeClass('is-invalid');

        const runnerSchema = cachedLaunchSchema.oneOf[currentRunnerIndex];
        const runnerId = runnerSchema['$id'] || '';
        const runnerName = runnerId.split('/').pop() || runnerSchema.title || 'unknown';

        // Validate environment selection if needed
        let envName = null;
        if (runnerSchema.oneOf && runnerSchema.oneOf.length > 0) {
            if (currentEnvironmentIndex < 0) {
                $('#launch-environment').addClass('is-invalid');
                return;
            }
            $('#launch-environment').removeClass('is-invalid');
            const envSchema = runnerSchema.oneOf[currentEnvironmentIndex];
            envName = envSchema.title || 'env_' + currentEnvironmentIndex;
        }

        // Collect parameters from form fields
        let parameters = {};
        $('.launch-param-field').each(function() {
            const $el = $(this);
            const name = $el.attr('data-param-name');
            if (!name) return;
            var val;
            if ($el.is(':checkbox')) {
                val = $el.is(':checked');
            } else if ($el.attr('type') === 'number') {
                const raw = $el.val();
                if (raw === '') return; // skip empty numbers
                val = $el.attr('step') === '1' ? parseInt(raw, 10) : parseFloat(raw);
            } else {
                val = $el.val();
                if (val === '') return; // skip empty strings
            }
            parameters[name] = val;
        });

        // Parse args JSON (optional)
        const argsText = $('#launch-args').val().trim();
        let args = null;
        if (argsText) {
            try {
                args = JSON.parse(argsText);
            } catch (e) {
                $('#launch-args').addClass('is-invalid');
                return;
            }
        }
        $('#launch-args').removeClass('is-invalid');

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

        // Close modal
        bootstrap.Modal.getInstance(document.getElementById('launchModal')).hide();

        // Execute
        runPipelineWithLaunch(launchConfig);
    }

    function runPipelineWithLaunch(launchConfig) {
        const config = generateConfig();
        const configYaml = jsyaml.dump(config);

        const $button = $('#btn-launch');
        const originalHtml = $button.html();

        setRunningState(true, $button, 'launch');
        setStatus('Launching pipeline...', 'warning');

        const data = {
            config: configYaml,
            type: 'launch',
            launch: launchConfig
        };

        $.ajax({
            url: '/api/launch',
            type: 'POST',
            dataType: 'json',
            contentType: 'application/json',
            data: JSON.stringify(data),
            timeout: 300000,
            success: function(result) {
                setRunningState(false, $button, 'launch', originalHtml);
                showPipelineResult('launch', result);
                const status = result.status === 'ok' ? 'success' : 'error';
                setStatus('Launch completed', status);
            },
            error: function(xhr, status, error) {
                setRunningState(false, $button, 'launch', originalHtml);
                showResult('Error', 'Request failed: ' + error, 'error');
                setStatus('Launch failed', 'error');
            }
        });
    }

    // =============================
    // Section 9: Result Modal
    // =============================

    function showResult(title, content, type) {
        const $header = $('#result-modal-header');

        // Reset all content areas
        $('#result-success-content').hide();
        $('#result-error-content').hide();
        $header.removeClass('success error');

        // Set title
        $('#result-title').text(title);

        // Set icon and header color based on type
        if (type === 'success') {
            $header.addClass('success');
            $('#result-icon').attr('class', 'bi bi-check-circle-fill me-2 text-success');
            $('#result-content').css('color', '#28a745');
        } else if (type === 'error') {
            $header.addClass('error');
            $('#result-icon').attr('class', 'bi bi-x-circle-fill me-2 text-danger');
            $('#result-content').css('color', '#dc3545');
        } else {
            $('#result-icon').attr('class', 'bi bi-info-circle me-2');
            $('#result-content').css('color', '#d4d4d4');
        }

        // Show generic content
        $('#result-content').text(content).show();

        const modal = new bootstrap.Modal(document.getElementById('resultModal'));
        modal.show();
    }

    function showPipelineResult(type, result) {
        const $header = $('#result-modal-header');
        const $successContent = $('#result-success-content');
        const $errorContent = $('#result-error-content');
        const $genericContent = $('#result-content');

        // Reset visibility
        $successContent.hide();
        $errorContent.hide();
        $genericContent.hide();
        $header.removeClass('success error');

        const title = type.charAt(0).toUpperCase() + type.slice(1) + ' Result';
        $('#result-title').text(title);

        const modules = result.spec && result.spec.modules ? result.spec.modules : [];
        const isSuccess = result.status === 'ok' || (modules.length > 0 && !result.error);
        const isError = result.status === 'error' || result.error;

        if (isSuccess) {
            $header.addClass('success');
            $('#result-icon').attr('class', 'bi bi-check-circle-fill me-2 text-success');
            $('#result-millis').text('Completed in ' + result.millis + 'ms');

            const $accordion = $('#schemaAccordion');
            $accordion.empty();

            // Store schemas from spec.modules (returned by both DryRun and Run)
            if (modules.length > 0) {
                modules.forEach(function(module) {
                    moduleSchemas[module.name] = module.schema;
                    updateNodeSchemaIndicator(module.name, module.schema);
                });
            }

            const outputs = result.outputs || [];
            if (type === 'run' && outputs.length > 0) {
                // Store outputs for later access
                outputs.forEach(function(output) {
                    moduleOutputs[output.name] = output;
                    updateNodeOutputIndicator(output.name, output);
                });

                $('#result-success-content').find('p.mb-2').hide();

                outputs.forEach(function(output, index) {
                    const accordionId = 'output-' + index;
                    const records = output.records || [];
                    const schema = output.schema || {};
                    const recordsHtml = renderRecordsTable(records, schema);

                    const $item = $('<div class="accordion-item">' +
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
                    $accordion.append($item);
                });

                $('#resultModal').one('hidden.bs.modal', function() {
                    $('#result-success-content').find('p.mb-2').show();
                });
            } else if (modules.length > 0) {
                modules.forEach(function(module, index) {
                    const accordionId = 'schema-' + index;
                    const schemaHtml = renderSchemaFields(module.schema.fields);

                    const $item = $('<div class="accordion-item">' +
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
                    $accordion.append($item);
                });
            } else {
                $accordion.html('<p class="text-muted">No output schemas available.</p>');
            }

            $successContent.show();
        } else if (isError) {
            $header.addClass('error');
            $('#result-icon').attr('class', 'bi bi-x-circle-fill me-2 text-danger');

            if (result.error) {
                $('#error-module-name').text(result.error.name || 'Unknown');

                const moduleType = result.error.module || '';
                const $badge = $('#error-module-type');
                $badge.text(moduleType);
                $badge.removeClass('bg-success bg-primary bg-warning');
                if (moduleType === 'source') {
                    $badge.addClass('bg-success');
                } else if (moduleType === 'transform') {
                    $badge.addClass('bg-primary');
                } else if (moduleType === 'sink') {
                    $badge.addClass('bg-warning');
                }

                const $messages = $('#error-messages');
                $messages.empty();
                if (result.error.messages && Array.isArray(result.error.messages)) {
                    result.error.messages.forEach(function(msg) {
                        $messages.append($('<li>').text(msg));
                    });
                } else if (result.error.message) {
                    $messages.append($('<li>').text(result.error.message));
                }
            }

            $('#error-millis').text('Failed after ' + (result.millis || 0) + 'ms');
            $errorContent.show();
        } else {
            $genericContent.text(JSON.stringify(result, null, 2)).show();
        }

        const modal = new bootstrap.Modal(document.getElementById('resultModal'));
        modal.show();
    }

    function showModuleSchema(moduleName, schema) {
        const schemaHtml = renderSchemaFields(schema.fields);

        $('#result-modal-header').removeClass('success error').addClass('success');
        $('#result-icon').attr('class', 'bi bi-file-earmark-text me-2 text-primary');
        $('#result-title').text('Schema: ' + moduleName);

        $('#result-success-content').hide();
        $('#result-error-content').hide();
        $('#result-content').hide();

        const $accordion = $('#schemaAccordion');
        $accordion.html(schemaHtml);
        $('#result-millis').text('');
        $('#result-success-content').find('p.mb-2').hide();
        $('#result-success-content').show();

        const modal = new bootstrap.Modal(document.getElementById('resultModal'));
        modal.show();

        $('#resultModal').one('hidden.bs.modal', function() {
            $('#result-success-content').find('p.mb-2').show();
        });
    }

    function showModuleRecords(moduleName, output) {
        const records = output.records || [];
        const schema = output.schema || {};
        const recordsHtml = renderRecordsTable(records, schema);

        $('#result-modal-header').removeClass('success error').addClass('success');
        $('#result-icon').attr('class', 'bi bi-table me-2 text-info');
        $('#result-title').text('Records: ' + moduleName);

        $('#result-success-content').hide();
        $('#result-error-content').hide();
        $('#result-content').hide();

        const $accordion = $('#schemaAccordion');
        $accordion.html(recordsHtml);
        $('#result-millis').text(records.length + ' records');
        $('#result-success-content').find('p.mb-2').hide();
        $('#result-success-content').show();

        const modal = new bootstrap.Modal(document.getElementById('resultModal'));
        modal.show();

        $('#resultModal').one('hidden.bs.modal', function() {
            $('#result-success-content').find('p.mb-2').show();
        });
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

        const configYaml = jsyaml.dump(config);

        const buttonId = type === 'dryrun' ? '#btn-dryrun' : '#btn-run';
        const $button = $(buttonId);
        const originalHtml = $button.html();

        setRunningState(true, $button, type);
        setStatus('Running ' + type + '...', 'warning');

        const data = {
            config: configYaml,
            type: type
        };

        $.ajax({
            url: '/api/pipeline',
            type: 'POST',
            dataType: 'json',
            contentType: 'application/json',
            data: JSON.stringify(data),
            timeout: 300000,
            success: function(result) {
                setRunningState(false, $button, type, originalHtml);
                showPipelineResult(type, result);
                const status = result.status === 'ok' ? 'success' : 'error';
                setStatus(type + ' completed', status);
            },
            error: function(xhr, status, error) {
                setRunningState(false, $button, type, originalHtml);
                showResult('Error', 'Request failed: ' + error, 'error');
                setStatus(type + ' failed', 'error');
            }
        });
    }

    function setRunningState(isRunning, $button, type, originalHtml) {
        const $allButtons = $('#btn-dryrun, #btn-run, #btn-launch');

        if (isRunning) {
            $allButtons.prop('disabled', true);
            const spinnerHtml = '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span> Running...';
            $button.html(spinnerHtml);
        } else {
            $allButtons.prop('disabled', false);
            if (originalHtml) {
                $button.html(originalHtml);
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
        const modal = new bootstrap.Modal(document.getElementById('agentModal'));
        modal.show();
    }

    function agentRenderMessages() {
        const $container = $('#agent-chat-messages');
        $container.empty();

        if (agentChatHistory.length === 0) {
            $container.html(
                '<div class="agent-welcome text-center text-muted py-5">' +
                    '<i class="bi bi-robot" style="font-size: 3rem;"></i>' +
                    '<p class="mt-3">How can I help you build your pipeline?</p>' +
                    '<p class="small">Describe what data you want to process and I\'ll generate the configuration.</p>' +
                '</div>'
            );
            return;
        }

        // Group tool calls together
        let i = 0;
        while (i < agentChatHistory.length) {
            const msg = agentChatHistory[i];

            if (msg.role === 'user') {
                $container.append(agentCreateMessageEl('user', msg.content));
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
                $container.append(agentCreateToolGroupEl(toolCalls));
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
                $container.append(agentCreateAssistantMessageEl(displayText, configYaml));
                i++;
            } else if (msg.role === 'tool') {
                // Orphaned tool result (shouldn't happen normally), skip
                i++;
            } else {
                i++;
            }
        }

        // Scroll to bottom
        $container.scrollTop($container[0].scrollHeight);
    }

    function agentCreateMessageEl(role, content) {
        const avatarIcon = role === 'user' ? 'bi-person' : 'bi-robot';
        return $(
            '<div class="agent-message ' + role + '">' +
                '<div class="agent-message-avatar"><i class="bi ' + avatarIcon + '"></i></div>' +
                '<div class="agent-message-bubble">' + escapeHtml(content) + '</div>' +
            '</div>'
        );
    }

    function agentCreateAssistantMessageEl(message, configYaml) {
        const $msg = $(
            '<div class="agent-message assistant">' +
                '<div class="agent-message-avatar"><i class="bi bi-robot"></i></div>' +
                '<div class="agent-message-bubble"></div>' +
            '</div>'
        );
        const $bubble = $msg.find('.agent-message-bubble');
        $bubble.append($('<div>').text(message));

        if (configYaml) {
            const $badge = $('<div class="agent-config-badge"><i class="bi bi-check-lg me-1"></i>Apply config to canvas</div>');
            $badge.on('click', function() {
                agentApplyConfig(configYaml);
            });
            $bubble.append($badge);
            $bubble.append($('<pre>').text(configYaml));
        }

        return $msg;
    }

    function agentCreateToolGroupEl(toolCalls) {
        const $group = $('<div class="agent-tool-group"></div>');
        const toolNames = toolCalls.map(function(tc) { return tc.call.toolCall.name; }).join(', ');
        const $toggle = $(
            '<div class="agent-tool-toggle">' +
                '<i class="bi bi-chevron-right"></i>' +
                '<i class="bi bi-gear"></i>' +
                '<span>Tool used: ' + escapeHtml(toolNames) + '</span>' +
            '</div>'
        );
        const $detail = $('<div class="agent-tool-detail"></div>');

        toolCalls.forEach(function(tc) {
            let detailText = 'Call: ' + tc.call.toolCall.name + '(' + (tc.call.toolCall.arguments || '') + ')\n';
            if (tc.result) {
                const resultContent = tc.result.content || '';
                detailText += 'Result: ' + (resultContent.length > 500 ? resultContent.substring(0, 500) + '...' : resultContent) + '\n';
            }
            $detail.append($('<div>').text(detailText));
        });

        $toggle.on('click', function() {
            $(this).toggleClass('expanded');
            $detail.toggleClass('show');
        });

        $group.append($toggle);
        $group.append($detail);
        return $group;
    }

    function agentSendMessage() {
        if (agentIsSending) return;

        const input = $('#agent-chat-input').val().trim();
        if (!input) return;

        // Add user message to history
        agentChatHistory.push({ role: 'user', content: input });
        $('#agent-chat-input').val('');
        agentRenderMessages();

        // Show loading
        agentIsSending = true;
        $('#btn-agent-send').prop('disabled', true);
        const $container = $('#agent-chat-messages');
        const $loading = $(
            '<div class="agent-loading" id="agent-loading">' +
                '<div class="agent-message-avatar"><i class="bi bi-robot"></i></div>' +
                '<div class="agent-loading-dots"><span></span><span></span><span></span></div>' +
            '</div>'
        );
        $container.append($loading);
        $container.scrollTop($container[0].scrollHeight);

        // Send to server
        $.ajax({
            url: '/api/agent',
            type: 'POST',
            dataType: 'json',
            contentType: 'application/json',
            data: JSON.stringify({ history: agentChatHistory }),
            timeout: 120000,
            success: function(newMessages) {
                // Append new messages to history
                if (Array.isArray(newMessages)) {
                    newMessages.forEach(function(msg) {
                        agentChatHistory.push(msg);
                    });
                }
                agentRenderMessages();

                // Auto-apply config if the last assistant message has one
                var lastAssistant = null;
                for (var j = agentChatHistory.length - 1; j >= 0; j--) {
                    if (agentChatHistory[j].role === 'assistant' && !agentChatHistory[j].toolCall) {
                        lastAssistant = agentChatHistory[j];
                        break;
                    }
                }
                if (lastAssistant) {
                    try {
                        var parsed = JSON.parse(lastAssistant.content);
                        if (parsed.config) {
                            agentApplyConfig(parsed.config);
                        }
                    } catch (e) {
                        // not JSON
                    }
                }
            },
            error: function(xhr, status, error) {
                agentChatHistory.push({
                    role: 'assistant',
                    content: JSON.stringify({ message: 'Sorry, an error occurred: ' + error })
                });
                agentRenderMessages();
            },
            complete: function() {
                agentIsSending = false;
                $('#btn-agent-send').prop('disabled', false);
                $('#agent-loading').remove();
            }
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

        // Reuse applyConfig logic: clear canvas and rebuild
        editor.clear();
        nodeCounter = { source: 0, transform: 0, sink: 0 };

        const nodeIdMap = {};
        const layoutConfig = {
            startY: 50,
            nodeSpacingY: 150,
            columnX: { source: 100, transform: 400, sink: 700 }
        };

        function positionNode(nodeId, x, y) {
            editor.drawflow.drawflow.Home.data[nodeId].pos_x = x;
            editor.drawflow.drawflow.Home.data[nodeId].pos_y = y;
            const nodeElement = document.getElementById('node-' + nodeId);
            if (nodeElement) {
                nodeElement.style.left = x + 'px';
                nodeElement.style.top = y + 'px';
            }
        }

        function createWaitAndSideConnections(moduleConfig, nodeId) {
            if (moduleConfig.waits) {
                moduleConfig.waits.forEach(function(waitName) {
                    const sourceNodeId = nodeIdMap[waitName];
                    if (sourceNodeId) editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_2');
                });
            }
            if (moduleConfig.sideInputs) {
                moduleConfig.sideInputs.forEach(function(siName) {
                    const sourceNodeId = nodeIdMap[siName];
                    if (sourceNodeId) editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_3');
                });
            }
        }

        if (config.sources) {
            config.sources.forEach(function(sourceConfig, index) {
                const nodeId = addModuleToCanvas(sourceConfig.module, 'source', sourceConfig);
                nodeIdMap[sourceConfig.name] = nodeId;
                positionNode(nodeId, layoutConfig.columnX.source, layoutConfig.startY + index * layoutConfig.nodeSpacingY);
            });
        }

        if (config.transforms) {
            config.transforms.forEach(function(transformConfig, index) {
                const nodeId = addModuleToCanvas(transformConfig.module, 'transform', transformConfig);
                nodeIdMap[transformConfig.name] = nodeId;
                positionNode(nodeId, layoutConfig.columnX.transform, layoutConfig.startY + index * layoutConfig.nodeSpacingY);
                if (transformConfig.inputs) {
                    transformConfig.inputs.forEach(function(inputName) {
                        const sourceNodeId = nodeIdMap[inputName];
                        if (sourceNodeId) editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_1');
                    });
                }
            });
        }

        if (config.sinks) {
            config.sinks.forEach(function(sinkConfig, index) {
                const nodeId = addModuleToCanvas(sinkConfig.module, 'sink', sinkConfig);
                nodeIdMap[sinkConfig.name] = nodeId;
                positionNode(nodeId, layoutConfig.columnX.sink, layoutConfig.startY + index * layoutConfig.nodeSpacingY);
                if (sinkConfig.inputs) {
                    sinkConfig.inputs.forEach(function(inputName) {
                        const sourceNodeId = nodeIdMap[inputName];
                        if (sourceNodeId) editor.addConnection(sourceNodeId, nodeId, 'output_1', 'input_1');
                    });
                }
            });
        }

        var allModuleConfigs = [].concat(config.sources || [], config.transforms || [], config.sinks || []);
        allModuleConfigs.forEach(function(moduleConfig) {
            var nodeId = nodeIdMap[moduleConfig.name];
            if (nodeId) createWaitAndSideConnections(moduleConfig, nodeId);
        });

        Object.values(nodeIdMap).forEach(function(nodeId) {
            editor.updateConnectionNodes('node-' + nodeId);
        });

        systemConfig = config.system || {};
        optionsConfig = config.options || {};

        editor.zoom_reset();
        setStatus('Agent config applied to canvas', 'success');
    }

    // =============================
    // Section 11: Init & Event Handlers
    // =============================

    function loadSpec() {
        return $.ajax({
            url: '/api/spec',
            type: 'GET',
            dataType: 'json'
        }).then(function(data) {
            const modules = data.modules || {};

            // Convert source summaries to module definitions
            const sources = (modules.sources || []).map(function(schema) {
                const schemaId = schema['$id'] || '';
                const name = schemaId.split('/').pop() || schema.title;
                return {
                    name: name,
                    label: schema.title || name,
                    description: schema.description || ''
                };
            });

            // Convert transform summaries to module definitions
            const transforms = (modules.transforms || []).map(function(schema) {
                const schemaId = schema['$id'] || '';
                const name = schemaId.split('/').pop() || schema.title;
                return {
                    name: name,
                    label: schema.title || name,
                    description: schema.description || ''
                };
            });

            // Convert sink summaries to module definitions
            const sinks = (modules.sinks || []).map(function(schema) {
                const schemaId = schema['$id'] || '';
                const name = schemaId.split('/').pop() || schema.title;
                return {
                    name: name,
                    label: schema.title || name,
                    description: schema.description || ''
                };
            });

            moduleDefs = {
                sources: sources,
                transforms: transforms,
                sinks: sinks
            };
        });
    }

    function initEventHandlers() {
        // Header buttons
        $('#btn-dryrun').on('click', function() {
            runPipeline('dryrun');
        });

        $('#btn-run').on('click', function() {
            runPipeline('run');
        });

        // Module Config Modal
        $('#btn-save-module').on('click', saveModuleConfig);
        $('#btn-delete-module').on('click', deleteModule);

        // System Modal
        $('#btn-system').on('click', openSystemModal);
        $('#btn-apply-system').on('click', applySystemConfig);

        // Options Modal
        $('#btn-options').on('click', openOptionsModal);
        $('#btn-apply-options').on('click', applyOptionsConfig);

        // Launch Modal
        $('#btn-launch').on('click', openLaunchModal);
        $('#launch-runner').on('change', onRunnerChanged);
        $('#launch-environment').on('change', onEnvironmentChanged);
        $('#btn-launch-execute').on('click', executeLaunch);

        // Agent Chat Modal
        $('#btn-agent').on('click', openAgentModal);
        $('#btn-agent-send').on('click', agentSendMessage);
        $('#btn-agent-clear').on('click', agentClearHistory);

        // IME composition handling for agent input
        document.getElementById('agent-chat-input').addEventListener('compositionstart', function() {
            agentIsComposing = true;
        });
        document.getElementById('agent-chat-input').addEventListener('compositionend', function() {
            agentIsComposing = false;
        });
        $('#agent-chat-input').on('keydown', function(e) {
            if (e.key === 'Enter' && !e.shiftKey && !agentIsComposing) {
                e.preventDefault();
                agentSendMessage();
            }
        });

        // Config Editor Modal
        $('#btn-edit').on('click', openConfigEditor);
        $('#edit-format').on('change', updateConfigEditorContent);
        $('#btn-copy-config').on('click', copyConfigToClipboard);
        $('#btn-download-config').on('click', downloadConfig);
        $('#btn-apply-config').on('click', applyConfig);
        $('#btn-clear-config').on('click', clearConfigEditor);

        // Monaco: modal shown handlers
        // Fetch module schema on demand so the HTTP round-trip provides a natural
        // macrotask boundary for the language service to initialize.
        $('#moduleConfigModal').on('shown.bs.modal', function() {
            var type = pendingModuleType;
            var name = pendingModuleName;
            var yaml = pendingModuleYaml;
            Promise.all([
                loadMonaco(),
                $.ajax({ url: '/api/spec/' + type + '/' + name, type: 'GET', dataType: 'json' })
            ]).then(function(results) {
                var moduleEditorSchema = results[1];
                if (yamlApi) {
                    var schemas = buildStaticSchemas();
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
        $('#editConfigModal').on('shown.bs.modal', function() {
            updateConfigEditorContent();
        });
        $('#systemModal').on('shown.bs.modal', function() {
            Promise.all([loadMonaco(), ensureSystemSchema()]).then(function() {
                if (yamlApi) {
                    yamlApi.update({ schemas: buildStaticSchemas() });
                }
                buildSchemaHelpTooltip('#system-help-icon', cachedSystemSchema);
                return setEditorValue('system-yaml-editor', pendingSystemYaml);
            });
        });
        $('#optionsModal').on('shown.bs.modal', function() {
            Promise.all([loadMonaco(), ensureOptionsSchema()]).then(function() {
                if (yamlApi) {
                    yamlApi.update({ schemas: buildStaticSchemas() });
                }
                buildSchemaHelpTooltip('#options-help-icon', cachedOptionsSchema);
                return setEditorValue('options-yaml-editor', pendingOptionsYaml);
            });
        });

        // Resize handle for left pane
        initResizeHandle();
    }

    function initResizeHandle() {
        const resizeHandle = document.getElementById('resize-handle');
        const leftPane = document.getElementById('left-pane');
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

    $(document).ready(function() {
        init();
    });

})(jQuery);

/**
 * util.js - DOM, HTTP and rendering helpers shared by all modules.
 */
'use strict';

// =============================
// DOM helpers
// =============================

export function $id(id) {
    return document.getElementById(id);
}

export function on(id, eventName, handler) {
    $id(id).addEventListener(eventName, handler);
}

export function show(el) {
    el.style.display = '';
}

export function hide(el) {
    el.style.display = 'none';
}

export function showModal(id) {
    bootstrap.Modal.getOrCreateInstance($id(id)).show();
}

export function hideModal(id) {
    const modal = bootstrap.Modal.getInstance($id(id));
    if (modal) modal.hide();
}

// =============================
// HTTP helpers
// =============================

export function getJson(url) {
    return fetch(url).then(function(res) {
        if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + res.statusText);
        return res.json();
    });
}

export function postJson(url, body, timeoutMs) {
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
// Text & status helpers
// =============================

export function escapeHtml(text) {
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

export function setStatus(message, type) {
    const status = $id('status-message');
    status.textContent = message;
    status.classList.remove('success', 'error', 'warning');
    if (type) {
        status.classList.add(type);
    }
}

export function dumpYaml(obj) {
    if (!obj || Object.keys(obj).length === 0) return '';
    try {
        return jsyaml.dump(obj, { lineWidth: -1 });
    } catch (e) {
        return JSON.stringify(obj, null, 2);
    }
}

// =============================
// Schema & records rendering
// =============================

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
let schemaPanelCounter = 0;
const schemaPanels = {};   // panel id -> raw schema (for "Copy JSON")

/**
 * Normalize a field or a nested type object (Schema.Field.toJsonObject /
 * Schema.FieldType.toJsonObject emit slightly different shapes) into
 * { type, fields, valueType, symbols, shape } where valueType is itself a
 * normalized type (array element / map value / matrix value), recursively.
 */
function normalizeType(t) {
    if (!t) return { type: 'unknown' };
    if (typeof t === 'string') return { type: t };
    const n = { type: t.type || 'unknown' };
    const fields = t.fields || (t.schema && t.schema.fields);
    if (fields && fields.length) n.fields = fields;
    if (n.type === 'array') {
        n.valueType = normalizeType(t.arrayValueType);
    } else if (n.type === 'map') {
        n.valueType = normalizeType(t.mapValueType || t.valueType);
    } else if (n.type === 'matrix') {
        n.valueType = normalizeType(t.matrixValueType || t.valueType);
        n.shape = t.shape;
    } else if (n.type === 'enumeration') {
        n.symbols = t.symbols || t.enumSymbols || [];
    }
    return n;
}

/** Innermost type that can carry fields (element), through array/map nesting. */
function leafType(n) {
    let cur = n;
    while (cur && (cur.type === 'array' || cur.type === 'map') && cur.valueType) cur = cur.valueType;
    return cur;
}

/** Type label such as array<element>, map<string, element>, matrix<float32, 2x3>. */
function typeLabel(n) {
    switch (n.type) {
        case 'array': return 'array<' + typeLabel(n.valueType) + '>';
        case 'map': return 'map<string, ' + typeLabel(n.valueType) + '>';
        case 'matrix': return 'matrix<' + typeLabel(n.valueType) + (n.shape ? ', ' + n.shape.join('x') : '') + '>';
        default: return n.type;
    }
}

function countFields(fields) {
    let count = 0;
    (fields || []).forEach(function(field) {
        count++;
        const leaf = leafType(normalizeType(field));
        if (leaf && leaf.fields) count += countFields(leaf.fields);
    });
    return count;
}

export function renderSchemaFields(fields, depth, parentId) {
    if (!fields || fields.length === 0) {
        return '<p class="text-muted">No fields</p>';
    }

    depth = depth || 0;
    const isRoot = depth === 0;

    let html = '<div class="schema-fields-list' + (isRoot ? '' : ' schema-nested ps-3 border-start') + '">';

    fields.forEach(function(field, index) {
        const fieldId = (parentId ? parentId + '-' : 'field-') + index;
        const type = normalizeType(field);
        const leaf = leafType(type);
        const nestedFields = (leaf && leaf.fields) ? leaf.fields : null;
        const collapseId = 'schema-collapse-' + (++schemaCollapseCounter);

        let modeBadge = '';
        if (field.mode === 'repeated') {
            modeBadge = '<span class="badge bg-info ms-1">repeated</span>';
        } else if (field.mode === 'required') {
            modeBadge = '<span class="badge bg-danger ms-1">required</span>';
        } else {
            modeBadge = '<span class="schema-field-nullable ms-1" title="nullable">?</span>';
        }

        const typeColorClass = getTypeColorClass(leaf && leaf.type === 'element' ? 'element' : type.type);
        let typeHtml = '<span class="badge ' + typeColorClass + ' ms-2">' + escapeHtml(typeLabel(type)) + '</span>';
        if (type.type === 'enumeration' && type.symbols.length) {
            typeHtml += '<span class="text-muted small ms-1">[' + type.symbols.map(escapeHtml).join(', ') + ']</span>';
        }

        html += '<div class="schema-field-item py-1' + (index < fields.length - 1 ? ' border-bottom' : '') + '" data-field-id="' + fieldId + '" data-field-name="' + escapeHtml(field.name) + '">';
        html += '<div class="d-flex align-items-center">';
        if (nestedFields) {
            html += '<button class="btn btn-sm btn-link text-decoration-none p-0 me-2 schema-toggle-btn" ';
            html += 'type="button" data-bs-toggle="collapse" data-bs-target="#' + collapseId + '" ';
            html += 'aria-expanded="false" aria-controls="' + collapseId + '">';
            html += '<i class="bi bi-chevron-right schema-toggle-icon"></i>';
            html += '</button>';
        } else {
            html += '<span class="schema-toggle-spacer me-2"></span>';
        }
        html += '<span class="schema-field-name fw-medium">' + escapeHtml(field.name) + '</span>';
        html += typeHtml;
        if (nestedFields) {
            html += '<span class="text-muted small ms-1">(' + nestedFields.length + ' fields)</span>';
        }
        html += modeBadge;
        html += '</div>';
        if (nestedFields) {
            html += '<div class="collapse mt-1" id="' + collapseId + '">';
            html += renderSchemaFields(nestedFields, depth + 1, fieldId);
            html += '</div>';
        }
        html += '</div>';
    });

    html += '</div>';
    return html;
}

/**
 * Convert the server's schema JSON (Schema.toJsonObject: type=array +
 * arrayValueType, map fields under mapValueType, ...) into the config-file
 * schema syntax that Schema.parse reads (mode=repeated, map valueType + fields,
 * matrix valueType + shape), so the copied text can be pasted into a module's
 * schema.fields. Nested arrays (array<array<..>>) cannot be expressed there and
 * are kept as-is.
 */
function toConfigSchema(schema) {
    function convertFields(fields) {
        return (fields || []).map(function(field) {
            const n = normalizeType(field);
            const out = { name: field.name };
            let t = n;
            let mode = field.mode || 'nullable';
            if (t.type === 'array' && t.valueType && t.valueType.type !== 'array') {
                mode = 'repeated';
                t = t.valueType;
            }
            out.type = t.type;
            // a matrix is inherently repeated; the config form carries only shape/valueType
            if (mode !== 'nullable' && t.type !== 'matrix') out.mode = mode;
            if (t.type === 'element' && t.fields) {
                out.fields = convertFields(t.fields);
            } else if (t.type === 'map' && t.valueType) {
                out.valueType = t.valueType.type;
                if (t.valueType.type === 'element' && t.valueType.fields) out.fields = convertFields(t.valueType.fields);
            } else if (t.type === 'matrix') {
                if (t.valueType) out.valueType = t.valueType.type;
                if (t.shape) out.shape = t.shape;
            } else if (t.type === 'enumeration') {
                out.symbols = t.symbols;
            } else if (t.type === 'array') {
                out.arrayValueType = field.arrayValueType;   // not expressible; keep raw
            }
            return out;
        });
    }
    return { fields: convertFields(schema.fields) };
}

/**
 * Schema fields with a toolbar: total field count, expand/collapse all,
 * name filter and "Copy JSON". Toolbar events are handled by delegation
 * (initSchemaPanelEvents, once).
 */
export function renderSchemaPanel(schema) {
    const fields = (schema && schema.fields) || [];
    // Drop schemas whose panel is no longer in the document (results are re-rendered per run)
    Object.keys(schemaPanels).forEach(function(id) {
        if (!document.getElementById(id)) delete schemaPanels[id];
    });
    const panelId = 'schema-panel-' + (++schemaPanelCounter);
    schemaPanels[panelId] = schema || {};
    const total = countFields(fields);
    const hasNested = total > fields.length;

    let html = '<div class="schema-panel" id="' + panelId + '">';
    html += '<div class="schema-toolbar d-flex align-items-center gap-2 mb-2">';
    html += '<span class="text-muted small">' + fields.length + ' fields' + (hasNested ? ' (' + total + ' incl. nested)' : '') + '</span>';
    html += '<input type="search" class="form-control form-control-sm schema-filter ms-auto" placeholder="Filter fields..." style="max-width: 200px;">';
    if (hasNested) {
        html += '<button type="button" class="btn btn-sm btn-outline-secondary" data-schema-action="expand" title="Expand all"><i class="bi bi-arrows-expand"></i></button>';
        html += '<button type="button" class="btn btn-sm btn-outline-secondary" data-schema-action="collapse" title="Collapse all"><i class="bi bi-arrows-collapse"></i></button>';
    }
    html += '<button type="button" class="btn btn-sm btn-outline-secondary" data-schema-action="copy" title="Copy as config schema (paste into a module\'s schema.fields)"><i class="bi bi-clipboard"></i></button>';
    html += '</div>';
    html += renderSchemaFields(fields);
    html += '</div>';
    return html;
}

function setSchemaCollapsed(panel, collapsed) {
    panel.querySelectorAll('.schema-fields-list .collapse').forEach(function(el) {
        el.classList.toggle('show', !collapsed);
    });
    panel.querySelectorAll('.schema-toggle-btn').forEach(function(btn) {
        btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    });
}

function filterSchemaFields(panel, query) {
    const q = query.trim().toLowerCase();
    const items = panel.querySelectorAll('.schema-field-item');
    if (!q) {
        items.forEach(function(item) { item.classList.remove('d-none'); });
        // Restore the expand state from before the filter was applied
        if (panel.__expandedBeforeFilter) {
            panel.querySelectorAll('.schema-fields-list .collapse').forEach(function(el) {
                el.classList.toggle('show', !!panel.__expandedBeforeFilter[el.id]);
            });
            panel.querySelectorAll('.schema-toggle-btn').forEach(function(btn) {
                const target = btn.getAttribute('data-bs-target').substring(1);
                btn.setAttribute('aria-expanded', panel.__expandedBeforeFilter[target] ? 'true' : 'false');
            });
            delete panel.__expandedBeforeFilter;
        }
        return;
    }
    if (!panel.__expandedBeforeFilter) {
        panel.__expandedBeforeFilter = {};
        panel.querySelectorAll('.schema-fields-list .collapse').forEach(function(el) {
            panel.__expandedBeforeFilter[el.id] = el.classList.contains('show');
        });
    }
    // Show items whose name matches or that contain a matching descendant; expand to reveal
    items.forEach(function(item) {
        const self = (item.dataset.fieldName || '').toLowerCase().indexOf(q) >= 0;
        const child = !self && !!Array.prototype.find.call(item.querySelectorAll('.schema-field-item'), function(c) {
            return (c.dataset.fieldName || '').toLowerCase().indexOf(q) >= 0;
        });
        item.classList.toggle('d-none', !(self || child));
    });
    setSchemaCollapsed(panel, false);
}

let schemaPanelEventsInstalled = false;
export function initSchemaPanelEvents() {
    if (schemaPanelEventsInstalled) return;
    schemaPanelEventsInstalled = true;
    document.addEventListener('click', function(e) {
        const btn = e.target.closest('[data-schema-action]');
        if (!btn) return;
        const panel = btn.closest('.schema-panel');
        if (!panel) return;
        const action = btn.dataset.schemaAction;
        if (action === 'expand') {
            setSchemaCollapsed(panel, false);
        } else if (action === 'collapse') {
            setSchemaCollapsed(panel, true);
        } else if (action === 'copy') {
            const json = JSON.stringify(toConfigSchema(schemaPanels[panel.id] || {}), null, 2);
            navigator.clipboard.writeText(json).then(function() {
                const icon = btn.querySelector('i');
                icon.className = 'bi bi-clipboard-check text-success';
                setTimeout(function() { icon.className = 'bi bi-clipboard'; }, 1500);
            });
        }
    });
    document.addEventListener('input', function(e) {
        if (!e.target.classList || !e.target.classList.contains('schema-filter')) return;
        const panel = e.target.closest('.schema-panel');
        if (panel) filterSchemaFields(panel, e.target.value);
    });
}

export function renderRecordsTable(records, schema) {
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

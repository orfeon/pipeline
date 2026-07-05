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

export function renderSchemaFields(fields, depth, parentId) {
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

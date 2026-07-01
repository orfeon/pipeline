# Frontend Architecture

This document describes the frontend architecture of Mercari Pipeline GUI Editor.

## Project Structure

```
src/main/webapp/
├── index.jsp                     # Main JSP page (entry point)
├── css/
│   └── index.css                 # Custom styles
└── js/
    └── app.js                    # Single consolidated application file (IIFE)
```

## Technology Stack

- jQuery 4.0.0 (DOM manipulation, AJAX)
- Bootstrap 5.3.2 (UI components, modals, accordion)
- Bootstrap Icons 1.11.1 (icon library)
- Drawflow 0.0.60 (visual node editor)
- js-yaml 4.1.0 (YAML parsing/serialization)
- Monaco Editor 0.53.0 (YAML code editor, loaded lazily via ESM)
- monaco-yaml-inline 1.0.0 (YAML language server for Monaco, loaded lazily via ESM)

## Architecture

All application code lives in a single IIFE in `app.js`:

```javascript
(function($) {
    'use strict';
    // All state, functions, and event handlers are private
    // No global namespace pollution
})(jQuery);
```

### Section Layout

`app.js` is organized into numbered sections:

| Section | Description |
|---------|-------------|
| 1 | State variables |
| 1.5 | Monaco Editor management (lazy loading, creation, value get/set) |
| 1.8 | YAML Schema builders and lazy-load helpers |
| 2 | Utilities (escapeHtml, setStatus, formatTimestamp, schema/records rendering) |
| 3 | Drawflow & Canvas (init, module list, node management) |
| 4 | Pipeline config generation (generateConfig, getValidationErrors) |
| 5 | Module Config Modal |
| 6 | System & Options Modals |
| 7 | Config Editor Modal |
| 8 | Launch Modal |
| 9 | Result Modal |
| 10 | Pipeline Execution (dryrun, run) |
| 10.5 | Agent Chat |
| 11 | Init & Event Handlers |

## Initialization Flow

```
init()
  → loadSpec()              // GET /api/spec (module summaries only)
  → initDrawflow()          // Initialize Drawflow editor
  → initModuleList()        // Render module items in left pane
  → initEventHandlers()     // Bind all event handlers
  → loadMonaco()            // Pre-load Monaco (fire-and-forget)
```

## Data Loading Strategy

The frontend uses a **lazy-loading** approach for schemas:

### Page Load (`GET /api/spec`)

Returns only lightweight module summaries (~3KB):

```json
{
  "modules": {
    "sources":    [{"$id": "...bigquery", "title": "BigQuery", "description": "..."}],
    "transforms": [...],
    "sinks":      [...]
  }
}
```

### On-Demand Schema Loading

Schemas are fetched and cached when the corresponding modal opens for the first time:

| Endpoint | Trigger | Cache variable |
|----------|---------|---------------|
| `GET /api/spec/{type}/{name}` | Module config modal opens | (not cached, fetched every time) |
| `GET /api/spec/system` | System modal opens | `cachedSystemSchema` |
| `GET /api/spec/options` | Options modal opens | `cachedOptionsSchema` |
| `GET /api/spec/launch` | Launch modal opens | `cachedLaunchSchema` |

Lazy-load helpers: `ensureSystemSchema()`, `ensureOptionsSchema()`, `ensureLaunchSchema()`.

## Monaco Editor Integration

Monaco Editor and the YAML plugin are loaded lazily via ESM dynamic `import()` on first use. The `loadMonaco()` function caches the result so imports happen only once.

### Schema Registration

The `yamlApi.update({ schemas: [...] })` call registers JSON Schemas with the YAML Language Server. Each schema entry has:

- `uri` - Unique identifier for the schema
- `fileMatch` - Array of model URIs this schema applies to (e.g., `['internal://server/module-yaml-editor.yaml']`)
- `schema` - The JSON Schema object

`buildStaticSchemas()` returns the system and options schemas (if cached). Module-specific schemas are fetched on demand and pushed to the array.

### Editor Model URIs

Each Monaco editor model has a URI derived from its container ID:

```
internal://server/{containerId}.yaml
```

Examples:
- `internal://server/module-yaml-editor.yaml`
- `internal://server/system-yaml-editor.yaml`
- `internal://server/options-yaml-editor.yaml`
- `internal://server/edit-content.yaml`

## State Variables

| Variable | Description |
|----------|-------------|
| `editor` | Drawflow editor instance |
| `moduleDefs` | Module definitions `{ sources: [], transforms: [], sinks: [] }` |
| `nodeCounter` | Per-type counter for auto-naming `{ source: 0, transform: 0, sink: 0 }` |
| `systemConfig` | Current system config object |
| `optionsConfig` | Current pipeline options object |
| `moduleSchemas` | Dryrun result cache (module name -> output schema) |
| `moduleOutputs` | Run result cache (module name -> output records) |
| `currentEditingNodeId` | Node ID being edited in module config modal |
| `yamlApi` | `configureMonacoYaml` return value for schema updates |
| `cachedSystemSchema` | Lazy-loaded system JSON Schema |
| `cachedOptionsSchema` | Lazy-loaded options JSON Schema |
| `cachedLaunchSchema` | Lazy-loaded launch JSON Schema |
| `monacoInstance` | Cached Monaco module promise |
| `monacoEditors` | Map of containerId -> Monaco editor instance |

## Key Functions

### Monaco Editor Management

| Function | Description |
|----------|-------------|
| `loadMonaco()` | Lazy-loads Monaco + YAML plugin, returns cached promise |
| `createOrGetEditor(containerId, language)` | Creates or retrieves a Monaco editor for a DOM container |
| `setEditorValue(containerId, value, language)` | Sets editor content (creates editor if needed) |
| `getEditorValue(containerId)` | Returns current editor content |

### Schema Helpers

| Function | Description |
|----------|-------------|
| `buildStaticSchemas()` | Builds schema array from cached system/options schemas |
| `ensureSystemSchema()` | Fetches and caches system schema (returns promise) |
| `ensureOptionsSchema()` | Fetches and caches options schema (returns promise) |
| `ensureLaunchSchema()` | Fetches and caches launch schema (returns promise) |

### Canvas & Config

| Function | Description |
|----------|-------------|
| `initDrawflow()` | Initializes the Drawflow editor with event handlers |
| `addModuleToCanvas(moduleName, moduleType, config)` | Adds a module node to the canvas |
| `createNodeHtml(moduleName, moduleType, name)` | Generates HTML for a Drawflow node |
| `generateConfig()` | Generates full pipeline config YAML from canvas state |
| `getValidationErrors(config)` | Returns array of validation error strings |
| `runPipeline(type)` | Executes pipeline (dryrun/run) via `/api/pipeline` |

### Modals

| Function | Description |
|----------|-------------|
| `openModuleConfig(nodeId)` | Opens module config modal for a node |
| `saveModuleConfig()` | Saves module config from modal to canvas node |
| `openSystemModal()` | Opens system settings modal (Monaco YAML editor) |
| `openOptionsModal()` | Opens pipeline options modal (Monaco YAML editor) |
| `openLaunchModal()` | Opens launch modal (lazy-loads launch schema, populates runners) |
| `showLaunchParametersForm(runnerSchema, envSchema)` | Generates HTML form fields from launch schema properties |
| `executeLaunch()` | Collects form values and sends launch request |
| `openConfigEditor()` | Opens full pipeline config editor modal |
| `showResult(title, content, type)` | Shows generic result modal |
| `showPipelineResult(type, result)` | Shows structured pipeline result |

### Agent Chat

| Function | Description |
|----------|-------------|
| `openAgentModal()` | Opens AI agent chat modal |
| `agentSendMessage()` | Sends user message to `/api/agent` |
| `agentApplyConfig(configText)` | Applies agent-generated config to canvas |

## Event Handlers

```javascript
// Header buttons
$('#btn-dryrun').on('click', () => runPipeline('dryrun'))
$('#btn-run').on('click', () => runPipeline('run'))
$('#btn-launch').on('click', openLaunchModal)
$('#btn-edit').on('click', openConfigEditor)

// Settings buttons
$('#btn-system').on('click', openSystemModal)
$('#btn-options').on('click', openOptionsModal)
$('#btn-apply-system').on('click', applySystemConfig)
$('#btn-apply-options').on('click', applyOptionsConfig)

// Module config modal
$('#btn-save-module').on('click', saveModuleConfig)
$('#btn-delete-module').on('click', deleteModule)

// Launch modal
$('#launch-runner').on('change', onRunnerChanged)
$('#launch-environment').on('change', onEnvironmentChanged)
$('#btn-launch-execute').on('click', executeLaunch)

// Agent chat
$('#btn-agent').on('click', openAgentModal)
$('#btn-agent-send').on('click', agentSendMessage)
$('#btn-agent-clear').on('click', agentClearHistory)

// Edit config modal
$('#btn-edit').on('click', openConfigEditor)
$('#edit-format').on('change', updateConfigEditorContent)
$('#btn-copy-config').on('click', copyConfigToClipboard)
$('#btn-download-config').on('click', downloadConfig)
$('#btn-apply-config').on('click', applyConfig)

// Monaco: modal shown handlers (schema loading + editor init)
$('#moduleConfigModal').on('shown.bs.modal', ...)  // Fetch module schema + init editor
$('#systemModal').on('shown.bs.modal', ...)         // ensureSystemSchema + init editor
$('#optionsModal').on('shown.bs.modal', ...)        // ensureOptionsSchema + init editor
$('#editConfigModal').on('shown.bs.modal', ...)     // Init editor

// Drawflow events
editor.on('nodeCreated', ...)
editor.on('nodeRemoved', ...)
editor.on('connectionCreated', ...)
editor.on('connectionRemoved', ...)
container.addEventListener('dblclick', handleDoubleClick)  // Opens module config modal
```

## HTML Structure (index.jsp)

### Main Layout IDs

```html
<!-- Header -->
#btn-agent, #btn-dryrun, #btn-run, #btn-launch, #btn-edit

<!-- Left Pane -->
#left-pane
#btn-system, #btn-options
#collapse-sources, #collapse-transforms, #collapse-sinks
#source-modules, #transform-modules, #sink-modules

<!-- Right Pane -->
#drawflow (Drawflow container)

<!-- Resize Handle -->
#resize-handle

<!-- Footer -->
#status-message
```

### Modal IDs

```html
<!-- Module Config Modal (Monaco YAML editor) -->
#moduleConfigModal
#modal-module-type, #modal-module-name
#module-name-input
#module-yaml-editor (Monaco container)
#btn-save-module, #btn-delete-module

<!-- System Modal (Monaco YAML editor) -->
#systemModal
#system-yaml-editor (Monaco container)
#btn-apply-system

<!-- Options Modal (Monaco YAML editor) -->
#optionsModal
#options-yaml-editor (Monaco container)
#btn-apply-options

<!-- Launch Modal (HTML form fields) -->
#launchModal
#launch-runner, #launch-runner-desc
#launch-args
#launch-environment-group, #launch-environment, #launch-environment-desc
#launch-parameters-container, #launch-parameters-fields
  → Dynamic fields with class .launch-param-field and data-param-name attribute
#btn-launch-execute

<!-- Edit Config Modal (Monaco editor, YAML or JSON) -->
#editConfigModal
#edit-format, #edit-content (Monaco container)
#btn-copy-config, #btn-download-config, #btn-apply-config

<!-- Result Modal -->
#resultModal
#result-modal-header (add class: success|error)
#result-icon, #result-title
#result-success-content, #result-millis, #schemaAccordion
#result-error-content, #error-module-name, #error-module-type, #error-messages, #error-millis
#result-content (generic pre element)

<!-- Agent Chat Modal -->
#agentModal
#agent-chat-messages, #agent-chat-input
#btn-agent-send, #btn-agent-clear

<!-- Hidden -->
#file-import (hidden file input)
```

## CSS Architecture (index.css)

### CSS Variables

```css
:root {
    --header-height: 56px;
    --footer-height: 32px;
    --left-pane-width: 280px;
    --resize-handle-width: 6px;
    --source-color: #198754;
    --transform-color: #0d6efd;
    --sink-color: #fd7e14;
    --source-bg: #d1e7dd;
    --transform-bg: #cfe2ff;
    --sink-bg: #ffe5d0;
    --focus-color: #fff3cd;
    --focus-border-color: #ffc107;
}
```

### Key CSS Classes

```css
/* Layout */
.main-container          /* Flex container for left/right panes */
.left-pane               /* Left sidebar */
.right-pane              /* Canvas area */
.resize-handle           /* Draggable resize handle between panes */
.drawflow-container      /* Drawflow wrapper with grid background */

/* Module items */
.module-item             /* Clickable module in left pane */
.module-item.source      /* Green styling */
.module-item.transform   /* Blue styling */
.module-item.sink        /* Orange styling */

/* Module categories */
.module-category         /* Collapsible category wrapper */
.module-category-header  /* Clickable header with collapse icon */
.collapse-icon           /* Chevron icon, rotates on collapse */

/* Drawflow nodes */
.drawflow-node           /* Node container */
.drawflow-node.selected  /* Selected state */
.node-content            /* Inner content wrapper */
.node-header             /* Colored header (source|transform|sink) */
.node-body               /* Name and type display */
.node-schema-indicator   /* Schema icon (bottom-right) */

/* Monaco containers */
.monaco-container        /* Base Monaco editor container */
.monaco-sm               /* Small height */
.monaco-md               /* Medium height */
.monaco-lg               /* Large height */
.monaco-xl               /* Extra-large height */

/* Code input */
.code-input              /* Monospace textarea for JSON editing (e.g., launch args) */
```

## Drawflow Integration

### Initialization

```javascript
const container = document.getElementById('drawflow');
editor = new Drawflow(container);
editor.reroute = true;
editor.curvature = 0.5;
editor.editor_mode = 'edit';
editor.start();
```

### Node Data Structure

```javascript
{
  moduleName: "bigquery",      // Module type name
  moduleType: "source",        // source|transform|sink
  name: "bigquery_1",          // Instance name (unique)
  config: { ... }              // Parsed YAML config (parameters, schema, strategy, etc.)
}
```

### Accessing Node Data

```javascript
// Get node data
const nodeData = editor.getNodeFromId(nodeId);
const customData = nodeData.data;

// Update node data
editor.updateNodeDataFromId(nodeId, newData);

// Export all nodes
const exportData = editor.export();
const nodes = exportData.drawflow.Home.data;

// Clear canvas
editor.clear();
```

## Pipeline Configuration Format

Generated config structure:

```yaml
system:
  args:
    key: value
  context: "gs://bucket/path"
  imports:
    - base: "gs://bucket/"
      files: ["common.yaml"]
  failure:
    failFast: true

options:
  jobName: "my-job"
  streaming: true
  tempLocation: "gs://bucket/temp/"
  dataflow:
    workerMachineType: "n1-standard-4"
    maxWorkers: 10

sources:
  - name: source_1
    module: bigquery
    parameters:
      query: "SELECT * FROM table"

transforms:
  - name: transform_1
    module: select
    inputs:
      - source_1
    parameters:
      select: [...]

sinks:
  - name: sink_1
    module: bigquery
    inputs:
      - transform_1
    parameters:
      table: "project.dataset.table"
```

## Adding New Features

### Adding a New Modal

1. Add HTML modal structure in `index.jsp`:
```html
<div class="modal fade" id="newModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title">New Modal</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
            </div>
            <div class="modal-body">...</div>
            <div class="modal-footer">...</div>
        </div>
    </div>
</div>
```

2. Add functions and event handlers in `app.js`:
```javascript
// In the appropriate section, add:
function openNewModal() {
    const modal = new bootstrap.Modal(document.getElementById('newModal'));
    modal.show();
}

// In initEventHandlers(), bind:
$('#btn-new').on('click', openNewModal);

// If Monaco editor is needed, add shown.bs.modal handler:
$('#newModal').on('shown.bs.modal', function() {
    loadMonaco().then(function() {
        // Set up schema and editor
    });
});
```

### Adding a New Lazy-Loaded Schema

1. Add a server endpoint in `SpecService.java`:
```java
public static void serveNewSchema(...) throws IOException {
    final JsonObject schema = prepareEditorSchema(ConfigSchema.getNewJsonSchema());
    response.getWriter().println(schema.toString());
}
```

2. Add routing in `PipelineApiServer.java`:
```java
case "new" -> SpecService.serveNewSchema(request, response);
```

3. Add a lazy-load helper in `app.js`:
```javascript
let cachedNewSchema = null;

function ensureNewSchema() {
    if (cachedNewSchema) return Promise.resolve(cachedNewSchema);
    return $.ajax({ url: '/api/spec/new', type: 'GET', dataType: 'json' }).then(function(data) {
        cachedNewSchema = data;
        return data;
    });
}
```

## Cache Busting

When making changes, increment the `id` query parameter in `index.jsp`:

```html
<link href="css/index.css?id=15" rel="stylesheet">
<script src="js/app.js?id=15"></script>
```

## Development Commands

```bash
# Build WAR file
mvn clean package -Pserver -DskipTests

# Access UI
http://localhost:8080/
```

## Browser Compatibility

- Modern browsers (Chrome, Firefox, Safari, Edge)
- Requires JavaScript enabled
- Uses ES6+ features (let, const, arrow functions, template literals, dynamic import)

## Known Limitations

- Single canvas/module space (Drawflow "Home")
- No undo/redo functionality
- No auto-save (manual export required)
- Connection ports limited to single input/output per side

## Troubleshooting

### YAML Autocompletion Not Working in Monaco Editor

The Monaco YAML editor uses `monaco-yaml-inline`, which internally relies on a YAML Language Server based on `vscode-json-languageservice`. This language server does **not** fully support JSON Schema Draft 2020-12.

**Symptoms:**
- No autocompletion suggestions when pressing Ctrl+Space
- No hover descriptions on property names
- No validation squiggles for invalid properties

**Root Cause:**
JSON Schema files served to the editor contain `$schema` and/or `$id` fields that are incompatible with the YAML Language Server:

- `"$schema": "https://json-schema.org/draft/2020-12/schema"` - Declares Draft 2020-12, which the LS does not fully support. Without this field, the LS defaults to Draft-07 which works correctly.
- `"$id": "https://mercari.com/..."` - External URI that the LS may attempt to resolve and fail.
- `$dynamicRef` / `$dynamicAnchor` - Draft 2020-12 features not supported at all.

**Solution:**
All JSON Schemas served to Monaco editors must be "flattened" before sending to the client. `SpecService.java` provides utilities for this:

- `prepareEditorSchema(JsonElement)` - Strips `$schema` and `$id` from config schemas (system, options, launch). Use this for schemas that don't contain `$ref`/`$defs`.
- `flattenSchemaFully(JsonObject)` / `flattenJsonSchema(JsonObject)` - Resolves `$ref`, inlines `$defs`, removes `$dynamicRef`/`$dynamicAnchor`/`$id`. Use this for module schemas that reference shared definitions.

**When adding new schemas for Monaco editors**, always strip or flatten before serving. Never pass raw JSON Schema 2020-12 files directly to `yamlApi.update()`.

### Modal Not Opening

1. Check modal ID in `index.jsp` matches the ID used in `app.js`
2. Verify event handler is bound in `initEventHandlers()`
3. Check browser console for errors

## Related Documentation

- [CLAUDE.md](../../../CLAUDE.md) - Project overview for AI assistants

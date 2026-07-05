# Frontend Architecture

This document describes the frontend architecture of Mercari Pipeline GUI Editor.

## Project Structure

```
src/main/webapp/
├── index.html                    # Main page (entry point, plain HTML — no JSP)
├── css/
│   └── index.css                 # Custom styles
└── js/                           # Native ES modules (no bundler / build step)
    ├── main.js                   # Entry point: loads spec, wires all modules together
    ├── util.js                   # DOM/HTTP helpers, escapeHtml/setStatus, schema & records rendering
    ├── monaco.js                 # Monaco editor management + JSON Schema cache/registration
    ├── canvas.js                 # Drawflow adapter + config generate/validate/import
    ├── result.js                 # Result modal + pipeline execution (dryrun/run/launch)
    ├── modals.js                 # Module config / System / Options / Config editor / Launch modals
    ├── agent.js                  # AI agent chat
    └── autosave.js               # localStorage workspace auto-save / restore
```

## Technology Stack

- Bootstrap 5.3.2 (UI components, modals, accordion)
- Bootstrap Icons 1.11.1 (icon library)
- Drawflow 0.0.60 (visual node editor)
- js-yaml 4.1.0 (YAML parsing/serialization)
- Monaco Editor 0.53.0 (YAML code editor, loaded lazily via ESM)
- monaco-yaml-inline 1.0.0 (YAML language server for Monaco, loaded lazily via ESM)

No framework and no jQuery: DOM access uses the native DOM API and HTTP uses `fetch`.
All CDN `<script>`/`<link>` tags from jsdelivr carry SRI (`integrity` + `crossorigin`)
attributes; when bumping a library version, recompute the hash
(`curl -s <url> | openssl dgst -sha384 -binary | openssl base64 -A`).

## Architecture

The application is split into native ES modules loaded via
`<script type="module" src="js/main.js">` — no bundler, npm, or build step.
State is private to each module; cross-module access goes through exported functions only.

Import graph (acyclic; `util.js` at the bottom, `main.js` at the top):

```
main.js ─→ canvas.js, monaco.js, modals.js, result.js, agent.js, autosave.js
modals.js ─→ util, monaco, canvas, result
result.js ─→ util, canvas
agent.js  ─→ util, canvas
autosave.js ─→ util, canvas
canvas.js ─→ util          (ALL Drawflow API access lives here — adapter)
monaco.js ─→ util
```

Two deliberate boundaries:

- **`canvas.js` is the Drawflow adapter.** No other module touches the Drawflow
  editor instance; they call exported functions (`getNodeData`, `updateNodeData`,
  `isNodeNameTaken`, `removeNode`, `generateConfig`, `importConfigToCanvas`, …).
  Replacing the node-editor library should touch this file only.
- **canvas → UI callbacks are injected.** `initDrawflow({ onEditNode, onShowSchema,
  onShowRecords })` receives the modal-opening functions from `main.js`, so
  `canvas.js` never imports UI modules and the graph stays acyclic.

## Initialization Flow

`main.js`:

```
init()
  → loadSpec()              // GET /api/spec (module summaries only)
  → initDrawflow(callbacks) // Initialize Drawflow editor (canvas.js)
  → initModuleList(defs)    // Render module items in left pane (canvas.js)
  → initRunButtons()        // Dry Run / Run buttons (result.js)
  → initModalEvents()       // All modal buttons + shown.bs.modal handlers (modals.js)
  → initAgent()             // Agent chat events (agent.js)
  → initResizeHandle()      // Left pane resizing (main.js)
  → loadMonaco()            // Pre-load Monaco (fire-and-forget)
  → initAutoSave()          // Restore saved workspace + start auto-saving (autosave.js)
```

## Workspace Auto-Save

`autosave.js` persists `{ config: generateConfig(), positions, savedAt }` to
`localStorage` (key `mercari-pipeline-workspace`) with a 1s debounce, and restores it
on page load, so a reload does not lose work. Change notifications come from
`canvas.js` (`setChangeListener`): Drawflow node/connection events, `updateNodeData`,
and `setSystemConfig`/`setOptionsConfig`. A corrupted or empty saved payload is
ignored (startup falls back to a blank canvas).

## Data Loading Strategy

The frontend uses a **lazy-loading** approach for schemas:

### Page Load (`GET /api/spec`)

Returns the module catalog taken from the agent-readable docs index
`src/main/resources/server/docs/module/index.yaml` (the `title` field there is the
registered module name; new modules must be added to that file to appear in the UI):

```json
{
  "modules": {
    "sources":    [{"name": "bigquery", "description": "...", "tags": ["source", "gcp"]}],
    "transforms": [...],
    "sinks":      [...]
  }
}
```

The left pane shows `name` as the label and `description` + `tags` as the tooltip.

### On-Demand Schema Loading

Schemas are fetched and cached when the corresponding modal opens for the first time:

| Endpoint | Trigger | Cache |
|----------|---------|-------|
| `GET /api/spec/{type}/{name}` | Module config modal opens | (not cached, fetched every time; a 404 is tolerated — the YAML editor then runs without completion for that module) |
| `GET /api/spec/system` | System modal opens | `schemaCache.system` |
| `GET /api/spec/options` | Options modal opens | `schemaCache.options` |
| `GET /api/spec/launch` | Launch modal opens | `schemaCache.launch` |

Lazy-load helper: `ensureSchema(kind)` with `kind` = `'system' | 'options' | 'launch'`.

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
| `pending` | Values handed from modal openers to their `shown.bs.modal` handlers (`moduleYaml`, `moduleType`, `moduleName`, `systemYaml`, `optionsYaml`) |
| `yamlApi` | `configureMonacoYaml` return value for schema updates |
| `schemaCache` | Lazy-loaded JSON Schemas keyed by kind (`system` / `options` / `launch`) |
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
| `ensureSchema(kind)` | Fetches and caches `/api/spec/{kind}` (system/options/launch, returns promise) |

### Canvas & Config

| Function | Description |
|----------|-------------|
| `initDrawflow()` | Initializes the Drawflow editor with event handlers |
| `addModuleToCanvas(moduleName, moduleType, config)` | Adds a module node to the canvas |
| `createNodeHtml(moduleName, moduleType, name)` | Generates HTML for a Drawflow node |
| `generateConfig()` | Generates full pipeline config YAML from canvas state |
| `getValidationErrors(config)` | Returns array of validation error strings |
| `importConfigToCanvas(config)` | Clears and rebuilds the canvas from a parsed config (used by the config editor's Apply and the agent's apply-config) |
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

Each module binds its own handlers in an `init*` function called from `main.js`
(`initRunButtons` in result.js, `initModalEvents` in modals.js, `initAgent` in agent.js),
via the `on(id, eventName, handler)` helper — a thin wrapper over `addEventListener`
(Bootstrap dispatches its modal events natively):

```javascript
// Header buttons
on('btn-dryrun', 'click', () => runPipeline('dryrun'))
on('btn-run', 'click', () => runPipeline('run'))
on('btn-launch', 'click', openLaunchModal)
on('btn-edit', 'click', openConfigEditor)

// Settings buttons
on('btn-system', 'click', openSystemModal)
on('btn-options', 'click', openOptionsModal)
on('btn-apply-system', 'click', applySystemConfig)
on('btn-apply-options', 'click', applyOptionsConfig)

// Module config modal
on('btn-save-module', 'click', saveModuleConfig)
on('btn-delete-module', 'click', deleteModule)

// Launch modal
on('launch-runner', 'change', onRunnerChanged)
on('launch-environment', 'change', onEnvironmentChanged)
on('btn-launch-execute', 'click', executeLaunch)

// Agent chat
on('btn-agent', 'click', openAgentModal)
on('btn-agent-send', 'click', agentSendMessage)
on('btn-agent-clear', 'click', agentClearHistory)

// Edit config modal
on('edit-format', 'change', updateConfigEditorContent)
on('btn-copy-config', 'click', copyConfigToClipboard)
on('btn-download-config', 'click', downloadConfig)
on('btn-apply-config', 'click', applyConfig)
on('btn-clear-config', 'click', clearConfigEditor)

// Monaco: modal shown handlers (schema loading + editor init)
on('moduleConfigModal', 'shown.bs.modal', ...)  // Fetch module schema + init editor
on('systemModal', 'shown.bs.modal', ...)        // ensureSchema('system') + init editor
on('optionsModal', 'shown.bs.modal', ...)       // ensureSchema('options') + init editor
on('editConfigModal', 'shown.bs.modal', ...)    // Init editor

// Drawflow events
editor.on('nodeRemoved', ...)
editor.on('connectionCreated', ...)
container.addEventListener('dblclick', handleDoubleClick)  // Opens module config modal
```

## HTML Structure (index.html)

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
#file-import (hidden file input), #btn-import-config
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

1. Add HTML modal structure in `index.html`:
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

2. Add functions and event handlers in `modals.js` (or the module that owns the feature):
```javascript
// In the appropriate section, add:
function openNewModal() {
    showModal('newModal');
}

// In the owning module's init*Events() function, bind:
on('btn-new', 'click', openNewModal);

// If Monaco editor is needed, add shown.bs.modal handler:
on('newModal', 'shown.bs.modal', function() {
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

3. Use the generic lazy-load helper in `monaco.js` — no new code needed:
```javascript
ensureSchema('new').then(function(schema) { ... });  // cached in schemaCache.new
```

## Caching

No manual cache busting is needed. `web.xml` overrides Jetty's `default` servlet with
`cacheControl=no-cache` + `etags=true`, so browsers revalidate every static resource
(HTML/CSS/JS modules) with a conditional request and receive `304 Not Modified` unless
the file changed. Note this override references
`org.eclipse.jetty.ee11.servlet.DefaultServlet` and is therefore Jetty-specific
(fine here: both the jib image and the maven plugin run Jetty 12 ee11).

## Development Commands

```bash
# Run the server locally against src/main/webapp (no packaging needed)
mvn -Pserver jetty:run -DskipTests

# Build WAR file
mvn clean package -Pserver -DskipTests

# Access UI
http://localhost:8080/
```

## Browser Compatibility

- Modern browsers (Chrome, Firefox, Safari, Edge)
- Requires JavaScript enabled
- Uses native ES modules, `fetch` + `AbortSignal.timeout`, and dynamic `import()`

## Known Limitations

- Single canvas/module space (Drawflow "Home")
- No undo/redo functionality
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

1. Check modal ID in `index.html` matches the ID used in the JS module
2. Verify event handler is bound in the owning module's `init*Events()` function
3. Check browser console for errors

## Related Documentation

- [CLAUDE.md](../../../CLAUDE.md) - Project overview for AI assistants

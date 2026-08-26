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
    ├── workspace.js              # Config store: the single source of truth (config + node positions)
    ├── monaco.js                 # Monaco editor management + JSON Schema cache/registration
    ├── canvas.js                 # Drawflow adapter + config generate/validate/import
    ├── result.js                 # Result modal + pipeline execution (dryrun/run/launch)
    ├── modals.js                 # Module config / System / Options / Launch modals
    ├── editor.js                 # Config text view (Monaco YAML/JSON) — the canvas's peer view
    ├── agent.js                  # AI agent pane (proposals, selection context, @mentions)
    ├── views.js                  # Canvas|Config tab switching + agent pane toggle
    ├── explorer.js               # Left pane: module catalog (upper) + pipeline outline (lower)
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

Import graph (acyclic; `util.js` / `workspace.js` at the bottom, `main.js` at the top):

```
main.js ─→ canvas.js, editor.js, views.js, explorer.js, modals.js, result.js, agent.js, autosave.js, workspace
explorer.js ─→ util, canvas, editor, modals, views, workspace
views.js ─→ util, canvas, editor   (announces `view-changed` on document; the explorer listens)
agent.js  ─→ util, canvas, views, workspace
modals.js ─→ util, monaco, canvas, result, workspace
editor.js ─→ util, monaco, workspace
result.js ─→ util, canvas, workspace
autosave.js ─→ util, workspace
canvas.js ─→ util, workspace   (ALL Drawflow API access lives here — adapter)
monaco.js ─→ util
workspace.js ─→ (nothing)      (config store — no DOM, no imports)
```

Three deliberate boundaries:

- **`workspace.js` is the single source of truth.** It holds the pipeline config
  (`system` / `options` / `sources` / `transforms` / `sinks` / `actions`) and the node
  positions sidecar, the agent's **pending proposal**, the **selection** (focused module)
  and the agent chat state, and notifies subscribers with `{ type, source }` on every
  mutation (`type`: `config` = module sections changed, `settings` = only
  system/options changed, `positions` = layout only, `pending` = proposal set / accepted /
  rejected, `selection`, `agent`). Views — the canvas, the config editor, the agent pane,
  auto-save — read from and write to the store; they never call each other to propagate a
  change. The canvas pushes its edits with `source: 'canvas'`
  (`setModules` / `setPositions`) and re-renders itself on any `config` event it did not
  originate, so store ↔ canvas syncing is loop-free. `getValidationErrors` lives here too.
- **`canvas.js` is the Drawflow adapter.** No other module touches the Drawflow
  editor instance; they call exported functions (`getNodeData`, `updateNodeData`,
  `isNodeNameTaken`, `removeNode`, `highlightNodeByName`, …). The canvas keeps no
  pipeline state of its own beyond Drawflow's node data — the config lives in the store.
  Module fields the canvas does not model (`description`, `args`, `outputType`,
  `outputFailure`, `failureSinks`, …) ride along in node data as `extra`
  (`extractExtraProps` / `NODE_CONFIG_PROPS`) and are written back by `exportModules`, so a
  canvas edit never drops what was written in the Config view; the module config modal
  shows them inline.
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
  → initModuleList(defs, onCatalogPick) // Render the catalog; clicks go to the explorer (canvas.js)
  → initRunButtons()        // Dry Run / Run buttons (result.js)
  → initModalEvents()       // All modal buttons + shown.bs.modal handlers (modals.js)
  → initAgent()             // Agent pane events + store subscription (agent.js)
  → initResizeHandle()      // Left pane resizing (main.js)
  → initEditor()            // Config text view: loads Monaco, subscribes to the store (editor.js)
  → initAutoSave()          // Restore saved workspace + start auto-saving (autosave.js)
  → initExplorer()          // Outline of the pipeline, subscribed to the store (explorer.js)
  → initViews()             // Canvas | Config tabs + agent pane toggle, remembered in localStorage (views.js)
```

## Center Pane Views: Canvas | Config

The center pane holds two views of the same store, switched by the tab bar
(`#tab-canvas` / `#tab-editor`, `showView()` in `views.js`; the choice is remembered in
`localStorage` key `mercari-pipeline-view`). Only one is visible; `setCanvasVisible` /
`setEditorVisible` tell each view whether to render.

- **Canvas** (`canvas.js`): Drawflow cannot lay out connections while its container is
  `display:none`, so store changes that arrive while hidden are deferred and rendered when
  the tab is shown.
- **Config** (`editor.js`): a Monaco YAML/JSON editor (`#config-editor`, model URI
  `internal://server/config-editor.yaml`) with Import / Copy / Download and a format switch
  in the tab bar. Text edits are parsed after a 500 ms pause and pushed to the store with
  `source: 'editor'` (skipped when identical to the store); unparseable or empty text is
  never pushed (the message shows in `#editor-status`, the store keeps the last good config —
  the header's Clear button is the way to empty a pipeline). `flushEditor()` pushes a pending
  edit immediately and returns whether the store now matches the text; it is called before
  Dry Run / Run / Launch (which are refused on `false`), before sending to the agent, before a
  catalog snippet insertion, and on the switch to the canvas tab (refused on `false`, so a
  broken text is never hidden behind a canvas showing the previous config). A config change
  from elsewhere while a push is pending discards those keystrokes (at most 500 ms) in favour
  of the change, with a status-bar note.
  **Round-trip protection:** the text is regenerated from the store only when someone else
  changed it (`stale`); while the store is unchanged since the editor's last push the text is
  kept verbatim, so comments and key order survive a visit to the canvas tab. A regeneration
  that drops comments shows a status-bar warning. `workspace.getValidationIssues()` is
  rendered as Monaco markers on the offending module's `name:` line.

## Explorer (left pane)

The left pane is an explorer with two stacked sections that look the same in both center
views; only what a click does differs (`explorer.js`):

| | Canvas view | Config view |
|---|---|---|
| **Catalog** (upper, `#source-modules` …; categories start collapsed) | add a node (`addModuleToCanvas`) | insert a module skeleton at the end of its section — `name`, `module`, required `parameters` from `/api/spec/{type}/{name}` (cached), `inputs: []`, `trigger: once` for actions; the section is created when missing, and an inline empty one (`sinks: []`) is converted to a block (`editor.insertModuleSnippet`, YAML only). Generated config text omits empty module sections |
| **Outline** (lower, `#outline`) — module row | `selectNodeByName`: flash the node + set the selection | `editor.jumpToModule`: cursor to its `- name:` line |
| Outline — `system` / `options` row | open the System / Options modal (the former left-pane buttons are gone) | `editor.jumpToSection`: cursor to the key, inserting `system:`/`options:` template when missing |

Outline rows show what the canvas cannot draw and per-module state: `system` / `options`
summaries (`args 2 · imports 1`, runner…; "not set" when absent), section counts, a red badge
with the module's validation issue count (`workspace.getValidationIssues`), pending-proposal
marks (`+` added as a ghost row / `~` modified / `−` removed, system/options tinted when they
change), `ignore: true` struck through, and the current selection highlighted. Pipeline-level
issues appear as a line under the settings rows.

## Agent Pane

The agent is the pipeline's co-author, so it lives in a persistent right pane
(`#agent-pane`, `agent.js`), not a modal. `#btn-agent` / Ctrl+L toggle it (state in
`localStorage` key `mercari-pipeline-agent-pane`; open by default), and
`#agent-resize-handle` resizes it.

- **Proposals.** A config returned by the agent is never applied directly: it becomes the
  store's pending proposal (`workspace.setPending`). The canvas marks the nodes it would
  modify / remove (`.node-pending-modified` / `.node-pending-removed`), the Config view
  swaps in a read-only Monaco diff (`#config-diff`, current -> proposed), and the pane's
  bar (`#agent-pending`) summarizes `workspace.getPendingDiff()` (added / modified /
  removed modules, system/options) with chips that flash the node. **Accept** commits
  (`acceptPending`, with an Undo of the previous config), **Reject** discards. Older
  messages keep a "Propose this config" badge to re-propose.
- **Selection context.** `workspace.selection` is set by clicking a canvas node
  (Drawflow `nodeSelected`), by the editor cursor (nearest enclosing `- name:` block), or by
  an `@module` mention; the pane shows it as a chip and sends it as `selection` in the
  `/api/agent` body, which `PipelineAgent` appends to the prompt context.
- **@mentions.** Typing `@` in the input lists `workspace.getModuleNames()`; Up/Down +
  Enter/Tab insert the name.
- **Persistence.** The chat (`history`, `conversationId`) is stored via
  `workspace.setAgentState` and included in the auto-save payload, so it survives a reload.

## Workspace Auto-Save

`autosave.js` persists the store — `{ config, positions, agent, savedAt }` — to
`localStorage` (key `mercari-pipeline-workspace`) with a 1s debounce, and restores it
on page load (`workspace.setConfig(config, 'restore', positions)`), so a reload does
not lose work. It simply subscribes to the store, so every mutation from any view
(canvas edits, editor/agent apply, system/options) is covered. A corrupted or empty
saved payload is ignored (startup falls back to a blank canvas).

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
    "sinks":      [...],
    "actions":    [{"name": "bigquery", "description": "...", "tags": ["action", "bigquery"]}]
  }
}
```

`actions` lists the action services (config section `actions`, `module:` = service name); the UI
shows them as a separate *Actions* sidebar group and an `action` node type, and exports them
under the `actions` key.

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

Two settings in `createOrGetEditor` exist for **paste** (Ctrl/Cmd+V), which otherwise does nothing
in Monaco 0.53 here while typing works: (1) the standalone keybinding service, which listens on the
editor's container, resolves Ctrl+V to a command and calls `preventDefault`, cancelling the browser's
native paste — a `keydown` listener on the editor's DOM node stops the key from reaching the container;
(2) `editContext: false` keeps the classic textarea input, since the EditContext input still did not
paste with (1) alone. Verified in a real Chrome (puppeteer/CDP key events cannot trigger native paste,
so the headless scripts do not cover it). The Config view's editor is also created on first display
(`ensureEditor`), not at startup.

### Schema Registration

The `yamlApi.update({ schemas: [...] })` call registers JSON Schemas with the YAML Language Server. Each schema entry has:

- `uri` - Unique identifier for the schema
- `fileMatch` - Array of model URIs this schema applies to (e.g., `['internal://server/module-yaml-editor.yaml']`)
- `schema` - The JSON Schema object

`buildStaticSchemas()` returns the system and options schemas (if cached) plus a top-level pipeline
schema for the config view (`buildPipelineSchema()`: section structure + module common fields, with
the cached system/options schemas embedded — module `parameters` are left open). Module-specific
schemas are fetched on demand and pushed to the array.

### Editor Model URIs

Each Monaco editor model has a URI derived from its container ID:

```
internal://server/{containerId}.yaml
```

Examples:
- `internal://server/module-yaml-editor.yaml`
- `internal://server/system-yaml-editor.yaml`
- `internal://server/options-yaml-editor.yaml`
- `internal://server/config-editor.yaml` (the Config view)

## State Variables

| Variable | Description |
|----------|-------------|
| `editor` | Drawflow editor instance |
| `moduleDefs` | Module definitions `{ sources: [], transforms: [], sinks: [], actions: [] }` (`actions` = action services, shown as their own sidebar group / `action` node type; exported under the `actions` config section) |
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
| `revealLine(containerId, line, column?)` / `insertText(containerId, line, column, text)` | Cursor placement / undoable insertion in an existing editor (explorer jumps + snippets) |
| `getEditorValue(containerId)` | Returns current editor content |

### Schema Helpers

| Function | Description |
|----------|-------------|
| `buildStaticSchemas()` | Builds schema array from cached system/options schemas |
| `ensureSchema(kind)` | Fetches and caches `/api/spec/{kind}` (system/options/launch, returns promise) |

### Workspace store (`workspace.js`)

| Function | Description |
|----------|-------------|
| `getConfig()` | The current pipeline config object (system / options / module sections) |
| `setConfig(config, source, positions?)` | Replace the whole config (editor Apply, agent apply, restore, clear); emits `config` |
| `setModules(sections, source, positions?)` | Replace only the module sections, keeping system / options (what the canvas pushes); emits `config` |
| `getSystem()` / `setSystem(system)` / `getOptions()` / `setOptions(options)` | System / options block accessors; emit `settings` |
| `getPositions()` / `setPositions(positions, source)` | Node positions sidecar (by module name); emits `positions` |
| `subscribe(listener)` | `listener({ type, source })` on every mutation; returns an unsubscribe function |
| `getValidationErrors(config?)` | Structural checks (inputs present, at least one source/action); array of messages |
| `hasModules(config?)` | True when any module section is non-empty |
| `getPending()` / `setPending(config)` / `acceptPending()` / `rejectPending()` | The agent's pending proposal; emits `pending` (accept also emits `config`) |
| `getPendingDiff()` | `{ added, removed, modified, settingsChanged }` of the proposal vs the config |
| `getSelection()` / `setSelection(name)` | Focused module (agent context); emits `selection` |
| `getModuleNames(config?)` | Every module name in the config |
| `getAgentState()` / `setAgentState(state)` | Chat history + conversationId; emits `agent` |

### Canvas

| Function | Description |
|----------|-------------|
| `initDrawflow(callbacks)` | Initializes the Drawflow editor, wires its events to the store and subscribes to store changes |
| `addModuleToCanvas(moduleName, moduleType, config)` | Adds a module node to the canvas (Drawflow's `nodeCreated` pushes it to the store) |
| `createNodeHtml(moduleName, moduleType, name)` | Generates HTML for a Drawflow node |
| `runPipeline(type)` | Executes pipeline (dryrun/run) via `/api/pipeline` (result.js; reads `getConfig()`) |

### Modals

| Function | Description |
|----------|-------------|
| `openModuleConfig(nodeId)` | Opens module config modal for a node |
| `saveModuleConfig()` | Saves module config from modal to canvas node |
| `openSystemModal()` / `openOptionsModal()` | Property editors for `system` / `options` (opened from the outline in the Canvas view) |
| `openLaunchModal()` | Opens launch modal (lazy-loads launch schema, populates runners) |
| `showLaunchParametersForm(runnerSchema, envSchema)` | Generates HTML form fields from launch schema properties |
| `executeLaunch()` | Collects form values and sends launch request |
| `showResult(title, content, type)` | Shows generic result modal |
| `showPipelineResult(type, result)` | Shows structured pipeline result |

### Agent Pane

| Function | Description |
|----------|-------------|
| `agentSend(input)` | Sends a user message (+ config YAML + selection) to `/api/agent` |
| `agentPropose(configText)` | Parses an agent config and sets it as the store's pending proposal |
| `agentAccept()` / `agentReject()` / `agentUndoAccept()` | Pending bar actions |
| `showView(view)` / `toggleAgentPane()` (views.js) | Center view switch / pane toggle |

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

// System / Options modals (opened from the explorer's outline rows)
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
on('btn-agent', 'click', toggleAgentPane)        // views.js (also Ctrl+L)
on('btn-agent-send', 'click', agentSendFromInput)
on('btn-agent-clear', 'click', agentClearHistory)
on('btn-agent-accept', 'click', agentAccept)
on('btn-agent-reject', 'click', agentReject)

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

// Drawflow events
editor.on('nodeRemoved', ...)
editor.on('connectionCreated', ...)
container.addEventListener('dblclick', handleDoubleClick)  // Opens module config modal
```

## HTML Structure (index.html)

### Main Layout IDs

```html
<!-- Header -->
#btn-agent, #btn-dryrun, #btn-run, #btn-launch, #btn-workspace-clear

<!-- Left Pane (explorer) -->
#left-pane
.explorer-catalog: #collapse-sources, #collapse-transforms, #collapse-sinks, #collapse-actions
                   #source-modules, #transform-modules, #sink-modules, #action-modules
.explorer-outline: #outline (.outline-section / .outline-row[data-key])

<!-- Center Pane -->
#tab-canvas, #tab-editor (view tabs)
#editor-toolbar: #editor-format, #file-import, #btn-import-config, #btn-copy-config, #btn-download-config
#view-canvas > #drawflow (Drawflow container)
#view-editor > #config-editor (Monaco container), #config-diff (proposal diff), #editor-status (parse problems)

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

<!-- Result Modal -->
#resultModal
#result-modal-header (add class: success|error)
#result-icon, #result-title
#result-success-content, #result-millis, #schemaAccordion
#result-error-content, #error-module-name, #error-module-type, #error-messages, #error-millis
#result-content (generic pre element)

<!-- Agent Pane (right; not a modal) -->
#agent-pane, #agent-resize-handle
#agent-chat-messages, #agent-chat-input, #agent-mention (completion popup)
#agent-pending: #agent-pending-summary, #agent-pending-modules, #btn-agent-accept, #btn-agent-reject
#agent-context, #agent-context-name, #btn-agent-context-clear
#btn-agent-send, #btn-agent-clear, #btn-agent-undo, #btn-agent-close

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

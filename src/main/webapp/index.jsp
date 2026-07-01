<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mercari Pipeline</title>
    <!-- Bootstrap 5 CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/font/bootstrap-icons.css" rel="stylesheet">
    <!-- Drawflow CSS -->
    <link href="https://cdn.jsdelivr.net/npm/drawflow@0.0.60/dist/drawflow.min.css" rel="stylesheet">
    <!-- Monaco Editor CSS -->
    <link rel="stylesheet" href="https://esm.sh/monaco-editor@0.53.0/es2022/monaco-editor.css">
    <!-- Custom CSS -->
    <link href="css/index.css?id=17" rel="stylesheet">
</head>
<body>
<!-- Header -->
<header class="navbar navbar-dark bg-dark fixed-top">
    <div class="container-fluid">
            <span class="navbar-brand mb-0 h1">
                <i class="bi bi-diagram-3"></i> Mercari Pipeline
            </span>
        <div class="d-flex gap-2">
            <button id="btn-agent" class="btn btn-outline-success btn-sm" title="Chat with AI Agent">
                <i class="bi bi-robot"></i> Agent
            </button>
            <div class="vr mx-1"></div>
            <button id="btn-dryrun" class="btn btn-outline-info btn-sm" title="Dry run the pipeline">
                <i class="bi bi-check-circle"></i> Dry Run
            </button>
            <button id="btn-run" class="btn btn-outline-warning btn-sm" title="Run the pipeline locally">
                <i class="bi bi-play-fill"></i> Run
            </button>
            <button id="btn-launch" class="btn btn-primary btn-sm" title="Launch on Dataflow">
                <i class="bi bi-rocket-takeoff"></i> Launch
            </button>
            <div class="vr mx-2"></div>
            <button id="btn-edit" class="btn btn-outline-light btn-sm" title="Edit configuration as YAML/JSON">
                <i class="bi bi-pencil-square"></i> Edit
            </button>
        </div>
    </div>
</header>

<!-- Main Content -->
<div class="main-container">
    <!-- Left Pane - Module Menu -->
    <aside id="left-pane" class="left-pane">
        <!-- Settings Buttons -->
        <div class="settings-buttons d-flex gap-2 mb-3">
            <button id="btn-system" class="btn btn-outline-secondary btn-sm flex-fill">
                <i class="bi bi-gear me-1"></i> System
            </button>
            <button id="btn-options" class="btn btn-outline-secondary btn-sm flex-fill">
                <i class="bi bi-sliders me-1"></i> Options
            </button>
        </div>

        <!-- Module Categories - Collapsible -->
        <div class="module-categories mt-3">
            <div class="module-category">
                <h6 class="module-category-header text-muted px-2 mb-2" data-bs-toggle="collapse" data-bs-target="#collapse-sources" role="button">
                    <i class="bi bi-box-arrow-in-right me-1"></i> Sources
                    <i class="bi bi-chevron-down float-end collapse-icon"></i>
                </h6>
                <div id="collapse-sources" class="collapse show">
                    <div id="source-modules" class="module-list">
                    </div>
                </div>
            </div>

            <div class="module-category mt-3">
                <h6 class="module-category-header text-muted px-2 mb-2" data-bs-toggle="collapse" data-bs-target="#collapse-transforms" role="button">
                    <i class="bi bi-arrow-left-right me-1"></i> Transforms
                    <i class="bi bi-chevron-down float-end collapse-icon"></i>
                </h6>
                <div id="collapse-transforms" class="collapse show">
                    <div id="transform-modules" class="module-list">
                    </div>
                </div>
            </div>

            <div class="module-category mt-3">
                <h6 class="module-category-header text-muted px-2 mb-2" data-bs-toggle="collapse" data-bs-target="#collapse-sinks" role="button">
                    <i class="bi bi-box-arrow-right me-1"></i> Sinks
                    <i class="bi bi-chevron-down float-end collapse-icon"></i>
                </h6>
                <div id="collapse-sinks" class="collapse show">
                    <div id="sink-modules" class="module-list">
                    </div>
                </div>
            </div>
        </div>
    </aside>

    <!-- Resize Handle -->
    <div id="resize-handle" class="resize-handle"></div>

    <!-- Right Pane - Pipeline Graph Editor -->
    <main id="right-pane" class="right-pane">
        <div id="drawflow" class="drawflow-container"></div>
    </main>
</div>

<!-- Module Configuration Modal -->
<div class="modal fade" id="moduleConfigModal" tabindex="-1" aria-labelledby="moduleConfigModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="moduleConfigModalLabel">
                    <span id="modal-module-type" class="badge me-2">Source</span>
                    <span id="modal-module-name">Module Configuration</span>
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <div class="mb-3">
                    <label for="module-name-input" class="form-label">Name <span class="text-danger">*</span></label>
                    <input type="text" class="form-control" id="module-name-input" required pattern="^[a-zA-Z][a-zA-Z0-9_]*$"
                           title="Name must start with a letter and contain only letters, numbers, and underscores">
                    <div class="form-text">Unique identifier for this module (alphanumeric and underscore only)</div>
                </div>
                <div class="mb-3">
                    <label class="form-label">Configuration (YAML)</label>
                    <div id="module-yaml-editor" class="monaco-container monaco-lg"></div>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-danger" id="btn-delete-module">
                    <i class="bi bi-trash"></i> Delete
                </button>
                <button type="button" class="btn btn-primary" id="btn-save-module">
                    <i class="bi bi-check-lg"></i> Save
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Result Modal -->
<div class="modal fade" id="resultModal" tabindex="-1" aria-labelledby="resultModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg">
        <div class="modal-content">
            <div class="modal-header" id="result-modal-header">
                <h5 class="modal-title" id="resultModalLabel">
                    <i id="result-icon" class="bi me-2"></i>
                    <span id="result-title">Result</span>
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <!-- Success content -->
                <div id="result-success-content" style="display: none;">
                    <div class="d-flex align-items-center mb-3">
                        <span class="badge bg-success me-2">Success</span>
                        <span id="result-millis" class="text-muted small"></span>
                    </div>
                    <p class="mb-2">Output schemas for each module:</p>
                    <div class="accordion" id="schemaAccordion">
                    </div>
                </div>
                <!-- Error content -->
                <div id="result-error-content" style="display: none;">
                    <div class="alert alert-danger" role="alert">
                        <h6 class="alert-heading">
                            <i class="bi bi-exclamation-triangle-fill me-2"></i>
                            Pipeline Error
                        </h6>
                        <hr>
                        <div class="mb-2">
                            <strong>Module:</strong> <span id="error-module-name"></span>
                            <span id="error-module-type" class="badge ms-2"></span>
                        </div>
                        <div class="mb-2">
                            <strong>Error Messages:</strong>
                            <ul id="error-messages" class="mb-0 mt-1"></ul>
                        </div>
                    </div>
                    <div class="text-muted small" id="error-millis"></div>
                </div>
                <!-- Generic content (for validation, etc.) -->
                <pre id="result-content" class="result-output" style="display: none;"></pre>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
</div>

<!-- Edit Configuration Modal -->
<div class="modal fade" id="editConfigModal" tabindex="-1" aria-labelledby="editConfigModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-xl">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="editConfigModalLabel">
                    <i class="bi bi-pencil-square me-2"></i> Edit Pipeline Configuration
                </h5>
                <button type="button" class="btn btn-outline-danger btn-sm ms-auto me-2" id="btn-clear-config">
                    <i class="bi bi-trash"></i> Clear
                </button>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <div class="mb-3">
                    <label for="edit-format" class="form-label">Format</label>
                    <select id="edit-format" class="form-select form-select-sm" style="width: auto; display: inline-block;">
                        <option value="yaml">YAML</option>
                        <option value="json">JSON</option>
                    </select>
                </div>
                <div class="mb-3">
                    <div id="edit-content" class="monaco-container monaco-xl"></div>
                    <div class="form-text">Edit the pipeline configuration directly. Changes will be applied to the visual editor when saved.</div>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-outline-secondary" id="btn-copy-config">
                    <i class="bi bi-clipboard"></i> Copy
                </button>
                <button type="button" class="btn btn-outline-secondary" id="btn-download-config">
                    <i class="bi bi-download"></i> Download
                </button>
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-primary" id="btn-apply-config">
                    <i class="bi bi-check-lg"></i> Apply
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Launch Configuration Modal -->
<div class="modal fade" id="launchModal" tabindex="-1" aria-labelledby="launchModalLabel" aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="launchModalLabel">
                    <i class="bi bi-rocket-takeoff me-2"></i> Launch Pipeline
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <!-- Runner Select -->
                <div class="mb-3">
                    <label for="launch-runner" class="form-label">Runner <span class="text-danger">*</span></label>
                    <select id="launch-runner" class="form-select" required>
                        <option value="">-- Select Runner --</option>
                    </select>
                    <div id="launch-runner-desc" class="form-text"></div>
                </div>
                <!-- Args (JSON) -->
                <div class="mb-3">
                    <label for="launch-args" class="form-label">Args (JSON)</label>
                    <textarea id="launch-args" class="form-control code-input" rows="3" placeholder='{"key": "value"}'></textarea>
                    <div class="form-text">Additional arguments as JSON object</div>
                </div>
                <!-- Environment Select -->
                <div id="launch-environment-group" class="mb-3" style="display: none;">
                    <label for="launch-environment" class="form-label">Environment <span class="text-danger">*</span></label>
                    <select id="launch-environment" class="form-select" required>
                        <option value="">-- Select Environment --</option>
                    </select>
                    <div id="launch-environment-desc" class="form-text"></div>
                </div>
                <!-- Parameters (form fields) -->
                <div id="launch-parameters-container" class="mb-3" style="display: none;">
                    <label class="form-label fw-bold">Parameters</label>
                    <div id="launch-parameters-fields"></div>
                </div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-primary" id="btn-launch-execute">
                    <i class="bi bi-rocket-takeoff"></i> Launch
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Hidden file input for import -->
<input type="file" id="file-import" accept=".yaml,.yml,.json" style="display: none;">

<!-- System Settings Modal -->
<div class="modal fade" id="systemModal" tabindex="-1" aria-labelledby="systemModalLabel" aria-hidden="true">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="systemModalLabel">
                    <i class="bi bi-gear me-2"></i> System Settings
                    <i id="system-help-icon" class="bi bi-question-circle text-muted ms-2" style="cursor: help; font-size: 0.85em;"></i>
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <div id="system-yaml-editor" class="monaco-container monaco-md"></div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-primary" id="btn-apply-system">
                    <i class="bi bi-check-lg"></i> Apply
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Options Settings Modal -->
<div class="modal fade" id="optionsModal" tabindex="-1" aria-labelledby="optionsModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title" id="optionsModalLabel">
                    <i class="bi bi-sliders me-2"></i> Pipeline Options
                    <i id="options-help-icon" class="bi bi-question-circle text-muted ms-2" style="cursor: help; font-size: 0.85em;"></i>
                </h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body">
                <div id="options-yaml-editor" class="monaco-container monaco-md"></div>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                <button type="button" class="btn btn-primary" id="btn-apply-options">
                    <i class="bi bi-check-lg"></i> Apply
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Agent Chat Modal -->
<div class="modal fade" id="agentModal" tabindex="-1" aria-labelledby="agentModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable" style="height: 80vh;">
        <div class="modal-content" style="height: 80vh;">
            <div class="modal-header py-2">
                <h5 class="modal-title" id="agentModalLabel">
                    <i class="bi bi-robot me-2"></i> Pipeline Builder Agent
                </h5>
                <div class="ms-auto me-3">
                    <button type="button" class="btn btn-outline-secondary btn-sm" id="btn-agent-clear" title="Clear chat history">
                        <i class="bi bi-trash"></i> Clear
                    </button>
                </div>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            <div class="modal-body p-0 d-flex flex-column" style="overflow: hidden;">
                <!-- Messages area -->
                <div id="agent-chat-messages" class="agent-chat-messages flex-grow-1">
                    <div class="agent-welcome text-center text-muted py-5">
                        <i class="bi bi-robot" style="font-size: 3rem;"></i>
                        <p class="mt-3">How can I help you build your pipeline?</p>
                        <p class="small">Describe what data you want to process and I'll generate the configuration.</p>
                    </div>
                </div>
                <!-- Input area -->
                <div class="agent-chat-input-area border-top p-3">
                    <div class="input-group">
                        <textarea id="agent-chat-input" class="form-control" rows="2"
                                  placeholder="Describe your pipeline..." style="resize: none;"></textarea>
                        <button id="btn-agent-send" class="btn btn-success" type="button" title="Send message">
                            <i class="bi bi-send"></i>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>

<!-- Footer -->
<footer class="footer">
    <div class="container-fluid d-flex justify-content-between align-items-center">
        <span class="text-muted small">Mercari Pipeline GUI Editor</span>
        <span id="status-message" class="text-muted small"></span>
    </div>
</footer>

<!-- Scripts -->
<script src="https://cdn.jsdelivr.net/npm/jquery@4.0.0/dist/jquery.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/drawflow@0.0.60/dist/drawflow.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/js-yaml@4.1.0/dist/js-yaml.min.js"></script>
<script src="js/app.js?id=24"></script>
</body>
</html>

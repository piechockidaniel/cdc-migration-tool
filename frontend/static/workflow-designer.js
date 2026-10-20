// Visual Workflow Designer
// Node-based graph editor for building data synchronization pipelines

const WorkflowDesigner = (function() {
    'use strict';
    
    // State
    let jsPlumbInstance = null;
    let currentWorkflow = null;
    let nodes = {};
    let edges = [];
    let nodeCounter = 0;
    let selectedNode = null;
    let zoomLevel = 1.0;
    let isDragging = false;
    let draggedNodeType = null;
    let connectionConfigs = {
        source: null,
        destination: null
    };
    let databasesCache = {
        source: [],
        destination: []
    };
    let tablesCache = {
        source: {},
        destination: {}
    };
    let columnsCache = {};
    
    // Node type definitions
    const NODE_TYPES = {
        // Control Flow
        start: {
            label: 'Start',
            icon: 'bi-play-circle',
            color: 'success',
            category: 'control',
            inputs: 0,
            outputs: 1,
            config: {}
        },
        stop: {
            label: 'Stop',
            icon: 'bi-stop-circle',
            color: 'danger',
            category: 'control',
            inputs: 1,
            outputs: 0,
            config: {}
        },
        
        // Sources
        source_database: {
            label: 'Source Database',
            icon: 'bi-server',
            color: 'primary',
            category: 'source',
            inputs: 0,
            outputs: 1,
            config: {
                server: '',
                database: '',
                port: null,
                use_windows_auth: true,
                username: '',
                password: ''
            }
        },
        source_table: {
            label: 'Source Table',
            icon: 'bi-table',
            color: 'primary',
            category: 'source',
            inputs: 1,
            outputs: 1,
            config: {
                schema: '',
                table: '',
                columns: []
            }
        },
        source_columns: {
            label: 'Column Selector',
            icon: 'bi-list-columns',
            color: 'primary',
            category: 'source',
            inputs: 1,
            outputs: 1,
            config: {
                selected_columns: [],
                exclude_columns: []
            }
        },
        
        // Transformations
        transform_json: {
            label: 'JSON Aggregation',
            icon: 'bi-braces',
            color: 'warning',
            category: 'transform',
            inputs: 1,
            outputs: 1,
            config: {
                source_columns: [],
                destination_column: '',
                json_structure: 'object'
            }
        },
        transform_concat: {
            label: 'Concatenation',
            icon: 'bi-link-45deg',
            color: 'warning',
            category: 'transform',
            inputs: 1,
            outputs: 1,
            config: {
                source_columns: [],
                destination_column: '',
                separator: ', '
            }
        },
        transform_sql: {
            label: 'Custom SQL',
            icon: 'bi-code-square',
            color: 'warning',
            category: 'transform',
            inputs: 1,
            outputs: 1,
            config: {
                expression: '',
                source_columns: [],
                destination_column: ''
            }
        },
        transform_duckdb: {
            label: 'DuckDB Transform',
            icon: 'bi-database-gear',
            color: 'warning',
            category: 'transform',
            inputs: 1,
            outputs: 1,
            config: {
                script_name: '',
                script_content: ''
            }
        },
        transform_filter: {
            label: 'Filter',
            icon: 'bi-funnel',
            color: 'warning',
            category: 'transform',
            inputs: 1,
            outputs: 1,
            config: {
                condition: '',
                filter_type: 'where'
            }
        },
        transform_dedupe: {
            label: 'Deduplicate',
            icon: 'bi-card-list',
            color: 'warning',
            category: 'transform',
            inputs: 1,
            outputs: 1,
            config: {
                unique_columns: [],
                keep_strategy: 'first'
            }
        },
        
        // Destinations
        dest_database: {
            label: 'Dest Database',
            icon: 'bi-server',
            color: 'info',
            category: 'destination',
            inputs: 1,
            outputs: 1,
            config: {
                server: '',
                database: '',
                port: null,
                use_windows_auth: true,
                username: '',
                password: ''
            }
        },
        dest_table: {
            label: 'Dest Table',
            icon: 'bi-table',
            color: 'info',
            category: 'destination',
            inputs: 1,
            outputs: 1,
            config: {
                schema: '',
                table: ''
            }
        },
        dest_mapper: {
            label: 'Column Mapper',
            icon: 'bi-arrow-left-right',
            color: 'info',
            category: 'destination',
            inputs: 1,
            outputs: 0,
            config: {
                mappings: []
            }
        },
        
        // Operators
        op_logger: {
            label: 'Logger',
            icon: 'bi-journal-text',
            color: 'secondary',
            category: 'operator',
            inputs: 1,
            outputs: 1,
            config: {
                log_level: 'info',
                message: '',
                log_data: true
            }
        },
        op_notifier: {
            label: 'Notifier',
            icon: 'bi-bell',
            color: 'secondary',
            category: 'operator',
            inputs: 1,
            outputs: 1,
            config: {
                notification_type: 'email',
                recipients: '',
                message_template: ''
            }
        },
        op_condition: {
            label: 'Condition',
            icon: 'bi-question-circle',
            color: 'secondary',
            category: 'operator',
            inputs: 1,
            outputs: 2,  // True and False branches
            config: {
                condition: '',
                condition_type: 'sql'
            }
        },
        op_fork: {
            label: 'Fork',
            icon: 'bi-diagram-3',
            color: 'secondary',
            category: 'operator',
            inputs: 1,
            outputs: 3,
            config: {
                fork_type: 'duplicate'
            }
        },
        op_join: {
            label: 'Join',
            icon: 'bi-diagram-2',
            color: 'secondary',
            category: 'operator',
            inputs: 2,
            outputs: 1,
            config: {
                join_type: 'merge',
                join_key: ''
            }
        },
        
        // Events
        event_error: {
            label: 'On Error',
            icon: 'bi-exclamation-triangle',
            color: 'danger',
            category: 'event',
            inputs: 1,
            outputs: 1,
            config: {
                action: 'log',
                retry_count: 0
            }
        },
        event_success: {
            label: 'On Success',
            icon: 'bi-check-circle',
            color: 'success',
            category: 'event',
            inputs: 1,
            outputs: 1,
            config: {
                action: 'continue',
                notification: false
            }
        },
        event_change: {
            label: 'On Change',
            icon: 'bi-arrow-repeat',
            color: 'info',
            category: 'event',
            inputs: 1,
            outputs: 1,
            config: {
                detect_fields: [],
                action: 'sync'
            }
        }
    };
    
    // Initialize jsPlumb
    function initJsPlumb() {
        const { ready, newInstance } = jsPlumbBrowserUI;
        
        ready(() => {
            const container = document.getElementById('workflow-canvas');
            
            jsPlumbInstance = newInstance({
                container: container,
                connector: {
                    type: "Flowchart",
                    options: { gap: 5, cornerRadius: 5, stub: 20 }
                },
                endpoint: {
                    type: "Dot",
                    options: { radius: 6 }
                },
                anchor: ["Left", "Right"],
                connectionOverlays: [
                    {
                        type: "Arrow",
                        options: { location: 1, width: 10, length: 10 }
                    }
                ],
                paintStyle: { stroke: "#6c757d", strokeWidth: 2 },
                hoverPaintStyle: { stroke: "#0d6efd", strokeWidth: 3 },
                endpointStyle: { fill: "#6c757d" },
                endpointHoverStyle: { fill: "#0d6efd" }
            });
            
            // Bind connection events
            jsPlumbInstance.bind("connection", onConnection);
            jsPlumbInstance.bind("connectionDetached", onConnectionDetached);
            jsPlumbInstance.bind("click", onConnectionClick);
            
            console.log('jsPlumb initialized successfully');
        });
    }
    
    // Initialize designer when tab is shown
    async function initDesigner() {
        if (!jsPlumbInstance) {
            initJsPlumb();
        }
        setupDragAndDrop();
        setupCanvasEvents();
        await loadConnectionConfigs();
    }
    
    // Load connection configurations from active workset
    async function loadConnectionConfigs() {
        try {
            const response = await fetch('/api/admin/workset/active');
            const workset = await response.json();
            
            if (workset && workset.source_connection && workset.destination_connection) {
                connectionConfigs.source = workset.source_connection;
                connectionConfigs.destination = workset.destination_connection;
                console.log('Loaded connection configs from active workset');
            } else {
                console.warn('No active workset found. Connection configs not available.');
            }
        } catch (error) {
            console.error('Error loading connection configs:', error);
        }
    }
    
    // Load databases for a connection type
    async function loadDatabases(connectionType) {
        const cacheKey = connectionType === 'source' ? 'source' : 'destination';
        
        if (databasesCache[cacheKey].length > 0) {
            return databasesCache[cacheKey];
        }
        
        try {
            const response = await fetch(`/api/admin/scan/databases?connection_type=${connectionType}`);
            const databases = await response.json();
            
            if (databases && Array.isArray(databases)) {
                databasesCache[cacheKey] = databases;
                return databases;
            }
        } catch (error) {
            console.error(`Error loading ${connectionType} databases:`, error);
        }
        
        return [];
    }
    
    // Load tables for a connection type and database
    async function loadTables(connectionType, database) {
        const cacheKey = connectionType === 'source' ? 'source' : 'destination';
        const fullKey = `${database}`;
        
        if (tablesCache[cacheKey][fullKey]) {
            return tablesCache[cacheKey][fullKey];
        }
        
        try {
            const response = await fetch(`/api/admin/scan/tables?connection_type=${connectionType}&database=${encodeURIComponent(database)}`);
            const tables = await response.json();
            
            if (tables && Array.isArray(tables)) {
                tablesCache[cacheKey][fullKey] = tables;
                return tables;
            }
        } catch (error) {
            console.error(`Error loading ${connectionType} tables:`, error);
        }
        
        return [];
    }
    
    // Load columns for a connection type, schema, and table
    async function loadColumns(connectionType, schema, table) {
        const cacheKey = `${connectionType}_${schema}_${table}`;
        
        if (columnsCache[cacheKey]) {
            return columnsCache[cacheKey];
        }
        
        try {
            const response = await fetch(`/api/admin/scan/columns?connection_type=${connectionType}&schema_name=${encodeURIComponent(schema)}&table_name=${encodeURIComponent(table)}`);
            const columns = await response.json();
            
            if (columns && Array.isArray(columns)) {
                columnsCache[cacheKey] = columns;
                return columns;
            }
        } catch (error) {
            console.error(`Error loading columns:`, error);
        }
        
        return [];
    }
    
    // Setup drag and drop from palette
    function setupDragAndDrop() {
        const paletteItems = document.querySelectorAll('.node-palette-item');
        const canvas = document.getElementById('workflow-canvas');
        
        paletteItems.forEach(item => {
            item.addEventListener('dragstart', (e) => {
                draggedNodeType = e.target.getAttribute('data-node-type');
                e.dataTransfer.effectAllowed = 'copy';
                e.dataTransfer.setData('text/plain', draggedNodeType);
            });
            
            item.addEventListener('dragend', () => {
                draggedNodeType = null;
            });
        });
        
        canvas.addEventListener('dragover', (e) => {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'copy';
        });
        
        canvas.addEventListener('drop', (e) => {
            e.preventDefault();
            if (draggedNodeType) {
                const rect = canvas.getBoundingClientRect();
                const x = e.clientX - rect.left;
                const y = e.clientY - rect.top;
                addNode(draggedNodeType, x, y);
            }
        });
    }
    
    // Setup canvas events (click, pan, etc.)
    function setupCanvasEvents() {
        const canvas = document.getElementById('workflow-canvas');
        
        // Click on canvas (deselect nodes)
        canvas.addEventListener('click', (e) => {
            if (e.target === canvas) {
                deselectAllNodes();
            }
        });
        
        // Remove welcome message when first node is added
        canvas.addEventListener('DOMNodeInserted', () => {
            const infoDiv = document.getElementById('workflow-info');
            if (Object.keys(nodes).length > 0 && infoDiv) {
                infoDiv.style.display = 'none';
            }
        });
    }
    
    // Add node to canvas
    function addNode(type, x, y) {
        const nodeId = `node_${++nodeCounter}`;
        const nodeType = NODE_TYPES[type];
        
        if (!nodeType) {
            console.error('Unknown node type:', type);
            return;
        }
        
        // Create node element
        const nodeEl = document.createElement('div');
        nodeEl.id = nodeId;
        nodeEl.className = `workflow-node type-${type.replace(/_/g, '-')}`;
        nodeEl.style.left = `${x}px`;
        nodeEl.style.top = `${y}px`;
        
        nodeEl.innerHTML = `
            <div class="node-header">
                <span class="node-icon"><i class="bi ${nodeType.icon}"></i></span>
                <span class="node-title">${nodeType.label}</span>
                <button class="node-delete" onclick="WorkflowDesigner.deleteNode('${nodeId}')">
                    <i class="bi bi-x-lg"></i>
                </button>
            </div>
            <div class="node-body">
                <small class="text-muted">Click to configure</small>
            </div>
        `;
        
        // Add to canvas
        const canvas = document.getElementById('workflow-canvas');
        canvas.appendChild(nodeEl);
        
        // Store node data
        nodes[nodeId] = {
            id: nodeId,
            type: type,
            position: { x, y },
            config: JSON.parse(JSON.stringify(nodeType.config)),  // Deep copy
            label: nodeType.label
        };
        
        // Make node draggable
        jsPlumbInstance.draggable(nodeEl, {
            containment: true,
            grid: [10, 10],
            stop: (params) => {
                nodes[nodeId].position = {
                    x: params.pos[0],
                    y: params.pos[1]
                };
            }
        });
        
        // Add click handler
        nodeEl.addEventListener('click', (e) => {
            e.stopPropagation();
            selectNode(nodeId);
        });
        
        // Add connection endpoints
        addNodeEndpoints(nodeEl, nodeId, nodeType);
        
        console.log('Added node:', nodeId, type);
        return nodeId;
    }
    
    // Add input/output endpoints to node
    function addNodeEndpoints(nodeEl, nodeId, nodeType) {
        // Add input ports
        for (let i = 0; i < nodeType.inputs; i++) {
            jsPlumbInstance.addEndpoint(nodeEl, {
                anchor: "Left",
                source: false,
                target: true,
                maxConnections: -1,
                cssClass: "node-port input"
            });
        }
        
        // Add output ports
        for (let i = 0; i < nodeType.outputs; i++) {
            jsPlumbInstance.addEndpoint(nodeEl, {
                anchor: "Right",
                source: true,
                target: false,
                maxConnections: -1,
                cssClass: "node-port output"
            });
        }
    }
    
    // Select node and show properties
    function selectNode(nodeId) {
        deselectAllNodes();
        
        const nodeEl = document.getElementById(nodeId);
        if (nodeEl) {
            nodeEl.classList.add('selected');
            selectedNode = nodeId;
            showNodeProperties(nodeId);
        }
    }
    
    // Deselect all nodes
    function deselectAllNodes() {
        document.querySelectorAll('.workflow-node').forEach(node => {
            node.classList.remove('selected');
        });
        selectedNode = null;
    }
    
    // Show node properties panel
    async function showNodeProperties(nodeId) {
        const node = nodes[nodeId];
        if (!node) return;
        
        const nodeType = NODE_TYPES[node.type];
        const panel = document.getElementById('properties-content');
        
        let html = `
            <div class="node-config-form">
                <div class="property-group">
                    <div class="property-label">Node Type</div>
                    <div class="property-value">
                        <span class="badge bg-${nodeType.color}">${nodeType.label}</span>
                    </div>
                </div>
                
                <div class="property-group">
                    <label class="property-label">Label</label>
                    <input type="text" class="form-control form-control-sm" 
                           value="${node.label}" 
                           onchange="WorkflowDesigner.updateNodeLabel('${nodeId}', this.value)">
                </div>
        `;
        
        // Add type-specific configuration fields (async)
        html += await getNodeConfigForm(nodeId, node.type, node.config);
        
        html += '</div>';
        panel.innerHTML = html;
    }
    
    // Find upstream database node
    function findUpstreamDatabaseNode(nodeId, connectionType) {
        const node = nodes[nodeId];
        if (!node) return null;
        
        // Check if this node itself is a database node
        if ((connectionType === 'source' && node.type === 'source_database') ||
            (connectionType === 'destination' && node.type === 'dest_database')) {
            return node;
        }
        
        // Find upstream database node through edges
        const upstreamEdge = edges.find(e => e.target === nodeId);
        if (upstreamEdge) {
            const upstreamNode = nodes[upstreamEdge.source];
            if (upstreamNode) {
                if ((connectionType === 'source' && upstreamNode.type === 'source_database') ||
                    (connectionType === 'destination' && upstreamNode.type === 'dest_database')) {
                    return upstreamNode;
                }
                // Recursively search upstream
                return findUpstreamDatabaseNode(upstreamNode.id, connectionType);
            }
        }
        
        return null;
    }
    
    // Find upstream table node
    function findUpstreamTableNode(nodeId) {
        const node = nodes[nodeId];
        if (!node) return null;
        
        // Check if this node itself is a table node
        if (node.type === 'source_table' || node.type === 'dest_table') {
            return node;
        }
        
        // Find upstream table node through edges
        const upstreamEdge = edges.find(e => e.target === nodeId);
        if (upstreamEdge) {
            const upstreamNode = nodes[upstreamEdge.source];
            if (upstreamNode) {
                if (upstreamNode.type === 'source_table' || upstreamNode.type === 'dest_table') {
                    return upstreamNode;
                }
                // Recursively search upstream
                return findUpstreamTableNode(upstreamNode.id);
            }
        }
        
        return null;
    }
    
    // Generate configuration form for node type
    async function getNodeConfigForm(nodeId, nodeType, config) {
        let html = '';
        
        switch(nodeType) {
            case 'source_database':
                {
                    const connConfig = connectionConfigs.source;
                    const serverName = connConfig ? `${connConfig.server}:${connConfig.port || null}` : 'Not configured';
                    const databases = await loadDatabases('source');
                    
                    html += `
                        <div class="property-group">
                            <label class="property-label">Server</label>
                            <input type="text" class="form-control form-control-sm" 
                                   value="${serverName}" 
                                   readonly
                                   style="background-color: #e9ecef; cursor: not-allowed;"
                                   title="Server name is taken from active working set configuration">
                            <small class="text-muted">From active working set (read-only)</small>
                        </div>
                        <div class="property-group">
                            <label class="property-label">Database</label>
                            <select class="form-select form-select-sm" 
                                    id="db-select-${nodeId}"
                                    onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'database', this.value); WorkflowDesigner.loadTablesForNode('${nodeId}', 'source', this.value)">
                                <option value="">Select database...</option>
                                ${databases.map(db => `
                                    <option value="${db.name}" ${config.database === db.name ? 'selected' : ''}>
                                        ${db.name}${db.cdc_enabled ? ' (CDC)' : ''}
                                    </option>
                                `).join('')}
                            </select>
                        </div>
                    `;
                    
                    // Store connection config in node
                    if (connConfig) {
                        nodes[nodeId].config.server = connConfig.server;
                        nodes[nodeId].config.port = connConfig.port || null;
                        nodes[nodeId].config.use_windows_auth = connConfig.use_windows_auth;
                        nodes[nodeId].config.username = connConfig.username || '';
                        nodes[nodeId].config.password = connConfig.password || '';
                    }
                }
                break;
                
            case 'dest_database':
                {
                    const connConfig = connectionConfigs.destination;
                    const serverName = connConfig ? `${connConfig.server}:${connConfig.port || null}` : 'Not configured';
                    const databases = await loadDatabases('destination');
                    
                    html += `
                        <div class="property-group">
                            <label class="property-label">Server</label>
                            <input type="text" class="form-control form-control-sm" 
                                   value="${serverName}" 
                                   readonly
                                   style="background-color: #e9ecef; cursor: not-allowed;"
                                   title="Server name is taken from active working set configuration">
                            <small class="text-muted">From active working set (read-only)</small>
                        </div>
                        <div class="property-group">
                            <label class="property-label">Database</label>
                            <select class="form-select form-select-sm" 
                                    id="db-select-${nodeId}"
                                    onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'database', this.value); WorkflowDesigner.loadTablesForNode('${nodeId}', 'destination', this.value)">
                                <option value="">Select database...</option>
                                ${databases.map(db => `
                                    <option value="${db.name}" ${config.database === db.name ? 'selected' : ''}>
                                        ${db.name}${db.cdc_enabled ? ' (CDC)' : ''}
                                    </option>
                                `).join('')}
                            </select>
                        </div>
                    `;
                    
                    // Store connection config in node
                    if (connConfig) {
                        nodes[nodeId].config.server = connConfig.server;
                        nodes[nodeId].config.port = connConfig.port || null;
                        nodes[nodeId].config.use_windows_auth = connConfig.use_windows_auth;
                        nodes[nodeId].config.username = connConfig.username || '';
                        nodes[nodeId].config.password = connConfig.password || '';
                    }
                }
                break;
                
            case 'source_table':
                {
                    const connectionType = 'source';
                    const dbNode = findUpstreamDatabaseNode(nodeId, connectionType);
                    const database = dbNode ? dbNode.config.database : config.database || '';
                    
                    if (database) {
                        const tables = await loadTables(connectionType, database);
                        const schemas = [...new Set(tables.map(t => t.schema_name))];
                        const selectedSchema = config.schema || '';
                        const filteredTables = selectedSchema ? tables.filter(t => t.schema_name === selectedSchema) : [];
                        
                        html += `
                            <div class="property-group">
                                <label class="property-label">Database</label>
                                <input type="text" class="form-control form-control-sm" 
                                       value="${database}" 
                                       readonly
                                       style="background-color: #e9ecef; cursor: not-allowed;"
                                       title="Database from upstream Source Database node">
                            </div>
                            <div class="property-group">
                                <label class="property-label">Schema</label>
                                <select class="form-select form-select-sm" 
                                        id="schema-select-${nodeId}"
                                        onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'schema', this.value); WorkflowDesigner.loadTablesForNode('${nodeId}', '${connectionType}', '${database}')">
                                    <option value="">Select schema...</option>
                                    ${schemas.map(schema => `
                                        <option value="${schema}" ${selectedSchema === schema ? 'selected' : ''}>${schema}</option>
                                    `).join('')}
                                </select>
                            </div>
                            <div class="property-group">
                                <label class="property-label">Table</label>
                                <select class="form-select form-select-sm" 
                                        id="table-select-${nodeId}"
                                        onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'table', this.value); WorkflowDesigner.loadColumnsForNode('${nodeId}', '${connectionType}')">
                                    <option value="">Select table...</option>
                                    ${filteredTables.map(table => `
                                        <option value="${table.table_name}" ${config.table === table.table_name ? 'selected' : ''}>
                                            ${table.table_name}${table.row_count !== null ? ` (${table.row_count.toLocaleString()} rows)` : ''}
                                        </option>
                                    `).join('')}
                                </select>
                            </div>
                        `;
                    } else {
                        html += `
                            <div class="alert alert-warning alert-sm">
                                <small>Connect this node to a Source Database node first</small>
                            </div>
                        `;
                    }
                }
                break;
                
            case 'dest_table':
                {
                    const connectionType = 'destination';
                    const dbNode = findUpstreamDatabaseNode(nodeId, connectionType);
                    const database = dbNode ? dbNode.config.database : config.database || '';
                    
                    if (database) {
                        const tables = await loadTables(connectionType, database);
                        const schemas = [...new Set(tables.map(t => t.schema_name))];
                        const selectedSchema = config.schema || '';
                        const filteredTables = selectedSchema ? tables.filter(t => t.schema_name === selectedSchema) : [];
                        
                        html += `
                            <div class="property-group">
                                <label class="property-label">Database</label>
                                <input type="text" class="form-control form-control-sm" 
                                       value="${database}" 
                                       readonly
                                       style="background-color: #e9ecef; cursor: not-allowed;"
                                       title="Database from upstream Destination Database node">
                            </div>
                            <div class="property-group">
                                <label class="property-label">Schema</label>
                                <select class="form-select form-select-sm" 
                                        id="schema-select-${nodeId}"
                                        onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'schema', this.value); WorkflowDesigner.loadTablesForNode('${nodeId}', '${connectionType}', '${database}')">
                                    <option value="">Select schema...</option>
                                    ${schemas.map(schema => `
                                        <option value="${schema}" ${selectedSchema === schema ? 'selected' : ''}>${schema}</option>
                                    `).join('')}
                                </select>
                            </div>
                            <div class="property-group">
                                <label class="property-label">Table</label>
                                <select class="form-select form-select-sm" 
                                        id="table-select-${nodeId}"
                                        onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'table', this.value)">
                                    <option value="">Select table...</option>
                                    ${filteredTables.map(table => `
                                        <option value="${table.table_name}" ${config.table === table.table_name ? 'selected' : ''}>
                                            ${table.table_name}${table.row_count !== null ? ` (${table.row_count.toLocaleString()} rows)` : ''}
                                        </option>
                                    `).join('')}
                                </select>
                            </div>
                        `;
                    } else {
                        html += `
                            <div class="alert alert-warning alert-sm">
                                <small>Connect this node to a Destination Database node first</small>
                            </div>
                        `;
                    }
                }
                break;
                
            case 'source_columns':
                {
                    const tableNode = findUpstreamTableNode(nodeId);
                    if (tableNode && tableNode.config.schema && tableNode.config.table) {
                        const connectionType = tableNode.type === 'source_table' ? 'source' : 'destination';
                        const columns = await loadColumns(connectionType, tableNode.config.schema, tableNode.config.table);
                        const selectedCols = config.selected_columns || [];
                        
                        html += `
                            <div class="property-group">
                                <label class="property-label">Source Table</label>
                                <input type="text" class="form-control form-control-sm" 
                                       value="${tableNode.config.schema}.${tableNode.config.table}" 
                                       readonly
                                       style="background-color: #e9ecef; cursor: not-allowed;">
                            </div>
                            <div class="property-group">
                                <label class="property-label">Select Columns</label>
                                <div style="max-height: 200px; overflow-y: auto; border: 1px solid #dee2e6; border-radius: 0.25rem; padding: 0.5rem;">
                                    ${columns.map(col => `
                                        <div class="form-check">
                                            <input class="form-check-input" type="checkbox" 
                                                   id="col-${nodeId}-${col.column_name}"
                                                   value="${col.column_name}"
                                                   ${selectedCols.includes(col.column_name) ? 'checked' : ''}
                                                   onchange="WorkflowDesigner.updateColumnSelection('${nodeId}', '${col.column_name}', this.checked)">
                                            <label class="form-check-label small" for="col-${nodeId}-${col.column_name}">
                                                ${col.column_name}
                                                <span class="text-muted">(${col.data_type}${col.is_primary_key ? ', PK' : ''})</span>
                                            </label>
                                        </div>
                                    `).join('')}
                                </div>
                            </div>
                        `;
                    } else {
                        html += `
                            <div class="alert alert-warning alert-sm">
                                <small>Connect this node to a Source Table node first</small>
                            </div>
                        `;
                    }
                }
                break;
                
            case 'transform_json':
                {
                    const tableNode = findUpstreamTableNode(nodeId);
                    if (tableNode && tableNode.config.schema && tableNode.config.table) {
                        const connectionType = tableNode.type === 'source_table' ? 'source' : 'destination';
                        const columns = await loadColumns(connectionType, tableNode.config.schema, tableNode.config.table);
                        const selectedCols = config.source_columns || [];
                        
                        html += `
                            <div class="property-group">
                                <label class="property-label">Source Columns</label>
                                <div style="max-height: 150px; overflow-y: auto; border: 1px solid #dee2e6; border-radius: 0.25rem; padding: 0.5rem;">
                                    ${columns.map(col => `
                                        <div class="form-check">
                                            <input class="form-check-input" type="checkbox" 
                                                   id="json-col-${nodeId}-${col.column_name}"
                                                   value="${col.column_name}"
                                                   ${selectedCols.includes(col.column_name) ? 'checked' : ''}
                                                   onchange="WorkflowDesigner.updateColumnSelectionForTransform('${nodeId}', 'source_columns', '${col.column_name}', this.checked)">
                                            <label class="form-check-label small" for="json-col-${nodeId}-${col.column_name}">
                                                ${col.column_name}
                                            </label>
                                        </div>
                                    `).join('')}
                                </div>
                            </div>
                            <div class="property-group">
                                <label class="property-label">Destination Column</label>
                                <input type="text" class="form-control form-control-sm" 
                                       value="${config.destination_column || ''}" 
                                       onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'destination_column', this.value)">
                            </div>
                        `;
                    } else {
                        html += `
                            <div class="alert alert-warning alert-sm">
                                <small>Connect this node to a Source Table node first</small>
                            </div>
                        `;
                    }
                }
                break;
                
            case 'transform_concat':
                html += `
                    <div class="property-group">
                        <label class="property-label">Source Columns</label>
                        <small class="text-muted d-block mb-1">Comma-separated</small>
                        <input type="text" class="form-control form-control-sm" 
                               value="${(config.source_columns || []).join(', ')}" 
                               onchange="WorkflowDesigner.updateNodeConfigArray('${nodeId}', 'source_columns', this.value)">
                    </div>
                    <div class="property-group">
                        <label class="property-label">Separator</label>
                        <input type="text" class="form-control form-control-sm" 
                               value="${config.separator || ', '}" 
                               onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'separator', this.value)">
                    </div>
                    <div class="property-group">
                        <label class="property-label">Destination Column</label>
                        <input type="text" class="form-control form-control-sm" 
                               value="${config.destination_column || ''}" 
                               onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'destination_column', this.value)">
                    </div>
                `;
                break;
                
            case 'transform_sql':
                html += `
                    <div class="property-group">
                        <label class="property-label">SQL Expression</label>
                        <textarea class="form-control form-control-sm font-monospace" rows="3"
                                  onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'expression', this.value)">${config.expression || ''}</textarea>
                    </div>
                    <div class="property-group">
                        <label class="property-label">Source Columns</label>
                        <input type="text" class="form-control form-control-sm" 
                               value="${(config.source_columns || []).join(', ')}" 
                               onchange="WorkflowDesigner.updateNodeConfigArray('${nodeId}', 'source_columns', this.value)">
                    </div>
                    <div class="property-group">
                        <label class="property-label">Destination Column</label>
                        <input type="text" class="form-control form-control-sm" 
                               value="${config.destination_column || ''}" 
                               onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'destination_column', this.value)">
                    </div>
                `;
                break;
                
            case 'transform_duckdb':
                html += `
                    <div class="property-group">
                        <label class="property-label">Script</label>
                        <select class="form-select form-select-sm mb-2"
                                onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'script_name', this.value)">
                            <option value="">Select script...</option>
                            <option value="__inline__">Inline Script</option>
                        </select>
                        <textarea class="form-control form-control-sm font-monospace" rows="4"
                                  placeholder="Enter DuckDB SQL..."
                                  onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'script_content', this.value)">${config.script_content || ''}</textarea>
                    </div>
                `;
                break;
                
            case 'transform_filter':
                html += `
                    <div class="property-group">
                        <label class="property-label">Filter Condition</label>
                        <textarea class="form-control form-control-sm font-monospace" rows="2"
                                  placeholder="e.g., status = 'ACTIVE'"
                                  onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'condition', this.value)">${config.condition || ''}</textarea>
                    </div>
                `;
                break;
                
            case 'transform_dedupe':
                html += `
                    <div class="property-group">
                        <label class="property-label">Unique Columns</label>
                        <input type="text" class="form-control form-control-sm" 
                               value="${(config.unique_columns || []).join(', ')}" 
                               placeholder="id, customer_id"
                               onchange="WorkflowDesigner.updateNodeConfigArray('${nodeId}', 'unique_columns', this.value)">
                    </div>
                    <div class="property-group">
                        <label class="property-label">Keep Strategy</label>
                        <select class="form-select form-select-sm"
                                onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'keep_strategy', this.value)">
                            <option value="first" ${config.keep_strategy === 'first' ? 'selected' : ''}>First</option>
                            <option value="last" ${config.keep_strategy === 'last' ? 'selected' : ''}>Last</option>
                        </select>
                    </div>
                `;
                break;
                
            case 'op_logger':
                html += `
                    <div class="property-group">
                        <label class="property-label">Log Level</label>
                        <select class="form-select form-select-sm"
                                onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'log_level', this.value)">
                            <option value="debug" ${config.log_level === 'debug' ? 'selected' : ''}>Debug</option>
                            <option value="info" ${config.log_level === 'info' ? 'selected' : ''}>Info</option>
                            <option value="warning" ${config.log_level === 'warning' ? 'selected' : ''}>Warning</option>
                            <option value="error" ${config.log_level === 'error' ? 'selected' : ''}>Error</option>
                        </select>
                    </div>
                    <div class="property-group">
                        <label class="property-label">Message</label>
                        <input type="text" class="form-control form-control-sm" 
                               value="${config.message || ''}" 
                               placeholder="Log message"
                               onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'message', this.value)">
                    </div>
                `;
                break;
                
            case 'op_notifier':
                html += `
                    <div class="property-group">
                        <label class="property-label">Type</label>
                        <select class="form-select form-select-sm"
                                onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'notification_type', this.value)">
                            <option value="email" ${config.notification_type === 'email' ? 'selected' : ''}>Email</option>
                            <option value="webhook" ${config.notification_type === 'webhook' ? 'selected' : ''}>Webhook</option>
                            <option value="slack" ${config.notification_type === 'slack' ? 'selected' : ''}>Slack</option>
                        </select>
                    </div>
                    <div class="property-group">
                        <label class="property-label">Recipients</label>
                        <input type="text" class="form-control form-control-sm" 
                               value="${config.recipients || ''}" 
                               placeholder="email@example.com"
                               onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'recipients', this.value)">
                    </div>
                `;
                break;
                
            case 'op_condition':
                html += `
                    <div class="property-group">
                        <label class="property-label">Condition</label>
                        <textarea class="form-control form-control-sm font-monospace" rows="2"
                                  placeholder="e.g., amount > 1000"
                                  onchange="WorkflowDesigner.updateNodeConfig('${nodeId}', 'condition', this.value)">${config.condition || ''}</textarea>
                        <small class="text-muted">True path: top output, False path: bottom output</small>
                    </div>
                `;
                break;
        }
        
        return html;
    }
    
    // Connection event handlers
    function onConnection(info) {
        const edgeId = `edge_${edges.length + 1}`;
        edges.push({
            id: edgeId,
            source: info.sourceId,
            target: info.targetId
        });
        console.log('Connection created:', edgeId);
    }
    
    function onConnectionDetached(info) {
        edges = edges.filter(edge => 
            !(edge.source === info.sourceId && edge.target === info.targetId)
        );
        console.log('Connection removed');
    }
    
    function onConnectionClick(connection, e) {
        if (confirm('Delete this connection?')) {
            jsPlumbInstance.deleteConnection(connection);
        }
    }
    
    // Public API
    return {
        init: initDesigner,
        
        // Node operations
        addNode: addNode,
        deleteNode: function(nodeId) {
            if (confirm('Delete this node?')) {
                const nodeEl = document.getElementById(nodeId);
                if (nodeEl) {
                    jsPlumbInstance.remove(nodeEl);
                    delete nodes[nodeId];
                    edges = edges.filter(e => e.source !== nodeId && e.target !== nodeId);
                    deselectAllNodes();
                    document.getElementById('properties-content').innerHTML = `
                        <i class="bi bi-gear fs-3"></i>
                        <p class="small mt-3 text-muted">Select a node to edit its properties</p>
                    `;
                }
            }
        },
        
        selectNode: selectNode,
        
        updateNodeLabel: function(nodeId, label) {
            if (nodes[nodeId]) {
                nodes[nodeId].label = label;
                const nodeEl = document.getElementById(nodeId);
                if (nodeEl) {
                    nodeEl.querySelector('.node-title').textContent = label;
                }
            }
        },
        
        updateNodeConfig: function(nodeId, key, value) {
            if (nodes[nodeId]) {
                nodes[nodeId].config[key] = value;
                updateNodeDisplay(nodeId);
            }
        },
        
        updateNodeConfigArray: function(nodeId, key, value) {
            if (nodes[nodeId]) {
                const array = value.split(',').map(s => s.trim()).filter(s => s);
                nodes[nodeId].config[key] = array;
                updateNodeDisplay(nodeId);
            }
        },
        
        updateColumnSelection: function(nodeId, columnName, checked) {
            if (nodes[nodeId]) {
                if (!nodes[nodeId].config.selected_columns) {
                    nodes[nodeId].config.selected_columns = [];
                }
                
                if (checked) {
                    if (!nodes[nodeId].config.selected_columns.includes(columnName)) {
                        nodes[nodeId].config.selected_columns.push(columnName);
                    }
                } else {
                    nodes[nodeId].config.selected_columns = nodes[nodeId].config.selected_columns.filter(c => c !== columnName);
                }
                
                updateNodeDisplay(nodeId);
            }
        },
        
        loadTablesForNode: async function(nodeId, connectionType, database) {
            const node = nodes[nodeId];
            if (!node || !database) return;
            
            const tables = await loadTables(connectionType, database);
            const schemaSelect = document.getElementById(`schema-select-${nodeId}`);
            const tableSelect = document.getElementById(`table-select-${nodeId}`);
            
            if (!schemaSelect || !tableSelect) return;
            
            const selectedSchema = schemaSelect.value;
            const filteredTables = selectedSchema ? tables.filter(t => t.schema_name === selectedSchema) : [];
            
            // Update table dropdown
            tableSelect.innerHTML = '<option value="">Select table...</option>';
            filteredTables.forEach(table => {
                const option = document.createElement('option');
                option.value = table.table_name;
                option.textContent = `${table.table_name}${table.row_count !== null ? ` (${table.row_count.toLocaleString()} rows)` : ''}`;
                if (node.config.table === table.table_name) {
                    option.selected = true;
                }
                tableSelect.appendChild(option);
            });
        },
        
        loadColumnsForNode: async function(nodeId, connectionType) {
            const node = nodes[nodeId];
            if (!node) return;
            
            const tableNode = findUpstreamTableNode(nodeId);
            if (!tableNode || !tableNode.config.schema || !tableNode.config.table) return;
            
            // Reload properties panel to refresh column list
            await showNodeProperties(nodeId);
        },
        
        // Workflow operations
        getWorkflowData: function() {
            return {
                nodes: Object.values(nodes),
                edges: edges
            };
        },
        
        loadWorkflowData: function(data) {
            clearWorkflow();
            if (data.nodes) {
                data.nodes.forEach(node => {
                    addNode(node.type, node.position.x, node.position.y);
                    const nodeId = Object.keys(nodes)[Object.keys(nodes).length - 1];
                    nodes[nodeId] = node;
                });
            }
            if (data.edges) {
                data.edges.forEach(edge => {
                    connectNodes(edge.source, edge.target);
                });
            }
        },
        
        clearAll: function() {
            if (confirm('Clear entire workflow? This cannot be undone.')) {
                clearWorkflow();
            }
        }
    };
    
    // Helper: Update node display based on config
    function updateNodeDisplay(nodeId) {
        const node = nodes[nodeId];
        if (!node) return;
        
        const nodeEl = document.getElementById(nodeId);
        if (!nodeEl) return;
        
        const bodyEl = nodeEl.querySelector('.node-body');
        let summary = '';
        
        switch(node.type) {
            case 'source_database':
            case 'dest_database':
                summary = node.config.server ? 
                    `<small>${node.config.server}/${node.config.database || '?'}</small>` :
                    '<small class="text-muted">Not configured</small>';
                break;
                
            case 'source_table':
            case 'dest_table':
                summary = node.config.table ? 
                    `<small>${node.config.schema || 'dbo'}.${node.config.table}</small>` :
                    '<small class="text-muted">Not configured</small>';
                break;
                
            case 'transform_json':
            case 'transform_concat':
                const cols = node.config.source_columns || [];
                summary = cols.length > 0 ?
                    `<small>${cols.length} column(s) → ${node.config.destination_column || '?'}</small>` :
                    '<small class="text-muted">No columns selected</small>';
                break;
                
            case 'transform_filter':
                summary = node.config.condition ?
                    `<small class="text-truncate">WHERE ${node.config.condition}</small>` :
                    '<small class="text-muted">No condition</small>';
                break;
                
            default:
                summary = '<small class="text-muted">Configured</small>';
        }
        
        bodyEl.innerHTML = summary;
    }
    
    // Helper: Connect two nodes
    function connectNodes(sourceId, targetId) {
        const sourceEl = document.getElementById(sourceId);
        const targetEl = document.getElementById(targetId);
        
        if (sourceEl && targetEl && jsPlumbInstance) {
            jsPlumbInstance.connect({
                source: sourceEl,
                target: targetEl
            });
        }
    }
    
    // Helper: Clear workflow
    function clearWorkflow() {
        if (jsPlumbInstance) {
            jsPlumbInstance.deleteEveryConnection();
            jsPlumbInstance.deleteEveryEndpoint();
        }
        
        Object.keys(nodes).forEach(nodeId => {
            const nodeEl = document.getElementById(nodeId);
            if (nodeEl) {
                nodeEl.remove();
            }
        });
        
        nodes = {};
        edges = [];
        nodeCounter = 0;
        selectedNode = null;
        currentWorkflow = null;
        
        document.getElementById('properties-content').innerHTML = `
            <i class="bi bi-gear fs-3"></i>
            <p class="small mt-3 text-muted">Select a node to edit its properties</p>
        `;
        
        const infoDiv = document.getElementById('workflow-info');
        if (infoDiv) {
            infoDiv.style.display = 'block';
        }
    }
})();

// Initialize when designer tab is shown
document.addEventListener('DOMContentLoaded', () => {
    const designerTab = document.getElementById('designer-tab');
    if (designerTab) {
        designerTab.addEventListener('shown.bs.tab', () => {
            WorkflowDesigner.init();
        });
    }
});

// Global functions for toolbar buttons
function createNewWorkflow() {
    if (Object.keys(WorkflowDesigner.getWorkflowData().nodes).length > 0) {
        if (!confirm('Create new workflow? Current workflow will be cleared.')) {
            return;
        }
    }
    WorkflowDesigner.clearAll();
    currentWorkflowId = null;
    currentWorkflowName = null;
    showAlert('success', 'New workflow created. Start by dragging nodes from the toolbox.');
}

async function saveWorkflow() {
    const workflowData = WorkflowDesigner.getWorkflowData();
    
    if (workflowData.nodes.length === 0) {
        showAlert('warning', 'Cannot save empty workflow');
        return;
    }
    
    // Prompt for name if new workflow
    let workflowId = currentWorkflowId;
    let workflowName = currentWorkflowName;
    
    if (!workflowId) {
        workflowName = prompt('Enter workflow name:');
        if (!workflowName) return;
        
        workflowId = 'workflow_' + workflowName.toLowerCase().replace(/[^a-z0-9]/g, '_') + '_' + Date.now();
    }
    
    const workflow = {
        id: workflowId,
        name: workflowName || workflowId,
        description: '',
        nodes: workflowData.nodes,
        edges: workflowData.edges
    };
    
    try {
        const response = await fetch('/api/admin/workflow/visual/create', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(workflow)
        });
        
        const result = await response.json();
        
        if (result.success) {
            currentWorkflowId = workflowId;
            currentWorkflowName = workflowName;
            showAlert('success', `Workflow '${workflowName}' saved successfully!`);
        } else {
            showAlert('danger', result.message || 'Failed to save workflow');
        }
    } catch (error) {
        showAlert('danger', `Error saving workflow: ${error.message}`);
    }
}

async function loadWorkflowList() {
    try {
        const response = await fetch('/api/admin/workflow/visual/list');
        const workflows = await response.json();
        
        if (workflows.length === 0) {
            showAlert('info', 'No saved workflows found');
            return;
        }
        
        // Show selection modal (simplified version)
        const selection = prompt('Available workflows:\n' + 
            workflows.map((w, i) => `${i+1}. ${w.name} (${w.nodes.length} nodes)`).join('\n') +
            '\n\nEnter number to load:');
        
        if (selection) {
            const index = parseInt(selection) - 1;
            if (index >= 0 && index < workflows.length) {
                await loadWorkflow(workflows[index].id);
            }
        }
    } catch (error) {
        showAlert('danger', `Error loading workflows: ${error.message}`);
    }
}

async function loadWorkflow(workflowId) {
    try {
        const response = await fetch(`/api/admin/workflow/visual/${workflowId}`);
        const workflow = await response.json();
        
        WorkflowDesigner.loadWorkflowData(workflow);
        currentWorkflowId = workflow.id;
        currentWorkflowName = workflow.name;
        
        showAlert('success', `Workflow '${workflow.name}' loaded successfully!`);
    } catch (error) {
        showAlert('danger', `Error loading workflow: ${error.message}`);
    }
}

async function validateWorkflow() {
    const workflowData = WorkflowDesigner.getWorkflowData();
    
    if (workflowData.nodes.length === 0) {
        showAlert('warning', 'Workflow is empty');
        return;
    }
    
    // Basic validation
    const hasStart = workflowData.nodes.some(n => n.type === 'start');
    const hasEnd = workflowData.nodes.some(n => n.type === 'stop' || n.type.startsWith('dest_'));
    
    if (!hasStart) {
        showAlert('warning', 'Workflow must have a Start node');
        return;
    }
    
    if (!hasEnd) {
        showAlert('warning', 'Workflow must have a Stop node or Destination');
        return;
    }
    
    // Check for disconnected nodes
    const connectedNodes = new Set();
    workflowData.edges.forEach(edge => {
        connectedNodes.add(edge.source);
        connectedNodes.add(edge.target);
    });
    
    const disconnectedNodes = workflowData.nodes.filter(n => 
        !connectedNodes.has(n.id) && n.type !== 'start'
    );
    
    if (disconnectedNodes.length > 0) {
        showAlert('warning', `Found ${disconnectedNodes.length} disconnected node(s)`);
        return;
    }
    
    showAlert('success', 'Workflow validation passed!');
}

async function compileWorkflow() {
    const workflowData = WorkflowDesigner.getWorkflowData();
    
    if (workflowData.nodes.length === 0) {
        showAlert('warning', 'Cannot compile empty workflow');
        return;
    }
    
    if (!currentWorkflowId) {
        showAlert('warning', 'Please save the workflow first');
        return;
    }
    
    try {
        const response = await fetch(`/api/admin/workflow/compile/${currentWorkflowId}`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', `Workflow compiled to mapping: ${result.mapping_id}`);
        } else {
            showAlert('danger', result.message || 'Failed to compile workflow');
        }
    } catch (error) {
        showAlert('danger', `Error compiling workflow: ${error.message}`);
    }
}

function zoomIn() {
    zoomLevel = Math.min(zoomLevel + 0.1, 2.0);
    applyZoom();
}

function zoomOut() {
    zoomLevel = Math.max(zoomLevel - 0.1, 0.5);
    applyZoom();
}

function fitToScreen() {
    zoomLevel = 1.0;
    applyZoom();
}

function applyZoom() {
    const canvas = document.getElementById('workflow-canvas');
    if (canvas) {
        canvas.style.transform = `scale(${zoomLevel})`;
        canvas.style.transformOrigin = '0 0';
    }
}

function clearWorkflow() {
    WorkflowDesigner.clearAll();
}

// Global state
let currentWorkflowId = null;
let currentWorkflowName = null;


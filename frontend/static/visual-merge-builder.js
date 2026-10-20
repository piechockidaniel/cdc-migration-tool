// Visual Merge Builder - Node-based drag-and-drop interface for multi-source mappings
// Uses jsPlumb for node connections

let visualMergeBuilder = {
    jsPlumbInstance: null,
    nodes: [],
    connections: [],
    nodeCounter: 0,
    zoom: 1.0,
    availableConnections: [],
    canvasOffset: { x: 0, y: 0 }
};

// Initialize visual merge builder
function initVisualMergeBuilder() {
    // Reset state
    visualMergeBuilder.nodes = [];
    visualMergeBuilder.connections = [];
    visualMergeBuilder.nodeCounter = 0;
    visualMergeBuilder.zoom = 1.0;

    // Initialize jsPlumb
    const container = document.getElementById('visualMergeCanvas');
    if (!container) return;

    // Clear canvas
    container.innerHTML = '';

    // Create jsPlumb instance
    visualMergeBuilder.jsPlumbInstance = jsPlumb.getInstance({
        Container: container,
        Connector: ["Flowchart", { stub: 30, gap: 5, cornerRadius: 5 }],
        PaintStyle: { stroke: "#2563eb", strokeWidth: 2 },
        HoverPaintStyle: { stroke: "#1e40af", strokeWidth: 3 },
        Endpoint: ["Dot", { radius: 5 }],
        EndpointStyle: { fill: "#2563eb" },
        EndpointHoverStyle: { fill: "#1e40af" },
        ConnectionOverlays: [
            ["Arrow", { width: 10, length: 10, location: 1 }]
        ]
    });

    // Load available connections
    loadConnectionsForVisualBuilder();

    // Setup connection event
    visualMergeBuilder.jsPlumbInstance.bind("connection", function(info) {
        onNodeConnection(info);
    });

    // Setup disconnection event
    visualMergeBuilder.jsPlumbInstance.bind("connectionDetached", function(info) {
        onNodeDisconnection(info);
    });

    renderVisualBuilder();
}

function showVisualMergeBuilder() {
    initVisualMergeBuilder();
    loadConnectionsForVisualBuilder();
    loadExistingMappings(); // Load existing multi-source mappings for dropdown
    const modal = new bootstrap.Modal(document.getElementById('visualMergeBuilderModal'));
    modal.show();
}

async function loadConnectionsForVisualBuilder() {
    try {
        const response = await fetch(`${API_BASE}/admin/connection/list`);
        if (!response.ok) throw new Error('Failed to load connections');
        visualMergeBuilder.availableConnections = await response.json();
    } catch (error) {
        console.error('Error loading connections:', error);
    }
}

// === NODE PALETTE ===

function showAddSourceNodeDialog() {
    // Populate connection dropdown
    const select = document.getElementById('visualSourceConnection');
    select.innerHTML = '<option value="">-- Select Connection --</option>';
    visualMergeBuilder.availableConnections.forEach(conn => {
        const option = document.createElement('option');
        option.value = conn.id;
        option.textContent = `${conn.name} (${conn.server}/${conn.database})`;
        select.appendChild(option);
    });

    // Reset form
    document.getElementById('visualSourceForm').reset();

    const modal = new bootstrap.Modal(document.getElementById('addVisualSourceModal'));
    modal.show();
}

function addSourceNodeToCanvas() {
    const connectionId = document.getElementById('visualSourceConnection').value;
    const alias = document.getElementById('visualSourceAlias').value.trim();
    const schema = document.getElementById('visualSourceSchema').value.trim();
    const table = document.getElementById('visualSourceTable').value.trim();
    const whereClause = document.getElementById('visualSourceWhere').value.trim() || null;

    if (!connectionId || !alias || !schema || !table) {
        showAlert('warning', 'Please fill in all required fields');
        return;
    }

    // Check for duplicate alias
    if (visualMergeBuilder.nodes.some(n => n.alias === alias)) {
        showAlert('warning', `Alias "${alias}" already exists. Use a unique alias.`);
        return;
    }

    const node = {
        id: `node_${visualMergeBuilder.nodeCounter++}`,
        type: 'source',
        connectionId: connectionId,
        alias: alias,
        schema: schema,
        table: table,
        whereClause: whereClause,
        x: 100,
        y: 100 + (visualMergeBuilder.nodes.length * 100)
    };

    visualMergeBuilder.nodes.push(node);
    renderNode(node);

    bootstrap.Modal.getInstance(document.getElementById('addVisualSourceModal')).hide();
    showAlert('success', `Source "${alias}" added to canvas`);
}

function addOperationNode(operationType) {
    const node = {
        id: `node_${visualMergeBuilder.nodeCounter++}`,
        type: 'operation',
        operationType: operationType,
        x: 400,
        y: 100 + (visualMergeBuilder.nodes.filter(n => n.type === 'operation').length * 120),
        config: {}
    };

    visualMergeBuilder.nodes.push(node);
    renderNode(node);
    showAlert('info', `${operationType.toUpperCase()} operation added`);
}

function addDestinationNode() {
    // Only allow one destination node
    if (visualMergeBuilder.nodes.some(n => n.type === 'destination')) {
        showAlert('warning', 'Only one destination node allowed');
        return;
    }

    const node = {
        id: `node_${visualMergeBuilder.nodeCounter++}`,
        type: 'destination',
        x: 700,
        y: 200
    };

    visualMergeBuilder.nodes.push(node);
    renderNode(node);
    showAlert('info', 'Destination node added');
}

// === NODE RENDERING ===

function renderNode(node) {
    const canvas = document.getElementById('visualMergeCanvas');
    const nodeEl = document.createElement('div');
    nodeEl.id = node.id;
    nodeEl.className = 'visual-merge-node';
    nodeEl.style.left = `${node.x}px`;
    nodeEl.style.top = `${node.y}px`;

    let nodeContent = '';
    let nodeClass = '';

    if (node.type === 'source') {
        nodeClass = 'node-source';
        nodeContent = `
            <div class="node-header bg-primary text-white">
                <i class="bi bi-database"></i> ${node.alias}
            </div>
            <div class="node-body">
                <small>${node.schema}.${node.table}</small>
                ${node.whereClause ? `<br><small class="text-info">WHERE: ${node.whereClause}</small>` : ''}
            </div>
            <div class="node-ports">
                <div class="port port-output" id="${node.id}_out"></div>
            </div>
            <button class="node-delete" onclick="deleteNode('${node.id}')"><i class="bi bi-x"></i></button>
        `;
    } else if (node.type === 'operation') {
        nodeClass = 'node-operation';
        let icon = 'bi-gear';
        if (node.operationType === 'join') icon = 'bi-diagram-3';
        else if (node.operationType === 'union') icon = 'bi-union';

        nodeContent = `
            <div class="node-header bg-info text-white">
                <i class="bi ${icon}"></i> ${node.operationType.toUpperCase()}
            </div>
            <div class="node-body">
                <button class="btn btn-sm btn-outline-secondary" onclick="configureOperationNode('${node.id}')">
                    <i class="bi bi-gear"></i> Configure
                </button>
            </div>
            <div class="node-ports">
                <div class="port port-input port-input-left" id="${node.id}_in_left" title="Left Input"></div>
                <div class="port port-input port-input-right" id="${node.id}_in_right" title="Right Input"></div>
                <div class="port port-output" id="${node.id}_out"></div>
            </div>
            <button class="node-delete" onclick="deleteNode('${node.id}')"><i class="bi bi-x"></i></button>
        `;
    } else if (node.type === 'destination') {
        nodeClass = 'node-destination';
        nodeContent = `
            <div class="node-header bg-success text-white">
                <i class="bi bi-box-arrow-in-down"></i> Destination
            </div>
            <div class="node-body">
                <small>Output</small>
            </div>
            <div class="node-ports">
                <div class="port port-input" id="${node.id}_in"></div>
            </div>
            <button class="node-delete" onclick="deleteNode('${node.id}')"><i class="bi bi-x"></i></button>
        `;
    }

    nodeEl.className += ' ' + nodeClass;
    nodeEl.innerHTML = nodeContent;
    canvas.appendChild(nodeEl);

    // Make node draggable
    visualMergeBuilder.jsPlumbInstance.draggable(nodeEl, {
        containment: true,
        grid: [10, 10]
    });

    // Add connection endpoints
    if (node.type === 'source') {
        visualMergeBuilder.jsPlumbInstance.addEndpoint(nodeEl, {
            anchor: "Right",
            isSource: true,
            isTarget: false,
            uuid: `${node.id}_out`,
            maxConnections: -1
        });
    } else if (node.type === 'operation') {
        // Left input
        visualMergeBuilder.jsPlumbInstance.addEndpoint(nodeEl, {
            anchor: [0, 0.3, -1, 0],
            isSource: false,
            isTarget: true,
            uuid: `${node.id}_in_left`,
            maxConnections: 1
        });
        // Right input
        visualMergeBuilder.jsPlumbInstance.addEndpoint(nodeEl, {
            anchor: [0, 0.7, -1, 0],
            isSource: false,
            isTarget: true,
            uuid: `${node.id}_in_right`,
            maxConnections: 1
        });
        // Output
        visualMergeBuilder.jsPlumbInstance.addEndpoint(nodeEl, {
            anchor: "Right",
            isSource: true,
            isTarget: false,
            uuid: `${node.id}_out`,
            maxConnections: -1
        });
    } else if (node.type === 'destination') {
        visualMergeBuilder.jsPlumbInstance.addEndpoint(nodeEl, {
            anchor: "Left",
            isSource: false,
            isTarget: true,
            uuid: `${node.id}_in`,
            maxConnections: 1
        });
    }
}

function deleteNode(nodeId) {
    // Remove all connections to/from this node
    const conns = visualMergeBuilder.jsPlumbInstance.getConnections();
    conns.forEach(conn => {
        if (conn.sourceId === nodeId || conn.targetId === nodeId) {
            visualMergeBuilder.jsPlumbInstance.deleteConnection(conn);
        }
    });

    // Remove node from array
    visualMergeBuilder.nodes = visualMergeBuilder.nodes.filter(n => n.id !== nodeId);

    // Remove DOM element
    const nodeEl = document.getElementById(nodeId);
    if (nodeEl) {
        visualMergeBuilder.jsPlumbInstance.remove(nodeEl);
    }

    showAlert('info', 'Node deleted');
}

function configureOperationNode(nodeId) {
    const node = visualMergeBuilder.nodes.find(n => n.id === nodeId);
    if (!node) return;

    // Show configuration modal based on operation type
    if (node.operationType === 'join') {
        // Populate current config
        document.getElementById('visualJoinType').value = node.config.joinType || 'INNER';
        document.getElementById('visualOnCondition').value = node.config.onCondition || '';

        // Show modal
        const modal = new bootstrap.Modal(document.getElementById('configureJoinModal'));
        modal.show();

        // Save reference for later
        window.currentConfigNodeId = nodeId;
    } else if (node.operationType === 'union') {
        document.getElementById('visualUnionAll').checked = node.config.unionAll !== false;

        const modal = new bootstrap.Modal(document.getElementById('configureUnionModal'));
        modal.show();

        window.currentConfigNodeId = nodeId;
    }
}

function saveJoinConfig() {
    const nodeId = window.currentConfigNodeId;
    const node = visualMergeBuilder.nodes.find(n => n.id === nodeId);
    if (!node) return;

    node.config.joinType = document.getElementById('visualJoinType').value;
    node.config.onCondition = document.getElementById('visualOnCondition').value.trim();

    bootstrap.Modal.getInstance(document.getElementById('configureJoinModal')).hide();
    showAlert('success', 'JOIN configuration saved');
}

function saveUnionConfig() {
    const nodeId = window.currentConfigNodeId;
    const node = visualMergeBuilder.nodes.find(n => n.id === nodeId);
    if (!node) return;

    node.config.unionAll = document.getElementById('visualUnionAll').checked;

    bootstrap.Modal.getInstance(document.getElementById('configureUnionModal')).hide();
    showAlert('success', 'UNION configuration saved');
}

// === CONNECTION HANDLING ===

function onNodeConnection(info) {
    const connection = {
        sourceId: info.sourceId,
        targetId: info.targetId,
        sourceEndpoint: info.sourceEndpoint.getUuid(),
        targetEndpoint: info.targetEndpoint.getUuid()
    };
    visualMergeBuilder.connections.push(connection);
    console.log('Connection created:', connection);
}

function onNodeDisconnection(info) {
    visualMergeBuilder.connections = visualMergeBuilder.connections.filter(c =>
        !(c.sourceId === info.sourceId && c.targetId === info.targetId &&
          c.sourceEndpoint === info.sourceEndpoint.getUuid())
    );
    console.log('Connection removed');
}

// === COMPILE TO MERGE PATTERN ===

function compileVisualMergePattern() {
    try {
        // Validate graph
        const sourceNodes = visualMergeBuilder.nodes.filter(n => n.type === 'source');
        const opNodes = visualMergeBuilder.nodes.filter(n => n.type === 'operation');
        const destNode = visualMergeBuilder.nodes.find(n => n.type === 'destination');

        if (sourceNodes.length < 2) {
            showAlert('warning', 'Need at least 2 source nodes');
            return null;
        }

        if (opNodes.length === 0) {
            showAlert('warning', 'Need at least one operation node');
            return null;
        }

        if (!destNode) {
            showAlert('warning', 'Need a destination node');
            return null;
        }

        // Build execution order using topological sort
        const operations = buildOperationPipeline();
        if (!operations) {
            showAlert('danger', 'Invalid node graph - check connections');
            return null;
        }

        // Build sources list
        const sources = sourceNodes.map(n => ({
            id: n.id,
            connection_id: n.connectionId,
            alias: n.alias,
            schema: n.schema,
            table: n.table,
            enable_cdc: true,
            where_clause: n.whereClause
        }));

        // For now, auto-generate output columns from final operation
        // In a full implementation, this would be a separate step
        const outputColumns = [];

        const mergePattern = {
            operations: operations,
            output_columns: outputColumns
        };

        return {
            is_multi_source: true,
            sources: sources,
            merge_pattern: mergePattern
        };

    } catch (error) {
        console.error('Error compiling merge pattern:', error);
        showAlert('danger', `Compilation error: ${error.message}`);
        return null;
    }
}

function buildOperationPipeline() {
    const opNodes = visualMergeBuilder.nodes.filter(n => n.type === 'operation');
    const operations = [];

    // Simple approach: process nodes in order they appear
    // In a full implementation, use topological sort
    opNodes.forEach((node, index) => {
        // Find connections to this node
        const leftConn = visualMergeBuilder.connections.find(c =>
            c.targetId === node.id && c.targetEndpoint.includes('in_left')
        );
        const rightConn = visualMergeBuilder.connections.find(c =>
            c.targetId === node.id && c.targetEndpoint.includes('in_right')
        );

        if (!leftConn || !rightConn) {
            console.warn(`Operation node ${node.id} missing inputs`);
            return;
        }

        // Get source nodes/aliases
        const leftSource = visualMergeBuilder.nodes.find(n => n.id === leftConn.sourceId);
        const rightSource = visualMergeBuilder.nodes.find(n => n.id === rightConn.sourceId);

        const leftAlias = leftSource.type === 'source' ? leftSource.alias : 'previous_result';
        const rightAlias = rightSource.type === 'source' ? rightSource.alias : 'previous_result';

        const operation = {
            type: node.operationType,
            left_source: leftAlias,
            right_source: rightAlias
        };

        if (node.operationType === 'join') {
            operation.join_type = node.config.joinType || 'INNER';
            operation.on_condition = node.config.onCondition || '';
        } else if (node.operationType === 'union') {
            operation.union_all = node.config.unionAll !== false;
        }

        operations.push(operation);
    });

    return operations.length > 0 ? operations : null;
}

async function previewVisualMerge() {
    const pattern = compileVisualMergePattern();
    if (!pattern) return;

    // Get basic mapping info
    const mappingId = document.getElementById('visualMappingId').value.trim() || `visual_${Date.now()}`;
    const mappingName = document.getElementById('visualMappingName').value.trim() || 'Visual Merge';
    const destSchema = document.getElementById('visualDestSchema').value.trim() || 'dbo';
    const destTable = document.getElementById('visualDestTable').value.trim() || 'MergedData';

    const mapping = {
        id: mappingId,
        name: mappingName,
        mapping_type: 'table',
        destination_schema: destSchema,
        destination_table: destTable,
        ...pattern
    };

    showLoading('Previewing visual merge...');

    try {
        const response = await fetch(`${API_BASE}/admin/mapping/preview-merge`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                mapping: mapping,
                limit: 10
            })
        });

        const result = await response.json();
        hideLoading();

        if (result.success) {
            showAlert('success', `Preview successful! ${result.row_count} rows returned.`);
            displayVisualPreviewResults(result);
        } else {
            showAlert('danger', `Preview failed: ${result.error}`);
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error: ${error.message}`);
    }
}

function displayVisualPreviewResults(result) {
    const container = document.getElementById('visualPreviewResults');

    if (!result.sample_data || result.sample_data.length === 0) {
        container.innerHTML = '<p class="text-muted">No results</p>';
        return;
    }

    let html = '<h6>Preview Results</h6><div class="table-responsive"><table class="table table-sm"><thead><tr>';
    result.columns.forEach(col => html += `<th>${col}</th>`);
    html += '</tr></thead><tbody>';

    result.sample_data.forEach(row => {
        html += '<tr>';
        result.columns.forEach(col => html += `<td>${row[col] !== null ? row[col] : '<em>NULL</em>'}</td>`);
        html += '</tr>';
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
}

async function saveVisualMerge() {
    const pattern = compileVisualMergePattern();
    if (!pattern) return;

    const mappingId = document.getElementById('visualMappingId').value.trim();
    const mappingName = document.getElementById('visualMappingName').value.trim();
    const destSchema = document.getElementById('visualDestSchema').value.trim();
    const destTable = document.getElementById('visualDestTable').value.trim();

    if (!mappingId || !mappingName || !destSchema || !destTable) {
        showAlert('warning', 'Please fill in all mapping details');
        return;
    }

    const mapping = {
        id: mappingId,
        name: mappingName,
        mapping_type: 'table',
        destination_schema: destSchema,
        destination_table: destTable,
        enabled: true,
        ...pattern
    };

    showLoading('Saving visual merge mapping...');

    try {
        const response = await fetch(`${API_BASE}/admin/mapping/create`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(mapping)
        });

        const result = await response.json();
        hideLoading();

        if (result.success || response.ok) {
            showAlert('success', 'Visual merge mapping saved!');
            bootstrap.Modal.getInstance(document.getElementById('visualMergeBuilderModal')).hide();
            loadMappings();
        } else {
            showAlert('danger', result.message || 'Failed to save');
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error: ${error.message}`);
    }
}

function clearVisualCanvas() {
    if (!confirm('Clear all nodes and connections?')) return;

    visualMergeBuilder.jsPlumbInstance.deleteEveryConnection();
    visualMergeBuilder.jsPlumbInstance.deleteEveryEndpoint();

    visualMergeBuilder.nodes.forEach(node => {
        const nodeEl = document.getElementById(node.id);
        if (nodeEl) nodeEl.remove();
    });

    visualMergeBuilder.nodes = [];
    visualMergeBuilder.connections = [];

    showAlert('info', 'Canvas cleared');
}

function renderVisualBuilder() {
    // Initial render - empty canvas
    console.log('Visual merge builder initialized');
}

// ==================== LOAD EXISTING MAPPING INTO VISUALIZER ====================

async function loadExistingMappings() {
    try {
        const response = await fetch(`${API_BASE}/admin/mapping/list`);
        if (!response.ok) throw new Error('Failed to load mappings');

        const mappings = await response.json();

        // Filter to only multi-source mappings
        const multiSourceMappings = mappings.filter(m => m.is_multi_source);

        const select = document.getElementById('visualLoadMappingSelect');
        if (!select) return;

        select.innerHTML = '<option value="">-- Select Mapping to Load --</option>';

        multiSourceMappings.forEach(mapping => {
            const option = document.createElement('option');
            option.value = mapping.id;
            option.textContent = `${mapping.name} (${mapping.sources ? mapping.sources.length : 0} sources)`;
            select.appendChild(option);
        });

    } catch (error) {
        console.error('Error loading mappings:', error);
        showAlert('danger', `Error loading mappings: ${error.message}`);
    }
}

async function loadMappingIntoVisualizer() {
    const select = document.getElementById('visualLoadMappingSelect');
    const mappingId = select.value;

    if (!mappingId) {
        showAlert('warning', 'Please select a mapping to load');
        return;
    }

    try {
        showLoading('Loading mapping into visualizer...');

        const response = await fetch(`${API_BASE}/admin/mapping/${mappingId}`);
        if (!response.ok) throw new Error('Failed to load mapping');

        const mapping = await response.json();
        hideLoading();

        if (!mapping.is_multi_source) {
            showAlert('warning', 'Selected mapping is not a multi-source mapping');
            return;
        }

        // Clear canvas first
        clearVisualCanvasNoConfirm();

        // Load mapping into visualizer
        await reconstructVisualMapping(mapping);

        showAlert('success', `Loaded mapping: ${mapping.name}`);

    } catch (error) {
        hideLoading();
        console.error('Error loading mapping:', error);
        showAlert('danger', `Error loading mapping: ${error.message}`);
    }
}

function clearVisualCanvasNoConfirm() {
    visualMergeBuilder.jsPlumbInstance.deleteEveryConnection();
    visualMergeBuilder.jsPlumbInstance.deleteEveryEndpoint();

    visualMergeBuilder.nodes.forEach(node => {
        const nodeEl = document.getElementById(node.id);
        if (nodeEl) nodeEl.remove();
    });

    visualMergeBuilder.nodes = [];
    visualMergeBuilder.connections = [];
}

async function reconstructVisualMapping(mapping) {
    // Store mapping ID and name for later save
    visualMergeBuilder.currentMappingId = mapping.id;
    visualMergeBuilder.currentMappingName = mapping.name;

    // Populate basic fields
    document.getElementById('visualMappingId').value = mapping.id || '';
    document.getElementById('visualMappingName').value = mapping.name || '';
    document.getElementById('visualDestSchema').value = mapping.destination_schema || '';
    document.getElementById('visualDestTable').value = mapping.destination_table || '';

    // Create source nodes
    const sourceNodeMap = {}; // alias -> node
    let xPos = 50;
    const yPos = 100;

    if (mapping.sources && mapping.sources.length > 0) {
        for (const source of mapping.sources) {
            const nodeId = `node_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

            const node = {
                id: nodeId,
                type: 'source',
                connectionId: source.connection_id,
                alias: source.alias,
                schema: source.schema,
                table: source.table,
                enableCDC: source.enable_cdc || true,
                whereClause: source.where_clause || '',
                x: xPos,
                y: yPos
            };

            visualMergeBuilder.nodes.push(node);
            sourceNodeMap[source.alias] = node;

            renderNode(node);

            xPos += 200;
        }
    }

    // Create operation nodes
    const operationNodeMap = {}; // index -> node
    xPos = 50;
    let opYPos = 300;

    if (mapping.merge_pattern && mapping.merge_pattern.operations) {
        for (let i = 0; i < mapping.merge_pattern.operations.length; i++) {
            const op = mapping.merge_pattern.operations[i];
            const nodeId = `node_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

            const node = {
                id: nodeId,
                type: 'operation',
                operationType: op.type,
                x: xPos + (i * 200),
                y: opYPos
            };

            // Store operation config
            if (op.type === 'join') {
                node.joinType = op.join_type;
                node.onCondition = op.on_condition;
            } else if (op.type === 'union') {
                node.unionAll = op.union_all;
            }

            visualMergeBuilder.nodes.push(node);
            operationNodeMap[i] = node;

            renderNode(node);
        }
    }

    // Create destination node
    const destNodeId = `node_${Date.now()}_dest`;
    const destNode = {
        id: destNodeId,
        type: 'destination',
        schema: mapping.destination_schema,
        table: mapping.destination_table,
        x: 300,
        y: 500
    };

    visualMergeBuilder.nodes.push(destNode);
    renderNode(destNode);

    // Wait for DOM to update
    await new Promise(resolve => setTimeout(resolve, 100));

    // Create connections
    if (mapping.merge_pattern && mapping.merge_pattern.operations) {
        for (let i = 0; i < mapping.merge_pattern.operations.length; i++) {
            const op = mapping.merge_pattern.operations[i];
            const opNode = operationNodeMap[i];

            // Connect left source
            if (op.left_source === 'previous_result' && i > 0) {
                // Connect from previous operation
                const prevOpNode = operationNodeMap[i - 1];
                visualMergeBuilder.jsPlumbInstance.connect({
                    source: prevOpNode.id,
                    target: opNode.id,
                    anchors: ['Right', 'Left']
                });
            } else if (sourceNodeMap[op.left_source]) {
                // Connect from source node
                visualMergeBuilder.jsPlumbInstance.connect({
                    source: sourceNodeMap[op.left_source].id,
                    target: opNode.id,
                    anchors: ['Right', 'Left']
                });
            }

            // Connect right source
            if (sourceNodeMap[op.right_source]) {
                visualMergeBuilder.jsPlumbInstance.connect({
                    source: sourceNodeMap[op.right_source].id,
                    target: opNode.id,
                    anchors: ['Right', 'Left']
                });
            }

            // Connect last operation to destination
            if (i === mapping.merge_pattern.operations.length - 1) {
                visualMergeBuilder.jsPlumbInstance.connect({
                    source: opNode.id,
                    target: destNodeId,
                    anchors: ['Right', 'Left']
                });
            }
        }
    }

    // If there are sources but no operations, connect directly to destination
    if (mapping.sources && mapping.sources.length > 0 &&
        (!mapping.merge_pattern || !mapping.merge_pattern.operations || mapping.merge_pattern.operations.length === 0)) {
        for (const alias in sourceNodeMap) {
            visualMergeBuilder.jsPlumbInstance.connect({
                source: sourceNodeMap[alias].id,
                target: destNodeId,
                anchors: ['Right', 'Left']
            });
        }
    }

    console.log('Reconstructed visual mapping:', mapping.name);
}

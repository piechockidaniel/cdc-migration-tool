// Multi-Source Mapping Builder (Form-based UI for Phase 2)

let multiSourceMapping = {
    id: '',
    name: '',
    is_multi_source: true,
    mapping_type: 'table',
    sources: [],
    merge_pattern: {
        operations: [],
        output_columns: []
    },
    destination_schema: '',
    destination_table: '',
    enabled: true,
    conflict_resolution: 'latest_wins',
    source_priority: []
};

let availableConnections = [];

// Initialize multi-source builder
function showMultiSourceBuilder() {
    initMultiSourceMapping();
    loadConnectionsForMultiSource();
    const modal = new bootstrap.Modal(document.getElementById('multiSourceModal'));
    modal.show();
}

function initMultiSourceMapping() {
    multiSourceMapping = {
        id: '',
        name: '',
        is_multi_source: true,
        mapping_type: 'table',
        sources: [],
        merge_pattern: {
            operations: [],
            output_columns: []
        },
        destination_schema: '',
        destination_table: '',
        enabled: true,
        conflict_resolution: 'latest_wins',
        source_priority: []
    };
    renderSourcesList();
    renderOperationsList();
    renderOutputColumnsList();
}

async function loadConnectionsForMultiSource() {
    try {
        const response = await fetch(`${API_BASE}/admin/connection/list`);
        if (!response.ok) throw new Error('Failed to load connections');

        availableConnections = await response.json();
    } catch (error) {
        console.error('Error loading connections:', error);
        showAlert('danger', `Error loading connections: ${error.message}`);
    }
}

// === SOURCE MANAGEMENT ===

function showAddSourceModal() {
    // Populate connection dropdown
    const select = document.getElementById('msSourceConnection');
    select.innerHTML = '<option value="">-- Select Connection --</option>';
    availableConnections.forEach(conn => {
        const option = document.createElement('option');
        option.value = conn.id;
        option.textContent = `${conn.name} (${conn.server}/${conn.database})`;
        select.appendChild(option);
    });

    // Reset form
    document.getElementById('multiSourceForm').reset();
    document.getElementById('msSourceId').value = `src_${Date.now()}`;

    const modal = new bootstrap.Modal(document.getElementById('addSourceModal'));
    modal.show();
}

function addSource() {
    const sourceId = document.getElementById('msSourceId').value.trim();
    const connectionId = document.getElementById('msSourceConnection').value;
    const alias = document.getElementById('msSourceAlias').value.trim();
    const schema = document.getElementById('msSourceSchema').value.trim();
    const table = document.getElementById('msSourceTable').value.trim();
    const whereClause = document.getElementById('msSourceWhere').value.trim() || null;

    // Validation
    if (!connectionId || !alias || !schema || !table) {
        showAlert('warning', 'Please fill in all required fields');
        return;
    }

    // Check for duplicate alias
    if (multiSourceMapping.sources.some(s => s.alias === alias)) {
        showAlert('warning', `Alias "${alias}" is already in use. Please choose a unique alias.`);
        return;
    }

    // Add source
    const source = {
        id: sourceId,
        connection_id: connectionId,
        alias: alias,
        schema: schema,
        table: table,
        enable_cdc: document.getElementById('msSourceEnableCDC').checked,
        where_clause: whereClause
    };

    multiSourceMapping.sources.push(source);
    renderSourcesList();

    // Close modal
    bootstrap.Modal.getInstance(document.getElementById('addSourceModal')).hide();
    showAlert('success', `Source "${alias}" added successfully`);
}

function removeSource(sourceId) {
    multiSourceMapping.sources = multiSourceMapping.sources.filter(s => s.id !== sourceId);
    renderSourcesList();
    showAlert('info', 'Source removed');
}

function renderSourcesList() {
    const container = document.getElementById('msSourcesList');
    if (multiSourceMapping.sources.length === 0) {
        container.innerHTML = '<p class="text-muted">No sources added. Click "Add Source" to begin.</p>';
        return;
    }

    let html = '<div class="list-group">';
    multiSourceMapping.sources.forEach(source => {
        const conn = availableConnections.find(c => c.id === source.connection_id);
        const connName = conn ? conn.name : source.connection_id;

        html += `
            <div class="list-group-item">
                <div class="d-flex justify-content-between align-items-start">
                    <div>
                        <h6 class="mb-1"><span class="badge bg-primary">${source.alias}</span> ${source.schema}.${source.table}</h6>
                        <small class="text-muted">Connection: ${connName}</small>
                        ${source.where_clause ? `<br><small class="text-info">Filter: ${source.where_clause}</small>` : ''}
                    </div>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeSource('${source.id}')">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </div>
        `;
    });
    html += '</div>';

    container.innerHTML = html;
}

// === MERGE OPERATIONS ===

function showAddOperationModal() {
    if (multiSourceMapping.sources.length < 2) {
        showAlert('warning', 'Please add at least 2 sources before creating merge operations');
        return;
    }

    // Populate source dropdowns
    const leftSelect = document.getElementById('msOpLeftSource');
    const rightSelect = document.getElementById('msOpRightSource');

    leftSelect.innerHTML = '<option value="">-- Select Source --</option>';
    rightSelect.innerHTML = '<option value="">-- Select Source --</option>';

    // Add "previous_result" option for left source if there are existing operations
    if (multiSourceMapping.merge_pattern.operations.length > 0) {
        const prevOption = document.createElement('option');
        prevOption.value = 'previous_result';
        prevOption.textContent = 'Previous Result';
        leftSelect.appendChild(prevOption);
    }

    multiSourceMapping.sources.forEach(source => {
        const leftOption = document.createElement('option');
        leftOption.value = source.alias;
        leftOption.textContent = `${source.alias} (${source.schema}.${source.table})`;
        leftSelect.appendChild(leftOption);

        const rightOption = document.createElement('option');
        rightOption.value = source.alias;
        rightOption.textContent = `${source.alias} (${source.schema}.${source.table})`;
        rightSelect.appendChild(rightOption);
    });

    // Reset form
    document.getElementById('mergeOperationForm').reset();
    onOperationTypeChange(); // Update UI based on default type

    const modal = new bootstrap.Modal(document.getElementById('addOperationModal'));
    modal.show();
}

function onOperationTypeChange() {
    const opType = document.getElementById('msOpType').value;
    const joinFields = document.getElementById('msOpJoinFields');
    const unionFields = document.getElementById('msOpUnionFields');

    joinFields.style.display = opType === 'join' ? 'block' : 'none';
    unionFields.style.display = opType === 'union' ? 'block' : 'none';
}

function addOperation() {
    const opType = document.getElementById('msOpType').value;
    const leftSource = document.getElementById('msOpLeftSource').value;
    const rightSource = document.getElementById('msOpRightSource').value;

    if (!leftSource || !rightSource) {
        showAlert('warning', 'Please select both left and right sources');
        return;
    }

    const operation = {
        type: opType,
        left_source: leftSource,
        right_source: rightSource
    };

    if (opType === 'join') {
        const joinType = document.getElementById('msOpJoinType').value;
        const onCondition = document.getElementById('msOpOnCondition').value.trim();

        if (!joinType || !onCondition) {
            showAlert('warning', 'Please specify join type and ON condition');
            return;
        }

        operation.join_type = joinType;
        operation.on_condition = onCondition;
    } else if (opType === 'union') {
        operation.union_all = document.getElementById('msOpUnionAll').checked;
    }

    multiSourceMapping.merge_pattern.operations.push(operation);
    renderOperationsList();

    bootstrap.Modal.getInstance(document.getElementById('addOperationModal')).hide();
    showAlert('success', 'Operation added successfully');
}

function removeOperation(index) {
    multiSourceMapping.merge_pattern.operations.splice(index, 1);
    renderOperationsList();
    showAlert('info', 'Operation removed');
}

function renderOperationsList() {
    const container = document.getElementById('msOperationsList');
    if (multiSourceMapping.merge_pattern.operations.length === 0) {
        container.innerHTML = '<p class="text-muted">No operations defined. Click "Add Operation" to merge sources.</p>';
        return;
    }

    let html = '<div class="list-group">';
    multiSourceMapping.merge_pattern.operations.forEach((op, index) => {
        let opDesc = '';
        if (op.type === 'join') {
            opDesc = `${op.join_type} JOIN on ${op.on_condition}`;
        } else if (op.type === 'union') {
            opDesc = op.union_all ? 'UNION ALL' : 'UNION';
        } else {
            opDesc = op.type.toUpperCase();
        }

        html += `
            <div class="list-group-item">
                <div class="d-flex justify-content-between align-items-start">
                    <div>
                        <h6 class="mb-1">Step ${index + 1}: ${opDesc}</h6>
                        <small class="text-muted">${op.left_source} + ${op.right_source}</small>
                    </div>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeOperation(${index})">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </div>
        `;
    });
    html += '</div>';

    container.innerHTML = html;
}

// === OUTPUT COLUMNS ===

function showAddOutputColumnModal() {
    // Populate source alias dropdown
    const select = document.getElementById('msOutSourceAlias');
    select.innerHTML = '<option value="">-- Select Source --</option>';
    select.innerHTML += '<option value="merged">Merged Result</option>';

    multiSourceMapping.sources.forEach(source => {
        const option = document.createElement('option');
        option.value = source.alias;
        option.textContent = source.alias;
        select.appendChild(option);
    });

    // Reset form
    document.getElementById('outputColumnForm').reset();

    const modal = new bootstrap.Modal(document.getElementById('addOutputColumnModal'));
    modal.show();
}

function addOutputColumn() {
    const sourceAlias = document.getElementById('msOutSourceAlias').value;
    const sourceColumn = document.getElementById('msOutSourceColumn').value.trim();
    const destColumn = document.getElementById('msOutDestColumn').value.trim();
    const transformation = document.getElementById('msOutTransformation').value.trim() || null;

    if (!sourceAlias || !sourceColumn || !destColumn) {
        showAlert('warning', 'Please fill in all required fields');
        return;
    }

    const outputCol = {
        source_alias: sourceAlias,
        source_column: sourceColumn,
        destination_column: destColumn,
        transformation: transformation
    };

    multiSourceMapping.merge_pattern.output_columns.push(outputCol);
    renderOutputColumnsList();

    bootstrap.Modal.getInstance(document.getElementById('addOutputColumnModal')).hide();
    showAlert('success', 'Output column added');
}

function removeOutputColumn(index) {
    multiSourceMapping.merge_pattern.output_columns.splice(index, 1);
    renderOutputColumnsList();
    showAlert('info', 'Output column removed');
}

function renderOutputColumnsList() {
    const container = document.getElementById('msOutputColumnsList');
    if (multiSourceMapping.merge_pattern.output_columns.length === 0) {
        container.innerHTML = '<p class="text-muted">No output columns defined. Click "Add Column" to map result columns.</p>';
        return;
    }

    let html = '<table class="table table-sm"><thead><tr><th>Source</th><th>Source Column</th><th>Destination Column</th><th>Actions</th></tr></thead><tbody>';
    multiSourceMapping.merge_pattern.output_columns.forEach((col, index) => {
        html += `
            <tr>
                <td><span class="badge bg-secondary">${col.source_alias}</span></td>
                <td>${col.source_column}</td>
                <td>${col.destination_column}</td>
                <td>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeOutputColumn(${index})">
                        <i class="bi bi-trash"></i>
                    </button>
                </td>
            </tr>
        `;
    });
    html += '</tbody></table>';

    container.innerHTML = html;
}

// === PREVIEW & SAVE ===

async function previewMultiSourceMerge() {
    // Collect basic info
    multiSourceMapping.id = document.getElementById('msId').value.trim() || `multi_${Date.now()}`;
    multiSourceMapping.name = document.getElementById('msName').value.trim();
    multiSourceMapping.destination_schema = document.getElementById('msDestSchema').value.trim();
    multiSourceMapping.destination_table = document.getElementById('msDestTable').value.trim();

    // Validation
    if (!multiSourceMapping.name || !multiSourceMapping.destination_schema || !multiSourceMapping.destination_table) {
        showAlert('warning', 'Please fill in mapping name and destination details');
        return;
    }

    if (multiSourceMapping.sources.length < 2) {
        showAlert('warning', 'Please add at least 2 sources');
        return;
    }

    if (multiSourceMapping.merge_pattern.operations.length === 0) {
        showAlert('warning', 'Please add at least one merge operation');
        return;
    }

    if (multiSourceMapping.merge_pattern.output_columns.length === 0) {
        showAlert('warning', 'Please add at least one output column mapping');
        return;
    }

    showLoading('Previewing merge results...');

    try {
        const response = await fetch(`${API_BASE}/admin/mapping/preview-merge`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                mapping: multiSourceMapping,
                limit: 10
            })
        });

        const result = await response.json();
        hideLoading();

        if (result.success) {
            showAlert('success', `Preview successful! Returned ${result.row_count} rows.`);
            displayPreviewResults(result);
        } else {
            showAlert('danger', `Preview failed: ${result.error}`);
            if (result.query) {
                console.log('Generated SQL:', result.query);
            }
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error previewing merge: ${error.message}`);
    }
}

function displayPreviewResults(result) {
    const container = document.getElementById('msPreviewResults');

    if (!result.sample_data || result.sample_data.length === 0) {
        container.innerHTML = '<p class="text-muted">No results returned</p>';
        return;
    }

    let html = '<h6>Preview Results (First 10 rows)</h6>';
    html += '<div class="table-responsive"><table class="table table-sm table-striped"><thead><tr>';

    // Headers
    result.columns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    html += '</tr></thead><tbody>';

    // Rows
    result.sample_data.forEach(row => {
        html += '<tr>';
        result.columns.forEach(col => {
            html += `<td>${row[col] !== null ? row[col] : '<em>NULL</em>'}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table></div>';

    if (result.query) {
        html += '<details><summary>Generated SQL Query</summary><pre>' + result.query + '</pre></details>';
    }

    container.innerHTML = html;
}

async function saveMultiSourceMapping() {
    // Collect basic info
    multiSourceMapping.id = document.getElementById('msId').value.trim() || `multi_${Date.now()}`;
    multiSourceMapping.name = document.getElementById('msName').value.trim();
    multiSourceMapping.destination_schema = document.getElementById('msDestSchema').value.trim();
    multiSourceMapping.destination_table = document.getElementById('msDestTable').value.trim();

    // Validation
    if (!multiSourceMapping.name || !multiSourceMapping.destination_schema || !multiSourceMapping.destination_table) {
        showAlert('warning', 'Please fill in mapping name and destination details');
        return;
    }

    if (multiSourceMapping.sources.length < 2) {
        showAlert('warning', 'Please add at least 2 sources');
        return;
    }

    if (multiSourceMapping.merge_pattern.operations.length === 0) {
        showAlert('warning', 'Please add at least one merge operation');
        return;
    }

    if (multiSourceMapping.merge_pattern.output_columns.length === 0) {
        showAlert('warning', 'Please add at least one output column mapping');
        return;
    }

    showLoading('Saving multi-source mapping...');

    try {
        const response = await fetch(`${API_BASE}/admin/mapping/create`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(multiSourceMapping)
        });

        const result = await response.json();
        hideLoading();

        if (result.success || response.ok) {
            showAlert('success', 'Multi-source mapping saved successfully!');
            bootstrap.Modal.getInstance(document.getElementById('multiSourceModal')).hide();
            loadMappings(); // Reload mappings list
        } else {
            showAlert('danger', result.message || 'Failed to save mapping');
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error saving mapping: ${error.message}`);
    }
}

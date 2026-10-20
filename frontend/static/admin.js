// Admin page JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // Toggle credentials fields based on Windows Auth checkbox
    srcWindowsAuth = document.getElementById('srcWindowsAuth');
    if(srcWindowsAuth) {
        srcWindowsAuth.addEventListener('change', function() {
            document.getElementById('srcCredentials').style.display = this.checked ? 'none' : 'block';
        });
    }

    destWindowsAuth = document.getElementById('destWindowsAuth');
    if(destWindowsAuth) {
        destWindowsAuth.addEventListener('change', function() {
            document.getElementById('destCredentials').style.display = this.checked ? 'none' : 'block';
        });
    }

    // Save connection
    sourceConnectionForm = document.getElementById('sourceConnectionForm');
    if(sourceConnectionForm) {
        sourceConnectionForm.addEventListener('submit', async function(e) {
            e.preventDefault();
            await saveConnection('source');
        });
    }

    destConnectionForm = document.getElementById('destConnectionForm');
    if(destConnectionForm) {
        destConnectionForm.addEventListener('submit', async function(e) {
            e.preventDefault();
            await saveConnection('destination');
        });
    }

    // Load initial data when tabs are clicked
    cdcTab = document.getElementById('cdc-tab');
    if(cdcTab) {
        cdcTab.addEventListener('click', loadConnectionsForCDC);
    }
    mappingsTab = document.getElementById('mappings-tab');
    if(mappingsTab) {
        mappingsTab.addEventListener('click', loadMappings);
    }
    worksetsTab = document.getElementById('worksets-tab');
    if(worksetsTab) {
        worksetsTab.addEventListener('click', loadWorksets);
    }
});

// Test connection
async function testConnection(type) {
    const prefix = type === 'source' ? 'src' : 'dest';
    const statusDiv = document.getElementById(`${prefix}ConnectionStatus`);
    
    const config = {
        name: document.getElementById(`${prefix}Name`).value,
        server: document.getElementById(`${prefix}Server`).value,
        port: parseInt(document.getElementById(`${prefix}Port`).value),
        database: document.getElementById(`${prefix}Database`).value,
        use_windows_auth: document.getElementById(`${prefix}WindowsAuth`).checked,
        username: document.getElementById(`${prefix}Username`).value || null,
        password: document.getElementById(`${prefix}Password`).value || null
    };
    
    try {
        statusDiv.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> Testing connection...';
        
        const response = await fetch(`${API_BASE}/admin/connect/test`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        
        if (result.success) {
            statusDiv.innerHTML = `<div class="alert alert-success">${result.message}</div>`;
        } else {
            statusDiv.innerHTML = `<div class="alert alert-danger">${result.message}</div>`;
        }
    } catch (error) {
        statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}



async function saveConnection(type) {
    const prefix = type === 'source' ? 'src' : 'dest';
    
    const config = {
        name: document.getElementById(`${prefix}Name`).value,
        server: document.getElementById(`${prefix}Server`).value,
        port: parseInt(document.getElementById(`${prefix}Port`).value),
        database: document.getElementById(`${prefix}Database`).value,
        use_windows_auth: document.getElementById(`${prefix}WindowsAuth`).checked,
        username: document.getElementById(`${prefix}Username`).value || null,
        password: document.getElementById(`${prefix}Password`).value || null
    };
    
    try {
        const response = await fetch(`${API_BASE}/admin/connect/set?connection_type=${type}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', `${type} connection saved successfully`);
        } else {
            showAlert('danger', `Error saving ${type} connection`);
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// CDC Management
// Load connections for CDC management
async function loadConnectionsForCDC() {
    const select = document.getElementById('cdcConnectionSelector');
    if (!select) return;

    try {
        const response = await fetch(`${API_BASE}/admin/connection/list`);
        if (!response.ok) {
            console.error('Failed to load connections');
            return;
        }

        const connections = await response.json();

        select.innerHTML = '<option value="">-- Select a connection --</option>';
        connections.forEach(conn => {
            const option = document.createElement('option');
            option.value = conn.id;
            const authType = conn.use_windows_auth ? 'Windows Auth' : 'SQL Auth';
            option.textContent = `${conn.name} (${conn.server}/${conn.database}) - ${authType}`;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading connections:', error);
        showAlert('danger', `Error loading connections: ${error.message}`);
    }
}

// Get selected connection config
async function getSelectedCDCConnection() {
    const connectionId = document.getElementById('cdcConnectionSelector').value;
    if (!connectionId) {
        showAlert('warning', 'Please select a connection first');
        return null;
    }

    try {
        const response = await fetch(`${API_BASE}/admin/connection/${connectionId}`);
        if (!response.ok) throw new Error('Failed to load connection');
        return await response.json();
    } catch (error) {
        showAlert('danger', `Error loading connection: ${error.message}`);
        return null;
    }
}

async function checkCDCStatus() {
    const statusDiv = document.getElementById('cdcStatus');
    const tablesDiv = document.getElementById('cdcTables');

    const connection = await getSelectedCDCConnection();
    if (!connection) return;

    try {
        statusDiv.innerHTML = '<div class="spinner-border spinner-border-sm"></div> Checking CDC status...';

        const response = await fetch(`${API_BASE}/admin/cdc/check-status`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({connection})
        });
        const result = await response.json();

        if (result.cdc_enabled) {
            statusDiv.innerHTML = `<div class="alert alert-success">CDC is enabled on database: ${result.database}</div>`;

            if (result.tables && result.tables.length > 0) {
                let tableHtml = '<table class="table"><thead><tr><th>Schema</th><th>Table</th><th>Capture Instance</th></tr></thead><tbody>';
                result.tables.forEach(table => {
                    tableHtml += `<tr><td>${table.source_schema}</td><td>${table.source_table}</td><td>${table.capture_instance}</td></tr>`;
                });
                tableHtml += '</tbody></table>';
                tablesDiv.innerHTML = tableHtml;
            } else {
                tablesDiv.innerHTML = '<p class="text-muted">No tables have CDC enabled</p>';
            }
        } else {
            statusDiv.innerHTML = `<div class="alert alert-warning">CDC is NOT enabled on database: ${result.database}</div>`;
            tablesDiv.innerHTML = '<p class="text-muted">Enable CDC on database first</p>';
        }
    } catch (error) {
        statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

async function enableDatabaseCDC() {
    const connection = await getSelectedCDCConnection();
    if (!connection) return;

    try {
        const response = await fetch(`${API_BASE}/admin/cdc/enable-database`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({connection})
        });

        const result = await response.json();

        if (result.success) {
            showAlert('success', result.message);
            checkCDCStatus();
        } else {
            showAlert('danger', result.message || 'Failed to enable CDC');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

async function enableTableCDC() {
    const connection = await getSelectedCDCConnection();
    if (!connection) return;

    const schema = document.getElementById('cdcSchema').value;
    const table = document.getElementById('cdcTable').value;

    if (!schema || !table) {
        showAlert('warning', 'Please enter schema and table name');
        return;
    }

    try {
        const response = await fetch(`${API_BASE}/admin/cdc/enable-table-with-connection`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                connection: connection,
                schema_name: schema,
                table_name: table
            })
        });

        const result = await response.json();

        if (result.success) {
            showAlert('success', result.message);
            checkCDCStatus();
        } else {
            showAlert('danger', result.message || 'Failed to enable CDC on table');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// Unified Mappings
async function loadMappings() {
    const listDiv = document.getElementById('mappingsList');
    const filterType = document.getElementById('mappingTypeFilter')?.value || '';
    
    try {
        listDiv.innerHTML = '<div class="spinner-border"></div> Loading mappings...';
        
        const url = filterType 
            ? `${API_BASE}/admin/mapping/list?mapping_type=${filterType}`
            : `${API_BASE}/admin/mapping/list`;
        
        const response = await fetch(url);
        const mappings = await response.json();
        
        if (mappings.length === 0) {
            listDiv.innerHTML = '<p class="text-muted">No mappings configured. Create one to get started.</p>';
            return;
        }
        
        let html = '';
        mappings.forEach(mapping => {
            let typeBadge = '';
            if (mapping.is_multi_source) {
                typeBadge = '<span class="badge bg-success">Multi-Source</span>';
            } else if (mapping.mapping_type === 'table') {
                typeBadge = '<span class="badge bg-primary">Table</span>';
            } else {
                typeBadge = '<span class="badge bg-info">SQL</span>';
            }

            let detailsHtml = '';

            if (mapping.is_multi_source) {
                // Multi-source mapping details
                const sourceCount = mapping.sources ? mapping.sources.length : 0;
                const operationCount = mapping.merge_pattern && mapping.merge_pattern.operations
                    ? mapping.merge_pattern.operations.length
                    : 0;

                detailsHtml = `
                    <strong>Sources:</strong> ${sourceCount} database${sourceCount !== 1 ? 's' : ''}<br>
                    <strong>Operations:</strong> ${operationCount} merge operation${operationCount !== 1 ? 's' : ''}<br>
                    ${mapping.sources && mapping.sources.length > 0
                        ? `<strong>Aliases:</strong> ${mapping.sources.map(s => s.alias).join(', ')}<br>`
                        : ''}
                `;
            } else if (mapping.mapping_type === 'table') {
                // Table mapping details
                let simpleCount = 0;
                let complexCount = 0;
                if (mapping.column_mappings) {
                    mapping.column_mappings.forEach(cm => {
                        if (cm.source_columns && cm.source_columns.length > 1) {
                            complexCount++;
                        } else if (cm.transformation || cm.transformation_type) {
                            complexCount++;
                        } else {
                            simpleCount++;
                        }
                    });
                }
                
                detailsHtml = `
                    <strong>Source:</strong> ${mapping.source_schema || 'N/A'}.${mapping.source_table || 'N/A'}<br>
                    <strong>Columns:</strong> ${mapping.column_mappings ? mapping.column_mappings.length : 0} mapped
                    ${simpleCount > 0 ? `<span class="badge bg-info ms-1">${simpleCount} direct</span>` : ''}
                    ${complexCount > 0 ? `<span class="badge bg-warning ms-1">${complexCount} transformed</span>` : ''}
                `;
            } else if (mapping.mapping_type === 'sql') {
                // SQL mapping details
                detailsHtml = `
                    <strong>Sync Mode:</strong> <span class="badge bg-secondary">${mapping.sync_mode || 'full'}</span><br>
                    <strong>Schedule:</strong> ${mapping.sync_schedule || 'Manual only'}<br>
                    ${mapping.key_columns && mapping.key_columns.length > 0 
                        ? `<strong>Key Columns:</strong> ${mapping.key_columns.join(', ')}<br>` 
                        : ''}
                `;
            }
            
            html += `
                <div class="card mb-3">
                    <div class="card-body">
                        <div class="d-flex justify-content-between align-items-start">
                            <div>
                                <h5 class="card-title">
                                    ${typeBadge} ${mapping.name || mapping.id}
                                </h5>
                                <p class="card-text mb-2">
                                    <strong>Destination:</strong> ${mapping.destination_schema}.${mapping.destination_table}<br>
                                    ${detailsHtml}
                                    <strong>Status:</strong> ${mapping.enabled ? '<span class="badge bg-success">Enabled</span>' : '<span class="badge bg-secondary">Disabled</span>'}
                                    ${mapping.assigned_worksets && mapping.assigned_worksets.length > 0 
                                        ? `<br><strong>Worksets:</strong> ${mapping.assigned_worksets.length} assigned` 
                                        : ''}
                                </p>
                            </div>
                            <div>
                                ${mapping.is_multi_source ? `
                                    <button class="btn btn-sm btn-success" onclick="startMultiSourceSync('${mapping.id}')">
                                        <i class="bi bi-play-fill"></i> Start Sync
                                    </button>
                                ` : ''}
                                <button class="btn btn-sm btn-info" onclick="viewMappingDetails('${mapping.id}')">
                                    <i class="bi bi-eye"></i> View
                                </button>
                                <button class="btn btn-sm btn-warning" onclick="showMappingWizardForEdit('${mapping.id}')">
                                    <i class="bi bi-pencil"></i> Edit
                                </button>
                                <button class="btn btn-sm btn-danger" onclick="deleteMapping('${mapping.id}')">
                                    <i class="bi bi-trash"></i> Delete
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });
        
        listDiv.innerHTML = html;
    } catch (error) {
        listDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

// ==================== MAPPING MODAL FUNCTIONALITY ====================

// Global variables to store column data
let sourceColumns = [];
let destColumns = [];
let columnMappingCounter = 0;

function showCreateMappingModal() {
    // Reset the form
    document.getElementById('createMappingForm').reset();
    document.getElementById('mapEnabled').checked = true;
    document.getElementById('mapSyncInserts').checked = true;
    document.getElementById('mapSyncUpdates').checked = true;
    document.getElementById('mapSyncDeletes').checked = true;
    
    // Clear previous data
    sourceColumns = [];
    destColumns = [];
    columnMappingCounter = 0;
    document.getElementById('columnMappingsContainer').innerHTML = '<p class="text-muted">Load source and destination columns, then add mappings.</p>';
    document.getElementById('mapId').value = '';
    
    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('createMappingModal'));
    modal.show();
    
    // Auto-load databases when modal opens
    loadSourceDatabases();
    loadDestDatabases();
}

// Load databases from source connection
async function loadSourceDatabases() {
    const dbSelect = document.getElementById('mapSourceDatabase');
    
    try {
        dbSelect.innerHTML = '<option value="">Loading databases...</option>';
        
        const response = await fetch(`${API_BASE}/admin/scan/databases?connection_type=source`);
        const databases = await response.json();
        
        if (databases && databases.length > 0) {
            dbSelect.innerHTML = '<option value="">Select database...</option>';
            databases.forEach(db => {
                const option = document.createElement('option');
                option.value = db.name;
                option.textContent = db.name + (db.cdc_enabled ? ' (CDC Enabled)' : '');
                // If this is the configured database, select it
                option.selected = db.cdc_enabled; // Select CDC-enabled by default
                dbSelect.appendChild(option);
            });
            
            // If a database is selected, show it
            if (dbSelect.value) {
                showAlert('success', `Source databases loaded. Using: ${dbSelect.value}`);
            }
        } else {
            dbSelect.innerHTML = '<option value="">No databases found</option>';
        }
    } catch (error) {
        dbSelect.innerHTML = '<option value="">Error loading databases</option>';
        showAlert('danger', `Error loading source databases: ${error.message}`);
    }
}

// Load databases from destination connection
async function loadDestDatabases() {
    const dbSelect = document.getElementById('mapDestDatabase');
    
    try {
        dbSelect.innerHTML = '<option value="">Loading databases...</option>';
        
        const response = await fetch(`${API_BASE}/admin/scan/databases?connection_type=destination`);
        const databases = await response.json();
        
        if (databases && databases.length > 0) {
            dbSelect.innerHTML = '<option value="">Select database...</option>';
            databases.forEach(db => {
                const option = document.createElement('option');
                option.value = db.name;
                option.textContent = db.name + (db.cdc_enabled ? ' (CDC Enabled)' : '');
                dbSelect.appendChild(option);
            });
            
            // Auto-select the first database or CDC-enabled one
            if (databases.length === 1) {
                dbSelect.value = databases[0].name;
            } else {
                const cdcDb = databases.find(db => db.cdc_enabled);
                if (cdcDb) {
                    dbSelect.value = cdcDb.name;
                }
            }
            
            if (dbSelect.value) {
                showAlert('success', `Destination databases loaded. Using: ${dbSelect.value}`);
            }
        } else {
            dbSelect.innerHTML = '<option value="">No databases found</option>';
        }
    } catch (error) {
        dbSelect.innerHTML = '<option value="">Error loading databases</option>';
        showAlert('danger', `Error loading destination databases: ${error.message}`);
    }
}

// Load tables from source database
async function loadSourceTables() {
    const schemaSelect = document.getElementById('mapSourceSchema');
    const tableSelect = document.getElementById('mapSourceTable');
    
    try {
        const response = await fetch(`${API_BASE}/admin/scan/tables?connection_type=source`);
        const result = await response.json();
        
        if (result.success && result.tables) {
            // Get unique schemas
            const schemas = [...new Set(result.tables.map(t => t.schema_name))];
            
            // Populate schema dropdown
            schemaSelect.innerHTML = '<option value="">Select schema...</option>';
            schemas.forEach(schema => {
                const option = document.createElement('option');
                option.value = schema;
                option.textContent = schema;
                schemaSelect.appendChild(option);
            });
            
            // Add event listener to load tables when schema is selected
            schemaSelect.onchange = function() {
                const selectedSchema = this.value;
                tableSelect.innerHTML = '<option value="">Select table...</option>';
                
                if (selectedSchema) {
                    const filteredTables = result.tables.filter(t => t.schema_name === selectedSchema);
                    filteredTables.forEach(table => {
                        const option = document.createElement('option');
                        option.value = table.table_name;
                        option.textContent = table.table_name;
                        tableSelect.appendChild(option);
                    });
                }
            };
            
            showAlert('success', 'Source tables loaded successfully');
        } else {
            showAlert('danger', 'Failed to load source tables');
        }
    } catch (error) {
        showAlert('danger', `Error loading source tables: ${error.message}`);
    }
}

// Load tables from destination database
async function loadDestTables() {
    const schemaSelect = document.getElementById('mapDestSchema');
    const tableSelect = document.getElementById('mapDestTable');
    
    try {
        const response = await fetch(`${API_BASE}/admin/scan/tables?connection_type=destination`);
        const result = await response.json();
        
        if (result.success && result.tables) {
            // Get unique schemas
            const schemas = [...new Set(result.tables.map(t => t.schema_name))];
            
            // Populate schema dropdown
            schemaSelect.innerHTML = '<option value="">Select schema...</option>';
            schemas.forEach(schema => {
                const option = document.createElement('option');
                option.value = schema;
                option.textContent = schema;
                schemaSelect.appendChild(option);
            });
            
            // Add event listener to load tables when schema is selected
            schemaSelect.onchange = function() {
                const selectedSchema = this.value;
                tableSelect.innerHTML = '<option value="">Select table...</option>';
                
                if (selectedSchema) {
                    const filteredTables = result.tables.filter(t => t.schema_name === selectedSchema);
                    filteredTables.forEach(table => {
                        const option = document.createElement('option');
                        option.value = table.table_name;
                        option.textContent = table.table_name;
                        tableSelect.appendChild(option);
                    });
                }
            };
            
            showAlert('success', 'Destination tables loaded successfully');
        } else {
            showAlert('danger', 'Failed to load destination tables');
        }
    } catch (error) {
        showAlert('danger', `Error loading destination tables: ${error.message}`);
    }
}

// Load columns from source table
async function loadSourceColumns() {
    const schema = document.getElementById('mapSourceSchema').value;
    const table = document.getElementById('mapSourceTable').value;
    
    if (!schema || !table) {
        showAlert('warning', 'Please select schema and table first');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/scan/columns?connection_type=source&schema=${schema}&table=${table}`);
        const result = await response.json();
        
        if (result.success && result.columns) {
            sourceColumns = result.columns;
            showAlert('success', `Loaded ${sourceColumns.length} source columns`);
            updateMappingId();
        } else {
            showAlert('danger', 'Failed to load source columns');
        }
    } catch (error) {
        showAlert('danger', `Error loading source columns: ${error.message}`);
    }
}

// Load columns from destination table
async function loadDestColumns() {
    const schema = document.getElementById('mapDestSchema').value;
    const table = document.getElementById('mapDestTable').value;
    
    if (!schema || !table) {
        showAlert('warning', 'Please select schema and table first');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/scan/columns?connection_type=destination&schema=${schema}&table=${table}`);
        const result = await response.json();
        
        if (result.success && result.columns) {
            destColumns = result.columns;
            showAlert('success', `Loaded ${destColumns.length} destination columns`);
            updateMappingId();
        } else {
            showAlert('danger', 'Failed to load destination columns');
        }
    } catch (error) {
        showAlert('danger', `Error loading destination columns: ${error.message}`);
    }
}

// Auto-generate mapping ID
function updateMappingId() {
    const sourceTable = document.getElementById('mapSourceTable').value;
    const destTable = document.getElementById('mapDestTable').value;
    
    if (sourceTable && destTable) {
        const mappingId = `map_${sourceTable}_to_${destTable}_${Date.now()}`;
        document.getElementById('mapId').value = mappingId;
    }
}

// Auto-map columns with matching names
function autoMapColumns() {
    if (sourceColumns.length === 0 || destColumns.length === 0) {
        showAlert('warning', 'Please load source and destination columns first');
        return;
    }
    
    clearColumnMappings();
    
    let mappedCount = 0;
    sourceColumns.forEach(srcCol => {
        const matchingDestCol = destColumns.find(destCol => 
            destCol.column_name.toLowerCase() === srcCol.column_name.toLowerCase()
        );
        
        if (matchingDestCol) {
            // Use array format for consistency with new UI
            addColumnMapping([srcCol.column_name], matchingDestCol.column_name);
            mappedCount++;
        }
    });
    
    if (mappedCount > 0) {
        showAlert('success', `Auto-mapped ${mappedCount} matching columns`);
    } else {
        showAlert('info', 'No matching columns found');
    }
}

// Add a single column mapping row with advanced options
function addColumnMapping(preSelectedSources = [], destCol = '', transformation = '', transformationType = '', ignoreChanges = false, autoGenerate = 'none', autoGenerateExpr = '') {
    if (sourceColumns.length === 0 || destColumns.length === 0) {
        showAlert('warning', 'Please load source and destination columns first');
        return;
    }
    
    const container = document.getElementById('columnMappingsContainer');
    
    // If this is the first mapping, clear the placeholder text
    if (columnMappingCounter === 0) {
        container.innerHTML = '';
    }
    
    const mappingId = columnMappingCounter++;
    
    // Convert single source to array for backward compatibility
    if (typeof preSelectedSources === 'string' && preSelectedSources) {
        preSelectedSources = [preSelectedSources];
    }
    
    const mappingRow = document.createElement('div');
    mappingRow.className = 'card mb-3 column-mapping-row';
    mappingRow.id = `mapping-row-${mappingId}`;
    mappingRow.innerHTML = `
        <div class="card-body">
            <div class="row align-items-start">
                <!-- Source Columns (Multi-select with checkboxes) -->
        <div class="col-md-5">
                    <label class="form-label fw-bold">Source Column(s)</label>
                    <div class="border rounded p-2" style="max-height: 200px; overflow-y: auto; background-color: #f8f9fa;">
                        ${sourceColumns.map(col => {
                            const isChecked = preSelectedSources.includes(col.column_name);
                            return `
                                <div class="form-check">
                                    <input class="form-check-input src-col-checkbox-${mappingId}" 
                                           type="checkbox" 
                                           value="${col.column_name}" 
                                           id="src-${mappingId}-${col.column_name}"
                                           ${isChecked ? 'checked' : ''}
                                           onchange="updateMappingPreview(${mappingId})">
                                    <label class="form-check-label small" for="src-${mappingId}-${col.column_name}">
                                        ${col.column_name} <span class="text-muted">(${col.data_type})</span>
                                    </label>
        </div>
                            `;
                        }).join('')}
        </div>
                    <small class="text-muted">Select one or more columns</small>
                    
                    <!-- Column Control Options -->
                    <div class="mt-3 p-2 border rounded bg-white">
                        <label class="form-label small fw-bold mb-2">Column Controls</label>
                        <div class="form-check form-switch">
                            <input class="form-check-input" type="checkbox" id="ignore-changes-${mappingId}" 
                                   ${ignoreChanges ? 'checked' : ''} onchange="updateMappingPreview(${mappingId})">
                            <label class="form-check-label small" for="ignore-changes-${mappingId}">
                                <i class="bi bi-x-circle text-warning"></i> Ignore Changes
                            </label>
                            <div><small class="text-muted">Skip syncing changes for these columns</small></div>
                        </div>
                    </div>
                </div>
                
                <div class="col-md-1 text-center d-flex align-items-center justify-content-center">
                    <i class="bi bi-arrow-right fs-4"></i>
                </div>
                
                <!-- Destination Column -->
        <div class="col-md-5">
                    <label class="form-label fw-bold">Destination Column</label>
                    <select class="form-select form-select-sm mb-2" id="dest-col-${mappingId}" required onchange="updateMappingPreview(${mappingId})">
                <option value="">Select destination column...</option>
                ${destColumns.map(col => `
                    <option value="${col.column_name}" ${col.column_name === destCol ? 'selected' : ''}>
                        ${col.column_name} (${col.data_type})
                    </option>
                `).join('')}
            </select>
                    
                    <!-- Transformation Type -->
                    <label class="form-label small">Transformation Type</label>
                    <select class="form-select form-select-sm mb-2" id="trans-type-${mappingId}" onchange="handleTransformationTypeChange(${mappingId})">
                        <option value="">Direct Mapping (No Transformation)</option>
                        <option value="json" ${transformationType === 'json' ? 'selected' : ''}>JSON Object</option>
                        <option value="concat" ${transformationType === 'concat' ? 'selected' : ''}>Concatenation</option>
                        <option value="custom" ${transformationType === 'custom' ? 'selected' : ''}>Custom SQL Expression</option>
                    </select>
                    
                    <!-- Custom Transformation Field -->
                    <div id="trans-custom-${mappingId}" style="display: ${transformationType === 'custom' ? 'block' : 'none'};">
                        <label class="form-label small">Custom SQL Expression</label>
                        <input type="text" class="form-control form-control-sm" id="trans-expr-${mappingId}" 
                               placeholder="e.g., CONCAT({col1}, ' - ', {col2})"
                               value="${transformation || ''}"
                               onchange="updateMappingPreview(${mappingId})">
                        <small class="text-muted">Use {col1}, {col2}, etc. as placeholders</small>
        </div>
                    
                    <!-- Auto-Generate Options -->
                    <div class="mt-2 p-2 border rounded bg-white">
                        <label class="form-label small fw-bold mb-2">Auto-Generate Value</label>
                        <select class="form-select form-select-sm mb-2" id="auto-gen-mode-${mappingId}" onchange="handleAutoGenerateChange(${mappingId})">
                            <option value="none" ${autoGenerate === 'none' ? 'selected' : ''}>No Auto-Generation</option>
                            <option value="on_insert" ${autoGenerate === 'on_insert' ? 'selected' : ''}>On Every Insert</option>
                            <option value="on_init" ${autoGenerate === 'on_init' ? 'selected' : ''}>On Init Only (Once)</option>
                        </select>
                        <div id="auto-gen-expr-div-${mappingId}" style="display: ${autoGenerate !== 'none' ? 'block' : 'none'};">
                            <input type="text" class="form-control form-control-sm" id="auto-gen-expr-${mappingId}" 
                                   placeholder="e.g., NEWID(), GETDATE(), CURRENT_TIMESTAMP"
                                   value="${autoGenerateExpr || ''}"
                                   onchange="updateMappingPreview(${mappingId})">
                            <small class="text-muted d-block">SQL expression to generate value</small>
                            <small class="text-muted">Common: NEWID(), GETDATE(), CURRENT_USER</small>
                        </div>
                    </div>
                    
                    <!-- Default Value -->
                    <div class="mt-2 p-2 border rounded bg-white">
                        <label class="form-label small fw-bold mb-2">Default Value</label>
                        <input type="text" class="form-control form-control-sm" id="default-value-${mappingId}" 
                               placeholder="e.g., 'UNKNOWN', 0, GETDATE()"
                               value=""
                               onchange="updateMappingPreview(${mappingId})">
                        <small class="text-muted">Value to use if source is NULL or missing</small>
                    </div>
                    
                    <!-- Preview -->
                    <div class="mt-2 p-2 bg-light border rounded" id="mapping-preview-${mappingId}">
                        <small class="text-muted">Mapping preview will appear here</small>
                    </div>
                </div>
                
        <div class="col-md-1 text-center">
                    <button type="button" class="btn btn-sm btn-danger" onclick="removeColumnMapping(${mappingId})" title="Remove mapping">
                <i class="bi bi-trash"></i>
            </button>
                </div>
            </div>
        </div>
    `;
    
    container.appendChild(mappingRow);
    
    // Update preview on creation
    updateMappingPreview(mappingId);
}

// Handle transformation type change
function handleTransformationTypeChange(mappingId) {
    const transType = document.getElementById(`trans-type-${mappingId}`).value;
    const customDiv = document.getElementById(`trans-custom-${mappingId}`);
    
    // Show/hide custom expression field
    if (transType === 'custom') {
        customDiv.style.display = 'block';
    } else {
        customDiv.style.display = 'none';
    }
    
    updateMappingPreview(mappingId);
}

// Handle auto-generate mode change
function handleAutoGenerateChange(mappingId) {
    const autoGenMode = document.getElementById(`auto-gen-mode-${mappingId}`).value;
    const exprDiv = document.getElementById(`auto-gen-expr-div-${mappingId}`);
    
    // Show/hide expression field based on mode
    if (autoGenMode !== 'none') {
        exprDiv.style.display = 'block';
    } else {
        exprDiv.style.display = 'none';
    }
    
    updateMappingPreview(mappingId);
}

// Update mapping preview
function updateMappingPreview(mappingId) {
    const previewDiv = document.getElementById(`mapping-preview-${mappingId}`);
    const destCol = document.getElementById(`dest-col-${mappingId}`).value;
    const transType = document.getElementById(`trans-type-${mappingId}`).value;
    const ignoreChanges = document.getElementById(`ignore-changes-${mappingId}`).checked;
    const autoGenMode = document.getElementById(`auto-gen-mode-${mappingId}`).value;
    
    // Get selected source columns
    const selectedSources = [];
    document.querySelectorAll(`.src-col-checkbox-${mappingId}:checked`).forEach(cb => {
        selectedSources.push(cb.value);
    });
    
    if (selectedSources.length === 0 || !destCol) {
        previewDiv.innerHTML = '<small class="text-muted">Select source and destination columns</small>';
        return;
    }
    
    let previewText = '';
    let badges = [];
    
    // Add control badges
    if (ignoreChanges) {
        badges.push('<span class="badge bg-warning">Ignore Changes</span>');
    }
    if (autoGenMode !== 'none') {
        const autoGenExpr = document.getElementById(`auto-gen-expr-${mappingId}`).value;
        const modeText = autoGenMode === 'on_insert' ? 'Auto-Gen (Insert)' : 'Auto-Gen (Init)';
        badges.push(`<span class="badge bg-info">${modeText}</span>`);
    }
    
    if (selectedSources.length === 1 && !transType && autoGenMode === 'none') {
        // Simple 1:1 mapping
        previewText = `<strong>${selectedSources[0]}</strong> → <strong>${destCol}</strong>`;
        if (badges.length > 0) {
            previewText += `<div class="mt-1">${badges.join(' ')}</div>`;
        }
    } else {
        // Complex mapping with transformation or controls
        let transformation = '';
        
        if (autoGenMode !== 'none') {
            const autoGenExpr = document.getElementById(`auto-gen-expr-${mappingId}`).value;
            transformation = autoGenExpr || '<i>Enter auto-generate expression</i>';
        } else if (transType === 'json') {
            const jsonPairs = selectedSources.map(col => `'${col}': ${col}`).join(', ');
            transformation = `JSON_OBJECT(${jsonPairs})`;
        } else if (transType === 'concat') {
            transformation = `CONCAT(${selectedSources.join(", ', ', ")})`;
        } else if (transType === 'custom') {
            const customExpr = document.getElementById(`trans-expr-${mappingId}`).value;
            transformation = customExpr || '<i>Enter custom expression</i>';
        } else {
            // Multiple columns without transformation
            transformation = `[${selectedSources.join(', ')}]`;
        }
        
        previewText = `
            <div><strong>Sources:</strong> ${selectedSources.join(', ')}</div>
            <div><strong>Destination:</strong> ${destCol}</div>
            <div><strong>Transform:</strong> <code class="small">${transformation}</code></div>
        `;
        
        if (badges.length > 0) {
            previewText += `<div class="mt-2">${badges.join(' ')}</div>`;
        }
    }
    
    previewDiv.innerHTML = previewText;
}

// Remove a column mapping row
function removeColumnMapping(mappingId) {
    const row = document.getElementById(`mapping-row-${mappingId}`);
    if (row) {
        row.remove();
    }
    
    // Check if there are any mappings left
    const container = document.getElementById('columnMappingsContainer');
    if (container.children.length === 0) {
        container.innerHTML = '<p class="text-muted">No column mappings. Click "Add Column Mapping" to start.</p>';
        columnMappingCounter = 0;
    }
}

// Clear all column mappings
function clearColumnMappings() {
    document.getElementById('columnMappingsContainer').innerHTML = '<p class="text-muted">No column mappings. Click "Add Column Mapping" to start.</p>';
    columnMappingCounter = 0;
}

// Save the mapping
async function saveMappingFromModal() {
    // Validate basic fields
    const sourceSchema = document.getElementById('mapSourceSchema').value;
    const sourceTable = document.getElementById('mapSourceTable').value;
    const destSchema = document.getElementById('mapDestSchema').value;
    const destTable = document.getElementById('mapDestTable').value;
    
    if (!sourceSchema || !sourceTable || !destSchema || !destTable) {
        showAlert('warning', 'Please select source and destination tables');
        return;
    }
    
    // Collect column mappings
    const columnMappings = [];
    const mappingRows = document.querySelectorAll('.column-mapping-row');
    
    if (mappingRows.length === 0) {
        showAlert('warning', 'Please add at least one column mapping');
        return;
    }
    
    let hasError = false;
    let errorMessage = '';
    
    mappingRows.forEach((row, index) => {
        const rowId = row.id.split('-')[2];
        
        // Get selected source columns (checkboxes)
        const selectedSources = [];
        document.querySelectorAll(`.src-col-checkbox-${rowId}:checked`).forEach(cb => {
            selectedSources.push(cb.value);
        });
        
        const destCol = document.getElementById(`dest-col-${rowId}`).value;
        const transType = document.getElementById(`trans-type-${rowId}`).value;
        const ignoreChanges = document.getElementById(`ignore-changes-${rowId}`).checked;
        const autoGenMode = document.getElementById(`auto-gen-mode-${rowId}`).value;
        const autoGenExpr = document.getElementById(`auto-gen-expr-${rowId}`)?.value || null;
        const defaultValue = document.getElementById(`default-value-${rowId}`)?.value || null;
        
        // Validation
        if (selectedSources.length === 0) {
            hasError = true;
            errorMessage = `Mapping ${index + 1}: Please select at least one source column`;
            return;
        }
        
        if (!destCol) {
            hasError = true;
            errorMessage = `Mapping ${index + 1}: Please select a destination column`;
            return;
        }
        
        // Validate auto-generate expression if mode is set
        if (autoGenMode !== 'none' && !autoGenExpr) {
            hasError = true;
            errorMessage = `Mapping ${index + 1}: Please enter an auto-generate expression`;
            return;
        }
        
        // Build transformation
        let transformation = null;
        let transformationType = null;
        
        if (transType) {
            transformationType = transType;
            
            if (transType === 'json') {
                // Generate JSON_OBJECT transformation
                const jsonPairs = selectedSources.map(col => `'${col}', ${col}`).join(', ');
                transformation = `JSON_OBJECT(${jsonPairs})`;
            } else if (transType === 'concat') {
                // Generate CONCAT transformation
                transformation = `CONCAT(${selectedSources.join(", ', ', ")})`;
            } else if (transType === 'custom') {
                // Use custom expression
                const customExpr = document.getElementById(`trans-expr-${rowId}`).value;
                if (!customExpr) {
                    hasError = true;
                    errorMessage = `Mapping ${index + 1}: Please enter a custom transformation expression`;
                    return;
                }
                transformation = customExpr;
                
                // Replace placeholders with actual column names
                selectedSources.forEach((col, idx) => {
                    const placeholder = `{col${idx + 1}}`;
                    transformation = transformation.replace(new RegExp(placeholder.replace(/[{}]/g, '\\$&'), 'g'), col);
                });
            }
        }
        
        // Create column mapping object
        const colMapping = {
            destination_column: destCol,
            transformation: transformation,
            transformation_type: transformationType,
            ignore_changes: ignoreChanges,
            auto_generate: autoGenMode,
            auto_generate_expression: autoGenExpr,
            default_value: defaultValue
        };
        
        // Add source column(s) - backward compatible
        if (selectedSources.length === 1) {
            colMapping.source_column = selectedSources[0];
        } else {
            colMapping.source_columns = selectedSources;
        }
        
        columnMappings.push(colMapping);
    });
    
    if (hasError) {
        showAlert('warning', errorMessage || 'Please complete all column mappings');
        return;
    }
    
    // Build the mapping object
    const useDuckDB = document.getElementById('mapUseDuckDB').checked;
    const performSnapshot = document.getElementById('mapPerformSnapshot').checked;
    const mapping = {
        id: document.getElementById('mapId').value || `map_${sourceTable}_to_${destTable}_${Date.now()}`,
        source_schema: sourceSchema,
        source_table: sourceTable,
        destination_schema: destSchema,
        destination_table: destTable,
        column_mappings: columnMappings,
        enabled: document.getElementById('mapEnabled').checked,
        sync_inserts: document.getElementById('mapSyncInserts').checked,
        sync_updates: document.getElementById('mapSyncUpdates').checked,
        sync_deletes: document.getElementById('mapSyncDeletes').checked,
        use_duckdb_transformation: useDuckDB,
        perform_initial_snapshot: performSnapshot
    };
    
    // Add DuckDB transformation details if enabled
    if (useDuckDB) {
        const scriptSelect = document.getElementById('mapDuckDBScript').value;
        const scriptContent = document.getElementById('mapDuckDBScriptContent').value;
        
        if (scriptSelect && scriptSelect !== '__inline__') {
            mapping.duckdb_script_name = scriptSelect;
        } else if (scriptContent && scriptContent.trim()) {
            mapping.duckdb_script_content = scriptContent;
        } else {
            showAlert('warning', 'DuckDB transformation enabled but no script provided');
            return;
        }
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/mapping/create`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(mapping)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', 'Mapping created successfully!');
            
            // Close the modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('createMappingModal'));
            modal.hide();
            
            // Refresh the mappings list
            loadMappings();
        } else {
            showAlert('danger', result.message || 'Failed to create mapping');
        }
    } catch (error) {
        showAlert('danger', `Error creating mapping: ${error.message}`);
    }
}

// View mapping details
async function viewMappingDetails(mappingId) {
    try {
        const response = await fetch(`${API_BASE}/admin/mapping/${mappingId}`);
        const mapping = await response.json();
        
        let detailsHtml = `
            <div class="mb-3">
                <h6>Source: ${mapping.source_schema}.${mapping.source_table}</h6>
                <h6>Destination: ${mapping.destination_schema}.${mapping.destination_table}</h6>
            </div>
            <hr>
            <h6 class="mb-3">Column Mappings:</h6>
            <div class="table-responsive">
                <table class="table table-sm table-bordered">
                    <thead>
                        <tr>
                            <th>Source Column(s)</th>
                            <th>Destination Column</th>
                            <th>Transformation</th>
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        mapping.column_mappings.forEach((cm, idx) => {
            let sourceDisplay = '';
            if (cm.source_columns && cm.source_columns.length > 0) {
                sourceDisplay = cm.source_columns.join(', ');
            } else if (cm.source_column) {
                sourceDisplay = cm.source_column;
            }
            
            let transformDisplay = '-';
            let controlBadges = [];
            
            // Add transformation display
            if (cm.transformation_type) {
                if (cm.transformation_type === 'json') {
                    transformDisplay = '<span class="badge bg-info">JSON Object</span>';
                } else if (cm.transformation_type === 'concat') {
                    transformDisplay = '<span class="badge bg-warning">Concatenation</span>';
                } else if (cm.transformation_type === 'custom') {
                    transformDisplay = '<span class="badge bg-primary">Custom</span>';
                }
                if (cm.transformation) {
                    transformDisplay += `<br><code class="small">${cm.transformation}</code>`;
                }
            } else if (cm.transformation) {
                transformDisplay = `<code class="small">${cm.transformation}</code>`;
            }
            
            // Add control badges
            if (cm.ignore_changes) {
                controlBadges.push('<span class="badge bg-warning"><i class="bi bi-x-circle"></i> Ignore Changes</span>');
            }
            if (cm.auto_generate && cm.auto_generate !== 'none') {
                const mode = cm.auto_generate === 'on_insert' ? 'On Insert' : 'On Init';
                controlBadges.push(`<span class="badge bg-info"><i class="bi bi-lightning"></i> Auto-Gen (${mode})</span>`);
                if (cm.auto_generate_expression) {
                    controlBadges.push(`<br><small><code>${cm.auto_generate_expression}</code></small>`);
                }
            }
            
            if (controlBadges.length > 0) {
                transformDisplay += '<br>' + controlBadges.join(' ');
            }
            
            detailsHtml += `
                <tr>
                    <td>${sourceDisplay}</td>
                    <td><strong>${cm.destination_column}</strong></td>
                    <td>${transformDisplay}</td>
                </tr>
            `;
        });
        
        detailsHtml += `
                    </tbody>
                </table>
            </div>
            <hr>
            <div class="row">
                <div class="col-md-4">
                    <strong>Sync Inserts:</strong> ${mapping.sync_inserts ? '✓' : '✗'}
                </div>
                <div class="col-md-4">
                    <strong>Sync Updates:</strong> ${mapping.sync_updates ? '✓' : '✗'}
                </div>
                <div class="col-md-4">
                    <strong>Sync Deletes:</strong> ${mapping.sync_deletes ? '✓' : '✗'}
                </div>
            </div>
            <hr>
            <div class="row">
                <div class="col-md-6">
                    <strong>Initial Snapshot:</strong> ${mapping.perform_initial_snapshot ? '<span class="badge bg-success">Enabled</span>' : '<span class="badge bg-secondary">Disabled</span>'}
                </div>
                <div class="col-md-6">
                    ${mapping.snapshot_completed_at ? `<strong>Completed:</strong> ${new Date(mapping.snapshot_completed_at).toLocaleString()}` : '<span class="text-muted">Not completed</span>'}
                </div>
            </div>
        `;
        
        // Show in alert or modal (for now, let's use a simple approach)
        // Create a temporary modal
        const existingModal = document.getElementById('mappingDetailsModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        const modalHtml = `
            <div class="modal fade" id="mappingDetailsModal" tabindex="-1">
                <div class="modal-dialog modal-lg">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">Mapping Details: ${mapping.id}</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            ${detailsHtml}
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        const modal = new bootstrap.Modal(document.getElementById('mappingDetailsModal'));
        modal.show();
        
        // Remove modal from DOM when hidden
        document.getElementById('mappingDetailsModal').addEventListener('hidden.bs.modal', function() {
            this.remove();
        });
        
    } catch (error) {
        showAlert('danger', `Error loading mapping details: ${error.message}`);
    }
}

async function deleteMapping(mappingId) {
    if (!confirm('Are you sure you want to delete this mapping?')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/mapping/${mappingId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', 'Mapping deleted successfully');
            loadMappings();
        } else {
            showAlert('danger', 'Failed to delete mapping');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// Working Sets
async function loadWorksets() {
    const listDiv = document.getElementById('worksetsList');
    
    try {
        listDiv.innerHTML = '<div class="spinner-border"></div> Loading working sets...';
        
        const response = await fetch(`${API_BASE}/admin/workset/list`);
        const worksets = await response.json();
        
        if (worksets.length === 0) {
            listDiv.innerHTML = '<p class="text-muted">No working sets configured. Click "Create Working Set" to get started.</p>';
            return;
        }
        
        let html = '';
        worksets.forEach(ws => {
            html += `
                <div class="card mb-3 ${ws.is_active ? 'border-success' : ''}">
                    <div class="card-body">
                        <h5 class="card-title">
                            <i class="bi bi-collection"></i> ${ws.name}
                            ${ws.is_active ? '<span class="badge bg-success ms-2"><i class="bi bi-check-circle"></i> Active</span>' : '<span class="badge bg-secondary ms-2">Inactive</span>'}
                        </h5>
                        <p class="card-text">
                            ${ws.description ? `<em>${ws.description}</em><br>` : ''}
                            <strong>ID:</strong> <code>${ws.id}</code><br>
                            <strong>Mappings:</strong> ${ws.mappings ? ws.mappings.length : (ws.table_mappings ? ws.table_mappings.length : 0) + (ws.sql_mappings ? ws.sql_mappings.length : 0)} mapping(s)<br>
                            <strong>Source:</strong> ${ws.source_connection.server}/${ws.source_connection.database}<br>
                            <strong>Destination:</strong> ${ws.destination_connection.server}/${ws.destination_connection.database}
                        </p>
                        <div class="btn-group" role="group">
                            <button class="btn btn-sm btn-info" onclick="viewWorksetDetails('${ws.id}')">
                                <i class="bi bi-eye"></i> View Details
                            </button>
                        <button class="btn btn-sm btn-primary" onclick="activateWorkset('${ws.id}')" ${ws.is_active ? 'disabled' : ''}>
                            <i class="bi bi-check-circle"></i> Activate
                        </button>
                        <button class="btn btn-sm btn-danger" onclick="deleteWorkset('${ws.id}')">
                            <i class="bi bi-trash"></i> Delete
                        </button>
                        </div>
                    </div>
                </div>
            `;
        });
        
        listDiv.innerHTML = html;
    } catch (error) {
        listDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

async function viewWorksetDetails(worksetId) {
    try {
        const response = await fetch(`${API_BASE}/admin/workset/${worksetId}`);
        const ws = await response.json();
        
        let detailsHtml = `
            <div class="mb-3">
                <h6><i class="bi bi-collection"></i> ${ws.name}</h6>
                <p class="text-muted">${ws.description || 'No description'}</p>
            </div>
            <hr>
            <h6 class="mb-3">Connections</h6>
            <div class="row mb-3">
                <div class="col-md-6">
                    <strong>Source Connection:</strong><br>
                    <small>
                        <strong>Server:</strong> ${ws.source_connection.server}:${ws.source_connection.port}<br>
                        <strong>Database:</strong> ${ws.source_connection.database}<br>
                        <strong>Auth:</strong> ${ws.source_connection.use_windows_auth ? 'Windows' : 'SQL'}
                    </small>
                </div>
                <div class="col-md-6">
                    <strong>Destination Connection:</strong><br>
                    <small>
                        <strong>Server:</strong> ${ws.destination_connection.server}:${ws.destination_connection.port}<br>
                        <strong>Database:</strong> ${ws.destination_connection.database}<br>
                        <strong>Auth:</strong> ${ws.destination_connection.use_windows_auth ? 'Windows' : 'SQL'}
                    </small>
                </div>
            </div>
            <hr>
            <h6 class="mb-3">Mappings (${ws.mappings ? ws.mappings.length : (ws.table_mappings ? ws.table_mappings.length : 0) + (ws.sql_mappings ? ws.sql_mappings.length : 0)})</h6>
            <ul class="list-group mb-3">
        `;
        
        const mappingIds = ws.mappings || (ws.table_mappings || []).concat(ws.sql_mappings || []);
        if (mappingIds && mappingIds.length > 0) {
            // Load mapping details
            try {
                const mappingPromises = mappingIds.map(id => 
                    fetch(`${API_BASE}/admin/mapping/${id}`).then(r => r.ok ? r.json() : null)
                );
                const mappings = await Promise.all(mappingPromises);
                
                mappings.forEach(mapping => {
                    if (mapping) {
                        const typeBadge = mapping.mapping_type === 'table' 
                            ? '<span class="badge bg-primary">Table</span>'
                            : '<span class="badge bg-info">SQL</span>';
                        detailsHtml += `<li class="list-group-item">${typeBadge} <code>${mapping.id}</code> - ${mapping.name || 'Unnamed'}</li>`;
                    }
                });
            } catch (e) {
                mappingIds.forEach(mappingId => {
                    detailsHtml += `<li class="list-group-item"><code>${mappingId}</code></li>`;
                });
            }
        } else {
            detailsHtml += `<li class="list-group-item text-muted">No mappings assigned</li>`;
        }
        
        detailsHtml += `
            </ul>
            <hr>
            <div class="row">
                <div class="col-md-12">
                    <strong>Status:</strong> ${ws.is_active ? '<span class="badge bg-success">Active</span>' : '<span class="badge bg-secondary">Inactive</span>'}<br>
                    <strong>Created:</strong> ${new Date(ws.created_at).toLocaleString()}<br>
                    <strong>Updated:</strong> ${new Date(ws.updated_at).toLocaleString()}
                </div>
            </div>
        `;
        
        // Create temporary modal for details
        const existingModal = document.getElementById('worksetDetailsModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        const modalHtml = `
            <div class="modal fade" id="worksetDetailsModal" tabindex="-1">
                <div class="modal-dialog modal-lg">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title">Working Set Details: ${ws.id}</h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                        </div>
                        <div class="modal-body">
                            ${detailsHtml}
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        const modal = new bootstrap.Modal(document.getElementById('worksetDetailsModal'));
        modal.show();
        
        // Remove modal from DOM when hidden
        document.getElementById('worksetDetailsModal').addEventListener('hidden.bs.modal', function() {
            this.remove();
        });
        
        
    } catch (error) {
        showAlert('danger', `Error loading working set details: ${error.message}`);
    }
}

// Load SQL mappings assigned to a workset
async function loadSQLMappingsForWorkset(worksetId) {
    try {
        const response = await fetch(`${API_BASE}/admin/sql-mapping/list`);
        const allMappings = await response.json();
        
        const assignedMappings = allMappings.filter(m => 
            m.assigned_worksets && m.assigned_worksets.includes(worksetId)
        );
        
        const container = document.getElementById('worksetSQLMappingsList');
        if (!container) return;
        
        if (assignedMappings.length === 0) {
            container.innerHTML = '<p class="text-muted small">No SQL mappings assigned to this workset</p>';
            return;
        }
        
        let html = '<ul class="list-group">';
        assignedMappings.forEach(mapping => {
            html += `
                <li class="list-group-item">
                    <strong>${mapping.name}</strong> (<code>${mapping.id}</code>)
                    <br><small class="text-muted">
                        ${mapping.destination_schema}.${mapping.destination_table} | 
                        Mode: ${mapping.sync_mode} | 
                        ${mapping.sync_schedule ? `Schedule: <code>${mapping.sync_schedule}</code>` : 'Manual'}
                    </small>
                </li>
            `;
        });
        html += '</ul>';
        container.innerHTML = html;
        
    } catch (error) {
        console.error('Error loading SQL mappings for workset:', error);
        const container = document.getElementById('worksetSQLMappingsList');
        if (container) {
            container.innerHTML = '<p class="text-danger small">Error loading SQL mappings</p>';
        }
    }
}

function showCreateWorksetModal() {
    // Reset the form
    document.getElementById('createWorksetForm').reset();
    document.getElementById('wsSetActive').checked = false;
    document.getElementById('wsSrcWindowsAuth').checked = true;
    document.getElementById('wsDestWindowsAuth').checked = true;
    document.getElementById('wsSrcPort').value = '1433';
    document.getElementById('wsDestPort').value = '1433';
    
    // Hide credentials initially
    document.getElementById('wsSrcCredentials').style.display = 'none';
    document.getElementById('wsDestCredentials').style.display = 'none';
    
    // Clear status
    document.getElementById('wsSrcConnectionStatus').innerHTML = '';
    document.getElementById('wsDestConnectionStatus').innerHTML = '';
    
    // Clear mappings
    document.getElementById('wsMappingsContainer').innerHTML = '<p class="text-muted">Click "Load Available Mappings" to see available table mappings</p>';
    document.getElementById('wsMappingCount').textContent = '0';
    
    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('createWorksetModal'));
    modal.show();
}

function toggleWsSourceCredentials() {
    const useWindowsAuth = document.getElementById('wsSrcWindowsAuth').checked;
    document.getElementById('wsSrcCredentials').style.display = useWindowsAuth ? 'none' : 'block';
}

function toggleWsDestCredentials() {
    const useWindowsAuth = document.getElementById('wsDestWindowsAuth').checked;
    document.getElementById('wsDestCredentials').style.display = useWindowsAuth ? 'none' : 'block';
}

async function testWorksetConnection(type) {
    const prefix = type === 'source' ? 'wsSrc' : 'wsDest';
    const statusDiv = document.getElementById(`${prefix}ConnectionStatus`);
    
    const config = {
        name: document.getElementById(`${prefix}Name`).value,
        server: document.getElementById(`${prefix}Server`).value,
        port: parseInt(document.getElementById(`${prefix}Port`).value),
        database: document.getElementById(`${prefix}Database`).value,
        use_windows_auth: document.getElementById(`${prefix}WindowsAuth`).checked,
        username: document.getElementById(`${prefix}Username`).value || null,
        password: document.getElementById(`${prefix}Password`).value || null
    };
    
    try {
        statusDiv.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> Testing connection...';
        
        const response = await fetch(`${API_BASE}/admin/connect/test`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        
        if (result.success) {
            statusDiv.innerHTML = `<div class="alert alert-success">${result.message}</div>`;
        } else {
            statusDiv.innerHTML = `<div class="alert alert-danger">${result.message}</div>`;
        }
    } catch (error) {
        statusDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

async function loadAvailableMappingsForWorkset() {
    const container = document.getElementById('wsMappingsContainer');
    
    try {
        container.innerHTML = '<div class="spinner-border"></div> Loading mappings...';
        
        const response = await fetch(`${API_BASE}/admin/mapping/list`);
        const mappings = await response.json();
        
        if (mappings.length === 0) {
            container.innerHTML = '<p class="text-muted">No mappings available. Create mappings first.</p>';
            return;
        }
        
        let html = '';
        mappings.forEach(mapping => {
            const typeBadge = mapping.mapping_type === 'table' 
                ? '<span class="badge bg-primary">Table</span>'
                : '<span class="badge bg-info">SQL</span>';
            
            let detailsText = '';
            if (mapping.mapping_type === 'table') {
                detailsText = `${mapping.source_schema || 'N/A'}.${mapping.source_table || 'N/A'} → ${mapping.destination_schema}.${mapping.destination_table} (${mapping.column_mappings ? mapping.column_mappings.length : 0} columns)`;
            } else {
                detailsText = `SQL Query → ${mapping.destination_schema}.${mapping.destination_table} (${mapping.sync_mode || 'full'} mode)`;
            }
            
            html += `
                <div class="form-check">
                    <input class="form-check-input ws-mapping-checkbox" type="checkbox" 
                           value="${mapping.id}" id="ws-map-${mapping.id}" 
                           onchange="updateWorksetMappingCount()">
                    <label class="form-check-label" for="ws-map-${mapping.id}">
                        ${typeBadge} <strong>${mapping.name || mapping.id}</strong><br>
                        <small class="text-muted">${detailsText}</small>
                    </label>
                </div>
                <hr class="my-2">
            `;
        });
        
        container.innerHTML = html;
        updateWorksetMappingCount();
        
    } catch (error) {
        container.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

function updateWorksetMappingCount() {
    const checkboxes = document.querySelectorAll('.ws-mapping-checkbox:checked');
    document.getElementById('wsMappingCount').textContent = checkboxes.length;
}

async function saveWorksetFromModal() {
    // Validate basic fields
    const wsId = document.getElementById('wsId').value;
    const wsName = document.getElementById('wsName').value;
    
    if (!wsId || !wsName) {
        showAlert('warning', 'Please fill in Working Set ID and Name');
        return;
    }
    
    // Build source connection
    const sourceConnection = {
        name: document.getElementById('wsSrcName').value,
        server: document.getElementById('wsSrcServer').value,
        port: parseInt(document.getElementById('wsSrcPort').value),
        database: document.getElementById('wsSrcDatabase').value,
        use_windows_auth: document.getElementById('wsSrcWindowsAuth').checked,
        username: document.getElementById('wsSrcUsername').value || null,
        password: document.getElementById('wsSrcPassword').value || null
    };
    
    // Build destination connection
    const destinationConnection = {
        name: document.getElementById('wsDestName').value,
        server: document.getElementById('wsDestServer').value,
        port: parseInt(document.getElementById('wsDestPort').value),
        database: document.getElementById('wsDestDatabase').value,
        use_windows_auth: document.getElementById('wsDestWindowsAuth').checked,
        username: document.getElementById('wsDestUsername').value || null,
        password: document.getElementById('wsDestPassword').value || null
    };
    
    // Get selected mappings (unified)
    const selectedMappings = [];
    document.querySelectorAll('.ws-mapping-checkbox:checked').forEach(cb => {
        selectedMappings.push(cb.value);
    });
    
    if (selectedMappings.length === 0) {
        showAlert('warning', 'Please select at least one mapping');
        return;
    }
    
    // Build working set object with unified mappings
    const workset = {
        id: wsId,
        name: wsName,
        description: document.getElementById('wsDescription').value || null,
        source_connection: sourceConnection,
        destination_connection: destinationConnection,
        mappings: selectedMappings,  // Unified mappings list
        is_active: false,  // Will be set via activate endpoint if needed
        data_ingestion_check: document.getElementById('wsDataIngestionCheck').checked
    };
    
    try {
        const response = await fetch(`${API_BASE}/admin/workset/create`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(workset)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', 'Working set created successfully!');
            
            // Activate if requested
            const setActive = document.getElementById('wsSetActive').checked;
            if (setActive) {
                await activateWorkset(wsId);
            }
            
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('createWorksetModal'));
            modal.hide();
            
            // Refresh list
            loadWorksets();
        } else {
            showAlert('danger', result.message || 'Failed to create working set');
        }
    } catch (error) {
        showAlert('danger', `Error creating working set: ${error.message}`);
    }
}

async function activateWorkset(worksetId) {
    try {
        const response = await fetch(`${API_BASE}/admin/workset/activate/${worksetId}`, {
            method: 'PUT'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', 'Working set activated successfully');
            loadWorksets();
        } else {
            showAlert('danger', 'Failed to activate working set');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

async function deleteWorkset(worksetId) {
    if (!confirm('Are you sure you want to delete this working set?')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/workset/${worksetId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', 'Working set deleted successfully');
            loadWorksets();
        } else {
            showAlert('danger', 'Failed to delete working set');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// Utility function to show alerts
function showAlert(type, message) {
    const alertHtml = `
        <div class="alert alert-${type} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    // Create container if it doesn't exist
    let container = document.getElementById('alertContainer');
    if (!container) {
        container = document.createElement('div');
        container.id = 'alertContainer';
        container.className = 'position-fixed top-0 end-0 p-3';
        container.style.zIndex = '11';
        document.body.appendChild(container);
    }
    
    container.innerHTML = alertHtml;
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        container.innerHTML = '';
    }, 5000);
}



// ==================== DUCKDB TRANSFORMATION FUNCTIONS ====================

function toggleDuckDBOptions() {
    const checkbox = document.getElementById('mapUseDuckDB');
    const options = document.getElementById('duckdbOptions');
    
    if (checkbox.checked) {
        options.style.display = 'block';
        loadAvailableDuckDBScripts();
    } else {
        options.style.display = 'none';
    }
}

async function loadAvailableDuckDBScripts() {
    const select = document.getElementById('mapDuckDBScript');
    
    try {
        const response = await fetch(`${API_BASE}/admin/duckdb/scripts/list`);
        const scripts = await response.json();
        
        // Keep the inline option
        select.innerHTML = '<option value="">Select script...</option><option value="__inline__">Inline Script (Enter Below)</option>';
        
        // Add templates
        const templates = scripts.filter(s => s.category === 'template');
        if (templates.length > 0) {
            const templateGroup = document.createElement('optgroup');
            templateGroup.label = 'Templates';
            templates.forEach(script => {
                const option = document.createElement('option');
                option.value = script.name;
                option.textContent = `${script.name} - ${script.description}`;
                option.dataset.category = 'template';
                templateGroup.appendChild(option);
            });
            select.appendChild(templateGroup);
        }
        
        // Add custom scripts
        const customs = scripts.filter(s => s.category === 'custom');
        if (customs.length > 0) {
            const customGroup = document.createElement('optgroup');
            customGroup.label = 'Custom Scripts';
            customs.forEach(script => {
                const option = document.createElement('option');
                option.value = script.name;
                option.textContent = `${script.name} - ${script.description}`;
                option.dataset.category = 'custom';
                customGroup.appendChild(option);
            });
            select.appendChild(customGroup);
        }
        
    } catch (error) {
        console.error('Error loading DuckDB scripts:', error);
        showAlert('warning', 'Could not load DuckDB scripts list');
    }
}

async function loadDuckDBScriptPreview() {
    const select = document.getElementById('mapDuckDBScript');
    const textarea = document.getElementById('mapDuckDBScriptContent');
    const scriptName = select.value;
    
    if (!scriptName || scriptName === '__inline__') {
        // Clear for inline entry
        if (scriptName === '__inline__') {
            textarea.value = '';
            textarea.readOnly = false;
            textarea.focus();
        }
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/duckdb/scripts/${scriptName}`);
        const result = await response.json();
        
        if (result.success && result.content) {
            textarea.value = result.content;
            textarea.readOnly = true; // Make read-only for loaded scripts
            showAlert('success', `Loaded script: ${scriptName}`);
        } else {
            showAlert('danger', 'Could not load script content');
        }
    } catch (error) {
        console.error('Error loading script content:', error);
        showAlert('danger', `Error loading script: ${error.message}`);
    }
}

async function uploadDuckDBScript() {
    const fileInput = document.getElementById('mapDuckDBScriptFile');
    const textarea = document.getElementById('mapDuckDBScriptContent');
    
    if (!fileInput.files || fileInput.files.length === 0) {
        showAlert('warning', 'Please select a file to upload');
        return;
    }
    
    const file = fileInput.files[0];
    
    try {
        const content = await file.text();
        textarea.value = content;
        textarea.readOnly = false;
        
        // Set select to inline
        document.getElementById('mapDuckDBScript').value = '__inline__';
        
        showAlert('success', `Loaded script from file: ${file.name}`);
    } catch (error) {
        console.error('Error reading file:', error);
        showAlert('danger', `Error reading file: ${error.message}`);
    }
}

async function validateDuckDBScript() {
    const content = document.getElementById('mapDuckDBScriptContent').value;
    
    if (!content || !content.trim()) {
        showAlert('warning', 'Please enter a script to validate');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/admin/duckdb/scripts/validate`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({content: content})
        });
        
        const result = await response.json();
        
        if (result.valid) {
            showAlert('success', 'Script validation passed! ' + result.message);
        } else {
            showAlert('danger', 'Script validation failed: ' + result.message);
        }
    } catch (error) {
        console.error('Error validating script:', error);
        showAlert('danger', `Error validating script: ${error.message}`);
    }
}

// Load and display latency statistics
async function loadLatencyStats(mappingId = null, hours = 24) {
    try {
        const url = `${API_BASE}/operations/latency/stats${mappingId ? `?mapping_id=${mappingId}&hours=${hours}` : `?hours=${hours}`}`;
        const response = await fetch(url);
        const stats = await response.json();
        
        if (stats.error) {
            return null;
        }
        
        return stats;
    } catch (error) {
        console.error('Error loading latency stats:', error);
        return null;
    }
}

// Display latency statistics in a card
function displayLatencyStats(stats, containerId) {
    const container = document.getElementById(containerId);
    if (!container || !stats || stats.total_records === 0) {
        if (container) {
            container.innerHTML = '<div class="alert alert-info">No latency data available yet</div>';
        }
        return;
    }
    
    const html = `
        <div class="card">
            <div class="card-header">
                <h6 class="mb-0"><i class="bi bi-speedometer2"></i> Latency Statistics (Last ${stats.hours} hours)</h6>
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-md-3">
                        <div class="text-center">
                            <div class="h4 text-primary">${stats.total_records.toLocaleString()}</div>
                            <small class="text-muted">Total Records</small>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="text-center">
                            <div class="h4 text-success">${stats.avg_latency_ms} ms</div>
                            <small class="text-muted">Average Latency</small>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="text-center">
                            <div class="h4 text-info">${stats.median_latency_ms} ms</div>
                            <small class="text-muted">Median Latency</small>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="text-center">
                            <div class="h4 text-warning">${stats.p95_latency_ms} ms</div>
                            <small class="text-muted">95th Percentile</small>
                        </div>
                    </div>
                </div>
                ${stats.min_latency_ms !== null ? `
                <hr>
                <div class="row">
                    <div class="col-md-6">
                        <strong>Min Latency:</strong> ${stats.min_latency_ms} ms
                    </div>
                    <div class="col-md-6">
                        <strong>Max Latency:</strong> ${stats.max_latency_ms} ms
                    </div>
                </div>
                ` : ''}
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

function viewDuckDBScripts() {
    showAlert('info', 'DuckDB scripts are located in the duckdb_scripts/ directory. Templates in duckdb_scripts/templates/, custom scripts in duckdb_scripts/custom/.');
}

// ==================== MULTI-SOURCE SYNC FUNCTIONS ====================

async function startMultiSourceSync(mappingId) {
    try {
        showLoading('Starting multi-source synchronization...');

        const response = await fetch(`${API_BASE}/operations/multi-source/start?mapping_id=${mappingId}`, {
            method: 'POST'
        });

        const result = await response.json();
        hideLoading();

        if (result.success) {
            showAlert('success', result.message);
            // Refresh multi-source status
            updateMultiSourceStatus();
        } else {
            showAlert('danger', result.message || 'Failed to start multi-source sync');
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error starting multi-source sync: ${error.message}`);
    }
}

async function stopMultiSourceSync() {
    try {
        showLoading('Stopping multi-source synchronization...');

        const response = await fetch(`${API_BASE}/operations/multi-source/stop`, {
            method: 'POST'
        });

        const result = await response.json();
        hideLoading();

        if (result.success) {
            showAlert('success', result.message);
            // Refresh multi-source status
            updateMultiSourceStatus();
        } else {
            showAlert('danger', result.message || 'Failed to stop multi-source sync');
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error stopping multi-source sync: ${error.message}`);
    }
}

async function pauseMultiSourceSync() {
    try {
        const response = await fetch(`${API_BASE}/operations/multi-source/pause`, {
            method: 'POST'
        });

        const result = await response.json();

        if (result.success) {
            showAlert('success', result.message);
            updateMultiSourceStatus();
        } else {
            showAlert('danger', result.message || 'Failed to pause multi-source sync');
        }
    } catch (error) {
        showAlert('danger', `Error pausing multi-source sync: ${error.message}`);
    }
}

async function resumeMultiSourceSync() {
    try {
        const response = await fetch(`${API_BASE}/operations/multi-source/resume`, {
            method: 'POST'
        });

        const result = await response.json();

        if (result.success) {
            showAlert('success', result.message);
            updateMultiSourceStatus();
        } else {
            showAlert('danger', result.message || 'Failed to resume multi-source sync');
        }
    } catch (error) {
        showAlert('danger', `Error resuming multi-source sync: ${error.message}`);
    }
}

async function updateMultiSourceStatus() {
    try {
        const response = await fetch(`${API_BASE}/operations/multi-source/status`);
        const status = await response.json();

        renderMultiSourceStatus(status);
    } catch (error) {
        console.error('Error fetching multi-source status:', error);
    }
}

function renderMultiSourceStatus(status) {
    const container = document.getElementById('multiSourceStatusContainer');
    if (!container) return;

    if (!status.is_running) {
        container.innerHTML = `
            <div class="alert alert-secondary">
                <i class="bi bi-info-circle"></i> Multi-source sync is not running
            </div>
        `;
        return;
    }

    const statusBadge = status.is_paused ?
        '<span class="badge bg-warning">Paused</span>' :
        '<span class="badge bg-success">Running</span>';

    let html = `
        <div class="card">
            <div class="card-header bg-info text-white d-flex justify-content-between align-items-center">
                <div>
                    <i class="bi bi-diagram-3"></i> Multi-Source Sync Status
                    ${statusBadge}
                </div>
                <div class="btn-group btn-group-sm">
                    ${status.is_paused ?
                        `<button class="btn btn-sm btn-success" onclick="resumeMultiSourceSync()">
                            <i class="bi bi-play-fill"></i> Resume
                        </button>` :
                        `<button class="btn btn-sm btn-warning" onclick="pauseMultiSourceSync()">
                            <i class="bi bi-pause-fill"></i> Pause
                        </button>`
                    }
                    <button class="btn btn-sm btn-danger" onclick="stopMultiSourceSync()">
                        <i class="bi bi-stop-fill"></i> Stop
                    </button>
                </div>
            </div>
            <div class="card-body">
                <h6 class="card-title">${status.mapping_name || 'Unknown Mapping'}</h6>

                <div class="row mt-3">
                    <div class="col-md-6">
                        <strong>Sources:</strong> ${status.source_count || 0}
                        <ul class="list-unstyled mt-2">
                            ${(status.sources || []).map(src => `
                                <li>
                                    <span class="badge bg-primary">${src.alias}</span>
                                    ${src.schema}.${src.table}
                                    ${src.cdc_enabled ? '<i class="bi bi-check-circle text-success" title="CDC Enabled"></i>' : ''}
                                </li>
                            `).join('')}
                        </ul>
                    </div>
                    <div class="col-md-6">
                        <div><strong>Queue Size:</strong> ${status.queue_size || 0}</div>
                        <div><strong>Pending Batches:</strong> ${status.pending_batches || 0}</div>
                    </div>
                </div>

                ${status.sync_statistics ? `
                    <hr>
                    <h6>Synchronization Statistics</h6>
                    <div class="row">
                        <div class="col-md-3">
                            <div class="text-center">
                                <div class="display-6">${status.sync_statistics.events_processed || 0}</div>
                                <small class="text-muted">Events Processed</small>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="text-center">
                                <div class="display-6">${status.sync_statistics.inserts || 0}</div>
                                <small class="text-muted">Inserts</small>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="text-center">
                                <div class="display-6">${status.sync_statistics.updates || 0}</div>
                                <small class="text-muted">Updates</small>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="text-center">
                                <div class="display-6">${status.sync_statistics.errors || 0}</div>
                                <small class="text-muted text-danger">Errors</small>
                            </div>
                        </div>
                    </div>
                    <div class="row mt-2">
                        <div class="col-md-6">
                            <strong>Runtime:</strong> ${(status.sync_statistics.runtime_seconds || 0).toFixed(2)} seconds
                        </div>
                        <div class="col-md-6">
                            <strong>Events/Second:</strong> ${(status.sync_statistics.events_per_second || 0).toFixed(2)}
                        </div>
                    </div>
                ` : ''}
            </div>
        </div>
    `;

    container.innerHTML = html;
}

// Auto-refresh multi-source status every 5 seconds if running
setInterval(() => {
    if (document.getElementById('multiSourceStatusContainer')) {
        updateMultiSourceStatus();
    }
}, 5000);




// Unified Mapping Creation Wizard - Redesigned

// Wizard state
let currentStep = 1;
let wizardSourceColumns = [];
let wizardDestColumns = [];
let wizardColumnMappingCounter = 0;

// Wizard data model
let mappingData = {
    mapping_type: null,
    // Common fields
    id: '',
    name: '',
    destination_schema: '',
    destination_table: '',
    enabled: true,
    assigned_worksets: [],
    sync_mode: 'full',
    sync_schedule: null,
    batch_size: 1000,
    timeout_seconds: 300,
    // TABLE-specific fields
    source_schema: '',
    source_table: '',
    column_mappings: [],
    sync_deletes: true,
    sync_updates: true,
    sync_inserts: true,
    use_duckdb_transformation: false,
    duckdb_script_name: null,
    duckdb_script_content: null,
    perform_initial_snapshot: false,
    // SQL-specific fields
    source_query: '',
    insert_query: null,
    update_query: null,
    delete_query: null,
    key_columns: [],
    // Connection info for testing (not saved with mapping)
    _test_connection: null,
    _test_connection_type: 'source' // 'source' or 'workset'
};

// Initialize wizard
function initMappingWizard() {
    currentStep = 1;
    wizardSourceColumns = [];
    wizardDestColumns = [];
    wizardColumnMappingCounter = 0;

    mappingData = {
        mapping_type: null,
        id: '',
        name: '',
        destination_schema: '',
        destination_table: '',
        enabled: true,
        assigned_worksets: [],
        sync_mode: 'full',
        sync_schedule: null,
        batch_size: 1000,
        timeout_seconds: 300,
        source_schema: '',
        source_table: '',
        column_mappings: [],
        sync_deletes: true,
        sync_updates: true,
        sync_inserts: true,
        use_duckdb_transformation: false,
        duckdb_script_name: null,
        duckdb_script_content: null,
        perform_initial_snapshot: false,
        source_query: '',
        insert_query: null,
        update_query: null,
        delete_query: null,
        key_columns: [],
        _test_connection: null,
        _test_connection_type: 'source'
    };

    updateWizardUI();
}

// Show wizard modal
function showMappingWizard() {
    initMappingWizard();
    const modal = new bootstrap.Modal(document.getElementById('mappingWizardModal'));
    modal.show();
}

// Show wizard for editing
function showMappingWizardForEdit(mappingId) {
    initMappingWizard();
    loadMappingForEdit(mappingId);
    const modal = new bootstrap.Modal(document.getElementById('mappingWizardModal'));
    modal.show();
}

// Load mapping for editing
async function loadMappingForEdit(mappingId) {
    showLoading('Loading mapping...');
    try {
        const response = await fetch(`${API_BASE}/admin/mapping/${mappingId}`);
        if (!response.ok) throw new Error('Failed to load mapping');

        const mapping = await response.json();

        // Populate mappingData
        Object.keys(mapping).forEach(key => {
            if (mapping[key] !== null && mapping[key] !== undefined) {
                mappingData[key] = mapping[key];
            }
        });

        currentStep = 2; // Start at configuration step for editing
        updateWizardUI();
        populateWizardForm();
        hideLoading();
        showAlert('success', 'Mapping loaded successfully');
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error loading mapping: ${error.message}`);
    }
}

// Update wizard UI based on current step
function updateWizardUI() {
    // Update step indicators
    for (let i = 1; i <= 5; i++) {
        const stepEl = document.getElementById(`wizardStep${i}`);
        if (stepEl) {
            if (i < currentStep) {
                stepEl.classList.add('completed');
                stepEl.classList.remove('active');
            } else if (i === currentStep) {
                stepEl.classList.add('active');
                stepEl.classList.remove('completed');
            } else {
                stepEl.classList.remove('active', 'completed');
            }
        }

        const stepContent = document.getElementById(`wizardStepContent${i}`);
        if (stepContent) {
            stepContent.style.display = i === currentStep ? 'block' : 'none';
        }
    }

    // Update buttons
    const prevBtn = document.getElementById('wizardPrevBtn');
    const nextBtn = document.getElementById('wizardNextBtn');

    if (prevBtn) {
        prevBtn.disabled = currentStep === 1;
    }

    if (nextBtn) {
        if (currentStep === 5) {
            if (mappingData.id === '') {
                nextBtn.innerHTML = '<i class="bi bi-save"></i> Save Mapping';
                nextBtn.onclick = saveMappingFromWizard;
            } else {
                nextBtn.innerHTML = '<i class="bi bi-save"></i> Save Mapping';
                nextBtn.onclick ;
            }
        } else {
            nextBtn.innerHTML = 'Next <i class="bi bi-arrow-right"></i>';
            nextBtn.onclick = nextWizardStep;
        }
    }

    // Load step-specific data when step becomes active
    if (currentStep === 2) {
        // Load worksets for connection selection
        loadWorksetsForConnectionSelector();
    } else if (currentStep === 4) {
        // Load worksets for assignment
        loadWorksetsForWizard();
    } else if (currentStep === 5) {
        // Populate review content
        populateReviewContent();
    }
}

// Navigate to next step
function nextWizardStep() {
    // Collect current step data before validation
    collectStepData();

    if (validateCurrentStep()) {
        if (currentStep < 5) {
            currentStep++;
            updateWizardUI();
        }
    }
}

// Navigate to previous step
function prevWizardStep() {
    if (currentStep > 1) {
        collectStepData(); // Save current step data
        currentStep--;
        updateWizardUI();
    }
}

// Validate current step
function validateCurrentStep() {
    switch (currentStep) {
        case 1:
            // Step 1: Select mapping type
            if (!mappingData.mapping_type) {
                showAlert('warning', 'Please select a mapping type');
                return false;
            }
            return true;

        case 2:
            // Step 2: Configure based on type
            if (!mappingData.id || !mappingData.name) {
                showAlert('warning', 'Please provide Mapping ID and Name');
                return false;
            }

            if (!mappingData.destination_schema || !mappingData.destination_table) {
                showAlert('warning', 'Please provide destination schema and table');
                return false;
            }

            if (mappingData.mapping_type === 'table') {
                if (!mappingData.source_schema || !mappingData.source_table) {
                    showAlert('warning', 'Please provide source schema and table');
                    return false;
                }

                // Collect column mappings before validation
                collectWizardColumnMappings();

                if (!mappingData.column_mappings || mappingData.column_mappings.length === 0) {
                    showAlert('warning', 'Please add at least one column mapping. Use "Load Columns" and "Auto-map" to get started.');
                    return false;
                }
            } else if (mappingData.mapping_type === 'sql') {
                if (!mappingData.source_query) {
                    showAlert('warning', 'Please provide a source SQL query');
                    return false;
                }
            }
            return true;

        case 3:
            // Step 3: Sync options (always valid)
            return true;

        case 4:
            // Step 4: Assign to worksets (optional)
            return true;

        case 5:
            // Step 5: Review (always valid)
            return true;

        default:
            return true;
    }
}

// Populate wizard form from mappingData
function populateWizardForm() {
    // Step 1: Mapping type
    if (mappingData.mapping_type) {
        document.getElementById('wizardMappingType').value = mappingData.mapping_type;
        onMappingTypeChange();
    }

    // Common fields
    document.getElementById('wizardMappingId').value = mappingData.id || '';
    document.getElementById('wizardMappingName').value = mappingData.name || '';
    document.getElementById('wizardDestSchema').value = mappingData.destination_schema || '';
    document.getElementById('wizardDestTable').value = mappingData.destination_table || '';
    document.getElementById('wizardEnabled').checked = mappingData.enabled !== false;

    // TABLE-specific fields
    if (mappingData.mapping_type === 'table') {
        document.getElementById('wizardSrcSchema').value = mappingData.source_schema || '';
        document.getElementById('wizardSrcTable').value = mappingData.source_table || '';
        document.getElementById('wizardSyncDeletes').checked = mappingData.sync_deletes !== false;
        document.getElementById('wizardSyncUpdates').checked = mappingData.sync_updates !== false;
        document.getElementById('wizardSyncInserts').checked = mappingData.sync_inserts !== false;
        document.getElementById('wizardPerformSnapshot').checked = mappingData.perform_initial_snapshot === true;
        document.getElementById('wizardUseDuckDB').checked = mappingData.use_duckdb_transformation === true;
    }

    // SQL-specific fields
    if (mappingData.mapping_type === 'sql') {
        document.getElementById('wizardSourceQuery').value = mappingData.source_query || '';

        if (mappingData.key_columns && mappingData.key_columns.length > 0) {
            document.getElementById('wizardKeyColumns').value = mappingData.key_columns.join(', ');
        }

        if (mappingData.insert_query) {
            document.getElementById('wizardInsertQuery').value = mappingData.insert_query;
        }
        if (mappingData.update_query) {
            document.getElementById('wizardUpdateQuery').value = mappingData.update_query;
        }
    }

    // Step 3: Sync options
    document.getElementById('wizardSyncMode').value = mappingData.sync_mode || 'full';
    document.getElementById('wizardSyncSchedule').value = mappingData.sync_schedule || '';
    document.getElementById('wizardBatchSize').value = mappingData.batch_size || 1000;
    document.getElementById('wizardTimeout').value = mappingData.timeout_seconds || 300;
}

// Handle mapping type change
function onMappingTypeChange() {
    const mappingType = document.getElementById('wizardMappingType').value;
    mappingData.mapping_type = mappingType;

    // Show/hide type-specific sections in step 2
    const tableSection = document.getElementById('wizardTableSection');
    const sqlSection = document.getElementById('wizardSQLSection');

    if (mappingType === 'table') {
        if (tableSection) tableSection.style.display = 'block';
        if (sqlSection) sqlSection.style.display = 'none';
    } else if (mappingType === 'sql') {
        if (tableSection) tableSection.style.display = 'none';
        if (sqlSection) sqlSection.style.display = 'block';
    } else {
        if (tableSection) tableSection.style.display = 'none';
        if (sqlSection) sqlSection.style.display = 'none';
    }
}

// Collect data from current step
function collectStepData() {
    switch (currentStep) {
        case 1:
            mappingData.mapping_type = document.getElementById('wizardMappingType').value;
            break;

        case 2:
            // Common fields
            mappingData.id = document.getElementById('wizardMappingId').value.trim();
            mappingData.name = document.getElementById('wizardMappingName').value.trim();
            mappingData.destination_schema = document.getElementById('wizardDestSchema').value.trim();
            mappingData.destination_table = document.getElementById('wizardDestTable').value.trim();
            mappingData.enabled = document.getElementById('wizardEnabled').checked;

            // Collect connection info for testing
            const connType = document.getElementById('wizardConnectionType').value;
            mappingData._test_connection_type = connType;

            if (connType === 'library') {
                const connectionId = document.getElementById('wizardConnectionLibrary').value;
                mappingData._test_connection = {type: 'library', connection_id: connectionId};
            } else if (connType === 'workset') {
                const worksetId = document.getElementById('wizardConnectionWorkset').value;
                mappingData._test_connection = {type: 'workset', workset_id: worksetId};
            }

            if (mappingData.mapping_type === 'table') {
                // TABLE-specific
                mappingData.source_schema = document.getElementById('wizardSrcSchema').value.trim();
                mappingData.source_table = document.getElementById('wizardSrcTable').value.trim();
                mappingData.sync_deletes = document.getElementById('wizardSyncDeletes').checked;
                mappingData.sync_updates = document.getElementById('wizardSyncUpdates').checked;
                mappingData.sync_inserts = document.getElementById('wizardSyncInserts').checked;
                mappingData.perform_initial_snapshot = document.getElementById('wizardPerformSnapshot').checked;
                mappingData.use_duckdb_transformation = document.getElementById('wizardUseDuckDB').checked;

                // Column mappings collected separately
                collectWizardColumnMappings();
            } else if (mappingData.mapping_type === 'sql') {
                // SQL-specific
                mappingData.source_query = document.getElementById('wizardSourceQuery').value.trim();
                const keyCols = document.getElementById('wizardKeyColumns').value;
                mappingData.key_columns = keyCols ? keyCols.split(',').map(s => s.trim()).filter(s => s) : [];
                mappingData.insert_query = document.getElementById('wizardInsertQuery').value.trim() || null;
                mappingData.update_query = document.getElementById('wizardUpdateQuery').value.trim() || null;
            }
            break;

        case 3:
            // Sync options
            mappingData.sync_mode = document.getElementById('wizardSyncMode').value;
            mappingData.sync_schedule = document.getElementById('wizardSyncSchedule').value.trim() || null;
            mappingData.batch_size = parseInt(document.getElementById('wizardBatchSize').value) || 1000;
            mappingData.timeout_seconds = parseInt(document.getElementById('wizardTimeout').value) || 300;
            break;

        case 4:
            // Worksets
            const checkedWorksets = Array.from(document.querySelectorAll('.wizard-workset-checkbox:checked'))
                .map(cb => cb.value);
            mappingData.assigned_worksets = checkedWorksets;
            break;

        case 5:
            // Review - no data to collect
            break;
    }
}

// Save mapping from wizard
async function saveMappingFromWizard() {
    // Collect all step data
    for (let i = 1; i <= 4; i++) {
        const savedStep = currentStep;
        currentStep = i;
        collectStepData();
        currentStep = savedStep;
    }

    // Final validation
    if (!mappingData.id || !mappingData.name) {
        showAlert('danger', 'Mapping ID and Name are required');
        return;
    }

    if (mappingData.mapping_type === 'table' && (!mappingData.column_mappings || mappingData.column_mappings.length === 0)) {
        showAlert('danger', 'Table mappings require at least one column mapping');
        return;
    }

    if (mappingData.mapping_type === 'sql' && !mappingData.source_query) {
        showAlert('danger', 'SQL mappings require a source query');
        return;
    }

    // Remove test connection info before saving
    const dataToSave = {...mappingData};
    delete dataToSave._test_connection;
    delete dataToSave._test_connection_type;

    showLoading('Saving mapping...');
    try {
        const url = `${API_BASE}/admin/mapping/create`;
        const method = 'POST';

        const response = await fetch(url, {
            method: method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(dataToSave)
        });

        const result = await response.json();

        hideLoading();
        if (result.success || response.ok) {
            showAlert('success', result.message || 'Mapping saved successfully');

            // Close modal after a short delay to allow user to see success message
            setTimeout(() => {
                bootstrap.Modal.getInstance(document.getElementById('mappingWizardModal')).hide();
            }, 1000);

            // Refresh mappings list
            if (typeof loadMappings === 'function') {
                loadMappings();
            }
        } else {
            showAlert('danger', result.message || 'Failed to save mapping');
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error saving mapping: ${error.message}`);
    }
}

// Connection selector functions
function onWizardConnectionTypeChange() {
    const connType = document.getElementById('wizardConnectionType').value;
    const libraryDiv = document.getElementById('wizardConnectionLibraryDiv');
    const worksetDiv = document.getElementById('wizardConnectionWorksetDiv');

    if (connType === 'library') {
        libraryDiv.style.display = 'block';
        worksetDiv.style.display = 'none';
        loadConnectionsForWizard();
    } else if (connType === 'workset') {
        libraryDiv.style.display = 'none';
        worksetDiv.style.display = 'block';
        loadWorksetsForConnectionSelector();
    }
}

async function loadConnectionsForWizard() {
    const select = document.getElementById('wizardConnectionLibrary');
    if (!select) return;

    try {
        const response = await fetch(`${API_BASE}/admin/connection/list`);
        if (!response.ok) {
            console.error('Failed to load connections');
            return;
        }

        const connections = await response.json();

        select.innerHTML = '<option value="">-- Select Connection --</option>';
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

async function loadWorksetsForConnectionSelector() {
    const select = document.getElementById('wizardConnectionWorkset');
    if (!select) return;

    try {
        const response = await fetch(`${API_BASE}/admin/workset/list`);
        if (!response.ok) return;

        const worksets = await response.json();

        select.innerHTML = '<option value="">-- Select Working Set --</option>';
        worksets.forEach(ws => {
            const option = document.createElement('option');
            option.value = ws.id;
            option.textContent = `${ws.name} ${ws.is_active ? '(Active)' : ''}`;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading worksets:', error);
    }
}

// Column mapping functions for wizard
async function wizardLoadColumns() {
    // Get connection info
    const connType = document.getElementById('wizardConnectionType').value;
    let connectionConfig;

    if (connType === 'library') {
        const connectionId = document.getElementById('wizardConnectionLibrary').value;
        if (!connectionId) {
            showAlert('warning', 'Please select a connection from the library first');
            return;
        }

        // Fetch connection from library
        showLoading('Loading connection from library...');
        try {
            const response = await fetch(`${API_BASE}/admin/connection/${connectionId}`);
            if (!response.ok) throw new Error('Failed to load connection');
            connectionConfig = await response.json();
        } catch (error) {
            hideLoading();
            showAlert('danger', `Error loading connection: ${error.message}`);
            return;
        }
    } else if (connType === 'workset') {
        const worksetId = document.getElementById('wizardConnectionWorkset').value;
        if (!worksetId) {
            showAlert('warning', 'Please select a working set first');
            return;
        }

        // Fetch workset to get connection
        showLoading('Loading workset connection...');
        try {
            const response = await fetch(`${API_BASE}/admin/workset/${worksetId}`);
            if (!response.ok) throw new Error('Failed to load workset');
            const workset = await response.json();
            connectionConfig = workset.source_connection;
        } catch (error) {
            hideLoading();
            showAlert('danger', `Error loading workset: ${error.message}`);
            return;
        }
    } else {
        hideLoading();
        showAlert('danger', 'Invalid connection type selected');
        return;
    }

    // Get schema and table names
    const srcSchema = document.getElementById('wizardSrcSchema').value.trim();
    const srcTable = document.getElementById('wizardSrcTable').value.trim();
    const destSchema = document.getElementById('wizardDestSchema').value.trim();
    const destTable = document.getElementById('wizardDestTable').value.trim();

    if (!srcSchema || !srcTable || !destSchema || !destTable) {
        showAlert('warning', 'Please fill in source and destination schema/table names first');
        return;
    }

    showLoading('Loading table columns...');
    try {
        // Load source columns
        const srcResponse = await fetch(`${API_BASE}/admin/scan/columns?connection_type=source&schema=${srcSchema}&table=${srcTable}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(connectionConfig)
        });

        // For destination, we'll use the same connection for now (user can change in workset)
        const destResponse = await fetch(`${API_BASE}/admin/scan/columns?connection_type=source&schema=${destSchema}&table=${destTable}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(connectionConfig)
        });

        if (!srcResponse.ok || !destResponse.ok) {
            throw new Error('Failed to load columns');
        }

        wizardSourceColumns = await srcResponse.json();
        wizardDestColumns = await destResponse.json();

        hideLoading();
        showAlert('success', `Loaded ${wizardSourceColumns.length} source columns and ${wizardDestColumns.length} destination columns`);

        // Clear existing mappings
        wizardClearColumnMappings();

    } catch (error) {
        hideLoading();
        showAlert('danger', `Error loading columns: ${error.message}`);
    }
}

function wizardAutoMapColumns() {
    if (wizardSourceColumns.length === 0 || wizardDestColumns.length === 0) {
        showAlert('warning', 'Please load columns first');
        return;
    }

    wizardClearColumnMappings();

    let mappedCount = 0;
    wizardSourceColumns.forEach(srcCol => {
        const matchingDestCol = wizardDestColumns.find(destCol =>
            destCol.column_name.toLowerCase() === srcCol.column_name.toLowerCase()
        );

        if (matchingDestCol) {
            wizardAddColumnMapping([srcCol.column_name], matchingDestCol.column_name);
            mappedCount++;
        }
    });

    if (mappedCount > 0) {
        showAlert('success', `Auto-mapped ${mappedCount} matching columns`);
    } else {
        showAlert('info', 'No matching columns found');
    }
}

function wizardAddColumnMapping(preSelectedSources = [], destCol = '', transformation = '', ignoreChanges = false) {
    if (wizardSourceColumns.length === 0 || wizardDestColumns.length === 0) {
        showAlert('warning', 'Please load columns first');
        return;
    }

    const container = document.getElementById('wizardColumnMappingsContainer');

    // If this is the first mapping, clear the placeholder text
    if (wizardColumnMappingCounter === 0) {
        container.innerHTML = '';
    }

    const mappingId = wizardColumnMappingCounter++;

    // Convert single source to array
    if (typeof preSelectedSources === 'string' && preSelectedSources) {
        preSelectedSources = [preSelectedSources];
    }

    const mappingRow = document.createElement('div');
    mappingRow.className = 'card mb-2 wizard-column-mapping-row';
    mappingRow.id = `wizard-mapping-row-${mappingId}`;
    mappingRow.innerHTML = `
        <div class="card-body p-2">
            <div class="row align-items-center">
                <div class="col-md-5">
                    <label class="form-label small mb-1"><strong>Source Column(s)</strong></label>
                    <select class="form-select form-select-sm wizard-src-col-${mappingId}" multiple size="3">
                        ${wizardSourceColumns.map(col => {
                            const isSelected = preSelectedSources.includes(col.column_name);
                            return `<option value="${col.column_name}" ${isSelected ? 'selected' : ''}>${col.column_name} (${col.data_type})</option>`;
                        }).join('')}
                    </select>
                </div>
                <div class="col-md-1 text-center">
                    <i class="bi bi-arrow-right"></i>
                </div>
                <div class="col-md-5">
                    <label class="form-label small mb-1"><strong>Destination</strong></label>
                    <select class="form-select form-select-sm wizard-dest-col-${mappingId}">
                        <option value="">-- Select --</option>
                        ${wizardDestColumns.map(col =>
                            `<option value="${col.column_name}" ${col.column_name === destCol ? 'selected' : ''}>${col.column_name} (${col.data_type})</option>`
                        ).join('')}
                    </select>
                    <input type="text" class="form-control form-control-sm mt-1 wizard-trans-${mappingId}" placeholder="Transformation (optional)" value="${transformation || ''}">
                    <div class="form-check form-check-inline mt-1">
                        <input class="form-check-input wizard-ignore-${mappingId}" type="checkbox" ${ignoreChanges ? 'checked' : ''}>
                        <label class="form-check-label small">Ignore changes</label>
                    </div>
                </div>
                <div class="col-md-1 text-end">
                    <button type="button" class="btn btn-sm btn-danger" onclick="wizardRemoveColumnMapping(${mappingId})">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </div>
        </div>
    `;

    container.appendChild(mappingRow);
}

function wizardRemoveColumnMapping(mappingId) {
    const row = document.getElementById(`wizard-mapping-row-${mappingId}`);
    if (row) {
        row.remove();
    }

    const container = document.getElementById('wizardColumnMappingsContainer');
    if (container.querySelectorAll('.wizard-column-mapping-row').length === 0) {
        container.innerHTML = '<p class="text-muted">No column mappings. Click "Load Columns" and "Auto-map" to start.</p>';
        wizardColumnMappingCounter = 0;
    }
}

function wizardClearColumnMappings() {
    const container = document.getElementById('wizardColumnMappingsContainer');
    container.innerHTML = '<p class="text-muted">No column mappings. Click "Load Columns" and "Auto-map" to start.</p>';
    wizardColumnMappingCounter = 0;
}

function collectWizardColumnMappings() {
    const columnMappings = [];
    const mappingRows = document.querySelectorAll('.wizard-column-mapping-row');

    mappingRows.forEach(row => {
        const mappingId = row.id.replace('wizard-mapping-row-', '');

        const srcSelect = row.querySelector(`.wizard-src-col-${mappingId}`);
        const destSelect = row.querySelector(`.wizard-dest-col-${mappingId}`);
        const transInput = row.querySelector(`.wizard-trans-${mappingId}`);
        const ignoreCheck = row.querySelector(`.wizard-ignore-${mappingId}`);

        const sourceColumns = Array.from(srcSelect.selectedOptions).map(opt => opt.value);
        const destColumn = destSelect.value;

        if (sourceColumns.length > 0 && destColumn) {
            const mapping = {
                source_columns: sourceColumns,
                destination_column: destColumn,
                ignore_changes: ignoreCheck.checked
            };

            // Add transformation if provided
            const transValue = transInput.value.trim();
            if (transValue) {
                mapping.transformation = transValue;
            }

            columnMappings.push(mapping);
        }
    });

    mappingData.column_mappings = columnMappings;
    return columnMappings;
}

// Test SQL query
async function testSQLQuery() {
    const query = document.getElementById('wizardSourceQuery').value.trim();
    if (!query) {
        showAlert('warning', 'Please enter a SQL query first');
        return;
    }

    // Get connection info
    const connType = document.getElementById('wizardConnectionType').value;
    let connectionConfig;

    if (connType === 'library') {
        const connectionId = document.getElementById('wizardConnectionLibrary').value;
        if (!connectionId) {
            showAlert('warning', 'Please select a connection from the library to test the query');
            return;
        }

        // Fetch connection from library
        showLoading('Loading connection from library...');
        try {
            const response = await fetch(`${API_BASE}/admin/connection/${connectionId}`);
            if (!response.ok) throw new Error('Failed to load connection');
            connectionConfig = await response.json();
        } catch (error) {
            hideLoading();
            showAlert('danger', `Error loading connection: ${error.message}`);
            return;
        }
    } else if (connType === 'workset') {
        const worksetId = document.getElementById('wizardConnectionWorkset').value;
        if (!worksetId) {
            showAlert('warning', 'Please select a working set to test the query');
            return;
        }

        // Fetch workset to get connection
        showLoading('Loading workset connection...');
        try {
            const response = await fetch(`${API_BASE}/admin/workset/${worksetId}`);
            if (!response.ok) throw new Error('Failed to load workset');
            const workset = await response.json();
            connectionConfig = workset.source_connection;
        } catch (error) {
            hideLoading();
            showAlert('danger', `Error loading workset: ${error.message}`);
            return;
        }
    } else {
        hideLoading();
        showAlert('danger', 'Invalid connection type selected');
        return;
    }

    showLoading('Testing query...');
    try {
        const response = await fetch(`${API_BASE}/admin/mapping/test-query`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                connection: connectionConfig,
                query: query,
                limit: 10
            })
        });

        const result = await response.json();

        hideLoading();
        if (result.success) {
            showAlert('success', result.message || `Query executed successfully. Returned ${result.row_count} row(s).`);

            // Optionally show sample data
            if (result.sample_data && result.sample_data.length > 0) {
                console.log('Sample data:', result.sample_data);
                console.log('Columns:', result.columns);
            }
        } else {
            showAlert('danger', `Query test failed: ${result.error}`);
        }
    } catch (error) {
        hideLoading();
        showAlert('danger', `Error testing query: ${error.message}`);
    }
}

// Load worksets for assignment
async function loadWorksetsForWizard() {
    const container = document.getElementById('wizardWorksets');

    if (!container) return;

    container.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"></div> Loading worksets...';

    try {
        const response = await fetch(`${API_BASE}/admin/workset/list`);

        if (!response.ok) {
            container.innerHTML = '<div class="alert alert-warning">Could not load worksets. You can assign this mapping to worksets later.</div>';
            return;
        }

        const worksets = await response.json();

        if (!worksets || worksets.length === 0) {
            container.innerHTML = `
                <div class="alert alert-info">
                    <i class="bi bi-info-circle"></i> No working sets available yet.
                    <br><small>Create working sets in the "Working Sets" tab and assign this mapping later.</small>
                </div>
            `;
            return;
        }

        container.innerHTML = '';

        worksets.forEach(workset => {
            const isChecked = mappingData.assigned_worksets && mappingData.assigned_worksets.includes(workset.id);
            const div = document.createElement('div');
            div.className = 'form-check mb-2';
            div.innerHTML = `
                <input class="form-check-input wizard-workset-checkbox" type="checkbox" value="${workset.id}" id="ws_${workset.id}" ${isChecked ? 'checked' : ''}>
                <label class="form-check-label" for="ws_${workset.id}">
                    <strong>${workset.name}</strong>
                    ${workset.is_active ? '<span class="badge bg-success ms-1">Active</span>' : '<span class="badge bg-secondary ms-1">Inactive</span>'}
                    <br><small class="text-muted">ID: ${workset.id}</small>
                </label>
            `;
            container.appendChild(div);
        });

        const helpText = document.createElement('div');
        helpText.className = 'mt-3 text-muted small';
        helpText.innerHTML = '<i class="bi bi-info-circle"></i> Select working sets to include this mapping in their synchronization runs.';
        container.appendChild(helpText);

    } catch (error) {
        console.error('Error loading worksets:', error);
        container.innerHTML = `
            <div class="alert alert-warning">
                <i class="bi bi-exclamation-triangle"></i> Could not load worksets: ${error.message}
                <br><small>You can assign this mapping to worksets later.</small>
            </div>
        `;
    }
}

// Populate review content for step 5
function populateReviewContent() {
    // Collect all data
    for (let i = 1; i <= 4; i++) {
        const savedStep = currentStep;
        currentStep = i;
        collectStepData();
        currentStep = savedStep;
    }

    const container = document.getElementById('wizardReviewContent');
    if (!container) return;

    const typeBadge = mappingData.mapping_type === 'table'
        ? '<span class="badge bg-primary">Table-based</span>'
        : '<span class="badge bg-info">SQL-based</span>';

    let html = `
        <div class="card mb-3">
            <div class="card-header">
                <h6 class="mb-0"><i class="bi bi-info-circle"></i> Basic Information</h6>
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-md-6">
                        <strong>Mapping ID:</strong> ${mappingData.id || '<em class="text-danger">Not set</em>'}<br>
                        <strong>Name:</strong> ${mappingData.name || '<em class="text-muted">Not set</em>'}<br>
                        <strong>Type:</strong> ${typeBadge}
                    </div>
                    <div class="col-md-6">
                        <strong>Destination:</strong> ${mappingData.destination_schema}.${mappingData.destination_table}<br>
                        <strong>Status:</strong> ${mappingData.enabled ? '<span class="badge bg-success">Enabled</span>' : '<span class="badge bg-secondary">Disabled</span>'}
                    </div>
                </div>
            </div>
        </div>
    `;

    // Type-specific details
    if (mappingData.mapping_type === 'table') {
        html += `
            <div class="card mb-3">
                <div class="card-header">
                    <h6 class="mb-0"><i class="bi bi-table"></i> Table Configuration</h6>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <strong>Source:</strong> ${mappingData.source_schema}.${mappingData.source_table}<br>
                            <strong>Column Mappings:</strong> ${mappingData.column_mappings ? mappingData.column_mappings.length : 0} configured
                        </div>
                        <div class="col-md-6">
                            <strong>Sync Operations:</strong><br>
                            ${mappingData.sync_inserts ? '<span class="badge bg-success me-1"><i class="bi bi-plus"></i> Inserts</span>' : ''}
                            ${mappingData.sync_updates ? '<span class="badge bg-warning me-1"><i class="bi bi-pencil"></i> Updates</span>' : ''}
                            ${mappingData.sync_deletes ? '<span class="badge bg-danger me-1"><i class="bi bi-trash"></i> Deletes</span>' : ''}
                        </div>
                    </div>
                    <hr>
                    <div class="row">
                        <div class="col-md-6">
                            <strong>Initial Snapshot:</strong> ${mappingData.perform_initial_snapshot ? '<span class="badge bg-info">Yes</span>' : '<span class="badge bg-secondary">No</span>'}
                        </div>
                        <div class="col-md-6">
                            <strong>DuckDB Transformation:</strong> ${mappingData.use_duckdb_transformation ? '<span class="badge bg-info">Yes</span>' : '<span class="badge bg-secondary">No</span>'}
                        </div>
                    </div>
                </div>
            </div>
        `;
    } else if (mappingData.mapping_type === 'sql') {
        html += `
            <div class="card mb-3">
                <div class="card-header">
                    <h6 class="mb-0"><i class="bi bi-code-square"></i> SQL Configuration</h6>
                </div>
                <div class="card-body">
                    <div class="mb-2">
                        <strong>Source Query:</strong>
                        <pre class="bg-light p-2 mt-1 small" style="max-height: 150px; overflow-y: auto;">${mappingData.source_query || '<em class="text-danger">Not set</em>'}</pre>
                    </div>
                    ${mappingData.key_columns && mappingData.key_columns.length > 0 ? `
                        <div class="mb-2">
                            <strong>Key Columns:</strong> ${mappingData.key_columns.join(', ')}
                        </div>
                    ` : ''}
                    ${mappingData.insert_query ? `
                        <div class="mb-2">
                            <strong>Custom INSERT:</strong> <span class="badge bg-info">Configured</span>
                        </div>
                    ` : ''}
                    ${mappingData.update_query ? `
                        <div class="mb-2">
                            <strong>Custom UPDATE:</strong> <span class="badge bg-info">Configured</span>
                        </div>
                    ` : ''}
                </div>
            </div>
        `;
    }

    // Sync options
    html += `
        <div class="card mb-3">
            <div class="card-header">
                <h6 class="mb-0"><i class="bi bi-gear"></i> Sync Options</h6>
            </div>
            <div class="card-body">
                <div class="row">
                    <div class="col-md-4">
                        <strong>Sync Mode:</strong> <span class="badge bg-secondary">${mappingData.sync_mode || 'full'}</span>
                    </div>
                    <div class="col-md-4">
                        <strong>Batch Size:</strong> ${mappingData.batch_size || 1000}
                    </div>
                    <div class="col-md-4">
                        <strong>Timeout:</strong> ${mappingData.timeout_seconds || 300}s
                    </div>
                </div>
                <div class="mt-2">
                    <strong>Schedule:</strong> ${mappingData.sync_schedule ? `<code>${mappingData.sync_schedule}</code>` : '<span class="text-muted">Manual only</span>'}
                </div>
            </div>
        </div>
    `;

    // Working sets
    const selectedWorksets = mappingData.assigned_worksets || [];
    html += `
        <div class="card mb-3">
            <div class="card-header">
                <h6 class="mb-0"><i class="bi bi-collection"></i> Working Sets</h6>
            </div>
            <div class="card-body">
                ${selectedWorksets.length > 0
                    ? `<strong>Assigned to ${selectedWorksets.length} working set(s):</strong><br>${selectedWorksets.map(id => `<span class="badge bg-primary me-1">${id}</span>`).join('')}`
                    : '<span class="text-muted">Not assigned to any working sets</span>'
                }
            </div>
        </div>
    `;

    // Validation warnings
    let warnings = [];
    if (!mappingData.id) warnings.push('Mapping ID is required');
    if (!mappingData.name) warnings.push('Mapping Name is required');
    if (mappingData.mapping_type === 'table' && (!mappingData.column_mappings || mappingData.column_mappings.length === 0)) {
        warnings.push('At least one column mapping is required for table-based mappings');
    }
    if (mappingData.mapping_type === 'sql' && !mappingData.source_query) {
        warnings.push('Source query is required for SQL-based mappings');
    }

    if (warnings.length > 0) {
        html += `
            <div class="alert alert-warning">
                <strong><i class="bi bi-exclamation-triangle"></i> Please address the following before saving:</strong>
                <ul class="mb-0 mt-2">
                    ${warnings.map(w => `<li>${w}</li>`).join('')}
                </ul>
            </div>
        `;
    } else {
        html += `
            <div class="alert alert-success">
                <i class="bi bi-check-circle"></i> Configuration looks good! Click "Save Mapping" to create the mapping.
            </div>
        `;
    }

    container.innerHTML = html;
}

// Helper: Show cron helper
function showCronHelper() {
    alert(`Cron Expression Examples:

    Every minute: * * * * *
    Every hour: 0 * * * *
    Every day at midnight: 0 0 * * *
    Every Monday at 9 AM: 0 9 * * 1
    Every 15 minutes: */15 * * * *
    Every day at 2:30 PM: 30 14 * * *

    Format: minute hour day month weekday`);
}

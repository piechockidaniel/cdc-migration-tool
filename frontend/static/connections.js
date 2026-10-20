// Connection Library Management
let currentConnectionId = null;  // For editing
let isEditMode = false;

// ============================================================================
// CONNECTION MANAGEMENT
// ============================================================================

// Load connections when page loads
document.addEventListener('DOMContentLoaded', () => {
    // Only load if we're on the admin page with connections tab
    if (document.getElementById('connectionsTableBody')) {
        loadConnections();
    }
});

// Load all connections from API
async function loadConnections() {
    const tbody = document.getElementById('connectionsTableBody');
    if (!tbody) return;

    tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted"><div class="spinner-border spinner-border-sm me-2"></div>Loading connections...</td></tr>';

    try {
        const response = await fetch(`${API_BASE}/admin/connection/list`);
        if (!response.ok) throw new Error('Failed to load connections');

        const connections = await response.json();

        if (!connections || connections.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" class="text-center text-muted">
                        <i class="bi bi-inbox"></i> No connections yet. Click "New Connection" to create one.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = '';
        connections.forEach(conn => {
            const row = createConnectionRow(conn);
            tbody.appendChild(row);
        });

    } catch (error) {
        console.error('Error loading connections:', error);
        tbody.innerHTML = `
            <tr>
                <td colspan="5" class="text-center text-danger">
                    <i class="bi bi-exclamation-circle"></i> Error loading connections: ${error.message}
                </td>
            </tr>
        `;
        showAlert('danger', `Failed to load connections: ${error.message}`);
    }
}

// Create table row for a connection
function createConnectionRow(conn) {
    const tr = document.createElement('tr');

    const authType = conn.use_windows_auth ?
        '<span class="badge bg-primary">Windows Auth</span>' :
        '<span class="badge bg-secondary">SQL Auth</span>';
    const engineBadge = `<span class="badge bg-dark">${conn.engine || 'mssql'}</span>`;

    const portInfo = conn.port ? `:${conn.port}` : '';

    tr.innerHTML = `
        <td>
            <strong>${conn.name || conn.id}</strong> ${engineBadge}
            <br><small class="text-muted">ID: ${conn.id}</small>
        </td>
        <td>${conn.server}${portInfo}</td>
        <td>${conn.database}</td>
        <td>${authType}</td>
        <td>
            <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-info" onclick="testConnectionById('${conn.id}')" title="Test Connection">
                    <i class="bi bi-check-circle"></i>
                </button>
                <button class="btn btn-outline-primary" onclick="editConnection('${conn.id}')" title="Edit">
                    <i class="bi bi-pencil"></i>
                </button>
                <button class="btn btn-outline-danger" onclick="deleteConnection('${conn.id}')" title="Delete">
                    <i class="bi bi-trash"></i>
                </button>
            </div>
        </td>
    `;

    return tr;
}

// Show connection modal for new or edit
function showConnectionModal(connectionId = null) {
    isEditMode = !!connectionId;
    currentConnectionId = connectionId;

    // Reset form
    document.getElementById('connectionForm').reset();
    document.getElementById('connWindowsAuth').checked = true;
    toggleCredentials();

    if (isEditMode) {
        document.getElementById('connectionModalTitle').textContent = 'Edit Connection';
        loadConnectionForEdit(connectionId);
    } else {
        document.getElementById('connectionModalTitle').textContent = 'New Connection';
        document.getElementById('connId').value = '';
        document.getElementById('connId').removeAttribute('readonly');
    }

    const modal = new bootstrap.Modal(document.getElementById('connectionModal'));
    modal.show();
}

// Load connection data for editing
async function loadConnectionForEdit(connectionId) {
    showLoading('Loading connection...');

    try {
        const response = await fetch(`${API_BASE}/admin/connection/${connectionId}`);
        if (!response.ok) throw new Error('Connection not found');

        const conn = await response.json();

        // Populate form
        document.getElementById('connId').value = conn.id || '';
        document.getElementById('connId').setAttribute('readonly', 'readonly');
        document.getElementById('connName').value = conn.name || '';
        document.getElementById('connServer').value = conn.server || '';
        document.getElementById('connPort').value = conn.port || '';
        document.getElementById('connDatabase').value = conn.database || '';
        document.getElementById('connWindowsAuth').checked = conn.use_windows_auth !== false;
        document.getElementById('connUsername').value = conn.username || '';
        document.getElementById('connPassword').value = conn.password || '';
        document.getElementById('connDriver').value = conn.driver || 'ODBC Driver 18 for SQL Server';
        document.getElementById('connEngine').value = conn.engine || 'mssql';
        document.getElementById('connOptions').value = conn.options && Object.keys(conn.options).length ? JSON.stringify(conn.options) : '';

        toggleCredentials();
        toggleEngineFields();
        hideLoading();

    } catch (error) {
        hideLoading();
        showAlert('danger', `Error loading connection: ${error.message}`);
        bootstrap.Modal.getInstance(document.getElementById('connectionModal')).hide();
    }
}

// Toggle credentials visibility
function toggleCredentials() {
    const useWindowsAuth = document.getElementById('connWindowsAuth').checked;
    const credDiv = document.getElementById('connCredentialsDiv');
    credDiv.style.display = useWindowsAuth ? 'none' : 'block';
}

// Save connection (create or update)
async function saveConnection() {
    // Collect form data
    const connId = document.getElementById('connId').value.trim();
    const connName = document.getElementById('connName').value.trim();
    const server = document.getElementById('connServer').value.trim();
    const database = document.getElementById('connDatabase').value.trim();

    // Validation
    if (!connName || !server || !database) {
        showAlert('warning', 'Please fill in all required fields (Name, Server, Database)');
        return;
    }

    // Generate ID from name if not provided
    const finalId = connId || connName.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');

    const portValue = document.getElementById('connPort').value.trim();
    const connectionData = {
        id: finalId,
        name: connName,
        server: server,
        database: database,
        use_windows_auth: document.getElementById('connWindowsAuth').checked,
        driver: document.getElementById('connDriver').value,
        engine: document.getElementById('connEngine').value
    };
    const optionsText = document.getElementById('connOptions').value.trim();
    if (optionsText) {
        try { connectionData.options = JSON.parse(optionsText); }
        catch (e) { showAlert('danger', 'Engine options must be valid JSON'); return; }
    }

    // Only include port if specified
    if (portValue) {
        connectionData.port = parseInt(portValue);
    }

    // Include credentials if SQL Auth
    if (!connectionData.use_windows_auth) {
        connectionData.username = document.getElementById('connUsername').value.trim();
        connectionData.password = document.getElementById('connPassword').value;
    }

    showLoading(isEditMode ? 'Updating connection...' : 'Saving connection...');

    try {
        const url = isEditMode ?
            `${API_BASE}/admin/connection/update` :
            `${API_BASE}/admin/connection/save`;

        const response = await fetch(url, {
            method: isEditMode ? 'PUT' : 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(connectionData)
        });

        const result = await response.json();

        hideLoading();

        if (result.success || response.ok) {
            showAlert('success', result.message || 'Connection saved successfully');
            bootstrap.Modal.getInstance(document.getElementById('connectionModal')).hide();
            loadConnections();  // Reload table
        } else {
            showAlert('danger', result.message || 'Failed to save connection');
        }

    } catch (error) {
        hideLoading();
        showAlert('danger', `Error saving connection: ${error.message}`);
    }
}

// Test connection in modal
async function testConnectionInModal() {
    // Collect form data
    const connName = document.getElementById('connName').value.trim();
    const server = document.getElementById('connServer').value.trim();
    const database = document.getElementById('connDatabase').value.trim();

    if (!server || !database) {
        showAlert('warning', 'Please provide at least Server and Database to test');
        return;
    }

    const portValue = document.getElementById('connPort').value.trim();
    const connectionData = {
        name: connName || 'Test Connection',
        server: server,
        database: database,
        use_windows_auth: document.getElementById('connWindowsAuth').checked,
        driver: document.getElementById('connDriver').value,
        engine: document.getElementById('connEngine').value
    };
    const optionsText = document.getElementById('connOptions').value.trim();
    if (optionsText) {
        try { connectionData.options = JSON.parse(optionsText); }
        catch (e) { showAlert('danger', 'Engine options must be valid JSON'); return; }
    }

    // Only include port if specified
    if (portValue) {
        connectionData.port = parseInt(portValue);
    }

    // Include credentials if SQL Auth
    if (!connectionData.use_windows_auth) {
        connectionData.username = document.getElementById('connUsername').value.trim();
        connectionData.password = document.getElementById('connPassword').value;
    }

    showLoading('Testing connection...');

    try {
        const response = await fetch(`${API_BASE}/admin/connect/test`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(connectionData)
        });

        const result = await response.json();

        hideLoading();

        if (result.success) {
            const version = result.server_version ? `<br><small>Server: ${result.server_version}</small>` : '';
            showAlert('success', `✓ Connection successful!${version}`);
        } else {
            showAlert('danger', `✗ Connection failed: ${result.message}`);
        }

    } catch (error) {
        hideLoading();
        showAlert('danger', `Error testing connection: ${error.message}`);
    }
}

// Test connection by ID from table
async function testConnectionById(connectionId) {
    showLoading('Testing connection...');

    try {
        // Load connection data
        const response = await fetch(`${API_BASE}/admin/connection/${connectionId}`);
        if (!response.ok) throw new Error('Connection not found');

        const conn = await response.json();

        // Test connection
        const testResponse = await fetch(`${API_BASE}/admin/connect/test`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(conn)
        });

        const result = await testResponse.json();

        hideLoading();

        if (result.success) {
            const version = result.server_version ? `<br><small>Server: ${result.server_version}</small>` : '';
            showAlert('success', `✓ Connection "${conn.name}" successful!${version}`);
        } else {
            showAlert('danger', `✗ Connection "${conn.name}" failed: ${result.message}`);
        }

    } catch (error) {
        hideLoading();
        showAlert('danger', `Error testing connection: ${error.message}`);
    }
}

// Edit connection
function editConnection(connectionId) {
    showConnectionModal(connectionId);
}

// Delete connection with confirmation
async function deleteConnection(connectionId) {
    if (!confirm(`Are you sure you want to delete connection "${connectionId}"?\n\nThis action cannot be undone.`)) {
        return;
    }

    showLoading('Deleting connection...');

    try {
        const response = await fetch(`${API_BASE}/admin/connection/${connectionId}`, {
            method: 'DELETE'
        });

        const result = await response.json();

        hideLoading();

        if (result.success || response.ok) {
            showAlert('success', result.message || 'Connection deleted successfully');
            loadConnections();  // Reload table
        } else {
            showAlert('danger', result.message || 'Failed to delete connection');
        }

    } catch (error) {
        hideLoading();
        showAlert('danger', `Error deleting connection: ${error.message}`);
    }
}


// Engine specific hints / visibility
const ENGINE_HINTS = {
    mssql: 'SQL Server: capture_instances {"dbo.T": "dbo_T_v2"}, service_broker_queue "dbo.cdcsync_notify_queue", read_only_intent, poll_min_seconds, poll_max_seconds, row_filter',
    postgresql: 'PostgreSQL: slot_name, plugin (wal2json | test_decoding), notify_channel, sslmode',
    mariadb: 'MariaDB/MySQL: server_id (unique replica id), batch_limit',
    mongodb: 'MongoDB: uri (overrides host/port), auth_database, options ("replicaSet=rs0" / "directConnection=true"), full_document_before_change',
    oracle: 'Oracle: service_name (or dsn), schema (owner), supplemental_mode (ALL | PRIMARY KEY), mode (SYSDBA), max_scn_window',
    sqlite: 'SQLite: path (file)'
};
function toggleEngineFields() {
    const engine = document.getElementById('connEngine').value;
    document.getElementById('connDriverDiv').style.display = (engine === 'mssql') ? '' : 'none';
    document.getElementById('connOptionsHint').textContent = ENGINE_HINTS[engine] || '';
    if (engine !== 'mssql' && document.getElementById('connWindowsAuth').checked) {
        document.getElementById('connWindowsAuth').checked = false;
        toggleCredentials();
    }
}

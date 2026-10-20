// Monitoring page JavaScript

let ws = null;
let reconnectInterval = null;

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    loadMappingsForVerification();
    setDefaultDates();
});

// WebSocket connection
function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/api/monitoring/events`;
    
    ws = new WebSocket(wsUrl);
    
    ws.onopen = function() {
        document.getElementById('wsStatus').textContent = 'Connected';
        document.getElementById('wsStatus').className = 'badge bg-success';
        console.log('WebSocket connected');
        
        // Clear reconnect interval if exists
        if (reconnectInterval) {
            clearInterval(reconnectInterval);
            reconnectInterval = null;
        }
        
        // Send ping every 30 seconds to keep connection alive
        setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send('ping');
            }
        }, 30000);
    };
    
    ws.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'cdc_event') {
                addLiveEvent(data);
            }
        } catch (error) {
            console.error('Error parsing WebSocket message:', error);
        }
    };
    
    ws.onerror = function(error) {
        console.error('WebSocket error:', error);
        document.getElementById('wsStatus').textContent = 'Error';
        document.getElementById('wsStatus').className = 'badge bg-danger';
    };
    
    ws.onclose = function() {
        console.log('WebSocket disconnected');
        document.getElementById('wsStatus').textContent = 'Disconnected';
        document.getElementById('wsStatus').className = 'badge bg-secondary';
        
        // Attempt to reconnect after 5 seconds
        if (!reconnectInterval) {
            reconnectInterval = setTimeout(() => {
                console.log('Attempting to reconnect...');
                connectWebSocket();
            }, 5000);
        }
    };
}

// Add live event to display
function addLiveEvent(event) {
    const container = document.getElementById('liveEvents');
    
    // Remove "no events" message if present
    if (container.querySelector('.text-muted')) {
        container.innerHTML = '';
    }
    
    const eventClass = event.operation.toLowerCase();
    const statusClass = event.status === 'failed' ? 'failed' : '';
    
    const eventHtml = `
        <div class="event-item ${eventClass} ${statusClass} new">
            <div class="d-flex justify-content-between">
                <div>
                    <strong>${event.source_table}</strong>
                    <span class="badge bg-${getOperationColor(event.operation)}">${event.operation}</span>
                    ${event.status === 'failed' ? '<span class="badge bg-danger">FAILED</span>' : '<span class="badge bg-success">SUCCESS</span>'}
                </div>
                <small class="text-muted">${new Date(event.timestamp).toLocaleTimeString()}</small>
            </div>
            ${event.error ? `<div class="text-danger mt-1"><small>Error: ${event.error}</small></div>` : ''}
        </div>
    `;
    
    // Prepend to container
    container.insertAdjacentHTML('afterbegin', eventHtml);
    
    // Limit to 100 events
    const events = container.querySelectorAll('.event-item');
    if (events.length > 100) {
        events[events.length - 1].remove();
    }
}

function getOperationColor(operation) {
    switch (operation.toUpperCase()) {
        case 'INSERT': return 'success';
        case 'UPDATE': return 'info';
        case 'DELETE': return 'danger';
        default: return 'secondary';
    }
}

function clearLiveEvents() {
    document.getElementById('liveEvents').innerHTML = '<p class="text-muted">No events to display</p>';
}

// Load statistics
async function loadStatistics() {
    try {
        const response = await fetch(`${API_BASE}/monitoring/statistics`);
        const stats = await response.json();
        
        document.getElementById('statsTotalEvents').textContent = stats.total_events || 0;
        document.getElementById('statsSuccessful').textContent = stats.successful_events || 0;
        document.getElementById('statsFailed').textContent = stats.failed_events || 0;
        document.getElementById('statsEPS').textContent = (stats.events_per_second || 0).toFixed(2);
    } catch (error) {
        console.error('Error loading statistics:', error);
    }
}

// Set default dates for history query
function setDefaultDates() {
    const today = new Date().toISOString().split('T')[0];
    document.getElementById('historyStartDate').value = today;
    document.getElementById('historyEndDate').value = today;
}

// Query history
async function queryHistory() {
    const startDate = document.getElementById('historyStartDate').value;
    const endDate = document.getElementById('historyEndDate').value;
    const status = document.getElementById('historyStatus').value;
    
    const resultsDiv = document.getElementById('historyResults');
    resultsDiv.innerHTML = '<div class="spinner-border"></div> Loading...';
    
    try {
        let url = `${API_BASE}/monitoring/history?`;
        if (startDate) url += `start_date=${startDate}&`;
        if (endDate) url += `end_date=${endDate}&`;
        if (status) url += `status=${status}&`;
        url += 'limit=100';
        
        const response = await fetch(url);
        const events = await response.json();
        
        if (events.length === 0) {
            resultsDiv.innerHTML = '<p class="text-muted">No events found for the specified criteria</p>';
            return;
        }
        
        let html = '<table class="table table-striped"><thead><tr><th>Time</th><th>Table</th><th>Operation</th><th>Status</th><th>Rows</th></tr></thead><tbody>';
        
        events.forEach(event => {
            const statusBadge = event.status === 'processed' ? 
                '<span class="badge bg-success">Success</span>' : 
                '<span class="badge bg-danger">Failed</span>';
            
            html += `
                <tr>
                    <td>${new Date(event.timestamp).toLocaleString()}</td>
                    <td>${event.source_table}</td>
                    <td><span class="badge bg-${getOperationColor(event.operation)}">${event.operation}</span></td>
                    <td>${statusBadge}</td>
                    <td>${event.affected_rows}</td>
                </tr>
            `;
        });
        
        html += '</tbody></table>';
        resultsDiv.innerHTML = html;
    } catch (error) {
        resultsDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

// Load mappings for verification dropdown
async function loadMappingsForVerification() {
    try {
        const response = await fetch(`${API_BASE}/admin/mapping/list`);
        const mappings = await response.json();
        
        const select = document.getElementById('verifyMappingId');
        select.innerHTML = '<option value="">Select a mapping...</option>';
        
        mappings.forEach(mapping => {
            const option = document.createElement('option');
            option.value = mapping.id;
            option.textContent = `${mapping.source_schema}.${mapping.source_table} → ${mapping.destination_schema}.${mapping.destination_table}`;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading mappings:', error);
    }
}

// Run verification
async function runVerification() {
    const mappingId = document.getElementById('verifyMappingId').value;
    const sampleSize = parseInt(document.getElementById('verifySampleSize').value);
    const checkRowCounts = document.getElementById('verifyRowCounts').checked;
    const checkReverseMapping = document.getElementById('verifyReverseMapping').checked;
    
    if (!mappingId) {
        alert('Please select a mapping to verify');
        return;
    }
    
    const resultsDiv = document.getElementById('verificationResults');
    resultsDiv.innerHTML = '<div class="spinner-border"></div> Running verification...';
    
    try {
        const response = await fetch(`${API_BASE}/monitoring/verify`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                mapping_id: mappingId,
                sample_size: sampleSize,
                check_row_counts: checkRowCounts,
                check_reverse_mapping: checkReverseMapping
            })
        });
        
        const result = await response.json();
        
        // Display results
        let html = '<div class="card mt-3"><div class="card-body">';
        html += '<h5 class="card-title">Verification Results</h5>';
        html += `<p><strong>Timestamp:</strong> ${new Date(result.timestamp).toLocaleString()}</p>`;
        
        if (checkRowCounts) {
            const rowCountMatch = result.row_count_match ? 
                '<span class="badge bg-success">Match</span>' : 
                '<span class="badge bg-danger">Mismatch</span>';
            
            html += `
                <h6>Row Count Comparison:</h6>
                <ul>
                    <li>Source Rows: ${result.source_row_count}</li>
                    <li>Destination Rows: ${result.destination_row_count}</li>
                    <li>Status: ${rowCountMatch}</li>
                </ul>
            `;
        }
        
        html += `
            <h6>Sample Verification:</h6>
            <ul>
                <li>Samples Checked: ${result.samples_checked}</li>
                <li>Samples Matched: ${result.samples_matched}</li>
                <li>Match Rate: ${result.samples_checked > 0 ? ((result.samples_matched / result.samples_checked) * 100).toFixed(2) : 0}%</li>
            </ul>
        `;
        
        if (checkReverseMapping) {
            html += `
                <h6>Reverse Mapping:</h6>
                <ul>
                    <li>Success Rate: ${(result.reverse_mapping_success_rate * 100).toFixed(2)}%</li>
                </ul>
            `;
        }
        
        if (result.mismatches && result.mismatches.length > 0) {
            html += `<h6 class="text-danger">Mismatches Found: ${result.mismatches.length}</h6>`;
            html += '<div class="alert alert-warning">Review mismatches in the detailed results</div>';
        }
        
        if (result.errors && result.errors.length > 0) {
            html += '<h6 class="text-danger">Errors:</h6><ul>';
            result.errors.forEach(error => {
                html += `<li>${error}</li>`;
            });
            html += '</ul>';
        }
        
        html += '</div></div>';
        
        resultsDiv.innerHTML = html;
    } catch (error) {
        resultsDiv.innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
    }
}

// Auto-load statistics when tab is shown
document.getElementById('statistics-tab').addEventListener('click', loadStatistics);

// Connect WebSocket when live events tab is shown
document.getElementById('live-tab').addEventListener('click', function() {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Auto-connect if not already connected
        setTimeout(() => {
            const btn = document.querySelector('#live button');
            if (btn && btn.textContent.includes('Connect')) {
                // Don't auto-connect, let user do it manually
            }
        }, 100);
    }
});






// Operations page JavaScript

let statusInterval = null;

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    refreshStatus();
    startStatusPolling();
});

// Start synchronization
async function startSync() {
    try {
        const response = await fetch(`${API_BASE}/operations/start`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', result.message);
            refreshStatus();
        } else {
            showAlert('danger', result.detail || 'Failed to start synchronization');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// Stop synchronization
async function stopSync() {
    if (!confirm('Are you sure you want to stop synchronization?')) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/operations/stop`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', result.message);
            refreshStatus();
        } else {
            showAlert('danger', 'Failed to stop synchronization');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// Pause synchronization
async function pauseSync() {
    try {
        const response = await fetch(`${API_BASE}/operations/pause`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('warning', result.message);
            refreshStatus();
        } else {
            showAlert('danger', 'Failed to pause synchronization');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// Resume synchronization
async function resumeSync() {
    try {
        const response = await fetch(`${API_BASE}/operations/resume`, {
            method: 'POST'
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('success', result.message);
            refreshStatus();
        } else {
            showAlert('danger', 'Failed to resume synchronization');
        }
    } catch (error) {
        showAlert('danger', `Error: ${error.message}`);
    }
}

// Refresh status
async function refreshStatus() {
    try {
        // Get sync status
        const statusResponse = await fetch(`${API_BASE}/operations/status`);
        const status = await statusResponse.json();
        
        // Update status display
        const statusBadge = document.getElementById('currentStatus');
        statusBadge.textContent = status.status.toUpperCase();
        statusBadge.className = 'status-badge status-' + status.status;
        
        document.getElementById('activeWorkset').textContent = status.working_set_name || 'None';
        document.getElementById('startTime').textContent = status.start_time ? new Date(status.start_time).toLocaleString() : 'N/A';
        document.getElementById('eventsProcessed').textContent = status.events_processed;
        
        // Update button states
        const isRunning = status.status === 'running';
        const isPaused = status.status === 'paused';
        
        document.getElementById('startBtn').disabled = isRunning || isPaused;
        document.getElementById('stopBtn').disabled = !isRunning && !isPaused;
        document.getElementById('pauseBtn').disabled = !isRunning;
        document.getElementById('resumeBtn').disabled = !isPaused;
        
        // Get statistics
        const statsResponse = await fetch(`${API_BASE}/operations/statistics`);
        const stats = await statsResponse.json();
        
        document.getElementById('totalEvents').textContent = stats.total_events || 0;
        document.getElementById('successfulEvents').textContent = stats.successful_events || 0;
        document.getElementById('failedEvents').textContent = stats.failed_events || 0;
        document.getElementById('eventsPerSecond').textContent = (stats.events_per_second || 0).toFixed(2);
        
    } catch (error) {
        console.error('Error refreshing status:', error);
    }
}

// Start polling for status updates
function startStatusPolling() {
    if (statusInterval) {
        clearInterval(statusInterval);
    }
    
    statusInterval = setInterval(refreshStatus, 5000); // Every 5 seconds
}

// Stop polling when page is hidden
document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
        if (statusInterval) {
            clearInterval(statusInterval);
            statusInterval = null;
        }
    } else {
        startStatusPolling();
        refreshStatus();
    }
});

// Utility function to show alerts
function showAlert(type, message) {
    const container = document.getElementById('alertContainer');
    
    const alertHtml = `
        <div class="alert alert-${type} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    container.innerHTML = alertHtml;
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        container.innerHTML = '';
    }, 5000);
}






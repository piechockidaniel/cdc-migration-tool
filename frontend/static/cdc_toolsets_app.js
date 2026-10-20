const API_BASE = '/api';


// ============================================================================
// SHARED UI FUNCTIONS (Loading & Alerts)
// ============================================================================

let loadingOverlay = null;
let alertContainer = null;

function initAlertContainer() {
    if (!alertContainer) {
        alertContainer = document.createElement('div');
        alertContainer.className = 'wizard-alert-container';
        document.body.appendChild(alertContainer);
    }
}

function showAlert(type, message, duration = 5000) {
    initAlertContainer();
    const typeConfig = {
        success: { icon: 'bi-check-circle-fill', color: 'success' },
        danger: { icon: 'bi-exclamation-circle-fill', color: 'danger' },
        warning: { icon: 'bi-exclamation-triangle-fill', color: 'warning' },
        info: { icon: 'bi-info-circle-fill', color: 'info' }
    };
    const config = typeConfig[type] || typeConfig.info;
    const alertDiv = document.createElement('div');
    alertDiv.className = `wizard-alert wizard-alert-${config.color}`;
    alertDiv.innerHTML = `
        <div class="wizard-alert-content">
            <i class="bi ${config.icon} wizard-alert-icon"></i>
            <div class="wizard-alert-message">${message}</div>
            <button type="button" class="wizard-alert-close" onclick="this.parentElement.parentElement.remove()">
                <i class="bi bi-x"></i>
            </button>
        </div>
    `;
    alertContainer.appendChild(alertDiv);
    setTimeout(() => alertDiv.classList.add('show'), 10);
    if (duration > 0) {
        setTimeout(() => {
            alertDiv.classList.remove('show');
            setTimeout(() => {
                if (alertDiv.parentNode) alertDiv.parentNode.removeChild(alertDiv);
            }, 300);
        }, duration);
    }
}

function showLoading(message = 'Processing...') {
    hideLoading();
    loadingOverlay = document.createElement('div');
    loadingOverlay.className = 'wizard-loading-overlay';
    loadingOverlay.innerHTML = `
        <div class="wizard-loading-content">
            <div class="spinner-border text-primary mb-3" role="status" style="width: 3rem; height: 3rem;">
                <span class="visually-hidden">Loading...</span>
            </div>
            <div class="wizard-loading-message">${message}</div>
        </div>
    `;
    document.body.appendChild(loadingOverlay);
    setTimeout(() => loadingOverlay.classList.add('show'), 10);
}

function hideLoading() {
    if (loadingOverlay) {
        loadingOverlay.classList.remove('show');
        setTimeout(() => {
            if (loadingOverlay && loadingOverlay.parentNode) {
                loadingOverlay.parentNode.removeChild(loadingOverlay);
            }
            loadingOverlay = null;
        }, 300);
    }
}
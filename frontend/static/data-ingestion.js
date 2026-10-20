/**
 * Data Ingestion Module
 * Handles multi-source data comparison and pattern detection
 */

let ingestionSources = [];
let ingestionColumnMappings = [];
let ingestionPatterns = [];
let ingestionSourceCounter = 0;
let ingestionColumnCounter = 0;
let lastAnalysisResult = null;

document.addEventListener('DOMContentLoaded', function() {
    const ingestionTab = document.getElementById('ingestion-tab');
    if (ingestionTab) {
        ingestionTab.addEventListener('shown.bs.tab', function() {
            loadIngestionPatterns();
            loadAvailableConnections();
        });
    }
});

async function loadIngestionPatterns() {
    try {
        const response = await fetch('/api/admin/ingestion/patterns');
        const data = await response.json();

        if (data.success) {
            ingestionPatterns = data.patterns;
            renderIngestionPatterns();
        } else {
            showAlert('Error loading patterns', 'danger');
        }
    } catch (error) {
        console.error('Error loading ingestion patterns:', error);
        showAlert('Failed to load patterns: ' + error.message, 'danger');
    }
}

function renderIngestionPatterns() {
    const container = document.getElementById('ingestionPatternsList');
    if (!container) return;

    if (ingestionPatterns.length === 0) {
        container.innerHTML = '<p class="text-muted">No patterns available</p>';
        return;
    }

    let html = '<div class="row">';
    ingestionPatterns.forEach(pattern => {
        html += `
            <div class="col-md-6 mb-2">
                <div class="form-check">
                    <input class="form-check-input" type="checkbox"
                           id="pattern_${pattern.id}"
                           value="${pattern.id}"
                           ${pattern.enabled ? 'checked' : ''}>
                    <label class="form-check-label" for="pattern_${pattern.id}">
                        <strong>${pattern.name}</strong>
                        <br>
                        <small class="text-muted">${pattern.description}</small>
                    </label>
                </div>
            </div>
        `;
    });
    html += '</div>';

    container.innerHTML = html;
}

function addIngestionSource() {
    ingestionSourceCounter++;
    const sourceId = `source_${ingestionSourceCounter}`;

    const sourceHtml = `
        <div class="card mb-2" id="${sourceId}_card">
            <div class="card-body">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <h6 class="mb-0">Source ${ingestionSourceCounter}</h6>
                    <button class="btn btn-sm btn-outline-danger" onclick="removeIngestionSource('${sourceId}')">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
                <div class="row">
                    <div class="col-md-4 mb-2">
                        <label class="form-label">Alias</label>
                        <input type="text" class="form-control form-control-sm"
                               id="${sourceId}_alias"
                               placeholder="e.g., source1">
                    </div>
                    <div class="col-md-8 mb-2">
                        <button class="btn btn-sm btn-outline-primary" onclick="loadConnectionsForSource('${sourceId}')">
                            <i class="bi bi-arrow-clockwise"></i>
                        </button>
                        <label class="form-label">Connection</label>
                        <select class="form-select form-control-sm" id="${sourceId}_connection">
                            <option value="">-- Select Connection --</option>
                        </select>

                    </div>
                </div>
                <div class="row">
                    <div class="col-md-4 mb-2">
                        <label class="form-label">Schema</label>
                        <input type="text" class="form-control form-control-sm"
                               id="${sourceId}_schema"
                               value="dbo">
                    </div>
                    <div class="col-md-4 mb-2">
                        <label class="form-label">Table</label>
                        <input type="text" class="form-control form-control-sm"
                               id="${sourceId}_table">
                    </div>
                    <div class="col-md-4 mb-2">
                        <label class="form-label">Columns (comma-separated or *)</label>
                        <input type="text" class="form-control form-control-sm"
                               id="${sourceId}_columns"
                               value="*">
                    </div>
                </div>
                <div class="row">
                    <div class="col-md-12 mb-2">
                        <label class="form-label">WHERE Clause (optional)</label>
                        <input type="text" class="form-control form-control-sm"
                               id="${sourceId}_where"
                               placeholder="e.g., status = 'active'">
                    </div>
                </div>
            </div>
        </div>
    `;

    document.getElementById('ingestionSourcesList').insertAdjacentHTML('beforeend', sourceHtml);
    loadConnectionsForSource(sourceId);
}

function removeIngestionSource(sourceId) {
    const card = document.getElementById(`${sourceId}_card`);
    if (card) {
        card.remove();
    }
}

async function loadConnectionsForSource(sourceId) {
    try {
        const response = await fetch('/api/admin/connection/list');
        const data = await response.json();

        const selectElement = document.getElementById(`${sourceId}_connection`);
        if (selectElement && data) {
            selectElement.innerHTML = '<option value="">-- Select Connection --</option>';
            data.forEach(conn => {
                selectElement.innerHTML += `<option value="${conn.id}">${conn.name}</option>`;
            });
        }
    } catch (error) {
        console.error('Error loading connections:', error);
    }
}

async function loadAvailableConnections() {
    document.querySelectorAll('[id$="_connection"]').forEach(async (select) => {
        if (select.id.startsWith('source_')) {
            const sourceId = select.id.replace('_connection', '');
            await loadConnectionsForSource(sourceId);
        }
    });
}

function addIngestionColumnMapping() {
    ingestionColumnCounter++;
    const mappingId = `colmap_${ingestionColumnCounter}`;

    const mappingHtml = `
        <div class="card mb-2" id="${mappingId}_card">
            <div class="card-body p-2">
                <div class="row align-items-center">
                    <div class="col-md-3">
                        <label class="form-label small mb-1">Logical Name</label>
                        <input type="text" class="form-control form-control-sm"
                               id="${mappingId}_name"
                               placeholder="e.g., customer_name">
                    </div>
                    <div class="col-md-8">
                        <label class="form-label small mb-1">Source Columns (format: alias:column)</label>
                        <input type="text" class="form-control form-control-sm"
                               id="${mappingId}_sources"
                               placeholder="e.g., source1:FirstName, source2:first_name">
                        <small class="text-muted">Map each source's column for this logical field</small>
                    </div>
                    <div class="col-md-1 text-end">
                        <button class="btn btn-sm btn-outline-danger mt-3"
                                onclick="removeIngestionColumnMapping('${mappingId}')">
                            <i class="bi bi-trash"></i>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;

    document.getElementById('ingestionColumnMappings').insertAdjacentHTML('beforeend', mappingHtml);
}

function removeIngestionColumnMapping(mappingId) {
    const card = document.getElementById(`${mappingId}_card`);
    if (card) {
        card.remove();
    }
}

function collectIngestionSources() {
    const sources = [];
    const sourceCards = document.querySelectorAll('[id$="_card"]');

    sourceCards.forEach(card => {
        const cardId = card.id.replace('_card', '');
        if (!cardId.startsWith('source_')) return;

        const alias = document.getElementById(`${cardId}_alias`)?.value;
        const connectionId = document.getElementById(`${cardId}_connection`)?.value;
        const schema = document.getElementById(`${cardId}_schema`)?.value;
        const table = document.getElementById(`${cardId}_table`)?.value;
        const columnsStr = document.getElementById(`${cardId}_columns`)?.value || '*';
        const whereClause = document.getElementById(`${cardId}_where`)?.value;

        if (alias && connectionId && schema && table) {
            const columns = columnsStr === '*' ? [] : columnsStr.split(',').map(c => c.trim());

            sources.push({
                connection_id: connectionId,
                schema_name: schema,
                table_name: table,
                alias: alias,
                columns: columns,
                where_clause: whereClause || null
            });
        }
    });

    return sources;
}

function collectColumnMappings() {
    const mappings = [];
    const mappingCards = document.querySelectorAll('[id^="colmap_"][id$="_card"]');

    mappingCards.forEach(card => {
        const cardId = card.id.replace('_card', '');
        const name = document.getElementById(`${cardId}_name`)?.value;
        const sourcesStr = document.getElementById(`${cardId}_sources`)?.value;

        if (name && sourcesStr) {
            const sourceColumns = {};
            const parts = sourcesStr.split(',');

            parts.forEach(part => {
                const [alias, column] = part.trim().split(':');
                if (alias && column) {
                    sourceColumns[alias.trim()] = column.trim();
                }
            });

            if (Object.keys(sourceColumns).length > 0) {
                mappings.push({
                    name: name,
                    source_columns: sourceColumns
                });
            }
        }
    });

    return mappings;
}

function collectSelectedPatterns() {
    const patterns = [];

    ingestionPatterns.forEach(pattern => {
        const checkbox = document.getElementById(`pattern_${pattern.id}`);
        if (checkbox && checkbox.checked) {
            patterns.push(pattern.id);
        }
    });

    return patterns;
}

async function runIngestionAnalysis() {
    const sources = collectIngestionSources();
    const columnMappings = collectColumnMappings();
    const joinKeysStr = document.getElementById('ingestionJoinKeys')?.value || '';
    const joinKeys = joinKeysStr.split(',').map(k => k.trim()).filter(k => k);
    const selectedPatterns = collectSelectedPatterns();
    const maxRecords = parseInt(document.getElementById('ingestionMaxRecords')?.value || 10000);

    if (sources.length < 2) {
        showAlert('Please add at least 2 sources to compare', 'warning');
        return;
    }

    if (columnMappings.length === 0) {
        showAlert('Please add at least one column mapping', 'warning');
        return;
    }

    if (joinKeys.length === 0) {
        showAlert('Please specify join keys', 'warning');
        return;
    }

    const request = {
        sources: sources,
        column_mappings: columnMappings,
        join_keys: joinKeys,
        apply_patterns: selectedPatterns,
        max_records: maxRecords
    };

    try {
        showAlert('Running analysis...', 'info');

        const response = await fetch('/api/admin/ingestion/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        });

        const data = await response.json();

        if (data.success) {
            lastAnalysisResult = data.result;
            displayIngestionResults(data.result);
            showAlert('Analysis completed successfully', 'success');
        } else {
            showAlert('Analysis failed: ' + (data.detail || 'Unknown error'), 'danger');
        }
    } catch (error) {
        console.error('Error running analysis:', error);
        showAlert('Failed to run analysis: ' + error.message, 'danger');
    }
}

function displayIngestionResults(result) {
    document.getElementById('ingestionTotalRecords').textContent = result.total_records;
    document.getElementById('ingestionMatchedRecords').textContent = result.matched_records;
    document.getElementById('ingestionUnmatchedRecords').textContent = result.unmatched_records;
    document.getElementById('ingestionMatchPercentage').textContent = result.match_percentage.toFixed(2) + '%';

    const mismatchesTable = document.getElementById('ingestionMismatchesTable');
    if (mismatchesTable) {
        mismatchesTable.innerHTML = '';

        result.mismatches.forEach(mismatch => {
            const valuesHtml = Object.entries(mismatch.values)
                .map(([source, value]) => `<div><strong>${source}:</strong> ${value}</div>`)
                .join('');

            const patternsHtml = mismatch.detected_patterns.length > 0
                ? mismatch.detected_patterns.map(p => `<span class="badge bg-warning text-dark">${p}</span>`).join(' ')
                : '<span class="text-muted">None</span>';

            const row = `
                <tr>
                    <td><small>${mismatch.row_id}</small></td>
                    <td>${mismatch.column_name}</td>
                    <td><small>${valuesHtml}</small></td>
                    <td>${patternsHtml}</td>
                    <td><small class="text-info">${mismatch.suggested_fix || 'N/A'}</small></td>
                </tr>
            `;
            mismatchesTable.insertAdjacentHTML('beforeend', row);
        });
    }

    document.getElementById('ingestionResultsCard').style.display = 'block';

    document.getElementById('ingestionResultsCard').scrollIntoView({ behavior: 'smooth' });
}

function exportIngestionResults(format) {
    if (!lastAnalysisResult) {
        showAlert('No analysis results to export', 'warning');
        return;
    }

    if (format === 'csv') {
        exportToCSV(lastAnalysisResult);
    } else if (format === 'json') {
        exportToJSON(lastAnalysisResult);
    }
}

function exportToCSV(result) {
    const headers = ['Row ID', 'Column', 'Source', 'Value', 'Detected Patterns', 'Suggested Fix'];
    const rows = [headers];

    result.mismatches.forEach(mismatch => {
        Object.entries(mismatch.values).forEach(([source, value]) => {
            rows.push([
                mismatch.row_id,
                mismatch.column_name,
                source,
                value,
                mismatch.detected_patterns.join('; '),
                mismatch.suggested_fix || ''
            ]);
        });
    });

    const csvContent = rows.map(row => row.map(cell => `"${cell}"`).join(',')).join('\n');
    downloadFile(csvContent, 'ingestion_analysis.csv', 'text/csv');
}

function exportToJSON(result) {
    const jsonContent = JSON.stringify(result, null, 2);
    downloadFile(jsonContent, 'ingestion_analysis.json', 'application/json');
}

function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}

function showAlert(message, type = 'info') {
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
